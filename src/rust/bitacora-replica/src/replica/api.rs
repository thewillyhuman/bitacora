//! The operations a replica offers to other programs.
//!
//! # Why this file exists
//!
//! Writers, readers, the controller and other replicas all reach a replica through one interface,
//! `ReplicaApi`, defined next to the types they share so that the transport, the simulator and the tests
//! can stand in for one another. This file is the real thing behind that interface: for each operation it
//! decides what must be checked for the node as a whole, finds or starts the task of the segment
//! concerned, and hands the operation over.
//!
//! # What it contains
//!
//! - `impl ReplicaApi for Replica`: append, read, fence, seal, repair, upload, delete, list, status and
//!   drain.
//!
//! # How it works
//!
//! Each operation is a few lines, and they differ in three things:
//!
//! | Operation | Subject to admission | For a segment the replica has nothing for |
//! | --- | --- | --- |
//! | append | yes | starts an empty copy |
//! | repair | yes | starts an empty copy |
//! | fence, seal | no | starts an empty copy, which remembers the fence or the seal |
//! | read, upload | no | answers that the segment is not found |
//! | delete | no | succeeds: there is nothing to delete |
//!
//! A read that may wait does its waiting here, on the progress the segment's task publishes, capped by
//! the replica's own limit; only then is the read itself sent to the task. An upload runs outside the
//! segment's task altogether: a sealed copy never changes, so appends to other segments and reads of this
//! one are not held up while it goes on.
//!
//! # Invariants
//!
//! - A stopped replica answers every operation as unavailable.
//! - Nothing here touches a copy directly: every change goes through the segment's task.
//! - Append and read are the hot path. Append costs the admission check, one lookup of the segment's
//!   task and the hop to it; read costs the lookup and the hop. The request is moved along, never copied.
//!
//! # See also
//!
//! - `../replica.rs`: the replica and its registry of segment tasks.
//! - `admission.rs`: the admission check.
//! - `../actor.rs`: the tasks, and `../copy/`: what each operation does to a copy.
//! - `../upload.rs`: the upload of a sealed copy.
//! - `docs/protocol/replication.md`, "A replica": the rules each operation enforces.

use std::sync::atomic::Ordering;

use async_trait::async_trait;
use bitacora_types::SegmentAddress;
use bitacora_types::replica_api::{
    AppendRequest, AppendResponse, FenceRequest, FenceResponse, ReadLimit, ReadRequest, ReadResponse, RepairRequest,
    RepairResponse, ReplicaApi, ReplicaError, ReplicaStatus, SealRequest, SealResponse, SegmentStatus, UploadRequest,
    UploadResponse,
};

use super::{Replica, directory_failure};
use crate::copy::RepairPace;
use crate::upload::{UploadPace, upload_sealed_copy};

#[async_trait]
impl ReplicaApi for Replica {
    /// Checks admission, then hands the append to the segment's task, starting one for a segment the
    /// replica sees for the first time.
    async fn append(&self, request: AppendRequest) -> Result<AppendResponse, ReplicaError> {
        let admission = self.admit_write().await?;
        let handle = self.handle_or_spawn(request.segment.address)?;
        handle.append(request, admission).await
    }

    /// Waits, if the reader asked to and reads up to the durable sequence, until there is something to
    /// read or the segment is sealed, then hands the read to the segment's task.
    async fn read(&self, request: ReadRequest) -> Result<ReadResponse, ReplicaError> {
        self.fail_if_stopped()?;
        let Some(handle) = self.existing_handle(request.segment) else {
            return Err(ReplicaError::SegmentNotFound {
                segment: request.segment,
            });
        };
        if let Some(requested_wait) = request.wait_for_records
            && matches!(request.limit, ReadLimit::DurableSequence)
        {
            let longest_wait = requested_wait.min(self.settings.longest_read_wait);
            handle.wait_until_readable(request.from_sequence, longest_wait).await;
        }
        handle.read(request).await
    }

    /// Hands the fence to the segment's task, starting one if the replica never saw the segment: the
    /// promise must be kept all the same.
    async fn fence(&self, request: FenceRequest) -> Result<FenceResponse, ReplicaError> {
        let handle = self.handle_or_spawn(request.segment)?;
        handle.fence(request.epoch).await
    }

    /// Hands the seal to the segment's task, starting one if the replica never saw the segment, so that
    /// a late append is refused.
    async fn seal(&self, request: SealRequest) -> Result<SealResponse, ReplicaError> {
        let handle = self.handle_or_spawn(request.segment)?;
        handle.seal(request.sealed_end).await
    }

    /// Checks admission, connects to the source replica and hands the repair to the segment's task.
    async fn repair_segment(&self, request: RepairRequest) -> Result<RepairResponse, ReplicaError> {
        let admission = self.admit_write().await?;
        let source = self
            .connector
            .connect(&request.source_replica_id, &request.source_address);
        let pace = RepairPace {
            chunk_bytes: self.settings.repair_chunk_bytes,
            bytes_per_second: self.settings.repair_bytes_per_second,
            request_timeout: self.settings.repair_request_timeout,
        };
        let segment = request.segment.address;
        let handle = self.handle_or_spawn(segment)?;
        let repaired = handle.repair(request, source, admission, pace).await?;
        tracing::info!(%segment, local_end = %repaired.local_end, "segment copy repaired");
        Ok(repaired)
    }

    /// Uploads the replica's copy to its object store, outside the segment's task, and refuses if the
    /// replica was given no object store.
    async fn upload_segment(&self, request: UploadRequest) -> Result<UploadResponse, ReplicaError> {
        self.fail_if_stopped()?;
        let Some(artifact_store) = &self.artifact_store else {
            return Err(ReplicaError::InvalidRequest {
                reason: "this replica has no object store to upload to".to_owned(),
            });
        };
        let segment = request.segment.address;
        let Some(handle) = self.existing_handle(segment) else {
            return Err(ReplicaError::SegmentNotFound { segment });
        };
        let pace = UploadPace {
            chunk_bytes: self.settings.upload_chunk_bytes,
            object_store_timeout: self.settings.object_store_timeout,
        };
        let uploaded = upload_sealed_copy(&handle, artifact_store.as_ref(), &request, pace).await?;
        tracing::info!(
            %segment,
            object_key = %request.object_key,
            size_bytes = uploaded.size_bytes,
            "segment copy uploaded to the object store"
        );
        Ok(uploaded)
    }

    /// Has the segment's task remove the copy's files, then forgets the task. A segment the replica has
    /// nothing for is already deleted.
    async fn delete_segment(&self, segment: SegmentAddress) -> Result<(), ReplicaError> {
        self.fail_if_stopped()?;
        let Some(handle) = self.existing_handle(segment) else {
            return Ok(());
        };
        handle.delete().await?;
        self.lock_registry().handles_by_address.remove(&segment);
        tracing::info!(%segment, "segment copy deleted");
        Ok(())
    }

    /// Asks every segment's task for its status and lists the copies the replica has something stored
    /// for, in the order of their addresses.
    async fn list_segments(&self) -> Result<Vec<SegmentStatus>, ReplicaError> {
        self.fail_if_stopped()?;
        let mut stored_copies = Vec::new();
        for handle in self.all_handles() {
            if let Some(status) = handle.status().await? {
                stored_copies.push(status);
            }
        }
        Ok(stored_copies)
    }

    /// Measures the disk afresh and counts the stored copies.
    ///
    /// # Panics
    ///
    /// Never in practice: a count of copies fits in 64 bits.
    async fn status(&self) -> Result<ReplicaStatus, ReplicaError> {
        self.fail_if_stopped()?;
        let space = match self.disk.space().await {
            Ok(space) => space,
            Err(io_error) => return Err(directory_failure("measure", &io_error)),
        };
        let segment_count = self.list_segments().await?.len();
        Ok(ReplicaStatus {
            replica_id: self.replica_id.clone(),
            capacity_bytes: space.capacity_bytes,
            free_bytes: space.free_bytes,
            is_draining: self.is_draining.load(Ordering::Relaxed),
            segment_count: u64::try_from(segment_count).expect("counts fit in 64 bits"),
        })
    }

    /// Switches the replica to draining: from now on it refuses appends and repairs, so that writers
    /// roll away from it. Nothing switches it back but opening the replica again.
    async fn drain(&self) -> Result<(), ReplicaError> {
        self.fail_if_stopped()?;
        self.is_draining.store(true, Ordering::Relaxed);
        tracing::info!(replica_id = %self.replica_id, "replica is draining");
        Ok(())
    }
}

#[cfg(test)]
#[path = "api_test.rs"]
mod tests;
