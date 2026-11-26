//! Fencing and sealing: how a copy is closed to its writer.
//!
//! # Why this file exists
//!
//! A stream has one writer at a time, and a new writer takes over without asking the old one. What makes
//! that safe is two promises of a replica (design, section 9; `docs/protocol/replication.md`, "A
//! replica"). A **fence** is the promise to refuse every writer below an epoch from now on: once enough
//! replicas have given it, the old writer can no longer commit. A **seal** records where the segment ends
//! for good, and throws away whatever the copy holds beyond that, which was never committed. Both must
//! survive a crash, or a restarted replica would take back what a recovery relied on. This file is where a
//! copy gives the two promises.
//!
//! # What it contains
//!
//! - `SegmentCopy::fence`: raises the fenced epoch and reports the copy's end.
//! - `SegmentCopy::seal`: records the sealed end and cuts the copy to it.
//!
//! # How it works
//!
//! **Fence.** An epoch below the current fence is refused. Otherwise the new epoch is written to the state
//! file, durably, before the answer is sent. The answer carries the copy's end, which the recovering
//! writer uses to find the longest copy. A replica that never received a record for the segment gives the
//! promise all the same, and says that it has no copy.
//!
//! **Seal.** A copy that already knows the same sealed end answers again; one that knows another refuses,
//! which can only happen if the metadata store was bypassed. Otherwise the records above the sealed end
//! are cut off first and the seal is recorded second: a crash in between leaves an open copy with a
//! shorter tail, which the next seal completes. Last, the preallocated space after the records is given
//! back, since a sealed copy never grows.
//!
//! # Invariants
//!
//! - The fenced epoch never goes down.
//! - A fence or a seal is answered only once it is on the disk.
//! - A seal never extends a copy. A copy that ends before the sealed end keeps what it has and reports
//!   its end; the controller has it completed by a repair.
//! - The sealed end, once recorded, never changes.
//! - Both operations are rare: a segment is fenced at most once per writer change and sealed once. They
//!   choose the simplest form over the fastest.
//!
//! # See also
//!
//! - `../copy.rs`: the copy, and `store_state`, which writes the state file.
//! - `append.rs`: where a fence and a seal are enforced against a writer.
//! - `read.rs`: what a recovering writer may read once it has fenced the copy.

use bitacora_segment::{SegmentError, SegmentState};
use bitacora_types::replica_api::{FenceResponse, ReplicaError, SealResponse};
use bitacora_types::{Epoch, SequenceNumber};

use super::{SegmentCopy, invalid_request};

impl SegmentCopy {
    /// Raises the fenced epoch and reports the end of the copy (design, section 9, step 3).
    ///
    /// The new epoch is on the disk before this returns. Repeating a fence at the same epoch writes
    /// nothing and answers again.
    ///
    /// # Arguments
    ///
    /// * `epoch` - The epoch of the writer that is taking over. Writers below it are refused from now on.
    ///
    /// # Returns
    ///
    /// The last sequence number the copy holds, or `None` if the replica has no records of the segment,
    /// and the sealed end if the copy knows one.
    ///
    /// # Errors
    ///
    /// * [`ReplicaError::Storage`] if the copy is damaged, or if the disk refuses to store the state.
    /// * [`ReplicaError::Fenced`] if the copy is already fenced at a higher epoch.
    pub(crate) async fn fence(&mut self, epoch: Epoch) -> Result<FenceResponse, ReplicaError> {
        self.fail_if_unsound()?;
        let fenced_epoch = self.fenced_epoch();
        if epoch < fenced_epoch {
            return Err(ReplicaError::Fenced {
                request_epoch: epoch,
                fenced_epoch,
            });
        }
        // A copy whose fence comes from its file's epoch alone has no state file yet: the first fence
        // writes one even at that same epoch.
        if epoch > self.state.fenced_epoch || !self.is_state_stored {
            self.store_state(SegmentState {
                fenced_epoch: epoch,
                sealed_end: self.state.sealed_end,
            })
            .await?;
        }
        Ok(FenceResponse {
            local_end: self.local_end(),
            sealed_end: self.state.sealed_end,
        })
    }

    /// Records the segment's final sequence number and discards every record above it.
    ///
    /// The seal is on the disk before this returns. Repeating a seal at the same end changes nothing and
    /// answers again.
    ///
    /// # Arguments
    ///
    /// * `sealed_end` - The segment's final sequence number, as recorded in the metadata store.
    ///
    /// # Returns
    ///
    /// The last sequence number the copy holds after the seal, or `None` if the replica has no records of
    /// the segment. It is below `sealed_end` if the copy is short.
    ///
    /// # Errors
    ///
    /// * [`ReplicaError::Storage`] if the copy is damaged, or if the disk refuses to cut the file, to
    ///   store the state, or to give back the unused space.
    /// * [`ReplicaError::ConflictingSeal`] if the copy already knows a different sealed end.
    /// * [`ReplicaError::InvalidRequest`] if `sealed_end` falls inside a batch of the copy. Nothing is
    ///   changed then.
    pub(crate) async fn seal(&mut self, sealed_end: SequenceNumber) -> Result<SealResponse, ReplicaError> {
        self.fail_if_unsound()?;
        if let Some(existing_sealed_end) = self.state.sealed_end {
            if existing_sealed_end != sealed_end {
                return Err(ReplicaError::ConflictingSeal {
                    existing_sealed_end,
                    requested_sealed_end: sealed_end,
                });
            }
            return Ok(SealResponse {
                local_end: self.local_end(),
            });
        }

        // Cut the tail before recording the seal. A crash in between leaves an open copy with a shorter
        // tail, which the next seal completes.
        self.discard_records_above(sealed_end).await?;
        self.store_state(SegmentState {
            fenced_epoch: self.state.fenced_epoch,
            sealed_end: Some(sealed_end),
        })
        .await?;
        self.release_unused_space().await?;
        Ok(SealResponse {
            local_end: self.local_end(),
        })
    }

    /// Cuts the records file so that it ends at `sealed_end`, if it runs past it. Records above a sealed
    /// end were never committed.
    async fn discard_records_above(&mut self, sealed_end: SequenceNumber) -> Result<(), ReplicaError> {
        let Some(file) = &mut self.file else {
            return Ok(());
        };
        if file.local_end() <= sealed_end {
            return Ok(());
        }
        match file.truncate_to(sealed_end).await {
            Ok(()) => Ok(()),
            Err(SegmentError::NotABatchBoundary { .. }) => Err(invalid_request(
                "sealed end is not a batch boundary of the replica's copy",
            )),
            Err(segment_error) => Err(self.damaged_by(&segment_error)),
        }
    }

    /// Gives back the preallocated space after the last record of the records file. A sealed copy never
    /// grows, so the reserve is of no more use.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaError::Storage`] if the disk refuses, which damages the copy.
    pub(super) async fn release_unused_space(&mut self) -> Result<(), ReplicaError> {
        let Some(file) = &mut self.file else {
            return Ok(());
        };
        match file.release_unused_space().await {
            Ok(()) => Ok(()),
            Err(segment_error) => Err(self.damaged_by(&segment_error)),
        }
    }
}

#[cfg(test)]
#[path = "fence_seal_test.rs"]
mod tests;
