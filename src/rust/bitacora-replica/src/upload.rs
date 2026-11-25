//! Uploading a sealed segment copy to the object store.
//!
//! # Why this file exists
//!
//! A sealed segment is moved to the object store some time after it is sealed (design, section 10,
//! "Tiering"). The bytes are on the replicas, so a replica does the upload: sending a gigabyte through the
//! controller would put the controller on the data path. This file is what a replica does when the
//! controller asks it to upload one of its copies.
//!
//! # What it contains
//!
//! - [`UploadPace`]: how large the reads are and how long the object store may take.
//! - [`upload_sealed_copy`]: reads the copy, builds the tiered object, stores it and verifies it.
//!
//! # How it works
//!
//! 1. The copy must be sealed exactly where the request says, and must not be damaged.
//! 2. Its batch frames are read through the segment's own task, in chunks, like any reader would. A sealed
//!    copy never changes, so appends and other reads are not held up while this goes on.
//! 3. [`encode_tiered_segment`] turns the frames into the object. It verifies every batch checksum and
//!    refuses frames that are not exactly the sealed segment, so a short or damaged copy stops here.
//! 4. The object is stored under the requested key.
//! 5. The object is read back in chunks and compared with what was built. Only then does the replica
//!    answer, and only then does the controller record where the segment lives.
//!
//! The whole object is held in memory while it is uploaded, because the artifact store takes an object in
//! one piece. The design lists a streaming upload as future work.
//!
//! # Invariants
//!
//! - The replica never reports success for an object it has not read back intact.
//! - Nothing here changes the local copy: uploading is a read.
//! - Uploading again replaces the object with the same bytes, so the operation is safe to repeat.
//!
//! # See also
//!
//! - `bitacora-segment/src/tiered.rs`: the layout of the object.
//! - `replica.rs`: where the request arrives and the object store is configured.

use std::time::Duration;

use bitacora_artifact::{ArtifactError, ArtifactStore};
use bitacora_segment::format::{SegmentHeader, decode_frames};
use bitacora_segment::tiered::encode_tiered_segment;
use bitacora_types::replica_api::{ReadLimit, ReadRequest, ReplicaError, UploadRequest, UploadResponse};
use bytes::{Bytes, BytesMut};
use tokio::time::timeout;

use crate::actor::SegmentHandle;

/// How an upload reads and how long it waits.
#[derive(Clone, Copy, Debug)]
pub(crate) struct UploadPace {
    /// The target size of one read of the local copy, and of one read of the stored object when it is
    /// verified. It bounds how much is fetched at once, not the size of the object.
    pub chunk_bytes: usize,
    /// How long one request to the object store may take before the upload gives up.
    pub object_store_timeout: Duration,
}

/// Uploads the sealed copy behind `handle` to the object store and verifies what was stored.
///
/// See the file header for the steps. The call does I/O on the local disk and on the object store, and
/// holds the whole object in memory until it returns.
///
/// # Arguments
///
/// * `handle` - The task that owns the local copy of the segment.
/// * `artifact_store` - The object store to upload to.
/// * `request` - Which segment, where it is sealed, and the key to store it under.
/// * `pace` - Read sizes and the object store timeout.
///
/// # Returns
///
/// The size of the stored object, once it was read back and found identical to what was built.
///
/// # Errors
///
/// * [`ReplicaError::SegmentNotFound`] if the replica holds no records of the segment.
/// * [`ReplicaError::InvalidRequest`] if the copy is not sealed at the requested end.
/// * [`ReplicaError::Storage`] if the copy is damaged or is not the whole sealed segment.
/// * [`ReplicaError::Unavailable`] if the object store fails, does not answer in time, or returns other
///   bytes than were stored. The upload can be repeated.
pub(crate) async fn upload_sealed_copy(
    handle: &SegmentHandle,
    artifact_store: &dyn ArtifactStore,
    request: &UploadRequest,
    pace: UploadPace,
) -> Result<UploadResponse, ReplicaError> {
    let address = request.segment.address;
    let Some(status) = handle.status().await? else {
        return Err(ReplicaError::SegmentNotFound { segment: address });
    };
    if status.sealed_end != Some(request.sealed_end) {
        return Err(ReplicaError::InvalidRequest {
            reason: format!(
                "copy is sealed at {:?}, not at the requested {}",
                status.sealed_end, request.sealed_end
            ),
        });
    }

    let frames = read_sealed_frames(handle, request, pace.chunk_bytes).await?;
    let segment_header = SegmentHeader {
        address,
        epoch: request.segment.epoch,
        first_sequence: request.segment.first_sequence,
    };
    let object = match encode_tiered_segment(segment_header, request.sealed_end, &frames) {
        Ok(object) => object,
        Err(tiered_error) => {
            return Err(ReplicaError::Storage {
                reason: format!("copy cannot be uploaded: {tiered_error}"),
            });
        }
    };
    // The frames were copied into the object; do not hold both while the object travels.
    drop(frames);

    store_object(artifact_store, &request.object_key, object.clone(), pace).await?;
    verify_stored_object(artifact_store, &request.object_key, &object, pace).await?;
    Ok(UploadResponse {
        size_bytes: object.len() as u64,
    })
}

/// Reads every batch frame of the sealed copy, from the segment's first sequence number to its sealed end.
///
/// Each chunk is decoded only to learn where it ends, so that the next read starts at the following batch.
async fn read_sealed_frames(
    handle: &SegmentHandle,
    request: &UploadRequest,
    chunk_bytes: usize,
) -> Result<Bytes, ReplicaError> {
    let address = request.segment.address;
    let mut frames = BytesMut::new();
    let mut next_sequence = request.segment.first_sequence;
    while next_sequence <= request.sealed_end {
        let chunk = handle
            .read(ReadRequest {
                segment: address,
                from_sequence: next_sequence,
                max_bytes: chunk_bytes,
                wait_for_records: None,
                limit: ReadLimit::DurableSequence,
            })
            .await?;
        let batches = match decode_frames(address, &chunk.batch_frames) {
            Ok(batches) => batches,
            Err(frames_error) => {
                return Err(ReplicaError::Storage {
                    reason: format!("copy cannot be uploaded: {frames_error}"),
                });
            }
        };
        let Some(last_batch) = batches.last() else {
            return Err(ReplicaError::Storage {
                reason: format!(
                    "copy cannot be uploaded: it ends before {next_sequence}, short of its sealed end {}",
                    request.sealed_end
                ),
            });
        };
        frames.extend_from_slice(&chunk.batch_frames);
        next_sequence = last_batch.last_sequence().next();
    }
    Ok(frames.freeze())
}

/// Stores the object under `object_key`, waiting no longer than the pace allows.
async fn store_object(
    artifact_store: &dyn ArtifactStore,
    object_key: &str,
    object: Bytes,
    pace: UploadPace,
) -> Result<(), ReplicaError> {
    let stored = timeout(pace.object_store_timeout, artifact_store.put_object(object_key, object)).await;
    match stored {
        Ok(Ok(())) => Ok(()),
        Ok(Err(artifact_error)) => Err(object_store_failed("store", object_key, &artifact_error)),
        Err(_elapsed) => Err(object_store_timed_out("store", object_key)),
    }
}

/// Reads the stored object back, chunk by chunk, and compares it with the object that was built.
///
/// Reading in chunks keeps a second copy of the object out of memory.
async fn verify_stored_object(
    artifact_store: &dyn ArtifactStore,
    object_key: &str,
    object: &Bytes,
    pace: UploadPace,
) -> Result<(), ReplicaError> {
    // A chunk size of zero would never advance.
    let chunk_bytes = pace.chunk_bytes.max(1);
    let mut verified_bytes = 0;
    while verified_bytes < object.len() {
        let chunk_end = object.len().min(verified_bytes + chunk_bytes);
        let fetched = timeout(
            pace.object_store_timeout,
            artifact_store.get_range(object_key, verified_bytes as u64..chunk_end as u64),
        )
        .await;
        let stored_chunk = match fetched {
            Ok(Ok(stored_chunk)) => stored_chunk,
            Ok(Err(artifact_error)) => {
                return Err(object_store_failed("verify", object_key, &artifact_error));
            }
            Err(_elapsed) => return Err(object_store_timed_out("verify", object_key)),
        };
        if stored_chunk != object.slice(verified_bytes..chunk_end) {
            return Err(ReplicaError::Unavailable {
                reason: format!(
                    "object store holds other bytes than were uploaded for {object_key:?}, \
                     between byte {verified_bytes} and byte {chunk_end}"
                ),
            });
        }
        verified_bytes = chunk_end;
    }
    Ok(())
}

/// The error for an object store request that failed. The upload did not complete and can be repeated,
/// which is what `Unavailable` tells the controller.
fn object_store_failed(operation: &str, object_key: &str, artifact_error: &ArtifactError) -> ReplicaError {
    ReplicaError::Unavailable {
        reason: format!("cannot {operation} object {object_key:?}: {artifact_error}"),
    }
}

/// The error for an object store request that did not answer in time.
fn object_store_timed_out(operation: &str, object_key: &str) -> ReplicaError {
    ReplicaError::Unavailable {
        reason: format!("cannot {operation} object {object_key:?}: the object store did not answer in time"),
    }
}

#[cfg(test)]
#[path = "upload_test.rs"]
mod tests;
