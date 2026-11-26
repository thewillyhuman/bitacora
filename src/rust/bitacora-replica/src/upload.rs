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
//! - [`upload_sealed_copy`]: reads the copy, sends it to the object store as the tiered object, and
//!   verifies what was stored.
//!
//! # How it works
//!
//! A segment is a gigabyte and a replica has a few to spare, so the object is never built: it goes to the
//! object store as it is read, and it is verified the same way.
//!
//! 1. The copy must be sealed exactly where the request says. Nothing is asked of the object store
//!    before that.
//! 2. An upload is started in the object store. Until it is completed, nothing is under the key.
//! 3. The batch frames of the copy are read through the segment's own task, a chunk at a time, like any
//!    reader would. A sealed copy never changes, so appends and other reads are not held up meanwhile.
//!    Each chunk is shown to a [`TieredSegmentEncoder`], which verifies every batch checksum and that the
//!    chunk continues the one before, and is then handed to the upload as it is: the frames of the object
//!    are the frames of the copy.
//! 4. When the copy has been read to its sealed end, the encoder gives out the end of the object, the
//!    block index and the footer, and refuses if the frames were not exactly the sealed segment. That
//!    goes to the upload as its last piece, and the upload is completed: the object appears.
//! 5. A copy that turns out short or damaged, or an object store that fails, aborts the upload instead,
//!    so that the object store drops what it was sent.
//! 6. The stored object is verified against the local copy, which is read a second time: each chunk is
//!    compared with the same bytes of the object, read back from the object store, and the end of the
//!    object with what a second encoder makes of the second reading. Only then does the replica answer,
//!    and only then does the controller record where the segment lives.
//!
//! Why a second reading and not a checksum. To verify the object without holding it, the replica needs
//! something to compare what it reads back with. A checksum taken while sending would do, but a checksum
//! of a gigabyte in 32 bits is weaker than a comparison, and a stronger one is a new dependency. The local
//! copy is still there, it does not change, and reading it is what a replica does all day: comparing with
//! it is exact, and costs one more sequential read of a segment that is uploaded once in its life. The
//! second reading also verifies every batch checksum again, so what the object is compared with is itself
//! a whole, undamaged segment; and the end of the object is derived from that reading alone, so the proof
//! does not lean on anything the first pass kept. What is established is that the object is, byte for
//! byte, what the local copy encodes to.
//!
//! # Memory
//!
//! What an upload holds does not grow with the segment, with one small exception:
//!
//! - One chunk of the local copy. A read stops after the first batch that takes it to the chunk size, so
//!   a chunk is less than [`UploadPace::chunk_bytes`] and one batch: 8 MiB by default, and less than
//!   16 MiB when batches are as large as a batch can be.
//! - While sending, what the upload of the object store holds: one part of 8 MiB for S3, nothing for a
//!   local directory.
//! - While verifying, the bytes of the object that the chunk is compared with: a second chunk.
//! - The block index: 16 bytes for every 64 KiB of the segment, in the encoder and once more as the end
//!   of the object. That is the exception, and it is 256 KiB for a segment of 1 GiB.
//!
//! So an upload of a segment of any size takes about 16 MiB with the default settings, and less than
//! 32 MiB with the largest batches. A test holds the replica to it: it uploads a segment many chunks long
//! through a store that writes down the largest piece it was handed and the largest range it was asked
//! for.
//!
//! # Invariants
//!
//! - The replica never reports success for an object it has not read back intact.
//! - Nothing of the object is under its key before the whole of it was sent, and nothing at all if the
//!   copy was not the whole sealed segment.
//! - The replica never holds the object, or the frames of the copy, in one piece.
//! - An upload that was started is completed or aborted, whatever goes wrong, unless the request itself
//!   is given up on; then the upload is dropped, and the object store removes what it took.
//! - Nothing here changes the local copy: uploading is two reads of it.
//! - Uploading again replaces the object with the same bytes, so the operation is safe to repeat.
//! - An upload happens once in the life of a segment. The code is written to be followed step by step,
//!   not to be fast; what it must not do is hold up the segment's task, and it does not.
//!
//! # See also
//!
//! - `bitacora-segment/src/tiered.rs`: the layout of the object, and the encoder.
//! - `bitacora-artifact/src/store.rs`: `ObjectUpload`, and what each object store does with one.
//! - `replica/api.rs`: where the request arrives, and `replica.rs`: where the object store is configured.
//! - `actor.rs`: the handle the copy is read through.

use std::time::Duration;

use bitacora_artifact::{ArtifactError, ArtifactStore, ObjectUpload};
use bitacora_segment::format::SegmentHeader;
use bitacora_segment::tiered::TieredSegmentEncoder;
use bitacora_types::replica_api::{ReadLimit, ReadRequest, ReplicaError, SegmentStatus, UploadRequest, UploadResponse};
use bytes::Bytes;
use tokio::time::timeout;

use crate::actor::SegmentHandle;

/// How an upload reads and how long it waits.
#[derive(Clone, Copy, Debug)]
pub(crate) struct UploadPace {
    /// The target size of one read of the local copy, which is also the size of the pieces the object
    /// store is handed and of the ranges that are read back from it. It bounds what an upload holds in
    /// memory, whatever the size of the segment: see the file header.
    pub chunk_bytes: usize,
    /// How long one request to the object store may take before the upload gives up.
    pub object_store_timeout: Duration,
}

/// Uploads the sealed copy behind `handle` to the object store and verifies what was stored.
///
/// See the file header for the steps. The call does I/O on the local disk and on the object store. It
/// holds a chunk or two of the segment at a time, never the segment.
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
/// The size of the stored object, once it was read back and found to be what the local copy encodes to.
///
/// # Errors
///
/// * [`ReplicaError::SegmentNotFound`] if the replica holds no records of the segment.
/// * [`ReplicaError::InvalidRequest`] if the copy is not sealed at the requested end.
/// * [`ReplicaError::Storage`] if the copy is damaged or is not the whole sealed segment. Nothing is
///   stored under the key then, unless the damage appeared after the object was stored.
/// * [`ReplicaError::Unavailable`] if the object store fails, does not answer in time, or returns other
///   bytes than were stored. The upload can be repeated.
pub(crate) async fn upload_sealed_copy(
    handle: &SegmentHandle,
    artifact_store: &dyn ArtifactStore,
    request: &UploadRequest,
    pace: UploadPace,
) -> Result<UploadResponse, ReplicaError> {
    let Some(status) = handle.status().await? else {
        return Err(ReplicaError::SegmentNotFound {
            segment: request.segment.address,
        });
    };
    fail_if_not_sealed_as_requested(&status, request)?;

    let starting = artifact_store.start_upload(&request.object_key);
    let mut upload = within_the_timeout("start the upload of", &request.object_key, pace, starting).await?;
    if let Err(replica_error) = send_sealed_copy(handle, upload.as_mut(), request, pace).await {
        abort_upload(upload, &request.object_key, pace).await;
        return Err(replica_error);
    }
    // A completion that fails ends the upload as well: the object store removes what it took.
    within_the_timeout("complete the upload of", &request.object_key, pace, upload.complete()).await?;

    let size_bytes = verify_stored_object(handle, artifact_store, request, pace).await?;
    Ok(UploadResponse { size_bytes })
}

/// Refuses the upload unless the copy that reported `status` is sealed exactly where `request` says.
fn fail_if_not_sealed_as_requested(status: &SegmentStatus, request: &UploadRequest) -> Result<(), ReplicaError> {
    if status.sealed_end == Some(request.sealed_end) {
        return Ok(());
    }
    Err(ReplicaError::InvalidRequest {
        reason: format!(
            "copy is sealed at {:?}, not at the requested {}",
            status.sealed_end, request.sealed_end
        ),
    })
}

/// Reads the sealed copy from its first record to its sealed end and hands `upload` the tiered object of
/// it, piece by piece: each chunk of frames as it was read, and then the block index and the footer.
///
/// The upload is neither completed nor aborted here. A chunk is dropped before the next one is read.
///
/// # Errors
///
/// * [`ReplicaError::Storage`] if the copy is damaged or is not the whole sealed segment.
/// * [`ReplicaError::Unavailable`] if the object store fails or does not answer in time.
async fn send_sealed_copy(
    handle: &SegmentHandle,
    upload: &mut dyn ObjectUpload,
    request: &UploadRequest,
    pace: UploadPace,
) -> Result<(), ReplicaError> {
    let mut encoder = encoder_of(request);
    while encoder.next_sequence() <= request.sealed_end {
        let chunk = read_next_chunk(handle, request, &mut encoder, pace.chunk_bytes).await?;
        within_the_timeout("store", &request.object_key, pace, upload.write(chunk)).await?;
    }
    let index_and_footer = end_of_object(encoder)?;
    within_the_timeout("store", &request.object_key, pace, upload.write(index_and_footer)).await
}

/// Reads the sealed copy a second time and compares the stored object with it: every chunk of frames
/// with the same bytes of the object, and the end of the object with the block index and the footer that
/// this reading gives.
///
/// A chunk and the bytes it is compared with are dropped before the next chunk is read, so that a second
/// copy of the object is never in memory, and not a first one either.
///
/// # Returns
///
/// The size of the stored object.
///
/// # Errors
///
/// * [`ReplicaError::Storage`] if the copy is damaged or is no longer the whole sealed segment.
/// * [`ReplicaError::Unavailable`] if the object store fails, does not answer in time, or holds other
///   bytes than the copy encodes to.
async fn verify_stored_object(
    handle: &SegmentHandle,
    artifact_store: &dyn ArtifactStore,
    request: &UploadRequest,
    pace: UploadPace,
) -> Result<u64, ReplicaError> {
    let mut encoder = encoder_of(request);
    let mut verified_bytes = 0;
    while encoder.next_sequence() <= request.sealed_end {
        let chunk = read_next_chunk(handle, request, &mut encoder, pace.chunk_bytes).await?;
        verified_bytes = fail_if_stored_otherwise(artifact_store, request, verified_bytes, &chunk, pace).await?;
    }
    let index_and_footer = end_of_object(encoder)?;
    fail_if_stored_otherwise(artifact_store, request, verified_bytes, &index_and_footer, pace).await
}

/// An encoder for the tiered object of the segment `request` names, before any of its frames.
fn encoder_of(request: &UploadRequest) -> TieredSegmentEncoder {
    let segment_header = SegmentHeader {
        address: request.segment.address,
        epoch: request.segment.epoch,
        first_sequence: request.segment.first_sequence,
    };
    TieredSegmentEncoder::new(segment_header, request.sealed_end)
}

/// Reads the next chunk of the sealed copy, from where the frames `encoder` has seen end, and shows it to
/// `encoder`, which verifies every batch in it.
///
/// # Returns
///
/// The frames of the chunk: whole batches, at least one.
///
/// # Errors
///
/// Fails with [`ReplicaError::Storage`] if the chunk is damaged or does not continue the frames before
/// it, or if it holds nothing, which means the copy ends short of its sealed end. A read that fails is
/// reported as the segment's task reported it.
async fn read_next_chunk(
    handle: &SegmentHandle,
    request: &UploadRequest,
    encoder: &mut TieredSegmentEncoder,
    chunk_bytes: usize,
) -> Result<Bytes, ReplicaError> {
    let from_sequence = encoder.next_sequence();
    let chunk = handle
        .read(ReadRequest {
            segment: request.segment.address,
            from_sequence,
            max_bytes: chunk_bytes,
            wait_for_records: None,
            limit: ReadLimit::DurableSequence,
        })
        .await?;
    if let Err(tiered_error) = encoder.add_frames(&chunk.batch_frames) {
        return Err(ReplicaError::Storage {
            reason: format!("copy cannot be uploaded: {tiered_error}"),
        });
    }
    if encoder.next_sequence() == from_sequence {
        return Err(ReplicaError::Storage {
            reason: format!(
                "copy cannot be uploaded: it ends before {from_sequence}, short of its sealed end {}",
                request.sealed_end
            ),
        });
    }
    Ok(chunk.batch_frames)
}

/// Ends the object of a copy that was read to its sealed end: the block index and the footer. Frames that
/// were not exactly the sealed segment are refused, which is the last thing that stops a wrong copy.
fn end_of_object(encoder: TieredSegmentEncoder) -> Result<Bytes, ReplicaError> {
    match encoder.finish() {
        Ok(end_of_object) => Ok(end_of_object),
        Err(tiered_error) => Err(ReplicaError::Storage {
            reason: format!("copy cannot be uploaded: {tiered_error}"),
        }),
    }
}

/// Reads back the bytes of the stored object that start at `offset_bytes` and must be `expected`, and
/// compares them.
///
/// # Returns
///
/// The offset in the object of the byte after `expected`.
///
/// # Errors
///
/// Fails with [`ReplicaError::Unavailable`] if the object store fails, does not answer in time, or holds
/// other bytes there, which includes an object that is too short.
async fn fail_if_stored_otherwise(
    artifact_store: &dyn ArtifactStore,
    request: &UploadRequest,
    offset_bytes: u64,
    expected: &Bytes,
    pace: UploadPace,
) -> Result<u64, ReplicaError> {
    let object_key = &request.object_key;
    let end_bytes = offset_bytes + expected.len() as u64;
    let reading = artifact_store.get_range(object_key, offset_bytes..end_bytes);
    let stored = within_the_timeout("verify", object_key, pace, reading).await?;
    if stored != *expected {
        return Err(ReplicaError::Unavailable {
            reason: format!(
                "object store holds other bytes than were uploaded for {object_key:?}, \
                 between byte {offset_bytes} and byte {end_bytes}"
            ),
        });
    }
    Ok(end_bytes)
}

/// Gives up an upload that cannot be completed, so that the object store drops what it was sent.
///
/// Nothing is returned: the upload has already failed for a reason its caller must hear, and nothing is
/// under the key whether the abort works or not. What an abort that fails leaves behind is the object
/// store's to clean up, and is logged, because for a bucket that means its lifecycle rule.
async fn abort_upload(upload: Box<dyn ObjectUpload>, object_key: &str, pace: UploadPace) {
    let aborted = within_the_timeout("abort the upload of", object_key, pace, upload.abort()).await;
    if let Err(replica_error) = aborted {
        tracing::warn!(
            object_key,
            reason = %replica_error,
            "upload could not be aborted; what it sent is left to the object store to remove"
        );
    }
}

/// Waits for one request to the object store, no longer than the pace allows.
///
/// # Arguments
///
/// * `operation` - What the request does, for the error message: it follows "cannot".
/// * `object_key` - The object the request is about.
/// * `pace` - Holds the timeout.
/// * `object_store_request` - The request.
///
/// # Errors
///
/// Fails with [`ReplicaError::Unavailable`] if the object store fails or does not answer in time. The
/// upload did not complete and can be repeated, which is what `Unavailable` tells the controller.
async fn within_the_timeout<Answer>(
    operation: &str,
    object_key: &str,
    pace: UploadPace,
    object_store_request: impl Future<Output = Result<Answer, ArtifactError>>,
) -> Result<Answer, ReplicaError> {
    match timeout(pace.object_store_timeout, object_store_request).await {
        Ok(Ok(answer)) => Ok(answer),
        Ok(Err(artifact_error)) => Err(ReplicaError::Unavailable {
            reason: format!("cannot {operation} object {object_key:?}: {artifact_error}"),
        }),
        Err(_elapsed) => Err(ReplicaError::Unavailable {
            reason: format!("cannot {operation} object {object_key:?}: the object store did not answer in time"),
        }),
    }
}

#[cfg(test)]
#[path = "upload_test.rs"]
mod tests;
