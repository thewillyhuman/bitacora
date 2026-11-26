//! Appends and heartbeats: how records reach a copy.
//!
//! # Why this file exists
//!
//! An append is the one request a replica answers for every record of every stream, and its answer is a
//! promise: the records are on this disk and will survive a crash. A writer counts acknowledgements to
//! decide what is committed, so the promise must never be given early, and a writer that has been
//! replaced must never get it at all (design, section 5, "Persist", and section 9). This file is where a
//! copy decides whether to accept an append, stores what it accepts, and only then answers.
//!
//! # What it contains
//!
//! - `SegmentCopy::append`: handles an append or a heartbeat.
//! - `SegmentCopy::batches_after` and `SegmentCopy::store_batches`: the two steps a repair shares with
//!   an append, dropping what the copy already holds and writing the rest durably.
//! - [`Admission`]: what the replica's disk allows right now.
//!
//! # How it works
//!
//! 1. A copy that is not sound refuses.
//! 2. The writer's epoch must not be below the segment's own epoch, nor below the copy's fence.
//! 3. A sealed copy refuses.
//! 4. The segment the request describes must be the one the records file was created for.
//! 5. Every frame is verified against its checksum.
//! 6. Batches the copy already holds are dropped: a retried request is acknowledged again without being
//!    stored twice. What remains must start right after the copy's end; otherwise the copy says where it
//!    ends, so that the writer resends from there.
//! 7. The new batches are written and flushed. The records file is created on the first append, if the
//!    disk has room for a new segment.
//! 8. The copy remembers the writer's durable sequence, which is what readers may be served up to, and
//!    answers with its new end.
//!
//! An append without frames is a heartbeat. It stores nothing, but it is the moment a stream goes idle,
//! so the copy uses it to bring the flushed mark of its file up to date: one flush when the stream stops,
//! none after that.
//!
//! # Invariants
//!
//! - The answer is sent only after the flush returned. There is no asynchronous-durability mode.
//! - A request that is refused changes nothing: no file is created, no record stored, no durable sequence
//!   learned.
//! - The durable sequence a copy knows never moves backwards.
//! - This is the hot path of the replica. An append makes one pass over the frames to verify them, one
//!   write and one flush; the frames are not copied here (decoding shares the request's buffer), nothing
//!   is locked, and the only allocation is the list of decoded batches. `append` is kept as one function
//!   with its checks in protocol order, so that the path from request to flush can be read top to bottom.
//! - A write or a flush that the disk refuses marks the copy for good; the process must stop.
//!
//! # See also
//!
//! - `../copy.rs`: the copy, its state and its health.
//! - `repair.rs`: the other caller of `batches_after` and `store_batches`.
//! - `../replica/admission.rs`: where an [`Admission`] comes from.
//! - `docs/protocol/replication.md`, "A replica": the rules in protocol terms.

use bitacora_segment::SegmentFile;
use bitacora_segment::format::{Batch, SegmentHeader, decode_frames};
use bitacora_types::SequenceNumber;
use bitacora_types::replica_api::{AppendRequest, AppendResponse, ReplicaError, SegmentDescriptor};

use super::{SegmentCopy, invalid_request};
use crate::paths::segment_file_path;

/// What the replica's disk allows an append to do right now.
///
/// The replica measures its disk before every append and repair and hands the result to the copy, so that
/// the copy itself never asks the disk for its free space.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Admission {
    /// Whether a new records file may be created. Appends to a file that exists are not affected.
    pub may_create_segments: bool,
    /// How full the disk is, in percent, to report when creating a file is refused.
    pub used_percent: u8,
}

impl SegmentCopy {
    /// Handles an append or a heartbeat (design, section 5, "Persist").
    ///
    /// See the file header for the steps. Writes and flushes the records file when the request brings new
    /// batches, and flushes its mark when the request is a heartbeat and the mark is behind.
    ///
    /// # Arguments
    ///
    /// * `request` - The segment, the writer's epoch, the batch frames (none for a heartbeat) and the
    ///   writer's durable sequence.
    /// * `admission` - What the replica's disk allows right now.
    ///
    /// # Returns
    ///
    /// The copy's end after the request: the last sequence number it holds, or the position before the
    /// segment's first record if it holds nothing.
    ///
    /// # Errors
    ///
    /// * [`ReplicaError::Storage`] if the copy is damaged, or if the disk refuses the write or the flush.
    /// * [`ReplicaError::InvalidRequest`] if the writer's epoch is below the segment's, the request
    ///   describes another segment than the copy's file, a frame fails its checksum, or a resent batch is
    ///   cut differently from the stored ones.
    /// * [`ReplicaError::Fenced`] if a newer writer has fenced the copy.
    /// * [`ReplicaError::Sealed`] if the copy is sealed.
    /// * [`ReplicaError::OutOfOrder`] if the batches leave a gap after the copy's end.
    /// * [`ReplicaError::NoCapacity`] if the records file would have to be created and the disk is too
    ///   full for a new segment.
    pub(crate) async fn append(
        &mut self,
        request: &AppendRequest,
        admission: Admission,
    ) -> Result<AppendResponse, ReplicaError> {
        self.fail_if_unsound()?;
        self.fail_if_writer_is_not_accepted(request)?;
        let batches = match decode_frames(self.address, &request.batch_frames) {
            Ok(batches) => batches,
            Err(frames_error) => return Err(invalid_request(&frames_error.to_string())),
        };

        let end_of_an_empty_copy = request.segment.first_sequence.preceding();
        let local_end = self.local_end_or(end_of_an_empty_copy);
        let new_batches = Self::batches_after(&batches, local_end)?;
        match new_batches.first() {
            Some(first_new_batch) => {
                if first_new_batch.first_sequence() != local_end.next() {
                    return Err(ReplicaError::OutOfOrder {
                        first_sequence: first_new_batch.first_sequence(),
                        local_end,
                    });
                }
                self.store_batches(request.segment, new_batches, admission).await?;
            }
            None => self.flush_mark_on_heartbeat().await?,
        }

        self.known_durable = self.known_durable.max(request.durable_sequence);
        Ok(AppendResponse {
            local_end: self.local_end_or(local_end),
        })
    }

    /// Checks the rules that depend on who is writing and to what: the writer's epoch against the
    /// segment's and against the fence, the seal, and that the request describes the copy's own file.
    fn fail_if_writer_is_not_accepted(&self, request: &AppendRequest) -> Result<(), ReplicaError> {
        if request.writer_epoch < request.segment.epoch {
            return Err(invalid_request("writer epoch is older than the segment's own epoch"));
        }
        let fenced_epoch = self.fenced_epoch();
        if request.writer_epoch < fenced_epoch {
            return Err(ReplicaError::Fenced {
                request_epoch: request.writer_epoch,
                fenced_epoch,
            });
        }
        if let Some(sealed_end) = self.state.sealed_end {
            return Err(ReplicaError::Sealed { sealed_end });
        }
        if let Some(file) = &self.file {
            let header = file.header();
            if header.epoch != request.segment.epoch || header.first_sequence != request.segment.first_sequence {
                return Err(invalid_request("segment descriptor does not match the replica's copy"));
            }
        }
        Ok(())
    }

    /// Returns the batches that lie after `local_end`, dropping the ones the copy already holds.
    ///
    /// A retried request may resend batches that were already stored. Within one segment there is only one
    /// possible record at each sequence number (design, section 4, rule 1), so they can be acknowledged
    /// again without comparing content.
    ///
    /// # Arguments
    ///
    /// * `batches` - Consecutive batches, as decoded from a request or from a peer's answer.
    /// * `local_end` - The last sequence number the copy holds.
    ///
    /// # Returns
    ///
    /// The tail of `batches` that starts after `local_end`. Empty if the copy holds them all.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaError::InvalidRequest`] if a batch straddles `local_end`: it was cut differently
    /// from the batches the copy stored, which a correct writer never does.
    pub(super) fn batches_after(batches: &[Batch], local_end: SequenceNumber) -> Result<&[Batch], ReplicaError> {
        let mut already_stored = 0;
        for batch in batches {
            if batch.last_sequence() > local_end {
                break;
            }
            already_stored += 1;
        }
        let new_batches = &batches[already_stored..];
        if let Some(first_new_batch) = new_batches.first()
            && first_new_batch.first_sequence() <= local_end
        {
            return Err(invalid_request("batch boundaries do not match the replica's copy"));
        }
        Ok(new_batches)
    }

    /// Writes and flushes `new_batches`, creating the records file first if the copy has none.
    ///
    /// # Arguments
    ///
    /// * `segment` - The segment's epoch and first sequence number, for the header of a new file.
    /// * `new_batches` - Batches that start right after the copy's end.
    /// * `admission` - Whether a new file may be created.
    ///
    /// # Errors
    ///
    /// * [`ReplicaError::NoCapacity`] if the file would have to be created and `admission` forbids it.
    /// * [`ReplicaError::Storage`] if the file cannot be created, which damages the copy, or if the disk
    ///   refuses the write or the flush, after which the process must stop.
    ///
    /// # Panics
    ///
    /// Never in practice: the file it expects was created or found just before.
    pub(super) async fn store_batches(
        &mut self,
        segment: SegmentDescriptor,
        new_batches: &[Batch],
        admission: Admission,
    ) -> Result<(), ReplicaError> {
        if self.file.is_none() {
            self.create_records_file(segment, admission).await?;
        }
        let file = self.file.as_mut().expect("the records file was just ensured");
        let stored = match file.append(new_batches).await {
            Ok(()) => file.flush().await,
            Err(segment_error) => Err(segment_error),
        };
        match stored {
            Ok(()) => Ok(()),
            Err(segment_error) => Err(self.write_refused_by(&segment_error)),
        }
    }

    /// Creates the copy's records file with the header of `segment`, if the disk has room for a new
    /// segment. A file that cannot be created damages the copy.
    async fn create_records_file(
        &mut self,
        segment: SegmentDescriptor,
        admission: Admission,
    ) -> Result<(), ReplicaError> {
        if !admission.may_create_segments {
            return Err(ReplicaError::NoCapacity {
                used_percent: admission.used_percent,
            });
        }
        let header = SegmentHeader {
            address: self.address,
            epoch: segment.epoch,
            first_sequence: segment.first_sequence,
        };
        let path = segment_file_path(self.address);
        let created = SegmentFile::create(self.disk.as_ref(), &path, header, self.preallocated_bytes).await;
        match created {
            Ok(file) => {
                self.file = Some(file);
                Ok(())
            }
            Err(segment_error) => Err(self.damaged_by(&segment_error)),
        }
    }

    /// Brings the flushed mark of the records file up to date.
    ///
    /// A heartbeat means the stream is idle. That is the moment to make the mark cover everything
    /// acknowledged: it costs one flush when the stream goes idle and nothing after that, because a mark
    /// that is current is not flushed again.
    async fn flush_mark_on_heartbeat(&mut self) -> Result<(), ReplicaError> {
        let Some(file) = &mut self.file else {
            return Ok(());
        };
        match file.flush_mark().await {
            Ok(()) => Ok(()),
            Err(segment_error) => Err(self.write_refused_by(&segment_error)),
        }
    }
}

#[cfg(test)]
#[path = "append_test.rs"]
mod tests;
