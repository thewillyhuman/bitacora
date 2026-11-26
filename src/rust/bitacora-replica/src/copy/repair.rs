//! Repair: making a copy of a sealed segment complete from another replica.
//!
//! # Why this file exists
//!
//! A sealed segment is supposed to have a full set of good copies. Copies go missing: a replica was away
//! while the segment was written, a disk lost a file, a checksum failed. The controller notices and orders
//! the replica that should hold the copy to make it complete by pulling what it lacks from a replica that
//! holds a good one (design, section 10, "Repair"). This file is what a copy does when it gets that order.
//!
//! # What it contains
//!
//! - `SegmentCopy::repair`: makes the copy a complete, sealed copy of the segment.
//! - [`RepairPace`]: how large the reads from the source are, how fast the repair may copy, and how long
//!   the source may take.
//! - `fetch_batches_after`: one verified read from the source.
//!
//! # How it works
//!
//! There are two ways to a complete copy, and which one is taken depends on what the copy is:
//!
//! - A copy that is sound and only **short** is completed where it is: the missing batches are fetched
//!   and appended to its own file. A replica without any copy is the same case, starting from nothing.
//! - A copy that **cannot be completed** (it is damaged, sealed elsewhere, runs past the sealed end, or
//!   was written for another segment header) is replaced. A whole new copy is built in a scratch file
//!   beside it, and moved over it with one atomic rename only once it is complete. Until that moment the
//!   old copy is not touched. If anything fails before, the scratch file is removed and the old copy is
//!   exactly as it was.
//!
//! Every batch that arrives from the source is verified against its checksum before it is stored, and
//! must continue the copy and stay within the sealed end. After each read the repair pauses in proportion
//! to what it copied, so that it stays within its rate limit.
//!
//! When the records are complete, the seal is recorded, with the fence kept, and the unused space of the
//! file is given back.
//!
//! # Invariants
//!
//! - A repair never destroys records before their replacement is safely on the disk. A damaged copy may
//!   be damaged in one place and still be the only copy of every other record of the segment.
//! - A copy whose disk refused a write is not repaired at all. The process is about to stop, and what it
//!   sees of the file may be pages that never reached the disk. The copy is read back from the disk when
//!   the replica starts again, and judged then.
//! - A fence is never forgotten, whatever happens to the records.
//! - A write to the scratch file that the disk refuses stops the process like any other refused write,
//!   though the old copy was not written to.
//! - Repair is a background task and must not starve appends: it is paced, and it runs on the segment's
//!   own task like every other operation on the copy. It is cold code: the simplest form is chosen.
//!
//! # See also
//!
//! - `../copy.rs`: the copy and its health.
//! - `append.rs`: `batches_after` and `store_batches`, which completing a copy in place shares with an
//!   append.
//! - `../paths.rs`: where the scratch file is, and `../replica.rs`, which removes a leftover one at start.
//! - `docs/protocol/disk-format.md`: the scratch file in the layout of the data directory.

use std::fmt::Display;
use std::time::Duration;

use bitacora_segment::format::{Batch, SegmentHeader, decode_frames};
use bitacora_segment::{SegmentFile, SegmentState};
use bitacora_types::SequenceNumber;
use bitacora_types::replica_api::{ReadLimit, ReadRequest, RepairRequest, RepairResponse, ReplicaApi, ReplicaError};
use bytes::Bytes;

use super::{Admission, Health, SegmentCopy};
use crate::paths::{segment_file_path, segment_repair_path};

/// How many microseconds a second has, for turning a byte count and a rate into a pause.
const MICROSECONDS_PER_SECOND: u64 = 1_000_000;

/// How fast a repair may pull a segment from its source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RepairPace {
    /// The target size of one read from the source, in bytes.
    pub chunk_bytes: usize,
    /// How many bytes per second the repair may copy. Zero means no limit.
    pub bytes_per_second: u64,
    /// How long one read from the source may take before the repair gives up.
    pub request_timeout: Duration,
}

impl RepairPace {
    /// Computes how long to pause after copying `copied_bytes` to stay within the rate limit.
    fn pause_after(self, copied_bytes: usize) -> Duration {
        if self.bytes_per_second == 0 {
            return Duration::ZERO;
        }
        let copied_bytes = u64::try_from(copied_bytes).unwrap_or(u64::MAX);
        Duration::from_micros(copied_bytes.saturating_mul(MICROSECONDS_PER_SECOND) / self.bytes_per_second)
    }
}

/// Batches fetched from the source of a repair.
struct FetchedBatches {
    /// The batches that continue the copy, in order.
    batches: Vec<Batch>,
    /// How many bytes of frames the source sent, for pacing.
    frame_bytes: usize,
}

/// Reads from `source` the batches of the sealed segment that follow `local_end`, and checks that they
/// are what a repair can store: valid, starting right after `local_end`, and within the sealed end.
///
/// A source that does not answer in time, sends frames that fail their checksum, or has nothing usable
/// is reported as unavailable: the repair can be tried again, possibly from another source.
async fn fetch_batches_after(
    source: &dyn ReplicaApi,
    request: &RepairRequest,
    local_end: SequenceNumber,
    pace: RepairPace,
) -> Result<FetchedBatches, ReplicaError> {
    let batch_frames = read_frames_after(source, request, local_end, pace).await?;
    let decoded = match decode_frames(request.segment.address, &batch_frames) {
        Ok(decoded) => decoded,
        Err(invalid) => return Err(source_unusable(format!("the source sent invalid frames: {invalid}"))),
    };
    let batches = SegmentCopy::batches_after(&decoded, local_end)?.to_vec();
    if !continue_the_copy_within_the_seal(&batches, local_end, request.sealed_end) {
        return Err(source_unusable(format!(
            "the source has no usable records after {local_end} for a segment sealed at {}",
            request.sealed_end
        )));
    }
    Ok(FetchedBatches {
        batches,
        frame_bytes: batch_frames.len(),
    })
}

/// Asks `source` once for the frames of the sealed segment that follow `local_end`, as a reader would,
/// and waits for the answer no longer than the pace allows.
async fn read_frames_after(
    source: &dyn ReplicaApi,
    request: &RepairRequest,
    local_end: SequenceNumber,
    pace: RepairPace,
) -> Result<Bytes, ReplicaError> {
    let read_chunk = source.read(ReadRequest {
        segment: request.segment.address,
        from_sequence: local_end.next(),
        max_bytes: pace.chunk_bytes,
        wait_for_records: None,
        limit: ReadLimit::DurableSequence,
    });
    match tokio::time::timeout(pace.request_timeout, read_chunk).await {
        Ok(answer) => Ok(answer?.batch_frames),
        Err(_elapsed) => Err(source_unusable("the source did not answer in time".to_owned())),
    }
}

/// Tells whether `batches` are what a repair can store after `local_end`: there is at least one, the
/// first starts right after `local_end`, and the last ends at or before `sealed_end`.
fn continue_the_copy_within_the_seal(batches: &[Batch], local_end: SequenceNumber, sealed_end: SequenceNumber) -> bool {
    let (Some(first_batch), Some(last_batch)) = (batches.first(), batches.last()) else {
        return false;
    };
    first_batch.first_sequence() == local_end.next() && last_batch.last_sequence() <= sealed_end
}

/// Builds the answer for a repair whose source could not provide what was needed.
fn source_unusable(reason: String) -> ReplicaError {
    ReplicaError::Unavailable { reason }
}

impl SegmentCopy {
    /// Makes the copy a complete, sealed copy of the segment by pulling what it lacks from `source`
    /// (design, section 10, "Repair").
    ///
    /// See the file header for the two ways this is done. Reads from the source over the network, and
    /// writes and flushes the copy's files, pausing to stay within the pace.
    ///
    /// # Arguments
    ///
    /// * `request` - The sealed segment to hold a copy of, and its sealed end.
    /// * `source` - A replica that holds a good copy.
    /// * `admission` - Whether the disk has room for a new records file.
    /// * `pace` - Read sizes, the rate limit and the timeout of one read from the source.
    ///
    /// # Returns
    ///
    /// The last sequence number of the repaired copy: the sealed end, or the position before the
    /// segment's first record for a segment sealed empty.
    ///
    /// # Errors
    ///
    /// * [`ReplicaError::Storage`] if the disk refused a write to this copy before, or refuses one now.
    /// * [`ReplicaError::Unavailable`] if the source does not answer in time, sends invalid frames, or
    ///   does not hold the segment up to its sealed end; any other error of the source is passed on.
    /// * [`ReplicaError::NoCapacity`] if a new records file is needed and the disk is too full.
    pub(crate) async fn repair(
        &mut self,
        request: &RepairRequest,
        source: &dyn ReplicaApi,
        admission: Admission,
        pace: RepairPace,
    ) -> Result<RepairResponse, ReplicaError> {
        if self.refused_write().is_some() {
            return Err(ReplicaError::Storage {
                reason: "the disk refused a write to this copy; it is verified again when the replica restarts"
                    .to_owned(),
            });
        }
        if self.can_be_completed_to(request) {
            self.complete_in_place(request, source, admission, pace).await?;
        } else {
            self.replace_with_a_new_copy(request, source, admission, pace).await?;
        }
        self.store_state(SegmentState {
            fenced_epoch: self.state.fenced_epoch.max(request.segment.epoch),
            sealed_end: Some(request.sealed_end),
        })
        .await?;
        self.release_unused_space().await?;
        let end_of_an_empty_copy = request.segment.first_sequence.preceding();
        Ok(RepairResponse {
            local_end: self.local_end_or(end_of_an_empty_copy),
        })
    }

    /// Tells whether the copy, as it is, can be extended into a complete copy of the sealed segment
    /// `request` describes: it is sound, not sealed elsewhere, and its records file, if it has one, was
    /// created for this segment and does not run past the sealed end.
    fn can_be_completed_to(&self, request: &RepairRequest) -> bool {
        if self.health != Health::Sound {
            return false;
        }
        if let Some(sealed_end) = self.state.sealed_end
            && sealed_end != request.sealed_end
        {
            return false;
        }
        let Some(file) = &self.file else {
            return true;
        };
        let header = file.header();
        header.epoch == request.segment.epoch
            && header.first_sequence == request.segment.first_sequence
            && file.local_end() <= request.sealed_end
    }

    /// Pulls the records the copy lacks from `source` and appends them to the copy's own file, until it
    /// reaches the sealed end.
    async fn complete_in_place(
        &mut self,
        request: &RepairRequest,
        source: &dyn ReplicaApi,
        admission: Admission,
        pace: RepairPace,
    ) -> Result<(), ReplicaError> {
        let end_of_an_empty_copy = request.segment.first_sequence.preceding();
        while self.local_end_or(end_of_an_empty_copy) < request.sealed_end {
            let local_end = self.local_end_or(end_of_an_empty_copy);
            let fetched = fetch_batches_after(source, request, local_end, pace).await?;
            self.store_batches(request.segment, &fetched.batches, admission).await?;
            // Hold back so that repair cannot starve appends of the disk and the network.
            tokio::time::sleep(pace.pause_after(fetched.frame_bytes)).await;
        }
        Ok(())
    }

    /// Builds a complete copy of the sealed segment in a scratch file next to the copy, and moves it over
    /// the copy once it is complete.
    ///
    /// If anything fails before the move, the scratch file is removed and the old copy is exactly as it
    /// was.
    async fn replace_with_a_new_copy(
        &mut self,
        request: &RepairRequest,
        source: &dyn ReplicaApi,
        admission: Admission,
        pace: RepairPace,
    ) -> Result<(), ReplicaError> {
        if !admission.may_create_segments {
            return Err(ReplicaError::NoCapacity {
                used_percent: admission.used_percent,
            });
        }
        let scratch_path = segment_repair_path(self.address);
        let built = self.build_new_copy(&scratch_path, request, source, pace).await;
        if let Err(replica_error) = built {
            // Best effort: a leftover is removed when the replica starts.
            drop(self.disk.remove_file(&scratch_path).await);
            return Err(replica_error);
        }
        self.move_new_copy_into_place(&scratch_path).await
    }

    /// Fetches the whole sealed segment from `source` into a new records file at `scratch_path`, flushing
    /// as it goes.
    async fn build_new_copy(
        &mut self,
        scratch_path: &str,
        request: &RepairRequest,
        source: &dyn ReplicaApi,
        pace: RepairPace,
    ) -> Result<(), ReplicaError> {
        let mut new_copy = self.create_scratch_file(scratch_path, request).await?;
        while new_copy.local_end() < request.sealed_end {
            let fetched = fetch_batches_after(source, request, new_copy.local_end(), pace).await?;
            let stored = match new_copy.append(&fetched.batches).await {
                Ok(()) => new_copy.flush().await,
                Err(segment_error) => Err(segment_error),
            };
            if let Err(segment_error) = stored {
                return Err(self.write_of_the_new_copy_refused("write it", &segment_error));
            }
            tokio::time::sleep(pace.pause_after(fetched.frame_bytes)).await;
        }
        if let Err(segment_error) = new_copy.release_unused_space().await {
            return Err(self.write_of_the_new_copy_refused("trim it", &segment_error));
        }
        Ok(())
    }

    /// Creates an empty records file for the sealed segment at `scratch_path`, in place of whatever an
    /// interrupted repair may have left there.
    async fn create_scratch_file(
        &mut self,
        scratch_path: &str,
        request: &RepairRequest,
    ) -> Result<SegmentFile, ReplicaError> {
        if let Err(io_error) = self.disk.remove_file(scratch_path).await {
            return Err(self.write_of_the_new_copy_refused("clear its place", &io_error));
        }
        let header = SegmentHeader {
            address: self.address,
            epoch: request.segment.epoch,
            first_sequence: request.segment.first_sequence,
        };
        let created = SegmentFile::create(self.disk.as_ref(), scratch_path, header, self.preallocated_bytes).await;
        match created {
            Ok(new_copy) => Ok(new_copy),
            Err(segment_error) => Err(self.write_of_the_new_copy_refused("create it", &segment_error)),
        }
    }

    /// Moves the complete new copy at `scratch_path` over the copy's records file and adopts it.
    ///
    /// From the rename on the replica holds the new copy and nothing else. The copy becomes sound; what
    /// the old copy knew about a seal described the old copy and is dropped, to be recorded afresh by the
    /// caller. The fence is kept: a fence is never forgotten.
    async fn move_new_copy_into_place(&mut self, scratch_path: &str) -> Result<(), ReplicaError> {
        self.file = None;
        let final_path = segment_file_path(self.address);
        if let Err(io_error) = self.disk.rename_file(scratch_path, &final_path).await {
            return Err(self.write_of_the_new_copy_refused("move it into place", &io_error));
        }
        let opened = SegmentFile::open(self.disk.as_ref(), &final_path, self.address, self.preallocated_bytes).await;
        match opened {
            Ok((file, _why_the_scan_stopped)) => {
                self.file = Some(file);
                self.health = Health::Sound;
                self.state.sealed_end = None;
                Ok(())
            }
            Err(segment_error) => Err(self.damaged_by(&segment_error)),
        }
    }

    /// Records that the disk refused a write while a new copy was being built, and returns the error to
    /// answer with. The process must stop, as after any refused write; the old copy was not written to.
    ///
    /// What was already wrong with the old copy stays the reason the copy reports: it is still the reason
    /// the old copy cannot be used.
    fn write_of_the_new_copy_refused(&mut self, step: &str, cause: &dyn Display) -> ReplicaError {
        let reason = format!("cannot rebuild the copy: failed to {step}: {cause}");
        tracing::error!(segment = %self.address, %reason, "the disk refused a write");
        let kept_reason = match &self.health {
            Health::Sound => reason.clone(),
            Health::Damaged { reason: earlier_reason } | Health::WriteRefused { reason: earlier_reason } => {
                earlier_reason.clone()
            }
        };
        self.health = Health::WriteRefused { reason: kept_reason };
        ReplicaError::Storage { reason }
    }
}

#[cfg(test)]
#[path = "repair_test.rs"]
mod tests;
