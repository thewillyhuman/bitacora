//! Truncation: advancing a stream's low-water mark and deleting the segments below it.
//!
//! # Why this file exists
//!
//! A log that only grows fills every disk. But a record may be deleted only when nothing can need it any more, and
//! in bitacora that is decided by the stream's materializers: each one says, with its checkpoint, "everything up to
//! here is in my artifacts", and the log below the lowest of those checkpoints is no longer the only copy of
//! anything (design, section 10, "Truncation and retention"). This file is the step that turns the checkpoints, the
//! owner's explicit request and the retention policy into the **low-water mark**, the first sequence number that can
//! still be read, and removes the segments that lie entirely below it. It is where invariant I5 is kept: no record
//! above the lowest registered checkpoint is ever deleted.
//!
//! # What it contains
//!
//! - `Controller::truncate`: the step, for one stream.
//! - `allowed_low_water_mark`: how far the materializers and the owner allow the mark to go.
//! - `hold_back_for_retention`: where the open segment and the retention period stop it.
//! - [`is_below_the_mark`]: whether a sealed segment lies entirely below a mark.
//!
//! # How it works
//!
//! The mark may go as far as the lowest of:
//!
//! - one past the checkpoint of every registered materializer (a materializer that has no checkpoint yet pins the
//!   log at its first record);
//! - the position the stream's owner asked to truncate before, if any.
//!
//! If there is neither a materializer nor a request, nothing may be deleted, and the step does nothing.
//!
//! The mark is then held back to the start of the first segment that must stay whole: the open segment, or a sealed
//! one that was sealed more recently than the retention period. It never moves backwards.
//!
//! Deletion is by whole segments. A sealed segment whose last record is below the mark is removed from the stream's
//! record, in the same conditional write that advances the mark. From that moment readers are told "truncated"; the
//! copies on the replicas are removed by the leftover sweep. A **tiered** segment below the mark is not removed here:
//! it stays listed until its object has been deleted (`tiering.rs`), so that a controller that stops in between
//! still knows the object exists.
//!
//! The write is conditional on the set of registered materializers being the one the checkpoints were read for. A
//! materializer registered in between starts below the mark this pass would set, so the pass decides nothing and the
//! next one decides with that materializer's checkpoint in hand.
//!
//! # Invariants
//!
//! - I5: the mark never passes the lowest checkpoint of the registered materializers.
//! - The mark never decreases, and never enters the open segment.
//! - A segment is deleted only as a whole, only when sealed, and only when all of it is below the mark.
//! - The rules are pure functions of the stream's record, the checkpoints and the time. They are tested as such.
//!
//! # See also
//!
//! - `tiering.rs`: deletes tiered segments below the mark, object first.
//! - `leftovers.rs`: removes the copies of deleted segments from the replicas.
//! - `bitacora-meta/src/store/checkpoints.rs`: where checkpoints are committed.

use std::time::Duration;

use bitacora_meta::{CheckpointRecord, MetaError, SegmentRecord, StreamRecord};
use bitacora_types::{SequenceNumber, Timestamp};

use super::{Controller, PassReport};

impl Controller {
    /// Advances the stream's low-water mark as far as its materializers, its owner and its retention policy allow,
    /// and deletes the untiered segments that lie entirely below it.
    ///
    /// # Arguments
    ///
    /// * `stream_name` - The stream. Its record and its checkpoints are read here.
    /// * `now` - The time of the pass, for the retention period.
    /// * `report` - Counts the segments deleted.
    ///
    /// # Errors
    ///
    /// Fails if the metadata store cannot be read or written.
    pub(super) async fn truncate(
        &self,
        stream_name: &str,
        now: Timestamp,
        report: &mut PassReport,
    ) -> Result<(), MetaError> {
        let stream = self.metadata.get_stream(stream_name).await?;
        let checkpoints = self.metadata.list_checkpoints(stream.stream_id).await?;
        let Some(allowed_mark) = allowed_low_water_mark(&stream, &checkpoints) else {
            return Ok(());
        };
        let (_updated, deleted_segments) = self
            .metadata
            .update_stream(stream_name, |record| {
                // A materializer registered since the checkpoints were read starts below the mark this pass would
                // set. Decide again on the next pass, with its checkpoint in hand.
                if record.materializers != stream.materializers {
                    return Ok(Vec::new());
                }
                let mark = hold_back_for_retention(record, allowed_mark, now);
                record.low_water_mark = record.low_water_mark.max(mark);
                Ok(take_segments_to_delete(record))
            })
            .await?;

        // The metadata store no longer lists the segments, so readers are told `Truncated`. Their copies are removed
        // by the leftover sweep at the end of this pass, or of a later one for a replica that is away now.
        for segment in &deleted_segments {
            tracing::info!(stream = stream_name, segment = %segment.segment_id, "deleted a segment");
        }
        report.segments_deleted += deleted_segments.len();
        Ok(())
    }
}

/// Removes from `record` the segments that truncation deletes at the record's low-water mark, and returns them.
///
/// Those are the sealed, untiered segments that lie entirely below the mark. The others stay, in their order.
fn take_segments_to_delete(record: &mut StreamRecord) -> Vec<SegmentRecord> {
    let low_water_mark = record.low_water_mark;
    let mut deleted_segments = Vec::new();
    let mut kept_segments = Vec::new();
    for segment in std::mem::take(&mut record.segments) {
        // A tiered segment stays listed until its object is deleted, so that a controller that stops in between
        // still knows about the object.
        if segment.tiered.is_none() && is_below_the_mark(&segment, low_water_mark) {
            deleted_segments.push(segment);
        } else {
            kept_segments.push(segment);
        }
    }
    record.segments = kept_segments;
    deleted_segments
}

/// Whether a segment is sealed and lies entirely below `low_water_mark`: none of its records can be read any more.
///
/// # Arguments
///
/// * `segment` - The segment's record.
/// * `low_water_mark` - The first sequence number that can still be read.
///
/// # Returns
///
/// `true` if the segment is sealed and its last record is below the mark. An open segment is never below the mark.
pub(super) fn is_below_the_mark(segment: &SegmentRecord, low_water_mark: SequenceNumber) -> bool {
    match segment.sealed_end() {
        Some(sealed_end) => sealed_end < low_water_mark,
        None => false,
    }
}

/// How far the stream's materializers and its owner allow the low-water mark to go.
///
/// Every record below the returned mark is in the artifacts of every registered materializer, and below the position
/// the owner asked to truncate before.
///
/// # Arguments
///
/// * `stream` - The stream's record: its registered materializers and the owner's request.
/// * `checkpoints` - The stream's checkpoint records. Records of materializers that are not registered are ignored.
///
/// # Returns
///
/// The mark, or `None` if no materializer is registered and the owner asked for nothing, in which case nothing may
/// be deleted.
fn allowed_low_water_mark(stream: &StreamRecord, checkpoints: &[CheckpointRecord]) -> Option<SequenceNumber> {
    let covered_by_all_materializers = first_sequence_not_covered_by_every_materializer(stream, checkpoints);
    match (covered_by_all_materializers, stream.truncate_before) {
        (Some(covered), Some(requested)) => Some(covered.min(requested)),
        (Some(mark), None) | (None, Some(mark)) => Some(mark),
        (None, None) => None,
    }
}

/// The first sequence number that some registered materializer has not checkpointed past, or `None` if no
/// materializer is registered.
fn first_sequence_not_covered_by_every_materializer(
    stream: &StreamRecord,
    checkpoints: &[CheckpointRecord],
) -> Option<SequenceNumber> {
    let mut lowest: Option<SequenceNumber> = None;
    for materializer in &stream.materializers {
        let not_covered = first_sequence_not_covered_by(materializer, checkpoints);
        lowest = match lowest {
            Some(lowest_so_far) => Some(lowest_so_far.min(not_covered)),
            None => Some(not_covered),
        };
    }
    lowest
}

/// The first sequence number that `materializer` has not checkpointed past: one after its checkpoint.
///
/// A registered materializer without a checkpoint record pins the log at its start. That cannot happen through the
/// metadata store's own operations, and assuming the worst is the safe reading if it does.
fn first_sequence_not_covered_by(materializer: &str, checkpoints: &[CheckpointRecord]) -> SequenceNumber {
    for checkpoint in checkpoints {
        if checkpoint.materializer == materializer {
            return checkpoint.checkpoint.next();
        }
    }
    SequenceNumber::FIRST
}

/// Holds the mark back to the start of the first segment that must stay whole: the open one, or one sealed more
/// recently than the stream's retention period.
///
/// # Arguments
///
/// * `stream` - The stream's record: its segments and its retention policy.
/// * `allowed_mark` - How far the materializers and the owner allow the mark to go.
/// * `now` - The time of the pass.
///
/// # Returns
///
/// `allowed_mark`, or the first sequence number of the first segment that must stay if that is lower. If every
/// segment may go, the limit is where the stream's next segment will start.
fn hold_back_for_retention(stream: &StreamRecord, allowed_mark: SequenceNumber, now: Timestamp) -> SequenceNumber {
    let keep_sealed_for = stream.retention.keep_sealed_for();
    let first_kept_segment = stream
        .segments
        .iter()
        .find(|segment| must_stay_whole(segment, keep_sealed_for, now));
    let retention_limit = match first_kept_segment {
        Some(segment) => segment.first_sequence,
        None => stream.next_segment_first_sequence(),
    };
    allowed_mark.min(retention_limit)
}

/// Whether retention keeps all of `segment`: it is open, or it was sealed less than `keep_sealed_for` ago.
fn must_stay_whole(segment: &SegmentRecord, keep_sealed_for: Option<Duration>, now: Timestamp) -> bool {
    let Some(seal) = segment.sealed else {
        return true;
    };
    match keep_sealed_for {
        Some(keep_for) => now.duration_since(seal.at) < keep_for,
        None => false,
    }
}

#[cfg(test)]
#[path = "truncation_test.rs"]
mod tests;
