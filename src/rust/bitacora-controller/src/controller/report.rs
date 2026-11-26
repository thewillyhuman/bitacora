//! What a pass and a round of the controller report.
//!
//! # Why this file exists
//!
//! The controller works unattended, so what it did has to be visible from outside: an operator alerts on segments
//! that are short of copies, a dashboard shows repairs and uploads, a test asserts that a pass did exactly one thing.
//! The pass counts what it does as it goes, and this file defines what is counted. It also holds the one number of
//! the report that is a judgement and not a count of actions: how many segments are short of good copies.
//!
//! # What it contains
//!
//! - [`PassReport`]: what one pass did, as counters, and what it found, as one gauge.
//! - [`Round`]: what one round of the controller's loop did: led and ran a pass, or stood by.
//! - `segments_missing_copies`: the gauge, as a pure function of a stream's record.
//!
//! # How it works
//!
//! A pass starts with an empty report and hands it to every step, which adds what it did. The process that hosts the
//! controller reads the report after every round; `bita-controller` exports each field as a metric.
//!
//! A segment is **missing copies** when it is sealed, still readable, still on replicas, and has fewer good copies
//! than the stream's replication asks for. That is "durability at risk" in the design's list of signals: such a
//! segment survives fewer failures than it should until repair catches up. Two kinds of segment are deliberately not
//! counted, because nothing is wrong with them: one that was left to the object store alone, and one that lies below
//! the low-water mark and only awaits deletion.
//!
//! # Invariants
//!
//! - Every field but `segments_missing_copies` counts things the pass did, so a pass that had nothing to do reports
//!   all zeroes. `segments_missing_copies` describes the cluster after the pass, so it stays above zero for as long
//!   as the condition lasts.
//!
//! # See also
//!
//! - `../controller.rs`: the pass that fills the report.
//! - `round.rs`: the round that returns it.
//! - `docs/operations/metrics.md`: the metrics the daemon makes of it.

use bitacora_meta::{CopyState, SegmentRecord, StreamRecord};

/// What one pass did, and what it found.
///
/// # Examples
///
/// ```
/// use bitacora_controller::PassReport;
///
/// let nothing_to_do = PassReport::default();
///
/// assert_eq!(nothing_to_do.copies_repaired, 0);
/// assert_eq!(nothing_to_do.segments_missing_copies, 0);
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PassReport {
    /// How many streams got a standby segment placed.
    pub standbys_placed: usize,
    /// How many copies of sealed segments were confirmed complete.
    pub copies_confirmed: usize,
    /// How many copies of sealed segments were found short, damaged or lost, and marked for replacement.
    pub copies_marked_bad: usize,
    /// How many bad copies of sealed segments were rebuilt from a good one.
    pub copies_repaired: usize,
    /// How many sealed segments were uploaded to the object store.
    pub segments_tiered: usize,
    /// How many tiered segments were left to the object store alone, their hot-read window being over.
    pub segments_released: usize,
    /// How many segments below the low-water mark were deleted.
    pub segments_deleted: usize,
    /// How many segment copies were removed from replicas because their segment was deleted, moved by repair, or
    /// their stream was deleted.
    pub copies_removed: usize,
    /// How many sealed segments had fewer good copies than the stream's replication asks for when the pass was done
    /// with their stream: durability at risk. Segments left to the object store alone and segments awaiting deletion
    /// are not counted.
    pub segments_missing_copies: usize,
    /// How many snapshots of the metadata store were written to the object store: one when a snapshot was due,
    /// otherwise none.
    pub metadata_snapshots_written: usize,
    /// How many streams could not be fully handled; they are retried on the next pass.
    pub streams_with_errors: usize,
}

/// What one round of the controller's loop did.
///
/// # Examples
///
/// ```
/// use bitacora_controller::{PassReport, Round};
///
/// fn describe(round: &Round) -> String {
///     match round {
///         Round::Led(report) => format!("led, and repaired {} copies", report.copies_repaired),
///         Round::StoodBy => "stood by".to_owned(),
///     }
/// }
///
/// assert_eq!(describe(&Round::StoodBy), "stood by");
/// assert_eq!(describe(&Round::Led(PassReport::default())), "led, and repaired 0 copies");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Round {
    /// This instance leads, and ran a pass.
    Led(PassReport),
    /// Another instance leads. This one did nothing and stands by.
    StoodBy,
}

/// Counts the sealed segments of `stream` that have fewer good copies than its replication asks for.
///
/// See the file header for what is counted and what is not.
///
/// # Arguments
///
/// * `stream` - The stream's record, as it is after the pass handled the stream.
///
/// # Returns
///
/// How many of the stream's segments are short of good copies.
pub(super) fn segments_missing_copies(stream: &StreamRecord) -> usize {
    let mut missing_count = 0;
    for segment in &stream.segments {
        if is_missing_copies(stream, segment) {
            missing_count += 1;
        }
    }
    missing_count
}

/// Whether `segment` is one that needs good copies on replicas and has too few of them.
fn is_missing_copies(stream: &StreamRecord, segment: &SegmentRecord) -> bool {
    let Some(sealed_end) = segment.sealed_end() else {
        // An open segment is the writer's business.
        return false;
    };
    let is_released = segment
        .tiered
        .as_ref()
        .is_some_and(|tiered| tiered.are_local_copies_dropped);
    if is_released || sealed_end < stream.low_water_mark {
        return false;
    }
    good_copy_count(segment) < stream.replication.replica_count()
}

/// Counts the copies of `segment` that are known to be complete.
///
/// # Arguments
///
/// * `segment` - The segment's record.
///
/// # Returns
///
/// How many of its copies are in the state [`CopyState::Good`].
pub(super) fn good_copy_count(segment: &SegmentRecord) -> usize {
    let mut good_copies = 0;
    for copy in &segment.copies {
        if copy.state == CopyState::Good {
            good_copies += 1;
        }
    }
    good_copies
}

#[cfg(test)]
#[path = "report_test.rs"]
mod tests;
