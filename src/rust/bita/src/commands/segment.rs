//! The `bita segment` commands.
//!
//! # Why this file exists
//!
//! A stream is a list of segments, and almost every question about durability is a question about that
//! list: which replicas hold this range, is a copy missing, has it reached the object store, is it about
//! to be deleted. The stream's description only counts segments; this file shows them.
//!
//! # What it contains
//!
//! - [`list`]: the segments of one stream.
//! - Its result: [`SegmentList`], [`SegmentRow`], [`CopyCell`].
//!
//! # How it works
//!
//! Everything comes from the stream's record in the metadata store; no replica is contacted. So the copy
//! states shown are what the controller last recorded, not what the disks hold at this instant.
//!
//! A segment is shown in one of four states:
//!
//! | State | Meaning |
//! | --- | --- |
//! | `open` | The writer is appending to it. |
//! | `sealed` | Complete and immutable, on replicas only. |
//! | `tiered` | Also uploaded to the object store; replicas still hold it. |
//! | `released` | In the object store alone; the replicas dropped their copies. |
//!
//! A sealed segment that lies entirely below the low-water mark is additionally marked as awaiting
//! deletion.
//!
//! # Invariants
//!
//! - Read-only: nothing is changed.
//!
//! # See also
//!
//! - `bitacora-meta/src/records.rs`: the segment record this is a view of.

use bitacora_meta::{CopyState, SegmentRecord, StreamRecord};
use bitacora_types::SequenceNumber;
use serde::Serialize;

use super::CommandError;
use crate::cluster::Cluster;
use crate::output::{Table, format_bytes};

/// One replica's copy of a segment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct CopyCell {
    /// The replica that holds the copy.
    pub(crate) replica_id: String,
    /// What the controller last recorded about it: `unverified`, `good` or `bad`.
    pub(crate) state: String,
}

/// One segment of a stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct SegmentRow {
    /// The segment's identifier within the stream.
    pub(crate) segment_id: u64,
    /// The epoch of the writer that authored its records.
    pub(crate) epoch: u64,
    /// The sequence number of its first record.
    pub(crate) first_sequence: u64,
    /// Its final sequence number, once sealed. One below the first if it was sealed empty.
    pub(crate) sealed_end: Option<u64>,
    /// `open`, `sealed`, `tiered` or `released`: see the file header.
    pub(crate) state: String,
    /// Whether the segment lies entirely below the low-water mark and only awaits deletion.
    pub(crate) awaits_deletion: bool,
    /// The copies on replicas, in replica-set order.
    pub(crate) copies: Vec<CopyCell>,
    /// The key of the segment's object in the object store, once uploaded.
    pub(crate) object_key: Option<String>,
    /// The size of that object.
    pub(crate) object_size_bytes: Option<u64>,
}

/// The segments of a stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct SegmentList {
    /// The stream the segments belong to.
    pub(crate) stream: String,
    /// The first sequence number of the stream that can still be read.
    pub(crate) low_water_mark: u64,
    /// The segments, oldest first.
    pub(crate) segments: Vec<SegmentRow>,
}

impl SegmentRow {
    /// The cells of this segment's row in a table made by [`segment_table`].
    pub(super) fn table_row(&self) -> Vec<String> {
        let sealed_end = match self.sealed_end {
            Some(end) => end.to_string(),
            None => "-".to_owned(),
        };
        let state = if self.awaits_deletion {
            format!("{} (awaits deletion)", self.state)
        } else {
            self.state.clone()
        };
        let mut copies = Vec::with_capacity(self.copies.len());
        for copy in &self.copies {
            copies.push(format!("{}={}", copy.replica_id, copy.state));
        }
        let object = match (&self.object_key, self.object_size_bytes) {
            (Some(object_key), Some(size_bytes)) => {
                format!("{object_key} ({})", format_bytes(size_bytes))
            }
            _no_object => "-".to_owned(),
        };
        vec![
            self.segment_id.to_string(),
            self.epoch.to_string(),
            self.first_sequence.to_string(),
            sealed_end,
            state,
            copies.join(" "),
            object,
        ]
    }
}

/// An empty table with the columns of a segment listing. The rebuild report shows its planned segments
/// in the same table.
pub(super) fn segment_table() -> Table {
    Table::new(&[
        "SEGMENT", "EPOCH", "FIRST", "END", "STATE", "COPIES", "OBJECT",
    ])
}

impl SegmentList {
    /// Renders the segments as a table, one row each.
    pub(crate) fn render_text(&self) -> String {
        let mut table = segment_table();
        for segment in &self.segments {
            table.push_row(segment.table_row());
        }
        table.render()
    }
}

/// The word shown for a copy state.
fn copy_state_name(state: CopyState) -> &'static str {
    match state {
        CopyState::Unverified => "unverified",
        CopyState::Good => "good",
        CopyState::Bad => "bad",
    }
}

/// The word shown for where a segment is in its life.
fn segment_state_name(segment: &SegmentRecord) -> &'static str {
    match (&segment.sealed, &segment.tiered) {
        (None, _) => "open",
        (Some(_), None) => "sealed",
        (Some(_), Some(tiered)) if tiered.are_local_copies_dropped => "released",
        (Some(_), Some(_)) => "tiered",
    }
}

/// Describes one segment of `stream`.
pub(super) fn segment_row(stream: &StreamRecord, segment: &SegmentRecord) -> SegmentRow {
    let mut copies = Vec::with_capacity(segment.copies.len());
    for copy in &segment.copies {
        copies.push(CopyCell {
            replica_id: copy.replica_id.to_string(),
            state: copy_state_name(copy.state).to_owned(),
        });
    }
    let awaits_deletion = segment
        .sealed_end()
        .is_some_and(|sealed_end| sealed_end < stream.low_water_mark);
    SegmentRow {
        segment_id: segment.segment_id.get(),
        epoch: segment.epoch.get(),
        first_sequence: segment.first_sequence.get(),
        sealed_end: segment.sealed_end().map(SequenceNumber::get),
        state: segment_state_name(segment).to_owned(),
        awaits_deletion,
        copies,
        object_key: segment
            .tiered
            .as_ref()
            .map(|tiered| tiered.object_key.clone()),
        object_size_bytes: segment.tiered.as_ref().map(|tiered| tiered.size_bytes),
    }
}

/// Lists the segments of a stream, oldest first.
///
/// # Arguments
///
/// * `cluster` - The interfaces to the cluster.
/// * `stream_name` - The stream's name.
///
/// # Errors
///
/// Returns [`CommandError::Metadata`] if no stream has this name or the metadata store is unavailable.
pub(crate) async fn list(
    cluster: &Cluster,
    stream_name: &str,
) -> Result<SegmentList, CommandError> {
    let stream = cluster.metadata.get_stream(stream_name).await?;
    let mut segments = Vec::with_capacity(stream.segments.len());
    for segment in &stream.segments {
        segments.push(segment_row(&stream, segment));
    }
    Ok(SegmentList {
        stream: stream.name.clone(),
        low_water_mark: stream.low_water_mark.get(),
        segments,
    })
}

#[cfg(test)]
#[path = "segment_test.rs"]
mod tests;
