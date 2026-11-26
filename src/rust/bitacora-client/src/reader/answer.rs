//! What one replica's answer to a read means.
//!
//! # Why this file exists
//!
//! A reader asks one replica at a time and decides from the answer alone what to do next: hand the records out, move to
//! the next segment, wait, or ask another replica (design, section 6). The replica is not trusted in this: a reader
//! verifies every batch checksum itself, and it never hands out a record other than the one it asked for. These
//! decisions do no I/O, so they are kept here as a plain function that can be tested with hand-made answers, apart from
//! the cursor that does the asking.
//!
//! # What it contains
//!
//! - [`ReplicaAnswer`]: the four things an answer can mean.
//! - [`classify_response`]: decides which of them a response is.
//!
//! # How it works
//!
//! `classify_response` decodes the batch frames of the response, which verifies each checksum against the address of
//! the segment and checks that the batches follow one another without a gap. Replicas serve whole batches, so the first
//! one may start before the position that was asked for; records below it are dropped. What is left must start exactly
//! at the position.
//!
//! If nothing is left, the segment's sealed end decides: a position past the end of a sealed segment means the reader
//! is done with the segment, and anything else means the replica simply holds nothing there, because nothing is
//! committed there yet or because its copy is short.
//!
//! # Invariants
//!
//! - A record is handed out only from a batch that passed its checksum for this segment.
//! - The records handed out are consecutive and start exactly at the position asked for.
//! - This runs once per answer on the reader's hot path, so it does one pass over the frames and fills one vector. The
//!   payloads are reference-counted slices of the response's buffer, not copies.
//!
//! # See also
//!
//! - `cursor.rs`: the caller, which sends the request and acts on the answer.
//! - `bitacora-segment/src/format.rs`: `decode_frames` and the layout of a batch frame.
//! - `../tiered.rs`: the same verification for bytes that come from the object store.

use bitacora_segment::format::{Batch, decode_frames};
use bitacora_types::replica_api::ReadResponse;
use bitacora_types::{LogRecord, SegmentAddress, SequenceNumber};

/// What one replica answered for a position in a segment.
#[derive(Debug, PartialEq)]
pub(super) enum ReplicaAnswer {
    /// One or more records, the first of which is the one at the position.
    Records(Vec<LogRecord>),
    /// The segment is sealed and the position is past its end.
    SegmentFinished,
    /// The replica has nothing at the position: it is caught up, behind, or holds a short copy.
    Nothing,
    /// The replica failed or sent something invalid. Holds why, for the error the reader may end up reporting.
    Failed(String),
}

/// Decides what a replica's response to a read at `next_sequence` means.
///
/// See the file header for the rules.
///
/// # Arguments
///
/// * `address` - The segment that was read. Batch checksums are verified against it.
/// * `response` - What the replica sent.
/// * `next_sequence` - The position that was asked for.
/// * `recorded_sealed_end` - The segment's sealed end according to the metadata the reader holds, if it is sealed
///   there. It takes precedence over what the replica says.
///
/// # Returns
///
/// The meaning of the response. [`ReplicaAnswer::Failed`] if a batch fails its checksum, the batches have a gap, or the
/// records do not start at `next_sequence`.
pub(super) fn classify_response(
    address: SegmentAddress,
    response: &ReadResponse,
    next_sequence: SequenceNumber,
    recorded_sealed_end: Option<SequenceNumber>,
) -> ReplicaAnswer {
    // The reader verifies every batch checksum itself (design, section 6, step 4): the replica is not trusted to have
    // done it.
    let batches = match decode_frames(address, &response.batch_frames) {
        Ok(batches) => batches,
        Err(frames_error) => return ReplicaAnswer::Failed(frames_error.to_string()),
    };
    let records = records_from(&batches, next_sequence);
    if let Some(first_record) = records.first() {
        if first_record.sequence != next_sequence {
            return ReplicaAnswer::Failed(format!(
                "answer starts at {} instead of {}",
                first_record.sequence, next_sequence
            ));
        }
        return ReplicaAnswer::Records(records);
    }
    let sealed_end = recorded_sealed_end.or(response.sealed_end);
    if sealed_end.is_some_and(|sealed_end| next_sequence > sealed_end) {
        ReplicaAnswer::SegmentFinished
    } else {
        ReplicaAnswer::Nothing
    }
}

/// The records of `batches` at or after `next_sequence`, in order.
fn records_from(batches: &[Batch], next_sequence: SequenceNumber) -> Vec<LogRecord> {
    let mut records = Vec::new();
    for batch in batches {
        for record in batch.records() {
            if record.sequence >= next_sequence {
                records.push(record);
            }
        }
    }
    records
}

#[cfg(test)]
#[path = "answer_test.rs"]
mod tests;
