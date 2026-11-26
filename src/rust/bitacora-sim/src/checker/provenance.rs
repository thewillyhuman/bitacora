//! Every record in the stream was appended by someone, once, and was not refused.
//!
//! # Why this file exists
//!
//! The five invariants of the design say what a log must do with the records it was given. They take for granted that
//! the log holds nothing else. A replica that serves bytes from a torn write, a repair that copies a record to the
//! wrong place, a writer that reports an append as refused and stores it anyway: each leaves the stream with a record
//! that no append explains, or with one too many. None of the five invariants need notice. This file states the rule
//! they assume, and checks it.
//!
//! # What it contains
//!
//! - [`explain_reads_by_appends`]: every record that was read must come from an append, appear at one sequence number
//!   only, and not come from an append the writer refused.
//!
//! # How it works
//!
//! The stream, as readers found it, is walked by sequence number. Each record is looked up by its payload among the
//! appends of the history. A record without an append was invented. A record whose append already explained an earlier
//! sequence number is a duplicate. A record whose append was refused before it got a sequence number should not exist.
//!
//! Fillers are skipped. A filler is a record with no information of its own, such as the barrier a key-value read
//! appends; every one of them has the same payload, nobody records appending them, and there may be any number.
//!
//! # Invariants
//!
//! - The rule relies on every append of a simulation writing a payload of its own; that is what lets a record name its
//!   append.
//! - An append of unknown outcome explains a record as well as a committed one does.
//! - Violations are found in the order of the stream.
//!
//! # See also
//!
//! - `checker.rs`: calls this after the history was read.
//! - `observations.rs`: the stream, the appends and the fillers.
//! - `uniqueness.rs`: the rule fillers are not exempt from.

use std::collections::BTreeMap;

use bitacora_types::SequenceNumber;

use super::Violation;
use super::observations::Observations;
use crate::history::AppendOutcome;

/// Requires that what the readers saw is explained by what the writers did: every record was appended by someone,
/// appears once, and was not refused.
///
/// # Arguments
///
/// * `observations` - The stream, the appends and the fillers of a history.
/// * `violations` - Where a [`Violation::UnknownRecord`], [`Violation::RecordAppearsTwice`] or
///   [`Violation::RefusedRecordAppeared`] is added for every record that breaks the rule, in the order of the stream.
pub(super) fn explain_reads_by_appends(observations: &Observations, violations: &mut Vec<Violation>) {
    let mut first_sequence_of_operation: BTreeMap<u64, SequenceNumber> = BTreeMap::new();
    for (sequence, first_read) in &observations.stream {
        if observations.fillers.contains(&first_read.payload) {
            continue;
        }
        let Some(append) = observations.appends_by_payload.get(&first_read.payload) else {
            violations.push(Violation::UnknownRecord {
                session: first_read.session,
                sequence: *sequence,
            });
            continue;
        };
        if let Some(first_sequence) = first_sequence_of_operation.get(&append.operation) {
            violations.push(Violation::RecordAppearsTwice {
                operation: append.operation,
                first_sequence: *first_sequence,
                second_sequence: *sequence,
            });
        } else {
            first_sequence_of_operation.insert(append.operation, *sequence);
        }
        if append.outcome == Some(AppendOutcome::NotAppended) {
            violations.push(Violation::RefusedRecordAppeared {
                operation: append.operation,
                sequence: *sequence,
            });
        }
    }
}

#[cfg(test)]
#[path = "provenance_test.rs"]
mod tests;
