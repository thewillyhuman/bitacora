//! I1 Durability: a record that was reported as committed stays in the stream, where it was reported.
//!
//! # Why this file exists
//!
//! "Committed" is the one promise a writer makes to its caller: the record is in the log at this sequence number, and
//! no failure the design covers will take it away. Everything a client does after an append returned rests on it. A log
//! can break the promise in two ways. It can keep another record at that sequence number, which happens when a
//! recovering writer seals the stream below something that was acknowledged. Or it can have nothing there any more,
//! which happens when the last copies of a segment are lost or wrongly deleted. This file checks for both.
//!
//! # What it contains
//!
//! - [`require_commit_to_be_what_was_read`]: a committed record against what readers found at its sequence number
//!   during the run.
//! - [`check_audit`]: every committed record against the one read of the whole stream after healing. It gathers an
//!   `AuditedHistory` from the events and then holds every committed append against it.
//!
//! # How it works
//!
//! During a run, readers read parts of the stream at moments of their own choosing. Whatever they found at a sequence
//! number that an append was reported committed at must be that append's record. That catches a replaced record, but
//! not a lost one: a record nobody happened to read may be gone without anyone noticing.
//!
//! So after healing, the audit reads the whole stream once more, from the low-water mark to the final record. Every
//! append reported committed at or above the position the audit started at must be in what the audit was handed, with
//! its own payload. Records below that position were truncated; whether that was allowed is I5, which is judged
//! separately.
//!
//! # Invariants
//!
//! - Only appends reported as committed are held against the stream. An append of unknown outcome may be there or not.
//! - The audit counts only what its own session read, not what other sessions read earlier.
//! - [`check_audit`] reports the missing records in the order their appends finished.
//!
//! # See also
//!
//! - `checker.rs`: calls the first rule for every committed append.
//! - `audit.rs`: makes the read that [`check_audit`] judges.
//! - `report.rs`: runs [`check_audit`] only for a run whose audit finished.

use std::collections::BTreeMap;

use bitacora_types::SequenceNumber;
use bytes::Bytes;

use super::Violation;
use super::observations::FirstRead;
use crate::history::{AppendOutcome, Event, TimedEvent};

/// An append that was reported as committed.
struct CommittedAppend {
    /// The identifier of the call.
    operation: u64,
    /// Where the writer reported the record committed.
    sequence: SequenceNumber,
}

/// What a history says about the audit's read and about the appends it is held against.
struct AuditedHistory {
    /// The payload the audit was handed at each sequence number.
    audited: BTreeMap<SequenceNumber, Bytes>,
    /// Where the audit started reading. Records below it were truncated before the audit.
    audited_from: SequenceNumber,
    /// The payload every append was called with.
    payload_by_operation: BTreeMap<u64, Bytes>,
    /// The appends that were reported as committed, in the order they finished.
    committed_appends: Vec<CommittedAppend>,
}

impl AuditedHistory {
    /// Gathers from `events` what the session `audit_session` read and what was appended.
    fn gather(events: &[TimedEvent], audit_session: u64) -> Self {
        let mut history = Self {
            audited: BTreeMap::new(),
            audited_from: SequenceNumber::FIRST,
            payload_by_operation: BTreeMap::new(),
            committed_appends: Vec::new(),
        };
        for timed in events {
            match &timed.event {
                Event::ReaderStarted { session, from_sequence } if *session == audit_session => {
                    history.audited_from = *from_sequence;
                }
                Event::RecordRead {
                    session,
                    sequence,
                    payload,
                } if *session == audit_session => {
                    history.audited.insert(*sequence, payload.clone());
                }
                Event::AppendInvoked { operation, payload } => {
                    history.payload_by_operation.insert(*operation, payload.clone());
                }
                Event::AppendFinished {
                    operation,
                    outcome: AppendOutcome::Committed { sequence, .. },
                } => history.committed_appends.push(CommittedAppend {
                    operation: *operation,
                    sequence: *sequence,
                }),
                _ => {}
            }
        }
        history
    }

    /// Whether the audit should have been handed the record of `committed` and was not: the record lies at or above
    /// the position the audit started at, and what the audit read there is not the payload of that append.
    fn is_missing(&self, committed: &CommittedAppend) -> bool {
        let was_truncated_before_the_audit = committed.sequence < self.audited_from;
        let was_audited = self.audited.get(&committed.sequence) == self.payload_by_operation.get(&committed.operation);
        !was_truncated_before_the_audit && !was_audited
    }
}

/// Requires that what readers found at `sequence`, if they read it, is the record of the append that was reported
/// committed there.
///
/// # Arguments
///
/// * `stream` - What was first read at each sequence number.
/// * `operation` - The append that was reported committed.
/// * `sequence` - Where it was reported committed.
/// * `payload` - The payload it wrote.
///
/// # Returns
///
/// [`Violation::CommittedRecordReplaced`] if a session read another payload at `sequence`; `None` if nobody read
/// `sequence` or what was read is the committed record.
pub(super) fn require_commit_to_be_what_was_read(
    stream: &BTreeMap<SequenceNumber, FirstRead>,
    operation: u64,
    sequence: SequenceNumber,
    payload: &Bytes,
) -> Option<Violation> {
    let first_read = stream.get(&sequence)?;
    if first_read.payload == *payload {
        return None;
    }
    Some(Violation::CommittedRecordReplaced {
        operation,
        sequence,
        session: first_read.session,
    })
}

/// Checks that the reader of `audit_session`, which read the whole stream after the cluster was healed, was handed
/// every record that was ever reported as committed (I1 Durability).
///
/// Reads the history once. Records below the position the audit started at were truncated; whether that was allowed is
/// I5, which [`check`](crate::check) judges.
///
/// # Arguments
///
/// * `events` - A whole history, in the order it was recorded.
/// * `audit_session` - The reading session of the audit's read of the whole stream.
///
/// # Returns
///
/// One [`Violation::CommittedRecordMissing`] for every append reported committed at or above the audit's starting
/// position whose record the audit was not handed at that sequence number, in the order the appends finished. Empty if
/// the audit found them all.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() {
/// use bitacora_sim::{AppendOutcome, History, Violation, check_audit};
/// use bitacora_types::{Epoch, SequenceNumber};
/// use bytes::Bytes;
///
/// let history = History::new();
/// let sequence = SequenceNumber::FIRST;
/// let epoch = Epoch::new(1);
/// let operation = history.append_invoked(Bytes::from_static(b"a"));
/// history.append_finished(operation, AppendOutcome::Committed { sequence, epoch });
/// // The audit reads the stream from the start and is handed nothing.
/// let audit_session = history.reader_started(sequence);
///
/// let violations = check_audit(&history.events(), audit_session);
///
/// assert_eq!(violations, [Violation::CommittedRecordMissing { operation, sequence }]);
/// # }
/// ```
#[must_use]
pub fn check_audit(events: &[TimedEvent], audit_session: u64) -> Vec<Violation> {
    let history = AuditedHistory::gather(events, audit_session);
    let mut violations = Vec::new();
    for committed in &history.committed_appends {
        if history.is_missing(committed) {
            violations.push(Violation::CommittedRecordMissing {
                operation: committed.operation,
                sequence: committed.sequence,
            });
        }
    }
    violations
}

#[cfg(test)]
#[path = "durability_test.rs"]
mod tests;
