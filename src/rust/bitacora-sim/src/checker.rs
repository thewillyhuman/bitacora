//! Judges a history against the invariants of the log (design, section 15).
//!
//! # Why this file exists
//!
//! A simulated run produces thousands of appends and reads, under faults nobody chose. Whether the log behaved
//! correctly cannot be seen by looking, and it cannot be asked of the system, which may be wrong in a way it believes
//! in. It has to be decided from the outside, by rules that mention only what clients did and saw. The design states
//! those rules as five invariants. This file and its children are the rules as code, and the verdict of a run is the
//! list of what they find.
//!
//! # What it contains
//!
//! - [`Violation`]: one way a history breaks an invariant.
//! - [`check`]: every violation of a history that readers and writers can observe.
//! - [`check_audit`]: whether the read of the whole stream after healing found every committed record.
//! - The rules, one file per invariant in `checker/`: `durability.rs` (I1), `uniqueness.rs` (I2), `prefix.rs` (I3),
//!   `single_writer.rs` (I4), and `provenance.rs` for the rule the design takes for granted: a log holds what was
//!   appended, once. `observations.rs` holds what a pass over the history collects for them.
//!
//! # How it works
//!
//! The checker sees only what clients saw. It knows nothing about replicas, epochs in the metadata store or faults, so
//! it cannot be fooled by a bug that the system itself believes in.
//!
//! [`check`] reads the history once, in order. On the way it applies the rules that can be judged event by event: each
//! reading session must be handed its records in order (I3), and no two sessions may be handed different records for
//! one sequence number (I2). The pass also collects what the writers were told and what the stream turned out to hold.
//! Three more passes over what was collected follow: every record read must have been appended, once, and not refused;
//! every append reported as committed must be what readers found at its sequence number (I1), and no two appends may be
//! reported at one sequence number (I4); and along the stream the committing epoch must never decrease (I4).
//!
//! Safe truncation (I5) has no file of its own. Whether records above a materializer's checkpoint were deleted can only
//! be seen where the checkpoint is known, so the audit and the materializer establish the fact and record it as an
//! event; the checker turns each such event into a violation.
//!
//! # Invariants
//!
//! - The verdict depends on the events alone, so a seed that fails keeps failing.
//! - Violations are listed in a fixed order: those found while reading the history, in the order of the events that
//!   reveal them; then invented, duplicated and refused records, by sequence number; then commits, by payload; then
//!   epochs, by sequence number.
//! - A correct history has no violation: there are no false alarms. An append whose outcome is unknown may be in the
//!   stream or not.
//! - The checker allocates per record of the history only what its maps need to remember the record; it clones no
//!   payload bytes.
//!
//! # See also
//!
//! - `history.rs`: the events judged here.
//! - `linearizability.rs`: the check of the key-value calls, which this checker ignores.
//! - `report.rs`: runs the checks and builds the verdict of a run.

mod durability;
mod observations;
mod prefix;
mod provenance;
mod single_writer;
mod uniqueness;

use bitacora_types::{Epoch, SequenceNumber};
use bytes::Bytes;

pub use self::durability::check_audit;
use self::observations::Observations;
use self::prefix::ReadPositions;
use self::single_writer::Commits;
use crate::history::{AppendOutcome, Event, TimedEvent};

/// One way a history breaks an invariant.
///
/// The checks return these; a run is correct only if they return none.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() {
/// use bitacora_sim::{History, Violation, check};
/// use bitacora_types::SequenceNumber;
/// use bytes::Bytes;
///
/// let history = History::new();
/// let session = history.reader_started(SequenceNumber::FIRST);
/// history.record_read(session, SequenceNumber::FIRST, Bytes::from_static(b"nobody appended this"));
///
/// let violations = check(&history.events());
///
/// let sequence = SequenceNumber::FIRST;
/// assert_eq!(violations, [Violation::UnknownRecord { session, sequence }]);
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Violation {
    /// I1 Durability: a record reported as committed was read back as something else.
    CommittedRecordReplaced {
        /// The append that was reported committed.
        operation: u64,
        /// Where it was reported committed.
        sequence: SequenceNumber,
        /// The session that read another record there.
        session: u64,
    },
    /// I1 Durability: a record reported as committed is missing from a full read of the stream.
    CommittedRecordMissing {
        /// The append that was reported committed.
        operation: u64,
        /// Where it was reported committed.
        sequence: SequenceNumber,
    },
    /// I2 Uniqueness: two readers observed different records at the same sequence number.
    ReadersDisagree {
        /// The sequence number they disagree on.
        sequence: SequenceNumber,
        /// The first session that read it.
        first_session: u64,
        /// The session that read something else.
        second_session: u64,
    },
    /// I3 Prefix: a reader did not observe a gapless prefix in order.
    ReaderOutOfOrder {
        /// The session that broke the order.
        session: u64,
        /// The sequence number it had to read next.
        expected: SequenceNumber,
        /// The sequence number it read.
        found: SequenceNumber,
    },
    /// I4 Single writer: two appends were both reported committed at the same sequence number.
    SequenceCommittedTwice {
        /// The contested sequence number.
        sequence: SequenceNumber,
        /// The first append reported committed there.
        first_operation: u64,
        /// The second append reported committed there.
        second_operation: u64,
    },
    /// I4 Single writer: an older epoch committed after a newer one, so two epochs were able to commit at the same
    /// time.
    EpochWentBackwards {
        /// A sequence number committed by the newer epoch.
        earlier_sequence: SequenceNumber,
        /// The newer epoch.
        earlier_epoch: Epoch,
        /// A later sequence number committed by the older epoch.
        later_sequence: SequenceNumber,
        /// The older epoch.
        later_epoch: Epoch,
    },
    /// I5 Safe truncation: records above a registered materializer's checkpoint were deleted.
    TruncatedPastCheckpoint {
        /// The materializer's committed checkpoint.
        checkpoint: SequenceNumber,
        /// The low-water mark that passed it.
        low_water_mark: SequenceNumber,
    },
    /// The calls to the key-value store on one key, and what they returned, cannot be explained by any order in which
    /// they could have taken effect one at a time.
    NotLinearizable {
        /// The key whose history has no such order.
        key: Bytes,
        /// The finished call that no order explored could get past.
        stuck_at_operation: u64,
    },
    /// The search for an order of the calls on one key was given up. It counts as a failure: a history that cannot be
    /// judged is not known to be correct.
    LinearizabilityUndecided {
        /// The key whose history was too tangled to judge.
        key: Bytes,
    },
    /// A reader observed a record nobody appended.
    UnknownRecord {
        /// The session that read it.
        session: u64,
        /// Where it read it.
        sequence: SequenceNumber,
    },
    /// The same record appears at two sequence numbers.
    RecordAppearsTwice {
        /// The append whose record is duplicated.
        operation: u64,
        /// The first place it appears.
        first_sequence: SequenceNumber,
        /// The second place it appears.
        second_sequence: SequenceNumber,
    },
    /// A record the writer refused without assigning a sequence number is in the stream.
    RefusedRecordAppeared {
        /// The append that was refused.
        operation: u64,
        /// Where its record appears.
        sequence: SequenceNumber,
    },
}

/// Checks a history against the invariants every reader and writer can observe: I1 to I5, plus the absence of invented,
/// duplicated and refused records.
///
/// Reads the history once and then judges what it collected; see the file header for the passes. Calls to the key-value
/// store are ignored: [`check_linearizability`](crate::check_linearizability) judges those. The committed records are
/// held against what readers happened to read; that every committed record is still there at the end is the business of
/// [`check_audit`].
///
/// # Arguments
///
/// * `events` - A whole history, in the order it was recorded.
///
/// # Returns
///
/// Every violation found, in a deterministic order. Empty for a correct history.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() {
/// use bitacora_sim::{AppendOutcome, History, Violation, check};
/// use bitacora_types::{Epoch, SequenceNumber};
/// use bytes::Bytes;
///
/// let history = History::new();
/// let sequence = SequenceNumber::FIRST;
/// let epoch = Epoch::new(1);
/// let committed = history.append_invoked(Bytes::from_static(b"a"));
/// history.append_finished(committed, AppendOutcome::Committed { sequence, epoch });
/// let other = history.append_invoked(Bytes::from_static(b"b"));
/// history.append_finished(other, AppendOutcome::Unknown);
/// // A reader finds the other record where the first was reported committed.
/// let session = history.reader_started(sequence);
/// history.record_read(session, sequence, Bytes::from_static(b"b"));
///
/// let violations = check(&history.events());
///
/// let operation = committed;
/// assert_eq!(violations, [Violation::CommittedRecordReplaced { operation, sequence, session }]);
/// # }
/// ```
#[must_use]
pub fn check(events: &[TimedEvent]) -> Vec<Violation> {
    let mut violations = Vec::new();
    let observations = observe(events, &mut violations);
    provenance::explain_reads_by_appends(&observations, &mut violations);
    let commits = hold_commits_against_reads(&observations, &mut violations);
    single_writer::require_epochs_in_order(&commits, &mut violations);
    violations
}

/// Replays the history, checking what can be checked event by event: that each session reads in order (I3), that
/// sessions agree (I2), and that nothing was truncated past a checkpoint (I5).
///
/// # Returns
///
/// What the writers were told and what the stream turned out to hold, for the passes that follow.
fn observe(events: &[TimedEvent], violations: &mut Vec<Violation>) -> Observations {
    let mut observations = Observations::default();
    let mut read_positions = ReadPositions::default();
    for timed in events {
        match &timed.event {
            Event::AppendInvoked { operation, payload } => observations.note_append_invoked(*operation, payload),
            Event::AppendFinished { operation, outcome } => observations.note_append_finished(*operation, outcome),
            Event::ReaderStarted { session, from_sequence } => read_positions.start_session(*session, *from_sequence),
            Event::RecordRead {
                session,
                sequence,
                payload,
            } => {
                if let Some(out_of_order) = read_positions.require_next_in_order(*session, *sequence) {
                    violations.push(out_of_order);
                }
                let disagreement =
                    uniqueness::require_agreement(&mut observations.stream, *session, *sequence, payload);
                if let Some(disagreement) = disagreement {
                    violations.push(disagreement);
                }
            }
            Event::TruncatedPastCheckpoint {
                checkpoint,
                low_water_mark,
            } => violations.push(Violation::TruncatedPastCheckpoint {
                checkpoint: *checkpoint,
                low_water_mark: *low_water_mark,
            }),
            Event::FillerDeclared { payload } => observations.note_filler(payload),
            // Calls to the key-value store are judged on their own, by the linearizability check.
            Event::KvInvoked { .. } | Event::KvReturned { .. } | Event::Fault { .. } => {}
        }
    }
    observations
}

/// Holds what the writers were told against what the readers saw: no two appends may be told the same sequence number
/// (I4), and a record reported as committed must be the record readers found there (I1).
///
/// # Returns
///
/// The committed appends by sequence number, for the check of the epochs.
fn hold_commits_against_reads(observations: &Observations, violations: &mut Vec<Violation>) -> Commits {
    let mut commits = Commits::default();
    for (payload, append) in &observations.appends_by_payload {
        let Some(AppendOutcome::Committed { sequence, epoch }) = &append.outcome else {
            continue;
        };
        if let Some(committed_twice) = commits.note_commit(*sequence, append.operation, *epoch) {
            violations.push(committed_twice);
        }
        let replaced =
            durability::require_commit_to_be_what_was_read(&observations.stream, append.operation, *sequence, payload);
        if let Some(replaced) = replaced {
            violations.push(replaced);
        }
    }
    commits
}

#[cfg(test)]
#[path = "checker_test.rs"]
mod tests;
