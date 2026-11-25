//! Judges a history against the invariants of the design (section 15).
//!
//! The checker sees only what clients saw. It knows nothing about replicas,
//! epochs in the metadata store or faults, so it cannot be fooled by a bug
//! that the system itself believes in.

use std::collections::BTreeMap;

use bitacora_types::{Epoch, SequenceNumber};
use bytes::Bytes;

use crate::history::{AppendOutcome, Event, TimedEvent};

/// One way a history breaks an invariant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Violation {
    /// I1 Durability: a record reported as committed was read back as
    /// something else.
    CommittedRecordReplaced {
        /// The append that was reported committed.
        operation: u64,
        /// Where it was reported committed.
        sequence: SequenceNumber,
        /// The session that read another record there.
        session: u64,
    },
    /// I1 Durability: a record reported as committed is missing from a full
    /// read of the stream.
    CommittedRecordMissing {
        /// The append that was reported committed.
        operation: u64,
        /// Where it was reported committed.
        sequence: SequenceNumber,
    },
    /// I2 Uniqueness: two readers observed different records at the same
    /// sequence number.
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
    /// I4 Single writer: two appends were both reported committed at the
    /// same sequence number.
    SequenceCommittedTwice {
        /// The contested sequence number.
        sequence: SequenceNumber,
        /// The first append reported committed there.
        first_operation: u64,
        /// The second append reported committed there.
        second_operation: u64,
    },
    /// I4 Single writer: an older epoch committed after a newer one, so two
    /// epochs were able to commit at the same time.
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
    /// I5 Safe truncation: records above a registered materializer's
    /// checkpoint were deleted.
    TruncatedPastCheckpoint {
        /// The materializer's committed checkpoint.
        checkpoint: SequenceNumber,
        /// The low-water mark that passed it.
        low_water_mark: SequenceNumber,
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
    /// A record the writer refused without assigning a sequence number is
    /// in the stream.
    RefusedRecordAppeared {
        /// The append that was refused.
        operation: u64,
        /// Where its record appears.
        sequence: SequenceNumber,
    },
}

/// What is known about one append call.
struct Append {
    operation: u64,
    outcome: Option<AppendOutcome>,
}

/// What the readers of a history saw, and what its writers were told.
#[derive(Default)]
struct Observations {
    appends_by_payload: BTreeMap<Bytes, Append>,
    /// What the stream holds at each sequence number, and who read it first.
    stream: BTreeMap<SequenceNumber, (Bytes, u64)>,
}

/// Checks a history against the invariants every reader and writer can
/// observe: I1 to I4, plus the absence of invented and duplicated records.
///
/// Returns every violation found, in a deterministic order.
#[must_use]
pub fn check(events: &[TimedEvent]) -> Vec<Violation> {
    let mut violations = Vec::new();
    let observations = observe(events, &mut violations);
    explain_reads_by_appends(&observations, &mut violations);
    let committed = hold_commits_against_reads(&observations, &mut violations);
    require_epochs_in_order(&committed, &mut violations);
    violations
}

/// Replays the history, checking what can be checked event by event: that
/// each session reads in order (I3) and that sessions agree (I2).
fn observe(events: &[TimedEvent], violations: &mut Vec<Violation>) -> Observations {
    let mut observations = Observations::default();
    let mut payload_by_operation: BTreeMap<u64, Bytes> = BTreeMap::new();
    let mut next_of_session: BTreeMap<u64, SequenceNumber> = BTreeMap::new();
    for timed in events {
        match &timed.event {
            Event::AppendInvoked { operation, payload } => {
                observations.appends_by_payload.insert(
                    payload.clone(),
                    Append {
                        operation: *operation,
                        outcome: None,
                    },
                );
                payload_by_operation.insert(*operation, payload.clone());
            }
            Event::AppendFinished { operation, outcome } => {
                if let Some(append) = payload_by_operation
                    .get(operation)
                    .and_then(|payload| observations.appends_by_payload.get_mut(payload))
                {
                    append.outcome = Some(outcome.clone());
                }
            }
            Event::ReaderStarted {
                session,
                from_sequence,
            } => {
                next_of_session.insert(*session, *from_sequence);
            }
            Event::RecordRead {
                session,
                sequence,
                payload,
            } => {
                let expected = next_of_session
                    .get(session)
                    .copied()
                    .unwrap_or(SequenceNumber::FIRST);
                if *sequence != expected {
                    violations.push(Violation::ReaderOutOfOrder {
                        session: *session,
                        expected,
                        found: *sequence,
                    });
                }
                next_of_session.insert(*session, sequence.next());

                match observations.stream.get(sequence) {
                    Some((first_payload, first_session)) if first_payload != payload => {
                        violations.push(Violation::ReadersDisagree {
                            sequence: *sequence,
                            first_session: *first_session,
                            second_session: *session,
                        });
                    }
                    Some(_) => {}
                    None => {
                        observations
                            .stream
                            .insert(*sequence, (payload.clone(), *session));
                    }
                }
            }
            Event::TruncatedPastCheckpoint {
                checkpoint,
                low_water_mark,
            } => violations.push(Violation::TruncatedPastCheckpoint {
                checkpoint: *checkpoint,
                low_water_mark: *low_water_mark,
            }),
            Event::Fault { .. } => {}
        }
    }
    observations
}

/// What the readers saw must be explained by what the writers did: every
/// record was appended by someone, appears once, and was not refused.
fn explain_reads_by_appends(observations: &Observations, violations: &mut Vec<Violation>) {
    let mut first_sequence_of_operation: BTreeMap<u64, SequenceNumber> = BTreeMap::new();
    for (sequence, (payload, session)) in &observations.stream {
        let Some(append) = observations.appends_by_payload.get(payload) else {
            violations.push(Violation::UnknownRecord {
                session: *session,
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

/// What the writers were told must hold in what the readers saw (I1), and
/// no two appends may be told the same sequence number (I4). Returns the
/// committed appends by sequence number.
fn hold_commits_against_reads(
    observations: &Observations,
    violations: &mut Vec<Violation>,
) -> BTreeMap<SequenceNumber, (u64, Epoch)> {
    let mut committed: BTreeMap<SequenceNumber, (u64, Epoch)> = BTreeMap::new();
    for (payload, append) in &observations.appends_by_payload {
        let Some(AppendOutcome::Committed { sequence, epoch }) = &append.outcome else {
            continue;
        };
        if let Some((first_operation, _epoch)) = committed.get(sequence) {
            violations.push(Violation::SequenceCommittedTwice {
                sequence: *sequence,
                first_operation: (*first_operation).min(append.operation),
                second_operation: (*first_operation).max(append.operation),
            });
        } else {
            committed.insert(*sequence, (append.operation, *epoch));
        }
        if let Some((read_payload, session)) = observations.stream.get(sequence)
            && read_payload != payload
        {
            violations.push(Violation::CommittedRecordReplaced {
                operation: append.operation,
                sequence: *sequence,
                session: *session,
            });
        }
    }
    committed
}

/// Along the stream, the committing epoch never decreases (I4).
fn require_epochs_in_order(
    committed: &BTreeMap<SequenceNumber, (u64, Epoch)>,
    violations: &mut Vec<Violation>,
) {
    let mut newest: Option<(SequenceNumber, Epoch)> = None;
    for (sequence, (_operation, epoch)) in committed {
        match newest {
            Some((earlier_sequence, earlier_epoch)) if *epoch < earlier_epoch => {
                violations.push(Violation::EpochWentBackwards {
                    earlier_sequence,
                    earlier_epoch,
                    later_sequence: *sequence,
                    later_epoch: *epoch,
                });
            }
            _ => newest = Some((*sequence, *epoch)),
        }
    }
}

/// Checks that the reader of `audit_session`, which read the whole stream
/// after the cluster was healed, was handed every record that was ever
/// reported as committed (I1 Durability).
///
/// Records below the position the audit started at were truncated; whether
/// that was allowed is I5, which [`check`] judges.
#[must_use]
pub fn check_audit(events: &[TimedEvent], audit_session: u64) -> Vec<Violation> {
    let mut audited: BTreeMap<SequenceNumber, Bytes> = BTreeMap::new();
    let mut audited_from = SequenceNumber::FIRST;
    let mut payload_by_operation: BTreeMap<u64, Bytes> = BTreeMap::new();
    let mut committed: Vec<(u64, SequenceNumber)> = Vec::new();
    for timed in events {
        match &timed.event {
            Event::ReaderStarted {
                session,
                from_sequence,
            } if *session == audit_session => audited_from = *from_sequence,
            Event::RecordRead {
                session,
                sequence,
                payload,
            } if *session == audit_session => {
                audited.insert(*sequence, payload.clone());
            }
            Event::AppendInvoked { operation, payload } => {
                payload_by_operation.insert(*operation, payload.clone());
            }
            Event::AppendFinished {
                operation,
                outcome: AppendOutcome::Committed { sequence, .. },
            } => committed.push((*operation, *sequence)),
            _ => {}
        }
    }
    committed
        .into_iter()
        .filter(|(_operation, sequence)| *sequence >= audited_from)
        .filter(|(operation, sequence)| {
            audited.get(sequence) != payload_by_operation.get(operation)
        })
        .map(|(operation, sequence)| Violation::CommittedRecordMissing {
            operation,
            sequence,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// Builds histories by hand, the way a broken log would produce them.
    #[derive(Default)]
    struct Script {
        events: Vec<TimedEvent>,
        next_operation: u64,
    }

    impl Script {
        fn push(&mut self, event: Event) {
            self.events.push(TimedEvent {
                at: Duration::ZERO,
                event,
            });
        }

        /// An append of `payload` that ends with `outcome`.
        fn append(&mut self, payload: &'static str, outcome: Option<AppendOutcome>) -> u64 {
            self.next_operation += 1;
            let operation = self.next_operation;
            self.push(Event::AppendInvoked {
                operation,
                payload: Bytes::from(payload),
            });
            if let Some(outcome) = outcome {
                self.push(Event::AppendFinished { operation, outcome });
            }
            operation
        }

        /// A session that starts at `from_sequence` and reads `records`.
        fn read(&mut self, session: u64, from_sequence: u64, records: &[(u64, &'static str)]) {
            self.push(Event::ReaderStarted {
                session,
                from_sequence: SequenceNumber::new(from_sequence),
            });
            for (sequence, payload) in records {
                self.push(Event::RecordRead {
                    session,
                    sequence: SequenceNumber::new(*sequence),
                    payload: Bytes::from(*payload),
                });
            }
        }
    }

    fn committed(sequence: u64, epoch: u64) -> AppendOutcome {
        AppendOutcome::Committed {
            sequence: SequenceNumber::new(sequence),
            epoch: Epoch::new(epoch),
        }
    }

    /// A history of a correct log: three appends, one of unknown outcome
    /// that made it into the stream, read by two sessions.
    fn correct_history() -> Script {
        let mut script = Script::default();
        script.append("a", Some(committed(1, 1)));
        script.append("b", Some(AppendOutcome::Unknown));
        script.append("c", Some(committed(3, 2)));
        script.append("crashed before any answer", None);
        script.append("refused", Some(AppendOutcome::NotAppended));
        script.read(1, 1, &[(1, "a"), (2, "b"), (3, "c")]);
        script.read(2, 2, &[(2, "b"), (3, "c")]);
        script
    }

    #[test]
    fn correct_history_has_no_violation() {
        assert_eq!(check(&correct_history().events), Vec::new());
    }

    #[test]
    fn empty_history_has_no_violation() {
        assert_eq!(check(&[]), Vec::new());
    }

    #[test]
    fn append_of_unknown_outcome_may_be_absent_from_the_stream() {
        let mut script = Script::default();
        script.append("a", Some(committed(1, 1)));
        script.append("lost", Some(AppendOutcome::Unknown));
        script.read(1, 1, &[(1, "a")]);
        assert_eq!(check(&script.events), Vec::new());
    }

    #[test]
    fn committed_record_read_back_as_another_is_a_durability_violation() {
        let mut script = Script::default();
        let lost = script.append("a", Some(committed(1, 1)));
        script.append("b", Some(AppendOutcome::Unknown));
        script.read(7, 1, &[(1, "b")]);
        assert_eq!(
            check(&script.events),
            [Violation::CommittedRecordReplaced {
                operation: lost,
                sequence: SequenceNumber::new(1),
                session: 7,
            }]
        );
    }

    #[test]
    fn readers_that_disagree_violate_uniqueness() {
        let mut script = Script::default();
        script.append("a", Some(AppendOutcome::Unknown));
        script.append("b", Some(AppendOutcome::Unknown));
        script.read(1, 1, &[(1, "a")]);
        script.read(2, 1, &[(1, "b")]);
        assert_eq!(
            check(&script.events),
            [Violation::ReadersDisagree {
                sequence: SequenceNumber::new(1),
                first_session: 1,
                second_session: 2,
            }]
        );
    }

    #[test]
    fn reader_that_skips_a_record_violates_the_prefix_property() {
        let mut script = correct_history();
        script.read(3, 1, &[(1, "a"), (3, "c")]);
        assert_eq!(
            check(&script.events),
            [Violation::ReaderOutOfOrder {
                session: 3,
                expected: SequenceNumber::new(2),
                found: SequenceNumber::new(3),
            }]
        );
    }

    #[test]
    fn reader_handed_a_record_twice_violates_the_prefix_property() {
        let mut script = correct_history();
        script.read(3, 1, &[(1, "a"), (1, "a")]);
        assert_eq!(
            check(&script.events),
            [Violation::ReaderOutOfOrder {
                session: 3,
                expected: SequenceNumber::new(2),
                found: SequenceNumber::new(1),
            }]
        );
    }

    #[test]
    fn reader_that_starts_past_where_it_asked_violates_the_prefix_property() {
        let mut script = correct_history();
        script.read(3, 1, &[(2, "b")]);
        assert_eq!(
            check(&script.events),
            [Violation::ReaderOutOfOrder {
                session: 3,
                expected: SequenceNumber::new(1),
                found: SequenceNumber::new(2),
            }]
        );
    }

    #[test]
    fn two_commits_at_one_sequence_violate_the_single_writer_property() {
        let mut script = Script::default();
        let first = script.append("a", Some(committed(1, 1)));
        let second = script.append("b", Some(committed(1, 2)));
        assert_eq!(
            check(&script.events),
            [Violation::SequenceCommittedTwice {
                sequence: SequenceNumber::new(1),
                first_operation: first,
                second_operation: second,
            }]
        );
    }

    #[test]
    fn older_epoch_committing_after_a_newer_one_violates_the_single_writer_property() {
        let mut script = Script::default();
        script.append("a", Some(committed(1, 2)));
        script.append("b", Some(committed(2, 1)));
        assert_eq!(
            check(&script.events),
            [Violation::EpochWentBackwards {
                earlier_sequence: SequenceNumber::new(1),
                earlier_epoch: Epoch::new(2),
                later_sequence: SequenceNumber::new(2),
                later_epoch: Epoch::new(1),
            }]
        );
    }

    #[test]
    fn record_nobody_appended_is_reported() {
        let mut script = correct_history();
        script.read(3, 4, &[(4, "invented")]);
        assert_eq!(
            check(&script.events),
            [Violation::UnknownRecord {
                session: 3,
                sequence: SequenceNumber::new(4),
            }]
        );
    }

    #[test]
    fn record_stored_at_two_sequences_is_reported() {
        let mut script = Script::default();
        let duplicated = script.append("a", Some(committed(1, 1)));
        script.read(1, 1, &[(1, "a"), (2, "a")]);
        assert_eq!(
            check(&script.events),
            [Violation::RecordAppearsTwice {
                operation: duplicated,
                first_sequence: SequenceNumber::new(1),
                second_sequence: SequenceNumber::new(2),
            }]
        );
    }

    #[test]
    fn refused_record_in_the_stream_is_reported() {
        let mut script = Script::default();
        let refused = script.append("refused", Some(AppendOutcome::NotAppended));
        script.read(1, 1, &[(1, "refused")]);
        assert_eq!(
            check(&script.events),
            [Violation::RefusedRecordAppeared {
                operation: refused,
                sequence: SequenceNumber::new(1),
            }]
        );
    }

    #[test]
    fn audit_that_read_every_committed_record_passes() {
        let script = correct_history();
        assert_eq!(check_audit(&script.events, 1), Vec::new());
    }

    #[test]
    fn audit_missing_a_committed_record_is_a_durability_violation() {
        let mut script = Script::default();
        script.append("a", Some(committed(1, 1)));
        let lost = script.append("b", Some(committed(2, 1)));
        script.read(9, 1, &[(1, "a")]);
        assert_eq!(
            check_audit(&script.events, 9),
            [Violation::CommittedRecordMissing {
                operation: lost,
                sequence: SequenceNumber::new(2),
            }]
        );
    }

    #[test]
    fn audit_does_not_miss_records_truncated_before_it_started() {
        let mut script = Script::default();
        script.append("a", Some(committed(1, 1)));
        script.append("b", Some(committed(2, 1)));
        script.read(9, 2, &[(2, "b")]);
        assert_eq!(check_audit(&script.events, 9), Vec::new());
    }

    #[test]
    fn truncation_past_a_checkpoint_is_reported() {
        let mut script = Script::default();
        script.push(Event::TruncatedPastCheckpoint {
            checkpoint: SequenceNumber::new(10),
            low_water_mark: SequenceNumber::new(15),
        });
        assert_eq!(
            check(&script.events),
            [Violation::TruncatedPastCheckpoint {
                checkpoint: SequenceNumber::new(10),
                low_water_mark: SequenceNumber::new(15),
            }]
        );
    }

    #[test]
    fn audit_only_counts_what_its_own_session_read() {
        let mut script = Script::default();
        let unread_by_the_audit = script.append("a", Some(committed(1, 1)));
        script.read(1, 1, &[(1, "a")]);
        script.read(9, 1, &[]);
        assert_eq!(
            check_audit(&script.events, 9),
            [Violation::CommittedRecordMissing {
                operation: unread_by_the_audit,
                sequence: SequenceNumber::new(1),
            }]
        );
    }
}
