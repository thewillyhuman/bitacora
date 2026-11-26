//! Tests of the log checker as a whole.
//!
//! # What is covered
//!
//! - A correct history, and an empty one, have no violation.
//! - Truncation past a checkpoint (I5) is reported for every event that records it.
//! - What the checker ignores: calls to the key-value store and the simulator's own notes.
//! - The order in which violations of different rules are listed.
//!
//! Each invariant is tested next to its rule, in `checker/<rule>_test.rs`.
//!
//! # How the fixtures work
//!
//! Histories are written by hand with the `Script` of the test support, the way a broken log would produce them.
//! `correct_history` is a history with appends of every outcome and two reading sessions; a test adds to it what makes
//! it wrong.

use bitacora_types::SequenceNumber;
use bytes::Bytes;

use super::*;
use crate::history::{KvCall, KvOutcome};
use crate::test_support::{Script, committed, correct_history};

#[test]
fn correct_history_has_no_violation() {
    assert_eq!(check(&correct_history().events), Vec::new());
}

#[test]
fn empty_history_has_no_violation() {
    assert_eq!(check(&[]), Vec::new());
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
fn every_truncation_past_a_checkpoint_is_reported() {
    let mut script = Script::default();
    for low_water_mark in [15, 20] {
        script.push(Event::TruncatedPastCheckpoint {
            checkpoint: SequenceNumber::new(10),
            low_water_mark: SequenceNumber::new(low_water_mark),
        });
    }

    assert_eq!(check(&script.events).len(), 2);
}

/// A stale read is a violation of the key-value store, for the linearizability check to find. The log checker must not
/// trip over the events.
#[test]
fn calls_to_the_key_value_store_are_not_judged_here() {
    let mut script = correct_history();
    script.push(Event::KvInvoked {
        operation: 100,
        call: KvCall::Get { key: Bytes::from("k") },
    });
    script.push(Event::KvReturned {
        operation: 100,
        outcome: KvOutcome::Read {
            value: Some(Bytes::from("a value nobody wrote")),
        },
    });

    assert_eq!(check(&script.events), Vec::new());
}

#[test]
fn notes_of_the_simulator_are_not_judged() {
    let mut script = correct_history();
    script.push(Event::Fault {
        description: "node 0 crashes".to_owned(),
    });

    assert_eq!(check(&script.events), Vec::new());
}

/// Violations found while the history is read come first, in the order of the events; then invented records; then
/// commits; then epochs. Reports of one history must compare equal, so the order is pinned.
#[test]
fn violations_are_listed_in_the_order_of_the_passes() {
    let mut script = Script::default();
    let newer = script.append("a", Some(committed(1, 2)));
    script.append("b", Some(committed(2, 1)));
    let contender = script.append("c", Some(committed(1, 3)));
    script.read(1, 1, &[(1, "a"), (2, "b"), (4, "invented")]);

    let violations = check(&script.events);

    assert_eq!(
        violations,
        [
            Violation::ReaderOutOfOrder {
                session: 1,
                expected: SequenceNumber::new(3),
                found: SequenceNumber::new(4),
            },
            Violation::UnknownRecord {
                session: 1,
                sequence: SequenceNumber::new(4),
            },
            Violation::SequenceCommittedTwice {
                sequence: SequenceNumber::new(1),
                first_operation: newer,
                second_operation: contender,
            },
            Violation::CommittedRecordReplaced {
                operation: contender,
                sequence: SequenceNumber::new(1),
                session: 1,
            },
            Violation::EpochWentBackwards {
                earlier_sequence: SequenceNumber::new(1),
                earlier_epoch: Epoch::new(2),
                later_sequence: SequenceNumber::new(2),
                later_epoch: Epoch::new(1),
            },
        ]
    );
}
