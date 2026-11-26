//! Tests of I4 Single writer.
//!
//! # What is covered
//!
//! - Two appends reported committed at one sequence number are a violation, named smaller identifier first.
//! - An older epoch committing after a newer one is a violation; epochs that stay or rise along the stream are not.
//! - Every commit by a stale writer is reported, each against the newest epoch seen before it.
//! - Appends that were not reported committed prove nothing.
//!
//! # How the fixtures work
//!
//! Histories are written by hand with the `Script` of the test support, and nobody reads the stream in them, so that
//! only this rule has something to say. The tests about `Commits` enter commits directly.

use super::*;
use crate::checker::check;
use crate::history::AppendOutcome;
use crate::test_support::{Script, committed};

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

/// The checker meets commits in the order of their payloads, not of their calls. The violation names them by identifier
/// all the same.
#[test]
fn contested_sequence_names_the_smaller_operation_first() {
    let mut script = Script::default();
    let earlier_call = script.append("z", Some(committed(1, 1)));
    let later_call = script.append("a", Some(committed(1, 2)));

    assert_eq!(
        check(&script.events),
        [Violation::SequenceCommittedTwice {
            sequence: SequenceNumber::new(1),
            first_operation: earlier_call,
            second_operation: later_call,
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
fn epochs_that_stay_or_rise_along_the_stream_are_fine() {
    let mut script = Script::default();
    script.append("a", Some(committed(1, 1)));
    script.append("b", Some(committed(2, 1)));
    script.append("c", Some(committed(3, 4)));

    assert_eq!(check(&script.events), Vec::new());
}

/// The order that matters is the order of the stream, not the order in which the appends returned.
#[test]
fn epochs_are_judged_in_the_order_of_the_stream() {
    let mut script = Script::default();
    script.append("returned first", Some(committed(2, 2)));
    script.append("returned second", Some(committed(1, 1)));

    assert_eq!(check(&script.events), Vec::new());
}

#[test]
fn every_commit_of_a_stale_writer_is_reported_against_the_newest_epoch() {
    let mut commits = Commits::default();
    let _ = commits.note_commit(SequenceNumber::new(1), 10, Epoch::new(3));
    let _ = commits.note_commit(SequenceNumber::new(2), 11, Epoch::new(1));
    let _ = commits.note_commit(SequenceNumber::new(3), 12, Epoch::new(2));

    let mut violations = Vec::new();
    require_epochs_in_order(&commits, &mut violations);

    assert_eq!(
        violations,
        [
            Violation::EpochWentBackwards {
                earlier_sequence: SequenceNumber::new(1),
                earlier_epoch: Epoch::new(3),
                later_sequence: SequenceNumber::new(2),
                later_epoch: Epoch::new(1),
            },
            Violation::EpochWentBackwards {
                earlier_sequence: SequenceNumber::new(1),
                earlier_epoch: Epoch::new(3),
                later_sequence: SequenceNumber::new(3),
                later_epoch: Epoch::new(2),
            },
        ]
    );
}

#[test]
fn first_commit_at_a_sequence_number_is_entered() {
    let mut commits = Commits::default();

    let violation = commits.note_commit(SequenceNumber::new(5), 10, Epoch::new(1));

    assert_eq!(violation, None);
}

#[test]
fn second_commit_at_a_sequence_number_does_not_replace_the_first() {
    let mut commits = Commits::default();
    let _ = commits.note_commit(SequenceNumber::new(5), 10, Epoch::new(1));
    let _ = commits.note_commit(SequenceNumber::new(5), 11, Epoch::new(2));

    let third = commits.note_commit(SequenceNumber::new(5), 12, Epoch::new(3));

    assert_eq!(
        third,
        Some(Violation::SequenceCommittedTwice {
            sequence: SequenceNumber::new(5),
            first_operation: 10,
            second_operation: 12,
        })
    );
}

/// A fenced writer is told its append has an unknown outcome. Whatever sequence number it had in mind proves nothing
/// about who was able to commit.
#[test]
fn appends_that_were_not_committed_are_not_held_against_each_other() {
    let mut script = Script::default();
    script.append("a", Some(committed(1, 2)));
    script.append("b", Some(AppendOutcome::Unknown));
    script.append("c", Some(AppendOutcome::NotAppended));
    script.append("d", None);

    assert_eq!(check(&script.events), Vec::new());
}
