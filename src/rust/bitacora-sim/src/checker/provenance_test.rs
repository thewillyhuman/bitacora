//! Tests of the rule that the stream holds what was appended, once.
//!
//! # What is covered
//!
//! - A record nobody appended, a record at two sequence numbers and a record whose append was refused are reported.
//! - Fillers need no append and may repeat.
//! - An append explains a record whatever its outcome, except "refused".
//!
//! # How the fixtures work
//!
//! Histories are written by hand with the `Script` of the test support. Most tests add one reading session to
//! `correct_history`, whose stream holds `a`, `b`, `c` at sequence numbers 1 to 3.

use super::*;
use crate::checker::check;
use crate::test_support::{Script, committed, correct_history};

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
fn filler_nobody_recorded_appending_is_not_reported() {
    let mut script = correct_history();
    script.declare_filler("barrier");
    script.read(3, 4, &[(4, "barrier")]);

    assert_eq!(check(&script.events), []);
}

#[test]
fn filler_may_appear_at_several_sequences() {
    let mut script = correct_history();
    script.declare_filler("barrier");
    script.read(3, 4, &[(4, "barrier"), (5, "barrier")]);

    assert_eq!(check(&script.events), []);
}

/// A payload is a filler only once it was declared one, whatever it looks like.
#[test]
fn payload_that_was_not_declared_a_filler_needs_an_append() {
    let mut script = correct_history();
    script.read(3, 4, &[(4, "barrier")]);

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
fn record_of_an_append_whose_client_crashed_is_explained() {
    let mut script = Script::default();
    script.append("the client crashed", None);
    script.read(1, 1, &[(1, "the client crashed")]);

    assert_eq!(check(&script.events), []);
}

#[test]
fn record_of_an_append_of_unknown_outcome_is_explained() {
    let mut script = Script::default();
    script.append("maybe", Some(AppendOutcome::Unknown));
    script.read(1, 1, &[(1, "maybe")]);

    assert_eq!(check(&script.events), []);
}

#[test]
fn record_read_by_several_sessions_at_one_sequence_appears_once() {
    let mut script = Script::default();
    script.append("a", Some(committed(1, 1)));
    script.read(1, 1, &[(1, "a")]);
    script.read(2, 1, &[(1, "a")]);

    assert_eq!(check(&script.events), []);
}
