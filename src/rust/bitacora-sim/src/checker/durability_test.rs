//! Tests of I1 Durability.
//!
//! # What is covered
//!
//! - A committed record that readers found replaced by another is a violation; one that nobody read is not, for the
//!   checker of the run.
//! - An append of unknown outcome may be in the stream or not.
//! - The audit passes when it read every committed record, and reports each one it was not handed; it does not count
//!   records truncated before it started, nor what other sessions read.
//!
//! # How the fixtures work
//!
//! Histories are written by hand with the `Script` of the test support. In the audit tests the audit's session is
//! session 9, unless the correct history is used, whose session 1 read the whole stream.

use super::*;
use crate::checker::check;
use crate::test_support::{Script, committed, correct_history};

/// The session the audit reads in, in the scripts written here.
const AUDIT_SESSION: u64 = 9;

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
fn committed_record_read_back_as_itself_is_fine() {
    let mut script = Script::default();
    script.append("a", Some(committed(1, 1)));
    script.read(7, 1, &[(1, "a")]);

    assert_eq!(check(&script.events), Vec::new());
}

/// Readers read what they happen to read. That a committed record is still there at the end is for the audit to find
/// out.
#[test]
fn committed_record_nobody_read_is_not_reported_by_the_check_of_the_run() {
    let mut script = Script::default();
    script.append("a", Some(committed(1, 1)));

    assert_eq!(check(&script.events), Vec::new());
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
fn append_of_unknown_outcome_may_be_in_the_stream() {
    let mut script = Script::default();
    script.append("a", Some(committed(1, 1)));
    script.append("made it", Some(AppendOutcome::Unknown));
    script.read(1, 1, &[(1, "a"), (2, "made it")]);

    assert_eq!(check(&script.events), Vec::new());
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
    script.read(AUDIT_SESSION, 1, &[(1, "a")]);

    assert_eq!(
        check_audit(&script.events, AUDIT_SESSION),
        [Violation::CommittedRecordMissing {
            operation: lost,
            sequence: SequenceNumber::new(2),
        }]
    );
}

#[test]
fn audit_handed_another_record_where_one_was_committed_misses_it() {
    let mut script = Script::default();
    let replaced = script.append("a", Some(committed(1, 1)));
    script.append("b", Some(AppendOutcome::Unknown));
    script.read(AUDIT_SESSION, 1, &[(1, "b")]);

    assert_eq!(
        check_audit(&script.events, AUDIT_SESSION),
        [Violation::CommittedRecordMissing {
            operation: replaced,
            sequence: SequenceNumber::new(1),
        }]
    );
}

#[test]
fn audit_does_not_miss_records_truncated_before_it_started() {
    let mut script = Script::default();
    script.append("a", Some(committed(1, 1)));
    script.append("b", Some(committed(2, 1)));
    script.read(AUDIT_SESSION, 2, &[(2, "b")]);

    assert_eq!(check_audit(&script.events, AUDIT_SESSION), Vec::new());
}

#[test]
fn audit_only_counts_what_its_own_session_read() {
    let mut script = Script::default();
    let unread_by_the_audit = script.append("a", Some(committed(1, 1)));
    script.read(1, 1, &[(1, "a")]);
    script.read(AUDIT_SESSION, 1, &[]);

    assert_eq!(
        check_audit(&script.events, AUDIT_SESSION),
        [Violation::CommittedRecordMissing {
            operation: unread_by_the_audit,
            sequence: SequenceNumber::new(1),
        }]
    );
}

#[test]
fn audit_does_not_require_appends_of_unknown_outcome() {
    let mut script = Script::default();
    script.append("lost", Some(AppendOutcome::Unknown));
    script.append("crashed before any answer", None);
    script.read(AUDIT_SESSION, 1, &[]);

    assert_eq!(check_audit(&script.events, AUDIT_SESSION), Vec::new());
}

#[test]
fn audit_reports_missing_records_in_the_order_their_appends_finished() {
    let mut script = Script::default();
    let first_lost = script.append("b", Some(committed(2, 1)));
    let second_lost = script.append("a", Some(committed(1, 1)));
    script.read(AUDIT_SESSION, 1, &[]);

    assert_eq!(
        check_audit(&script.events, AUDIT_SESSION),
        [
            Violation::CommittedRecordMissing {
                operation: first_lost,
                sequence: SequenceNumber::new(2),
            },
            Violation::CommittedRecordMissing {
                operation: second_lost,
                sequence: SequenceNumber::new(1),
            },
        ]
    );
}
