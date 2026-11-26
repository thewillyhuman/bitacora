//! Tests of I2 Uniqueness.
//!
//! # What is covered
//!
//! - Readers that are handed different records at one sequence number disagree; readers that are handed the same record
//!   do not.
//! - Every reader that differs is reported against the first one.
//! - Fillers are not exempt: readers must agree on where each one is.
//!
//! # How the fixtures work
//!
//! Histories are written by hand with the `Script` of the test support. The appends behind the records have an unknown
//! outcome, so that only this rule has something to say about them.

use super::*;
use crate::checker::check;
use crate::history::AppendOutcome;
use crate::test_support::{Script, correct_history};

/// A script with two appends of unknown outcome, `a` and `b`, either of which may be anywhere in the stream.
fn two_appends_of_unknown_outcome() -> Script {
    let mut script = Script::default();
    script.append("a", Some(AppendOutcome::Unknown));
    script.append("b", Some(AppendOutcome::Unknown));
    script
}

#[test]
fn readers_that_disagree_violate_uniqueness() {
    let mut script = two_appends_of_unknown_outcome();
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
fn readers_that_are_handed_the_same_record_agree() {
    let mut script = two_appends_of_unknown_outcome();
    script.read(1, 1, &[(1, "a")]);
    script.read(2, 1, &[(1, "a")]);

    assert_eq!(check(&script.events), Vec::new());
}

/// The first read is the reference for the whole run: a third reader that sides with the second is reported too, and
/// against the first.
#[test]
fn every_reader_that_differs_is_reported_against_the_first() {
    let mut script = two_appends_of_unknown_outcome();
    script.read(1, 1, &[(1, "a")]);
    script.read(2, 1, &[(1, "b")]);
    script.read(3, 1, &[(1, "b")]);

    assert_eq!(
        check(&script.events),
        [
            Violation::ReadersDisagree {
                sequence: SequenceNumber::new(1),
                first_session: 1,
                second_session: 2,
            },
            Violation::ReadersDisagree {
                sequence: SequenceNumber::new(1),
                first_session: 1,
                second_session: 3,
            },
        ]
    );
}

#[test]
fn readers_must_still_agree_on_where_a_filler_is() {
    let mut script = correct_history();
    script.declare_filler("barrier");
    script.read(3, 3, &[(3, "barrier")]);

    assert_eq!(
        check(&script.events),
        [Violation::ReadersDisagree {
            sequence: SequenceNumber::new(3),
            first_session: 1,
            second_session: 3,
        }]
    );
}

#[test]
fn first_read_of_a_sequence_number_is_remembered() {
    let mut stream = BTreeMap::new();

    let violation = require_agreement(&mut stream, 4, SequenceNumber::new(9), &Bytes::from("a"));

    assert_eq!(violation, None);
    assert_eq!(stream.len(), 1);
}

#[test]
fn read_that_differs_does_not_replace_the_first_read() {
    let mut stream = BTreeMap::new();
    let sequence = SequenceNumber::new(9);
    let _ = require_agreement(&mut stream, 4, sequence, &Bytes::from("a"));

    let _ = require_agreement(&mut stream, 5, sequence, &Bytes::from("b"));

    let remembered = stream.get(&sequence).expect("the first read was remembered");
    assert_eq!(remembered.payload, Bytes::from("a"));
    assert_eq!(remembered.session, 4);
}
