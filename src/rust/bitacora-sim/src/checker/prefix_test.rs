//! Tests of I3 Prefix.
//!
//! # What is covered
//!
//! - A reader that skips a record, is handed a record twice, or starts past where it asked breaks the order.
//! - One gap is reported once, not for every record after it.
//! - Sessions are judged one by one: they may start at different places.
//! - A record read in a session that was never started is expected at the first sequence number.
//!
//! # How the fixtures work
//!
//! The tests about whole histories add one reading session to the `correct_history` of the test support, whose stream
//! holds `a`, `b`, `c` at sequence numbers 1 to 3. The tests about `ReadPositions` feed it sequence numbers directly.

use super::*;
use crate::checker::check;
use crate::test_support::correct_history;

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
fn reader_that_starts_in_the_middle_and_reads_on_in_order_is_fine() {
    let mut script = correct_history();
    script.read(3, 2, &[(2, "b"), (3, "c")]);

    assert_eq!(check(&script.events), Vec::new());
}

#[test]
fn reader_that_is_handed_nothing_is_fine() {
    let mut script = correct_history();
    script.read(3, 1, &[]);

    assert_eq!(check(&script.events), Vec::new());
}

#[test]
fn one_gap_is_reported_once() {
    let mut positions = ReadPositions::default();
    positions.start_session(1, SequenceNumber::new(1));

    let mut violations = Vec::new();
    for sequence in [1, 3, 4, 5] {
        let out_of_order = positions.require_next_in_order(1, SequenceNumber::new(sequence));
        if let Some(out_of_order) = out_of_order {
            violations.push(out_of_order);
        }
    }

    assert_eq!(
        violations,
        [Violation::ReaderOutOfOrder {
            session: 1,
            expected: SequenceNumber::new(2),
            found: SequenceNumber::new(3),
        }]
    );
}

#[test]
fn sessions_keep_positions_of_their_own() {
    let mut positions = ReadPositions::default();
    positions.start_session(1, SequenceNumber::new(1));
    positions.start_session(2, SequenceNumber::new(40));

    let first = positions.require_next_in_order(1, SequenceNumber::new(1));
    let second = positions.require_next_in_order(2, SequenceNumber::new(40));

    assert_eq!([first, second], [None, None]);
}

#[test]
fn session_that_was_never_started_is_expected_at_the_first_sequence_number() {
    let mut positions = ReadPositions::default();

    let violation = positions.require_next_in_order(8, SequenceNumber::new(5));

    assert_eq!(
        violation,
        Some(Violation::ReaderOutOfOrder {
            session: 8,
            expected: SequenceNumber::FIRST,
            found: SequenceNumber::new(5),
        })
    );
}

#[test]
fn session_that_starts_again_is_expected_at_its_new_position() {
    let mut positions = ReadPositions::default();
    positions.start_session(1, SequenceNumber::new(1));
    let _ = positions.require_next_in_order(1, SequenceNumber::new(1));

    positions.start_session(1, SequenceNumber::new(30));
    let violation = positions.require_next_in_order(1, SequenceNumber::new(30));

    assert_eq!(violation, None);
}
