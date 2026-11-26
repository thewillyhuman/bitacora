//! Tests of the one decision the admin handle makes itself.
//!
//! # What is covered
//!
//! - Which truncation position is recorded when the owner asks for one: the requested position on a stream that was
//!   never truncated, and otherwise the later of the recorded and the requested one, so that it never moves back.
//!
//! Everything else the handle does is one call to the metadata store, covered where it is implemented
//! (`bitacora-meta`), and end to end in `tests/admin_test.rs`.
//!
//! # How the fixtures work
//!
//! The function under test is pure. The tests call it with sequence numbers and compare the answer.

use super::*;

#[test]
fn stream_that_was_never_truncated_records_the_requested_position() {
    let recorded = later_truncation_position(None, SequenceNumber::new(200));

    assert_eq!(recorded, SequenceNumber::new(200));
}

#[test]
fn request_past_the_recorded_position_moves_it_forward() {
    let recorded = later_truncation_position(Some(SequenceNumber::new(200)), SequenceNumber::new(500));

    assert_eq!(recorded, SequenceNumber::new(500));
}

#[test]
fn request_before_the_recorded_position_leaves_it_where_it_is() {
    let recorded = later_truncation_position(Some(SequenceNumber::new(500)), SequenceNumber::new(200));

    assert_eq!(recorded, SequenceNumber::new(500));
}

#[test]
fn request_at_the_recorded_position_leaves_it_where_it_is() {
    let recorded = later_truncation_position(Some(SequenceNumber::new(500)), SequenceNumber::new(500));

    assert_eq!(recorded, SequenceNumber::new(500));
}
