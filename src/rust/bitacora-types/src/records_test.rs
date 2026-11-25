//! Tests of ranges of records.
//!
//! # What is covered
//!
//! - Counting and membership: both ends belong to a range, their neighbours do not.
//! - The two ways two positions fail to be a range, in code and when deserialized.
//! - How a range is displayed and that it survives serialization.
//!
//! # How the fixtures work
//!
//! `range(first, last)` builds a valid range from two plain numbers. Most tests use the range 5 to 9.

use super::*;

/// The range from `first` to `last`, both included. Panics if the two do not form a range.
fn range(first: u64, last: u64) -> SequenceRange {
    SequenceRange::new(SequenceNumber::new(first), SequenceNumber::new(last))
        .expect("the test names a first position that is not after the last")
}

#[test]
fn range_of_one_record_counts_one() {
    assert_eq!(range(5, 5).record_count(), 1);
}

#[test]
fn range_counts_both_ends() {
    assert_eq!(range(5, 9).record_count(), 5);
}

#[test]
fn range_reports_its_first_position() {
    assert_eq!(range(5, 9).first(), SequenceNumber::new(5));
}

#[test]
fn range_reports_its_last_position() {
    assert_eq!(range(5, 9).last(), SequenceNumber::new(9));
}

#[test]
fn range_contains_its_first_position() {
    assert!(range(5, 9).contains(SequenceNumber::new(5)));
}

#[test]
fn range_contains_its_last_position() {
    assert!(range(5, 9).contains(SequenceNumber::new(9)));
}

#[test]
fn range_excludes_the_position_before_it() {
    assert!(!range(5, 9).contains(SequenceNumber::new(4)));
}

#[test]
fn range_excludes_the_position_after_it() {
    assert!(!range(5, 9).contains(SequenceNumber::new(10)));
}

#[test]
fn range_cannot_start_at_the_empty_position() {
    let refused = SequenceRange::new(SequenceNumber::ZERO, SequenceNumber::new(3));

    assert_eq!(refused, Err(InvalidSequenceRange::StartsAtEmptyPosition));
}

#[test]
fn range_cannot_end_before_it_starts() {
    let first = SequenceNumber::new(9);
    let last = SequenceNumber::new(5);

    let refused = SequenceRange::new(first, last);

    assert_eq!(refused, Err(InvalidSequenceRange::EndsBeforeStart { first, last }));
}

#[test]
fn range_displays_as_an_inclusive_interval() {
    assert_eq!(range(5, 9).to_string(), "5..=9");
}

#[test]
fn range_is_validated_when_deserialized() {
    let deserialized = serde_json::from_str::<SequenceRange>(r#"{"first":9,"last":5}"#);

    assert!(deserialized.is_err());
}

#[test]
fn range_serializes_as_its_two_ends() {
    let serialized = serde_json::to_string(&range(5, 9)).expect("two numbers serialize");

    assert_eq!(serialized, r#"{"first":5,"last":9}"#);
}

#[test]
fn range_survives_a_serialization_round_trip() {
    let serialized = serde_json::to_string(&range(5, 9)).expect("two numbers serialize");

    let deserialized: SequenceRange = serde_json::from_str(&serialized).expect("what was just written is valid");

    assert_eq!(deserialized, range(5, 9));
}

/// The limit is part of the protocol: replicas, readers and clients in other languages size their buffers
/// by it. Changing it is a protocol change, not a tuning knob.
#[test]
fn largest_record_payload_is_four_mebibytes() {
    assert_eq!(MAX_RECORD_PAYLOAD_BYTES, 4_194_304);
}
