//! Tests of the identifiers and positions.
//!
//! # What is covered
//!
//! - Positions: where numbering starts, moving forward and back, counting records between two positions,
//!   and failing fast at the end of the number space.
//! - Epochs and segment identifiers: what "next" is.
//! - How every identifier is displayed and serialized, because both forms are read by other programs.
//! - Replica identifiers: what is accepted, each way of being refused, and that a stored identifier is
//!   checked when it is read.
//!
//! # How the fixtures work
//!
//! There are none: every test builds the one or two values it needs.

use super::*;

#[test]
fn first_record_follows_the_empty_position() {
    let after_the_empty_position = SequenceNumber::ZERO.next();

    assert_eq!(after_the_empty_position, SequenceNumber::FIRST);
}

#[test]
fn advancing_moves_forward_by_the_record_count() {
    let advanced = SequenceNumber::new(10).advanced_by(5);

    assert_eq!(advanced, SequenceNumber::new(15));
}

#[test]
fn advancing_by_no_records_stays_in_place() {
    let advanced = SequenceNumber::new(10).advanced_by(0);

    assert_eq!(advanced, SequenceNumber::new(10));
}

#[test]
fn position_before_the_first_record_is_the_empty_position() {
    let before_the_first_record = SequenceNumber::FIRST.preceding();

    assert_eq!(before_the_first_record, SequenceNumber::ZERO);
}

#[test]
fn position_before_the_empty_position_stays_empty() {
    let before_the_empty_position = SequenceNumber::ZERO.preceding();

    assert_eq!(before_the_empty_position, SequenceNumber::ZERO);
}

#[test]
fn records_since_counts_the_records_in_between() {
    let later = SequenceNumber::new(12);

    let record_count = later.records_since(SequenceNumber::new(9));

    assert_eq!(record_count, 3);
}

#[test]
fn records_since_a_later_position_is_zero() {
    let earlier = SequenceNumber::new(9);

    let record_count = earlier.records_since(SequenceNumber::new(12));

    assert_eq!(record_count, 0);
}

#[test]
#[should_panic(expected = "sequence numbers exhausted")]
fn advancing_past_the_end_of_the_sequence_space_fails_fast() {
    let _wrapped_around = SequenceNumber::new(u64::MAX).next();
}

#[test]
fn sequence_numbers_order_by_position() {
    assert!(SequenceNumber::new(2) > SequenceNumber::new(1));
}

#[test]
fn new_writer_claims_the_following_epoch() {
    let claimed = Epoch::ZERO.next();

    assert_eq!(claimed, Epoch::new(1));
}

#[test]
#[should_panic(expected = "epochs exhausted")]
fn claiming_an_epoch_past_the_end_of_the_epoch_space_fails_fast() {
    let _wrapped_around = Epoch::new(u64::MAX).next();
}

#[test]
fn segment_after_the_first_is_the_second() {
    let second_segment = SegmentId::FIRST.next();

    assert_eq!(second_segment, SegmentId::new(2));
}

#[test]
#[should_panic(expected = "segment identifiers exhausted")]
fn opening_a_segment_past_the_end_of_the_identifier_space_fails_fast() {
    let _wrapped_around = SegmentId::new(u64::MAX).next();
}

#[test]
fn stream_identifier_displays_as_a_plain_number() {
    assert_eq!(StreamId::new(7).to_string(), "7");
}

#[test]
fn segment_identifier_displays_as_a_plain_number() {
    assert_eq!(SegmentId::new(3).to_string(), "3");
}

#[test]
fn epoch_displays_as_a_plain_number() {
    assert_eq!(Epoch::new(2).to_string(), "2");
}

#[test]
fn sequence_number_displays_as_a_plain_number() {
    assert_eq!(SequenceNumber::new(41).to_string(), "41");
}

#[test]
fn segment_address_displays_as_stream_then_segment() {
    let address = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));

    assert_eq!(address.to_string(), "7/3");
}

#[test]
fn positions_serialize_as_plain_numbers() {
    let serialized = serde_json::to_string(&SequenceNumber::new(41)).expect("a number serializes");

    assert_eq!(serialized, "41");
}

#[test]
fn replica_id_accepts_host_like_names() {
    let replica_id = ReplicaId::new("replica-0.zone_a").expect("a host-like name is a valid identifier");

    assert_eq!(replica_id.as_str(), "replica-0.zone_a");
}

#[test]
fn replica_id_displays_as_its_text() {
    let replica_id = ReplicaId::new("replica-a").expect("a valid identifier");

    assert_eq!(replica_id.to_string(), "replica-a");
}

#[test]
fn replica_id_rejects_empty_text() {
    let refused = ReplicaId::new("");

    assert_eq!(refused, Err(InvalidReplicaId::Empty));
}

#[test]
fn replica_id_rejects_path_separators() {
    let refused = ReplicaId::new("zone/a");

    assert_eq!(refused, Err(InvalidReplicaId::ForbiddenCharacter { character: '/' }));
}

#[test]
fn replica_id_accepts_text_of_exactly_the_longest_length() {
    let longest = "r".repeat(ReplicaId::MAX_LENGTH_BYTES);

    let accepted = ReplicaId::new(longest);

    assert!(accepted.is_ok(), "{accepted:?}");
}

#[test]
fn replica_id_rejects_overlong_text() {
    let overlong = "r".repeat(ReplicaId::MAX_LENGTH_BYTES + 1);

    let refused = ReplicaId::new(overlong);

    assert_eq!(
        refused,
        Err(InvalidReplicaId::TooLong {
            length_bytes: ReplicaId::MAX_LENGTH_BYTES + 1,
            limit_bytes: ReplicaId::MAX_LENGTH_BYTES,
        })
    );
}

#[test]
fn replica_id_is_validated_when_deserialized() {
    let deserialized = serde_json::from_str::<ReplicaId>("\"zone/a\"");

    assert!(deserialized.is_err());
}

#[test]
fn replica_id_serializes_as_its_text() {
    let replica_id = ReplicaId::new("replica-a").expect("a valid identifier");

    let serialized = serde_json::to_string(&replica_id).expect("text serializes");

    assert_eq!(serialized, "\"replica-a\"");
}
