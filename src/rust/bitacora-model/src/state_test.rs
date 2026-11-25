//! Tests of the model's state: what it starts as, what its queries answer, and what its fingerprint tells
//! apart.
//!
//! # What is covered
//!
//! - The initial state: one open, empty first segment of the first epoch.
//! - `SegmentRecord::contains` for open and sealed segments.
//! - What a replica serves to readers before and after it learns a seal, and that two replicas serving
//!   different records is noticed.
//! - `stream_holds`: a flushed record of the right segment counts, one cut off by a seal does not.
//! - The fingerprint: blind to which replica is which, but not to anything else, including which replica
//!   a writer's belief is about.
//!
//! # How the fixtures work
//!
//! Tests start from `initial_state`, the model's initial state under the largest bounds, and set the
//! fields they need by hand. That builds states the transition system might never reach, which is the
//! point: these functions are checked on their own, not through the protocol.

use super::*;

/// The model's initial state under the bounds that leave room for every segment.
fn initial_state() -> State {
    State::initial(Bounds::ROLL_RACING_FAILOVER)
}

/// Seals the first segment of `state` at `sealed_end` in the metadata store, without telling any replica.
fn seal_first_segment(state: &mut State, sealed_end: Sequence) {
    state.metadata.segments[FIRST_SEGMENT] = Some(SegmentRecord {
        sealed_end: Some(sealed_end),
        ..state.segment(FIRST_SEGMENT)
    });
}

#[test]
fn model_starts_with_one_segment() {
    let state = initial_state();

    assert_eq!(state.segment_count(), 1);
}

#[test]
fn first_segment_starts_open_and_empty_and_belongs_to_the_first_epoch() {
    let state = initial_state();

    assert_eq!(
        state.segment(FIRST_SEGMENT),
        SegmentRecord {
            epoch: FIRST_EPOCH,
            first_sequence: 1,
            sealed_end: None,
        }
    );
}

#[test]
fn open_segment_does_not_contain_what_precedes_its_start() {
    let open_segment = SegmentRecord {
        epoch: FIRST_EPOCH,
        first_sequence: 2,
        sealed_end: None,
    };

    assert!(!open_segment.contains(1));
}

#[test]
fn open_segment_contains_everything_from_its_start() {
    let open_segment = SegmentRecord {
        epoch: FIRST_EPOCH,
        first_sequence: 2,
        sealed_end: None,
    };

    assert!(open_segment.contains(2));
    assert!(open_segment.contains(4));
}

#[test]
fn sealed_segment_contains_exactly_its_range() {
    let sealed_segment = SegmentRecord {
        epoch: FIRST_EPOCH,
        first_sequence: 2,
        sealed_end: Some(3),
    };

    let mut contained = Vec::new();
    for sequence in 1..=4 {
        contained.push(sealed_segment.contains(sequence));
    }

    assert_eq!(contained, [false, true, true, false]);
}

#[test]
fn replica_serves_an_open_segment_only_up_to_the_durable_sequence_it_knows() {
    let mut state = initial_state();
    state.copies[0][FIRST_SEGMENT].local_end = 2;
    state.copies[0][FIRST_SEGMENT].known_durable = 1;

    assert_eq!(state.readable_end(0, FIRST_SEGMENT), 1);
}

#[test]
fn replica_that_knows_the_seal_serves_its_whole_copy() {
    let mut state = initial_state();
    state.copies[0][FIRST_SEGMENT].local_end = 2;
    state.copies[0][FIRST_SEGMENT].knows_seal = true;

    assert_eq!(state.readable_end(0, FIRST_SEGMENT), 2);
}

#[test]
fn readers_that_are_served_different_records_are_noticed() {
    let mut state = initial_state();
    seal_first_segment(&mut state, EMPTY_POSITION);
    let second_segment = state.open_segment(SECOND_EPOCH, 1);
    // One replica still serves the first writer's record at sequence 1, another serves the second
    // writer's record at the same sequence.
    state.copies[0][FIRST_SEGMENT].local_end = 1;
    state.copies[0][FIRST_SEGMENT].known_durable = 1;
    state.copies[1][second_segment].local_end = 1;
    state.copies[1][second_segment].known_durable = 1;

    state.absorb_readable_records();

    assert_eq!(state.observations.breach, Some(Breach::ReadersDisagree));
}

#[test]
fn stream_holds_a_record_that_a_replica_has_flushed_in_its_segment() {
    let mut state = initial_state();
    state.copies[2][FIRST_SEGMENT].flushed_end = 1;

    assert!(state.stream_holds(1, FIRST_EPOCH));
}

#[test]
fn stream_does_not_hold_a_record_cut_off_by_a_seal() {
    let mut state = initial_state();
    state.copies[2][FIRST_SEGMENT].flushed_end = 1;

    seal_first_segment(&mut state, EMPTY_POSITION);

    assert!(!state.stream_holds(1, FIRST_EPOCH));
}

#[test]
fn states_that_differ_only_in_replica_order_have_one_fingerprint() {
    let mut one_order = initial_state();
    one_order.copies[0][FIRST_SEGMENT].local_end = 2;
    one_order.first_writer.appender.acknowledged_ends[0] = 1;
    let mut other_order = initial_state();
    other_order.copies[2][FIRST_SEGMENT].local_end = 2;
    other_order.first_writer.appender.acknowledged_ends[2] = 1;

    assert_eq!(one_order.fingerprint(), other_order.fingerprint());
}

#[test]
fn states_that_differ_in_what_a_replica_holds_have_different_fingerprints() {
    let mut longer_copy = initial_state();
    longer_copy.copies[0][FIRST_SEGMENT].local_end = 2;

    assert_ne!(initial_state().fingerprint(), longer_copy.fingerprint());
}

/// Sorting the replicas must not separate a replica's copy from what a writer believes about that same
/// replica: here the same copy and the same belief exist, once on one replica and once on two.
#[test]
fn states_that_pair_copies_and_acknowledgements_differently_differ() {
    let mut same_replica = initial_state();
    same_replica.copies[0][FIRST_SEGMENT].local_end = 2;
    same_replica.first_writer.appender.acknowledged_ends[0] = 1;
    let mut different_replicas = initial_state();
    different_replicas.copies[0][FIRST_SEGMENT].local_end = 2;
    different_replicas.first_writer.appender.acknowledged_ends[1] = 1;

    assert_ne!(same_replica.fingerprint(), different_replicas.fingerprint());
}
