//! Tests of the invariant predicates, each on a state built by hand.
//!
//! # What is covered
//!
//! - States that must satisfy every invariant: the initial state, and one with a record committed on a
//!   write quorum.
//! - For each invariant, the smallest states that violate it, one per way of violating it.
//! - The names the invariants are printed with.
//!
//! These tests check the predicates themselves. That the protocol never reaches a violating state, and
//! that broken rules do, is checked in `protocol_test.rs`.
//!
//! # How the fixtures work
//!
//! States are built from the model's initial state by setting fields directly, without going through the
//! transition system. `state_with_a_committed_record` is the common starting point: the first writer
//! committed sequence 1 on two replicas.

use super::*;
use crate::rules::Bounds;
use crate::state::{EMPTY_POSITION, FIRST_SEGMENT, SECOND_EPOCH, SegmentRecord};

/// The model's initial state under the bounds that leave room for every segment.
fn initial_state() -> State {
    State::initial(Bounds::ROLL_RACING_FAILOVER)
}

/// A state in which the first writer committed sequence 1 on replicas 0 and 1, which both flushed it.
fn state_with_a_committed_record() -> State {
    let mut state = initial_state();
    for replica in [0, 1] {
        state.copies[replica][FIRST_SEGMENT].local_end = 1;
        state.copies[replica][FIRST_SEGMENT].flushed_end = 1;
    }
    state.observations.record_commit(1, FIRST_EPOCH);
    state
}

/// Seals the first segment of `state` at `sealed_end` in the metadata store.
fn seal_first_segment(state: &mut State, sealed_end: Sequence) {
    state.metadata.segments[FIRST_SEGMENT] = Some(SegmentRecord {
        sealed_end: Some(sealed_end),
        ..state.segment(FIRST_SEGMENT)
    });
}

#[test]
fn initial_state_satisfies_every_invariant() {
    let state = initial_state();

    assert_eq!(violated_invariant(&state), None);
}

#[test]
fn committed_record_on_a_write_quorum_satisfies_every_invariant() {
    let state = state_with_a_committed_record();

    assert_eq!(violated_invariant(&state), None);
}

#[test]
fn committed_record_cut_off_by_a_seal_violates_durability() {
    let mut state = state_with_a_committed_record();

    seal_first_segment(&mut state, EMPTY_POSITION);

    assert_eq!(violated_invariant(&state), Some(Invariant::Durability));
}

#[test]
fn committed_record_no_replica_has_flushed_violates_durability() {
    let mut state = state_with_a_committed_record();

    for replica in [0, 1] {
        state.copies[replica][FIRST_SEGMENT].flushed_end = EMPTY_POSITION;
    }

    assert_eq!(violated_invariant(&state), Some(Invariant::Durability));
}

#[test]
fn readers_served_different_records_violate_uniqueness() {
    let mut state = initial_state();

    state.observations.breach = Some(Breach::ReadersDisagree);

    assert_eq!(violated_invariant(&state), Some(Invariant::Uniqueness));
}

#[test]
fn readable_record_cut_off_by_a_seal_violates_the_prefix_property() {
    let mut state = initial_state();
    state.copies[0][FIRST_SEGMENT].local_end = 1;
    state.copies[0][FIRST_SEGMENT].flushed_end = 1;
    state.copies[0][FIRST_SEGMENT].known_durable = 1;
    state.absorb_readable_records();

    seal_first_segment(&mut state, EMPTY_POSITION);

    assert_eq!(violated_invariant(&state), Some(Invariant::Prefix));
}

#[test]
fn segments_that_leave_a_gap_violate_the_prefix_property() {
    let mut state = initial_state();
    seal_first_segment(&mut state, 1);

    let first_sequence_after_a_gap = 3;
    state.open_segment(SECOND_EPOCH, first_sequence_after_a_gap);

    assert_eq!(violated_invariant(&state), Some(Invariant::Prefix));
}

#[test]
fn two_epochs_committing_one_sequence_violate_the_single_writer_property() {
    let mut state = state_with_a_committed_record();

    state.observations.record_commit(1, SECOND_EPOCH);

    assert_eq!(violated_invariant(&state), Some(Invariant::SingleWriter));
}

#[test]
fn old_epoch_committing_a_record_assigned_after_the_takeover_violates_single_writer() {
    let mut state = state_with_a_committed_record();

    state.observations.assigned_after_takeover[1] = true;

    assert_eq!(violated_invariant(&state), Some(Invariant::SingleWriter));
}

#[test]
fn fenced_replica_persisting_an_older_epoch_violates_single_writer() {
    let mut state = initial_state();

    state.observations.breach = Some(Breach::FencedReplicaPersistedAnOlderEpoch);

    assert_eq!(violated_invariant(&state), Some(Invariant::SingleWriter));
}

#[test]
fn invariants_are_named_as_in_the_design() {
    let invariants = [
        Invariant::Durability,
        Invariant::Uniqueness,
        Invariant::Prefix,
        Invariant::SingleWriter,
    ];

    let mut names = Vec::new();
    for invariant in invariants {
        names.push(invariant.to_string());
    }

    assert_eq!(
        names,
        ["I1 Durability", "I2 Uniqueness", "I3 Prefix", "I4 Single writer"]
    );
}
