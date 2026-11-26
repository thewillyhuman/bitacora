//! Tests of what a pass over a history collects.
//!
//! # What is covered
//!
//! - An append is known by its payload, without an outcome until it finishes.
//! - An outcome finds its append by the identifier of the call; an outcome without an append is ignored.
//! - Declared fillers are remembered.
//!
//! # How the fixtures work
//!
//! The tests call the noting methods directly, as the checker's pass does for each event.

use bitacora_types::Epoch;

use super::*;

/// The outcome of an append reported committed at sequence number 3 in epoch 1.
fn committed_at_3() -> AppendOutcome {
    AppendOutcome::Committed {
        sequence: SequenceNumber::new(3),
        epoch: Epoch::new(1),
    }
}

#[test]
fn append_that_began_is_known_by_its_payload() {
    let mut observations = Observations::default();

    observations.note_append_invoked(7, &Bytes::from("a"));

    let append = observations
        .appends_by_payload
        .get(&Bytes::from("a"))
        .expect("the append was noted");
    assert_eq!(append.operation, 7);
}

#[test]
fn append_has_no_outcome_until_it_finishes() {
    let mut observations = Observations::default();

    observations.note_append_invoked(7, &Bytes::from("a"));

    let append = observations
        .appends_by_payload
        .get(&Bytes::from("a"))
        .expect("the append was noted");
    assert_eq!(append.outcome, None);
}

#[test]
fn outcome_finds_its_append_by_the_identifier_of_the_call() {
    let mut observations = Observations::default();
    observations.note_append_invoked(7, &Bytes::from("a"));
    observations.note_append_invoked(8, &Bytes::from("b"));

    observations.note_append_finished(8, &committed_at_3());

    let finished = observations
        .appends_by_payload
        .get(&Bytes::from("b"))
        .expect("the append was noted");
    let unfinished = observations
        .appends_by_payload
        .get(&Bytes::from("a"))
        .expect("the append was noted");
    assert_eq!(finished.outcome, Some(committed_at_3()));
    assert_eq!(unfinished.outcome, None);
}

#[test]
fn outcome_of_a_call_that_never_began_is_ignored() {
    let mut observations = Observations::default();

    observations.note_append_finished(99, &AppendOutcome::Unknown);

    assert_eq!(observations.appends_by_payload.len(), 0);
}

#[test]
fn declared_filler_is_remembered() {
    let mut observations = Observations::default();

    observations.note_filler(&Bytes::from("barrier"));

    assert!(observations.fillers.contains(&Bytes::from("barrier")));
}

#[test]
fn new_observations_hold_nothing() {
    let observations = Observations::default();

    assert_eq!(observations.appends_by_payload.len(), 0);
    assert_eq!(observations.stream.len(), 0);
    assert_eq!(observations.fillers.len(), 0);
}
