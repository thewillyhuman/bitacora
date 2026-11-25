//! The checks of the protocol model: that the protocol holds, that the model is not vacuous, and that
//! broken rules are noticed.
//!
//! # What is covered
//!
//! - **The protocol as designed.** Three bounded configurations (failover, roll, one racing the other) are
//!   explored exhaustively and hold invariants I1 to I4. This is the exit gate of milestone M0.
//! - **The model is not vacuous.** "No violation" means nothing if the interesting states cannot be
//!   reached. So for each behaviour the protocol is about, a test shows a reachable state that has it:
//!   both writers commit, a record of unknown outcome survives or is discarded, a roll resends a record, a
//!   replica keeps a tail until it learns the seal, and so on. Two tests show states that must *not* be
//!   reachable.
//! - **Broken rules are noticed.** Each of the five deliberately broken variants of the protocol produces
//!   a counterexample, of the expected invariant.
//! - The check's own contract: a counterexample replays to a violating state, and a check that hits its
//!   state limit claims nothing.
//!
//! # How the fixtures work
//!
//! `can_reach` asks the explorer whether the protocol as designed can reach a state with a given property.
//! `run` takes a hand-written list of steps one by one and fails if a step could not happen where it is
//! taken, so a trace in a test is known to be a real run. Replicas, segments and sequence numbers are
//! small numbers: replica 0, segment 0, sequence 1.

use super::*;
use crate::state::{
    AbandonedSegment, EMPTY_POSITION, Epoch, FIRST_EPOCH, FIRST_SEGMENT, REPLICA_COUNT,
    RecoveryPhase, SECOND_EPOCH, Sequence,
};

/// More states than any bounded model below has. A check that reaches it has run away and proves nothing.
const STATE_LIMIT: usize = 5_000_000;

/// Checks that `model` holds every invariant in every reachable state, and that it explored at least
/// `at_least_states` states: a model that reaches only a handful of states proves little.
fn assert_every_invariant_holds(model: &ProtocolModel, at_least_states: usize) {
    match model.check(STATE_LIMIT) {
        CheckOutcome::Holds { states_explored } => {
            println!("{:?}: {states_explored} states explored", model.bounds);
            assert!(
                states_explored >= at_least_states,
                "only {states_explored} states were explored: the model is too small to mean much"
            );
        }
        unexpected_outcome => {
            panic!("the protocol as designed must hold, but: {unexpected_outcome:#?}")
        }
    }
}

/// The counterexample the check finds for the protocol under broken `rules`.
fn counterexample_of(rules: Rules, bounds: Bounds) -> Counterexample {
    match (ProtocolModel { rules, bounds }).check(STATE_LIMIT) {
        CheckOutcome::Violated { counterexample, .. } => counterexample,
        unexpected_outcome => {
            panic!("the broken rules {rules:?} went unnoticed: {unexpected_outcome:?}")
        }
    }
}

/// Whether the model as designed can reach a state that `is_wanted`.
fn can_reach(bounds: Bounds, is_wanted: impl Fn(&State) -> bool) -> bool {
    let outcome = search(&ProtocolModel::as_designed(bounds), STATE_LIMIT, is_wanted);
    matches!(outcome, SearchOutcome::Found { .. })
}

/// Takes the steps of `trace` one by one from the initial state of `model`, checking that each of them
/// can actually happen where it is taken.
fn run(model: &ProtocolModel, trace: &[Action]) -> State {
    let mut state = model.initial_state();
    let mut enabled = Vec::new();
    for action in trace {
        enabled.clear();
        model.enabled_actions(&state, &mut enabled);
        assert!(
            enabled.contains(action),
            "{action:?} cannot happen here; what can: {enabled:?}"
        );
        state = model.apply(&state, action);
    }
    state
}

/// Whether the recovering writer has finished the takeover and appends to its own segment.
fn is_recovering_writer_writing(state: &State) -> bool {
    matches!(state.recovering_writer.phase, RecoveryPhase::Writing(_))
}

/// Whether some replica's copy of `segment` reaches exactly `local_end`.
fn some_replica_holds_through(state: &State, segment: usize, local_end: Sequence) -> bool {
    for replica in 0..REPLICA_COUNT {
        if state.copies[replica][segment].local_end == local_end {
            return true;
        }
    }
    false
}

/// Whether no replica serves any record of `segment` to readers.
fn no_replica_serves_anything_of(state: &State, segment: usize) -> bool {
    for replica in 0..REPLICA_COUNT {
        if state.readable_end(replica, segment) != EMPTY_POSITION {
            return false;
        }
    }
    true
}

/// Whether some replica knows the seal of a segment and still holds records above the sealed end.
fn some_replica_keeps_a_tail_above_a_seal_it_knows(state: &State) -> bool {
    for segment in 0..state.segment_count() {
        let Some(sealed_end) = state.segment(segment).sealed_end else {
            continue;
        };
        for replica in 0..REPLICA_COUNT {
            let copy = state.copies[replica][segment];
            if copy.knows_seal && copy.local_end > sealed_end {
                return true;
            }
        }
    }
    false
}

/// Whether the stream has a segment of an older epoch after a segment of a newer one.
fn has_a_segment_of_an_older_epoch_after_a_newer_one(state: &State) -> bool {
    // Below every real epoch, so the first segment never looks older than what precedes it.
    let mut previous_segment_epoch: Epoch = 0;
    for segment in 0..state.segment_count() {
        let segment_epoch = state.segment(segment).epoch;
        if segment_epoch < previous_segment_epoch {
            return true;
        }
        previous_segment_epoch = segment_epoch;
    }
    false
}

// ---- the protocol as designed ----

#[test]
fn failover_at_any_point_of_two_appends_holds_every_invariant() {
    let model = ProtocolModel::as_designed(Bounds::FAILOVER);

    assert_every_invariant_holds(&model, 100_000);
}

#[test]
fn roll_at_any_point_of_three_appends_holds_every_invariant() {
    let model = ProtocolModel::as_designed(Bounds::ROLL);

    assert_every_invariant_holds(&model, 10_000);
}

#[test]
fn roll_raced_against_failover_holds_every_invariant() {
    let model = ProtocolModel::as_designed(Bounds::ROLL_RACING_FAILOVER);

    assert_every_invariant_holds(&model, 100_000);
}

// ---- the model is not vacuous: the interesting states are reachable ----

#[test]
fn first_writer_can_commit_all_its_records() {
    let is_reachable = can_reach(Bounds::FAILOVER, |state| {
        state.observations.committed[1] == Some(FIRST_EPOCH)
            && state.observations.committed[2] == Some(FIRST_EPOCH)
    });

    assert!(is_reachable);
}

#[test]
fn recovering_writer_can_commit_after_the_first_writers_records() {
    let is_reachable = can_reach(Bounds::FAILOVER, |state| {
        state.observations.committed[2] == Some(FIRST_EPOCH)
            && state.observations.committed[3] == Some(SECOND_EPOCH)
    });

    assert!(is_reachable);
}

/// Sequence 1 was never reported as committed, yet the recovery sealed it into the stream and the new
/// writer continued after it.
#[test]
fn record_of_unknown_outcome_can_survive_the_takeover() {
    let is_reachable = can_reach(Bounds::FAILOVER, |state| {
        state.observations.committed[1].is_none()
            && state.observations.committed[2] == Some(SECOND_EPOCH)
    });

    assert!(is_reachable);
}

/// The first writer assigned sequence 1, but the new writer's own record ended up there.
#[test]
fn record_of_unknown_outcome_can_be_discarded_by_the_takeover() {
    let is_reachable = can_reach(Bounds::FAILOVER, |state| {
        state.first_writer.appender.assigned_end >= 1
            && state.observations.committed[1] == Some(SECOND_EPOCH)
    });

    assert!(is_reachable);
}

/// The reason invariant I4 is stated about records assigned after the takeover: acknowledgements that
/// were on their way when the fence arrived still reach the old writer, and committing on them is safe.
#[test]
fn old_writer_may_commit_on_acknowledgements_that_predate_the_fence() {
    let model = ProtocolModel::as_designed(Bounds::FAILOVER);

    let state = run(
        &model,
        &[
            Action::FirstWriterAssigns,
            Action::ReplicaPersists {
                replica: 0,
                segment: 0,
                through: 1,
            },
            Action::ReplicaPersists {
                replica: 1,
                segment: 0,
                through: 1,
            },
            Action::RecoveringWriterClaims,
            Action::ReplicaIsFenced {
                replica: 0,
                reply_reaches_writer: true,
            },
            Action::ReplicaIsFenced {
                replica: 2,
                reply_reaches_writer: true,
            },
            Action::RecoveringWriterChoosesEnd,
            Action::RecoveringWriterRepairs { replica: 2 },
            Action::RecoveringWriterSeals,
            Action::FirstWriterLearnsAcknowledgement { replica: 0, end: 1 },
            Action::FirstWriterLearnsAcknowledgement { replica: 1, end: 1 },
        ],
    );

    assert_eq!(state.observations.committed[1], Some(FIRST_EPOCH));
    assert_eq!(state.segment(FIRST_SEGMENT).sealed_end, Some(1));
    assert_eq!(violated_invariant(&state), None);
}

#[test]
fn takeover_can_follow_a_roll_and_recover_the_second_segment() {
    let is_reachable = can_reach(Bounds::ROLL_RACING_FAILOVER, |state| {
        state.first_writer.rolled_from.is_some()
            && is_recovering_writer_writing(state)
            && state.segment_count() == 3
    });

    assert!(is_reachable);
}

#[test]
fn takeover_can_come_first_and_keep_the_old_writer_from_rolling() {
    let can_take_over_first = can_reach(Bounds::ROLL_RACING_FAILOVER, |state| {
        state.first_writer.rolled_from.is_none() && is_recovering_writer_writing(state)
    });
    // Had the old writer rolled after the takeover, the stream would have a segment of the old epoch
    // after one of the new epoch.
    let can_roll_afterwards = can_reach(
        Bounds::ROLL_RACING_FAILOVER,
        has_a_segment_of_an_older_epoch_after_a_newer_one,
    );

    assert!(can_take_over_first);
    assert!(!can_roll_afterwards);
}

/// The roll sealed the first segment after sequence 1, and sequence 2, which was already sent to the
/// first segment, then committed on the second one.
#[test]
fn roll_can_resend_an_uncommitted_record_to_the_new_segment() {
    let abandoned_after_sending_two_records = Some(AbandonedSegment {
        segment: FIRST_SEGMENT,
        offered_through: 2,
    });

    let is_reachable = can_reach(Bounds::ROLL, |state| {
        state.segment(FIRST_SEGMENT).sealed_end == Some(1)
            && state.first_writer.rolled_from == abandoned_after_sending_two_records
            && state.observations.committed[2] == Some(FIRST_EPOCH)
    });

    assert!(is_reachable);
}

#[test]
fn replica_that_misses_the_seal_can_keep_a_tail_until_it_learns_it() {
    let is_reachable = can_reach(Bounds::ROLL, |state| {
        state.segment(FIRST_SEGMENT).sealed_end == Some(1)
            && some_replica_holds_through(state, FIRST_SEGMENT, 2)
    });

    assert!(is_reachable);
}

#[test]
fn replica_that_learns_the_seal_discards_its_tail() {
    let keeps_a_tail_after_learning = can_reach(
        Bounds::ROLL,
        some_replica_keeps_a_tail_above_a_seal_it_knows,
    );

    assert!(!keeps_a_tail_after_learning);
}

#[test]
fn replica_can_restart_and_forget_what_it_may_serve() {
    let is_reachable = can_reach(Bounds::FAILOVER, |state| {
        state.observations.ever_readable[1] == Some(FIRST_EPOCH)
            && no_replica_serves_anything_of(state, FIRST_SEGMENT)
    });

    assert!(is_reachable);
}

#[test]
fn takeover_without_a_standby_segment_seals_but_cannot_open() {
    let no_standby = Bounds {
        standby_segments: 0,
        ..Bounds::FAILOVER
    };

    let can_seal = can_reach(no_standby, |state| {
        matches!(state.recovering_writer.phase, RecoveryPhase::Sealed { .. })
    });
    let can_open = can_reach(no_standby, is_recovering_writer_writing);

    assert!(can_seal);
    assert!(!can_open);
}

// ---- broken rules are noticed ----

/// The shortest way this goes wrong is the old writer committing alone, on the one replica that is not
/// fenced, after the takeover.
#[test]
fn committing_below_the_write_quorum_is_noticed() {
    let counterexample = counterexample_of(Rules::COMMITS_BELOW_THE_WRITE_QUORUM, Bounds::FAILOVER);

    assert_eq!(
        counterexample.invariant,
        Invariant::SingleWriter,
        "{counterexample:#?}"
    );
}

/// Without the single-writer clause the same mistake still costs durability: the takeover seals the
/// segment below a committed record.
#[test]
fn committing_below_the_write_quorum_can_lose_a_committed_record() {
    let model = ProtocolModel {
        rules: Rules::COMMITS_BELOW_THE_WRITE_QUORUM,
        bounds: Bounds::FAILOVER,
    };

    let state = run(
        &model,
        &[
            Action::FirstWriterAssigns,
            Action::ReplicaPersists {
                replica: 0,
                segment: 0,
                through: 1,
            },
            Action::FirstWriterLearnsAcknowledgement { replica: 0, end: 1 },
            Action::RecoveringWriterClaims,
            Action::ReplicaIsFenced {
                replica: 1,
                reply_reaches_writer: true,
            },
            Action::ReplicaIsFenced {
                replica: 2,
                reply_reaches_writer: true,
            },
            Action::RecoveringWriterChoosesEnd,
            Action::RecoveringWriterSeals,
        ],
    );

    assert_eq!(violated_invariant(&state), Some(Invariant::Durability));
}

#[test]
fn replicas_that_ignore_fencing_are_noticed() {
    let counterexample = counterexample_of(Rules::REPLICAS_IGNORE_FENCING, Bounds::FAILOVER);

    assert_eq!(
        counterexample.invariant,
        Invariant::SingleWriter,
        "{counterexample:#?}"
    );
}

#[test]
fn replicas_that_acknowledge_before_flushing_are_noticed() {
    let counterexample = counterexample_of(
        Rules::REPLICAS_ACKNOWLEDGE_BEFORE_FLUSHING,
        Bounds::FAILOVER,
    );

    assert_eq!(
        counterexample.invariant,
        Invariant::Durability,
        "{counterexample:#?}"
    );
}

#[test]
fn recovery_that_takes_the_shortest_responder_is_noticed() {
    let counterexample = counterexample_of(
        Rules::RECOVERY_TAKES_THE_SHORTEST_RESPONDER,
        Bounds::FAILOVER,
    );

    assert_eq!(
        counterexample.invariant,
        Invariant::Durability,
        "{counterexample:#?}"
    );
}

#[test]
fn recovery_that_skips_its_quorum_is_noticed() {
    let counterexample = counterexample_of(Rules::RECOVERY_SKIPS_ITS_QUORUM, Bounds::FAILOVER);

    assert_eq!(
        counterexample.invariant,
        Invariant::Durability,
        "{counterexample:#?}"
    );
}

// ---- the check's own contract ----

#[test]
fn counterexample_replays_to_a_state_that_violates_its_invariant() {
    let model = ProtocolModel {
        rules: Rules::RECOVERY_SKIPS_ITS_QUORUM,
        bounds: Bounds::FAILOVER,
    };
    let counterexample = counterexample_of(model.rules, model.bounds);

    let violating_state = model.replay(&counterexample.trace);

    assert_eq!(
        violated_invariant(&violating_state),
        Some(counterexample.invariant)
    );
}

#[test]
fn check_that_hits_its_state_limit_proves_nothing() {
    let model = ProtocolModel::as_designed(Bounds::FAILOVER);
    let state_limit = 100;

    let outcome = model.check(state_limit);

    assert_eq!(
        outcome,
        CheckOutcome::Inconclusive {
            states_explored: 100
        }
    );
}
