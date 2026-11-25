//! An executable model of bitacora's append, roll and failover protocols.
//!
//! The model is the first layer of the testing the design calls for
//! (section 15): the protocol written down as a small transition system and
//! checked exhaustively, before and alongside the implementation. It is
//! independent of the implementation crates on purpose, so that a mistake
//! in one cannot hide a mistake in the other.
//!
//! It describes one stream with three replicas and a write quorum of two, a
//! metadata store that only changes through conditional writes, a first
//! writer that appends and may roll, and a recovering writer that runs the
//! eight failover steps and then appends its own records. Messages can be
//! lost, delayed, duplicated and reordered; replicas and writers can crash
//! between any two steps.
//!
//! [`ProtocolModel::check`] explores every reachable state and checks
//! invariants I1 to I4 in each. [`Rules`] other than [`Rules::DESIGN`]
//! break the protocol on purpose; the tests use them to show that the
//! checker finds a counterexample when there is one.

mod explorer;
mod invariants;
mod rules;
mod state;
mod transitions;

pub use explorer::{Model, SearchOutcome, search};
pub use invariants::Invariant;
pub use rules::{Bounds, RecoveryEnd, Rules};
pub use state::{
    Epoch, FIRST_EPOCH, RECOVERY_QUORUM, REPLICA_COUNT, SECOND_EPOCH, Sequence, State, WRITE_QUORUM,
};
pub use transitions::Action;

/// The protocol under a set of rules, explored within some bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProtocolModel {
    /// The rules the writers and replicas follow.
    pub rules: Rules,
    /// How large a system is explored.
    pub bounds: Bounds,
}

/// A shortest run of the model that ends in a state violating an invariant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Counterexample {
    /// The invariant the last state violates.
    pub invariant: Invariant,
    /// The steps from the initial state to that state.
    pub trace: Vec<Action>,
}

/// What an exhaustive check of the model found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckOutcome {
    /// Every reachable state satisfies every invariant.
    Holds {
        /// How many distinct states were explored.
        states_explored: usize,
    },
    /// Some reachable state violates an invariant.
    Violated {
        /// How many distinct states were explored before it was found.
        states_explored: usize,
        /// How to get there.
        counterexample: Counterexample,
    },
    /// The state limit was reached first. Nothing was proven.
    Inconclusive {
        /// How many distinct states were explored.
        states_explored: usize,
    },
}

impl ProtocolModel {
    /// The protocol as designed, within `bounds`.
    #[must_use]
    pub const fn as_designed(bounds: Bounds) -> Self {
        Self {
            rules: Rules::DESIGN,
            bounds,
        }
    }

    /// Explores every state the model can reach, up to `state_limit`
    /// states, and checks invariants I1 to I4 in each.
    #[must_use]
    pub fn check(&self, state_limit: usize) -> CheckOutcome {
        let outcome = search(self, state_limit, |state| {
            invariants::violated_invariant(state).is_some()
        });
        match outcome {
            SearchOutcome::Exhausted { states_explored } => CheckOutcome::Holds { states_explored },
            SearchOutcome::LimitReached { states_explored } => {
                CheckOutcome::Inconclusive { states_explored }
            }
            SearchOutcome::Found {
                states_explored,
                trace,
            } => {
                let violating_state = self.replay(&trace);
                let invariant = invariants::violated_invariant(&violating_state)
                    .expect("the search stopped at a state that violates an invariant");
                CheckOutcome::Violated {
                    states_explored,
                    counterexample: Counterexample { invariant, trace },
                }
            }
        }
    }

    /// Returns the state the model is in after taking the steps of `trace`
    /// from its initial state.
    #[must_use]
    pub fn replay(&self, trace: &[Action]) -> State {
        trace.iter().fold(self.initial_state(), |state, action| {
            self.apply(&state, action)
        })
    }
}

impl Model for ProtocolModel {
    type State = State;
    type Action = Action;
    type Fingerprint = Vec<u8>;

    fn initial_state(&self) -> State {
        State::initial(self.bounds)
    }

    fn fingerprint(&self, state: &State) -> Vec<u8> {
        state.fingerprint()
    }

    fn enabled_actions(&self, state: &State, actions: &mut Vec<Action>) {
        transitions::enabled_actions(state, &self.rules, self.bounds, actions);
    }

    fn apply(&self, state: &State, action: &Action) -> State {
        transitions::apply(state, &self.rules, *action)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::RecoveryPhase;

    /// More states than any bounded model below has. A check that reaches
    /// it has run away and proves nothing.
    const STATE_LIMIT: usize = 5_000_000;

    fn assert_holds(model: &ProtocolModel, at_least_states: usize) {
        match model.check(STATE_LIMIT) {
            CheckOutcome::Holds { states_explored } => {
                println!("{:?}: {states_explored} states explored", model.bounds);
                assert!(
                    states_explored >= at_least_states,
                    "only {states_explored} states were explored: the model is too small to mean much"
                );
            }
            other => panic!("the protocol as designed must hold, but: {other:#?}"),
        }
    }

    fn counterexample_of(rules: Rules, bounds: Bounds) -> Counterexample {
        match (ProtocolModel { rules, bounds }).check(STATE_LIMIT) {
            CheckOutcome::Violated { counterexample, .. } => counterexample,
            other => panic!("the broken rules {rules:?} went unnoticed: {other:?}"),
        }
    }

    /// Whether the model as designed can reach a state that `is_wanted`.
    fn can_reach(bounds: Bounds, is_wanted: impl Fn(&State) -> bool) -> bool {
        matches!(
            search(&ProtocolModel::as_designed(bounds), STATE_LIMIT, is_wanted),
            SearchOutcome::Found { .. }
        )
    }

    // ---- the protocol as designed ----

    #[test]
    fn failover_at_any_point_of_two_appends_holds_every_invariant() {
        assert_holds(&ProtocolModel::as_designed(Bounds::FAILOVER), 100_000);
    }

    #[test]
    fn roll_at_any_point_of_three_appends_holds_every_invariant() {
        assert_holds(&ProtocolModel::as_designed(Bounds::ROLL), 10_000);
    }

    #[test]
    fn roll_raced_against_failover_holds_every_invariant() {
        assert_holds(
            &ProtocolModel::as_designed(Bounds::ROLL_RACING_FAILOVER),
            100_000,
        );
    }

    // ---- the model is not vacuous: the interesting states are reachable ----

    #[test]
    fn first_writer_can_commit_all_its_records() {
        assert!(can_reach(Bounds::FAILOVER, |state| {
            state.observations.committed[1] == Some(FIRST_EPOCH)
                && state.observations.committed[2] == Some(FIRST_EPOCH)
        }));
    }

    #[test]
    fn recovering_writer_can_commit_after_the_first_writers_records() {
        assert!(can_reach(Bounds::FAILOVER, |state| {
            state.observations.committed[2] == Some(FIRST_EPOCH)
                && state.observations.committed[3] == Some(SECOND_EPOCH)
        }));
    }

    #[test]
    fn record_of_unknown_outcome_can_survive_the_takeover() {
        // Sequence 1 was never reported as committed, yet the recovery
        // sealed it into the stream and the new writer continued after it.
        assert!(can_reach(Bounds::FAILOVER, |state| {
            state.observations.committed[1].is_none()
                && state.observations.committed[2] == Some(SECOND_EPOCH)
        }));
    }

    #[test]
    fn record_of_unknown_outcome_can_be_discarded_by_the_takeover() {
        // The first writer assigned sequence 1, but the new writer's own
        // record ended up there.
        assert!(can_reach(Bounds::FAILOVER, |state| {
            state.first_writer.appender.assigned_end >= 1
                && state.observations.committed[1] == Some(SECOND_EPOCH)
        }));
    }

    /// Takes the steps of `trace` one by one, checking that each of them
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

    /// The reason invariant I4 is stated about records assigned after the
    /// takeover: acknowledgements that were on their way when the fence
    /// arrived still reach the old writer, and committing on them is safe.
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
        assert_eq!(
            (
                state.observations.committed[1],
                state.segment(0).sealed_end,
                invariants::violated_invariant(&state)
            ),
            (Some(FIRST_EPOCH), Some(1), None)
        );
    }

    #[test]
    fn takeover_can_follow_a_roll_and_recover_the_second_segment() {
        assert!(can_reach(Bounds::ROLL_RACING_FAILOVER, |state| {
            state.first_writer.rolled_from.is_some()
                && matches!(state.recovering_writer.phase, RecoveryPhase::Writing(_))
                && state.segments().count() == 3
        }));
    }

    #[test]
    fn takeover_can_come_first_and_keep_the_old_writer_from_rolling() {
        let can_take_over_first = can_reach(Bounds::ROLL_RACING_FAILOVER, |state| {
            state.first_writer.rolled_from.is_none()
                && matches!(state.recovering_writer.phase, RecoveryPhase::Writing(_))
        });
        // Had the old writer rolled after the takeover, the stream would
        // have a segment of the old epoch after one of the new epoch.
        let can_roll_afterwards = can_reach(Bounds::ROLL_RACING_FAILOVER, |state| {
            let epochs: Vec<Epoch> = state.segments().map(|(_, segment)| segment.epoch).collect();
            epochs.windows(2).any(|pair| pair[0] > pair[1])
        });
        assert!(can_take_over_first && !can_roll_afterwards);
    }

    #[test]
    fn roll_can_resend_an_uncommitted_record_to_the_new_segment() {
        // The roll sealed the first segment after sequence 1, and sequence
        // 2, which was already sent to the first segment, then committed on
        // the second one.
        assert!(can_reach(Bounds::ROLL, |state| {
            state.segment(0).sealed_end == Some(1)
                && state.first_writer.rolled_from == Some((0, 2))
                && state.observations.committed[2] == Some(FIRST_EPOCH)
        }));
    }

    #[test]
    fn replica_that_misses_the_seal_can_keep_a_tail_until_it_learns_it() {
        assert!(can_reach(Bounds::ROLL, |state| {
            state.segment(0).sealed_end == Some(1)
                && (0..REPLICA_COUNT).any(|replica| state.copies[replica][0].local_end == 2)
        }));
    }

    #[test]
    fn replica_that_learns_the_seal_discards_its_tail() {
        let keeps_a_tail_after_learning = can_reach(Bounds::ROLL, |state| {
            state.segments().any(|(index, segment)| {
                segment.sealed_end.is_some_and(|sealed_end| {
                    (0..REPLICA_COUNT).any(|replica| {
                        let copy = state.copies[replica][index];
                        copy.knows_seal && copy.local_end > sealed_end
                    })
                })
            })
        });
        assert!(!keeps_a_tail_after_learning);
    }

    #[test]
    fn replica_can_restart_and_forget_what_it_may_serve() {
        assert!(can_reach(Bounds::FAILOVER, |state| {
            state.observations.ever_readable[1] == Some(FIRST_EPOCH)
                && (0..REPLICA_COUNT).all(|replica| state.readable_end(replica, 0) == 0)
        }));
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
        let can_open = can_reach(no_standby, |state| {
            matches!(state.recovering_writer.phase, RecoveryPhase::Writing(_))
        });
        assert!(can_seal && !can_open);
    }

    // ---- broken rules are noticed ----

    /// The shortest way this goes wrong is the old writer committing alone,
    /// on the one replica that is not fenced, after the takeover.
    #[test]
    fn committing_below_the_write_quorum_is_noticed() {
        let counterexample =
            counterexample_of(Rules::COMMITS_BELOW_THE_WRITE_QUORUM, Bounds::FAILOVER);
        assert_eq!(
            counterexample.invariant,
            Invariant::SingleWriter,
            "{counterexample:#?}"
        );
    }

    /// Without the single-writer clause the same mistake still costs
    /// durability: the takeover seals the segment below a committed record.
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
        assert_eq!(
            invariants::violated_invariant(&state),
            Some(Invariant::Durability)
        );
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

    #[test]
    fn counterexample_replays_to_a_state_that_violates_its_invariant() {
        let model = ProtocolModel {
            rules: Rules::RECOVERY_SKIPS_ITS_QUORUM,
            bounds: Bounds::FAILOVER,
        };
        let counterexample = counterexample_of(model.rules, model.bounds);
        let violating_state = model.replay(&counterexample.trace);
        assert_eq!(
            invariants::violated_invariant(&violating_state),
            Some(counterexample.invariant)
        );
    }

    #[test]
    fn check_that_hits_its_state_limit_proves_nothing() {
        let outcome = ProtocolModel::as_designed(Bounds::FAILOVER).check(100);
        assert_eq!(
            outcome,
            CheckOutcome::Inconclusive {
                states_explored: 100
            }
        );
    }
}
