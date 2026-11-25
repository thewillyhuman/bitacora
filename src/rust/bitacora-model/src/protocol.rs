//! The protocol model as a whole: rules and bounds put together, and checked.
//!
//! # Why this file exists
//!
//! The other files each hold one piece: what a state is, which steps exist, what must hold, how to search.
//! Someone who wants to know whether the protocol is safe needs one question and one answer: "under these
//! rules, within these bounds, does every reachable state satisfy the invariants, and if not, how do I
//! get to one that does not?" This file is that question and that answer, and it is the crate's public
//! interface.
//!
//! # What it contains
//!
//! - [`ProtocolModel`]: the protocol under a set of rules, within some bounds.
//! - [`ProtocolModel::check`]: explores every reachable state and checks invariants I1 to I4 in each.
//! - [`CheckOutcome`]: what the check found.
//! - [`Counterexample`]: a shortest run to a violating state, with the invariant it violates.
//! - [`ProtocolModel::replay`]: takes the steps of a trace, to look at the state they lead to.
//!
//! # How it works
//!
//! [`ProtocolModel`] implements the explorer's `Model` by delegating to the other files: the initial state
//! and the fingerprint come from `state.rs`, the steps from `transitions.rs`. A check is one search for a
//! state in which `invariants.rs` finds a violation. If the search exhausts the state space, the
//! invariants hold within the bounds. If it finds a state, the trace is replayed to name the invariant.
//! If it reaches the caller's state limit first, nothing was proven, and the outcome says so and does not
//! pretend otherwise.
//!
//! # Invariants
//!
//! - A check is deterministic: the same model always gives the same outcome, the same number of states and
//!   the same counterexample.
//! - A counterexample's trace, replayed, ends in a state that violates the counterexample's invariant.
//! - [`CheckOutcome::Holds`] is only returned when every reachable state was visited.
//!
//! # See also
//!
//! - `explorer.rs`: the search.
//! - `rules.rs`: the rules and bounds a model is made of.
//! - `protocol_test.rs`: the checks themselves, and the proof that the model is not vacuous.

use crate::explorer::{Model, SearchOutcome, search};
use crate::invariants::{Invariant, violated_invariant};
use crate::rules::{Bounds, Rules};
use crate::state::State;
use crate::transitions::{self, Action};

/// The protocol under a set of rules, explored within some bounds.
///
/// Build one with [`ProtocolModel::as_designed`] to check the protocol itself, or as a struct literal with
/// one of the broken [`Rules`] to see the checker catch a mistake.
///
/// # Examples
///
/// One append, with no faults, holds every invariant:
///
/// ```
/// use bitacora_model::{Bounds, CheckOutcome, ProtocolModel};
///
/// let one_append_without_faults = Bounds {
///     first_writer_records: 1,
///     recovering_writer_records: 0,
///     standby_segments: 0,
///     first_writer_may_roll: false,
///     recovering_writer_takes_over: false,
///     replicas_may_restart: false,
/// };
/// let model = ProtocolModel::as_designed(one_append_without_faults);
///
/// let outcome = model.check(100_000);
///
/// assert!(matches!(outcome, CheckOutcome::Holds { .. }));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProtocolModel {
    /// The rules the writers and replicas follow.
    pub rules: Rules,
    /// How large a system is explored.
    pub bounds: Bounds,
}

/// A shortest run of the model that ends in a state violating an invariant.
///
/// Print it with `{:#?}` to read the run step by step, and hand its trace to [`ProtocolModel::replay`] to
/// look at the state it ends in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Counterexample {
    /// The invariant the last state violates.
    pub invariant: Invariant,
    /// The steps from the initial state to that state. No shorter run violates an invariant.
    pub trace: Vec<Action>,
}

/// What an exhaustive check of the model found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckOutcome {
    /// Every reachable state satisfies every invariant. Within the model's bounds, this is a proof.
    Holds {
        /// How many distinct states were explored. A very small number means the bounds allow too little
        /// for the result to mean much.
        states_explored: usize,
    },
    /// Some reachable state violates an invariant.
    Violated {
        /// How many distinct states were explored before it was found.
        states_explored: usize,
        /// How to get there, by a shortest run.
        counterexample: Counterexample,
    },
    /// The state limit was reached first. Nothing was proven either way.
    Inconclusive {
        /// How many distinct states were explored: the state limit.
        states_explored: usize,
    },
}

impl ProtocolModel {
    /// Builds the model of the protocol as the design specifies it.
    ///
    /// # Arguments
    ///
    /// * `bounds` - How large a system to explore.
    ///
    /// # Returns
    ///
    /// A model whose rules are [`Rules::DESIGN`].
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_model::{Bounds, ProtocolModel, Rules};
    ///
    /// let model = ProtocolModel::as_designed(Bounds::FAILOVER);
    ///
    /// assert_eq!(model.rules, Rules::DESIGN);
    /// ```
    #[must_use]
    pub const fn as_designed(bounds: Bounds) -> Self {
        Self {
            rules: Rules::DESIGN,
            bounds,
        }
    }

    /// Explores every state the model can reach and checks invariants I1 to I4 in each.
    ///
    /// The search is breadth first, so a violation is reported with a shortest run that leads to it. It
    /// is pure computation: no I/O, no threads. Memory and time grow with the number of states, which for
    /// the named bounds is around a million.
    ///
    /// # Arguments
    ///
    /// * `state_limit` - The most states to explore before giving up. It guards against bounds that make
    ///   the state space run away; a check that reaches it proves nothing.
    ///
    /// # Returns
    ///
    /// * [`CheckOutcome::Holds`] if every reachable state satisfies every invariant.
    /// * [`CheckOutcome::Violated`] with a shortest counterexample if some state does not.
    /// * [`CheckOutcome::Inconclusive`] if `state_limit` states were explored first.
    ///
    /// # Panics
    ///
    /// Panics if the model's bounds ask for more records or standby segments than a state has room for:
    /// at most 4 records in total and 2 standby segments.
    ///
    /// # Examples
    ///
    /// A recovery that proceeds before it has heard from a recovery quorum loses a committed record, and
    /// the check shows how:
    ///
    /// ```
    /// use bitacora_model::{Bounds, CheckOutcome, Invariant, ProtocolModel, Rules};
    ///
    /// let model = ProtocolModel {
    ///     rules: Rules::RECOVERY_SKIPS_ITS_QUORUM,
    ///     bounds: Bounds::FAILOVER,
    /// };
    ///
    /// let CheckOutcome::Violated { counterexample, .. } = model.check(1_000_000) else {
    ///     panic!("the broken rule went unnoticed");
    /// };
    ///
    /// assert_eq!(counterexample.invariant, Invariant::Durability);
    /// println!("{:#?}", counterexample.trace);
    /// ```
    #[must_use]
    pub fn check(&self, state_limit: usize) -> CheckOutcome {
        match search(self, state_limit, violates_an_invariant) {
            SearchOutcome::Exhausted { states_explored } => CheckOutcome::Holds { states_explored },
            SearchOutcome::LimitReached { states_explored } => {
                CheckOutcome::Inconclusive { states_explored }
            }
            SearchOutcome::Found {
                states_explored,
                trace,
            } => {
                let violating_state = self.replay(&trace);
                let invariant = violated_invariant(&violating_state)
                    .expect("the search stopped at a state that violates an invariant");
                CheckOutcome::Violated {
                    states_explored,
                    counterexample: Counterexample { invariant, trace },
                }
            }
        }
    }

    /// Takes the steps of a trace from the model's initial state and returns the state they lead to.
    ///
    /// Use it to look at the state a counterexample ends in, or to build a state for a question of your
    /// own. The steps are applied as given: nothing checks that each step could actually happen where it
    /// is taken.
    ///
    /// # Arguments
    ///
    /// * `trace` - The steps to take, first step first. An empty trace gives the initial state.
    ///
    /// # Returns
    ///
    /// The state after the last step.
    ///
    /// # Panics
    ///
    /// Panics if the model's bounds do not fit a state (see [`ProtocolModel::check`]), and may panic if
    /// the trace takes a step that cannot happen where it is taken.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_model::{Bounds, CheckOutcome, ProtocolModel, Rules};
    ///
    /// let model = ProtocolModel {
    ///     rules: Rules::REPLICAS_IGNORE_FENCING,
    ///     bounds: Bounds::FAILOVER,
    /// };
    /// let CheckOutcome::Violated { counterexample, .. } = model.check(1_000_000) else {
    ///     panic!("the broken rule went unnoticed");
    /// };
    ///
    /// let violating_state = model.replay(&counterexample.trace);
    ///
    /// println!("{violating_state:#?}");
    /// ```
    #[must_use]
    pub fn replay(&self, trace: &[Action]) -> State {
        let mut state = self.initial_state();
        for action in trace {
            state = self.apply(&state, action);
        }
        state
    }
}

/// Whether a state violates any of the invariants: what a check searches for.
fn violates_an_invariant(state: &State) -> bool {
    violated_invariant(state).is_some()
}

impl Model for ProtocolModel {
    type State = State;
    type Action = Action;
    type Fingerprint = Vec<u8>;

    /// The state the protocol starts in under this model's bounds.
    fn initial_state(&self) -> State {
        State::initial(self.bounds)
    }

    /// The state's own fingerprint, which is blind to which replica is which.
    fn fingerprint(&self, state: &State) -> Vec<u8> {
        state.fingerprint()
    }

    /// The steps the protocol allows in `state` under this model's rules and bounds.
    fn enabled_actions(&self, state: &State, actions: &mut Vec<Action>) {
        transitions::enabled_actions(state, &self.rules, self.bounds, actions);
    }

    /// The state `action` leads to under this model's rules.
    fn apply(&self, state: &State, action: &Action) -> State {
        transitions::apply(state, &self.rules, *action)
    }
}

#[cfg(test)]
#[path = "protocol_test.rs"]
mod tests;
