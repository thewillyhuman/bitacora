//! A small explicit-state model checker.
//!
//! # Why this file exists
//!
//! The protocol model is only useful if every state it can reach is actually looked at. Tools exist for
//! that (TLA+ with TLC, Stateright), but each brings a second language or a dozen dependencies, threads
//! and a random source, into a workspace that keeps all of those out for the sake of determinism
//! (decision record `docs/decisions/0001-protocol-model.md`). What the model needs from a checker is one
//! breadth-first search, which is this file.
//!
//! # What it contains
//!
//! - `Model`: what a system has to provide to be explored: an initial state, the steps possible in a
//!   state, what a step leads to, and what identifies a state.
//! - `search`: the breadth-first search over a model for a state that matches a predicate.
//! - `SearchOutcome`: how a search ended.
//!
//! # How it works
//!
//! The search keeps three things: the set of fingerprints of every state discovered so far, a queue of
//! discovered states whose steps have not been tried yet, and for every discovered state the state it was
//! first reached from and the step that led there.
//!
//! It takes the oldest state from the queue, tries every step possible in it, and for each resulting state
//! that was not discovered before, records where it came from, checks it against the predicate, and puts
//! it in the queue. Because the queue is first in, first out, states are discovered in order of their
//! distance from the initial state. The first state that matches is therefore one of the nearest, and
//! walking back through "where it came from" gives a shortest run to it. A shortest counterexample is the
//! most readable one.
//!
//! A state is identified by its fingerprint, which the model defines, and not by a hash of it: two
//! different fingerprints are never confused. The model may deliberately give two states the same
//! fingerprint when they only differ by a symmetry.
//!
//! `Model` is a trait with one implementation in the crate, the protocol. It is a trait all the same
//! because the search has a second implementation to run on in its own tests: a toy system whose states can
//! be counted by hand, which is the only way to test a checker independently of what it checks.
//!
//! Nothing here is public outside the crate. The crate's interface is `ProtocolModel::check`, which names
//! the one thing callers search for: a state that violates an invariant.
//!
//! # Invariants
//!
//! - The search is deterministic: the same model is explored in the same order every time. It uses no
//!   threads, no hashing and no randomness.
//! - A state is tried once. The number of states explored is the number of distinct fingerprints.
//! - A trace returned for a match is a shortest one.
//! - `search` is the hot loop of the crate: it runs a few million times per check. It is one function on
//!   purpose, generic over the model and the predicate so that both are inlined, and it reuses one vector
//!   for the steps of every state.
//!
//! # See also
//!
//! - `protocol.rs`: the model that is searched, and the public entry point.
//! - `state.rs`: the protocol's fingerprint.

use std::collections::{BTreeSet, VecDeque};
use std::fmt::Debug;

/// A system described as states and the actions that lead from one to the next.
///
/// Implement it to make a system explorable by [`search`]. The explorer never looks inside a state or an
/// action; it only asks these four questions.
pub(crate) trait Model {
    /// One configuration of the whole system.
    type State: Clone;
    /// One step some part of the system can take.
    type Action: Clone + Debug;
    /// What identifies a state. Two states with the same fingerprint are explored once, so it must capture
    /// everything that can influence the future; it may deliberately equate states that only differ by a
    /// symmetry, such as which of three identical replicas is which.
    type Fingerprint: Ord;

    /// Returns the state the system starts in.
    fn initial_state(&self) -> Self::State;

    /// Returns what identifies `state` among all states of the system.
    fn fingerprint(&self, state: &Self::State) -> Self::Fingerprint;

    /// Appends to `actions` every action that can happen in `state`.
    ///
    /// The order matters only for which of several equally short traces a search reports. The caller
    /// hands in an empty vector and reuses it between states, so that listing actions does not allocate.
    fn enabled_actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>);

    /// Returns the state that `action` leads to from `state`. `action` is one that
    /// [`Model::enabled_actions`] listed for `state`.
    fn apply(&self, state: &Self::State, action: &Self::Action) -> Self::State;
}

/// How a search ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SearchOutcome<Action> {
    /// Every reachable state was visited and none matched.
    Exhausted {
        /// How many distinct states were visited.
        states_explored: usize,
    },
    /// A matching state was reached.
    Found {
        /// How many distinct states were visited before it was found, the matching one included.
        states_explored: usize,
        /// A shortest sequence of actions from the initial state to the match. Empty if the initial state
        /// itself matches.
        trace: Vec<Action>,
    },
    /// The search gave up before visiting every state. Nothing was proven.
    LimitReached {
        /// How many distinct states were visited.
        states_explored: usize,
    },
}

/// The record, for one discovered state, of how the search first got there: the position of the state it
/// came from and the action taken. `None` for the initial state, which came from nowhere.
type ReachedBy<Action> = Option<(usize, Action)>;

/// Searches the states reachable in a model, breadth first, for one that matches a predicate.
///
/// Used with "an invariant is violated" as the predicate, an [`SearchOutcome::Exhausted`] result is a
/// proof that the invariant holds in every reachable state, and a [`SearchOutcome::Found`] result is a
/// shortest counterexample. Used with a desirable state as the predicate, a `Found` result shows that the
/// model can actually get there, which is how the tests show the model is not vacuous.
///
/// Memory grows with the number of distinct states: one fingerprint, one queue entry and one trace link
/// each.
///
/// # Arguments
///
/// * `model` - The system to explore.
/// * `state_limit` - The most states to visit. Reaching it ends the search without a result.
/// * `is_target` - The predicate: whether a state is what the search looks for. It is called once for
///   every distinct state.
///
/// # Returns
///
/// * [`SearchOutcome::Found`] with a shortest trace, as soon as a matching state is discovered.
/// * [`SearchOutcome::Exhausted`] if every reachable state was visited and none matched.
/// * [`SearchOutcome::LimitReached`] if `state_limit` states were visited first.
pub(crate) fn search<SearchedModel: Model>(
    model: &SearchedModel,
    state_limit: usize,
    is_target: impl Fn(&SearchedModel::State) -> bool,
) -> SearchOutcome<SearchedModel::Action> {
    let initial_state = model.initial_state();
    if is_target(&initial_state) {
        return SearchOutcome::Found {
            states_explored: 1,
            trace: Vec::new(),
        };
    }
    let mut discovered = BTreeSet::from([model.fingerprint(&initial_state)]);
    // One entry per discovered state, in order of discovery. A state's position here is its index.
    let mut reached_by: Vec<ReachedBy<SearchedModel::Action>> = vec![None];
    let mut frontier = VecDeque::from([(0, initial_state)]);
    let mut actions = Vec::new();

    while let Some((state_index, state)) = frontier.pop_front() {
        actions.clear();
        model.enabled_actions(&state, &mut actions);
        for action in &actions {
            let successor = model.apply(&state, action);
            let is_newly_discovered = discovered.insert(model.fingerprint(&successor));
            if !is_newly_discovered {
                continue;
            }
            let successor_index = reached_by.len();
            reached_by.push(Some((state_index, action.clone())));
            if is_target(&successor) {
                return SearchOutcome::Found {
                    states_explored: reached_by.len(),
                    trace: trace_to(&reached_by, successor_index),
                };
            }
            if reached_by.len() >= state_limit {
                return SearchOutcome::LimitReached {
                    states_explored: reached_by.len(),
                };
            }
            frontier.push_back((successor_index, successor));
        }
    }
    SearchOutcome::Exhausted {
        states_explored: reached_by.len(),
    }
}

/// Walks back from a discovered state to the initial state and returns the actions on the way, first
/// action first.
///
/// # Arguments
///
/// * `reached_by` - How each discovered state was first reached, by state index.
/// * `state_index` - The index of the state the trace should end in.
fn trace_to<Action: Clone>(reached_by: &[ReachedBy<Action>], state_index: usize) -> Vec<Action> {
    let mut trace = Vec::new();
    let mut current_index = state_index;
    while let Some((previous_index, action)) = &reached_by[current_index] {
        trace.push(action.clone());
        current_index = *previous_index;
    }
    trace.reverse();
    trace
}

#[cfg(test)]
#[path = "explorer_test.rs"]
mod tests;
