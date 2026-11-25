//! A small explicit-state model checker.
//!
//! It explores every state a [`Model`] can reach, breadth first, and stops at
//! the first one that matches what the caller is looking for. Breadth-first
//! order means the trace it returns is a shortest one, which is what makes a
//! counterexample readable. The exploration is deterministic: the same model
//! always visits the same states in the same order.

use std::collections::{BTreeSet, VecDeque};
use std::fmt::Debug;

/// A system described as states and the actions that lead from one to the
/// next.
pub trait Model {
    /// One configuration of the whole system.
    type State: Clone;
    /// One step some part of the system can take.
    type Action: Clone + Debug;
    /// What identifies a state. Two states with the same fingerprint are
    /// explored once, so it must capture everything that can influence the
    /// future; it may deliberately equate states that only differ by a
    /// symmetry, such as which of three identical replicas is which.
    type Fingerprint: Ord;

    /// The state the system starts in.
    fn initial_state(&self) -> Self::State;

    /// Identifies `state`.
    fn fingerprint(&self, state: &Self::State) -> Self::Fingerprint;

    /// Appends to `actions` every action that can happen in `state`.
    fn enabled_actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>);

    /// Returns the state that `action` leads to from `state`.
    fn apply(&self, state: &Self::State, action: &Self::Action) -> Self::State;
}

/// How a search ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SearchOutcome<Action> {
    /// Every reachable state was visited and none matched.
    Exhausted {
        /// How many distinct states were visited.
        states_explored: usize,
    },
    /// A matching state was reached.
    Found {
        /// How many distinct states were visited before it was found.
        states_explored: usize,
        /// A shortest sequence of actions from the initial state to it.
        trace: Vec<Action>,
    },
    /// The search gave up before visiting every state. Nothing was proven.
    LimitReached {
        /// How many distinct states were visited.
        states_explored: usize,
    },
}

/// Searches the states reachable in `model` for one that `is_target`.
///
/// Used with "an invariant is violated" as the target, an
/// [`SearchOutcome::Exhausted`] result is a proof that the invariant holds
/// in every reachable state, and a [`SearchOutcome::Found`] result is a
/// shortest counterexample. Used with a desirable state as the target, a
/// `Found` result shows that the model can actually get there.
///
/// At most `state_limit` states are visited.
pub fn search<SearchedModel: Model>(
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
    // For every discovered state, the state it was first reached from and
    // the action that led there. The initial state has neither.
    let mut reached_by: Vec<Option<(usize, SearchedModel::Action)>> = vec![None];
    let mut frontier = VecDeque::from([(0, initial_state)]);
    let mut actions = Vec::new();

    while let Some((state_index, state)) = frontier.pop_front() {
        actions.clear();
        model.enabled_actions(&state, &mut actions);
        for action in &actions {
            let successor = model.apply(&state, action);
            if !discovered.insert(model.fingerprint(&successor)) {
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

/// Walks back from `state_index` to the initial state and returns the
/// actions on the way, first action first.
fn trace_to<Action: Clone>(
    reached_by: &[Option<(usize, Action)>],
    state_index: usize,
) -> Vec<Action> {
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
mod tests {
    use super::*;

    /// Two counters that can each be raised up to a limit, in any order.
    struct TwoCounters {
        limit: u8,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Raise {
        First,
        Second,
    }

    impl Model for TwoCounters {
        type State = (u8, u8);
        type Action = Raise;
        type Fingerprint = (u8, u8);

        fn initial_state(&self) -> (u8, u8) {
            (0, 0)
        }

        fn fingerprint(&self, state: &(u8, u8)) -> (u8, u8) {
            *state
        }

        fn enabled_actions(&self, state: &(u8, u8), actions: &mut Vec<Raise>) {
            if state.0 < self.limit {
                actions.push(Raise::First);
            }
            if state.1 < self.limit {
                actions.push(Raise::Second);
            }
        }

        fn apply(&self, state: &(u8, u8), action: &Raise) -> (u8, u8) {
            match action {
                Raise::First => (state.0 + 1, state.1),
                Raise::Second => (state.0, state.1 + 1),
            }
        }
    }

    const NO_LIMIT: usize = usize::MAX;

    #[test]
    fn search_without_a_match_visits_every_reachable_state_once() {
        let outcome = search(&TwoCounters { limit: 3 }, NO_LIMIT, |_state| false);
        assert_eq!(
            outcome,
            SearchOutcome::Exhausted {
                states_explored: 16
            }
        );
    }

    #[test]
    fn search_returns_a_shortest_trace_to_the_match() {
        let outcome = search(&TwoCounters { limit: 3 }, NO_LIMIT, |state| {
            *state == (1, 2)
        });
        let SearchOutcome::Found { trace, .. } = outcome else {
            panic!("the state (1, 2) is reachable, but the search ended with {outcome:?}");
        };
        assert_eq!(trace.len(), 3);
    }

    #[test]
    fn trace_leads_from_the_initial_state_to_the_match() {
        let model = TwoCounters { limit: 3 };
        let outcome = search(&model, NO_LIMIT, |state| *state == (2, 3));
        let SearchOutcome::Found { trace, .. } = outcome else {
            panic!("the state (2, 3) is reachable, but the search ended with {outcome:?}");
        };
        let reached = trace.iter().fold(model.initial_state(), |state, action| {
            model.apply(&state, action)
        });
        assert_eq!(reached, (2, 3));
    }

    #[test]
    fn initial_state_that_matches_is_found_with_an_empty_trace() {
        let outcome = search(&TwoCounters { limit: 3 }, NO_LIMIT, |state| {
            *state == (0, 0)
        });
        assert_eq!(
            outcome,
            SearchOutcome::Found {
                states_explored: 1,
                trace: Vec::new(),
            }
        );
    }

    #[test]
    fn unreachable_match_exhausts_the_search() {
        let outcome = search(&TwoCounters { limit: 3 }, NO_LIMIT, |state| state.0 == 4);
        assert!(matches!(outcome, SearchOutcome::Exhausted { .. }));
    }

    #[test]
    fn search_gives_up_at_its_state_limit() {
        let outcome = search(&TwoCounters { limit: 200 }, 50, |_state| false);
        assert_eq!(
            outcome,
            SearchOutcome::LimitReached {
                states_explored: 50
            }
        );
    }

    #[test]
    fn search_is_deterministic() {
        let find = || {
            search(&TwoCounters { limit: 5 }, NO_LIMIT, |state| {
                *state == (3, 3)
            })
        };
        assert_eq!(find(), find());
    }
}
