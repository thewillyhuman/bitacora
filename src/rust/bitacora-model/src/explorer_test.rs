//! Tests of the explorer on a system small enough to count by hand.
//!
//! # What is covered
//!
//! - An exhaustive search visits every reachable state exactly once.
//! - A match is reported with a shortest trace, and the trace really leads to the match.
//! - The corner cases: the initial state matches, nothing matches, the state limit is reached.
//! - Two searches of the same model give the same result.
//!
//! # How the fixtures work
//!
//! `TwoCounters` is two counters that can each be raised by one, in any order, up to a limit. With a
//! limit of 3 it has 4 × 4 = 16 states, and the state "first counter at 1, second at 2" is exactly three
//! steps from the start. Those numbers can be checked without running anything, which is what makes this
//! a test of the explorer and not of a protocol.

use super::*;

/// A state of the toy system: how far each counter was raised.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Counters {
    /// The value of the first counter.
    first: u8,
    /// The value of the second counter.
    second: u8,
}

/// The toy system: two counters that can each be raised up to a limit, in any order.
struct TwoCounters {
    /// The highest value either counter may reach.
    limit: u8,
}

/// A step of the toy system: raising one of the counters by one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Raise {
    /// Raise the first counter.
    First,
    /// Raise the second counter.
    Second,
}

impl Model for TwoCounters {
    type State = Counters;
    type Action = Raise;
    type Fingerprint = Counters;

    /// Both counters start at zero.
    fn initial_state(&self) -> Counters {
        Counters { first: 0, second: 0 }
    }

    /// A state is its own fingerprint: the toy system has no symmetry to exploit.
    fn fingerprint(&self, counters: &Counters) -> Counters {
        *counters
    }

    /// Each counter below the limit can be raised.
    fn enabled_actions(&self, counters: &Counters, actions: &mut Vec<Raise>) {
        if counters.first < self.limit {
            actions.push(Raise::First);
        }
        if counters.second < self.limit {
            actions.push(Raise::Second);
        }
    }

    /// Raises the counter the action names.
    fn apply(&self, counters: &Counters, action: &Raise) -> Counters {
        match action {
            Raise::First => Counters {
                first: counters.first + 1,
                second: counters.second,
            },
            Raise::Second => Counters {
                first: counters.first,
                second: counters.second + 1,
            },
        }
    }
}

/// A state limit no search of the toy system reaches.
const NO_LIMIT: usize = usize::MAX;

/// The toy system in which each counter can reach 3: 16 states.
fn counters_up_to_three() -> TwoCounters {
    TwoCounters { limit: 3 }
}

/// Searches `model` for the state in which the counters are at `first` and `second`.
fn search_for(model: &TwoCounters, first: u8, second: u8) -> SearchOutcome<Raise> {
    let wanted = Counters { first, second };
    search(model, NO_LIMIT, |counters| *counters == wanted)
}

/// The trace of a search that is expected to have found its target.
fn found_trace(outcome: SearchOutcome<Raise>) -> Vec<Raise> {
    match outcome {
        SearchOutcome::Found { trace, .. } => trace,
        unexpected_outcome => {
            panic!("the state is reachable, but the search ended with {unexpected_outcome:?}")
        }
    }
}

#[test]
fn search_without_a_match_visits_every_reachable_state_once() {
    let model = counters_up_to_three();

    let outcome = search(&model, NO_LIMIT, |_counters| false);

    assert_eq!(outcome, SearchOutcome::Exhausted { states_explored: 16 });
}

#[test]
fn search_returns_a_shortest_trace_to_the_match() {
    let model = counters_up_to_three();

    let trace = found_trace(search_for(&model, 1, 2));

    assert_eq!(trace.len(), 3);
}

#[test]
fn trace_leads_from_the_initial_state_to_the_match() {
    let model = counters_up_to_three();
    let trace = found_trace(search_for(&model, 2, 3));

    let mut reached = model.initial_state();
    for action in &trace {
        reached = model.apply(&reached, action);
    }

    assert_eq!(reached, Counters { first: 2, second: 3 });
}

#[test]
fn initial_state_that_matches_is_found_with_an_empty_trace() {
    let model = counters_up_to_three();

    let outcome = search_for(&model, 0, 0);

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
    let model = counters_up_to_three();
    let value_above_the_limit = 4;

    let outcome = search(&model, NO_LIMIT, |counters| counters.first == value_above_the_limit);

    assert!(matches!(outcome, SearchOutcome::Exhausted { .. }));
}

#[test]
fn search_gives_up_at_its_state_limit() {
    let model_with_many_states = TwoCounters { limit: 200 };
    let state_limit = 50;

    let outcome = search(&model_with_many_states, state_limit, |_counters| false);

    assert_eq!(outcome, SearchOutcome::LimitReached { states_explored: 50 });
}

#[test]
fn search_is_deterministic() {
    let model = TwoCounters { limit: 5 };

    let first_search = search_for(&model, 3, 3);
    let second_search = search_for(&model, 3, 3);

    assert_eq!(first_search, second_search);
}
