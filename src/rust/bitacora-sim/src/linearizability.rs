//! Judges whether the calls made to the key-value store could have taken effect one at a time.
//!
//! # Why this file exists
//!
//! The log checker in `checker.rs` proves that the log kept its promises. A key-value store built on the
//! log makes a stronger one: every call appears to take effect at a single moment between its start and
//! its end, and every read returns what the calls before that moment left behind. That is
//! linearizability, and it is what "bitacora-kv histories are linearizable under the fault matrix"
//! (design, section 18, the exit gate of M4) asks to be checked. A store can break it without breaking
//! the log: by answering a read from a replaced writer, from pages that lag, or from a checkpoint.
//!
//! # What it contains
//!
//! - [`check_linearizability`]: every violation in a history, one per key at most.
//!
//! # How it works
//!
//! Like the log checker, it sees only what clients saw: the calls and what they returned, in the order
//! the simulation recorded them. Keys are independent registers, so each key is judged on its own, which
//! keeps the search small.
//!
//! For one key, the question is whether its calls can be put in an order such that
//!
//! 1. a call that finished before another began comes first, and
//! 2. every read returns the value of the last write before it, or "absent" if there is none or it was
//!    a delete.
//!
//! The search is the one of Wing and Gong, with the memory of Lowe: it builds the order one call at a
//! time. At each step it may pick any remaining call that began before the earliest remaining finished
//! call returned; nothing else could come next without breaking rule 1. A read may only be picked if it
//! returned the current value. A dead end is abandoned, and every state reached (which calls are placed,
//! what the value is) is remembered so that it is never explored twice.
//!
//! Three kinds of calls need care:
//!
//! - A write whose outcome its caller never learned, because the store said so or the client crashed.
//!   It may take effect at any moment after it began, even long after its caller gave up, or never. It
//!   is optional: the search may place it anywhere after its start, or leave it out.
//! - A write the store refused, and a read that gave no answer. They are left out: they promise nothing.
//!   If a refused write shows up in a later read, that read has no write to explain it and the search
//!   fails, as it should.
//! - An optional put whose value no read ever returned is dropped before the search. Leaving it out of
//!   an order never breaks the order, since no read stood on it, so dropping it loses nothing; and it
//!   removes most of the optional calls, which are what makes the search expensive. This relies on every
//!   put of a simulation writing a value of its own.
//!
//! The search is given a budget of states. If it runs out, the key is reported as undecided, which is a
//! failure too: a history that cannot be judged is not known to be correct.
//!
//! # Invariants
//!
//! - A history is reported as not linearizable only if no order exists. There are no false alarms.
//! - A history is passed only if an order was found. There is no "probably fine".
//! - The verdict depends only on the events, so a seed that fails keeps failing.
//!
//! # See also
//!
//! - `history.rs`: the events judged here.
//! - `checker.rs`: the invariants of the log itself.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use crate::checker::Violation;
use crate::history::{Event, KvCall, KvOutcome, TimedEvent};

/// How many states the search for one key may reach before it gives up. A clean run of the simulator
/// stays three orders of magnitude below it.
const MAX_STATES_PER_KEY: usize = 400_000;

/// What a call did to the value of its key, or learned about it.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Effect {
    /// Set the value: `Some` for a put, `None` for a delete.
    Write(Option<Bytes>),
    /// Returned this value: `None` for a key that does not exist.
    Read(Option<Bytes>),
}

/// One call on one key, reduced to what the search needs.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Call {
    /// The call's identifier in the history, for reporting.
    operation: u64,
    /// The position in the history at which the call began.
    invoked_at: usize,
    /// The position at which the call returned with a result that binds it: a write reported as done, or
    /// a read with a value. `None` for a write that may take effect at any later moment, or never.
    returned_at: Option<usize>,
    /// What the call did or saw.
    effect: Effect,
}

/// What the search concluded about one key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verdict {
    /// An order exists.
    Linearizable,
    /// No order exists. The search never got past the named call.
    NotLinearizable {
        /// The finished call that returns first among those the deepest attempt could not place.
        stuck_at_operation: u64,
    },
    /// The budget of states ran out.
    Undecided,
}

/// Checks the key-value calls of a history for linearizability, key by key.
///
/// # Arguments
///
/// * `events` - A whole history, in the order it was recorded. Events that are not key-value calls are
///   ignored.
///
/// # Returns
///
/// One [`Violation::NotLinearizable`] for every key whose calls have no valid order, and one
/// [`Violation::LinearizabilityUndecided`] for every key whose search was given up; in key order. Empty
/// if every key is linearizable.
///
/// # Examples
///
/// ```
/// use bitacora_sim::{History, KvCall, KvOutcome, check_linearizability};
/// use bytes::Bytes;
///
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() {
/// let history = History::new();
/// let key = Bytes::from_static(b"colour");
/// let put = history.kv_invoked(KvCall::Put { key: key.clone(), value: Bytes::from_static(b"red") });
/// history.kv_returned(put, KvOutcome::Written);
/// let get = history.kv_invoked(KvCall::Get { key });
/// // The put had finished, yet the read does not see it.
/// history.kv_returned(get, KvOutcome::Read { value: None });
///
/// assert_eq!(check_linearizability(&history.events()).len(), 1);
/// # }
/// ```
#[must_use]
pub fn check_linearizability(events: &[TimedEvent]) -> Vec<Violation> {
    let mut violations = Vec::new();
    for (key, calls) in calls_by_key(events) {
        let calls = without_puts_nobody_observed(calls);
        match judge_key(&calls, MAX_STATES_PER_KEY) {
            Verdict::Linearizable => {}
            Verdict::NotLinearizable { stuck_at_operation } => {
                violations.push(Violation::NotLinearizable {
                    key,
                    stuck_at_operation,
                });
            }
            Verdict::Undecided => violations.push(Violation::LinearizabilityUndecided { key }),
        }
    }
    violations
}

/// Reads the key-value calls out of a history and groups them by key, each group in the order the
/// calls began.
fn calls_by_key(events: &[TimedEvent]) -> BTreeMap<Bytes, Vec<Call>> {
    let mut calls_by_key: BTreeMap<Bytes, Vec<Call>> = BTreeMap::new();
    // The calls that have begun and not returned yet: where they began and what they are.
    let mut in_flight: BTreeMap<u64, (usize, KvCall)> = BTreeMap::new();
    for (position, timed) in events.iter().enumerate() {
        match &timed.event {
            Event::KvInvoked { operation, call } => {
                in_flight.insert(*operation, (position, call.clone()));
            }
            Event::KvReturned { operation, outcome } => {
                let Some((invoked_at, call)) = in_flight.remove(operation) else {
                    continue;
                };
                let Some((effect, binds_at_return)) = effect_of(&call, Some(outcome)) else {
                    continue;
                };
                calls_by_key
                    .entry(call.key().clone())
                    .or_default()
                    .push(Call {
                        operation: *operation,
                        invoked_at,
                        returned_at: binds_at_return.then_some(position),
                        effect,
                    });
            }
            _other => {}
        }
    }
    // What never returned belongs to clients that crashed.
    for (operation, (invoked_at, call)) in in_flight {
        let Some((effect, _binds_at_return)) = effect_of(&call, None) else {
            continue;
        };
        calls_by_key
            .entry(call.key().clone())
            .or_default()
            .push(Call {
                operation,
                invoked_at,
                returned_at: None,
                effect,
            });
    }
    for calls in calls_by_key.values_mut() {
        calls.sort_by_key(|call| call.invoked_at);
    }
    calls_by_key
}

/// What a call contributes to the search, given how it ended: its effect, and whether the moment it
/// returned bounds the moment it took effect.
///
/// # Arguments
///
/// * `call` - The call.
/// * `outcome` - What it returned, or `None` if it never returned.
///
/// # Returns
///
/// `None` for a call that promises nothing and is left out: a refused write, a read without an answer.
fn effect_of(call: &KvCall, outcome: Option<&KvOutcome>) -> Option<(Effect, bool)> {
    let written_value = match call {
        KvCall::Put { value, .. } => Some(Some(value.clone())),
        KvCall::Delete { .. } => Some(None),
        KvCall::Get { .. } => None,
    };
    match (written_value, outcome) {
        (Some(value), Some(KvOutcome::Written)) => Some((Effect::Write(value), true)),
        (Some(value), Some(KvOutcome::WriteUnknown) | None) => Some((Effect::Write(value), false)),
        (None, Some(KvOutcome::Read { value })) => Some((Effect::Read(value.clone()), true)),
        _promises_nothing => None,
    }
}

/// Drops the optional puts whose value no read returned. See the file documentation for why that
/// changes no verdict.
fn without_puts_nobody_observed(calls: Vec<Call>) -> Vec<Call> {
    let mut observed_values = BTreeSet::new();
    for call in &calls {
        if let Effect::Read(Some(value)) = &call.effect {
            observed_values.insert(value.clone());
        }
    }
    let mut kept = Vec::with_capacity(calls.len());
    for call in calls {
        let is_an_unobserved_optional_put = match &call.effect {
            Effect::Write(Some(value)) => {
                call.returned_at.is_none() && !observed_values.contains(value)
            }
            _other => false,
        };
        if !is_an_unobserved_optional_put {
            kept.push(call);
        }
    }
    kept
}

/// The calls of one key with their values replaced by small numbers, which is all the search compares.
struct Register {
    /// The calls, in the order they began.
    calls: Vec<Call>,
    /// For each call, the number of the value it wrote or read. Zero stands for "absent".
    value_of_call: Vec<u32>,
    /// The order in which candidates are tried: calls that must take effect first, then optional ones.
    /// Trying the certain before the speculative is what finds the order of a correct history fast.
    candidate_order: Vec<usize>,
    /// How many calls must take effect.
    required_count: usize,
}

impl Register {
    /// Numbers the values of the calls and fixes the order candidates are tried in.
    fn new(calls: &[Call]) -> Self {
        let mut number_of_value: BTreeMap<Bytes, u32> = BTreeMap::new();
        let mut value_of_call = Vec::with_capacity(calls.len());
        for call in calls {
            let (Effect::Write(value) | Effect::Read(value)) = &call.effect;
            let number = match value {
                None => 0,
                Some(value) => {
                    let next_number = u32::try_from(number_of_value.len() + 1)
                        .expect("a history has far fewer than 2^32 values");
                    *number_of_value.entry(value.clone()).or_insert(next_number)
                }
            };
            value_of_call.push(number);
        }
        let mut candidate_order = Vec::with_capacity(calls.len());
        for (position, call) in calls.iter().enumerate() {
            if call.returned_at.is_some() {
                candidate_order.push(position);
            }
        }
        let required_count = candidate_order.len();
        for (position, call) in calls.iter().enumerate() {
            if call.returned_at.is_none() {
                candidate_order.push(position);
            }
        }
        Self {
            calls: calls.to_vec(),
            value_of_call,
            candidate_order,
            required_count,
        }
    }
}

/// Which calls an attempt has placed so far: one bit per call.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Placed {
    /// The bits, sixty-four calls to a word.
    words: Vec<u64>,
}

impl Placed {
    /// Nothing placed, for `call_count` calls.
    fn none_of(call_count: usize) -> Self {
        Self {
            words: vec![0; call_count.div_ceil(64)],
        }
    }

    /// Whether the call at `position` is placed.
    fn contains(&self, position: usize) -> bool {
        self.words[position / 64] & (1 << (position % 64)) != 0
    }

    /// Marks the call at `position` as placed.
    fn insert(&mut self, position: usize) {
        self.words[position / 64] |= 1 << (position % 64);
    }

    /// Marks the call at `position` as not placed.
    fn remove(&mut self, position: usize) {
        self.words[position / 64] &= !(1 << (position % 64));
    }
}

/// One step of an attempt: the call that was placed, and where to go on if it leads nowhere.
struct Step {
    /// The call placed at this step, or `None` for the start of the attempt.
    placed_call: Option<usize>,
    /// The value of the register before the call was placed, to undo it.
    value_before: u32,
    /// Which entry of the candidate order to try next from the state this step led to.
    next_candidate: usize,
}

/// Searches for an order of `calls`, reaching at most `max_states` states.
///
/// # Arguments
///
/// * `calls` - The calls of one key, in the order they began.
/// * `max_states` - The budget. Tests pass a small one to see the search give up.
fn judge_key(calls: &[Call], max_states: usize) -> Verdict {
    let register = Register::new(calls);
    let mut placed = Placed::none_of(calls.len());
    let mut value = 0;
    let mut required_placed = 0;
    let mut seen_states: BTreeSet<(Placed, u32)> = BTreeSet::new();
    let mut most_required_placed = 0;
    let mut stuck_at_operation = first_to_return(&register, &placed);
    let mut steps = vec![Step {
        placed_call: None,
        value_before: 0,
        next_candidate: 0,
    }];

    while let Some(step) = steps.last_mut() {
        if required_placed == register.required_count {
            return Verdict::Linearizable;
        }
        let earliest_return = earliest_return_of_unplaced(&register, &placed);
        let mut entered_a_new_state = false;
        while step.next_candidate < register.candidate_order.len() {
            let position = register.candidate_order[step.next_candidate];
            step.next_candidate += 1;
            let call = &register.calls[position];
            if placed.contains(position) || call.invoked_at >= earliest_return {
                continue;
            }
            let value_after = match call.effect {
                Effect::Write(_) => register.value_of_call[position],
                Effect::Read(_) if register.value_of_call[position] == value => value,
                Effect::Read(_) => continue,
            };
            placed.insert(position);
            if !seen_states.insert((placed.clone(), value_after)) {
                placed.remove(position);
                continue;
            }
            if seen_states.len() > max_states {
                return Verdict::Undecided;
            }
            if call.returned_at.is_some() {
                required_placed += 1;
            }
            steps.push(Step {
                placed_call: Some(position),
                value_before: value,
                next_candidate: 0,
            });
            value = value_after;
            entered_a_new_state = true;
            break;
        }
        if entered_a_new_state {
            if required_placed > most_required_placed {
                most_required_placed = required_placed;
                stuck_at_operation = first_to_return(&register, &placed);
            }
            continue;
        }
        // Every candidate from this state leads nowhere: take the last call back.
        let abandoned = steps
            .pop()
            .expect("the loop runs only while a step is left");
        if let Some(position) = abandoned.placed_call {
            placed.remove(position);
            value = abandoned.value_before;
            if register.calls[position].returned_at.is_some() {
                required_placed -= 1;
            }
        }
    }
    Verdict::NotLinearizable {
        stuck_at_operation: stuck_at_operation.unwrap_or_default(),
    }
}

/// The position in the history at which the first unplaced call that must take effect returned. A call
/// may only be placed next if it began before that. Without such a call nothing bounds the choice.
fn earliest_return_of_unplaced(register: &Register, placed: &Placed) -> usize {
    let mut earliest_return = usize::MAX;
    for (position, call) in register.calls.iter().enumerate() {
        if let Some(returned_at) = call.returned_at
            && !placed.contains(position)
        {
            earliest_return = earliest_return.min(returned_at);
        }
    }
    earliest_return
}

/// The identifier of the unplaced call that must take effect and returned first, if there is one: the
/// call an attempt that got this far could not get past.
fn first_to_return(register: &Register, placed: &Placed) -> Option<u64> {
    let mut first: Option<(usize, u64)> = None;
    for (position, call) in register.calls.iter().enumerate() {
        let Some(returned_at) = call.returned_at else {
            continue;
        };
        if placed.contains(position) {
            continue;
        }
        if first.is_none_or(|(earliest_return, _operation)| returned_at < earliest_return) {
            first = Some((returned_at, call.operation));
        }
    }
    first.map(|(_returned_at, operation)| operation)
}

#[cfg(test)]
#[path = "linearizability_test.rs"]
mod tests;
