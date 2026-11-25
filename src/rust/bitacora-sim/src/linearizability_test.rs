//! Tests of the linearizability check.
//!
//! # What is covered
//!
//! - Histories that are linearizable and must pass: sequential ones, concurrent ones in either order,
//!   and ones with writes of unknown outcome that took effect late or never.
//! - Histories that are not and must fail: stale reads, lost writes, invented values, values going back
//!   in time, effects before their cause.
//! - Calls that promise nothing, and that the check must ignore.
//! - The cost of the search on long and on tangled histories, and what happens when its budget runs out.
//!
//! # How the fixtures work
//!
//! [`Script`] writes a history by hand, one event at a time, the way a broken store would produce it.
//! `begin_*` starts a call and returns its identifier; `end` finishes it. The shorthand methods `put`,
//! `delete` and `get` do both at once, for calls that do not overlap anything. Every put in a script
//! writes a value of its own, as every put of a simulation does.

use std::time::Duration;

use super::*;

/// A key or value from text.
fn bytes(text: &str) -> Bytes {
    Bytes::copy_from_slice(text.as_bytes())
}

/// A history written by hand.
#[derive(Default)]
struct Script {
    /// The events so far.
    events: Vec<TimedEvent>,
    /// The identifier the last call got.
    last_operation: u64,
}

impl Script {
    /// Appends an event. The check orders events by position, so their time does not matter.
    fn push(&mut self, event: Event) {
        self.events.push(TimedEvent {
            at: Duration::ZERO,
            event,
        });
    }

    /// Starts a call and returns its identifier.
    fn begin(&mut self, call: KvCall) -> u64 {
        self.last_operation += 1;
        let operation = self.last_operation;
        self.push(Event::KvInvoked { operation, call });
        operation
    }

    /// Starts a put of `value` under `key`.
    fn begin_put(&mut self, key: &str, value: &str) -> u64 {
        self.begin(KvCall::Put {
            key: bytes(key),
            value: bytes(value),
        })
    }

    /// Starts a delete of `key`.
    fn begin_delete(&mut self, key: &str) -> u64 {
        self.begin(KvCall::Delete { key: bytes(key) })
    }

    /// Starts a read of `key`.
    fn begin_get(&mut self, key: &str) -> u64 {
        self.begin(KvCall::Get { key: bytes(key) })
    }

    /// Finishes the call `operation` with `outcome`.
    fn end(&mut self, operation: u64, outcome: KvOutcome) {
        self.push(Event::KvReturned { operation, outcome });
    }

    /// Finishes the read `operation` with the value it returned.
    fn end_get(&mut self, operation: u64, value: Option<&str>) {
        self.end(
            operation,
            KvOutcome::Read {
                value: value.map(bytes),
            },
        );
    }

    /// A put that begins and is reported done before anything else happens.
    fn put(&mut self, key: &str, value: &str) -> u64 {
        let operation = self.begin_put(key, value);
        self.end(operation, KvOutcome::Written);
        operation
    }

    /// A delete that begins and is reported done before anything else happens.
    fn delete(&mut self, key: &str) -> u64 {
        let operation = self.begin_delete(key);
        self.end(operation, KvOutcome::Written);
        operation
    }

    /// A read that begins and returns `value` before anything else happens.
    fn get(&mut self, key: &str, value: Option<&str>) -> u64 {
        let operation = self.begin_get(key);
        self.end_get(operation, value);
        operation
    }

    /// A put whose caller is told the outcome is unknown, before anything else happens.
    fn put_of_unknown_outcome(&mut self, key: &str, value: &str) -> u64 {
        let operation = self.begin_put(key, value);
        self.end(operation, KvOutcome::WriteUnknown);
        operation
    }

    /// What the check says about the script.
    fn violations(&self) -> Vec<Violation> {
        check_linearizability(&self.events)
    }
}

/// The violation of a history of key `k` in which the call `stuck_at_operation` cannot be placed.
fn not_linearizable_at(stuck_at_operation: u64) -> Vec<Violation> {
    vec![Violation::NotLinearizable {
        key: bytes("k"),
        stuck_at_operation,
    }]
}

// ---- linearizable histories ----

#[test]
fn history_without_calls_is_linearizable() {
    assert_eq!(check_linearizability(&[]), []);
}

#[test]
fn read_of_a_key_never_written_finds_it_absent() {
    let mut script = Script::default();
    script.get("k", None);

    assert_eq!(script.violations(), []);
}

#[test]
fn read_after_a_put_returns_its_value() {
    let mut script = Script::default();
    script.put("k", "a");
    script.get("k", Some("a"));

    assert_eq!(script.violations(), []);
}

#[test]
fn read_after_two_puts_returns_the_later_value() {
    let mut script = Script::default();
    script.put("k", "a");
    script.put("k", "b");
    script.get("k", Some("b"));

    assert_eq!(script.violations(), []);
}

#[test]
fn read_after_a_delete_finds_the_key_absent() {
    let mut script = Script::default();
    script.put("k", "a");
    script.delete("k");
    script.get("k", None);

    assert_eq!(script.violations(), []);
}

#[test]
fn read_concurrent_with_a_put_may_return_the_old_value() {
    let mut script = Script::default();
    script.put("k", "a");
    let put = script.begin_put("k", "b");
    script.get("k", Some("a"));
    script.end(put, KvOutcome::Written);

    assert_eq!(script.violations(), []);
}

#[test]
fn read_concurrent_with_a_put_may_return_the_new_value() {
    let mut script = Script::default();
    script.put("k", "a");
    let put = script.begin_put("k", "b");
    script.get("k", Some("b"));
    script.end(put, KvOutcome::Written);

    assert_eq!(script.violations(), []);
}

#[test]
fn two_concurrent_puts_may_take_effect_in_either_order() {
    for winner in ["a", "b"] {
        let mut script = Script::default();
        let first_put = script.begin_put("k", "a");
        let second_put = script.begin_put("k", "b");
        script.end(first_put, KvOutcome::Written);
        script.end(second_put, KvOutcome::Written);
        script.get("k", Some(winner));

        assert_eq!(script.violations(), [], "{winner} wins");
    }
}

#[test]
fn put_of_unknown_outcome_may_never_take_effect() {
    let mut script = Script::default();
    script.put("k", "a");
    script.put_of_unknown_outcome("k", "b");
    script.get("k", Some("a"));

    assert_eq!(script.violations(), []);
}

#[test]
fn put_of_unknown_outcome_may_take_effect_at_once() {
    let mut script = Script::default();
    script.put("k", "a");
    script.put_of_unknown_outcome("k", "b");
    script.get("k", Some("b"));

    assert_eq!(script.violations(), []);
}

#[test]
fn put_of_unknown_outcome_may_take_effect_long_after_its_caller_gave_up() {
    let mut script = Script::default();
    script.put_of_unknown_outcome("k", "late");
    script.put("k", "a");
    script.get("k", Some("a"));
    script.get("k", Some("late"));

    assert_eq!(script.violations(), []);
}

#[test]
fn put_of_a_client_that_crashed_may_take_effect() {
    let mut script = Script::default();
    script.put("k", "a");
    script.begin_put("k", "b");
    script.get("k", Some("b"));

    assert_eq!(script.violations(), []);
}

#[test]
fn delete_of_unknown_outcome_may_take_effect_later() {
    let mut script = Script::default();
    script.put("k", "a");
    let delete = script.begin_delete("k");
    script.end(delete, KvOutcome::WriteUnknown);
    script.get("k", Some("a"));
    script.get("k", None);

    assert_eq!(script.violations(), []);
}

#[test]
fn read_that_gave_no_answer_is_ignored() {
    let mut script = Script::default();
    script.put("k", "a");
    let failed_get = script.begin_get("k");
    script.end(failed_get, KvOutcome::ReadFailed);

    assert_eq!(script.violations(), []);
}

#[test]
fn write_that_was_refused_and_never_seen_is_ignored() {
    let mut script = Script::default();
    script.put("k", "a");
    let refused_put = script.begin_put("k", "b");
    script.end(refused_put, KvOutcome::NotWritten);
    script.get("k", Some("a"));

    assert_eq!(script.violations(), []);
}

#[test]
fn keys_are_judged_independently() {
    let mut script = Script::default();
    script.put("k", "a");
    script.put("other", "b");
    script.get("k", Some("a"));
    script.get("other", Some("b"));

    assert_eq!(script.violations(), []);
}

// ---- histories that are not linearizable ----

#[test]
fn read_that_misses_a_finished_put_is_stale() {
    let mut script = Script::default();
    script.put("k", "a");
    script.put("k", "b");
    let stale_get = script.get("k", Some("a"));

    assert_eq!(script.violations(), not_linearizable_at(stale_get));
}

#[test]
fn read_that_finds_a_written_key_absent_lost_the_write() {
    let mut script = Script::default();
    script.put("k", "a");
    let get = script.get("k", None);

    assert_eq!(script.violations(), not_linearizable_at(get));
}

#[test]
fn read_of_a_value_nobody_wrote_is_reported() {
    let mut script = Script::default();
    script.put("k", "a");
    let get = script.get("k", Some("invented"));

    assert_eq!(script.violations(), not_linearizable_at(get));
}

#[test]
fn read_of_a_value_whose_write_was_refused_is_reported() {
    let mut script = Script::default();
    let refused_put = script.begin_put("k", "a");
    script.end(refused_put, KvOutcome::NotWritten);
    let get = script.get("k", Some("a"));

    assert_eq!(script.violations(), not_linearizable_at(get));
}

#[test]
fn read_that_sees_a_deleted_key_again_is_reported() {
    let mut script = Script::default();
    script.put("k", "a");
    script.delete("k");
    let get = script.get("k", Some("a"));

    assert_eq!(script.violations(), not_linearizable_at(get));
}

#[test]
fn reads_that_go_back_in_time_are_reported() {
    let mut script = Script::default();
    script.put("k", "a");
    let put = script.begin_put("k", "b");
    script.get("k", Some("b"));
    let get_of_the_past = script.get("k", Some("a"));
    script.end(put, KvOutcome::Written);

    assert_eq!(script.violations(), not_linearizable_at(get_of_the_past));
}

#[test]
fn read_that_returns_before_the_put_it_saw_began_is_reported() {
    let mut script = Script::default();
    let get_of_the_future = script.get("k", Some("a"));
    script.put("k", "a");

    assert_eq!(script.violations(), not_linearizable_at(get_of_the_future));
}

#[test]
fn put_of_unknown_outcome_cannot_take_effect_before_it_began() {
    let mut script = Script::default();
    let get_of_the_future = script.get("k", Some("a"));
    script.put_of_unknown_outcome("k", "a");

    assert_eq!(script.violations(), not_linearizable_at(get_of_the_future));
}

#[test]
fn put_of_unknown_outcome_takes_effect_at_most_once() {
    let mut script = Script::default();
    script.put("k", "a");
    script.put_of_unknown_outcome("k", "b");
    script.get("k", Some("b"));
    script.put("k", "c");
    let get = script.get("k", Some("b"));

    assert_eq!(script.violations(), not_linearizable_at(get));
}

#[test]
fn value_that_was_seen_cannot_be_unseen_without_another_write() {
    let mut script = Script::default();
    script.put("k", "a");
    script.put_of_unknown_outcome("k", "b");
    script.get("k", Some("b"));
    let get = script.get("k", Some("a"));

    assert_eq!(script.violations(), not_linearizable_at(get));
}

#[test]
fn violation_names_the_key_it_is_on() {
    let mut script = Script::default();
    script.put("fine", "a");
    script.get("fine", Some("a"));
    script.put("broken", "b");
    let get = script.get("broken", None);

    assert_eq!(
        script.violations(),
        [Violation::NotLinearizable {
            key: bytes("broken"),
            stuck_at_operation: get,
        }]
    );
}

// ---- cost of the search ----

#[test]
fn long_history_of_calls_one_after_another_is_judged() {
    let mut script = Script::default();
    for round in 0..3_000 {
        let value = format!("value-{round}");
        script.put("k", &value);
        script.get("k", Some(&value));
    }

    assert_eq!(script.violations(), []);
}

#[test]
fn history_with_many_puts_of_unknown_outcome_nobody_saw_is_judged() {
    let mut script = Script::default();
    for round in 0..500 {
        let value = format!("value-{round}");
        script.put_of_unknown_outcome("k", &format!("unknown-{round}"));
        script.put("k", &value);
        script.get("k", Some(&value));
    }

    assert_eq!(script.violations(), []);
}

#[test]
fn history_with_many_deletes_of_unknown_outcome_is_judged() {
    let mut script = Script::default();
    for round in 0..40 {
        let value = format!("value-{round}");
        let delete = script.begin_delete("k");
        script.end(delete, KvOutcome::WriteUnknown);
        script.put("k", &value);
        script.get("k", Some(&value));
    }

    assert_eq!(script.violations(), []);
}

#[test]
fn history_with_several_calls_in_flight_at_all_times_is_judged() {
    let mut script = Script::default();
    let mut in_flight = Vec::new();
    for round in 0..400 {
        let value = format!("value-{round}");
        in_flight.push(script.begin_put("k", &value));
        if in_flight.len() == 4 {
            for put in in_flight.drain(..) {
                script.end(put, KvOutcome::Written);
            }
            script.get("k", Some(&value));
        }
    }

    assert_eq!(script.violations(), []);
}

#[test]
fn search_that_runs_out_of_budget_is_undecided() {
    let mut script = Script::default();
    script.put("k", "a");
    script.put("k", "b");
    script.get("k", Some("b"));
    let calls = calls_by_key(&script.events)
        .remove(&bytes("k"))
        .expect("the script has calls on the key");

    let verdict = judge_key(&calls, 1);

    assert_eq!(verdict, Verdict::Undecided);
}

#[test]
fn search_within_its_budget_reaches_a_verdict() {
    let mut script = Script::default();
    script.put("k", "a");
    script.put("k", "b");
    script.get("k", Some("b"));
    let calls = calls_by_key(&script.events)
        .remove(&bytes("k"))
        .expect("the script has calls on the key");

    let verdict = judge_key(&calls, 10);

    assert_eq!(verdict, Verdict::Linearizable);
}
