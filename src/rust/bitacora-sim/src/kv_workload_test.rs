//! Tests of the decisions of the key-value workload that need no store.
//!
//! # What is covered
//!
//! - What the history is told about a write, for each thing a store can return, with and without the bug that reports
//!   unknown writes as done.
//! - Whether a node goes on with its store after a read, and that a read which finds the log truncated is recorded.
//! - The fillers a run declares, the settings of its stores, and the calls a node draws.
//!
//! # How the fixtures work
//!
//! Results of store calls are built by hand: no store is opened. The calls a compute node draws are taken from a node
//! built on a world of the test support; the node is never run.
//!
//! What a node does with a store, under faults, is tested by every run of `tests/fault_matrix_test.rs`.

use std::collections::BTreeSet;

use bitacora_client::AppendError;

use super::*;
use crate::history::Event;
use crate::test_support::start_world;
use crate::world::STREAM;

/// The error of a write the store refused before it reached the log.
fn refused_write() -> KvError {
    KvError::NotWritten {
        reason: AppendError::Closed,
    }
}

/// The error of a write that may or may not have reached the log.
fn write_of_unknown_outcome() -> KvError {
    KvError::OutcomeUnknown {
        reason: AppendError::Fenced,
    }
}

/// The error of a read whose materializer found its next record truncated away.
fn read_of_a_truncated_log() -> KvError {
    KvError::Materializer(RunnerError::LogTruncated {
        next_sequence: SequenceNumber::new(11),
        low_water_mark: SequenceNumber::new(15),
    })
}

/// Builds compute node 1 on a new world. The node is not run.
async fn compute_node_with_seed(seed: u64) -> ComputeNode {
    let world = start_world(seed).await;
    ComputeNode {
        node_number: 1,
        stream: STREAM,
        dependencies: world.store_dependencies("compute-1"),
        history: world.history.clone(),
        injected_bug: InjectedBug::None,
        random: DeterministicRandom::from_seed(seed),
        puts_made: 0,
    }
}

#[test]
fn write_that_was_made_is_reported_as_written() {
    let outcome = outcome_of_a_write(&Ok(SequenceNumber::new(7)), InjectedBug::None);

    assert_eq!(outcome, KvOutcome::Written);
}

#[test]
fn refused_write_is_reported_as_not_written() {
    let outcome = outcome_of_a_write(&Err(refused_write()), InjectedBug::None);

    assert_eq!(outcome, KvOutcome::NotWritten);
}

#[test]
fn write_of_unknown_outcome_is_reported_as_unknown() {
    let outcome = outcome_of_a_write(&Err(write_of_unknown_outcome()), InjectedBug::None);

    assert_eq!(outcome, KvOutcome::WriteUnknown);
}

#[test]
fn injected_bug_reports_a_write_of_unknown_outcome_as_done() {
    let outcome = outcome_of_a_write(&Err(write_of_unknown_outcome()), InjectedBug::UnknownWritesReportedDone);

    assert_eq!(outcome, KvOutcome::Written);
}

/// The bug is about writes nobody knows the outcome of. A write that was refused is still reported as refused.
#[test]
fn injected_bug_still_reports_a_refused_write_as_not_written() {
    let outcome = outcome_of_a_write(&Err(refused_write()), InjectedBug::UnknownWritesReportedDone);

    assert_eq!(outcome, KvOutcome::NotWritten);
}

#[tokio::test(start_paused = true)]
async fn node_goes_on_after_a_read_that_was_answered() {
    let history = History::new();

    let can_go_on = can_go_on_after_a_read(&Ok(None), &history);

    assert!(can_go_on);
}

#[tokio::test(start_paused = true)]
async fn node_stops_using_a_store_whose_read_could_not_be_proven_current() {
    let history = History::new();
    let not_current = KvError::NotCurrent {
        reason: AppendError::Fenced,
    };

    let can_go_on = can_go_on_after_a_read(&Err(not_current), &history);

    assert!(!can_go_on);
}

#[tokio::test(start_paused = true)]
async fn node_stops_using_a_store_whose_materializer_stopped() {
    let history = History::new();
    let stopped = KvError::Materializer(RunnerError::StreamEnded);

    let can_go_on = can_go_on_after_a_read(&Err(stopped), &history);

    assert!(!can_go_on);
}

#[tokio::test(start_paused = true)]
async fn node_goes_on_after_a_read_that_only_failed_this_time() {
    let history = History::new();
    let damaged = KvError::DamagedBucket {
        bucket: 3,
        reason: "a test says so",
    };

    let can_go_on = can_go_on_after_a_read(&Err(damaged), &history);

    assert!(can_go_on);
}

#[tokio::test(start_paused = true)]
async fn node_stops_using_a_store_that_found_the_log_truncated() {
    let history = History::new();

    let can_go_on = can_go_on_after_a_read(&Err(read_of_a_truncated_log()), &history);

    assert!(!can_go_on);
}

/// The materializer needed record 11 and the log starts at 15: its checkpoint was 10.
#[tokio::test(start_paused = true)]
async fn read_that_found_the_log_truncated_is_recorded_with_the_checkpoint() {
    let history = History::new();

    let _ = can_go_on_after_a_read(&Err(read_of_a_truncated_log()), &history);

    assert_eq!(
        history.events()[0].event,
        Event::TruncatedPastCheckpoint {
            checkpoint: SequenceNumber::new(10),
            low_water_mark: SequenceNumber::new(15),
        }
    );
}

#[tokio::test(start_paused = true)]
async fn read_that_was_answered_records_nothing_about_truncation() {
    let history = History::new();

    let _ = can_go_on_after_a_read(&Ok(None), &history);

    assert_eq!(history.events(), Vec::new());
}

#[tokio::test(start_paused = true)]
async fn run_declares_the_barrier_and_one_delete_per_key_as_fillers() {
    let history = History::new();

    declare_fillers(&history);

    let mut filler_count = 0;
    for timed in history.events() {
        if matches!(timed.event, Event::FillerDeclared { .. }) {
            filler_count += 1;
        }
    }
    assert_eq!(filler_count, 1 + KEY_COUNT);
}

#[test]
fn stores_of_a_simulation_keep_the_takeover_policy_they_are_asked_for() {
    let settings = store_settings(TakeoverPolicy::Force);

    assert_eq!(settings.takeover, TakeoverPolicy::Force);
}

#[test]
fn stores_of_a_simulation_checkpoint_every_two_seconds() {
    let settings = store_settings(TakeoverPolicy::WaitForLeaseExpiry);

    assert_eq!(settings.checkpoint_after, Duration::from_secs(2));
}

#[test]
fn keys_are_named_after_their_number() {
    assert_eq!(name_key(4), Bytes::from("key-4"));
}

#[tokio::test(start_paused = true)]
async fn node_draws_calls_on_the_keys_of_the_simulated_key_space() {
    let mut node = compute_node_with_seed(3).await;
    let mut known_keys = Vec::new();
    for key_number in 0..KEY_COUNT {
        known_keys.push(name_key(key_number));
    }

    let mut calls_on_unknown_keys = Vec::new();
    for _draw in 0..200 {
        let call = node.next_call();
        if !known_keys.contains(call.key()) {
            calls_on_unknown_keys.push(call);
        }
    }

    assert_eq!(calls_on_unknown_keys, Vec::new());
}

#[tokio::test(start_paused = true)]
async fn every_put_of_a_node_writes_a_value_of_its_own() {
    let mut node = compute_node_with_seed(3).await;

    let mut values = BTreeSet::new();
    let mut put_count = 0;
    for _draw in 0..200 {
        if let KvCall::Put { value, .. } = node.next_call() {
            values.insert(value);
            put_count += 1;
        }
    }

    assert_eq!(values.len(), put_count);
}

#[tokio::test(start_paused = true)]
async fn values_name_the_node_and_the_number_of_the_put() {
    let mut node = compute_node_with_seed(3).await;

    let mut first_value = None;
    while first_value.is_none() {
        if let KvCall::Put { value, .. } = node.next_call() {
            first_value = Some(value);
        }
    }

    assert_eq!(first_value, Some(Bytes::from("compute-1-value-1")));
}

#[tokio::test(start_paused = true)]
async fn node_draws_puts_deletes_and_reads() {
    let mut node = compute_node_with_seed(3).await;

    let mut has_drawn_a_put = false;
    let mut has_drawn_a_delete = false;
    let mut has_drawn_a_read = false;
    for _draw in 0..200 {
        match node.next_call() {
            KvCall::Put { .. } => has_drawn_a_put = true,
            KvCall::Delete { .. } => has_drawn_a_delete = true,
            KvCall::Get { .. } => has_drawn_a_read = true,
        }
    }

    assert_eq!(
        [has_drawn_a_put, has_drawn_a_delete, has_drawn_a_read],
        [true, true, true]
    );
}

#[tokio::test(start_paused = true)]
async fn nodes_with_the_same_seed_draw_the_same_calls() {
    let mut first_node = compute_node_with_seed(9).await;
    let mut second_node = compute_node_with_seed(9).await;

    let mut first_calls = Vec::new();
    let mut second_calls = Vec::new();
    for _draw in 0..50 {
        first_calls.push(first_node.next_call());
        second_calls.push(second_node.next_call());
    }

    assert_eq!(first_calls, second_calls);
}
