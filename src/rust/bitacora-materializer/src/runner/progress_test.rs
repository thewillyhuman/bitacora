//! Tests of the progress a runner publishes, with the two ends against each other.
//!
//! # What is covered
//!
//! - What a handle sees: the starting position, the latest position, and no reason while the runner runs.
//! - Waiting: for a record to be applied and for a checkpoint to be committed, also when it already happened.
//! - Stopping: a waiter is woken when the publisher goes away, with the reason if one was left and without one
//!   otherwise, and only the first reason is kept.
//! - What [`RunnerStopped`] says.
//!
//! # How the fixtures work
//!
//! A [`ProgressPublisher`] stands for the runner; no cluster is involved. `progress_at` builds the positions a
//! runner publishes from three numbers. A waiter that must be woken later runs in a task of its own, and the test
//! yields once so that the waiter is really waiting before the publisher moves.
//!
//! The same handle is tested against a real runner in `runner_test.rs`.

use tokio::task::yield_now;

use super::*;

/// The positions of a runner that has applied `applied`, committed `committed` and knows of `durable`.
fn progress_at(applied: u64, committed: u64, durable: u64) -> Progress {
    Progress {
        applied_sequence: SequenceNumber::new(applied),
        committed_checkpoint: SequenceNumber::new(committed),
        durable_sequence: SequenceNumber::new(durable),
    }
}

#[test]
fn handle_starts_at_the_restored_checkpoint() {
    let publisher = ProgressPublisher::starting_at(SequenceNumber::new(7));

    let latest = publisher.handle().latest();

    assert_eq!(latest, progress_at(7, 7, 7));
}

#[test]
fn handle_sees_what_was_published_last() {
    let publisher = ProgressPublisher::starting_at(SequenceNumber::ZERO);
    let handle = publisher.handle();

    publisher.publish(progress_at(5, 0, 9));
    publisher.publish(progress_at(8, 5, 9));

    assert_eq!(handle.latest(), progress_at(8, 5, 9));
}

#[test]
fn handle_taken_after_a_publication_sees_it() {
    let publisher = ProgressPublisher::starting_at(SequenceNumber::ZERO);
    publisher.publish(progress_at(5, 0, 9));

    let handle = publisher.handle();

    assert_eq!(handle.latest(), progress_at(5, 0, 9));
}

#[test]
fn handle_of_a_runner_that_runs_has_no_stop_reason() {
    let publisher = ProgressPublisher::starting_at(SequenceNumber::ZERO);

    let reason = publisher.handle().stop_reason();

    assert_eq!(reason, None);
}

#[tokio::test]
async fn wait_for_a_record_that_is_applied_already_returns_at_once() {
    let publisher = ProgressPublisher::starting_at(SequenceNumber::new(7));
    let mut handle = publisher.handle();

    let reached = handle.wait_until_applied(SequenceNumber::new(7)).await;

    assert_eq!(reached, Ok(progress_at(7, 7, 7)));
}

#[tokio::test]
async fn waiter_is_woken_once_its_record_is_applied() {
    let publisher = ProgressPublisher::starting_at(SequenceNumber::ZERO);
    let mut handle = publisher.handle();
    let waiter = tokio::spawn(async move { handle.wait_until_applied(SequenceNumber::new(5)).await });
    yield_now().await;

    publisher.publish(progress_at(4, 0, 9));
    publisher.publish(progress_at(6, 0, 9));

    let reached = waiter.await.expect("the waiter does not panic");
    assert_eq!(reached, Ok(progress_at(6, 0, 9)));
}

#[tokio::test]
async fn waiter_is_woken_once_its_checkpoint_is_committed() {
    let publisher = ProgressPublisher::starting_at(SequenceNumber::ZERO);
    let mut handle = publisher.handle();
    let waiter = tokio::spawn(async move { handle.wait_until_committed(SequenceNumber::new(5)).await });
    yield_now().await;

    publisher.publish(progress_at(9, 0, 9));
    publisher.publish(progress_at(9, 9, 9));

    let reached = waiter.await.expect("the waiter does not panic");
    assert_eq!(reached, Ok(progress_at(9, 9, 9)));
}

#[tokio::test]
async fn applied_record_does_not_satisfy_a_wait_for_its_checkpoint() {
    let publisher = ProgressPublisher::starting_at(SequenceNumber::ZERO);
    let mut handle = publisher.handle();
    publisher.publish(progress_at(9, 0, 9));

    drop(publisher);
    let reached = handle.wait_until_committed(SequenceNumber::new(5)).await;

    assert_eq!(reached, Err(RunnerStopped { reason: None }));
}

#[tokio::test]
async fn waiter_of_a_runner_that_stopped_gets_the_reason() {
    let publisher = ProgressPublisher::starting_at(SequenceNumber::ZERO);
    let mut handle = publisher.handle();
    let waiter = tokio::spawn(async move { handle.wait_until_applied(SequenceNumber::new(5)).await });
    yield_now().await;

    publisher.leave_stop_reason(RunnerError::StreamEnded);
    drop(publisher);

    let reached = waiter.await.expect("the waiter does not panic");
    assert_eq!(
        reached,
        Err(RunnerStopped {
            reason: Some(RunnerError::StreamEnded)
        })
    );
}

#[tokio::test]
async fn waiter_of_a_runner_that_was_dropped_gets_no_reason() {
    let publisher = ProgressPublisher::starting_at(SequenceNumber::ZERO);
    let mut handle = publisher.handle();

    drop(publisher);
    let reached = handle.wait_until_applied(SequenceNumber::new(5)).await;

    assert_eq!(reached, Err(RunnerStopped { reason: None }));
}

#[test]
fn handle_keeps_the_last_position_after_the_runner_stopped() {
    let publisher = ProgressPublisher::starting_at(SequenceNumber::ZERO);
    let handle = publisher.handle();
    publisher.publish(progress_at(8, 5, 9));

    drop(publisher);

    assert_eq!(handle.latest(), progress_at(8, 5, 9));
}

#[test]
fn stop_reason_is_readable_once_it_was_left() {
    let publisher = ProgressPublisher::starting_at(SequenceNumber::ZERO);
    let handle = publisher.handle();

    publisher.leave_stop_reason(RunnerError::StreamEnded);

    assert_eq!(handle.stop_reason(), Some(RunnerError::StreamEnded));
}

#[test]
fn only_the_first_stop_reason_is_kept() {
    let publisher = ProgressPublisher::starting_at(SequenceNumber::ZERO);
    let handle = publisher.handle();
    let fenced = RunnerError::Fenced {
        held_token: 1,
        current_token: 2,
    };

    publisher.leave_stop_reason(fenced.clone());
    publisher.leave_stop_reason(RunnerError::StreamEnded);

    assert_eq!(handle.stop_reason(), Some(fenced));
}

#[test]
fn stopped_runner_with_a_reason_says_the_reason() {
    let stopped = RunnerStopped {
        reason: Some(RunnerError::StreamEnded),
    };

    assert_eq!(
        stopped.to_string(),
        "the materializer runner has stopped: the stream ended"
    );
}

#[test]
fn runner_stopped_from_outside_says_so() {
    let stopped = RunnerStopped { reason: None };

    assert_eq!(stopped.to_string(), "the materializer runner was stopped from outside");
}
