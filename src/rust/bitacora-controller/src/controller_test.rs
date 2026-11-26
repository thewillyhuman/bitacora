//! Tests of the pass as a whole and of the plumbing its steps share.
//!
//! # What is covered
//!
//! - A pass over a cluster without streams does nothing, and a pass fails only when the metadata store cannot be
//!   listed.
//! - A stream that is deleted while a pass is at work is not counted as an error of the pass; a stream that fails
//!   for any other reason is.
//! - The condition every write about a copy carries: the copy is found only while it is still the one that was
//!   looked at.
//! - Asking a replica or the object store with a time limit: an answer, a refusal and silence are told apart.
//!
//! The steps of a pass are tested next to each step, in `controller/*_test.rs`.
//!
//! # How the fixtures work
//!
//! `stream_record` from the shared test support gives a record whose segments each have one good copy on
//! `replica-a`. The time-limit tests run on a paused clock, so the limit they wait for takes no real time.

use std::future::pending;

use bitacora_meta::{CopyState, StoreUnavailable};
use bitacora_testkit::Cluster;
use bitacora_types::SegmentId;

use super::test_support::{
    CONTROLLER_IDENTITY, STREAM, controller_named, create_stream, replica_id, run_pass, stream, stream_record,
};
use super::*;

/// The time limit the plumbing tests use.
const TIME_LIMIT: Duration = Duration::from_secs(5);

/// The copy that [`stream_record`] gives every segment.
fn copy_on_replica_a() -> CopyRecord {
    CopyRecord {
        replica_id: replica_id("replica-a"),
        state: CopyState::Good,
    }
}

#[tokio::test(start_paused = true)]
async fn pass_over_a_cluster_without_streams_does_nothing() {
    let cluster = Cluster::start(3).await;

    let report = run_pass(&cluster).await;

    assert_eq!(report, PassReport::default());
}

#[tokio::test(start_paused = true)]
async fn pass_fails_when_the_metadata_store_cannot_be_listed() {
    let cluster = Cluster::start(3).await;
    create_stream(&cluster).await;
    cluster.key_value_store().set_unavailable(true);
    let controller = test_support::controller_named(&cluster, test_support::CONTROLLER_IDENTITY);

    let pass = controller.run_pass().await;

    assert!(pass.is_err(), "{pass:?}");
}

#[tokio::test(start_paused = true)]
async fn pass_that_handles_every_stream_reports_no_stream_errors() {
    let cluster = Cluster::start(3).await;
    create_stream(&cluster).await;

    let report = run_pass(&cluster).await;

    assert_eq!(report.streams_with_errors, 0);
}

/// What a pass runs into when somebody deletes a stream between its listing and its turn: the steps find no stream.
#[tokio::test(start_paused = true)]
async fn handling_a_stream_that_was_deleted_meanwhile_fails_as_not_found() {
    let cluster = Cluster::start(3).await;
    create_stream(&cluster).await;
    let listed_stream = stream(&cluster).await;
    let controller = controller_named(&cluster, CONTROLLER_IDENTITY);
    let survey = controller.survey().await.expect("the cluster can be surveyed");
    cluster
        .metadata()
        .delete_stream(STREAM)
        .await
        .expect("the stream is deleted");

    let handled = controller
        .handle_stream(&listed_stream, &survey, &mut PassReport::default())
        .await;

    assert_eq!(
        handled,
        Err(MetaError::StreamNotFound {
            name: STREAM.to_owned()
        })
    );
}

/// A stream that is gone has nothing left to handle. Counting it would raise an alert on every deletion.
#[test]
fn stream_deleted_during_the_pass_is_not_an_error_of_the_pass() {
    let mut report = PassReport::default();
    let deleted_meanwhile = MetaError::StreamNotFound {
        name: STREAM.to_owned(),
    };

    note_unhandled_stream(STREAM, &deleted_meanwhile, &mut report);

    assert_eq!(report.streams_with_errors, 0);
}

#[test]
fn stream_that_could_not_be_handled_for_another_reason_is_counted() {
    let mut report = PassReport::default();
    let store_is_down = MetaError::Unavailable(StoreUnavailable {
        reason: "the metadata store did not answer".to_owned(),
    });

    note_unhandled_stream(STREAM, &store_is_down, &mut report);

    assert_eq!(report.streams_with_errors, 1);
}

/// Another stream going missing says nothing about this one.
#[test]
fn another_stream_that_is_not_found_is_counted() {
    let mut report = PassReport::default();
    let other_stream_missing = MetaError::StreamNotFound {
        name: "payments".to_owned(),
    };

    note_unhandled_stream(STREAM, &other_stream_missing, &mut report);

    assert_eq!(report.streams_with_errors, 1);
}

#[test]
fn copy_that_is_unchanged_is_found() {
    let mut record = stream_record();

    let found = unchanged_copy_mut(&mut record, SegmentId::new(1), 0, &copy_on_replica_a());

    assert_eq!(found.cloned(), Some(copy_on_replica_a()));
}

#[test]
fn copy_whose_state_changed_is_not_found() {
    let mut record = stream_record();
    record.segments[0].copies[0].state = CopyState::Bad;

    let found = unchanged_copy_mut(&mut record, SegmentId::new(1), 0, &copy_on_replica_a());

    assert!(found.is_none());
}

#[test]
fn copy_that_moved_to_another_replica_is_not_found() {
    let mut record = stream_record();
    record.segments[0].copies[0].replica_id = replica_id("replica-z");

    let found = unchanged_copy_mut(&mut record, SegmentId::new(1), 0, &copy_on_replica_a());

    assert!(found.is_none());
}

#[test]
fn copy_of_a_segment_that_was_deleted_is_not_found() {
    let mut record = stream_record();
    record.segments.remove(0);

    let found = unchanged_copy_mut(&mut record, SegmentId::new(1), 0, &copy_on_replica_a());

    assert!(found.is_none());
}

#[test]
fn copy_at_a_position_the_replica_set_no_longer_has_is_not_found() {
    let mut record = stream_record();

    let found = unchanged_copy_mut(&mut record, SegmentId::new(1), 3, &copy_on_replica_a());

    assert!(found.is_none());
}

#[tokio::test(start_paused = true)]
async fn replica_answer_in_time_is_passed_on() {
    let answer = replica_answer_within(TIME_LIMIT, async { Ok(7) }).await;

    assert!(matches!(answer, Ok(7)));
}

#[tokio::test(start_paused = true)]
async fn replica_refusal_is_told_apart_from_silence() {
    let refusal: Result<(), ReplicaError> = Err(ReplicaError::Draining);

    let answer = replica_answer_within(TIME_LIMIT, async { refusal }).await;

    assert!(matches!(answer, Err(ReplicaFailure::Refused(ReplicaError::Draining))));
}

#[tokio::test(start_paused = true)]
async fn replica_that_never_answers_times_out() {
    let silence = pending::<Result<(), ReplicaError>>();

    let answer = replica_answer_within(TIME_LIMIT, silence).await;

    assert!(matches!(answer, Err(ReplicaFailure::TimedOut)));
}

#[tokio::test(start_paused = true)]
async fn replica_is_waited_for_no_longer_than_the_time_limit() {
    let asked_at = tokio::time::Instant::now();

    let _timed_out = replica_answer_within(TIME_LIMIT, pending::<Result<(), ReplicaError>>()).await;

    assert_eq!(asked_at.elapsed(), TIME_LIMIT);
}

#[tokio::test(start_paused = true)]
async fn object_store_answer_in_time_is_passed_on() {
    let answer = object_store_answer_within(TIME_LIMIT, async { Ok(7) }).await;

    assert!(matches!(answer, Ok(7)));
}

#[tokio::test(start_paused = true)]
async fn object_store_error_is_told_apart_from_silence() {
    let store_error: Result<(), ArtifactError> = Err(ArtifactError::Unavailable {
        reason: "injected outage".to_owned(),
    });

    let answer = object_store_answer_within(TIME_LIMIT, async { store_error }).await;

    assert!(matches!(
        answer,
        Err(ObjectStoreFailure::Failed(ArtifactError::Unavailable { .. }))
    ));
}

#[tokio::test(start_paused = true)]
async fn object_store_that_never_answers_times_out() {
    let silence = pending::<Result<(), ArtifactError>>();

    let answer = object_store_answer_within(TIME_LIMIT, silence).await;

    assert!(matches!(answer, Err(ObjectStoreFailure::TimedOut)));
}

#[test]
fn replica_timeout_says_what_happened() {
    assert_eq!(
        ReplicaFailure::TimedOut.to_string(),
        "the replica did not answer in time"
    );
}

#[test]
fn object_store_timeout_says_what_happened() {
    assert_eq!(
        ObjectStoreFailure::TimedOut.to_string(),
        "the object store did not answer in time"
    );
}
