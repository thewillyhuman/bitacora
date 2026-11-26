//! Tests of the leftover sweep.
//!
//! # What is covered
//!
//! - The rule that decides whether a copy may be deleted, as a pure function: a copy its segment lists, a copy
//!   outside the replica set, a copy of the open segment, copies of segments that are no longer listed, older or
//!   newer than what is, above or below the low-water mark.
//! - The sweep against a cluster: copies of truncated segments, a replica that was away when its copies became
//!   leftovers, the copy a repair moved away from, a deleted stream and when it is forgotten, and a stream the
//!   metadata store does not know.
//!
//! # How the fixtures work
//!
//! The pure tests use `stream_record` from the shared test support (segments 1 to 9 and 10 to 19 sealed, an open one
//! from 20, each with its one copy on `replica-a`) and `held_copy` for what a replica reports. The cluster tests use
//! `cluster_with_segments` and `cluster_with_a_sealed_segment`.

use bitacora_meta::{KeyValueStore, Mutation};
use bitacora_types::SequenceNumber;

use super::super::test_support::{
    STREAM, Writes, checkpoint_at, cluster_with_a_sealed_segment, cluster_with_segments, held_copy, held_segments,
    lose_node, renew_registrations, replica_id, run_pass, stream_record,
};
use super::*;

/// The stream of [`stream_record`] after its second segment was deleted while its first is still listed: the first
/// is tiered, and its object could not be deleted yet. The low-water mark is past both.
fn stream_with_a_deleted_segment_after_a_listed_one() -> StreamRecord {
    let mut stream = stream_record();
    stream.segments.remove(1);
    stream.low_water_mark = SequenceNumber::new(20);
    stream
}

/// The stream of [`stream_record`] with no segment listed and everything below record 20 truncated.
fn stream_without_segments() -> StreamRecord {
    let mut stream = stream_record();
    stream.segments.clear();
    stream.low_water_mark = SequenceNumber::new(20);
    stream
}

#[test]
fn copy_of_a_listed_segment_on_its_replica_is_kept() {
    let is_leftover = is_leftover_copy(&stream_record(), &replica_id("replica-a"), &held_copy(1, Some(9)));

    assert!(!is_leftover);
}

#[test]
fn copy_of_a_sealed_segment_on_a_replica_outside_its_set_is_leftover() {
    let is_leftover = is_leftover_copy(&stream_record(), &replica_id("replica-z"), &held_copy(1, Some(9)));

    assert!(is_leftover);
}

#[test]
fn copy_of_the_open_segment_is_never_leftover() {
    let is_leftover = is_leftover_copy(&stream_record(), &replica_id("replica-z"), &held_copy(3, None));

    assert!(!is_leftover);
}

#[test]
fn copy_older_than_every_listed_segment_is_leftover() {
    let mut stream = stream_record();
    stream.segments.remove(0);

    let is_leftover = is_leftover_copy(&stream, &replica_id("replica-a"), &held_copy(1, Some(9)));

    assert!(is_leftover);
}

#[test]
fn copy_newer_than_every_listed_segment_is_kept() {
    let is_leftover = is_leftover_copy(&stream_record(), &replica_id("replica-a"), &held_copy(4, Some(30)));

    assert!(!is_leftover);
}

#[test]
fn copy_of_a_segment_deleted_after_one_that_is_still_listed_is_leftover() {
    let stream = stream_with_a_deleted_segment_after_a_listed_one();

    let is_leftover = is_leftover_copy(&stream, &replica_id("replica-a"), &held_copy(2, Some(19)));

    assert!(is_leftover);
}

#[test]
fn unlisted_copy_that_reaches_the_mark_is_kept_between_listed_segments() {
    let mut stream = stream_with_a_deleted_segment_after_a_listed_one();
    stream.low_water_mark = SequenceNumber::new(19);

    let is_leftover = is_leftover_copy(&stream, &replica_id("replica-a"), &held_copy(2, Some(19)));

    assert!(!is_leftover);
}

#[test]
fn unlisted_copy_without_records_is_kept_between_listed_segments() {
    let stream = stream_with_a_deleted_segment_after_a_listed_one();

    let is_leftover = is_leftover_copy(&stream, &replica_id("replica-a"), &held_copy(2, None));

    assert!(!is_leftover);
}

#[test]
fn copy_below_the_mark_of_a_stream_without_segments_is_leftover() {
    let is_leftover = is_leftover_copy(
        &stream_without_segments(),
        &replica_id("replica-a"),
        &held_copy(2, Some(19)),
    );

    assert!(is_leftover);
}

#[test]
fn copy_at_or_above_the_mark_of_a_stream_without_segments_is_kept() {
    let is_leftover = is_leftover_copy(
        &stream_without_segments(),
        &replica_id("replica-a"),
        &held_copy(3, Some(25)),
    );

    assert!(!is_leftover);
}

#[test]
fn copy_of_a_deleted_stream_is_leftover_whatever_it_holds() {
    let deleted_stream = DeletedStreamRecord {
        stream_id: held_copy(3, None).address.stream_id,
        name: STREAM.to_owned(),
    };
    let known_streams = known_streams_of(Vec::new(), &[deleted_stream]);

    let is_leftover = known_streams.is_leftover(&replica_id("replica-a"), &held_copy(3, None));

    assert!(is_leftover);
}

#[test]
fn copy_of_a_stream_the_metadata_store_does_not_know_is_kept() {
    let known_streams = known_streams_of(Vec::new(), &[]);

    let is_leftover = known_streams.is_leftover(&replica_id("replica-a"), &held_copy(1, Some(9)));

    assert!(!is_leftover);
}

#[tokio::test(start_paused = true)]
async fn deleted_segments_are_removed_from_their_replicas() {
    let cluster = cluster_with_segments(3, 3).await;
    checkpoint_at(&cluster, "pages", 2).await;

    let report = run_pass(&cluster).await;

    assert_eq!(report.copies_removed, 6);
    assert_eq!(held_segments(&cluster, 0).await, [3]);
}

#[tokio::test(start_paused = true)]
async fn replica_that_is_away_keeps_its_copies_of_deleted_segments() {
    let cluster = cluster_with_segments(3, 3).await;
    checkpoint_at(&cluster, "pages", 2).await;
    cluster.nodes()[2].set_reachable(false);

    run_pass(&cluster).await;

    assert_eq!(held_segments(&cluster, 2).await, [1, 2, 3]);
}

#[tokio::test(start_paused = true)]
async fn replica_that_missed_a_deletion_is_cleaned_up_when_it_returns() {
    let cluster = cluster_with_segments(3, 3).await;
    checkpoint_at(&cluster, "pages", 2).await;
    cluster.nodes()[2].set_reachable(false);
    run_pass(&cluster).await;
    cluster.nodes()[2].set_reachable(true);

    let report = run_pass(&cluster).await;

    assert_eq!(report.copies_removed, 2);
    assert_eq!(held_segments(&cluster, 2).await, [3]);
}

#[tokio::test(start_paused = true)]
async fn copy_replaced_by_repair_is_removed_when_its_replica_returns() {
    let cluster = cluster_with_a_sealed_segment(4, Writes::ThreeRecords).await;
    run_pass(&cluster).await;
    lose_node(&cluster, 2).await;
    run_pass(&cluster).await;
    cluster.nodes()[2].restart().await;
    renew_registrations(&cluster).await;

    let report = run_pass(&cluster).await;

    assert_eq!(report.copies_removed, 1);
    assert_eq!(held_segments(&cluster, 2).await, Vec::<u64>::new());
}

#[tokio::test(start_paused = true)]
async fn deleted_stream_is_removed_from_replicas() {
    let cluster = cluster_with_segments(3, 2).await;
    cluster
        .metadata()
        .delete_stream(STREAM)
        .await
        .expect("the stream exists");

    let report = run_pass(&cluster).await;

    assert_eq!(report.copies_removed, 6);
    assert_eq!(held_segments(&cluster, 0).await, Vec::<u64>::new());
}

#[tokio::test(start_paused = true)]
async fn deleted_stream_is_forgotten_once_its_copies_are_gone() {
    let cluster = cluster_with_segments(3, 2).await;
    cluster
        .metadata()
        .delete_stream(STREAM)
        .await
        .expect("the stream exists");

    run_pass(&cluster).await;

    assert_eq!(cluster.metadata().list_deleted_streams().await, Ok(Vec::new()));
}

#[tokio::test(start_paused = true)]
async fn deleted_stream_is_remembered_while_a_replica_has_not_been_cleaned() {
    let cluster = cluster_with_segments(3, 2).await;
    cluster.nodes()[2].set_reachable(false);
    cluster
        .metadata()
        .delete_stream(STREAM)
        .await
        .expect("the stream exists");

    run_pass(&cluster).await;

    let remembered_streams = cluster
        .metadata()
        .list_deleted_streams()
        .await
        .expect("the metadata store is available");
    assert_eq!(remembered_streams.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn deleted_stream_is_forgotten_once_the_last_replica_was_cleaned() {
    let cluster = cluster_with_segments(3, 2).await;
    cluster.nodes()[2].set_reachable(false);
    cluster
        .metadata()
        .delete_stream(STREAM)
        .await
        .expect("the stream exists");
    run_pass(&cluster).await;
    cluster.nodes()[2].set_reachable(true);

    run_pass(&cluster).await;

    assert_eq!(held_segments(&cluster, 2).await, Vec::<u64>::new());
    assert_eq!(cluster.metadata().list_deleted_streams().await, Ok(Vec::new()));
}

/// What a metadata store restored from an older snapshot does not know must survive until an operator rebuilds the
/// metadata from it.
#[tokio::test(start_paused = true)]
async fn segments_of_a_stream_the_metadata_store_does_not_know_are_left_alone() {
    let cluster = cluster_with_segments(3, 2).await;
    // Forget the stream without leaving a note, as a restore would.
    cluster
        .key_value_store()
        .transact(
            Vec::new(),
            vec![Mutation::Delete {
                key: format!("streams/{STREAM}"),
            }],
        )
        .await
        .expect("the key-value store is available");

    let report = run_pass(&cluster).await;

    assert_eq!(report.copies_removed, 0);
    assert_eq!(held_segments(&cluster, 0).await, [1, 2]);
}
