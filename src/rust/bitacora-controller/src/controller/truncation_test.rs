//! Tests of truncation.
//!
//! # What is covered
//!
//! - The rule for how far the mark may go, as a pure function: the slowest materializer, a materializer without a
//!   checkpoint, the owner's request, and both together.
//! - The rule for where retention stops it, as a pure function: the open segment, a recently sealed segment, a
//!   stream without an open segment.
//! - Which segments count as below a mark.
//! - The step against a cluster: what is deleted and what the mark becomes for each of the cases above, and that a
//!   materializer registered late does not move the mark back.
//!
//! # How the fixtures work
//!
//! The pure tests use `stream_record` from the shared test support: segments 1 to 9 and 10 to 19 sealed long before
//! [`NOW`], and an open segment from 20. The cluster tests use `cluster_with_segments(3, n)`: `n` sealed segments of
//! one record each, so that segment `k` holds exactly record `k`, and an open one after them.

use bitacora_meta::RetentionPolicy;

use super::super::test_support::{
    STREAM, checkpoint_at, checkpoint_record, cluster_with_segments, first_sealed_segment, listed_segments,
    open_segment, renew_registrations, run_pass, stream, stream_record,
};
use super::*;

/// The moment the pure tests are evaluated at: hours after both seals of [`stream_record`].
const NOW: Timestamp = Timestamp::from_unix_millis(10_000_000);

/// When the second segment of [`stream_record`] was sealed, in milliseconds.
const SECOND_SEAL_AT_MILLIS: u64 = 2_000;

/// The stream of [`stream_record`] with the materializers `names` registered.
fn stream_with_materializers(names: &[&str]) -> StreamRecord {
    let mut stream = stream_record();
    for name in names {
        stream.materializers.insert((*name).to_owned());
    }
    stream
}

#[test]
fn nothing_may_be_deleted_without_a_materializer_or_a_request() {
    let allowed_mark = allowed_low_water_mark(&stream_record(), &[]);

    assert_eq!(allowed_mark, None);
}

#[test]
fn mark_follows_the_slowest_materializer() {
    let stream = stream_with_materializers(&["blocks", "pages"]);
    let checkpoints = [checkpoint_record("blocks", 15), checkpoint_record("pages", 9)];

    let allowed_mark = allowed_low_water_mark(&stream, &checkpoints);

    assert_eq!(allowed_mark, Some(SequenceNumber::new(10)));
}

#[test]
fn materializer_that_never_checkpointed_pins_the_log() {
    let stream = stream_with_materializers(&["pages"]);

    let allowed_mark = allowed_low_water_mark(&stream, &[checkpoint_record("pages", 0)]);

    assert_eq!(allowed_mark, Some(SequenceNumber::FIRST));
}

#[test]
fn registered_materializer_without_a_checkpoint_record_pins_the_log() {
    let stream = stream_with_materializers(&["blocks", "pages"]);

    let allowed_mark = allowed_low_water_mark(&stream, &[checkpoint_record("blocks", 15)]);

    assert_eq!(allowed_mark, Some(SequenceNumber::FIRST));
}

#[test]
fn owner_request_alone_sets_the_mark() {
    let mut stream = stream_record();
    stream.truncate_before = Some(SequenceNumber::new(12));

    let allowed_mark = allowed_low_water_mark(&stream, &[]);

    assert_eq!(allowed_mark, Some(SequenceNumber::new(12)));
}

#[test]
fn owner_request_cannot_pass_a_materializers_checkpoint() {
    let mut stream = stream_with_materializers(&["pages"]);
    stream.truncate_before = Some(SequenceNumber::new(18));

    let allowed_mark = allowed_low_water_mark(&stream, &[checkpoint_record("pages", 9)]);

    assert_eq!(allowed_mark, Some(SequenceNumber::new(10)));
}

#[test]
fn owner_request_below_every_checkpoint_sets_the_mark() {
    let mut stream = stream_with_materializers(&["pages"]);
    stream.truncate_before = Some(SequenceNumber::new(5));

    let allowed_mark = allowed_low_water_mark(&stream, &[checkpoint_record("pages", 9)]);

    assert_eq!(allowed_mark, Some(SequenceNumber::new(5)));
}

#[test]
fn checkpoint_of_an_unregistered_materializer_is_ignored() {
    let stream = stream_with_materializers(&["pages"]);
    let checkpoints = [checkpoint_record("gone", 2), checkpoint_record("pages", 15)];

    let allowed_mark = allowed_low_water_mark(&stream, &checkpoints);

    assert_eq!(allowed_mark, Some(SequenceNumber::new(16)));
}

#[test]
fn mark_never_enters_the_open_segment() {
    let mark = hold_back_for_retention(&stream_record(), SequenceNumber::new(500), NOW);

    assert_eq!(mark, SequenceNumber::new(20));
}

#[test]
fn mark_below_the_retention_limit_is_kept_as_it_is() {
    let mark = hold_back_for_retention(&stream_record(), SequenceNumber::new(12), NOW);

    assert_eq!(mark, SequenceNumber::new(12));
}

#[test]
fn retention_period_holds_the_mark_at_a_recently_sealed_segment() {
    let mut stream = stream_record();
    stream.retention = RetentionPolicy::keep_for(Duration::from_hours(1));
    let a_minute_after_the_second_seal = Timestamp::from_unix_millis(SECOND_SEAL_AT_MILLIS + 60_000);

    let mark = hold_back_for_retention(&stream, SequenceNumber::new(500), a_minute_after_the_second_seal);

    assert_eq!(mark, SequenceNumber::new(1));
}

#[test]
fn retention_period_releases_segments_sealed_long_ago() {
    let mut stream = stream_record();
    stream.retention = RetentionPolicy::keep_for(Duration::from_hours(1));

    let mark = hold_back_for_retention(&stream, SequenceNumber::new(500), NOW);

    assert_eq!(mark, SequenceNumber::new(20));
}

#[test]
fn stream_with_only_sealed_segments_can_be_truncated_to_its_end() {
    let mut stream = stream_record();
    stream.segments.pop();

    let mark = hold_back_for_retention(&stream, SequenceNumber::new(500), NOW);

    assert_eq!(mark, SequenceNumber::new(20));
}

#[test]
fn sealed_segment_that_ends_before_the_mark_is_below_it() {
    let segment = first_sealed_segment();

    assert!(is_below_the_mark(&segment, SequenceNumber::new(10)));
}

#[test]
fn sealed_segment_whose_last_record_is_the_mark_is_not_below_it() {
    let segment = first_sealed_segment();

    assert!(!is_below_the_mark(&segment, SequenceNumber::new(9)));
}

#[test]
fn open_segment_is_never_below_the_mark() {
    let segment = open_segment();

    assert!(!is_below_the_mark(&segment, SequenceNumber::new(500)));
}

#[tokio::test(start_paused = true)]
async fn nothing_is_deleted_without_a_materializer_or_a_request() {
    let cluster = cluster_with_segments(3, 3).await;

    let report = run_pass(&cluster).await;

    assert_eq!(report.segments_deleted, 0);
    assert_eq!(listed_segments(&cluster).await, [1, 2, 3, 4]);
}

#[tokio::test(start_paused = true)]
async fn segments_below_every_checkpoint_are_deleted() {
    let cluster = cluster_with_segments(3, 3).await;
    checkpoint_at(&cluster, "pages", 2).await;

    let report = run_pass(&cluster).await;

    assert_eq!(report.segments_deleted, 2);
    assert_eq!(listed_segments(&cluster).await, [3, 4]);
}

#[tokio::test(start_paused = true)]
async fn mark_moves_to_one_past_the_checkpoint() {
    let cluster = cluster_with_segments(3, 3).await;
    checkpoint_at(&cluster, "pages", 2).await;

    run_pass(&cluster).await;

    assert_eq!(stream(&cluster).await.low_water_mark, SequenceNumber::new(3));
}

#[tokio::test(start_paused = true)]
async fn slowest_materializer_pins_the_log() {
    let cluster = cluster_with_segments(3, 3).await;
    checkpoint_at(&cluster, "pages", 3).await;
    checkpoint_at(&cluster, "blocks", 0).await;

    let report = run_pass(&cluster).await;

    assert_eq!(report.segments_deleted, 0);
    assert_eq!(listed_segments(&cluster).await, [1, 2, 3, 4]);
}

#[tokio::test(start_paused = true)]
async fn unregistering_the_slow_materializer_releases_the_log() {
    let cluster = cluster_with_segments(3, 3).await;
    checkpoint_at(&cluster, "pages", 3).await;
    checkpoint_at(&cluster, "blocks", 0).await;
    cluster
        .metadata()
        .unregister_materializer(STREAM, "blocks")
        .await
        .expect("the materializer is registered");

    run_pass(&cluster).await;

    assert_eq!(listed_segments(&cluster).await, [4]);
}

#[tokio::test(start_paused = true)]
async fn owner_request_deletes_whole_segments_only() {
    let cluster = cluster_with_segments(3, 3).await;
    cluster
        .metadata()
        .update_stream(STREAM, |record| {
            record.truncate_before = Some(SequenceNumber::new(3));
            Ok(())
        })
        .await
        .expect("the stream exists");

    run_pass(&cluster).await;

    assert_eq!(listed_segments(&cluster).await, [3, 4]);
    assert_eq!(stream(&cluster).await.low_water_mark, SequenceNumber::new(3));
}

#[tokio::test(start_paused = true)]
async fn open_segment_survives_a_checkpoint_past_its_start() {
    let cluster = cluster_with_segments(3, 2).await;
    checkpoint_at(&cluster, "pages", 500).await;

    run_pass(&cluster).await;

    assert_eq!(listed_segments(&cluster).await, [3]);
    assert_eq!(stream(&cluster).await.low_water_mark, SequenceNumber::new(3));
}

/// Sets the retention period of the test stream to one hour.
async fn keep_sealed_segments_for_an_hour(cluster: &bitacora_testkit::Cluster) {
    cluster
        .metadata()
        .update_stream(STREAM, |record| {
            record.retention = RetentionPolicy::keep_for(Duration::from_hours(1));
            Ok(())
        })
        .await
        .expect("the stream exists");
}

#[tokio::test(start_paused = true)]
async fn retention_period_keeps_recently_sealed_segments() {
    let cluster = cluster_with_segments(3, 3).await;
    keep_sealed_segments_for_an_hour(&cluster).await;
    checkpoint_at(&cluster, "pages", 3).await;

    let report = run_pass(&cluster).await;

    assert_eq!(report.segments_deleted, 0);
}

#[tokio::test(start_paused = true)]
async fn segments_are_deleted_once_their_retention_period_is_over() {
    let cluster = cluster_with_segments(3, 3).await;
    keep_sealed_segments_for_an_hour(&cluster).await;
    checkpoint_at(&cluster, "pages", 3).await;
    run_pass(&cluster).await;
    tokio::time::advance(Duration::from_hours(2)).await;
    renew_registrations(&cluster).await;

    run_pass(&cluster).await;

    // The jump of two hours makes the fixture's writer roll, so only the first three segments are looked at.
    let oldest_segment_left = listed_segments(&cluster).await.into_iter().min();
    assert!(oldest_segment_left > Some(3), "oldest left is {oldest_segment_left:?}");
}

#[tokio::test(start_paused = true)]
async fn materializer_registered_after_a_truncation_starts_at_the_mark() {
    let cluster = cluster_with_segments(3, 3).await;
    checkpoint_at(&cluster, "pages", 2).await;
    run_pass(&cluster).await;

    let late_checkpoint = cluster
        .metadata()
        .register_materializer(STREAM, "blocks")
        .await
        .expect("the stream exists");

    assert_eq!(late_checkpoint.checkpoint, SequenceNumber::new(2));
}

#[tokio::test(start_paused = true)]
async fn materializer_registered_after_a_truncation_does_not_move_the_mark_back() {
    let cluster = cluster_with_segments(3, 3).await;
    checkpoint_at(&cluster, "pages", 2).await;
    run_pass(&cluster).await;
    cluster
        .metadata()
        .register_materializer(STREAM, "blocks")
        .await
        .expect("the stream exists");

    run_pass(&cluster).await;

    assert_eq!(stream(&cluster).await.low_water_mark, SequenceNumber::new(3));
}
