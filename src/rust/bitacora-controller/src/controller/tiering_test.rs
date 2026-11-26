//! Tests of the controller's tiering passes.
//!
//! # What is covered
//!
//! - The rules that decide when a segment is uploaded and when its local copies go, as pure functions.
//! - Upload: after the delay, only with a full set of good copies, retried after a failure, limited per
//!   pass, and never recorded for an object the controller cannot see.
//! - Release: local copies stay through the hot-read window, go after it, and stay if the object is gone.
//! - Deletion: truncation and stream deletion remove objects, and wait for an object store that is down.
//! - The reason for all of it: records survive the loss of every replica disk.
//!
//! # How the fixtures work
//!
//! The integration tests run a real controller against the testkit cluster, whose replicas upload to the
//! cluster's in-memory object store. `cluster_with_segments(3, n)` from the controller's shared test
//! helpers leaves `n` sealed segments holding one record each (segment `k` holds record `k`) and an open
//! one. Time is virtual: `let_pass` moves it and renews the replicas' registrations, as their own timers
//! would. A jump of hours makes the helper's writer, which is left running, take its replicas for silent and
//! roll to a new segment, so tests that jump look at the sealed segments by number and not at the whole list.

use std::sync::Arc;

use bitacora_artifact::MemoryArtifactStore;
use bitacora_client::{ReadError, StreamReader};
use bitacora_meta::{CopyRecord, CopyState, SealRecord};
use bitacora_testkit::Cluster;
use bitacora_types::{Epoch, SequenceNumber, SequenceRange};

use super::super::test_support::{
    STREAM, checkpoint_at, client, cluster_with_segments, controller_with_object_store, held_segments, let_pass,
    listed_segments, renew_registrations, replica_id, stream, stream_record,
};
use super::*;
use crate::ControllerSettings;

/// The moment the pure tests are evaluated at.
const NOW: Timestamp = Timestamp::from_unix_millis(100_000_000);
/// The tiering delay the pure tests use.
const DELAY: Duration = Duration::from_mins(15);
/// The hot-read window the pure tests use.
const HOT_READ_WINDOW: Duration = Duration::from_hours(24);

/// A copy on replica `name` in `state`.
fn copy(name: &str, state: CopyState) -> CopyRecord {
    CopyRecord {
        replica_id: replica_id(name),
        state,
    }
}

/// A segment holding records 10 to 19, sealed `sealed_for` before [`NOW`], with three good copies.
fn sealed_segment(sealed_for: Duration) -> SegmentRecord {
    let sealed_at_millis = NOW.unix_millis() - u64::try_from(sealed_for.as_millis()).expect("small");
    SegmentRecord {
        segment_id: SegmentId::new(2),
        epoch: Epoch::new(1),
        first_sequence: SequenceNumber::new(10),
        sealed: Some(SealRecord {
            end: SequenceNumber::new(19),
            at: Timestamp::from_unix_millis(sealed_at_millis),
        }),
        copies: vec![
            copy("replica-a", CopyState::Good),
            copy("replica-b", CopyState::Good),
            copy("replica-c", CopyState::Good),
        ],
        tiered: None,
    }
}

/// A stream replicated three times whose low-water mark is at `low_water_mark`. The pure rule looks at
/// nothing else in it.
fn stream_with_mark_at(low_water_mark: u64) -> StreamRecord {
    let mut stream = stream_record();
    stream.low_water_mark = SequenceNumber::new(low_water_mark);
    stream
}

#[test]
fn object_key_names_the_stream_and_pads_the_segment() {
    let object_key = tiered_object_key(StreamId::new(7), SegmentId::new(3));

    assert_eq!(object_key, "streams/7/segments/00000000000000000003");
}

#[test]
fn segment_sealed_for_the_delay_with_three_good_copies_is_ready() {
    let segment = sealed_segment(DELAY);

    assert!(is_ready_to_tier(&stream_with_mark_at(1), &segment, NOW, DELAY));
}

#[test]
fn segment_sealed_more_recently_than_the_delay_is_not_ready() {
    let one_second_short_of_the_delay = Duration::from_secs(DELAY.as_secs() - 1);
    let segment = sealed_segment(one_second_short_of_the_delay);

    assert!(!is_ready_to_tier(&stream_with_mark_at(1), &segment, NOW, DELAY));
}

#[test]
fn open_segment_is_not_ready() {
    let mut segment = sealed_segment(DELAY);
    segment.sealed = None;

    assert!(!is_ready_to_tier(&stream_with_mark_at(1), &segment, NOW, DELAY));
}

#[test]
fn segment_with_an_unverified_copy_is_not_ready() {
    let mut segment = sealed_segment(DELAY);
    segment.copies[1].state = CopyState::Unverified;

    assert!(!is_ready_to_tier(&stream_with_mark_at(1), &segment, NOW, DELAY));
}

#[test]
fn segment_with_a_bad_copy_is_not_ready() {
    let mut segment = sealed_segment(DELAY);
    segment.copies[2].state = CopyState::Bad;

    assert!(!is_ready_to_tier(&stream_with_mark_at(1), &segment, NOW, DELAY));
}

#[test]
fn segment_already_in_the_object_store_is_not_ready() {
    let mut segment = sealed_segment(DELAY);
    segment.tiered = Some(TieredRecord {
        object_key: "streams/7/segments/00000000000000000002".to_owned(),
        size_bytes: 100,
        uploaded_at: NOW,
        are_local_copies_dropped: false,
    });

    assert!(!is_ready_to_tier(&stream_with_mark_at(1), &segment, NOW, DELAY));
}

#[test]
fn segment_sealed_empty_is_not_ready() {
    let mut segment = sealed_segment(DELAY);
    segment.sealed = Some(SealRecord {
        end: SequenceNumber::new(9),
        at: Timestamp::from_unix_millis(0),
    });

    assert!(!is_ready_to_tier(&stream_with_mark_at(1), &segment, NOW, DELAY));
}

#[test]
fn segment_below_the_low_water_mark_is_not_ready() {
    let segment = sealed_segment(DELAY);

    assert!(!is_ready_to_tier(&stream_with_mark_at(20), &segment, NOW, DELAY));
}

#[test]
fn segment_that_the_low_water_mark_has_entered_is_still_ready() {
    let segment = sealed_segment(DELAY);

    assert!(is_ready_to_tier(&stream_with_mark_at(15), &segment, NOW, DELAY));
}

/// The record of an object that was uploaded `uploaded_for` before [`NOW`] and whose local copies are still there.
fn tiered_for(uploaded_for: Duration) -> TieredRecord {
    let uploaded_at_millis = NOW.unix_millis() - u64::try_from(uploaded_for.as_millis()).expect("small");
    TieredRecord {
        object_key: "streams/7/segments/00000000000000000002".to_owned(),
        size_bytes: 100,
        uploaded_at: Timestamp::from_unix_millis(uploaded_at_millis),
        are_local_copies_dropped: false,
    }
}

#[test]
fn local_copies_are_released_once_the_hot_read_window_is_over() {
    let tiered = tiered_for(HOT_READ_WINDOW);

    assert!(is_release_due(&tiered, NOW, HOT_READ_WINDOW));
}

#[test]
fn local_copies_stay_through_the_hot_read_window() {
    let one_second_short_of_the_window = Duration::from_secs(HOT_READ_WINDOW.as_secs() - 1);
    let tiered = tiered_for(one_second_short_of_the_window);

    assert!(!is_release_due(&tiered, NOW, HOT_READ_WINDOW));
}

#[test]
fn local_copies_that_were_released_are_not_released_again() {
    let mut tiered = tiered_for(HOT_READ_WINDOW);
    tiered.are_local_copies_dropped = true;

    assert!(!is_release_due(&tiered, NOW, HOT_READ_WINDOW));
}

/// Runs one pass of a controller that tiers to the cluster's object store with the default settings.
async fn run_tiering_pass(cluster: &Cluster) -> PassReport {
    controller_with_object_store(cluster, ControllerSettings::default(), cluster.artifact_store())
        .run_pass()
        .await
        .expect("the pass runs")
}

/// A cluster with `sealed_segment_count` sealed segments that have been sealed for the tiering delay.
async fn cluster_ready_to_tier(sealed_segment_count: u64) -> Cluster {
    let cluster = cluster_with_segments(3, sealed_segment_count).await;
    let_pass(&cluster, ControllerSettings::default().tiering_delay).await;
    cluster
}

/// A cluster with two sealed segments that were uploaded a moment ago.
async fn cluster_with_tiered_segments() -> Cluster {
    let cluster = cluster_ready_to_tier(2).await;
    run_tiering_pass(&cluster).await;
    cluster
}

/// The keys of the segment objects in the cluster's object store. The controller also keeps snapshots of
/// the metadata store there, which these tests do not look at.
async fn stored_object_keys(cluster: &Cluster) -> Vec<String> {
    let objects = cluster
        .artifact_store()
        .list_prefix("streams/")
        .await
        .expect("the object store is up");
    let mut object_keys = Vec::new();
    for object in objects {
        object_keys.push(object.object_key);
    }
    object_keys
}

/// The keys of everything materializers wrote to the cluster's object store.
async fn materializer_object_keys(cluster: &Cluster) -> Vec<String> {
    let objects = cluster
        .artifact_store()
        .list_prefix("materializers/")
        .await
        .expect("the object store is up");
    let mut object_keys = Vec::new();
    for object in objects {
        object_keys.push(object.object_key);
    }
    object_keys
}

/// The key of the object of segment `segment_id` of the test stream.
async fn object_key_of(cluster: &Cluster, segment_id: u64) -> String {
    tiered_object_key(stream(cluster).await.stream_id, SegmentId::new(segment_id))
}

/// A reader of the test stream that was given the cluster's object store.
fn reader(cluster: &Cluster) -> StreamReader {
    client(cluster, bitacora_client::WriterSettings::default())
        .with_artifact_store(Arc::new(cluster.artifact_store().clone()))
        .open_reader(STREAM)
}

/// The range of the records `first` to `last`.
fn range(first: u64, last: u64) -> SequenceRange {
    SequenceRange::new(SequenceNumber::new(first), SequenceNumber::new(last)).expect("valid range")
}

#[tokio::test(start_paused = true)]
async fn segment_is_not_uploaded_before_the_tiering_delay() {
    let cluster = cluster_with_segments(3, 2).await;

    let report = run_tiering_pass(&cluster).await;

    assert_eq!(report.segments_tiered, 0);
    assert_eq!(stored_object_keys(&cluster).await, Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn segments_sealed_for_the_tiering_delay_are_uploaded() {
    let cluster = cluster_ready_to_tier(2).await;

    let report = run_tiering_pass(&cluster).await;

    assert_eq!(report.segments_tiered, 2);
    assert_eq!(
        stored_object_keys(&cluster).await,
        [object_key_of(&cluster, 1).await, object_key_of(&cluster, 2).await]
    );
}

#[tokio::test(start_paused = true)]
async fn uploaded_segment_is_recorded_with_the_size_of_its_object() {
    let cluster = cluster_with_tiered_segments().await;

    let recorded = stream(&cluster).await.segments[0]
        .tiered
        .clone()
        .expect("the segment is recorded as tiered");

    let object = cluster
        .artifact_store()
        .get_object(&recorded.object_key)
        .await
        .expect("the object is stored");
    assert_eq!(recorded.size_bytes, object.len() as u64);
}

#[tokio::test(start_paused = true)]
async fn uploaded_segment_is_not_uploaded_again() {
    let cluster = cluster_with_tiered_segments().await;

    let second_pass = run_tiering_pass(&cluster).await;

    assert_eq!(second_pass.segments_tiered, 0);
}

#[tokio::test(start_paused = true)]
async fn open_segment_is_never_uploaded() {
    let cluster = cluster_with_tiered_segments().await;

    let open_segment = stream(&cluster)
        .await
        .segments
        .last()
        .cloned()
        .expect("the stream has an open segment");

    assert_eq!(open_segment.tiered, None);
}

#[tokio::test(start_paused = true)]
async fn controller_without_an_object_store_uploads_nothing() {
    let cluster = cluster_ready_to_tier(2).await;
    let controller_without_object_store = Controller::new(
        cluster.metadata().clone(),
        cluster.connector(),
        cluster.clock(),
        ControllerSettings::default(),
    );

    let report = controller_without_object_store.run_pass().await.expect("the pass runs");

    assert_eq!(report.segments_tiered, 0);
    assert_eq!(stored_object_keys(&cluster).await, Vec::<String>::new());
}

/// The second segment was sealed last, and one of its replicas has been unreachable since: nobody could
/// confirm that copy, so the segment is not yet known to be fully replicated.
#[tokio::test(start_paused = true)]
async fn segment_with_a_copy_nobody_could_verify_is_not_uploaded() {
    let cluster = cluster_with_segments(3, 2).await;
    cluster.nodes()[2].set_reachable(false);
    let_pass(&cluster, ControllerSettings::default().tiering_delay).await;

    run_tiering_pass(&cluster).await;

    assert_eq!(stream(&cluster).await.segments[1].tiered, None);
}

#[tokio::test(start_paused = true)]
async fn segment_is_uploaded_once_its_last_copy_is_verified() {
    let cluster = cluster_with_segments(3, 2).await;
    cluster.nodes()[2].set_reachable(false);
    let_pass(&cluster, ControllerSettings::default().tiering_delay).await;
    run_tiering_pass(&cluster).await;

    cluster.nodes()[2].set_reachable(true);
    run_tiering_pass(&cluster).await;

    assert!(stream(&cluster).await.segments[1].tiered.is_some());
}

/// A copy that was verified earlier still counts while its replica is briefly unreachable: the upload
/// only needs one reachable good copy.
#[tokio::test(start_paused = true)]
async fn verified_segment_is_uploaded_while_one_of_its_replicas_is_unreachable() {
    let cluster = cluster_with_segments(3, 2).await;
    cluster.nodes()[2].set_reachable(false);
    let_pass(&cluster, ControllerSettings::default().tiering_delay).await;

    run_tiering_pass(&cluster).await;

    assert!(stream(&cluster).await.segments[0].tiered.is_some());
}

#[tokio::test(start_paused = true)]
async fn failed_upload_is_retried_on_the_next_pass() {
    let cluster = cluster_ready_to_tier(2).await;
    cluster.artifact_store().set_unavailable(true);
    let during_the_outage = run_tiering_pass(&cluster).await;

    cluster.artifact_store().set_unavailable(false);
    let after_the_outage = run_tiering_pass(&cluster).await;

    assert_eq!(during_the_outage.segments_tiered, 0);
    assert_eq!(after_the_outage.segments_tiered, 2);
}

#[tokio::test(start_paused = true)]
async fn failed_upload_leaves_the_segment_recorded_as_not_tiered() {
    let cluster = cluster_ready_to_tier(2).await;
    cluster.artifact_store().set_unavailable(true);

    run_tiering_pass(&cluster).await;

    assert_eq!(stream(&cluster).await.segments[0].tiered, None);
}

#[tokio::test(start_paused = true)]
async fn one_pass_uploads_no_more_segments_than_its_limit() {
    let cluster = cluster_ready_to_tier(3).await;
    let two_uploads_per_pass = ControllerSettings {
        max_uploads_per_pass: 2,
        ..ControllerSettings::default()
    };
    let controller = controller_with_object_store(&cluster, two_uploads_per_pass, cluster.artifact_store());

    let first_pass = controller.run_pass().await.expect("the pass runs");
    let second_pass = controller.run_pass().await.expect("the pass runs");

    assert_eq!(first_pass.segments_tiered, 2);
    assert_eq!(second_pass.segments_tiered, 1);
}

/// A controller and replicas configured with different buckets must not produce metadata that points at
/// an object the controller cannot manage.
#[tokio::test(start_paused = true)]
async fn upload_that_the_controller_cannot_see_is_not_recorded() {
    let cluster = cluster_ready_to_tier(2).await;
    let another_bucket = MemoryArtifactStore::new();
    let controller = controller_with_object_store(&cluster, ControllerSettings::default(), &another_bucket);

    let report = controller.run_pass().await.expect("the pass runs");

    assert_eq!(report.segments_tiered, 0);
    assert_eq!(stream(&cluster).await.segments[0].tiered, None);
}

#[tokio::test(start_paused = true)]
async fn local_copies_stay_during_the_hot_read_window() {
    let cluster = cluster_with_tiered_segments().await;
    let_pass(&cluster, Duration::from_hours(23)).await;

    let report = run_tiering_pass(&cluster).await;

    let held_by_the_first_node = held_segments(&cluster, 0).await;
    assert_eq!(report.segments_released, 0);
    assert!(held_by_the_first_node.contains(&1), "{held_by_the_first_node:?}");
    assert!(held_by_the_first_node.contains(&2), "{held_by_the_first_node:?}");
}

#[tokio::test(start_paused = true)]
async fn local_copies_are_removed_after_the_hot_read_window() {
    let cluster = cluster_with_tiered_segments().await;
    let_pass(&cluster, ControllerSettings::default().hot_read_window).await;

    let report = run_tiering_pass(&cluster).await;

    let held_by_the_first_node = held_segments(&cluster, 0).await;
    assert_eq!(report.segments_released, 2);
    assert!(!held_by_the_first_node.contains(&1), "{held_by_the_first_node:?}");
    assert!(!held_by_the_first_node.contains(&2), "{held_by_the_first_node:?}");
}

#[tokio::test(start_paused = true)]
async fn released_segment_lists_no_replicas() {
    let cluster = cluster_with_tiered_segments().await;
    let_pass(&cluster, ControllerSettings::default().hot_read_window).await;

    run_tiering_pass(&cluster).await;

    let released_segment = stream(&cluster).await.segments[0].clone();
    assert_eq!(released_segment.copies, Vec::new());
    assert!(
        released_segment
            .tiered
            .is_some_and(|tiered| tiered.are_local_copies_dropped)
    );
}

#[tokio::test(start_paused = true)]
async fn released_segment_is_read_from_the_object_store() {
    let cluster = cluster_with_tiered_segments().await;
    let_pass(&cluster, ControllerSettings::default().hot_read_window).await;
    run_tiering_pass(&cluster).await;

    let records = reader(&cluster).read_range(range(1, 2)).await.expect("reads");

    assert_eq!(records.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn local_copies_are_kept_when_the_object_has_gone_missing() {
    let cluster = cluster_with_tiered_segments().await;
    cluster
        .artifact_store()
        .delete_object(&object_key_of(&cluster, 1).await)
        .await
        .expect("the object is removed behind the controller's back");
    let_pass(&cluster, ControllerSettings::default().hot_read_window).await;

    let report = run_tiering_pass(&cluster).await;

    let held_by_the_first_node = held_segments(&cluster, 0).await;
    assert_eq!(report.segments_released, 1);
    assert!(held_by_the_first_node.contains(&1), "{held_by_the_first_node:?}");
    assert!(!held_by_the_first_node.contains(&2), "{held_by_the_first_node:?}");
}

#[tokio::test(start_paused = true)]
async fn truncation_deletes_tiered_segments_and_their_objects() {
    let cluster = cluster_with_tiered_segments().await;
    checkpoint_at(&cluster, "pages", 2).await;

    let report = run_tiering_pass(&cluster).await;

    assert_eq!(report.segments_deleted, 2);
    assert_eq!(listed_segments(&cluster).await, [3]);
    assert_eq!(stored_object_keys(&cluster).await, Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn truncation_deletes_released_segments_too() {
    let cluster = cluster_with_tiered_segments().await;
    let_pass(&cluster, ControllerSettings::default().hot_read_window).await;
    run_tiering_pass(&cluster).await;
    checkpoint_at(&cluster, "pages", 2).await;

    run_tiering_pass(&cluster).await;

    let listed = listed_segments(&cluster).await;
    assert!(!listed.contains(&1), "{listed:?}");
    assert!(!listed.contains(&2), "{listed:?}");
    assert_eq!(stored_object_keys(&cluster).await, Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn tiered_segment_stays_listed_while_its_object_cannot_be_deleted() {
    let cluster = cluster_with_tiered_segments().await;
    checkpoint_at(&cluster, "pages", 2).await;
    cluster.artifact_store().set_unavailable(true);

    let during_the_outage = run_tiering_pass(&cluster).await;
    let listed_during_the_outage = listed_segments(&cluster).await;
    cluster.artifact_store().set_unavailable(false);
    let after_the_outage = run_tiering_pass(&cluster).await;

    assert_eq!(during_the_outage.segments_deleted, 0);
    assert_eq!(listed_during_the_outage, [1, 2, 3]);
    assert_eq!(after_the_outage.segments_deleted, 2);
    assert_eq!(stored_object_keys(&cluster).await, Vec::<String>::new());
}

/// Segment 1 is tiered and cannot be deleted while the object store is down, so it stays listed. Segment
/// 2 was never tiered and is deleted outright. Its copies must not wait for segment 1 to go.
#[tokio::test(start_paused = true)]
async fn copies_of_a_deleted_segment_are_removed_while_an_older_tiered_one_awaits_deletion() {
    let cluster = cluster_ready_to_tier(3).await;
    let one_upload_per_pass = ControllerSettings {
        max_uploads_per_pass: 1,
        ..ControllerSettings::default()
    };
    let controller = controller_with_object_store(&cluster, one_upload_per_pass, cluster.artifact_store());
    controller.run_pass().await.expect("the pass runs");
    checkpoint_at(&cluster, "pages", 2).await;
    cluster.artifact_store().set_unavailable(true);

    controller.run_pass().await.expect("the pass runs");

    let listed = listed_segments(&cluster).await;
    let held_by_the_first_node = held_segments(&cluster, 0).await;
    assert!(listed.contains(&1), "{listed:?}");
    assert!(!listed.contains(&2), "{listed:?}");
    assert!(!held_by_the_first_node.contains(&2), "{held_by_the_first_node:?}");
}

#[tokio::test(start_paused = true)]
async fn truncated_segment_that_awaits_deletion_is_no_longer_readable() {
    let cluster = cluster_with_tiered_segments().await;
    checkpoint_at(&cluster, "pages", 2).await;
    cluster.artifact_store().set_unavailable(true);
    run_tiering_pass(&cluster).await;

    let read = reader(&cluster).read_range(range(1, 2)).await;

    assert!(matches!(read, Err(ReadError::Truncated { .. })), "{read:?}");
}

#[tokio::test(start_paused = true)]
async fn deleted_stream_leaves_nothing_in_the_object_store() {
    let cluster = cluster_with_tiered_segments().await;
    cluster.metadata().delete_stream(STREAM).await.expect("deleted");

    run_tiering_pass(&cluster).await;

    assert_eq!(stored_object_keys(&cluster).await, Vec::<String>::new());
    assert_eq!(cluster.metadata().list_deleted_streams().await, Ok(Vec::new()));
}

/// What a stream's materializers wrote is garbage once the stream is gone: its checkpoints are deleted
/// with it, so nothing refers to those objects any more.
#[tokio::test(start_paused = true)]
async fn deleted_stream_leaves_no_materializer_artifacts_in_the_object_store() {
    let cluster = cluster_with_tiered_segments().await;
    let stream_id = stream(&cluster).await.stream_id;
    cluster
        .artifact_store()
        .put_object(
            &format!("materializers/{stream_id}/pages/00000000000000000001/0000000001-layer"),
            bytes::Bytes::from_static(b"a layer file"),
        )
        .await
        .expect("the artifact is stored");
    cluster.metadata().delete_stream(STREAM).await.expect("deleted");

    run_tiering_pass(&cluster).await;

    assert_eq!(materializer_object_keys(&cluster).await, Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn artifacts_of_other_streams_survive_the_deletion_of_a_stream() {
    let cluster = cluster_with_tiered_segments().await;
    let another_stream_id = stream(&cluster).await.stream_id.get() + 1;
    let artifact_of_another_stream =
        format!("materializers/{another_stream_id}/pages/00000000000000000001/0000000001-layer");
    cluster
        .artifact_store()
        .put_object(&artifact_of_another_stream, bytes::Bytes::from_static(b"a layer file"))
        .await
        .expect("the artifact is stored");
    cluster.metadata().delete_stream(STREAM).await.expect("deleted");

    run_tiering_pass(&cluster).await;

    assert_eq!(materializer_object_keys(&cluster).await, [artifact_of_another_stream]);
}

#[tokio::test(start_paused = true)]
async fn deleted_stream_is_remembered_while_the_object_store_is_down() {
    let cluster = cluster_with_tiered_segments().await;
    cluster.metadata().delete_stream(STREAM).await.expect("deleted");
    cluster.artifact_store().set_unavailable(true);

    run_tiering_pass(&cluster).await;

    let remembered_streams = cluster.metadata().list_deleted_streams().await.expect("lists");
    assert_eq!(remembered_streams.len(), 1);
}

/// The object store is the backup: a cluster that loses every replica node keeps all data older than the
/// tiering delay (design, section 10).
#[tokio::test(start_paused = true)]
async fn records_survive_the_loss_of_every_replica_disk() {
    let cluster = cluster_with_tiered_segments().await;
    for node in cluster.nodes() {
        node.crash();
        node.lose_disk();
        node.restart().await;
    }
    renew_registrations(&cluster).await;

    let records = reader(&cluster).read_range(range(1, 2)).await.expect("reads");

    assert_eq!(records.len(), 2);
}
