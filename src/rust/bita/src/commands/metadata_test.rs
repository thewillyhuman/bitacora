//! Tests of the `bita metadata snapshot` and `bita metadata restore` commands, against an in-process
//! cluster.
//!
//! # What is covered
//!
//! - Listing: nothing before a snapshot exists, snapshots oldest first, other objects ignored.
//! - Restoring: into a lost store, by key and as `latest`; next to replicas that registered again since
//!   the loss, whose registrations are kept; refused into a store that holds more than registrations
//!   unless forced; a dry run writes nothing and says whether force is needed.
//! - Refusals: no object store, no snapshot, a missing snapshot, a damaged one.
//!
//! # How the fixtures work
//!
//! See `testing.rs`. A controller pass writes a snapshot of the metadata store into the cluster's object
//! store; `lose_the_metadata_store` then deletes every key, as the loss of etcd would.

use std::time::Duration;

use bitacora_artifact::ArtifactError;
use bitacora_meta::snapshot_object_key;

use super::*;
use crate::commands::stream;
use crate::commands::testing::{STREAM, TestCluster};

/// A cluster with one stream and one snapshot of its metadata store. Returns the snapshot's object key.
async fn cluster_with_a_snapshot() -> (TestCluster, String) {
    let started = TestCluster::with_stream(3).await;
    let object_key = snapshot_object_key(started.cluster.clock.now());
    started.run_controller_pass().await;
    (started, object_key)
}

#[tokio::test(start_paused = true)]
async fn no_snapshot_is_listed_before_a_controller_wrote_one() {
    let started = TestCluster::with_stream(3).await;

    let listed = list_snapshots(&started.cluster).await.expect("lists");

    assert_eq!(listed.snapshots, Vec::new());
}

#[tokio::test(start_paused = true)]
async fn snapshot_is_listed_with_its_key_and_time() {
    let (started, object_key) = cluster_with_a_snapshot().await;
    let taken_at = format_timestamp(started.cluster.clock.now());

    let listed = list_snapshots(&started.cluster).await.expect("lists");

    assert_eq!(listed.snapshots.len(), 1);
    assert_eq!(listed.snapshots[0].object_key, object_key);
    assert_eq!(listed.snapshots[0].taken_at, taken_at);
}

#[tokio::test(start_paused = true)]
async fn snapshots_are_listed_oldest_first_with_their_age() {
    let (started, first_object_key) = cluster_with_a_snapshot().await;
    started.let_pass(Duration::from_mins(5)).await;
    started.run_controller_pass().await;

    let listed = list_snapshots(&started.cluster).await.expect("lists");

    assert_eq!(listed.snapshots.len(), 2);
    assert_eq!(listed.snapshots[0].object_key, first_object_key);
    assert_eq!(listed.snapshots[0].age_seconds, 300);
    assert_eq!(listed.snapshots[1].age_seconds, 0);
}

#[tokio::test(start_paused = true)]
async fn objects_that_are_no_snapshots_are_not_listed() {
    let (started, _object_key) = cluster_with_a_snapshot().await;
    started
        .nodes
        .artifact_store()
        .put_object("metadata-snapshots/README", Bytes::from_static(b"notes"))
        .await
        .expect("the object is stored");

    let listed = list_snapshots(&started.cluster).await.expect("lists");

    assert_eq!(listed.snapshots.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn listing_needs_an_object_store() {
    let mut started = TestCluster::with_stream(3).await;
    started.cluster.artifact_store = None;

    let refused = list_snapshots(&started.cluster).await;

    assert_eq!(refused, Err(CommandError::NoObjectStore));
}

#[tokio::test(start_paused = true)]
async fn listing_fails_while_the_object_store_is_down() {
    let (started, _object_key) = cluster_with_a_snapshot().await;
    started.nodes.artifact_store().set_unavailable(true);

    let failed = list_snapshots(&started.cluster).await;

    assert!(matches!(failed, Err(CommandError::ObjectStore { .. })));
}

#[tokio::test(start_paused = true)]
async fn latest_snapshot_brings_a_lost_stream_back() {
    let (started, _object_key) = cluster_with_a_snapshot().await;
    started.lose_the_metadata_store().await;

    restore(&started.cluster, "latest", false, false)
        .await
        .expect("the snapshot is restored");

    let described = stream::describe(&started.cluster, STREAM).await;
    assert!(described.is_ok(), "{described:?}");
}

#[tokio::test(start_paused = true)]
async fn snapshot_named_by_its_key_is_restored() {
    let (started, object_key) = cluster_with_a_snapshot().await;
    started.lose_the_metadata_store().await;

    let report = restore(&started.cluster, &object_key, false, false)
        .await
        .expect("the snapshot is restored");

    assert_eq!(report.snapshot, object_key);
    assert!(!report.is_dry_run);
}

#[tokio::test(start_paused = true)]
async fn restore_reports_the_keys_it_wrote() {
    let (started, _object_key) = cluster_with_a_snapshot().await;
    let keys_before_the_loss = started.metadata_key_count().await;
    started.lose_the_metadata_store().await;

    let report = restore(&started.cluster, "latest", false, false)
        .await
        .expect("the snapshot is restored");

    assert_eq!(report.keys_in_store_before, 0);
    assert_eq!(report.keys_written, keys_before_the_loss);
    assert_eq!(report.keys_deleted, 0);
    assert_eq!(report.keys_kept, 0);
}

/// Found on a real cluster. The procedure for a lost store leaves the replicas running, and they register
/// again within seconds, so the store an operator restores into is never empty. Refusing it made `--force`
/// part of every restore.
#[tokio::test(start_paused = true)]
async fn lost_store_that_the_replicas_registered_in_again_is_restored_without_force() {
    let (started, _object_key) = cluster_with_a_snapshot().await;
    started.lose_the_metadata_store_under_running_replicas().await;

    restore(&started.cluster, "latest", false, false)
        .await
        .expect("the snapshot is restored");

    let described = stream::describe(&started.cluster, STREAM).await;
    assert!(described.is_ok(), "{described:?}");
}

#[tokio::test(start_paused = true)]
async fn restore_next_to_registered_replicas_reports_the_registrations_it_kept() {
    let (started, _object_key) = cluster_with_a_snapshot().await;
    let keys_before_the_loss = started.metadata_key_count().await;
    let registered_replica_count = started.lose_the_metadata_store_under_running_replicas().await;

    let report = restore(&started.cluster, "latest", false, false)
        .await
        .expect("the snapshot is restored");

    assert_eq!(report.keys_in_store_before, registered_replica_count);
    assert_eq!(report.keys_kept, registered_replica_count);
    assert_eq!(report.keys_written, keys_before_the_loss - registered_replica_count);
    assert_eq!(report.keys_deleted, 0);
}

#[tokio::test(start_paused = true)]
async fn dry_run_next_to_registered_replicas_does_not_ask_for_force() {
    let (started, _object_key) = cluster_with_a_snapshot().await;
    started.lose_the_metadata_store_under_running_replicas().await;

    let report = restore(&started.cluster, "latest", false, true)
        .await
        .expect("the snapshot is read");

    assert!(!report.needs_force);
}

#[tokio::test(start_paused = true)]
async fn restore_into_a_store_that_holds_keys_is_refused() {
    let (started, _object_key) = cluster_with_a_snapshot().await;

    let refused = restore(&started.cluster, "latest", false, false).await;

    assert!(matches!(refused, Err(CommandError::Refused { .. })));
}

#[tokio::test(start_paused = true)]
async fn refused_restore_changes_nothing() {
    let (started, _object_key) = cluster_with_a_snapshot().await;
    stream::create(&started.cluster, "created-after-the-snapshot", 3, 2, None)
        .await
        .expect("the stream is created");

    let _refused = restore(&started.cluster, "latest", false, false).await;

    let listed = stream::list(&started.cluster).await.expect("lists");
    assert_eq!(listed.streams.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn refusal_of_a_restore_says_how_to_go_on() {
    let (started, _object_key) = cluster_with_a_snapshot().await;

    let refused = restore(&started.cluster, "latest", false, false)
        .await
        .expect_err("a store in use is not overwritten unasked");

    assert!(
        refused.to_string().contains("--dry-run, then pass --force"),
        "{refused}"
    );
}

#[tokio::test(start_paused = true)]
async fn forced_restore_makes_the_store_what_the_snapshot_was() {
    let (started, _object_key) = cluster_with_a_snapshot().await;
    stream::create(&started.cluster, "created-after-the-snapshot", 3, 2, None)
        .await
        .expect("the stream is created");

    let report = restore(&started.cluster, "latest", true, false)
        .await
        .expect("the snapshot is restored");

    let listed = stream::list(&started.cluster).await.expect("lists");
    assert_eq!(listed.streams.len(), 1);
    assert_eq!(report.keys_deleted, 1);
}

#[tokio::test(start_paused = true)]
async fn dry_run_says_what_a_restore_would_write() {
    let (started, _object_key) = cluster_with_a_snapshot().await;
    let keys_before_the_loss = started.metadata_key_count().await;
    started.lose_the_metadata_store().await;

    let report = restore(&started.cluster, "latest", false, true)
        .await
        .expect("the snapshot is read");

    assert!(report.is_dry_run);
    assert_eq!(report.keys_written, keys_before_the_loss);
}

#[tokio::test(start_paused = true)]
async fn dry_run_writes_nothing() {
    let (started, _object_key) = cluster_with_a_snapshot().await;
    started.lose_the_metadata_store().await;

    restore(&started.cluster, "latest", false, true)
        .await
        .expect("the snapshot is read");

    assert_eq!(started.metadata_key_count().await, 0);
}

#[tokio::test(start_paused = true)]
async fn dry_run_into_a_store_that_holds_keys_says_what_would_be_deleted() {
    let (started, _object_key) = cluster_with_a_snapshot().await;
    stream::create(&started.cluster, "created-after-the-snapshot", 3, 2, None)
        .await
        .expect("the stream is created");

    let report = restore(&started.cluster, "latest", false, true)
        .await
        .expect("the snapshot is read");

    assert_eq!(report.keys_deleted, 1);
}

#[tokio::test(start_paused = true)]
async fn dry_run_into_a_store_that_holds_keys_says_that_the_restore_needs_force() {
    let (started, _object_key) = cluster_with_a_snapshot().await;

    let report = restore(&started.cluster, "latest", false, true)
        .await
        .expect("the snapshot is read");

    assert!(report.needs_force);
}

/// The note is for the operator who has not forced yet; one who has needs no reminder.
#[tokio::test(start_paused = true)]
async fn forced_dry_run_does_not_ask_for_force() {
    let (started, _object_key) = cluster_with_a_snapshot().await;

    let report = restore(&started.cluster, "latest", true, true)
        .await
        .expect("the snapshot is read");

    assert!(!report.needs_force);
}

#[tokio::test(start_paused = true)]
async fn restoring_the_latest_snapshot_when_there_is_none_fails() {
    let started = TestCluster::with_stream(3).await;

    let failed = restore(&started.cluster, "latest", false, false).await;

    assert!(matches!(failed, Err(CommandError::NothingToDo { .. })));
}

#[tokio::test(start_paused = true)]
async fn restoring_a_snapshot_that_does_not_exist_fails() {
    let (started, _object_key) = cluster_with_a_snapshot().await;

    let failed = restore(
        &started.cluster,
        "metadata-snapshots/00000000000000000001",
        false,
        false,
    )
    .await;

    assert!(matches!(
        failed,
        Err(CommandError::ObjectStore {
            reason: ArtifactError::NotFound { .. }
        })
    ));
}

#[tokio::test(start_paused = true)]
async fn damaged_snapshot_is_not_restored() {
    let (started, object_key) = cluster_with_a_snapshot().await;
    started.lose_the_metadata_store().await;
    let offset_inside_the_entries = 40;
    started
        .nodes
        .artifact_store()
        .corrupt_byte(&object_key, offset_inside_the_entries, 0x01);

    let failed = restore(&started.cluster, &object_key, false, false).await;

    assert!(matches!(failed, Err(CommandError::UnreadableSnapshot { .. })));
    assert_eq!(started.metadata_key_count().await, 0);
}

#[tokio::test(start_paused = true)]
async fn restoring_needs_an_object_store() {
    let mut started = TestCluster::with_stream(3).await;
    started.cluster.artifact_store = None;

    let refused = restore(&started.cluster, "latest", false, false).await;

    assert_eq!(refused, Err(CommandError::NoObjectStore));
}
