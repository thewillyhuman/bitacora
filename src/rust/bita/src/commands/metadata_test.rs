//! Tests of the `bita metadata snapshot` and `bita metadata restore` commands, against an in-process
//! cluster.
//!
//! # What is covered
//!
//! - Listing: nothing before a snapshot exists, snapshots oldest first, other objects ignored.
//! - Restoring: into a lost store, by key and as `latest`; refused into a store that holds keys unless
//!   forced; a dry run writes nothing.
//! - Refusals: no object store, no snapshot, a missing snapshot, a damaged one.
//! - The rule that decides how a restore writes, as a pure function: `restore_mode_for`.
//!
//! # How the fixtures work
//!
//! See `testing.rs`. A controller pass writes a snapshot of the metadata store into the cluster's object
//! store; `lose_the_metadata_store` then deletes every key, as the loss of etcd would.

use std::time::Duration;

use bitacora_artifact::ArtifactError;
use bitacora_meta::{RestoreMode, snapshot_object_key};

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
}

#[tokio::test(start_paused = true)]
async fn restore_into_a_store_that_holds_keys_is_refused() {
    let (started, _object_key) = cluster_with_a_snapshot().await;

    let refused = restore(&started.cluster, "latest", false, false).await;

    assert!(matches!(refused, Err(CommandError::Refused { .. })));
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

/// Without force the metadata store itself still refuses if a key appeared since the command looked.
#[test]
fn restore_into_an_empty_store_requires_it_to_stay_empty() {
    let mode = restore_mode_for(0, false);

    assert_eq!(mode, Ok(RestoreMode::IntoEmptyStore));
}

#[test]
fn forced_restore_replaces_everything_the_store_holds() {
    let mode = restore_mode_for(12, true);

    assert_eq!(mode, Ok(RestoreMode::ReplaceEverything));
}

#[test]
fn restore_into_a_store_with_keys_is_refused_without_force() {
    let refused = restore_mode_for(12, false);

    assert!(matches!(refused, Err(CommandError::Refused { .. })), "{refused:?}");
}

#[test]
fn refusal_of_a_restore_says_how_many_keys_would_be_replaced() {
    let refused = restore_mode_for(12, false).expect_err("a store with keys is not overwritten unasked");

    assert!(refused.to_string().contains("holds 12 keys"), "{refused}");
}
