//! Tests of the controller's metadata snapshots.
//!
//! # What is covered
//!
//! - When a snapshot is taken: on the first pass, again after the interval, not in between, and not by a
//!   controller that just started while a recent snapshot exists.
//! - What it holds: the metadata store as it was, readable back from the object.
//! - Pruning: only the newest snapshots are kept, the newest always, and other objects are left alone.
//! - Failure: an object store outage costs the snapshot, not the pass, and the next pass tries again.
//!
//! # How the fixtures work
//!
//! A real controller runs against the testkit cluster and its in-memory object store. Time is virtual:
//! `tokio::time::advance` moves it, and the replicas' registrations are renewed by hand so that the
//! replicas stay alive across the jump.

use std::sync::Arc;
use std::time::Duration;

use bitacora_artifact::MemoryArtifactStore;
use bitacora_meta::{MetadataSnapshot, RetentionPolicy};
use bitacora_testkit::Cluster;
use bitacora_types::ReplicationSettings;
use bytes::Bytes;

use super::*;
use crate::ControllerSettings;

/// A cluster of three replicas with one stream, so that the metadata store is not empty.
async fn cluster_with_a_stream() -> Cluster {
    let cluster = Cluster::start(3).await;
    cluster
        .metadata()
        .create_stream(
            "orders",
            ReplicationSettings::DEFAULT,
            RetentionPolicy::default(),
            None,
        )
        .await
        .expect("the stream is created");
    cluster
}

/// A controller over the cluster that snapshots to `artifact_store`.
fn controller_with(
    cluster: &Cluster,
    settings: ControllerSettings,
    artifact_store: &MemoryArtifactStore,
) -> Controller {
    Controller::new(
        cluster.metadata().clone(),
        cluster.connector(),
        cluster.clock(),
        settings,
    )
    .with_artifact_store(Arc::new(artifact_store.clone()))
}

/// A controller with the default settings that snapshots to the cluster's object store.
fn controller(cluster: &Cluster) -> Controller {
    controller_with(
        cluster,
        ControllerSettings::default(),
        cluster.artifact_store(),
    )
}

/// Runs one pass and returns how many snapshots it wrote.
async fn snapshots_written_by_a_pass(controller: &Controller) -> usize {
    controller
        .run_pass()
        .await
        .expect("the pass runs")
        .metadata_snapshots_written
}

/// Lets `duration` pass for the cluster.
async fn let_pass(cluster: &Cluster, duration: Duration) {
    tokio::time::advance(duration).await;
    cluster
        .renew_registrations()
        .await
        .expect("registrations are renewed");
}

/// The keys of the snapshot objects in the cluster's object store, oldest first.
async fn snapshot_keys(cluster: &Cluster) -> Vec<String> {
    let objects = cluster
        .artifact_store()
        .list_prefix(SNAPSHOT_KEY_PREFIX)
        .await
        .expect("the object store is up");
    let mut object_keys = Vec::new();
    for object in objects {
        object_keys.push(object.object_key);
    }
    object_keys
}

#[tokio::test(start_paused = true)]
async fn first_pass_takes_a_snapshot() {
    let cluster = cluster_with_a_stream().await;

    let written = snapshots_written_by_a_pass(&controller(&cluster)).await;

    assert_eq!(written, 1);
    assert_eq!(snapshot_keys(&cluster).await.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn snapshot_is_named_after_the_time_of_its_pass() {
    let cluster = cluster_with_a_stream().await;
    let time_of_the_pass = cluster.clock().now();

    snapshots_written_by_a_pass(&controller(&cluster)).await;

    assert_eq!(
        snapshot_keys(&cluster).await,
        [snapshot_object_key(time_of_the_pass)]
    );
}

#[tokio::test(start_paused = true)]
async fn snapshot_reads_back_as_the_metadata_store_it_was_taken_of() {
    let cluster = cluster_with_a_stream().await;
    let time_of_the_pass = cluster.clock().now();

    snapshots_written_by_a_pass(&controller(&cluster)).await;

    let stored_object = cluster
        .artifact_store()
        .get_object(&snapshot_object_key(time_of_the_pass))
        .await
        .expect("the snapshot is stored");
    let read_back = MetadataSnapshot::decode(&stored_object).expect("a valid snapshot");
    let store_now = cluster
        .metadata()
        .snapshot(time_of_the_pass)
        .await
        .expect("the metadata store is up");
    assert_eq!(read_back, store_now);
}

#[tokio::test(start_paused = true)]
async fn controller_without_an_object_store_takes_no_snapshot() {
    let cluster = cluster_with_a_stream().await;
    let controller_without_object_store = Controller::new(
        cluster.metadata().clone(),
        cluster.connector(),
        cluster.clock(),
        ControllerSettings::default(),
    );

    let written = snapshots_written_by_a_pass(&controller_without_object_store).await;

    assert_eq!(written, 0);
    assert_eq!(snapshot_keys(&cluster).await, Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn no_second_snapshot_is_taken_before_the_interval_has_passed() {
    let cluster = cluster_with_a_stream().await;
    let controller = controller(&cluster);
    snapshots_written_by_a_pass(&controller).await;
    let_pass(&cluster, Duration::from_mins(4)).await;

    let written = snapshots_written_by_a_pass(&controller).await;

    assert_eq!(written, 0);
    assert_eq!(snapshot_keys(&cluster).await.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn another_snapshot_is_taken_once_the_interval_has_passed() {
    let cluster = cluster_with_a_stream().await;
    let controller = controller(&cluster);
    snapshots_written_by_a_pass(&controller).await;
    let_pass(
        &cluster,
        ControllerSettings::default().metadata_snapshot_interval,
    )
    .await;

    let written = snapshots_written_by_a_pass(&controller).await;

    assert_eq!(written, 1);
    assert_eq!(snapshot_keys(&cluster).await.len(), 2);
}

/// A controller that just started, or just took the leadership, knows nothing of the snapshots another
/// instance wrote. It must look before it writes.
#[tokio::test(start_paused = true)]
async fn controller_that_just_started_respects_a_snapshot_another_instance_took() {
    let cluster = cluster_with_a_stream().await;
    snapshots_written_by_a_pass(&controller(&cluster)).await;
    let_pass(&cluster, Duration::from_mins(1)).await;

    let written = snapshots_written_by_a_pass(&controller(&cluster)).await;

    assert_eq!(written, 0);
}

#[tokio::test(start_paused = true)]
async fn controller_that_just_started_takes_a_snapshot_if_the_newest_is_old() {
    let cluster = cluster_with_a_stream().await;
    snapshots_written_by_a_pass(&controller(&cluster)).await;
    let_pass(&cluster, Duration::from_mins(6)).await;

    let written = snapshots_written_by_a_pass(&controller(&cluster)).await;

    assert_eq!(written, 1);
}

#[tokio::test(start_paused = true)]
async fn only_the_newest_snapshots_are_kept() {
    let cluster = cluster_with_a_stream().await;
    let keep_two = ControllerSettings {
        metadata_snapshots_kept: 2,
        ..ControllerSettings::default()
    };
    let interval = keep_two.metadata_snapshot_interval;
    let controller = controller_with(&cluster, keep_two, cluster.artifact_store());
    let mut times_of_the_passes = Vec::new();
    for _pass in 0..4 {
        times_of_the_passes.push(cluster.clock().now());
        snapshots_written_by_a_pass(&controller).await;
        let_pass(&cluster, interval).await;
    }

    let kept = snapshot_keys(&cluster).await;

    assert_eq!(
        kept,
        [
            snapshot_object_key(times_of_the_passes[2]),
            snapshot_object_key(times_of_the_passes[3])
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn newest_snapshot_is_kept_even_if_none_is_to_be_kept() {
    let cluster = cluster_with_a_stream().await;
    let keep_none = ControllerSettings {
        metadata_snapshots_kept: 0,
        ..ControllerSettings::default()
    };
    let controller = controller_with(&cluster, keep_none, cluster.artifact_store());

    snapshots_written_by_a_pass(&controller).await;

    assert_eq!(snapshot_keys(&cluster).await.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn objects_that_are_no_snapshots_are_left_alone_by_pruning() {
    let cluster = cluster_with_a_stream().await;
    let foreign_key = format!("{SNAPSHOT_KEY_PREFIX}README");
    cluster
        .artifact_store()
        .put_object(&foreign_key, Bytes::from_static(b"not a snapshot"))
        .await
        .expect("the object is stored");
    let keep_one = ControllerSettings {
        metadata_snapshots_kept: 1,
        ..ControllerSettings::default()
    };
    let interval = keep_one.metadata_snapshot_interval;
    let controller = controller_with(&cluster, keep_one, cluster.artifact_store());
    snapshots_written_by_a_pass(&controller).await;
    let_pass(&cluster, interval).await;

    snapshots_written_by_a_pass(&controller).await;

    let kept = snapshot_keys(&cluster).await;
    assert_eq!(kept.len(), 2);
    assert!(kept.contains(&foreign_key), "{kept:?}");
}

#[tokio::test(start_paused = true)]
async fn pass_succeeds_without_a_snapshot_while_the_object_store_is_down() {
    let cluster = cluster_with_a_stream().await;
    cluster.artifact_store().set_unavailable(true);

    let report = controller(&cluster).run_pass().await;

    assert_eq!(
        report.map(|report| report.metadata_snapshots_written),
        Ok(0)
    );
}

#[tokio::test(start_paused = true)]
async fn failed_snapshot_is_taken_on_the_next_pass() {
    let cluster = cluster_with_a_stream().await;
    let controller = controller(&cluster);
    cluster.artifact_store().set_unavailable(true);
    snapshots_written_by_a_pass(&controller).await;
    cluster.artifact_store().set_unavailable(false);

    let written = snapshots_written_by_a_pass(&controller).await;

    assert_eq!(written, 1);
}

/// The listing succeeds and the write does not: nothing may be remembered as done.
#[tokio::test(start_paused = true)]
async fn snapshot_whose_write_timed_out_is_taken_on_the_next_pass() {
    let cluster = cluster_with_a_stream().await;
    let controller = controller(&cluster);
    snapshots_written_by_a_pass(&controller).await;
    let_pass(
        &cluster,
        ControllerSettings::default().metadata_snapshot_interval,
    )
    .await;
    cluster.artifact_store().set_unresponsive(true);
    let written_during_the_outage = snapshots_written_by_a_pass(&controller).await;
    cluster.artifact_store().set_unresponsive(false);

    let written_afterwards = snapshots_written_by_a_pass(&controller).await;

    assert_eq!(written_during_the_outage, 0);
    assert_eq!(written_afterwards, 1);
}
