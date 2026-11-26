//! Tests of standby placement.
//!
//! # What is covered
//!
//! - The rule for whether a replica can take a new segment, as a pure function: unregistered, silent, draining, full.
//! - The step against a cluster: a stream without a standby gets one; a usable standby is left alone; a consumed
//!   standby is placed again; a standby that names a dead replica is replaced; too few replicas place nothing and
//!   are not an error.
//! - The reason for all of it: a writer keeps rolling while a controller runs.
//!
//! # How the fixtures work
//!
//! The cluster tests create the test stream without a first segment, so that the first pass has a standby to place.
//! The pure tests build a replica registration by hand, registered at [`NOW`] with half of its disk free.

use std::sync::Arc;
use std::time::Duration;

use bitacora_client::{TakeoverPolicy, WriterSettings};
use bitacora_meta::RECORD_SCHEMA_VERSION;
use bitacora_testkit::Cluster;
use bytes::Bytes;

use super::super::test_support::{
    CONTROLLER_IDENTITY, STREAM, client, controller_named, create_stream, open_writer, renew_registrations, replica_id,
    run_pass, stream,
};
use super::*;

/// The time of the pass in the pure tests.
const NOW: Timestamp = Timestamp::from_unix_millis(10_000_000);

/// The capacity of the disk of [`registration`], in bytes.
const DISK_CAPACITY_BYTES: u64 = 1_000;

/// A replica that registered at [`NOW`], is not draining, and has half of its disk free.
fn registration() -> ReplicaRecord {
    ReplicaRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        replica_id: replica_id("replica-a"),
        address: "replica-a:7000".to_owned(),
        failure_domains: vec!["host-a".to_owned()],
        capacity_bytes: DISK_CAPACITY_BYTES,
        free_bytes: DISK_CAPACITY_BYTES / 2,
        is_draining: false,
        heartbeat_at: NOW,
    }
}

#[test]
fn live_replica_with_room_can_take_segments() {
    let replica = registration();

    assert!(can_take_segments(Some(&replica), NOW, PlacementPolicy::default()));
}

#[test]
fn unregistered_replica_cannot_take_segments() {
    assert!(!can_take_segments(None, NOW, PlacementPolicy::default()));
}

#[test]
fn replica_silent_for_longer_than_the_liveness_window_cannot_take_segments() {
    let policy = PlacementPolicy::default();
    let replica = registration();
    let after_the_liveness_window = NOW.plus(policy.liveness_window * 2);

    assert!(!can_take_segments(Some(&replica), after_the_liveness_window, policy));
}

#[test]
fn draining_replica_cannot_take_segments() {
    let replica = ReplicaRecord {
        is_draining: true,
        ..registration()
    };

    assert!(!can_take_segments(Some(&replica), NOW, PlacementPolicy::default()));
}

#[test]
fn replica_with_a_full_disk_cannot_take_segments() {
    let replica = ReplicaRecord {
        free_bytes: 0,
        ..registration()
    };

    assert!(!can_take_segments(Some(&replica), NOW, PlacementPolicy::default()));
}

#[tokio::test(start_paused = true)]
async fn pass_places_a_standby_for_a_stream_without_one() {
    let cluster = Cluster::start(3).await;
    create_stream(&cluster).await;

    let report = run_pass(&cluster).await;

    assert_eq!(report.standbys_placed, 1);
    assert_eq!(
        stream(&cluster).await.standby_replicas,
        Some(cluster.replica_ids(&[0, 1, 2]))
    );
}

#[tokio::test(start_paused = true)]
async fn usable_standby_is_left_alone() {
    let cluster = Cluster::start(3).await;
    create_stream(&cluster).await;
    run_pass(&cluster).await;

    let second_pass = run_pass(&cluster).await;

    assert_eq!(second_pass.standbys_placed, 0);
}

#[tokio::test(start_paused = true)]
async fn writer_that_opens_a_segment_consumes_the_standby() {
    let cluster = Cluster::start(3).await;
    create_stream(&cluster).await;
    run_pass(&cluster).await;

    let _writer = open_writer(&cluster).await;

    assert_eq!(stream(&cluster).await.standby_replicas, None);
}

#[tokio::test(start_paused = true)]
async fn standby_is_placed_again_after_a_writer_consumes_it() {
    let cluster = Cluster::start(3).await;
    create_stream(&cluster).await;
    run_pass(&cluster).await;
    let _writer = open_writer(&cluster).await;

    let report = run_pass(&cluster).await;

    assert_eq!(report.standbys_placed, 1);
}

#[tokio::test(start_paused = true)]
async fn standby_naming_a_dead_replica_is_replaced() {
    let cluster = Cluster::start(4).await;
    create_stream(&cluster).await;
    run_pass(&cluster).await;
    cluster.nodes()[1].crash();
    let longer_than_the_liveness_window = Duration::from_secs(30);
    tokio::time::advance(longer_than_the_liveness_window).await;
    renew_registrations(&cluster).await;

    run_pass(&cluster).await;

    assert_eq!(
        stream(&cluster).await.standby_replicas,
        Some(cluster.replica_ids(&[0, 2, 3]))
    );
}

#[tokio::test(start_paused = true)]
async fn too_few_replicas_place_no_standby() {
    let cluster = Cluster::start(2).await;
    create_stream(&cluster).await;

    run_pass(&cluster).await;

    assert_eq!(stream(&cluster).await.standby_replicas, None);
}

#[tokio::test(start_paused = true)]
async fn too_few_replicas_are_not_reported_as_an_error() {
    let cluster = Cluster::start(2).await;
    create_stream(&cluster).await;

    let report = run_pass(&cluster).await;

    assert_eq!(report, PassReport::default());
}

#[tokio::test(start_paused = true)]
async fn writer_keeps_rolling_while_the_controller_runs() {
    let cluster = Arc::new(Cluster::start(3).await);
    create_stream(&cluster).await;
    let running_controller = {
        let cluster = Arc::clone(&cluster);
        tokio::spawn(async move { controller_named(&cluster, CONTROLLER_IDENTITY).run().await })
    };
    tokio::time::sleep(Duration::from_secs(2)).await;
    let one_record_per_segment = WriterSettings {
        target_segment_bytes: 1024,
        ..WriterSettings::default()
    };
    let writer = client(&cluster, one_record_per_segment)
        .open_writer(STREAM, TakeoverPolicy::Force)
        .await
        .expect("the controller placed a standby");

    for _roll in 0..3 {
        writer
            .append(Bytes::from(vec![0xAB; 2048]))
            .await
            .expect("every replica answers");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    running_controller.abort();
    assert_eq!(stream(&cluster).await.segments.len(), 4);
}
