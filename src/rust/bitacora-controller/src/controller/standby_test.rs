//! Tests of standby placement.
//!
//! # What is covered
//!
//! - The rule for whether a replica can take a new segment, as a pure function: unregistered, silent, draining, full.
//! - The step against a cluster: a stream without a standby gets one; a usable standby is left alone; a consumed
//!   standby is placed again; a standby that names a dead replica is replaced; too few replicas place nothing and
//!   are not an error.
//! - Spread: a standby that was placed while a zone was down is replaced once the zone is back, and a standby that
//!   is as widely spread as the cluster allows is left alone, also when placement would now name other replicas.
//! - Load: the standbys of many streams go to both replicas of a zone, not all to one.
//! - The reason for all of it: a writer keeps rolling while a controller runs.
//!
//! # How the fixtures work
//!
//! The cluster tests create the test stream without a first segment, so that the first pass has a standby to place.
//! The pure tests build a replica registration by hand, registered at [`NOW`] with half of its disk free.

use std::sync::Arc;
use std::time::Duration;

use bitacora_client::{TakeoverPolicy, WriterSettings};
use bitacora_meta::{RECORD_SCHEMA_VERSION, RetentionPolicy};
use bitacora_replica::ReplicaSettings;
use bitacora_testkit::{Cluster, FailureDomainLayout};
use bitacora_types::ReplicationSettings;
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

    let standby = stream(&cluster).await.standby_replicas.expect("a standby is placed");
    assert_eq!(report.standbys_placed, 1);
    assert_eq!(in_identifier_order(standby), cluster.replica_ids(&[0, 1, 2]));
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
    let first_standby = stream(&cluster).await.standby_replicas.expect("a standby is placed");
    let replica_that_dies = first_standby[0].clone();
    cluster.nodes()[node_number_of(&cluster, &replica_that_dies)].crash();
    let longer_than_the_liveness_window = Duration::from_secs(30);
    tokio::time::advance(longer_than_the_liveness_window).await;
    renew_registrations(&cluster).await;

    run_pass(&cluster).await;

    let standby = stream(&cluster)
        .await
        .standby_replicas
        .expect("three replicas are left to place on");
    assert!(!standby.contains(&replica_that_dies), "{standby:?}");
}

/// The replicas of `replica_set` in the order of their identifiers, to compare two sets whatever order placement
/// chose their replicas in.
fn in_identifier_order(mut replica_set: Vec<ReplicaId>) -> Vec<ReplicaId> {
    replica_set.sort();
    replica_set
}

/// The number of the node whose replica is `replica_id`.
fn node_number_of(cluster: &Cluster, replica_id: &ReplicaId) -> usize {
    for (node_number, node) in cluster.nodes().iter().enumerate() {
        if node.replica_id() == *replica_id {
            return node_number;
        }
    }
    panic!("{replica_id} is not a replica of the cluster");
}

/// How many nodes the cluster with two nodes in every zone has.
const NODES_IN_THREE_FULL_ZONES: usize = 6;

/// Six nodes in three zones, two in each: node `N` and node `N + 3` share zone `N`.
async fn cluster_with_two_nodes_in_every_zone() -> Cluster {
    let three_zones = FailureDomainLayout::HostsInZones { zone_count: 3 };
    Cluster::start_with(NODES_IN_THREE_FULL_ZONES, ReplicaSettings::default(), three_zones).await
}

/// Creates `stream_count` streams, `stream-0` and so on, without placing their first segments.
async fn create_streams(cluster: &Cluster, stream_count: usize) {
    for stream_number in 0..stream_count {
        cluster
            .metadata()
            .create_stream(
                &format!("stream-{stream_number}"),
                ReplicationSettings::DEFAULT,
                RetentionPolicy::default(),
                None,
            )
            .await
            .expect("the stream does not exist yet");
    }
}

/// The replica set that has, for every replica of `replica_set`, the other replica of its zone in the cluster with
/// two nodes in every zone. It is spread exactly as widely as `replica_set`.
fn other_replicas_of_the_same_zones(cluster: &Cluster, replica_set: &[ReplicaId]) -> Vec<ReplicaId> {
    let every_replica = cluster.replica_ids(&[0, 1, 2, 3, 4, 5]);
    let mut other_replicas = Vec::new();
    for replica_id in replica_set {
        for (node_number, candidate) in every_replica.iter().enumerate() {
            if candidate == replica_id {
                let zone_neighbour = (node_number + 3) % NODES_IN_THREE_FULL_ZONES;
                other_replicas.push(every_replica[zone_neighbour].clone());
            }
        }
    }
    other_replicas
}

/// What happened on a real cluster with two replicas in a zone: every stream got the same one of the two, because it
/// was a little emptier, and the other stayed idle. The key of each stream's next segment now decides between them.
#[tokio::test(start_paused = true)]
async fn standbys_of_many_streams_use_both_replicas_of_a_zone() {
    let cluster = cluster_with_two_nodes_in_every_zone().await;
    create_streams(&cluster, 32).await;

    run_pass(&cluster).await;

    let streams = cluster
        .metadata()
        .list_streams()
        .await
        .expect("the metadata store is available");
    let first_node_of_zone_0 = cluster.replica_ids(&[0])[0].clone();
    let mut standbys_on_the_first_node = 0;
    for stream in &streams {
        let standby = stream.standby_replicas.as_deref().unwrap_or_default();
        if standby.contains(&first_node_of_zone_0) {
            standbys_on_the_first_node += 1;
        }
    }
    assert!(
        (8..=24).contains(&standbys_on_the_first_node),
        "{standbys_on_the_first_node}"
    );
}

/// A standby that placement would not name today, because the key, the room or the registry changed since it was
/// placed, is still as good as the one it would name. Replacing it would move standbys around for nothing.
#[tokio::test(start_paused = true)]
async fn standby_spread_as_widely_as_the_fresh_choice_is_left_alone_although_it_names_other_replicas() {
    let cluster = cluster_with_two_nodes_in_every_zone().await;
    create_stream(&cluster).await;
    run_pass(&cluster).await;
    let fresh_choice = stream(&cluster).await.standby_replicas.expect("a standby is placed");
    let equally_spread_standby = other_replicas_of_the_same_zones(&cluster, &fresh_choice);
    cluster
        .metadata()
        .update_stream(STREAM, |record| {
            record.standby_replicas = Some(equally_spread_standby.clone());
            Ok(())
        })
        .await
        .expect("the standby is written");

    run_pass(&cluster).await;

    assert_eq!(stream(&cluster).await.standby_replicas, Some(equally_spread_standby));
}

/// Four nodes in three zones: nodes 0 and 3 share zone 0, node 1 is all of zone 1, node 2 all of zone 2.
async fn cluster_with_one_node_in_zone_1() -> Cluster {
    let three_zones = FailureDomainLayout::HostsInZones { zone_count: 3 };
    Cluster::start_with(4, ReplicaSettings::default(), three_zones).await
}

/// What a rolling upgrade does to a cluster in which one zone has a single replica: the standby is placed while
/// that replica is away, in two zones, and nothing made it right again when the replica returned. The next segment
/// of the stream then had two of its three copies in one zone.
#[tokio::test(start_paused = true)]
async fn standby_placed_while_a_zone_was_down_is_replaced_when_the_zone_is_back() {
    let cluster = cluster_with_one_node_in_zone_1().await;
    create_stream(&cluster).await;
    cluster.nodes()[1].crash();
    let longer_than_the_liveness_window = Duration::from_secs(30);
    tokio::time::advance(longer_than_the_liveness_window).await;
    renew_registrations(&cluster).await;
    run_pass(&cluster).await;
    cluster.nodes()[1].restart().await;
    renew_registrations(&cluster).await;

    run_pass(&cluster).await;

    let standby = stream(&cluster).await.standby_replicas.expect("a standby is placed");
    let node_of_zone_1 = cluster.replica_ids(&[1])[0].clone();
    assert!(standby.contains(&node_of_zone_1), "{standby:?}");
}

#[tokio::test(start_paused = true)]
async fn standby_placed_while_a_zone_was_down_spans_the_zones_that_were_up() {
    let cluster = cluster_with_one_node_in_zone_1().await;
    create_stream(&cluster).await;
    cluster.nodes()[1].crash();
    let longer_than_the_liveness_window = Duration::from_secs(30);
    tokio::time::advance(longer_than_the_liveness_window).await;
    renew_registrations(&cluster).await;

    run_pass(&cluster).await;

    let standby = stream(&cluster)
        .await
        .standby_replicas
        .expect("three replicas are up to place on");
    assert_eq!(in_identifier_order(standby), cluster.replica_ids(&[0, 2, 3]));
}

/// The standby is replaced for a wider spread only: a set that is spread as widely as the cluster allows stays,
/// pass after pass.
#[tokio::test(start_paused = true)]
async fn standby_spread_over_every_zone_is_left_alone() {
    let cluster = cluster_with_one_node_in_zone_1().await;
    create_stream(&cluster).await;
    run_pass(&cluster).await;

    let second_pass = run_pass(&cluster).await;

    assert_eq!(second_pass.standbys_placed, 0);
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
