//! The controller against an in-process cluster with real replicas.

use std::sync::Arc;
use std::time::Duration;

use bitacora_client::{
    BitacoraClient, ClientSettings, StreamWriter, TakeoverPolicy, WriterSettings,
};
use bitacora_meta::{CopyState, RetentionPolicy, StreamRecord};
use bitacora_testkit::Cluster;
use bitacora_types::replica_api::ReplicaApi;
use bitacora_types::{ReplicationSettings, SegmentAddress, SegmentId, SequenceNumber};
use bytes::Bytes;

use crate::{Controller, ControllerSettings, PassReport};

const STREAM: &str = "orders";

fn controller(cluster: &Cluster, identity: &str) -> Controller {
    Controller::new(
        cluster.metadata().clone(),
        cluster.connector(),
        cluster.clock(),
        ControllerSettings {
            identity: identity.to_owned(),
            ..ControllerSettings::default()
        },
    )
}

async fn run_pass(cluster: &Cluster) -> PassReport {
    controller(cluster, "controller-1")
        .run_pass()
        .await
        .expect("the pass runs")
}

/// Creates [`STREAM`] without placing its first segment.
async fn create_stream(cluster: &Cluster) {
    cluster
        .metadata()
        .create_stream(
            STREAM,
            ReplicationSettings::DEFAULT,
            RetentionPolicy::default(),
            None,
        )
        .await
        .expect("the stream is created");
}

async fn stream(cluster: &Cluster) -> StreamRecord {
    cluster
        .metadata()
        .get_stream(STREAM)
        .await
        .expect("the stream exists")
}

fn client(cluster: &Cluster, settings: WriterSettings) -> BitacoraClient {
    BitacoraClient::new(
        cluster.metadata().clone(),
        cluster.connector(),
        cluster.clock(),
        ClientSettings {
            writer: settings,
            ..ClientSettings::default()
        },
    )
}

async fn open_writer(cluster: &Cluster) -> StreamWriter {
    client(cluster, WriterSettings::default())
        .open_writer(STREAM, TakeoverPolicy::Force)
        .await
        .expect("the writer opens")
}

async fn append(writer: &StreamWriter, count: usize) {
    for number in 0..count {
        writer
            .append(Bytes::from(format!("record-{number}")))
            .await
            .expect("the record commits");
    }
}

/// The states of the copies of segment 1, in replica-set order.
async fn copy_states(cluster: &Cluster) -> Vec<CopyState> {
    stream(cluster).await.segments[0]
        .copies
        .iter()
        .map(|copy| copy.state)
        .collect()
}

/// A cluster of `node_count` nodes whose stream has one sealed segment
/// holding three records, written while `during_writes` had its way.
async fn cluster_with_a_sealed_segment(
    node_count: usize,
    during_writes: impl AsyncFnOnce(&Cluster, &StreamWriter),
) -> Cluster {
    let cluster = Cluster::start(node_count).await;
    create_stream(&cluster).await;
    run_pass(&cluster).await;
    let writer = open_writer(&cluster).await;
    during_writes(&cluster, &writer).await;
    writer.close().await.expect("the writer closes");
    cluster
}

async fn first_segment_address(cluster: &Cluster) -> SegmentAddress {
    SegmentAddress::new(stream(cluster).await.stream_id, SegmentId::FIRST)
}

// ---- standby placement ----

#[tokio::test(start_paused = true)]
async fn pass_places_a_standby_for_a_stream_without_one() {
    let cluster = Cluster::start(3).await;
    create_stream(&cluster).await;
    let report = run_pass(&cluster).await;
    assert_eq!(
        (
            report.standbys_placed,
            stream(&cluster).await.standby_replicas
        ),
        (1, Some(cluster.replica_ids(&[0, 1, 2])))
    );
}

#[tokio::test(start_paused = true)]
async fn usable_standby_is_left_alone() {
    let cluster = Cluster::start(3).await;
    create_stream(&cluster).await;
    run_pass(&cluster).await;
    assert_eq!(run_pass(&cluster).await.standbys_placed, 0);
}

#[tokio::test(start_paused = true)]
async fn standby_is_placed_again_after_a_writer_consumes_it() {
    let cluster = Cluster::start(3).await;
    create_stream(&cluster).await;
    run_pass(&cluster).await;
    let _writer = open_writer(&cluster).await;
    let consumed = stream(&cluster).await.standby_replicas;
    let report = run_pass(&cluster).await;
    assert_eq!((consumed, report.standbys_placed), (None, 1));
}

#[tokio::test(start_paused = true)]
async fn standby_naming_a_dead_replica_is_replaced() {
    let cluster = Cluster::start(4).await;
    create_stream(&cluster).await;
    run_pass(&cluster).await;
    cluster.nodes()[1].crash();
    tokio::time::advance(Duration::from_secs(30)).await;
    cluster.renew_registrations().await.expect("renewed");
    run_pass(&cluster).await;
    assert_eq!(
        stream(&cluster).await.standby_replicas,
        Some(cluster.replica_ids(&[0, 2, 3]))
    );
}

#[tokio::test(start_paused = true)]
async fn too_few_replicas_place_no_standby_and_report_no_error() {
    let cluster = Cluster::start(2).await;
    create_stream(&cluster).await;
    let report = run_pass(&cluster).await;
    assert_eq!(
        (report, stream(&cluster).await.standby_replicas),
        (PassReport::default(), None)
    );
}

#[tokio::test(start_paused = true)]
async fn writer_keeps_rolling_while_the_controller_runs() {
    let cluster = Arc::new(Cluster::start(3).await);
    create_stream(&cluster).await;
    let running_controller = {
        let cluster = Arc::clone(&cluster);
        tokio::spawn(async move { controller(&cluster, "controller-1").run().await })
    };
    tokio::time::sleep(Duration::from_secs(2)).await;

    let small_segments = WriterSettings {
        target_segment_bytes: 1024,
        ..WriterSettings::default()
    };
    let writer = client(&cluster, small_segments)
        .open_writer(STREAM, TakeoverPolicy::Force)
        .await
        .expect("the writer opens");
    for _roll in 0..3 {
        writer
            .append(Bytes::from(vec![0xAB; 2048]))
            .await
            .expect("the record commits");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    running_controller.abort();
    assert_eq!(stream(&cluster).await.segments.len(), 4);
}

// ---- reconciliation of sealed copies ----

#[tokio::test(start_paused = true)]
async fn complete_copies_of_a_sealed_segment_become_good() {
    let cluster = cluster_with_a_sealed_segment(3, async |_cluster, writer| {
        append(writer, 3).await;
    })
    .await;
    let report = run_pass(&cluster).await;
    assert_eq!(
        (report.copies_confirmed, copy_states(&cluster).await),
        (3, vec![CopyState::Good; 3])
    );
}

#[tokio::test(start_paused = true)]
async fn confirmed_copies_are_not_confirmed_again() {
    let cluster = cluster_with_a_sealed_segment(3, async |_cluster, writer| {
        append(writer, 3).await;
    })
    .await;
    run_pass(&cluster).await;
    assert_eq!(run_pass(&cluster).await.copies_confirmed, 0);
}

#[tokio::test(start_paused = true)]
async fn replica_that_missed_the_seal_learns_it_from_the_controller() {
    let cluster = cluster_with_a_sealed_segment(3, async |cluster, writer| {
        append(writer, 3).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        cluster.nodes()[2].set_reachable(false);
    })
    .await;
    cluster.nodes()[2].set_reachable(true);
    let segment = first_segment_address(&cluster).await;
    let sealed_before = cluster.nodes()[2].readable_sequences(segment).await;
    run_pass(&cluster).await;

    let known_seal: Vec<_> = cluster.nodes()[2]
        .replica()
        .expect("the node is running")
        .list_segments()
        .await
        .expect("lists")
        .iter()
        .map(|status| status.sealed_end)
        .collect();
    assert_eq!(
        (sealed_before, known_seal, copy_states(&cluster).await[2]),
        (
            vec![1, 2, 3],
            vec![Some(SequenceNumber::new(3))],
            CopyState::Good
        )
    );
}

#[tokio::test(start_paused = true)]
async fn short_copy_of_a_sealed_segment_becomes_bad() {
    let cluster = cluster_with_a_sealed_segment(3, async |cluster, writer| {
        append(writer, 1).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        cluster.nodes()[2].set_reachable(false);
        append(writer, 2).await;
    })
    .await;
    cluster.nodes()[2].set_reachable(true);
    let report = run_pass(&cluster).await;
    assert_eq!(
        (report.copies_marked_bad, copy_states(&cluster).await),
        (1, vec![CopyState::Good, CopyState::Good, CopyState::Bad])
    );
}

#[tokio::test(start_paused = true)]
async fn copy_on_a_replica_that_does_not_answer_stays_unverified_at_first() {
    let cluster = cluster_with_a_sealed_segment(3, async |_cluster, writer| {
        append(writer, 3).await;
    })
    .await;
    cluster.nodes()[2].set_reachable(false);
    run_pass(&cluster).await;
    assert_eq!(copy_states(&cluster).await[2], CopyState::Unverified);
}

#[tokio::test(start_paused = true)]
async fn copy_on_a_replica_silent_for_long_becomes_bad() {
    let cluster = cluster_with_a_sealed_segment(3, async |_cluster, writer| {
        append(writer, 3).await;
    })
    .await;
    run_pass(&cluster).await;
    cluster.nodes()[2].crash();
    tokio::time::advance(ControllerSettings::default().replica_lost_after * 2).await;
    cluster.renew_registrations().await.expect("renewed");
    run_pass(&cluster).await;
    assert_eq!(copy_states(&cluster).await[2], CopyState::Bad);
}

#[tokio::test(start_paused = true)]
async fn copy_given_up_for_lost_is_good_again_when_its_replica_returns() {
    let cluster = cluster_with_a_sealed_segment(3, async |_cluster, writer| {
        append(writer, 3).await;
    })
    .await;
    cluster.nodes()[2].crash();
    tokio::time::advance(ControllerSettings::default().replica_lost_after * 2).await;
    cluster.renew_registrations().await.expect("renewed");
    run_pass(&cluster).await;
    cluster.nodes()[2].restart().await;
    cluster.renew_registrations().await.expect("renewed");
    run_pass(&cluster).await;
    assert_eq!(copy_states(&cluster).await[2], CopyState::Good);
}

#[tokio::test(start_paused = true)]
async fn copy_on_a_replica_removed_from_the_registry_becomes_bad() {
    let cluster = cluster_with_a_sealed_segment(3, async |_cluster, writer| {
        append(writer, 3).await;
    })
    .await;
    cluster
        .metadata()
        .remove_replica(cluster.nodes()[2].replica_id())
        .await
        .expect("removed");
    run_pass(&cluster).await;
    assert_eq!(copy_states(&cluster).await[2], CopyState::Bad);
}

#[tokio::test(start_paused = true)]
async fn copy_found_damaged_by_the_scrubber_becomes_bad() {
    let cluster = cluster_with_a_sealed_segment(3, async |_cluster, writer| {
        append(writer, 3).await;
    })
    .await;
    run_pass(&cluster).await;
    let segment = first_segment_address(&cluster).await;
    let node = &cluster.nodes()[0];
    node.disk().corrupt_byte(
        &format!("{}/{}.seg", segment.stream_id, segment.segment_id),
        100,
        0x20,
    );
    node.replica()
        .expect("the node is running")
        .scrub_sealed_segments()
        .await
        .expect("scrubs");
    run_pass(&cluster).await;
    assert_eq!(copy_states(&cluster).await[0], CopyState::Bad);
}

#[tokio::test(start_paused = true)]
async fn segment_sealed_empty_is_complete_on_every_replica() {
    let cluster = cluster_with_a_sealed_segment(3, async |_cluster, _writer| {}).await;
    run_pass(&cluster).await;
    assert_eq!(copy_states(&cluster).await, vec![CopyState::Good; 3]);
}

#[tokio::test(start_paused = true)]
async fn open_segment_is_not_judged() {
    let cluster = Cluster::start(3).await;
    create_stream(&cluster).await;
    run_pass(&cluster).await;
    let writer = open_writer(&cluster).await;
    append(&writer, 3).await;
    run_pass(&cluster).await;
    assert_eq!(copy_states(&cluster).await, vec![CopyState::Unverified; 3]);
}

// ---- leadership ----

async fn leader(cluster: &Cluster) -> Option<String> {
    cluster
        .metadata()
        .get_leader()
        .await
        .expect("the store is available")
        .map(|leader| leader.holder)
}

#[tokio::test(start_paused = true)]
async fn second_controller_takes_over_when_the_first_one_stops() {
    let cluster = Arc::new(Cluster::start(3).await);
    let spawn_controller = |identity: &'static str| {
        let cluster = Arc::clone(&cluster);
        tokio::spawn(async move { controller(&cluster, identity).run().await })
    };
    let first = spawn_controller("controller-1");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let _second = spawn_controller("controller-2");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let leader_while_both_run = leader(&cluster).await;

    first.abort();
    tokio::time::sleep(ControllerSettings::default().leadership_duration * 2).await;
    assert_eq!(
        (leader_while_both_run, leader(&cluster).await),
        (
            Some("controller-1".to_owned()),
            Some("controller-2".to_owned())
        )
    );
}

#[tokio::test(start_paused = true)]
async fn controller_keeps_running_through_a_metadata_outage() {
    let cluster = Arc::new(Cluster::start(3).await);
    let running_controller = {
        let cluster = Arc::clone(&cluster);
        tokio::spawn(async move { controller(&cluster, "controller-1").run().await })
    };
    cluster.key_value_store().set_unavailable(true);
    tokio::time::sleep(Duration::from_secs(5)).await;
    cluster.key_value_store().set_unavailable(false);
    create_stream(&cluster).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    running_controller.abort();
    assert!(stream(&cluster).await.standby_replicas.is_some());
}
