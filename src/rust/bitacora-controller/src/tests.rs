//! The controller against an in-process cluster with real replicas.

use std::sync::Arc;
use std::time::Duration;

use bitacora_client::{
    BitacoraClient, ClientSettings, StreamWriter, TakeoverPolicy, WriterSettings,
};
use bitacora_meta::{CopyState, KeyValueStore, RetentionPolicy, StreamRecord};
use bitacora_replica::ReplicaSettings;
use bitacora_testkit::Cluster;
use bitacora_types::replica_api::ReplicaApi;
use bitacora_types::{ReplicationSettings, SegmentAddress, SegmentId, SequenceNumber};
use bytes::Bytes;

use crate::{Controller, ControllerSettings, PassReport};

pub(crate) const STREAM: &str = "orders";

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

/// Replica settings with small segment files, as the testkit uses.
fn small_segment_files() -> ReplicaSettings {
    ReplicaSettings {
        preallocated_segment_bytes: 4 * 1024,
        ..ReplicaSettings::default()
    }
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

pub(crate) async fn stream(cluster: &Cluster) -> StreamRecord {
    cluster
        .metadata()
        .get_stream(STREAM)
        .await
        .expect("the stream exists")
}

pub(crate) fn client(cluster: &Cluster, settings: WriterSettings) -> BitacoraClient {
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
async fn short_copy_of_a_sealed_segment_is_found_bad() {
    let cluster = cluster_with_a_sealed_segment(3, async |cluster, writer| {
        append(writer, 1).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        cluster.nodes()[2].set_reachable(false);
        append(writer, 2).await;
    })
    .await;
    cluster.nodes()[2].set_reachable(true);
    let report = run_pass(&cluster).await;
    assert_eq!((report.copies_confirmed, report.copies_marked_bad), (2, 1));
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
        .remove_replica(&cluster.nodes()[2].replica_id())
        .await
        .expect("removed");
    run_pass(&cluster).await;
    assert_eq!(copy_states(&cluster).await[2], CopyState::Bad);
}

#[tokio::test(start_paused = true)]
async fn copy_found_damaged_by_the_scrubber_is_found_bad() {
    let cluster = cluster_with_a_sealed_segment(3, async |_cluster, writer| {
        append(writer, 3).await;
    })
    .await;
    run_pass(&cluster).await;
    let segment = first_segment_address(&cluster).await;
    let node = &cluster.nodes()[0];
    node.disk().corrupt_byte(
        &format!("{}/{}.seg", segment.stream_id, segment.segment_id),
        bitacora_segment::format::SEGMENT_HEADER_BYTES + 30,
        0x20,
    );
    node.replica()
        .expect("the node is running")
        .scrub_sealed_segments()
        .await
        .expect("scrubs");
    assert_eq!(run_pass(&cluster).await.copies_marked_bad, 1);
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

// ---- repair ----

#[tokio::test(start_paused = true)]
async fn short_copy_is_completed_in_place() {
    let cluster = cluster_with_a_sealed_segment(3, async |cluster, writer| {
        append(writer, 1).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        cluster.nodes()[2].set_reachable(false);
        append(writer, 2).await;
    })
    .await;
    cluster.nodes()[2].set_reachable(true);
    let report = run_pass(&cluster).await;
    let segment = first_segment_address(&cluster).await;
    assert_eq!(
        (
            report.copies_repaired,
            copy_states(&cluster).await,
            cluster.nodes()[2].local_end(segment).await
        ),
        (1, vec![CopyState::Good; 3], Some(SequenceNumber::new(3)))
    );
}

#[tokio::test(start_paused = true)]
async fn damaged_copy_is_rebuilt_in_place() {
    let cluster = cluster_with_a_sealed_segment(3, async |_cluster, writer| {
        append(writer, 3).await;
    })
    .await;
    run_pass(&cluster).await;
    let segment = first_segment_address(&cluster).await;
    let node = &cluster.nodes()[0];
    node.disk().corrupt_byte(
        &format!("{}/{}.seg", segment.stream_id, segment.segment_id),
        bitacora_segment::format::SEGMENT_HEADER_BYTES + 30,
        0x20,
    );
    let replica = node.replica().expect("the node is running");
    replica.scrub_sealed_segments().await.expect("scrubs");
    run_pass(&cluster).await;
    assert_eq!(
        (
            copy_states(&cluster).await,
            replica.scrub_sealed_segments().await
        ),
        (vec![CopyState::Good; 3], Ok(0))
    );
}

#[tokio::test(start_paused = true)]
async fn copy_on_a_lost_replica_is_rebuilt_on_another_one() {
    let cluster = cluster_with_a_sealed_segment(4, async |_cluster, writer| {
        append(writer, 3).await;
    })
    .await;
    run_pass(&cluster).await;
    cluster.nodes()[2].crash();
    tokio::time::advance(ControllerSettings::default().replica_lost_after * 2).await;
    cluster.renew_registrations().await.expect("renewed");
    let report = run_pass(&cluster).await;

    let segment = first_segment_address(&cluster).await;
    let holders: Vec<_> = stream(&cluster).await.segments[0]
        .copies
        .iter()
        .map(|copy| copy.replica_id.clone())
        .collect();
    assert_eq!(
        (
            report.copies_repaired,
            holders,
            copy_states(&cluster).await,
            cluster.nodes()[3].local_end(segment).await
        ),
        (
            1,
            cluster.replica_ids(&[0, 1, 3]),
            vec![CopyState::Good; 3],
            Some(SequenceNumber::new(3))
        )
    );
}

#[tokio::test(start_paused = true)]
async fn segment_without_a_good_copy_is_left_alone() {
    let cluster = cluster_with_a_sealed_segment(3, async |_cluster, writer| {
        append(writer, 3).await;
    })
    .await;
    run_pass(&cluster).await;
    let segment = first_segment_address(&cluster).await;
    for node in cluster.nodes() {
        node.disk().corrupt_byte(
            &format!("{}/{}.seg", segment.stream_id, segment.segment_id),
            bitacora_segment::format::SEGMENT_HEADER_BYTES + 30,
            0x20,
        );
        node.replica()
            .expect("the node is running")
            .scrub_sealed_segments()
            .await
            .expect("scrubs");
    }
    let report = run_pass(&cluster).await;
    assert_eq!(
        (report.copies_repaired, copy_states(&cluster).await),
        (0, vec![CopyState::Bad; 3])
    );
}

#[tokio::test(start_paused = true)]
async fn lost_copy_without_a_spare_replica_stays_bad_until_one_appears() {
    let cluster = cluster_with_a_sealed_segment(3, async |_cluster, writer| {
        append(writer, 3).await;
    })
    .await;
    run_pass(&cluster).await;
    cluster.nodes()[2].crash();
    tokio::time::advance(ControllerSettings::default().replica_lost_after * 2).await;
    cluster.renew_registrations().await.expect("renewed");
    let while_lost = run_pass(&cluster).await;
    cluster.nodes()[2].restart().await;
    cluster.renew_registrations().await.expect("renewed");
    run_pass(&cluster).await;
    assert_eq!(
        (while_lost.copies_repaired, copy_states(&cluster).await),
        (0, vec![CopyState::Good; 3])
    );
}

#[tokio::test(start_paused = true)]
async fn replacement_copy_stays_in_the_failure_domain_that_lost_one() {
    let zone_of = |node_number: usize| {
        vec![
            format!("zone-{}", node_number % 3),
            format!("host-{node_number}"),
        ]
    };
    let cluster = Cluster::start_with(6, small_segment_files(), zone_of).await;
    create_stream(&cluster).await;
    run_pass(&cluster).await;
    let writer = open_writer(&cluster).await;
    append(&writer, 3).await;
    writer.close().await.expect("the writer closes");
    run_pass(&cluster).await;
    let holders_before = stream(&cluster).await.segments[0].copies.clone();

    // Lose the copy in zone 1 for good.
    let lost = holders_before
        .iter()
        .position(|copy| copy.replica_id == cluster.replica_ids(&[1])[0])
        .expect("the first replica set holds one copy per zone");
    cluster.nodes()[1].crash();
    tokio::time::advance(ControllerSettings::default().replica_lost_after * 2).await;
    cluster.renew_registrations().await.expect("renewed");
    run_pass(&cluster).await;

    assert_eq!(
        stream(&cluster).await.segments[0].copies[lost].replica_id,
        cluster.replica_ids(&[4])[0],
        "replica-4 is the other node of zone 1"
    );
}

// ---- truncation and retention ----

/// A cluster whose stream has `sealed_segment_count` sealed segments of one
/// record each (so segment `n` holds exactly record `n`) and an open one.
pub(crate) async fn cluster_with_segments(node_count: usize, sealed_segment_count: u64) -> Cluster {
    let cluster = Cluster::start(node_count).await;
    create_stream(&cluster).await;
    run_pass(&cluster).await;
    let one_record_per_segment = WriterSettings {
        target_segment_bytes: 1024,
        // Only size rolls the segment, however far a test moves the clock.
        target_segment_age: Duration::from_hours(100_000),
        ..WriterSettings::default()
    };
    let writer = client(&cluster, one_record_per_segment)
        .open_writer(STREAM, TakeoverPolicy::Force)
        .await
        .expect("the writer opens");
    for _segment in 0..sealed_segment_count {
        run_pass(&cluster).await;
        writer
            .append(Bytes::from(vec![0xAB; 2048]))
            .await
            .expect("the record commits");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    // Leak the writer: the stream keeps an open segment and a live lease,
    // which is the state truncation has to respect.
    std::mem::forget(writer);
    assert_eq!(
        stream(&cluster).await.segments.len(),
        usize::try_from(sealed_segment_count).expect("small") + 1
    );
    cluster
}

/// Registers materializer `name` and commits its checkpoint at `sequence`.
pub(crate) async fn checkpoint_at(cluster: &Cluster, name: &str, sequence: u64) {
    let metadata = cluster.metadata();
    let stream_id = stream(cluster).await.stream_id;
    metadata
        .register_materializer(STREAM, name)
        .await
        .expect("registered");
    let token = metadata
        .acquire_checkpoint(stream_id, name)
        .await
        .expect("acquired")
        .fencing_token;
    metadata
        .commit_checkpoint(stream_id, name, token, SequenceNumber::new(sequence), None)
        .await
        .expect("committed");
}

pub(crate) async fn listed_segments(cluster: &Cluster) -> Vec<u64> {
    stream(cluster)
        .await
        .segments
        .iter()
        .map(|segment| segment.segment_id.get())
        .collect()
}

/// The segments of the stream that the node at `node_number` holds a copy
/// of. The open segment is not among them until a record is appended to it.
pub(crate) async fn held_segments(cluster: &Cluster, node_number: usize) -> Vec<u64> {
    cluster.nodes()[node_number]
        .replica()
        .expect("the node is running")
        .list_segments()
        .await
        .expect("lists")
        .iter()
        .map(|status| status.address.segment_id.get())
        .collect()
}

#[tokio::test(start_paused = true)]
async fn nothing_is_deleted_without_a_materializer_or_a_request() {
    let cluster = cluster_with_segments(3, 3).await;
    let report = run_pass(&cluster).await;
    assert_eq!(
        (report.segments_deleted, listed_segments(&cluster).await),
        (0, vec![1, 2, 3, 4])
    );
}

#[tokio::test(start_paused = true)]
async fn segments_below_every_checkpoint_are_deleted() {
    let cluster = cluster_with_segments(3, 3).await;
    checkpoint_at(&cluster, "pages", 2).await;
    let report = run_pass(&cluster).await;
    assert_eq!(
        (
            report.segments_deleted,
            listed_segments(&cluster).await,
            stream(&cluster).await.low_water_mark
        ),
        (2, vec![3, 4], SequenceNumber::new(3))
    );
}

#[tokio::test(start_paused = true)]
async fn deleted_segments_are_removed_from_their_replicas() {
    let cluster = cluster_with_segments(3, 3).await;
    checkpoint_at(&cluster, "pages", 2).await;
    let report = run_pass(&cluster).await;
    assert_eq!(
        (report.copies_removed, held_segments(&cluster, 0).await),
        (6, vec![3])
    );
}

#[tokio::test(start_paused = true)]
async fn slowest_materializer_pins_the_log() {
    let cluster = cluster_with_segments(3, 3).await;
    checkpoint_at(&cluster, "pages", 3).await;
    checkpoint_at(&cluster, "blocks", 0).await;
    let report = run_pass(&cluster).await;
    assert_eq!(
        (report.segments_deleted, listed_segments(&cluster).await),
        (0, vec![1, 2, 3, 4])
    );
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
        .expect("unregistered");
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
        .expect("requested");
    run_pass(&cluster).await;
    assert_eq!(
        (
            listed_segments(&cluster).await,
            stream(&cluster).await.low_water_mark
        ),
        (vec![3, 4], SequenceNumber::new(3))
    );
}

#[tokio::test(start_paused = true)]
async fn open_segment_survives_a_checkpoint_past_its_start() {
    let cluster = cluster_with_segments(3, 2).await;
    checkpoint_at(&cluster, "pages", 500).await;
    run_pass(&cluster).await;
    assert_eq!(
        (
            listed_segments(&cluster).await,
            stream(&cluster).await.low_water_mark
        ),
        (vec![3], SequenceNumber::new(3))
    );
}

#[tokio::test(start_paused = true)]
async fn retention_period_keeps_recently_sealed_segments() {
    let cluster = cluster_with_segments(3, 3).await;
    cluster
        .metadata()
        .update_stream(STREAM, |record| {
            record.retention = RetentionPolicy::keep_for(Duration::from_hours(1));
            Ok(())
        })
        .await
        .expect("policy set");
    checkpoint_at(&cluster, "pages", 3).await;
    let while_recent = run_pass(&cluster).await;
    tokio::time::advance(Duration::from_hours(2)).await;
    cluster.renew_registrations().await.expect("renewed");
    run_pass(&cluster).await;
    let oldest_segment_left = listed_segments(&cluster).await.into_iter().min();
    assert!(
        while_recent.segments_deleted == 0 && oldest_segment_left > Some(3),
        "deleted {} while recent, oldest left is {oldest_segment_left:?}",
        while_recent.segments_deleted
    );
}

#[tokio::test(start_paused = true)]
async fn materializer_registered_after_a_truncation_does_not_move_the_mark_back() {
    let cluster = cluster_with_segments(3, 3).await;
    checkpoint_at(&cluster, "pages", 2).await;
    run_pass(&cluster).await;
    let late_checkpoint = cluster
        .metadata()
        .register_materializer(STREAM, "blocks")
        .await
        .expect("registered");
    run_pass(&cluster).await;
    assert_eq!(
        (
            late_checkpoint.checkpoint,
            stream(&cluster).await.low_water_mark
        ),
        (SequenceNumber::new(2), SequenceNumber::new(3))
    );
}

#[tokio::test(start_paused = true)]
async fn replica_that_missed_a_deletion_is_cleaned_up_when_it_returns() {
    let cluster = cluster_with_segments(3, 3).await;
    checkpoint_at(&cluster, "pages", 2).await;
    cluster.nodes()[2].set_reachable(false);
    run_pass(&cluster).await;
    let held_while_away = held_segments(&cluster, 2).await;
    cluster.nodes()[2].set_reachable(true);
    let report = run_pass(&cluster).await;
    assert_eq!(
        (
            held_while_away,
            report.copies_removed,
            held_segments(&cluster, 2).await
        ),
        (vec![1, 2, 3], 2, vec![3])
    );
}

#[tokio::test(start_paused = true)]
async fn copy_replaced_by_repair_is_removed_when_its_replica_returns() {
    let cluster = cluster_with_a_sealed_segment(4, async |_cluster, writer| {
        append(writer, 3).await;
    })
    .await;
    run_pass(&cluster).await;
    cluster.nodes()[2].crash();
    tokio::time::advance(ControllerSettings::default().replica_lost_after * 2).await;
    cluster.renew_registrations().await.expect("renewed");
    run_pass(&cluster).await;

    cluster.nodes()[2].restart().await;
    cluster.renew_registrations().await.expect("renewed");
    let report = run_pass(&cluster).await;
    assert_eq!(
        (report.copies_removed, held_segments(&cluster, 2).await),
        (1, Vec::new())
    );
}

#[tokio::test(start_paused = true)]
async fn deleted_stream_is_removed_from_replicas_and_then_forgotten() {
    let cluster = cluster_with_segments(3, 2).await;
    cluster
        .metadata()
        .delete_stream(STREAM)
        .await
        .expect("deleted");
    let report = run_pass(&cluster).await;
    assert_eq!(
        (
            report.copies_removed,
            held_segments(&cluster, 0).await,
            cluster.metadata().list_deleted_streams().await
        ),
        (6, Vec::new(), Ok(Vec::new()))
    );
}

#[tokio::test(start_paused = true)]
async fn deleted_stream_is_remembered_while_a_replica_has_not_been_cleaned() {
    let cluster = cluster_with_segments(3, 2).await;
    cluster.nodes()[2].set_reachable(false);
    cluster
        .metadata()
        .delete_stream(STREAM)
        .await
        .expect("deleted");
    run_pass(&cluster).await;
    let remembered = cluster
        .metadata()
        .list_deleted_streams()
        .await
        .expect("lists")
        .len();
    cluster.nodes()[2].set_reachable(true);
    run_pass(&cluster).await;
    assert_eq!(
        (
            remembered,
            held_segments(&cluster, 2).await,
            cluster.metadata().list_deleted_streams().await
        ),
        (1, Vec::new(), Ok(Vec::new()))
    );
}

/// What a metadata store restored from an older snapshot does not know
/// must survive until an operator rebuilds the metadata from it.
#[tokio::test(start_paused = true)]
async fn segments_of_a_stream_the_metadata_store_does_not_know_are_left_alone() {
    let cluster = cluster_with_segments(3, 2).await;
    let known_stream = stream(&cluster).await;
    let key = "streams/orders";
    // Forget the stream without leaving a note, as a restore would.
    cluster
        .key_value_store()
        .transact(
            Vec::new(),
            vec![bitacora_meta::Mutation::Delete {
                key: key.to_owned(),
            }],
        )
        .await
        .expect("forgotten");
    let report = run_pass(&cluster).await;
    assert_eq!(
        (report.copies_removed, held_segments(&cluster, 0).await),
        (0, vec![1, 2]),
        "stream {} was known before",
        known_stream.stream_id
    );
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
