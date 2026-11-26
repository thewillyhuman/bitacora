//! What the tests of the controller's steps share.
//!
//! # Why this file exists
//!
//! Every step of a pass is tested in its own file, and most of those tests need the same things: a cluster with a
//! stream, a controller to run a pass with, a segment that was sealed under some condition, a replica that is lost.
//! Written once per test file they would drift apart; written here they say the same thing everywhere. The rules
//! that are pure functions are tested on hand-built records, and the builders of those records are here too.
//!
//! # What it contains
//!
//! For tests against the in-process cluster of `bitacora-testkit`:
//!
//! - [`controller_named`], [`run_pass`]: a controller with the default settings, and one pass of it.
//! - [`controller_with_object_store`]: a controller that tiers and snapshots.
//! - [`create_stream`], [`stream`], [`client`], [`open_writer`], [`append`]: the stream [`STREAM`] and writers to it.
//! - [`cluster_with_a_sealed_segment`] and [`Writes`]: one sealed segment, written in a way a test names.
//! - [`cluster_with_segments`]: several sealed segments of one record each, and an open one.
//! - [`checkpoint_at`]: a materializer with a committed checkpoint.
//! - [`lose_node`], [`damage_first_segment_on`]: the two ways a test destroys a copy.
//! - [`let_pass`], [`renew_registrations`]: moving the virtual clock without losing the replicas.
//! - [`copy_states`], [`listed_segments`], [`held_segments`], [`first_segment_address`]: what tests look at.
//!
//! For tests of the pure rules:
//!
//! - [`stream_record`], [`checkpoint_record`], [`held_copy`], [`replica_id`]: hand-built records.
//!   [`stream_record`] has two segments sealed long ago and an open one, which [`first_sealed_segment`],
//!   [`second_sealed_segment`] and [`open_segment`] also give one by one.
//!
//! # How it works
//!
//! The cluster is real replicas on in-memory disks behind in-process links, with a virtual clock; a test moves time
//! with `tokio::time::advance` and must then renew the replicas' registrations, as their own timers would. Nothing
//! here starts a controller loop: a test runs passes one at a time, so that it can look in between.
//!
//! # Invariants
//!
//! - Fixtures panic with a reason when the cluster cannot be brought into the state they promise. A test never has
//!   to check that its fixture worked.
//! - Every helper that changes the cluster says so in its name.
//!
//! # See also
//!
//! - `bitacora-testkit/src/cluster.rs`: the in-process cluster.
//! - The `*_test.rs` files next to this one: the tests that use it.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use bitacora_artifact::MemoryArtifactStore;
use bitacora_client::{BitacoraClient, ClientSettings, StreamWriter, TakeoverPolicy, WriterSettings};
use bitacora_meta::{
    CheckpointRecord, CopyRecord, CopyState, RECORD_SCHEMA_VERSION, RetentionPolicy, SealRecord, SegmentRecord,
    StreamRecord,
};
use bitacora_testkit::Cluster;
use bitacora_types::replica_api::{ReplicaApi, SegmentStatus};
use bitacora_types::{
    Epoch, ReplicaId, ReplicationSettings, SegmentAddress, SegmentId, SequenceNumber, StreamId, Timestamp,
};
use bytes::Bytes;

use super::{Controller, ControllerSettings, PassReport};

/// The stream every cluster test works on.
pub(crate) const STREAM: &str = "orders";

/// The identity of the controller that [`run_pass`] runs.
pub(crate) const CONTROLLER_IDENTITY: &str = "controller-1";

/// The identifier of the stream that [`stream_record`] describes.
pub(crate) const RECORD_STREAM_ID: StreamId = StreamId::new(7);

/// A byte offset that lies inside the first batch of a segment file, counted from the end of the file's header.
const OFFSET_INSIDE_THE_FIRST_BATCH: usize = 30;

/// The bit that [`damage_first_segment_on`] flips.
const FLIPPED_BIT: u8 = 0x20;

/// How long a test waits for heartbeats to tell every replica what is durable.
const HEARTBEAT_SETTLE: Duration = Duration::from_millis(200);

/// The size of the records that [`cluster_with_segments`] appends: larger than its segments' target size, so that
/// every record fills a segment.
const SEGMENT_FILLING_RECORD_BYTES: usize = 2048;

/// The target segment size of the writer of [`cluster_with_segments`].
const ONE_RECORD_SEGMENT_BYTES: u64 = 1024;

/// Builds a controller over `cluster` with the default settings and the given identity. It has no object store.
pub(crate) fn controller_named(cluster: &Cluster, identity: &str) -> Controller {
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

/// Builds a controller over `cluster` with `settings` that tiers and snapshots to `artifact_store`.
pub(crate) fn controller_with_object_store(
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

/// Runs one pass of a controller with the default settings and no object store.
pub(crate) async fn run_pass(cluster: &Cluster) -> PassReport {
    controller_named(cluster, CONTROLLER_IDENTITY)
        .run_pass()
        .await
        .expect("the metadata store of the test cluster is available")
}

/// Creates [`STREAM`] without placing its first segment, so that a pass has a standby to place.
pub(crate) async fn create_stream(cluster: &Cluster) {
    cluster
        .metadata()
        .create_stream(STREAM, ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
        .await
        .expect("the stream does not exist yet");
}

/// Reads the record of [`STREAM`].
pub(crate) async fn stream(cluster: &Cluster) -> StreamRecord {
    cluster
        .metadata()
        .get_stream(STREAM)
        .await
        .expect("the test created the stream")
}

/// Builds a client of `cluster` whose writers use `settings`.
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

/// Opens a writer of [`STREAM`] with the default settings, taking the stream over if it has a writer.
pub(crate) async fn open_writer(cluster: &Cluster) -> StreamWriter {
    client(cluster, WriterSettings::default())
        .open_writer(STREAM, TakeoverPolicy::Force)
        .await
        .expect("the stream has a standby and enough replicas answer")
}

/// Appends `count` small records and waits for each to commit.
pub(crate) async fn append(writer: &StreamWriter, count: usize) {
    for number in 0..count {
        writer
            .append(Bytes::from(format!("record-{number}")))
            .await
            .expect("enough replicas answer for the record to commit");
    }
}

/// What happens while the one segment of [`cluster_with_a_sealed_segment`] is written.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Writes {
    /// Nothing is written: the segment is sealed empty.
    Nothing,
    /// Three records reach every replica.
    ThreeRecords,
    /// Three records reach every replica, and then the third node is cut off, so that it misses the seal. It is
    /// reachable again when the fixture returns.
    ThreeRecordsThenTheThirdNodeMissesTheSeal,
    /// One record reaches every replica; the third node is then cut off and misses two more records and the seal.
    /// It is reachable again when the fixture returns, with a copy that is two records short.
    OneRecordThenTwoThatTheThirdNodeMisses,
}

/// Starts a cluster of `node_count` nodes whose stream has exactly one segment, sealed, written as `what_is_written`
/// says.
///
/// The segment's copies are unverified: no pass has run since the seal. `node_count` must be at least three, the
/// stream's replication.
pub(crate) async fn cluster_with_a_sealed_segment(node_count: usize, what_is_written: Writes) -> Cluster {
    let cluster = Cluster::start(node_count).await;
    create_stream(&cluster).await;
    run_pass(&cluster).await;
    let writer = open_writer(&cluster).await;
    let third_node = &cluster.nodes()[2];
    match what_is_written {
        Writes::Nothing => {}
        Writes::ThreeRecords => append(&writer, 3).await,
        Writes::ThreeRecordsThenTheThirdNodeMissesTheSeal => {
            append(&writer, 3).await;
            tokio::time::sleep(HEARTBEAT_SETTLE).await;
            third_node.set_reachable(false);
        }
        Writes::OneRecordThenTwoThatTheThirdNodeMisses => {
            append(&writer, 1).await;
            tokio::time::sleep(HEARTBEAT_SETTLE).await;
            third_node.set_reachable(false);
            append(&writer, 2).await;
        }
    }
    writer.close().await.expect("closing seals the segment");
    third_node.set_reachable(true);
    cluster
}

/// Starts a cluster whose stream has `sealed_segment_count` sealed segments of one record each, so that segment `n`
/// holds exactly record `n`, and an open, empty segment after them.
///
/// The writer is left running: the stream keeps an open segment and a live lease, which is the state truncation has
/// to respect. A test that moves the clock by hours makes that writer take its replicas for silent and roll, so
/// such a test must look at the sealed segments by number and not at the whole list.
pub(crate) async fn cluster_with_segments(node_count: usize, sealed_segment_count: u64) -> Cluster {
    let cluster = Cluster::start(node_count).await;
    create_stream(&cluster).await;
    run_pass(&cluster).await;
    let one_record_per_segment = WriterSettings {
        target_segment_bytes: ONE_RECORD_SEGMENT_BYTES,
        // Only size rolls the segment, however far a test moves the clock.
        target_segment_age: Duration::from_hours(100_000),
        ..WriterSettings::default()
    };
    let writer = client(&cluster, one_record_per_segment)
        .open_writer(STREAM, TakeoverPolicy::Force)
        .await
        .expect("the stream has a standby and every replica answers");
    for _segment in 0..sealed_segment_count {
        // The pass places the standby that the roll after this record consumes.
        run_pass(&cluster).await;
        writer
            .append(Bytes::from(vec![0xAB; SEGMENT_FILLING_RECORD_BYTES]))
            .await
            .expect("every replica answers");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    // Leak the writer; see the function's documentation.
    std::mem::forget(writer);
    let expected_segment_count = usize::try_from(sealed_segment_count).expect("a small number") + 1;
    assert_eq!(
        stream(&cluster).await.segments.len(),
        expected_segment_count,
        "the fixture did not roll once per record"
    );
    cluster
}

/// Registers materializer `name` on [`STREAM`] and commits its checkpoint at `sequence`.
pub(crate) async fn checkpoint_at(cluster: &Cluster, name: &str, sequence: u64) {
    let metadata = cluster.metadata();
    let stream_id = stream(cluster).await.stream_id;
    metadata
        .register_materializer(STREAM, name)
        .await
        .expect("the stream exists");
    let fencing_token = metadata
        .acquire_checkpoint(stream_id, name)
        .await
        .expect("the materializer was just registered")
        .fencing_token;
    metadata
        .commit_checkpoint(stream_id, name, fencing_token, SequenceNumber::new(sequence), None)
        .await
        .expect("the token was just acquired");
}

/// Crashes the node at `node_number` and lets enough time pass for the controller to give its copies up. The other
/// nodes renew their registrations, as their timers would have.
pub(crate) async fn lose_node(cluster: &Cluster, node_number: usize) {
    cluster.nodes()[node_number].crash();
    tokio::time::advance(ControllerSettings::default().replica_lost_after * 2).await;
    renew_registrations(cluster).await;
}

/// Moves the virtual clock forward by `duration` and renews the registration of every running node, as their own
/// timers would have.
pub(crate) async fn let_pass(cluster: &Cluster, duration: Duration) {
    tokio::time::advance(duration).await;
    renew_registrations(cluster).await;
}

/// Renews the registration of every running node of `cluster`.
pub(crate) async fn renew_registrations(cluster: &Cluster) {
    cluster
        .renew_registrations()
        .await
        .expect("the metadata store of the test cluster is available");
}

/// Flips a bit inside the first batch of the first segment's copy on the node at `node_number`, and has the node's
/// scrubber find it.
pub(crate) async fn damage_first_segment_on(cluster: &Cluster, node_number: usize) {
    let segment = first_segment_address(cluster).await;
    let node = &cluster.nodes()[node_number];
    node.disk().corrupt_byte(
        &format!("{}/{}.seg", segment.stream_id, segment.segment_id),
        bitacora_segment::format::SEGMENT_HEADER_BYTES + OFFSET_INSIDE_THE_FIRST_BATCH,
        FLIPPED_BIT,
    );
    node.replica()
        .expect("the node is running")
        .scrub_sealed_segments()
        .await
        .expect("the replica is running");
}

/// The address of the first segment of [`STREAM`].
pub(crate) async fn first_segment_address(cluster: &Cluster) -> SegmentAddress {
    SegmentAddress::new(stream(cluster).await.stream_id, SegmentId::FIRST)
}

/// The states of the copies of the first listed segment of [`STREAM`], in replica-set order.
pub(crate) async fn copy_states(cluster: &Cluster) -> Vec<CopyState> {
    let stream = stream(cluster).await;
    let mut states = Vec::new();
    for copy in &stream.segments[0].copies {
        states.push(copy.state);
    }
    states
}

/// The identifiers of the segments that the record of [`STREAM`] lists, oldest first.
pub(crate) async fn listed_segments(cluster: &Cluster) -> Vec<u64> {
    let stream = stream(cluster).await;
    let mut segment_ids = Vec::new();
    for segment in &stream.segments {
        segment_ids.push(segment.segment_id.get());
    }
    segment_ids
}

/// The identifiers of the segments that the node at `node_number` holds something of, in order. The open segment is
/// not among them until a record is appended to it.
pub(crate) async fn held_segments(cluster: &Cluster, node_number: usize) -> Vec<u64> {
    let statuses = cluster.nodes()[node_number]
        .replica()
        .expect("the node is running")
        .list_segments()
        .await
        .expect("the replica is running");
    let mut segment_ids = Vec::new();
    for status in &statuses {
        segment_ids.push(status.address.segment_id.get());
    }
    segment_ids
}

/// The replica identifier `name`.
pub(crate) fn replica_id(name: &str) -> ReplicaId {
    ReplicaId::new(name).expect("the test uses a valid identifier")
}

/// The first segment of [`stream_record`]: records 1 to 9, sealed at 1 second, with one good copy on `replica-a`.
pub(crate) fn first_sealed_segment() -> SegmentRecord {
    segment_with_one_good_copy(1, 1, Some(seal_record(9, 1_000)))
}

/// The second segment of [`stream_record`]: records 10 to 19, sealed at 2 seconds, with one good copy on
/// `replica-a`.
pub(crate) fn second_sealed_segment() -> SegmentRecord {
    segment_with_one_good_copy(2, 10, Some(seal_record(19, 2_000)))
}

/// The last segment of [`stream_record`]: open, starting at record 20, with one good copy on `replica-a`.
pub(crate) fn open_segment() -> SegmentRecord {
    segment_with_one_good_copy(3, 20, None)
}

/// A segment record that starts at `first_sequence` and has one good copy on `replica-a`.
fn segment_with_one_good_copy(segment_id: u64, first_sequence: u64, sealed: Option<SealRecord>) -> SegmentRecord {
    SegmentRecord {
        segment_id: SegmentId::new(segment_id),
        epoch: Epoch::new(1),
        first_sequence: SequenceNumber::new(first_sequence),
        sealed,
        copies: vec![CopyRecord {
            replica_id: replica_id("replica-a"),
            state: CopyState::Good,
        }],
        tiered: None,
    }
}

/// The seal of a segment whose last record is `end`, set `sealed_at_millis` milliseconds into the test's time.
fn seal_record(end: u64, sealed_at_millis: u64) -> SealRecord {
    SealRecord {
        end: SequenceNumber::new(end),
        at: Timestamp::from_unix_millis(sealed_at_millis),
    }
}

/// A hand-built stream record: [`first_sealed_segment`], [`second_sealed_segment`] and [`open_segment`]. No
/// materializer is registered, nothing was truncated, and replication is the default.
pub(crate) fn stream_record() -> StreamRecord {
    StreamRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        stream_id: RECORD_STREAM_ID,
        name: STREAM.to_owned(),
        replication: ReplicationSettings::DEFAULT,
        retention: RetentionPolicy::default(),
        epoch: Epoch::new(1),
        writer: None,
        segments: vec![first_sealed_segment(), second_sealed_segment(), open_segment()],
        next_segment_id: SegmentId::new(4),
        standby_replicas: None,
        truncate_before: None,
        low_water_mark: SequenceNumber::FIRST,
        materializers: BTreeSet::new(),
    }
}

/// A hand-built checkpoint record of `materializer` at `checkpoint`.
pub(crate) fn checkpoint_record(materializer: &str, checkpoint: u64) -> CheckpointRecord {
    CheckpointRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        materializer: materializer.to_owned(),
        checkpoint: SequenceNumber::new(checkpoint),
        fencing_token: 1,
        manifest: None,
    }
}

/// What a replica reports about its copy of segment `segment_id` of the stream of [`stream_record`]: sealed at
/// `sealed_end` and complete, or holding nothing and knowing no seal if `sealed_end` is `None`.
pub(crate) fn held_copy(segment_id: u64, sealed_end: Option<u64>) -> SegmentStatus {
    SegmentStatus {
        address: SegmentAddress::new(RECORD_STREAM_ID, SegmentId::new(segment_id)),
        local_end: sealed_end.map(SequenceNumber::new),
        fenced_epoch: Epoch::new(1),
        sealed_end: sealed_end.map(SequenceNumber::new),
        size_bytes: 0,
        is_damaged: false,
        descriptor: None,
    }
}
