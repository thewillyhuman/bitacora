//! The whole log over gRPC: a client that writes, reads and takes over a
//! stream on three replicas it only reaches through sockets.
//!
//! # What is covered
//!
//! The protocol itself is tested in process, by the client's own tests and
//! by the simulator. This file checks that nothing of it is lost when the
//! replicas are behind the transport: a writer commits records on a quorum,
//! a reader follows them, a second writer fences the first and continues
//! the log, and the first learns that it lost the stream. That last step
//! only works if a replica's `Fenced` error reaches the writer as `Fenced`.
//!
//! # How the fixtures work
//!
//! [`GrpcCluster`] opens three replicas on in-memory disks, serves each on
//! a free loopback port and registers it, with that address, in a metadata
//! store that lives in memory. The clients it hands out get a
//! `GrpcReplicaConnector` and nothing else to reach the replicas with.
//!
//! No controller runs, so the fixture does the one part of its job a
//! takeover needs: placing the standby segment.
//!
//! Time is real here, because sockets are. Nothing sleeps: a reader waits
//! for records by following the stream, under a generous time limit that
//! only matters when the test is about to fail.

mod support;

use std::sync::Arc;
use std::time::Duration;

use bitacora_client::{
    AppendError, BitacoraClient, ClientSettings, RecordStream, StreamWriter, TakeoverPolicy,
    WriterSettings,
};
use bitacora_meta::{
    MemoryKeyValueStore, MetadataStore, RECORD_SCHEMA_VERSION, ReplicaRecord, RetentionPolicy,
};
use bitacora_proto::{GrpcConnectorSettings, GrpcReplicaConnector};
use bitacora_replica::{Replica, ReplicaSettings};
use bitacora_segment::{Disk, MemoryDisk};
use bitacora_types::{Clock, ReplicaId, ReplicationSettings, SequenceNumber, SystemClock};
use bytes::Bytes;
use support::serve_on_loopback;
use tokio::time::timeout;

/// The stream every test writes to.
const STREAM: &str = "orders";

/// The names, and identities, of the three replicas.
const REPLICA_NAMES: [&str; 3] = ["replica-a", "replica-b", "replica-c"];

/// Longer than any step takes on a working transport. A test that waits
/// this long is failing, and the limit only keeps it from hanging.
const GENEROUS_WAIT: Duration = Duration::from_secs(20);

/// Three replicas served over gRPC and the metadata store they are
/// registered in.
struct GrpcCluster {
    /// The metadata store, with [`STREAM`] created on the three replicas.
    metadata: MetadataStore,
    /// The identities of the replicas, in the order of [`REPLICA_NAMES`].
    replica_ids: Vec<ReplicaId>,
    /// The only way the clients of this cluster reach its replicas.
    connector: GrpcReplicaConnector,
}

impl GrpcCluster {
    /// Starts the three replicas, registers them and creates [`STREAM`]
    /// with its first segment placed on them.
    async fn start() -> Self {
        let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
        let connector = GrpcReplicaConnector::new(GrpcConnectorSettings::default());
        let mut replica_ids = Vec::new();
        for replica_name in REPLICA_NAMES {
            let replica_id = ReplicaId::new(replica_name).expect("a valid identifier");
            let registration = serve_a_replica(&replica_id, &connector).await;
            metadata
                .put_replica(&registration)
                .await
                .expect("the replica is registered");
            replica_ids.push(replica_id);
        }
        metadata
            .create_stream(
                STREAM,
                ReplicationSettings::DEFAULT,
                RetentionPolicy::default(),
                Some(replica_ids.clone()),
            )
            .await
            .expect("the stream is created");
        Self {
            metadata,
            replica_ids,
            connector,
        }
    }

    /// A client that reaches the replicas over gRPC and whose writers
    /// identify as `writer_identity`.
    fn client(&self, writer_identity: &str) -> BitacoraClient {
        BitacoraClient::new(
            self.metadata.clone(),
            Arc::new(self.connector.clone()),
            Arc::new(SystemClock),
            ClientSettings {
                writer: WriterSettings {
                    writer_identity: writer_identity.to_owned(),
                    ..WriterSettings::default()
                },
                ..ClientSettings::default()
            },
        )
    }

    /// Opens a writer of [`STREAM`] that takes the stream from whoever
    /// holds it.
    async fn open_writer(&self, writer_identity: &str) -> StreamWriter {
        let client = self.client(writer_identity);
        let opening = client.open_writer(STREAM, TakeoverPolicy::Force);
        timeout(GENEROUS_WAIT, opening)
            .await
            .expect("the writer opens in time")
            .expect("the writer opens")
    }

    /// Follows [`STREAM`] from its first record.
    async fn follow_from_the_start(&self) -> RecordStream {
        self.client("a-reader")
            .open_reader(STREAM)
            .follow_from(SequenceNumber::FIRST)
            .await
            .expect("the stream exists")
    }

    /// Does the controller's job of keeping a standby segment ready, which
    /// the next writer needs to open its segment on.
    async fn place_standby(&self) {
        let standby_replicas = self.replica_ids.clone();
        self.metadata
            .update_stream(STREAM, |stream| {
                stream.standby_replicas = Some(standby_replicas.clone());
                Ok(())
            })
            .await
            .expect("the standby is placed");
    }
}

/// Opens a replica on an empty in-memory disk and serves it over gRPC.
///
/// # Arguments
///
/// * `replica_id` - The identity the replica adopts and is served under.
/// * `connector` - How the replica reaches its peers.
///
/// # Returns
///
/// The replica's registration, with the address it is served at.
async fn serve_a_replica(
    replica_id: &ReplicaId,
    connector: &GrpcReplicaConnector,
) -> ReplicaRecord {
    let disk = MemoryDisk::new();
    let settings = ReplicaSettings {
        preallocated_segment_bytes: 1024,
        ..ReplicaSettings::default()
    };
    let replica = Replica::open(
        Arc::new(disk.clone()),
        replica_id.clone(),
        settings,
        Arc::new(connector.clone()),
        Arc::new(|_segment, _reason| {}),
    )
    .await
    .expect("the replica opens on a working disk");
    let address = serve_on_loopback(Arc::new(replica), replica_id.clone()).await;
    let space = disk
        .space()
        .await
        .expect("a memory disk always reports its space");
    ReplicaRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        replica_id: replica_id.clone(),
        address,
        failure_domains: vec![replica_id.to_string()],
        capacity_bytes: space.capacity_bytes,
        free_bytes: space.free_bytes,
        is_draining: false,
        heartbeat_at: SystemClock.now(),
    }
}

/// A record payload holding `text`.
fn record(text: &str) -> Bytes {
    Bytes::copy_from_slice(text.as_bytes())
}

/// Waits for the next `record_count` committed records of `following` and
/// returns their payloads, in order.
async fn next_payloads(following: &mut RecordStream, record_count: usize) -> Vec<Bytes> {
    let mut payloads = Vec::new();
    while payloads.len() < record_count {
        let committed_record = timeout(GENEROUS_WAIT, following.next())
            .await
            .expect("the record is served in time")
            .expect("the stream goes on")
            .expect("the record is readable");
        payloads.push(committed_record.payload);
    }
    payloads
}

#[tokio::test]
async fn appended_records_are_numbered_from_one() {
    let cluster = GrpcCluster::start().await;
    let writer = cluster.open_writer("compute-1").await;

    let first = writer.append(record("first")).await;
    let second = writer.append(record("second")).await;

    assert_eq!(
        [first, second],
        [Ok(SequenceNumber::new(1)), Ok(SequenceNumber::new(2))]
    );
}

#[tokio::test]
async fn reader_follows_what_the_writer_commits() {
    let cluster = GrpcCluster::start().await;
    let writer = cluster.open_writer("compute-1").await;
    let mut following = cluster.follow_from_the_start().await;

    writer
        .append(record("first"))
        .await
        .expect("the record commits");
    writer
        .append(record("second"))
        .await
        .expect("the record commits");
    let payloads = next_payloads(&mut following, 2).await;

    assert_eq!(payloads, ["first", "second"]);
}

#[tokio::test]
async fn new_writer_continues_the_log_after_the_old_one() {
    let cluster = GrpcCluster::start().await;
    let first_writer = cluster.open_writer("compute-1").await;
    first_writer
        .append(record("from the first writer"))
        .await
        .expect("the record commits");
    cluster.place_standby().await;

    let second_writer = cluster.open_writer("compute-2").await;
    let appended = second_writer.append(record("from the second writer")).await;

    assert_eq!(appended, Ok(SequenceNumber::new(2)));
}

/// The replicas answer the old writer with `Fenced`. Only if that error
/// arrives as `Fenced`, and not as some transport failure, does the writer
/// know that it lost the stream for good.
#[tokio::test]
async fn old_writer_learns_that_it_lost_the_stream() {
    let cluster = GrpcCluster::start().await;
    let first_writer = cluster.open_writer("compute-1").await;
    first_writer
        .append(record("from the first writer"))
        .await
        .expect("the record commits");
    cluster.place_standby().await;
    let _second_writer = cluster.open_writer("compute-2").await;

    let too_late = timeout(GENEROUS_WAIT, first_writer.append(record("too late")))
        .await
        .expect("the append is answered in time");

    assert_eq!(too_late, Err(AppendError::Fenced));
}

#[tokio::test]
async fn reader_follows_the_log_across_a_takeover() {
    let cluster = GrpcCluster::start().await;
    let mut following = cluster.follow_from_the_start().await;
    let first_writer = cluster.open_writer("compute-1").await;
    first_writer
        .append(record("from the first writer"))
        .await
        .expect("the record commits");
    cluster.place_standby().await;
    let second_writer = cluster.open_writer("compute-2").await;
    second_writer
        .append(record("from the second writer"))
        .await
        .expect("the record commits");

    let payloads = next_payloads(&mut following, 2).await;

    assert_eq!(
        payloads,
        ["from the first writer", "from the second writer"]
    );
}
