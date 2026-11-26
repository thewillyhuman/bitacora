//! What the client's integration tests share: an in-process cluster with a stream on it, and the steps most tests are
//! made of.
//!
//! # Why this file exists
//!
//! Every test of the client needs replicas, a metadata store and a stream to write to. Most also need a writer, a few
//! numbered records, and a short way to say which records a reader returned. `bitacora-testkit` supplies the cluster:
//! real replicas on in-memory disks, an in-memory metadata store, and links that a test can cut. This file adds what is
//! specific to testing the client, once, so that each test reads as its scenario and nothing else.
//!
//! # What it contains
//!
//! - [`Cluster`]: the test kit's cluster with the stream [`STREAM`] created on it, clients, and the part of the
//!   controller's job the tests do by hand.
//! - [`open_writer`], [`append_numbered`]: a writer and records whose payloads name their sequence numbers.
//! - [`range`], [`sequences`], [`take`]: asking for records and naming the ones that came back.
//! - [`settle`], [`HEARTBEAT_SETTLE`]: letting virtual time pass.
//!
//! # How it works
//!
//! Every test runs on a paused clock (`#[tokio::test(start_paused = true)]`). Time then moves only when every task is
//! waiting, and jumps straight to the next timer, so a test that waits ten seconds for a failure timeout takes no real
//! time and always sees the same order of events.
//!
//! No controller runs in these tests. Where the protocol needs one, to place the standby segment that a roll or a
//! takeover opens, the test calls [`Cluster::place_standby`] itself, which also lets it choose when.

#![allow(
    dead_code,
    reason = "each test binary compiles this module and uses its own part of it"
)]

use std::ops::Deref;
use std::time::Duration;

use bitacora_client::{BitacoraClient, ClientSettings, RecordStream, StreamWriter, TakeoverPolicy, WriterSettings};
use bitacora_meta::{RetentionPolicy, StreamRecord};
use bitacora_types::{LogRecord, ReplicationSettings, SegmentAddress, SegmentId, SequenceNumber, SequenceRange};
use bytes::Bytes;

/// The stream every test writes to.
pub const STREAM: &str = "orders";

/// Long enough for the writer's heartbeats to tell every replica what is durable.
pub const HEARTBEAT_SETTLE: Duration = Duration::from_millis(200);

/// An in-process cluster with [`STREAM`] created on it.
///
/// Everything the test kit's cluster offers (nodes, metadata store, object store) is reachable through it.
pub struct Cluster {
    /// The test kit's cluster.
    cluster: bitacora_testkit::Cluster,
}

impl Deref for Cluster {
    type Target = bitacora_testkit::Cluster;

    /// Gives access to the test kit's cluster.
    fn deref(&self) -> &Self::Target {
        &self.cluster
    }
}

impl Cluster {
    /// Starts `node_count` replicas and creates [`STREAM`] with its first segment placed on the first three.
    pub async fn start(node_count: usize) -> Self {
        let cluster = bitacora_testkit::Cluster::start(node_count).await;
        cluster
            .metadata()
            .create_stream(
                STREAM,
                ReplicationSettings::DEFAULT,
                RetentionPolicy::default(),
                Some(cluster.replica_ids(&[0, 1, 2])),
            )
            .await
            .expect("the name is free on a new cluster");
        Self { cluster }
    }

    /// A client whose writers identify as `writer_identity` and use `settings` for everything else.
    pub fn client_with(&self, writer_identity: &str, settings: &WriterSettings) -> BitacoraClient {
        BitacoraClient::new(
            self.metadata().clone(),
            self.connector(),
            self.clock(),
            ClientSettings {
                writer: WriterSettings {
                    writer_identity: writer_identity.to_owned(),
                    ..settings.clone()
                },
                ..ClientSettings::default()
            },
        )
    }

    /// A client with the default settings whose writers identify as `writer_identity`.
    pub fn client(&self, writer_identity: &str) -> BitacoraClient {
        self.client_with(writer_identity, &WriterSettings::default())
    }

    /// Does the controller's job of keeping a standby segment ready: places it on the nodes `node_numbers`.
    pub async fn place_standby(&self, node_numbers: &[usize]) {
        let standby_replicas = self.replica_ids(node_numbers);
        self.metadata()
            .update_stream(STREAM, |stream| {
                stream.standby_replicas = Some(standby_replicas.clone());
                Ok(())
            })
            .await
            .expect("the stream exists and the store is up");
    }

    /// The record of [`STREAM`] as the metadata store holds it now.
    pub async fn stream(&self) -> StreamRecord {
        self.metadata()
            .get_stream(STREAM)
            .await
            .expect("the stream exists and the store is up")
    }

    /// The address of segment `segment_id` of [`STREAM`].
    pub async fn segment_address(&self, segment_id: u64) -> SegmentAddress {
        SegmentAddress::new(self.stream().await.stream_id, SegmentId::new(segment_id))
    }
}

/// Lets every task that is ready run, and the timers up to `duration` from now fire.
pub async fn settle(duration: Duration) {
    tokio::time::sleep(duration).await;
}

/// Opens a writer of [`STREAM`] that identifies as `writer_identity`, taking the stream over at once.
pub async fn open_writer(cluster: &Cluster, writer_identity: &str) -> StreamWriter {
    cluster
        .client(writer_identity)
        .open_writer(STREAM, TakeoverPolicy::Force)
        .await
        .expect("the stream has a recovery quorum and a segment to open")
}

/// Appends `count` records, one at a time, each with a payload that names its own sequence number: `record-7` for the
/// seventh record of the stream.
pub async fn append_numbered(writer: &StreamWriter, count: u64) {
    let first = writer.durable_sequence().get() + 1;
    for sequence in first..first + count {
        writer
            .append(Bytes::from(format!("record-{sequence}")))
            .await
            .expect("a write quorum is reachable");
    }
}

/// The range of sequence numbers from `first` to `last`, both included.
pub fn range(first: u64, last: u64) -> SequenceRange {
    SequenceRange::new(SequenceNumber::new(first), SequenceNumber::new(last)).expect("first is not after last")
}

/// The sequence numbers of `records`, in the order given.
pub fn sequences(records: &[LogRecord]) -> Vec<u64> {
    let mut sequence_numbers = Vec::with_capacity(records.len());
    for record in records {
        sequence_numbers.push(record.sequence.get());
    }
    sequence_numbers
}

/// Takes the next `count` records of a followed stream and returns their sequence numbers.
pub async fn take(followed: &mut RecordStream, count: usize) -> Vec<u64> {
    let mut taken = Vec::with_capacity(count);
    while taken.len() < count {
        let record = followed
            .next()
            .await
            .expect("a followed stream does not end")
            .expect("the follower's position was not truncated");
        taken.push(record.sequence.get());
    }
    taken
}
