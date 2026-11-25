//! What the tests of this crate share: a cluster with a stream to materialize.
//!
//! # Why this file exists
//!
//! The runner and both adapters are tested against the real thing: replicas on in-memory disks, the
//! client library, the controller, an in-memory artifact store. Every such test needs the same few steps
//! first, and the same way to write records and to cut segments. They live here once.
//!
//! # What it contains
//!
//! - [`Harness`]: the in-process cluster, the artifact store, and the stream [`STREAM`] with the
//!   materializer [`MATERIALIZER`] registered on it.
//! - [`numbered_payload`]: the payload the helpers append, so that tests can say what a record holds.
//!
//! # How it works
//!
//! The cluster is the one from `bitacora-testkit`; nothing is mocked. Time is Tokio's paused clock, so a
//! test that waits a minute for a checkpoint takes no real time. The controller is not running in the
//! background: a test calls [`Harness::run_controller_pass`] when it wants placement or truncation to
//! happen, which keeps every test's sequence of events exact.
//!
//! # See also
//!
//! - `runner_test.rs`, `page_store_test.rs`, `artifact_catalog_test.rs`: the tests that use it.

use std::sync::Arc;
use std::time::Duration;

use bitacora_artifact::MemoryArtifactStore;
use bitacora_client::{
    BitacoraClient, ClientSettings, StreamWriter, TakeoverPolicy, WriterSettings,
};
use bitacora_controller::{Controller, ControllerSettings, PassReport};
use bitacora_meta::{CheckpointRecord, RetentionPolicy, StreamRecord};
use bitacora_testkit::Cluster;
use bitacora_types::{ReplicationSettings, SequenceNumber};
use bytes::Bytes;

use crate::runner::RunnerDependencies;

/// The stream every test materializes.
pub(crate) const STREAM: &str = "orders";

/// The name the materializer under test is registered with.
pub(crate) const MATERIALIZER: &str = "test-materializer";

/// The payload of the `record_number`-th record a test appends.
pub(crate) fn numbered_payload(record_number: u64) -> Bytes {
    Bytes::from(format!("record-{record_number}"))
}

/// An in-process cluster of three replicas with [`STREAM`] created and [`MATERIALIZER`] registered.
pub(crate) struct Harness {
    /// The replicas and the metadata store.
    pub(crate) cluster: Cluster,
    /// The artifact store the materializer under test writes to.
    pub(crate) artifact_store: MemoryArtifactStore,
}

impl Harness {
    /// Starts the cluster, creates the stream with a standby segment placed, and registers the
    /// materializer at the start of the stream.
    pub(crate) async fn start() -> Self {
        let cluster = Cluster::start(3).await;
        cluster
            .metadata()
            .create_stream(
                STREAM,
                ReplicationSettings::DEFAULT,
                RetentionPolicy::default(),
                None,
            )
            .await
            .expect("a fresh metadata store accepts a stream");
        cluster
            .metadata()
            .register_materializer(STREAM, MATERIALIZER)
            .await
            .expect("a fresh stream accepts a materializer");
        let harness = Self {
            cluster,
            artifact_store: MemoryArtifactStore::new(),
        };
        harness.run_controller_pass().await;
        harness
    }

    /// A client whose writers use `writer_settings`.
    pub(crate) fn client_with(&self, writer_settings: WriterSettings) -> BitacoraClient {
        BitacoraClient::new(
            self.cluster.metadata().clone(),
            self.cluster.connector(),
            self.cluster.clock(),
            ClientSettings {
                writer: writer_settings,
                ..ClientSettings::default()
            },
        )
    }

    /// What a runner of the materializer under test is built from.
    pub(crate) fn dependencies(&self) -> RunnerDependencies {
        RunnerDependencies {
            client: self.client_with(WriterSettings::default()),
            metadata: self.cluster.metadata().clone(),
            artifact_store: Arc::new(self.artifact_store.clone()),
        }
    }

    /// Becomes the writer of the stream, with default settings.
    pub(crate) async fn open_writer(&self) -> StreamWriter {
        self.client_with(WriterSettings::default())
            .open_writer(STREAM, TakeoverPolicy::Force)
            .await
            .expect("the writer opens")
    }

    /// Becomes the writer of the stream with segments so small that every record of
    /// [`Harness::append_one_segment_each`] fills one.
    pub(crate) async fn open_writer_of_small_segments(&self) -> StreamWriter {
        let one_record_per_segment = WriterSettings {
            target_segment_bytes: 1024,
            // Only size rolls the segment, however far a test moves the clock.
            target_segment_age: Duration::from_hours(100_000),
            ..WriterSettings::default()
        };
        self.client_with(one_record_per_segment)
            .open_writer(STREAM, TakeoverPolicy::Force)
            .await
            .expect("the writer opens")
    }

    /// Appends `record_count` records of two kilobytes each through a writer of small segments, so that
    /// each one ends up alone in a sealed segment. Record `n` of the stream is then segment `n`.
    pub(crate) async fn append_one_segment_each(&self, writer: &StreamWriter, record_count: u64) {
        for _record in 0..record_count {
            self.run_controller_pass().await;
            writer
                .append(Bytes::from(vec![0xAB; 2048]))
                .await
                .expect("the record commits");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Runs one pass of the controller: placement of the standby segment, reconciliation, truncation.
    pub(crate) async fn run_controller_pass(&self) -> PassReport {
        Controller::new(
            self.cluster.metadata().clone(),
            self.cluster.connector(),
            self.cluster.clock(),
            ControllerSettings::default(),
        )
        .run_pass()
        .await
        .expect("the metadata store is available")
    }

    /// The stream's metadata as it is now.
    pub(crate) async fn stream(&self) -> StreamRecord {
        self.cluster
            .metadata()
            .get_stream(STREAM)
            .await
            .expect("the stream exists")
    }

    /// The committed checkpoint of the materializer under test.
    pub(crate) async fn committed_checkpoint(&self) -> CheckpointRecord {
        let stream_id = self.stream().await.stream_id;
        self.cluster
            .metadata()
            .get_checkpoint(stream_id, MATERIALIZER)
            .await
            .expect("the materializer is registered")
    }

    /// How many objects the artifact store holds, whoever wrote them.
    pub(crate) async fn stored_object_count(&self) -> usize {
        use bitacora_artifact::ArtifactStore as _;
        self.artifact_store
            .list_prefix("")
            .await
            .expect("the artifact store is available")
            .len()
    }
}

/// Appends `record_count` records whose payloads are [`numbered_payload`] of 1, 2, 3 and so on, counted
/// from the end of what `writer` has committed.
pub(crate) async fn append_numbered(writer: &StreamWriter, record_count: u64) -> SequenceNumber {
    let first = writer.durable_sequence().get() + 1;
    let mut last_sequence = writer.durable_sequence();
    for record_number in first..first + record_count {
        last_sequence = writer
            .append(numbered_payload(record_number))
            .await
            .expect("the record commits");
    }
    last_sequence
}
