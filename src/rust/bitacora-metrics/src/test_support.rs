//! What the tests of this crate share: samples, records, a block policy with small numbers, a cluster.
//!
//! # Why this file exists
//!
//! Every test file of the crate needs a label set, a few samples, a log record that holds them. The
//! tests that run a store need a cluster first. Written once here, a test reads as what it checks and
//! not as how its data is built.
//!
//! # What it contains
//!
//! - [`job`], [`sample`], [`samples_at`], [`batch`], [`record`]: test data.
//! - [`SmallBlocks`]: a block policy that cuts a block after eight samples or one second.
//! - [`TestArtifacts`]: an in-memory artifact store with the namespace of one materializer.
//! - [`Harness`]: an in-process cluster with one stream prepared for a metrics store.
//!
//! # How it works
//!
//! The cluster is the one from `bitacora-testkit`; nothing is mocked. Time is Tokio's paused clock. The
//! controller is not running in the background: a test calls [`Harness::run_controller_pass`] when it
//! wants placement or truncation to happen, which keeps every test's sequence of events exact.
//!
//! # See also
//!
//! - `builder_test.rs`, `query_test.rs`, `store_test.rs`, `reader_test.rs`: the tests that use it.

use std::sync::Arc;
use std::time::Duration;

use bitacora_artifact::MemoryArtifactStore;
use bitacora_client::{BitacoraClient, ClientSettings, TakeoverPolicy, WriterSettings};
use bitacora_controller::{Controller, ControllerSettings};
use bitacora_materializer::{ArtifactNamespace, CompactionPolicy, namespace_prefix};
use bitacora_meta::RetentionPolicy;
use bitacora_testkit::Cluster;
use bitacora_types::{LogRecord, ReplicationSettings, SequenceNumber, StreamId};
use bytes::Bytes;

use crate::builder::BlockPolicy;
use crate::materializer::METRICS_MATERIALIZER;
use crate::record::encode_record;
use crate::sample::{Labels, Sample, SeriesSamples};
use crate::store::{MetricsDependencies, MetricsSettings, MetricsStore};

/// The label set `job="<job_name>"`.
pub(crate) fn job(job_name: &str) -> Labels {
    Labels::from_pairs(&[("job", job_name)]).expect("valid labels")
}

/// A sample at `timestamp_millis` with the value `value`.
pub(crate) fn sample(timestamp_millis: i64, value: f64) -> Sample {
    Sample {
        timestamp_millis,
        value,
    }
}

/// One sample per timestamp, each with its timestamp as value.
pub(crate) fn samples_at(timestamps: &[i64]) -> Vec<Sample> {
    let mut samples = Vec::with_capacity(timestamps.len());
    for timestamp_millis in timestamps {
        #[allow(clippy::cast_precision_loss)]
        samples.push(sample(*timestamp_millis, *timestamp_millis as f64));
    }
    samples
}

/// The samples of one record: for the series `job="<job_name>"`, one sample per timestamp, each with its
/// timestamp as value.
pub(crate) fn batch(job_name: &str, timestamps: &[i64]) -> Vec<SeriesSamples> {
    vec![SeriesSamples {
        labels: job(job_name),
        samples: samples_at(timestamps),
    }]
}

/// The series `job="<job_name>"` with the given samples, as a query returns it.
pub(crate) fn series(job_name: &str, samples: Vec<Sample>) -> SeriesSamples {
    SeriesSamples {
        labels: job(job_name),
        samples,
    }
}

/// The payload of a record that holds no series: format version 1 and a series count of 0. This crate's
/// encoder refuses to write it, because appending it changes nothing; a producer in another language may.
pub(crate) const RECORD_WITHOUT_SERIES: [u8; 5] = [1, 0, 0, 0, 0];

/// The log record at `sequence` that holds `batch`. A batch without a sample becomes
/// [`RECORD_WITHOUT_SERIES`].
pub(crate) fn record(sequence: u64, batch: &[SeriesSamples]) -> LogRecord {
    let payload = match encode_record(batch) {
        Ok(payload) => payload,
        Err(_no_samples) => Bytes::from_static(&RECORD_WITHOUT_SERIES),
    };
    LogRecord {
        sequence: SequenceNumber::new(sequence),
        payload,
    }
}

/// A block policy with small numbers: a block is cut after eight samples or one second of them, and two
/// neighbouring blocks are merged. Every block a test makes is in the lowest size tier.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SmallBlocks;

impl BlockPolicy for SmallBlocks {
    const MAX_BLOCK_SAMPLES: u64 = 8;
    const MAX_BLOCK_SPAN_MILLIS: u64 = 1_000;
    const COMPACTION: CompactionPolicy = CompactionPolicy {
        merge_fan_in: 2,
        smallest_tier_bytes: 1024 * 1024,
    };
}

/// A block policy that never cuts a block by itself and never merges: blocks are cut by checkpoints
/// alone, and stay as they were cut.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct UnmergedBlocks;

impl BlockPolicy for UnmergedBlocks {
    const MAX_BLOCK_SAMPLES: u64 = u64::MAX;
    const MAX_BLOCK_SPAN_MILLIS: u64 = u64::MAX;
    const COMPACTION: CompactionPolicy = CompactionPolicy {
        merge_fan_in: usize::MAX,
        smallest_tier_bytes: 1024 * 1024,
    };
}

/// The stream identifier the tests without a cluster store their artifacts under.
pub(crate) const TEST_STREAM_ID: StreamId = StreamId::new(7);

/// An in-memory artifact store with the namespace of a metrics materializer, for tests without a cluster.
pub(crate) struct TestArtifacts {
    /// The store, for damaging artifacts, counting reads and making the store fail.
    pub(crate) store: MemoryArtifactStore,
    /// The namespace, as the runner that holds fencing token 1 sees it.
    pub(crate) namespace: ArtifactNamespace,
}

impl TestArtifacts {
    /// An empty store and a namespace that can write to it.
    pub(crate) fn new() -> Self {
        let store = MemoryArtifactStore::new();
        let namespace = ArtifactNamespace::for_runner(Arc::new(store.clone()), TEST_STREAM_ID, METRICS_MATERIALIZER, 1);
        Self { store, namespace }
    }

    /// The key an artifact of the namespace has in the store.
    pub(crate) fn object_key(artifact_key: &str) -> String {
        format!(
            "{}{artifact_key}",
            namespace_prefix(TEST_STREAM_ID, METRICS_MATERIALIZER)
        )
    }

    /// How many range reads the store has served for the artifacts of the namespace.
    pub(crate) fn range_reads(&self) -> u64 {
        self.store
            .served_range_reads(&namespace_prefix(TEST_STREAM_ID, METRICS_MATERIALIZER))
    }
}

/// The stream the stores under test live on.
pub(crate) const STREAM: &str = "tenant-7";

/// How long a test waits for something that must happen. Simulated time.
pub(crate) const PATIENCE: Duration = Duration::from_secs(300);

/// How often the stores under test checkpoint, unless a test says otherwise.
pub(crate) const CHECKPOINT_AFTER: Duration = Duration::from_secs(10);

/// An in-process cluster with [`STREAM`] prepared for a metrics store.
pub(crate) struct Harness {
    /// The replicas and the metadata store.
    pub(crate) cluster: Cluster,
    /// Where the stores keep their blocks.
    pub(crate) artifact_store: MemoryArtifactStore,
}

impl Harness {
    /// Starts three replicas, creates the stream, places its first segment and prepares it for a store.
    pub(crate) async fn start() -> Self {
        let cluster = Cluster::start(3).await;
        cluster
            .metadata()
            .create_stream(STREAM, ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
            .await
            .expect("a fresh metadata store accepts a stream");
        MetricsStore::initialize(cluster.metadata(), STREAM)
            .await
            .expect("the stream exists");
        let harness = Self {
            cluster,
            artifact_store: MemoryArtifactStore::new(),
        };
        harness.run_controller_pass().await;
        harness
    }

    /// What a store whose writer uses `writer_settings` is built from.
    pub(crate) fn dependencies(&self, writer_settings: WriterSettings) -> MetricsDependencies {
        let client = BitacoraClient::new(
            self.cluster.metadata().clone(),
            self.cluster.connector(),
            self.cluster.clock(),
            ClientSettings {
                writer: writer_settings,
                ..ClientSettings::default()
            },
        );
        MetricsDependencies {
            client,
            metadata: self.cluster.metadata().clone(),
            artifact_store: Arc::new(self.artifact_store.clone()),
        }
    }

    /// Opens a store with the block policy `P` that takes the stream at once and checkpoints every
    /// `checkpoint_after`. The controller runs first: a writer opens its segment on the standby
    /// replicas, and the store opened before used the last ones up.
    pub(crate) async fn open_with<P: BlockPolicy>(
        &self,
        writer_settings: WriterSettings,
        checkpoint_after: Duration,
    ) -> MetricsStore<P> {
        self.run_controller_pass().await;
        let settings = MetricsSettings {
            takeover: TakeoverPolicy::Force,
            checkpoint_after,
            ..MetricsSettings::default()
        };
        MetricsStore::open(self.dependencies(writer_settings), STREAM, settings)
            .await
            .expect("the store opens")
    }

    /// Opens a store of small blocks that checkpoints every [`CHECKPOINT_AFTER`].
    pub(crate) async fn open(&self) -> MetricsStore<SmallBlocks> {
        self.open_with(WriterSettings::default(), CHECKPOINT_AFTER).await
    }

    /// Runs one pass of the controller: placement of the standby segment, reconciliation, truncation.
    ///
    /// The replicas renew their registrations first, as they do on a timer outside tests: a test that
    /// moves the clock far would otherwise find them too long silent to be given a segment.
    pub(crate) async fn run_controller_pass(&self) {
        self.cluster
            .renew_registrations()
            .await
            .expect("the metadata store is available");
        Controller::new(
            self.cluster.metadata().clone(),
            self.cluster.connector(),
            self.cluster.clock(),
            ControllerSettings::default(),
        )
        .run_pass()
        .await
        .expect("the metadata store is available");
    }

    /// The identifier of [`STREAM`].
    pub(crate) async fn stream_id(&self) -> StreamId {
        self.cluster
            .metadata()
            .get_stream(STREAM)
            .await
            .expect("the stream exists")
            .stream_id
    }

    /// The first sequence number the log still holds.
    pub(crate) async fn low_water_mark(&self) -> SequenceNumber {
        self.cluster
            .metadata()
            .get_stream(STREAM)
            .await
            .expect("the stream exists")
            .low_water_mark
    }
}
