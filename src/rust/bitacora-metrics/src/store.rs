//! The front end of the metrics store: append samples, query them.
//!
//! # Why this file exists
//!
//! Everything else in the crate is a part: records, chunks, blocks, a head, a builder. This file is the
//! whole. It is the "compute node" of the design: it owns the writer of one stream and a materialized
//! view of it, and turns two calls, append and query, into the right use of both.
//!
//! # What it contains
//!
//! - [`MetricsStore`]: the store. `append`, `query`, and what it holds.
//! - [`QueryPosition`]: how current an answer a query asks for.
//! - [`MetricsDependencies`], [`MetricsSettings`]: what a store is built from.
//! - [`MetricsError`]: what can go wrong, split by what the caller may conclude.
//!
//! # How it works
//!
//! A store is a `StreamWriter` and a `MaterializerRunner` on the same stream.
//!
//! **Appending.** A batch of samples becomes one record (`record.rs`) and is appended. The call returns
//! the record's sequence number once the log has committed it. The samples are not in the store's view
//! yet: the materializer applies committed records a moment later.
//!
//! **Querying.** A query is answered from the blocks and the head (`query.rs`). The answer says which
//! record it is exact up to. A caller that has just appended and wants to see its own samples passes the
//! sequence number it got back as [`QueryPosition::AtLeast`]: the query then first waits until the
//! materializer has applied that record, up to [`MetricsSettings::query_wait`], and gives no answer
//! rather than one that lacks the samples. [`QueryPosition::Applied`] takes whatever is applied now.
//!
//! There is no read that is "linearizable" in the sense of bitacora-kv's `get`: a metrics store has many
//! producers and no notion of a latest value that a reader could be wrong about. Read-your-writes by
//! sequence number is the guarantee a dashboard or an alert rule needs.
//!
//! # Invariants
//!
//! - A sample reported as appended is in the log once, at the returned sequence number.
//! - An answer with `as_of` = N holds exactly what the records 1 to N make of the selected series.
//! - The store answers only from records the log has committed.
//!
//! # See also
//!
//! - `query.rs`: how an answer is put together.
//! - `materializer.rs`: the materialized view.
//! - `reader.rs`: querying committed blocks from a process that is not the store.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use bitacora_artifact::ArtifactStore;
use bitacora_client::{AppendError, BitacoraClient, OpenError, StreamWriter, TakeoverPolicy};
use bitacora_materializer::{
    ArtifactNamespace, CatalogEntry, CatalogView, MaterializerError, MaterializerRunner, RunnerDependencies,
    RunnerError, RunnerProgress, RunnerSettings,
};
use bitacora_meta::{MetaError, MetadataStore};
use bitacora_types::SequenceNumber;
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::builder::{BlockPolicy, StandardBlocks};
use crate::head::SharedHead;
use crate::materializer::{METRICS_MATERIALIZER, MetricsMaterializer};
use crate::query::{BlockIndexCache, QueryFailure, QueryResult, query_head_and_blocks};
use crate::record::{InvalidBatch, encode_record};
use crate::sample::{SeriesSamples, TimeRange};
use crate::selector::Selector;

/// How long a query waits for the materializer to apply the record it asked to see, unless the settings say
/// otherwise.
const DEFAULT_QUERY_WAIT: Duration = Duration::from_secs(10);

/// What a store needs to reach the cluster.
///
/// The example of [`MetricsStore`] builds one for a cluster that runs inside the process.
#[derive(Clone)]
pub struct MetricsDependencies {
    /// The client the store writes and follows the stream with.
    pub client: BitacoraClient,
    /// The metadata store, for the materializer's checkpoint.
    pub metadata: MetadataStore,
    /// Where the materializer keeps its blocks.
    pub artifact_store: Arc<dyn ArtifactStore>,
}

/// How a store takes its stream over, paces its checkpoints and bounds its queries.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use bitacora_metrics::MetricsSettings;
///
/// // A store whose queries give up sooner than the default ten seconds.
/// let settings = MetricsSettings {
///     query_wait: Duration::from_secs(2),
///     ..MetricsSettings::default()
/// };
/// assert_eq!(settings.checkpoint_after, Duration::from_secs(60));
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricsSettings {
    /// What to do if another store still holds the writer's lease.
    pub takeover: TakeoverPolicy,
    /// A checkpoint is written once this many bytes of records were applied since the last one.
    pub checkpoint_after_bytes: u64,
    /// A checkpoint is written once this much time has passed since the last one.
    pub checkpoint_after: Duration,
    /// How long the materializer waits before it tries a failed checkpoint again.
    pub retry_interval: Duration,
    /// How long a query waits for the materializer to apply the record it asked to see.
    pub query_wait: Duration,
}

impl MetricsSettings {
    /// The settings of the runner that runs this store's materializer: the store's pacing under the store's name.
    fn runner_settings(&self) -> RunnerSettings {
        RunnerSettings {
            checkpoint_after_bytes: self.checkpoint_after_bytes,
            checkpoint_after: self.checkpoint_after,
            retry_interval: self.retry_interval,
            ..RunnerSettings::for_materializer(METRICS_MATERIALIZER)
        }
    }
}

/// Opens, for reading, the place in the object store where the materializer of `stream_name` keeps its blocks.
///
/// A stream that cannot be looked up is reported as a materializer that cannot start, which is what it means for
/// the store.
async fn blocks_of_stream(
    dependencies: &MetricsDependencies,
    stream_name: &str,
) -> Result<ArtifactNamespace, MetricsError> {
    let stream = match dependencies.metadata.get_stream(stream_name).await {
        Ok(stream) => stream,
        Err(meta_error) => return Err(MetricsError::Materializer(RunnerError::Metadata(meta_error))),
    };
    Ok(ArtifactNamespace::read_only(
        Arc::clone(&dependencies.artifact_store),
        stream.stream_id,
        METRICS_MATERIALIZER,
    ))
}

impl Default for MetricsSettings {
    /// Waits for a live writer's lease to lapse, checkpoints every 64 MiB or 60 seconds, and gives a
    /// query ten seconds to see the record it asked for.
    fn default() -> Self {
        let runner = RunnerSettings::for_materializer(METRICS_MATERIALIZER);
        Self {
            takeover: TakeoverPolicy::WaitForLeaseExpiry,
            checkpoint_after_bytes: runner.checkpoint_after_bytes,
            checkpoint_after: runner.checkpoint_after,
            retry_interval: runner.retry_interval,
            query_wait: DEFAULT_QUERY_WAIT,
        }
    }
}

/// How current an answer a query asks for.
///
/// # Examples
///
/// ```
/// use bitacora_metrics::QueryPosition;
/// use bitacora_types::SequenceNumber;
///
/// // What a dashboard asks for, and what a client that has just appended record 42 asks for.
/// let for_a_dashboard = QueryPosition::Applied;
/// let to_read_my_own_samples = QueryPosition::AtLeast(SequenceNumber::new(42));
/// assert_ne!(for_a_dashboard, to_read_my_own_samples);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryPosition {
    /// Whatever the materializer has applied when the query runs. Never waits.
    Applied,
    /// At least the record at this sequence number: the query waits until the materializer has applied
    /// it. Pass what [`MetricsStore::append`] returned to read your own samples.
    AtLeast(SequenceNumber),
}

/// Why an operation on a store failed, split by what the caller may conclude from it.
///
/// # Examples
///
/// ```
/// use bitacora_client::AppendError;
/// use bitacora_metrics::MetricsError;
///
/// /// Whether the samples may be in the log. Appending them again is safe either way.
/// fn may_be_appended(append_error: &MetricsError) -> bool {
///     matches!(append_error, MetricsError::OutcomeUnknown { .. })
/// }
///
/// assert!(may_be_appended(&MetricsError::OutcomeUnknown { reason: AppendError::Fenced }));
/// assert!(!may_be_appended(&MetricsError::NotWritten { reason: AppendError::Closed }));
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum MetricsError {
    /// The store could not become the writer of the stream. Nothing changed.
    #[error("the store could not become the writer: {0}")]
    Open(#[from] OpenError),
    /// The store's materializer could not start, or has stopped; the reason says which. A store whose
    /// materializer stopped can no longer see new samples: open a new one.
    #[error("the store's materializer is not running: {0}")]
    Materializer(#[from] RunnerError),
    /// The batch cannot be a record. Nothing was appended.
    #[error("the samples cannot be appended: {0}")]
    InvalidBatch(#[from] InvalidBatch),
    /// The record was refused before it reached the log. It did not and will not take effect.
    #[error("the samples were not appended: {reason}")]
    NotWritten {
        /// Why the log refused the record.
        reason: AppendError,
    },
    /// The store lost contact or was replaced before it learned whether the record was committed. The
    /// samples may be in the log, may still get there, or never will. Appending them again is safe: a
    /// sample for a series and timestamp that already exists replaces it with the same value.
    #[error("the outcome of the append is unknown: {reason}")]
    OutcomeUnknown {
        /// What interrupted the append.
        reason: AppendError,
    },
    /// The materializer did not apply the record the query asked to see in time.
    #[error("record {sequence} was not applied within {waited:?}")]
    NotApplied {
        /// The record the query asked to see.
        sequence: SequenceNumber,
        /// How long the query waited.
        waited: Duration,
    },
    /// A block could not be read, or is damaged.
    #[error("a block could not be read: {0}")]
    Block(#[from] MaterializerError),
    /// The catalog of blocks changed under the query at every attempt. Ask again.
    #[error("the catalog of blocks kept changing while the query was reading it")]
    CatalogKeptChanging,
}

impl From<QueryFailure> for MetricsError {
    /// A query's failure as the store reports it.
    fn from(failure: QueryFailure) -> Self {
        match failure {
            QueryFailure::Block(block_error) => Self::Block(block_error),
            QueryFailure::CatalogKeptChanging => Self::CatalogKeptChanging,
        }
    }
}

/// A time-series store on one stream: the writer of the stream and a materialized view of it.
///
/// Dropping the store stops its materializer and its writer at once, like a crash. The next store opened
/// on the stream recovers whatever was in flight.
///
/// The type parameter is the [`BlockPolicy`]: when blocks are cut and merged. Leave it out for
/// [`StandardBlocks`].
///
/// # Examples
///
/// A store on the stream `tenant-7` of a cluster that runs inside this process:
///
/// ```
/// # use std::sync::Arc;
/// # use bitacora_artifact::MemoryArtifactStore;
/// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions};
/// # use bitacora_metrics::{MetricsDependencies, MetricsSettings, MetricsStore};
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let cluster = bitacora_testkit::Cluster::start(3).await;
/// # let client = BitacoraClient::new(
/// #     cluster.metadata().clone(),
/// #     cluster.connector(),
/// #     cluster.clock(),
/// #     ClientSettings::default(),
/// # );
/// # client.admin().create_stream("tenant-7", StreamOptions::default()).await?;
/// # let dependencies = MetricsDependencies {
/// #     client,
/// #     metadata: cluster.metadata().clone(),
/// #     artifact_store: Arc::new(MemoryArtifactStore::new()),
/// # };
/// use bitacora_metrics::{Labels, Matcher, QueryPosition, Sample, Selector, SeriesSamples, TimeRange};
///
/// MetricsStore::initialize(&dependencies.metadata, "tenant-7").await?;
/// let store: MetricsStore = MetricsStore::open(dependencies, "tenant-7", MetricsSettings::default()).await?;
///
/// let requests = SeriesSamples {
///     labels: Labels::from_pairs(&[("__name__", "requests_total"), ("job", "api")])?,
///     samples: vec![Sample { timestamp_millis: 1_700_000_000_000, value: 12.0 }],
/// };
/// let appended_at = store.append(&[requests]).await?;
///
/// let from_the_api = Selector::new(vec![Matcher::equal("job", "api")]);
/// let answer = store.query(&from_the_api, TimeRange::ALL, QueryPosition::AtLeast(appended_at)).await?;
/// assert_eq!(answer.series.len(), 1);
/// # Ok(())
/// # }
/// ```
pub struct MetricsStore<Policy: BlockPolicy = StandardBlocks> {
    /// The writer of the stream.
    writer: StreamWriter,
    /// The samples the materializer has applied and not yet sealed.
    head: Arc<SharedHead>,
    /// The blocks, as the materializer publishes them.
    catalog: CatalogView,
    /// Where the blocks are read from.
    artifacts: ArtifactNamespace,
    /// The indexes of the blocks in use.
    block_indexes: BlockIndexCache,
    /// Where the materializer is.
    progress: RunnerProgress,
    /// The task that runs the materializer. Aborted when the store is dropped.
    runner_task: JoinHandle<()>,
    /// How long a query waits for the record it asked to see.
    query_wait: Duration,
    /// The policy is a type; nothing of it is stored.
    policy: PhantomData<fn() -> Policy>,
}

impl MetricsStore<StandardBlocks> {
    /// Prepares a stream for a metrics store, once: registers the store's materializer on it, so that
    /// the log is not truncated beyond what the store has put into blocks.
    ///
    /// The step is the same whatever the block policy of the stores that will open the stream, which
    /// is why it is not generic. Doing it again changes nothing.
    ///
    /// It is a separate step from [`MetricsStore::open`] on purpose: a store that registered itself
    /// whenever it found itself unregistered would come back empty, and silent, after an operator
    /// removed it and the log moved on.
    ///
    /// # Arguments
    ///
    /// * `metadata` - The metadata store.
    /// * `stream_name` - The stream the store will live on. It must exist.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::StreamNotFound`] if the stream does not exist, or [`MetaError::Unavailable`]
    /// if the metadata store cannot be reached.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use bitacora_artifact::MemoryArtifactStore;
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions};
    /// # use bitacora_metrics::{MetricsDependencies, MetricsSettings, MetricsStore};
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// # client.admin().create_stream("tenant-7", StreamOptions::default()).await?;
    /// # let dependencies = MetricsDependencies {
    /// #     client,
    /// #     metadata: cluster.metadata().clone(),
    /// #     artifact_store: Arc::new(MemoryArtifactStore::new()),
    /// # };
    /// MetricsStore::initialize(&dependencies.metadata, "tenant-7").await?;
    /// // Preparing a stream twice changes nothing.
    /// MetricsStore::initialize(&dependencies.metadata, "tenant-7").await?;
    ///
    /// let stream = dependencies.metadata.get_stream("tenant-7").await?;
    /// assert!(stream.materializers.contains(bitacora_metrics::METRICS_MATERIALIZER));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn initialize(metadata: &MetadataStore, stream_name: &str) -> Result<(), MetaError> {
        metadata
            .register_materializer(stream_name, METRICS_MATERIALIZER)
            .await?;
        Ok(())
    }
}

impl<Policy: BlockPolicy> MetricsStore<Policy> {
    /// Opens the store on `stream_name`: becomes the writer of the stream, restores the materializer
    /// from its last checkpoint and starts it.
    ///
    /// Any other store open on the stream is replaced: its appends fail and its materializer can no
    /// longer checkpoint. The new store catches up with the log in the background.
    ///
    /// # Arguments
    ///
    /// * `dependencies` - How to reach the cluster.
    /// * `stream_name` - The stream the store lives on, prepared with [`MetricsStore::initialize`].
    /// * `settings` - Takeover, checkpoint pacing and how long queries wait.
    ///
    /// # Returns
    ///
    /// The store, with its materializer running in a task of its own. It is the writer of the stream.
    ///
    /// # Errors
    ///
    /// Returns [`MetricsError::Open`] if the store could not become the writer; nothing changed then.
    /// Returns [`MetricsError::Materializer`] if the materializer could not start, for example because
    /// [`MetricsStore::initialize`] was never run; the stream is then left without a writer until a
    /// store is opened on it.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use bitacora_artifact::MemoryArtifactStore;
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions};
    /// # use bitacora_metrics::{MetricsDependencies, MetricsSettings, MetricsStore};
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// # client.admin().create_stream("tenant-7", StreamOptions::default()).await?;
    /// # let dependencies = MetricsDependencies {
    /// #     client,
    /// #     metadata: cluster.metadata().clone(),
    /// #     artifact_store: Arc::new(MemoryArtifactStore::new()),
    /// # };
    /// MetricsStore::initialize(&dependencies.metadata, "tenant-7").await?;
    /// let store: MetricsStore = MetricsStore::open(dependencies, "tenant-7", MetricsSettings::default()).await?;
    ///
    /// // Nothing was appended yet, so there is no block.
    /// assert!(store.blocks().is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn open(
        dependencies: MetricsDependencies,
        stream_name: &str,
        settings: MetricsSettings,
    ) -> Result<Self, MetricsError> {
        let writer = dependencies.client.open_writer(stream_name, settings.takeover).await?;
        let artifacts = blocks_of_stream(&dependencies, stream_name).await?;
        let runner_settings = settings.runner_settings();
        let runner_dependencies = RunnerDependencies {
            client: dependencies.client,
            metadata: dependencies.metadata,
            artifact_store: dependencies.artifact_store,
        };
        let runner =
            MaterializerRunner::<MetricsMaterializer<Policy>>::start(runner_dependencies, stream_name, runner_settings)
                .await?;
        let head = runner.materializer().builder().head();
        let catalog = runner.materializer().view();
        let progress = runner.progress();
        let runner_task = tokio::spawn(async move {
            let reason = runner.run().await;
            tracing::warn!(%reason, "the metrics store's materializer stopped");
        });
        Ok(Self {
            writer,
            head,
            catalog,
            artifacts,
            block_indexes: BlockIndexCache::default(),
            progress,
            runner_task,
            query_wait: settings.query_wait,
            policy: PhantomData,
        })
    }

    /// Appends a batch of samples as one record and waits until the log has committed it.
    ///
    /// The samples of one batch are applied together: a query sees all of them or none.
    ///
    /// # Arguments
    ///
    /// * `batch` - The samples, by series, in any order. A sample for a series and timestamp that already
    ///   has one replaces it.
    ///
    /// # Returns
    ///
    /// The sequence number of the record. Pass it to [`MetricsStore::query`] as
    /// [`QueryPosition::AtLeast`] to see the samples.
    ///
    /// # Errors
    ///
    /// Returns [`MetricsError::InvalidBatch`] if the batch is empty or too large for one record,
    /// [`MetricsError::NotWritten`] if the log refused the record, and [`MetricsError::OutcomeUnknown`]
    /// if the store cannot tell whether the record was committed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use bitacora_artifact::MemoryArtifactStore;
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions};
    /// # use bitacora_metrics::{MetricsDependencies, MetricsSettings, MetricsStore};
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// # client.admin().create_stream("tenant-7", StreamOptions::default()).await?;
    /// # let dependencies = MetricsDependencies {
    /// #     client,
    /// #     metadata: cluster.metadata().clone(),
    /// #     artifact_store: Arc::new(MemoryArtifactStore::new()),
    /// # };
    /// # MetricsStore::initialize(&dependencies.metadata, "tenant-7").await?;
    /// # let store: MetricsStore = MetricsStore::open(dependencies, "tenant-7", MetricsSettings::default()).await?;
    /// use bitacora_metrics::{Labels, Sample, SeriesSamples};
    ///
    /// let requests = SeriesSamples {
    ///     labels: Labels::from_pairs(&[("__name__", "requests_total"), ("job", "api")])?,
    ///     samples: vec![Sample { timestamp_millis: 1_700_000_000_000, value: 12.0 }],
    /// };
    /// let first_append = store.append(&[requests.clone()]).await?;
    /// let second_append = store.append(&[requests]).await?;
    ///
    /// // Each batch is one record of the stream.
    /// assert!(second_append > first_append);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn append(&self, batch: &[SeriesSamples]) -> Result<SequenceNumber, MetricsError> {
        let payload = encode_record(batch)?;
        match self.writer.append(payload).await {
            Ok(sequence) => Ok(sequence),
            Err(reason @ (AppendError::Closed | AppendError::Rejected(_))) => Err(MetricsError::NotWritten { reason }),
            Err(reason @ (AppendError::Fenced | AppendError::Indeterminate { .. })) => {
                Err(MetricsError::OutcomeUnknown { reason })
            }
        }
    }

    /// Returns the samples of the selected series in a span of time.
    ///
    /// The answer comes from the blocks in the object store and the samples in memory; see `query.rs`.
    /// Only the parts of the blocks that the query needs are fetched.
    ///
    /// # Arguments
    ///
    /// * `selector` - Which series.
    /// * `time_range` - Which samples of them, both ends included.
    /// * `position` - How current the answer must be; see [`QueryPosition`].
    ///
    /// # Returns
    ///
    /// The answer and the record it is exact up to. With [`QueryPosition::AtLeast`] that record is at or
    /// after the one asked for.
    ///
    /// # Errors
    ///
    /// Returns [`MetricsError::NotApplied`] if the record asked for was not applied in time,
    /// [`MetricsError::Materializer`] if the materializer has stopped short of it,
    /// [`MetricsError::Block`] if a block cannot be read or is damaged, and
    /// [`MetricsError::CatalogKeptChanging`] if blocks were replaced under the query at every attempt.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use bitacora_artifact::MemoryArtifactStore;
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions};
    /// # use bitacora_metrics::{MetricsDependencies, MetricsSettings, MetricsStore};
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// # client.admin().create_stream("tenant-7", StreamOptions::default()).await?;
    /// # let dependencies = MetricsDependencies {
    /// #     client,
    /// #     metadata: cluster.metadata().clone(),
    /// #     artifact_store: Arc::new(MemoryArtifactStore::new()),
    /// # };
    /// # MetricsStore::initialize(&dependencies.metadata, "tenant-7").await?;
    /// # let store: MetricsStore = MetricsStore::open(dependencies, "tenant-7", MetricsSettings::default()).await?;
    /// use bitacora_metrics::{Labels, QueryPosition, Sample, Selector, SeriesSamples, TimeRange};
    ///
    /// let requests = SeriesSamples {
    ///     labels: Labels::from_pairs(&[("__name__", "requests_total"), ("job", "api")])?,
    ///     samples: vec![Sample { timestamp_millis: 1_700_000_000_000, value: 12.0 }],
    /// };
    /// let appended_at = store.append(&[requests.clone()]).await?;
    ///
    /// // The store answers with the samples that were just appended, because the query asks to see them.
    /// let answer = store
    ///     .query(&Selector::all(), TimeRange::ALL, QueryPosition::AtLeast(appended_at))
    ///     .await?;
    ///
    /// assert_eq!(answer.series, vec![requests]);
    /// assert_eq!(answer.as_of, appended_at);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query(
        &self,
        selector: &Selector,
        time_range: TimeRange,
        position: QueryPosition,
    ) -> Result<QueryResult, MetricsError> {
        if let QueryPosition::AtLeast(sequence) = position {
            self.wait_until_applied(sequence).await?;
        }
        let answer = query_head_and_blocks(
            &self.head,
            &self.catalog,
            &self.artifacts,
            &self.block_indexes,
            selector,
            time_range,
        )
        .await?;
        Ok(answer)
    }

    /// Lists the blocks the store's materializer has sealed.
    ///
    /// # Returns
    ///
    /// The catalog entries of the blocks, in log order: gapless and without overlap. The samples the store still
    /// holds in memory are in no block yet and are not listed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use bitacora_artifact::MemoryArtifactStore;
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions};
    /// # use bitacora_metrics::{MetricsDependencies, MetricsSettings, MetricsStore};
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// # client.admin().create_stream("tenant-7", StreamOptions::default()).await?;
    /// # let dependencies = MetricsDependencies {
    /// #     client,
    /// #     metadata: cluster.metadata().clone(),
    /// #     artifact_store: Arc::new(MemoryArtifactStore::new()),
    /// # };
    /// # MetricsStore::initialize(&dependencies.metadata, "tenant-7").await?;
    /// # let store: MetricsStore = MetricsStore::open(dependencies, "tenant-7", MetricsSettings::default()).await?;
    /// use bitacora_metrics::BlockSummary;
    ///
    /// let mut samples_in_blocks = 0;
    /// for block in store.blocks() {
    ///     if let Some(summary) = BlockSummary::decode(&block.artifact.summary) {
    ///         samples_in_blocks += summary.sample_count;
    ///     }
    /// }
    /// // Nothing was appended, so nothing was cut into a block.
    /// assert_eq!(samples_in_blocks, 0);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn blocks(&self) -> Vec<CatalogEntry> {
        self.catalog.entries()
    }

    /// Gives a handle that shows where the store's materializer is: applied, checkpointed, lag.
    ///
    /// # Returns
    ///
    /// A handle that can be cloned, read and waited on from any task. It also says why the materializer
    /// stopped, once it has.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use bitacora_artifact::MemoryArtifactStore;
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions};
    /// # use bitacora_metrics::{MetricsDependencies, MetricsSettings, MetricsStore};
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// # client.admin().create_stream("tenant-7", StreamOptions::default()).await?;
    /// # let dependencies = MetricsDependencies {
    /// #     client,
    /// #     metadata: cluster.metadata().clone(),
    /// #     artifact_store: Arc::new(MemoryArtifactStore::new()),
    /// # };
    /// # MetricsStore::initialize(&dependencies.metadata, "tenant-7").await?;
    /// # let store: MetricsStore = MetricsStore::open(dependencies, "tenant-7", MetricsSettings::default()).await?;
    /// use bitacora_metrics::{Labels, Sample, SeriesSamples};
    ///
    /// let requests = SeriesSamples {
    ///     labels: Labels::from_pairs(&[("__name__", "requests_total"), ("job", "api")])?,
    ///     samples: vec![Sample { timestamp_millis: 1_700_000_000_000, value: 12.0 }],
    /// };
    /// let appended_at = store.append(&[requests]).await?;
    ///
    /// let mut progress = store.progress();
    /// let reached = progress.wait_until_applied(appended_at).await?;
    /// assert!(reached.applied_sequence >= appended_at);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn progress(&self) -> RunnerProgress {
        self.progress.clone()
    }

    /// Waits until the materializer has applied the record at `sequence`, and says why if it does not.
    async fn wait_until_applied(&self, sequence: SequenceNumber) -> Result<(), MetricsError> {
        let mut progress = self.progress.clone();
        let not_applied = MetricsError::NotApplied {
            sequence,
            waited: self.query_wait,
        };
        match timeout(self.query_wait, progress.wait_until_applied(sequence)).await {
            Ok(Ok(_progress)) => Ok(()),
            Err(_timed_out) => Err(not_applied),
            // A materializer that has stopped will never get there. It says why, unless its task was
            // stopped from outside.
            Ok(Err(runner_stopped)) => match runner_stopped.reason {
                Some(reason) => Err(MetricsError::Materializer(reason)),
                None => Err(not_applied),
            },
        }
    }
}

impl<Policy: BlockPolicy> Drop for MetricsStore<Policy> {
    /// Stops the materializer. The writer stops when its own field is dropped.
    fn drop(&mut self) {
        self.runner_task.abort();
    }
}

#[cfg(test)]
#[path = "store_test.rs"]
mod tests;
