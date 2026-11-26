//! What a runner is built from: how it reaches the cluster and how it paces its checkpoints.
//!
//! # Why this file exists
//!
//! A runner needs three things from outside (a client, the metadata store, the artifact store), a name, and three
//! numbers that decide how often it checkpoints. Whoever starts a runner fills these in: a store's front end, a
//! daemon reading its configuration, a test. Keeping them apart from the loop lets a reader see what can be tuned,
//! and with which defaults, without reading the loop.
//!
//! # What it contains
//!
//! - [`RunnerDependencies`]: the handles a runner reaches the cluster with.
//! - [`RunnerSettings`]: the materializer's name and the pacing of its checkpoints.
//!
//! # How it works
//!
//! Both are plain values with public fields. [`RunnerSettings::for_materializer`] gives the pacing the design
//! proposes (section 11, "What the runner does"); a caller changes single fields from there.
//!
//! A checkpoint is due when either threshold is passed, bytes or time, and something was applied since the last one.
//! A checkpoint lets the log be truncated up to it and bounds what the next instance applies again, so the two
//! thresholds trade the cost of writing artifacts against the length of the log kept and of a restart.
//!
//! # Invariants
//!
//! - Nothing here does I/O.
//! - The name in the settings is the materializer's identity: two runners with the same name on the same stream are
//!   two instances of one materializer, and the later one takes over.
//!
//! # See also
//!
//! - `runner.rs`: the loop that uses both.
//! - `progress.rs`: what a runner gives back.

use std::sync::Arc;
use std::time::Duration;

use bitacora_artifact::ArtifactStore;
use bitacora_client::BitacoraClient;
use bitacora_meta::MetadataStore;

/// A checkpoint is written once this many bytes of record payloads were applied since the last one: 64 MiB.
const DEFAULT_CHECKPOINT_AFTER_BYTES: u64 = 64 * 1024 * 1024;

/// A checkpoint is written once this much time has passed since the last one, if anything was applied.
const DEFAULT_CHECKPOINT_AFTER: Duration = Duration::from_secs(60);

/// How long a runner waits before it tries a checkpoint again after a store could not be reached.
const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// What a runner needs to reach the cluster.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use bitacora_artifact::MemoryArtifactStore;
/// use bitacora_client::{BitacoraClient, ClientSettings};
/// use bitacora_materializer::RunnerDependencies;
///
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() {
/// # let cluster = bitacora_testkit::Cluster::start(3).await;
/// let client = BitacoraClient::new(
///     cluster.metadata().clone(),
///     cluster.connector(),
///     cluster.clock(),
///     ClientSettings::default(),
/// );
/// let dependencies = RunnerDependencies {
///     client,
///     metadata: cluster.metadata().clone(),
///     artifact_store: Arc::new(MemoryArtifactStore::new()),
/// };
/// # drop(dependencies);
/// # }
/// ```
#[derive(Clone)]
pub struct RunnerDependencies {
    /// The client the runner follows the stream with.
    pub client: BitacoraClient,
    /// The metadata store, for the materializer's checkpoint and fencing token.
    pub metadata: MetadataStore,
    /// Where the materializer's artifacts and manifests are stored.
    pub artifact_store: Arc<dyn ArtifactStore>,
}

/// How a runner paces its checkpoints.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use bitacora_materializer::RunnerSettings;
///
/// let settings = RunnerSettings {
///     checkpoint_after: Duration::from_secs(10),
///     ..RunnerSettings::for_materializer("word-count")
/// };
/// assert_eq!(settings.materializer, "word-count");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunnerSettings {
    /// The materializer's name, as registered on the stream. Two runners with the same name on the same
    /// stream are two instances of one materializer: the later one takes over.
    pub materializer: String,
    /// A checkpoint is written once this many bytes of record payloads were applied since the last one.
    pub checkpoint_after_bytes: u64,
    /// A checkpoint is written once this much time has passed since the last one, if anything was applied.
    pub checkpoint_after: Duration,
    /// How long the runner waits before it tries a checkpoint again after a store could not be reached.
    pub retry_interval: Duration,
}

impl RunnerSettings {
    /// The default pacing for one materializer: a checkpoint every 64 MiB or 60 seconds (design, section 11), and
    /// one second between attempts while a store is unreachable.
    ///
    /// # Arguments
    ///
    /// * `materializer` - The materializer's name, as registered on the stream.
    ///
    /// # Returns
    ///
    /// Settings with that name and the default pacing.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use bitacora_materializer::RunnerSettings;
    ///
    /// let settings = RunnerSettings::for_materializer("bitacora-kv");
    /// assert_eq!(settings.checkpoint_after, Duration::from_secs(60));
    /// ```
    #[must_use]
    pub fn for_materializer(materializer: &str) -> Self {
        Self {
            materializer: materializer.to_owned(),
            checkpoint_after_bytes: DEFAULT_CHECKPOINT_AFTER_BYTES,
            checkpoint_after: DEFAULT_CHECKPOINT_AFTER,
            retry_interval: DEFAULT_RETRY_INTERVAL,
        }
    }
}

#[cfg(test)]
#[path = "settings_test.rs"]
mod tests;
