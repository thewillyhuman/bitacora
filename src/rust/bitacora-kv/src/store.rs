//! The front end of bitacora-kv: the compute node of the design.
//!
//! # Why this file exists
//!
//! A key-value store on bitacora is three things wired together: the writer of a stream, a materializer
//! that turns the stream into bucket pages, and reads that look keys up in those pages (design, section
//! 12). This file wires them, and it is where the one hard question of the store is answered: when is a
//! read allowed to return what the pages say?
//!
//! # What it contains
//!
//! - [`KvStore`]: `put`, `delete`, `write`, `get`, `get_as_of`, `scan`.
//! - [`KvDependencies`] and [`KvSettings`]: what a store is built from.
//! - [`KvError`]: what can go wrong, split by what the caller may conclude.
//! - [`KV_MATERIALIZER`]: the name the store's materializer is registered under.
//!
//! # How it works
//!
//! [`KvStore::open`] becomes the writer of the stream, then takes the materializer over and runs it in a
//! background task: a [`PageStore`] of [`KvPages`] under a [`MaterializerRunner`]. The order matters. If
//! becoming the writer fails, nothing was taken from the store that is the writer.
//!
//! A write is one append. It returns when the record is committed, with the record's sequence number.
//! One call appends at most one record: the store never retries an append, and the log deduplicates the
//! writer's own resends by sequence number. A write that fails with [`KvError::OutcomeUnknown`] may or
//! may not be in the log; the store does not know, and says so. The caller that tries again makes a
//! second, separate write. That is harmless for a put that nobody else has overwritten in between, and a
//! caller for whom it would not be harmless reads the key back first: [`KvStore::get`] is linearizable.
//!
//! ## How a read is made linearizable
//!
//! [`KvStore::get`] must never return a value that a write, finished before the read began, has
//! replaced. The pages alone cannot promise that: this node's materializer may lag, and this node may
//! have been replaced as the writer without knowing it yet. So a read takes three steps:
//!
//! 1. It appends a barrier: a record without operations. The append returns the barrier's sequence
//!    number S once the barrier is committed.
//! 2. It waits until the materializer has applied the record at S.
//! 3. It reads the key's bucket as of S.
//!
//! Why S is a safe place to read at. Every write that finished before the read began is in the log
//! below S. If this node committed it, it has a lower sequence number, because this node assigns them in
//! order. If a newer writer committed it, that writer had fenced a recovery quorum of the replicas and
//! sealed this node's segment before it could commit anything; the barrier, appended after that, cannot
//! gather a write quorum of unfenced replicas, so step 1 fails and the read returns no value at all. In
//! short: a barrier that commits proves this node was still the writer when the read was under way, and
//! the commit rule of the log is the proof.
//!
//! What a read does when it cannot do this. If the barrier cannot be committed, the read fails with
//! [`KvError::NotCurrent`]. If the materializer does not reach S within [`KvSettings::read_wait`], it
//! fails with [`KvError::Read`]. It never falls back to an answer it cannot vouch for.
//! [`KvStore::get_as_of`] and [`KvStore::scan`] read at a sequence the caller names and need no barrier;
//! they are the reads a replica that is not the writer can serve.
//!
//! The cost is one small record per linearizable read. A writer that could confirm its epoch with a
//! round of heartbeats instead would avoid it; the client library does not offer that yet.
//!
//! # Invariants
//!
//! - A value returned by [`KvStore::get`] was the committed value of the key at some moment between the
//!   call and its return.
//! - A write reported as done is in the log at the returned sequence number, once.
//! - The store answers reads only from records the log has committed.
//!
//! # See also
//!
//! - `record.rs`: the records appended here.
//! - `pages.rs`: the pages read here.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bitacora_artifact::ArtifactStore;
use bitacora_client::{AppendError, BitacoraClient, OpenError, StreamWriter, TakeoverPolicy};
use bitacora_materializer::{
    MaterializerRunner, PageReadError, PageReader, PageStore, RunnerDependencies, RunnerError, RunnerProgress,
    RunnerSettings,
};
use bitacora_meta::{MetaError, MetadataStore};
use bitacora_types::{Epoch, SequenceNumber};
use bytes::Bytes;
use thiserror::Error;
use tokio::task::JoinHandle;

use crate::pages::{BUCKET_COUNT, KvPages, bucket_of, decode_bucket, value_in_bucket};
use crate::record::{KvOperation, KvRecord};

/// The name the materializer of a bitacora-kv store is registered under on its stream.
pub const KV_MATERIALIZER: &str = "bitacora-kv";

/// What a store needs to reach the cluster.
#[derive(Clone)]
pub struct KvDependencies {
    /// The client the store writes and follows the stream with.
    pub client: BitacoraClient,
    /// The metadata store, for the materializer's checkpoint.
    pub metadata: MetadataStore,
    /// Where the materializer keeps its layers.
    pub artifact_store: Arc<dyn ArtifactStore>,
}

/// How a store takes its stream over, paces its checkpoints and bounds its reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvSettings {
    /// What to do if another store still holds the writer's lease.
    pub takeover: TakeoverPolicy,
    /// A checkpoint is written once this many bytes of records were applied since the last one.
    pub checkpoint_after_bytes: u64,
    /// A checkpoint is written once this much time has passed since the last one.
    pub checkpoint_after: Duration,
    /// How long the materializer waits before it tries a failed checkpoint again.
    pub retry_interval: Duration,
    /// How long a read waits for the materializer to apply the sequence it reads at.
    pub read_wait: Duration,
}

impl Default for KvSettings {
    /// Waits for a live writer's lease to lapse, checkpoints every 64 MiB or 60 seconds, and gives a
    /// read ten seconds.
    fn default() -> Self {
        let runner = RunnerSettings::for_materializer(KV_MATERIALIZER);
        Self {
            takeover: TakeoverPolicy::WaitForLeaseExpiry,
            checkpoint_after_bytes: runner.checkpoint_after_bytes,
            checkpoint_after: runner.checkpoint_after,
            retry_interval: runner.retry_interval,
            read_wait: Duration::from_secs(10),
        }
    }
}

/// Why an operation on a store failed, split by what the caller may conclude from it.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum KvError {
    /// The store could not become the writer of the stream. Nothing changed.
    #[error("the store could not become the writer: {0}")]
    Open(#[from] OpenError),
    /// The store's materializer could not start, or has stopped; the reason says which. A store whose
    /// materializer stopped can no longer read: open a new one.
    #[error("the store's materializer is not running: {0}")]
    Materializer(#[from] RunnerError),
    /// The write was refused before it reached the log. It did not and will not take effect.
    #[error("the write was not made: {reason}")]
    NotWritten {
        /// Why the log refused the record.
        reason: AppendError,
    },
    /// The store lost contact or was replaced before it learned whether the write was committed. The
    /// write may have taken effect, may still take effect, or never will.
    #[error("the outcome of the write is unknown: {reason}")]
    OutcomeUnknown {
        /// What interrupted the write.
        reason: AppendError,
    },
    /// The store could not prove that it is still the writer, so the read gave no answer rather than one
    /// that might be stale.
    #[error("the read could not be proven current: {reason}")]
    NotCurrent {
        /// Why the barrier could not be committed.
        reason: AppendError,
    },
    /// The pages could not be read at the requested sequence: it was not applied in time, lies below
    /// what the store still keeps, or a layer is damaged.
    #[error(transparent)]
    Read(#[from] PageReadError),
    /// A bucket page does not hold what a bucket page should.
    #[error("bucket {bucket} is damaged: {reason}")]
    DamagedBucket {
        /// The bucket whose page was read.
        bucket: u32,
        /// What is wrong with the page.
        reason: &'static str,
    },
}

/// A key-value store on one stream: the writer of the stream and a materialized view of it.
///
/// Dropping the store stops its materializer and its writer at once, like a crash. The next store opened
/// on the stream recovers whatever was in flight.
///
/// # Examples
///
/// ```no_run
/// # use bitacora_kv::{KvDependencies, KvSettings, KvStore};
/// # use bytes::Bytes;
/// # async fn example(dependencies: KvDependencies) -> Result<(), bitacora_kv::KvError> {
/// KvStore::initialize(&dependencies.metadata, "inventory")
///     .await
///     .expect("the stream exists");
/// let store = KvStore::open(dependencies, "inventory", KvSettings::default()).await?;
///
/// store.put(Bytes::from_static(b"apples"), Bytes::from_static(b"12")).await?;
/// assert_eq!(store.get(b"apples").await?, Some(Bytes::from_static(b"12")));
///
/// store.delete(Bytes::from_static(b"apples")).await?;
/// assert_eq!(store.get(b"apples").await?, None);
/// # Ok(())
/// # }
/// ```
pub struct KvStore {
    /// The writer of the stream.
    writer: StreamWriter,
    /// The bucket pages the materializer maintains.
    pages: PageReader<KvPages>,
    /// Where the materializer is.
    progress: RunnerProgress,
    /// The task that runs the materializer. Aborted when the store is dropped.
    runner_task: JoinHandle<()>,
    /// How long reads wait.
    read_wait: Duration,
}

impl KvStore {
    /// Prepares a stream for a key-value store, once: registers the store's materializer on it, so that
    /// the log is not truncated beyond what the store has checkpointed.
    ///
    /// Doing it again changes nothing. It is a separate step from [`KvStore::open`] on purpose: a store
    /// that registered itself whenever it found itself unregistered would come back empty, and silent,
    /// after an operator removed it and the log moved on.
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
    pub async fn initialize(metadata: &MetadataStore, stream_name: &str) -> Result<(), MetaError> {
        metadata.register_materializer(stream_name, KV_MATERIALIZER).await?;
        Ok(())
    }

    /// Opens the store on `stream_name`: becomes the writer of the stream, restores the materializer
    /// from its last checkpoint and starts it.
    ///
    /// Any other store open on the stream is replaced: its writes fail, its linearizable reads fail,
    /// and its materializer can no longer checkpoint. The new store catches up with the log in the
    /// background; a read waits for that as long as [`KvSettings::read_wait`] allows.
    ///
    /// # Arguments
    ///
    /// * `dependencies` - How to reach the cluster.
    /// * `stream_name` - The stream the store lives on, prepared with [`KvStore::initialize`].
    /// * `settings` - Takeover, checkpoint pacing and read waits.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::Open`] if the store could not become the writer; nothing changed then. Returns
    /// [`KvError::Materializer`] if the materializer could not start, for example because
    /// [`KvStore::initialize`] was never run; the stream is then left without a writer until a store is
    /// opened on it.
    pub async fn open(dependencies: KvDependencies, stream_name: &str, settings: KvSettings) -> Result<Self, KvError> {
        let writer = dependencies.client.open_writer(stream_name, settings.takeover).await?;
        let runner_settings = RunnerSettings {
            checkpoint_after_bytes: settings.checkpoint_after_bytes,
            checkpoint_after: settings.checkpoint_after,
            retry_interval: settings.retry_interval,
            ..RunnerSettings::for_materializer(KV_MATERIALIZER)
        };
        let runner_dependencies = RunnerDependencies {
            client: dependencies.client,
            metadata: dependencies.metadata,
            artifact_store: dependencies.artifact_store,
        };
        let runner =
            MaterializerRunner::<PageStore<KvPages>>::start(runner_dependencies, stream_name, runner_settings).await?;
        let pages = runner.materializer().reader();
        let progress = runner.progress();
        let runner_task = tokio::spawn(async move {
            let reason = runner.run().await;
            tracing::warn!(%reason, "the key-value store's materializer stopped");
        });
        Ok(Self {
            writer,
            pages,
            progress,
            runner_task,
            read_wait: settings.read_wait,
        })
    }

    /// Sets the value of a key and waits until that is committed.
    ///
    /// # Returns
    ///
    /// The sequence number of the record that made the change. [`KvStore::get_as_of`] at that sequence
    /// sees it.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::NotWritten`] if the record was refused, for example for its size, and
    /// [`KvError::OutcomeUnknown`] if the store cannot tell whether the put was committed.
    pub async fn put(&self, key: Bytes, value: Bytes) -> Result<SequenceNumber, KvError> {
        self.write(vec![KvOperation::Put { key, value }]).await
    }

    /// Removes a key and waits until that is committed. Removing a key that does not exist succeeds.
    ///
    /// # Returns
    ///
    /// The sequence number of the record that made the change.
    ///
    /// # Errors
    ///
    /// As for [`KvStore::put`].
    pub async fn delete(&self, key: Bytes) -> Result<SequenceNumber, KvError> {
        self.write(vec![KvOperation::Delete { key }]).await
    }

    /// Makes several changes as one: they are committed together or not at all, and every read sees
    /// all of them or none.
    ///
    /// # Arguments
    ///
    /// * `operations` - The changes, applied in order. Later ones win over earlier ones on the same key.
    ///
    /// # Returns
    ///
    /// The sequence number of the one record that carries the changes.
    ///
    /// # Errors
    ///
    /// As for [`KvStore::put`].
    pub async fn write(&self, operations: Vec<KvOperation>) -> Result<SequenceNumber, KvError> {
        let payload = KvRecord { operations }.encode();
        match self.writer.append(payload).await {
            Ok(sequence) => Ok(sequence),
            Err(reason @ (AppendError::Closed | AppendError::Rejected(_))) => Err(KvError::NotWritten { reason }),
            Err(reason @ (AppendError::Fenced | AppendError::Indeterminate { .. })) => {
                Err(KvError::OutcomeUnknown { reason })
            }
        }
    }

    /// Reads the latest committed value of a key. Linearizable: it reflects every write that finished
    /// before the call, and nothing that was never committed.
    ///
    /// It appends a barrier record, waits until the materializer has applied it, and reads at its
    /// sequence; see the file documentation for why.
    ///
    /// # Returns
    ///
    /// The key's value, or `None` if the key does not exist.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::NotCurrent`] if the store could not prove it is still the writer; the read
    /// then gives no answer rather than a stale one. Returns [`KvError::Read`] or
    /// [`KvError::Materializer`] if the materializer did not get to the barrier in time or has stopped.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, KvError> {
        let barrier = self
            .writer
            .append(KvRecord::barrier().encode())
            .await
            .map_err(|reason| KvError::NotCurrent { reason })?;
        self.get_as_of(key, barrier).await
    }

    /// Reads the value a key had right after the record at `sequence`.
    ///
    /// This is a read of history: it says nothing about what was written after `sequence`. It needs no
    /// barrier and works on a store that is no longer the writer.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to read.
    /// * `sequence` - The sequence to read at. The read waits for the materializer to reach it, up to
    ///   [`KvSettings::read_wait`].
    ///
    /// # Returns
    ///
    /// The key's value as of `sequence`, or `None` if the key did not exist then.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::Read`] if `sequence` was not applied in time or is older than what the store
    /// still keeps, and [`KvError::Materializer`] if the materializer has stopped short of `sequence`.
    pub async fn get_as_of(&self, key: &[u8], sequence: SequenceNumber) -> Result<Option<Bytes>, KvError> {
        let bucket = bucket_of(key);
        let page = self.read_bucket(bucket, sequence).await?;
        value_in_bucket(page.as_ref(), key).map_err(|damaged| KvError::DamagedBucket {
            bucket,
            reason: damaged.reason,
        })
    }

    /// Reads every key from `from_key` up to, but not including, `to_key`, as of one sequence.
    ///
    /// It reads every bucket, because keys are spread over buckets by hash: the cost does not depend on
    /// how narrow the range is.
    ///
    /// # Arguments
    ///
    /// * `from_key` - The first key of the range.
    /// * `to_key` - The key the range ends before.
    /// * `sequence` - The sequence to read at, as for [`KvStore::get_as_of`].
    ///
    /// # Returns
    ///
    /// The pairs in the range as of `sequence`, in ascending key order.
    ///
    /// # Errors
    ///
    /// As for [`KvStore::get_as_of`].
    pub async fn scan(
        &self,
        from_key: &[u8],
        to_key: &[u8],
        sequence: SequenceNumber,
    ) -> Result<Vec<(Bytes, Bytes)>, KvError> {
        let mut pairs_in_range = BTreeMap::new();
        for bucket in 0..BUCKET_COUNT {
            let Some(page) = self.read_bucket(bucket, sequence).await? else {
                continue;
            };
            let pairs = decode_bucket(&page).map_err(|damaged| KvError::DamagedBucket {
                bucket,
                reason: damaged.reason,
            })?;
            for (key, value) in pairs {
                if &key[..] >= from_key && &key[..] < to_key {
                    pairs_in_range.insert(key, value);
                }
            }
        }
        Ok(pairs_in_range.into_iter().collect())
    }

    /// The last record the store's materializer has applied.
    #[must_use]
    pub fn applied_sequence(&self) -> SequenceNumber {
        self.pages.applied_sequence()
    }

    /// The epoch this store writes with.
    #[must_use]
    pub fn writer_epoch(&self) -> Epoch {
        self.writer.epoch()
    }

    /// A handle that shows where the store's materializer is: applied, checkpointed, lag.
    #[must_use]
    pub fn progress(&self) -> RunnerProgress {
        self.progress.clone()
    }

    /// Reads one bucket page as of `sequence`, and says why if it cannot.
    async fn read_bucket(&self, bucket: u32, sequence: SequenceNumber) -> Result<Option<Bytes>, KvError> {
        let read = self.pages.read_page(&bucket, sequence, self.read_wait).await;
        match read {
            Ok(page) => Ok(page),
            Err(not_reached @ PageReadError::NotReached { .. }) => {
                // A materializer that has stopped will never get there; say that instead.
                match self.progress.stop_reason() {
                    Some(reason) => Err(KvError::Materializer(reason)),
                    None => Err(KvError::Read(not_reached)),
                }
            }
            Err(read_error) => Err(KvError::Read(read_error)),
        }
    }
}

impl Drop for KvStore {
    /// Stops the materializer. The writer stops when its own field is dropped.
    fn drop(&mut self) {
        self.runner_task.abort();
    }
}

#[cfg(test)]
#[path = "store_test.rs"]
mod tests;
