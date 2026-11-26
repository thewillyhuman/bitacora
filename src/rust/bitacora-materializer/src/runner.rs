//! The loop that drives a materializer.
//!
//! # Why this file exists
//!
//! A materializer only says how records change state and how state is written down. Something has to
//! tail the log, decide when to checkpoint, make the checkpoint count, and make sure that a second copy of
//! the same materializer cannot do the same at the same time. That is the same for every workload, and it
//! is where the mistakes that lose data are made: committing a checkpoint whose artifacts are not there,
//! or letting the log be truncated above what was really persisted. This file does it once.
//!
//! # What it contains
//!
//! - [`MaterializerRunner`]: takes a materializer over, restores it and runs it.
//! - [`RunnerError`]: why a runner could not start, or stopped.
//!
//! What a runner is built from and what it gives back are in files of their own, and are part of this module:
//! `runner/settings.rs` holds [`RunnerDependencies`] and [`RunnerSettings`], `runner/progress.rs` holds
//! [`RunnerProgress`], [`Progress`] and [`RunnerStopped`].
//!
//! # How it works
//!
//! [`MaterializerRunner::start`] does steps 1 and 2 of the design (section 11, "What the runner does"):
//!
//! 1. It raises the materializer's fencing token in the metadata store and reads the checkpoint that came
//!    with it. Whoever held the token before can no longer commit.
//! 2. It reads the manifest the checkpoint points at, restores the materializer from it, and starts
//!    following the stream at the record after the checkpoint.
//!
//! [`MaterializerRunner::run`] then loops over steps 3 to 6 until it has to stop:
//!
//! 3. It applies every batch of committed records, in order.
//! 4. When enough bytes or time have passed, or the materializer asks, it has the materializer write a
//!    checkpoint, and stores the manifest as one more artifact.
//! 5. It commits the checkpoint: one conditional write of the new sequence and manifest pointer, valid
//!    only while the fencing token is still its own. Then it deletes the artifacts the new manifest no
//!    longer names.
//! 6. It publishes how far it has applied and committed, and how far the log is known to be committed.
//!
//! A step that fails because a store cannot be reached is tried again after a pause, and the runner keeps
//! applying records meanwhile: nothing it has applied is lost, the checkpoint is only late. A step that
//! fails for a reason that waiting cannot change stops the runner with the reason.
//!
//! # Invariants
//!
//! - A checkpoint is committed only after every artifact its manifest names, and the manifest, are stored.
//! - The committed checkpoint never exceeds the last record applied, and never moves backwards.
//! - A runner that learns its token was taken stops and never commits again. `run` consumes the runner,
//!   so there is nothing left to commit with.
//! - Records are applied exactly once per run, in order, without gaps. A gap the log cannot fill is an
//!   error, never skipped.
//! - Applying a batch and deciding whether a checkpoint is due run for every batch of the log. They are a
//!   few comparisons and one pass over the batch to count its bytes; nothing is allocated and nothing is
//!   awaited between two batches unless a checkpoint is due. Their cost was not measured.
//! - The asynchronous helpers take the runner exclusively even where they change nothing: a shared borrow
//!   held across an await would require every materializer to be `Sync`.
//! - [`MaterializerRunner::start`] is longer than the functions around it on purpose: the steps of taking
//!   a materializer over await each other's results, and the order is the protocol. It stays in one place.
//!
//! # See also
//!
//! - `contract.rs`: what the runner calls.
//! - `manifest.rs`: the stored form of a manifest.
//! - `sweeper.rs`: the deletion of artifacts no manifest names.

mod progress;
mod settings;

use std::collections::BTreeSet;

use bitacora_client::{ReadError, RecordStream};
use bitacora_meta::{CheckpointRecord, MetaError, MetadataStore};
use bitacora_types::{LogRecord, SequenceNumber, StreamId};
use thiserror::Error;
use tokio::time::{Instant, timeout_at};

use self::progress::ProgressPublisher;
pub use self::progress::{Progress, RunnerProgress, RunnerStopped};
pub use self::settings::{RunnerDependencies, RunnerSettings};
use crate::contract::{CheckpointManifest, Materializer, MaterializerError};
use crate::manifest::{decode_manifest, encode_manifest};
use crate::namespace::ArtifactNamespace;
use crate::sweeper::delete_unreferenced;

/// The label of the artifact a manifest is stored as.
const MANIFEST_LABEL: &str = "manifest";

/// Why a runner could not start, or stopped.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum RunnerError {
    /// The metadata store refused or could not be reached while the runner was starting, or the
    /// materializer is not registered on the stream.
    #[error(transparent)]
    Metadata(#[from] MetaError),
    /// The stream could not be followed: it was deleted, or the metadata store could not be reached while
    /// the runner was starting.
    #[error(transparent)]
    Read(#[from] ReadError),
    /// The materializer failed in a way that trying again cannot fix.
    #[error(transparent)]
    Materializer(#[from] MaterializerError),
    /// Another runner has taken the materializer over. This one must not be started again in its place:
    /// start a new runner, which takes the token back.
    #[error("another runner took the materializer over: token {held_token} was superseded by {current_token}")]
    Fenced {
        /// The token this runner held.
        held_token: u64,
        /// The materializer's current token.
        current_token: u64,
    },
    /// The records the runner needs next were deleted from the log. For the runner that holds the token
    /// this must never happen: the log is only truncated up to the committed checkpoint.
    #[error("record {next_sequence} is needed but the log now starts at {low_water_mark}")]
    LogTruncated {
        /// The record the runner had to apply next.
        next_sequence: SequenceNumber,
        /// The first record the log still holds.
        low_water_mark: SequenceNumber,
    },
    /// The stream ended: it was deleted.
    #[error("the stream ended")]
    StreamEnded,
}

/// Runs one instance of a materializer: restores it, feeds it the log, and commits its checkpoints.
///
/// # Examples
///
/// A materializer that counts records, run against a cluster inside this process until it has applied the
/// record that was just appended. `contract.rs` shows what a materializer that keeps its count across
/// restarts looks like.
///
/// ```
/// # use async_trait::async_trait;
/// # use bitacora_materializer::{ArtifactNamespace, CheckpointManifest, Materializer, MaterializerError};
/// # use bitacora_types::LogRecord;
/// # struct RecordCounter;
/// # #[async_trait]
/// # impl Materializer for RecordCounter {
/// #     fn apply_records(&mut self, _committed_records: &[LogRecord]) -> Result<(), MaterializerError> {
/// #         Ok(())
/// #     }
/// #     async fn write_checkpoint(
/// #         &mut self,
/// #         _artifacts: &ArtifactNamespace,
/// #     ) -> Result<CheckpointManifest, MaterializerError> {
/// #         Ok(CheckpointManifest::default())
/// #     }
/// #     async fn restore_from_checkpoint(
/// #         _last_checkpoint: Option<CheckpointManifest>,
/// #         _artifacts: &ArtifactNamespace,
/// #     ) -> Result<Self, MaterializerError> {
/// #         Ok(Self)
/// #     }
/// # }
/// use std::sync::Arc;
///
/// use bitacora_artifact::MemoryArtifactStore;
/// use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions, TakeoverPolicy};
/// use bitacora_materializer::{MaterializerRunner, RunnerDependencies, RunnerSettings};
///
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let cluster = bitacora_testkit::Cluster::start(3).await;
/// # let client = BitacoraClient::new(
/// #     cluster.metadata().clone(),
/// #     cluster.connector(),
/// #     cluster.clock(),
/// #     ClientSettings::default(),
/// # );
/// client.admin().create_stream("orders", StreamOptions::default()).await?;
/// client.admin().register_materializer("orders", "record-counter").await?;
/// let dependencies = RunnerDependencies {
///     client: client.clone(),
///     metadata: cluster.metadata().clone(),
///     artifact_store: Arc::new(MemoryArtifactStore::new()),
/// };
///
/// let settings = RunnerSettings::for_materializer("record-counter");
/// let runner = MaterializerRunner::<RecordCounter>::start(dependencies, "orders", settings).await?;
/// let mut progress = runner.progress();
/// let running = tokio::spawn(runner.run());
///
/// let writer = client.open_writer("orders", TakeoverPolicy::Force).await?;
/// let appended_sequence = writer.append("order 1 placed".into()).await?;
/// let reached = progress.wait_until_applied(appended_sequence).await?;
/// assert_eq!(reached.applied_sequence, appended_sequence);
///
/// // Dropping the runner's task stops it like a crash would; the next runner resumes from its checkpoint.
/// running.abort();
/// # Ok(())
/// # }
/// ```
pub struct MaterializerRunner<Workload: Materializer> {
    /// The instance being run.
    materializer: Workload,
    /// The committed records of the stream, from the record after the restored checkpoint.
    following: RecordStream,
    /// The metadata store, to commit checkpoints.
    metadata: MetadataStore,
    /// The stream being materialized.
    stream_id: StreamId,
    /// The materializer's artifacts, writable with this runner's fencing token.
    artifacts: ArtifactNamespace,
    /// The token this runner took the materializer over with.
    fencing_token: u64,
    /// How checkpoints are paced.
    settings: RunnerSettings,
    /// The last record applied.
    applied_sequence: SequenceNumber,
    /// The last record covered by a committed checkpoint.
    committed_checkpoint: SequenceNumber,
    /// How many payload bytes were applied since the last committed checkpoint.
    bytes_since_checkpoint: u64,
    /// When the next checkpoint is due for time alone.
    checkpoint_due_at: Instant,
    /// The earliest moment the next checkpoint attempt may start, after one failed.
    retry_not_before: Instant,
    /// Where progress and the reason for stopping are published.
    progress: ProgressPublisher,
}

impl<Workload: Materializer> MaterializerRunner<Workload> {
    /// Takes the materializer over and restores it from its last committed checkpoint.
    ///
    /// Raises the fencing token in the metadata store, so that any other runner of the same materializer
    /// can no longer commit; reads the committed manifest from the artifact store; restores the
    /// materializer; and opens the stream at the record after the checkpoint. Nothing is applied yet.
    ///
    /// # Arguments
    ///
    /// * `dependencies` - How to reach the cluster.
    /// * `stream_name` - The stream to materialize.
    /// * `settings` - The materializer's name and how its checkpoints are paced.
    ///
    /// # Returns
    ///
    /// A runner ready for [`MaterializerRunner::run`]. Until then [`MaterializerRunner::materializer`]
    /// gives access to the restored instance, for example to take a handle that serves reads.
    ///
    /// # Errors
    ///
    /// Returns [`RunnerError::Metadata`] if the stream does not exist, the materializer is not registered
    /// on it (register it with `StreamAdmin::register_materializer` first; the runner never does, because
    /// a materializer that registers itself late silently starts without the records already truncated),
    /// or the metadata store cannot be reached. Returns [`RunnerError::Materializer`] if the committed
    /// manifest or an artifact cannot be read.
    ///
    /// # Examples
    ///
    /// The example of [`MaterializerRunner`] starts and runs a materializer; here only the call is shown.
    ///
    /// ```
    /// use bitacora_materializer::{
    ///     Materializer, MaterializerRunner, RunnerDependencies, RunnerError, RunnerSettings,
    /// };
    ///
    /// /// Takes the word count of the stream `orders` over from whoever ran it before.
    /// async fn take_the_word_count_over<Workload: Materializer>(
    ///     dependencies: RunnerDependencies,
    /// ) -> Result<MaterializerRunner<Workload>, RunnerError> {
    ///     let settings = RunnerSettings::for_materializer("word-count");
    ///     MaterializerRunner::start(dependencies, "orders", settings).await
    /// }
    /// ```
    pub async fn start(
        dependencies: RunnerDependencies,
        stream_name: &str,
        settings: RunnerSettings,
    ) -> Result<Self, RunnerError> {
        let metadata = dependencies.metadata;
        let stream = metadata.get_stream(stream_name).await?;
        let checkpoint = metadata
            .acquire_checkpoint(stream.stream_id, &settings.materializer)
            .await?;
        let artifacts = ArtifactNamespace::for_runner(
            dependencies.artifact_store,
            stream.stream_id,
            &settings.materializer,
            checkpoint.fencing_token,
        );
        let materializer = restore_materializer(&artifacts, &checkpoint).await?;
        let following = dependencies
            .client
            .open_reader(stream_name)
            .follow_from(checkpoint.checkpoint.next())
            .await?;
        tracing::info!(
            stream = stream_name,
            materializer = %settings.materializer,
            fencing_token = checkpoint.fencing_token,
            checkpoint = %checkpoint.checkpoint,
            "materializer restored"
        );
        Ok(Self::restored_at(
            &checkpoint,
            stream.stream_id,
            materializer,
            following,
            metadata,
            artifacts,
            settings,
        ))
    }

    /// Builds the runner of an instance that was just restored from `checkpoint` and has applied nothing since.
    fn restored_at(
        checkpoint: &CheckpointRecord,
        stream_id: StreamId,
        materializer: Workload,
        following: RecordStream,
        metadata: MetadataStore,
        artifacts: ArtifactNamespace,
        settings: RunnerSettings,
    ) -> Self {
        let now = Instant::now();
        Self {
            materializer,
            following,
            metadata,
            stream_id,
            artifacts,
            fencing_token: checkpoint.fencing_token,
            checkpoint_due_at: now + settings.checkpoint_after,
            retry_not_before: now,
            settings,
            applied_sequence: checkpoint.checkpoint,
            committed_checkpoint: checkpoint.checkpoint,
            bytes_since_checkpoint: 0,
            progress: ProgressPublisher::starting_at(checkpoint.checkpoint),
        }
    }

    /// The instance being run, as restored.
    ///
    /// Use it before [`MaterializerRunner::run`] to take whatever handles the materializer offers for reading its
    /// state: `run` takes the runner, and the instance with it.
    ///
    /// # Returns
    ///
    /// The restored instance. Nothing was applied to it yet.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_materializer::{MaterializerRunner, PageMaterializer, PageReader, PageStore};
    ///
    /// /// Takes the handle that reads pages, before the runner is sent off to run.
    /// fn page_reader_of<Engine: PageMaterializer>(
    ///     runner: &MaterializerRunner<PageStore<Engine>>,
    /// ) -> PageReader<Engine> {
    ///     runner.materializer().reader()
    /// }
    /// ```
    #[must_use]
    pub const fn materializer(&self) -> &Workload {
        &self.materializer
    }

    /// Gives a handle that shows where the runner is.
    ///
    /// # Returns
    ///
    /// A handle that can be cloned and sent to other tasks. It stays valid after `run` has taken the runner, and
    /// after the runner has stopped.
    ///
    /// # Examples
    ///
    /// The example of [`MaterializerRunner`] waits on such a handle.
    ///
    /// ```
    /// use bitacora_materializer::{Materializer, MaterializerRunner, RunnerProgress};
    ///
    /// /// Starts the runner in a task of its own and keeps the handle to watch it by.
    /// fn run_in_the_background<Workload: Materializer>(runner: MaterializerRunner<Workload>) -> RunnerProgress {
    ///     let progress = runner.progress();
    ///     tokio::spawn(runner.run());
    ///     progress
    /// }
    /// ```
    #[must_use]
    pub fn progress(&self) -> RunnerProgress {
        self.progress.handle()
    }

    /// The fencing token this runner took the materializer over with.
    ///
    /// # Returns
    ///
    /// The token, which is higher than that of every runner of this materializer before it. Every artifact this
    /// runner writes carries it in its key.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_materializer::{Materializer, MaterializerRunner};
    ///
    /// /// Whether `runner` took over after the runner that held `earlier_token`.
    /// fn is_successor_of<Workload: Materializer>(runner: &MaterializerRunner<Workload>, earlier_token: u64) -> bool {
    ///     runner.fencing_token() > earlier_token
    /// }
    /// ```
    #[must_use]
    pub const fn fencing_token(&self) -> u64 {
        self.fencing_token
    }

    /// Applies the log and commits checkpoints until the runner has to stop.
    ///
    /// Dropping the returned future stops the runner at once, like a crash: whatever was applied since
    /// the last committed checkpoint is applied again by the next runner.
    ///
    /// # Returns
    ///
    /// Why the runner stopped. It never stops without a reason:
    ///
    /// - [`RunnerError::Fenced`]: another runner took over.
    /// - [`RunnerError::LogTruncated`]: the log no longer holds the next record.
    /// - [`RunnerError::Materializer`]: a record could not be applied, or a checkpoint failed for good.
    /// - [`RunnerError::Read`] or [`RunnerError::StreamEnded`]: the stream was deleted.
    ///
    /// # Examples
    ///
    /// The example of [`MaterializerRunner`] runs a materializer in a task of its own.
    ///
    /// ```
    /// use bitacora_materializer::{Materializer, MaterializerRunner, RunnerError};
    ///
    /// /// Runs to the end and says whether a new runner may take this one's place.
    /// async fn run_and_say_whether_to_restart<Workload: Materializer>(runner: MaterializerRunner<Workload>) -> bool {
    ///     let why_it_stopped = runner.run().await;
    ///     !matches!(why_it_stopped, RunnerError::Fenced { .. })
    /// }
    /// ```
    pub async fn run(mut self) -> RunnerError {
        let reason = self.run_until_stopped().await;
        // Left for the progress handles before the runner, and with it the progress channel, is dropped:
        // whoever is woken by the channel closing finds the reason there.
        self.progress.leave_stop_reason(reason.clone());
        reason
    }

    /// Follows the stream, applies and checkpoints until something stops the runner, and returns what.
    async fn run_until_stopped(&mut self) -> RunnerError {
        loop {
            if let Err(stop) = self.checkpoint_if_due().await {
                return stop;
            }
            let next_batch = match self.next_wake_up() {
                Some(wake_up_at) => timeout_at(wake_up_at, self.following.next_batch()).await,
                None => Ok(self.following.next_batch().await),
            };
            let records = match next_batch {
                Err(_time_to_checkpoint) => continue,
                Ok(None) => return RunnerError::StreamEnded,
                Ok(Some(Err(ReadError::Truncated { low_water_mark }))) => {
                    return self.stopped_by_truncation(low_water_mark).await;
                }
                Ok(Some(Err(read_error))) => return RunnerError::Read(read_error),
                Ok(Some(Ok(records))) => records,
            };
            if let Err(materializer_error) = self.apply(&records) {
                return RunnerError::Materializer(materializer_error);
            }
        }
    }

    /// Feeds one batch of consecutive records to the materializer and publishes the new position.
    fn apply(&mut self, records: &[LogRecord]) -> Result<(), MaterializerError> {
        let Some(last_record) = records.last() else {
            return Ok(());
        };
        self.materializer.apply_records(records)?;
        self.applied_sequence = last_record.sequence;
        for record in records {
            self.bytes_since_checkpoint += record.payload.len() as u64;
        }
        self.publish_progress();
        Ok(())
    }

    /// Writes and commits a checkpoint if one is due and may be attempted now.
    ///
    /// # Errors
    ///
    /// Returns the reason to stop if the checkpoint failed for good. A store that cannot be reached is not
    /// such a reason: the attempt is put off and `Ok` is returned.
    async fn checkpoint_if_due(&mut self) -> Result<(), RunnerError> {
        let now = Instant::now();
        if !self.is_checkpoint_due(now) || now < self.retry_not_before {
            return Ok(());
        }
        match self.checkpoint().await {
            Ok(()) => Ok(()),
            Err(RunnerError::Materializer(materializer_error)) if materializer_error.is_transient() => {
                self.put_checkpoint_off(&materializer_error.to_string());
                Ok(())
            }
            Err(RunnerError::Metadata(MetaError::Unavailable(unavailable))) => {
                self.put_checkpoint_off(&unavailable.to_string());
                Ok(())
            }
            Err(stop) => Err(stop),
        }
    }

    /// Whether there is something to checkpoint and a reason to do it now.
    fn is_checkpoint_due(&self, now: Instant) -> bool {
        if self.applied_sequence == self.committed_checkpoint {
            return false;
        }
        self.bytes_since_checkpoint >= self.settings.checkpoint_after_bytes
            || now >= self.checkpoint_due_at
            || self.materializer.wants_checkpoint()
    }

    /// When the loop must stop waiting for records in order to checkpoint, or `None` if only new records
    /// can make a checkpoint due.
    fn next_wake_up(&self) -> Option<Instant> {
        if self.applied_sequence == self.committed_checkpoint {
            return None;
        }
        if self.is_checkpoint_due(Instant::now()) {
            // Due already and not attempted: the last attempt failed, so wait for the retry time.
            return Some(self.retry_not_before);
        }
        Some(self.checkpoint_due_at)
    }

    /// Remembers that a checkpoint attempt failed because a store could not be reached.
    fn put_checkpoint_off(&mut self, reason: &str) {
        tracing::warn!(
            materializer = %self.settings.materializer,
            reason,
            "checkpoint put off; it is tried again shortly"
        );
        self.retry_not_before = Instant::now() + self.settings.retry_interval;
    }

    /// Steps 4 and 5: has the materializer persist its state, stores the manifest, commits the pointer,
    /// and deletes what the new manifest no longer names.
    async fn checkpoint(&mut self) -> Result<(), RunnerError> {
        let checkpoint = self.applied_sequence;
        let manifest = self.materializer.write_checkpoint(&self.artifacts).await?;
        let stored_manifest = encode_manifest(checkpoint, &manifest);
        let manifest_key = self.artifacts.put_artifact(MANIFEST_LABEL, stored_manifest).await?;
        self.commit(checkpoint, &manifest_key).await?;

        self.committed_checkpoint = checkpoint;
        self.bytes_since_checkpoint = 0;
        self.checkpoint_due_at = Instant::now() + self.settings.checkpoint_after;
        self.publish_progress();
        tracing::info!(
            materializer = %self.settings.materializer,
            %checkpoint,
            artifacts = manifest.artifact_keys.len(),
            "checkpoint committed"
        );
        self.delete_what_the_manifest_dropped(manifest_key, manifest).await;
        Ok(())
    }

    /// Step 5 alone: points the checkpoint record at the manifest that was just stored. The write is valid
    /// only while the fencing token is still this runner's.
    ///
    /// # Errors
    ///
    /// Returns [`RunnerError::Fenced`] if another runner took the materializer over, and
    /// [`RunnerError::Metadata`] for anything else the metadata store refuses or cannot do.
    async fn commit(&mut self, checkpoint: SequenceNumber, manifest_key: &str) -> Result<(), RunnerError> {
        let committed = self
            .metadata
            .commit_checkpoint(
                self.stream_id,
                &self.settings.materializer,
                self.fencing_token,
                checkpoint,
                Some(manifest_key.to_owned()),
            )
            .await;
        match committed {
            Ok(()) => Ok(()),
            Err(MetaError::MaterializerFenced {
                held_token,
                current_token,
            }) => Err(RunnerError::Fenced {
                held_token,
                current_token,
            }),
            Err(meta_error) => Err(RunnerError::Metadata(meta_error)),
        }
    }

    /// Deletes every artifact of this runner and of the runners before it that the manifest just
    /// committed does not name. Nothing of this runner is in flight right after its own commit, so its
    /// own unnamed artifacts are garbage too. Failing to delete is not an error: the next sweep tries
    /// again.
    async fn delete_what_the_manifest_dropped(&mut self, manifest_key: String, manifest: CheckpointManifest) {
        let mut referenced: BTreeSet<String> = manifest.artifact_keys.into_iter().collect();
        referenced.insert(manifest_key);
        let swept = delete_unreferenced(&self.artifacts, &referenced, self.fencing_token + 1).await;
        if let Err(sweep_error) = swept {
            tracing::warn!(
                materializer = %self.settings.materializer,
                %sweep_error,
                "artifacts no longer in use could not be deleted"
            );
        }
    }

    /// Says why the runner stops when the log no longer holds its next record: because another runner
    /// took over and the log moved on with it, or, for the runner that still holds the token, because the
    /// log was truncated above its checkpoint.
    async fn stopped_by_truncation(&mut self, low_water_mark: SequenceNumber) -> RunnerError {
        let current = self
            .metadata
            .get_checkpoint(self.stream_id, &self.settings.materializer)
            .await;
        if let Ok(current) = current
            && current.fencing_token != self.fencing_token
        {
            return RunnerError::Fenced {
                held_token: self.fencing_token,
                current_token: current.fencing_token,
            };
        }
        RunnerError::LogTruncated {
            next_sequence: self.applied_sequence.next(),
            low_water_mark,
        }
    }

    /// Publishes where the runner is now.
    fn publish_progress(&self) {
        let durable_sequence = self.following.durable_sequence().max(self.applied_sequence);
        self.progress.publish(Progress {
            applied_sequence: self.applied_sequence,
            committed_checkpoint: self.committed_checkpoint,
            durable_sequence,
        });
    }
}

/// Step 2 of the start: reads the manifest the checkpoint record points at, if it points at one, and
/// restores the materializer from it.
async fn restore_materializer<Workload: Materializer>(
    artifacts: &ArtifactNamespace,
    checkpoint: &CheckpointRecord,
) -> Result<Workload, MaterializerError> {
    let last_checkpoint = match &checkpoint.manifest {
        Some(manifest_key) => Some(load_manifest(artifacts, manifest_key, checkpoint.checkpoint).await?),
        None => None,
    };
    Workload::restore_from_checkpoint(last_checkpoint, artifacts).await
}

/// Reads the manifest a checkpoint record points at and checks that it belongs to that checkpoint.
async fn load_manifest(
    artifacts: &ArtifactNamespace,
    manifest_key: &str,
    expected_checkpoint: SequenceNumber,
) -> Result<CheckpointManifest, MaterializerError> {
    let stored_manifest = artifacts.get_artifact(manifest_key).await?;
    let (checkpoint, manifest) = decode_manifest(&stored_manifest)?;
    if checkpoint != expected_checkpoint {
        return Err(MaterializerError::InvalidManifest {
            reason: format!("manifest {manifest_key:?} is of checkpoint {checkpoint}, not {expected_checkpoint}"),
        });
    }
    Ok(manifest)
}

#[cfg(test)]
#[path = "runner_test.rs"]
mod tests;
