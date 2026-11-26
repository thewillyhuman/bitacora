//! Materializers and their checkpoints.
//!
//! # Why this file exists
//!
//! A materializer turns the log into state and, from time to time, states durably how far it got: its checkpoint. The
//! log may delete records only below the lowest checkpoint of the materializers registered on a stream, so a checkpoint
//! is a promise in both directions: the materializer will not ask for anything below it again, and the log will keep
//! everything above it (design, sections 10 and 11). Such a promise must have exactly one author at a time, also when
//! two runners of the same materializer are started by mistake. This file holds the operations that make it so.
//!
//! # What it contains
//!
//! - `MetadataStore::register_materializer`, `MetadataStore::unregister_materializer`: whether a materializer's
//!   checkpoint holds the stream's low-water mark back.
//! - `MetadataStore::acquire_checkpoint`: a runner taking over a materializer.
//! - `MetadataStore::commit_checkpoint`: the runner stating how far it got.
//! - `MetadataStore::get_checkpoint`, `MetadataStore::list_checkpoints`: reads.
//! - `require_fencing_token`, `require_no_step_back`: the two rules a commit must pass, as pure functions over a
//!   checkpoint record.
//!
//! # How it works
//!
//! A checkpoint has a key of its own, `checkpoints/<stream id>/<materializer>`, so that committing one never contends
//! with the writer changing the stream record. The stream record only lists the names of the registered materializers.
//!
//! **Registering** adds the name to the stream record and creates the checkpoint in one transaction, conditional on the
//! stream record. The new checkpoint starts right below the stream's low-water mark: nothing earlier can be read any
//! more, and because the transaction is conditional on the stream record, the mark cannot move between reading it and
//! registering.
//!
//! **Fencing** works like a writer's epoch. A checkpoint carries a token. A runner takes over by raising the token by
//! one, and a commit is valid only while the token is still the one the runner took. Two runners can therefore be alive
//! at once, but only the last one to take the token can commit, and the other learns it from
//! [`MetaError::MaterializerFenced`] and stops.
//!
//! # Invariants
//!
//! - A registered materializer always has a checkpoint, and an unregistered one has none.
//! - A checkpoint never moves backwards.
//! - Only the holder of the current fencing token commits.
//! - A new checkpoint never lies at or above the stream's low-water mark.
//!
//! # See also
//!
//! - `../store.rs`: the read-modify-write shape and the helpers used here.
//! - `bitacora-materializer/src/runner.rs`: the caller.
//! - `bitacora-controller/src/controller/truncation.rs`: reads the checkpoints to move the low-water mark.

use bitacora_types::{SequenceNumber, StreamId};

use super::{MAX_UPDATE_ATTEMPTS, MetadataStore, delete_key, put_record, too_much_contention};
use crate::error::MetaError;
use crate::key_value::{Condition, TransactionOutcome};
use crate::keys::{checkpoint_key, checkpoints_key_prefix, stream_key, validate_name};
use crate::records::{CheckpointRecord, RECORD_SCHEMA_VERSION};

/// The fencing token of a checkpoint that no runner has taken yet. The first runner to take the materializer over holds
/// the token one above it.
const TOKEN_BEFORE_THE_FIRST_RUNNER: u64 = 0;

impl MetadataStore {
    /// Registers a materializer on a stream, so that its checkpoint holds the stream's low-water mark back.
    ///
    /// The checkpoint starts right below the current mark, because nothing earlier can be read any more. The name is
    /// added to the stream record and the checkpoint is created in one transaction. Registering a materializer that is
    /// registered already changes nothing and returns its checkpoint as it is.
    ///
    /// # Arguments
    ///
    /// * `stream_name` - The stream the materializer follows.
    /// * `materializer` - The materializer's name, unique within the stream: 1 to [`crate::MAX_NAME_LENGTH_BYTES`]
    ///   ASCII letters, digits, `.`, `_` or `-`.
    ///
    /// # Returns
    ///
    /// The materializer's checkpoint: the new one, or the existing one if it was registered before.
    ///
    /// # Errors
    ///
    /// * [`MetaError::InvalidName`] if the materializer's name cannot be used in a key.
    /// * [`MetaError::StreamNotFound`] if no stream has this name.
    /// * [`MetaError::TooMuchContention`] or [`MetaError::Unavailable`] if the store could not do it now.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RetentionPolicy};
    /// # use bitacora_types::{ReplicationSettings, SequenceNumber};
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// # metadata
    /// #     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    /// #     .await?;
    /// let checkpoint = metadata.register_materializer("orders", "pages").await?;
    ///
    /// let stream = metadata.get_stream("orders").await?;
    /// assert!(stream.materializers.contains("pages"));
    /// assert_eq!(checkpoint.checkpoint, SequenceNumber::ZERO);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn register_materializer(
        &self,
        stream_name: &str,
        materializer: &str,
    ) -> Result<CheckpointRecord, MetaError> {
        validate_name(materializer)?;
        for _attempt in 0..MAX_UPDATE_ATTEMPTS {
            let registered = self.try_register_materializer(stream_name, materializer).await?;
            if let Some(checkpoint) = registered {
                return Ok(checkpoint);
            }
        }
        Err(too_much_contention(&stream_key(stream_name)))
    }

    /// Makes one attempt to register a materializer: reads the stream record, and writes the record with the
    /// materializer's name and the new checkpoint if the record was not touched meanwhile.
    ///
    /// # Returns
    ///
    /// The materializer's checkpoint: the existing one if it was registered already, the new one if the write was
    /// applied. `None` if somebody else changed the stream record in between and the attempt must be repeated.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::StreamNotFound`] if no stream has this name, and [`MetaError::Unavailable`] if the store
    /// cannot be reached.
    async fn try_register_materializer(
        &self,
        stream_name: &str,
        materializer: &str,
    ) -> Result<Option<CheckpointRecord>, MetaError> {
        let stored = self.read_stream(stream_name).await?;
        let mut record = stored.record;
        if record.materializers.contains(materializer) {
            let existing_checkpoint = self.get_checkpoint(record.stream_id, materializer).await?;
            return Ok(Some(existing_checkpoint));
        }
        let checkpoint = new_checkpoint_record(materializer, record.low_water_mark);
        record.materializers.insert(materializer.to_owned());
        // One transaction, conditional on the stream record: the mark cannot advance between reading it and
        // registering.
        let stream_key = stream_key(stream_name);
        let stream_is_unchanged = Condition::HasRevision {
            key: stream_key.clone(),
            revision: stored.revision,
        };
        let mutations = vec![
            put_record(&stream_key, &record),
            put_record(&checkpoint_key(record.stream_id, materializer), &checkpoint),
        ];
        let outcome = self
            .key_value_store
            .transact(vec![stream_is_unchanged], mutations)
            .await?;
        match outcome {
            TransactionOutcome::Applied => Ok(Some(checkpoint)),
            TransactionOutcome::ConditionFailed => Ok(None),
        }
    }

    /// Unregisters a materializer and deletes its checkpoint, so that it no longer pins the log.
    ///
    /// The name is removed from the stream record and the checkpoint is deleted in one transaction. Unregistering a
    /// materializer that is not registered succeeds and changes nothing.
    ///
    /// # Arguments
    ///
    /// * `stream_name` - The stream the materializer follows.
    /// * `materializer` - The materializer's name.
    ///
    /// # Errors
    ///
    /// * [`MetaError::StreamNotFound`] if no stream has this name.
    /// * [`MetaError::TooMuchContention`] or [`MetaError::Unavailable`] if the store could not do it now.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RetentionPolicy};
    /// # use bitacora_types::ReplicationSettings;
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// # let stream = metadata
    /// #     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    /// #     .await?;
    /// metadata.register_materializer("orders", "pages").await?;
    ///
    /// metadata.unregister_materializer("orders", "pages").await?;
    ///
    /// assert!(metadata.list_checkpoints(stream.stream_id).await?.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn unregister_materializer(&self, stream_name: &str, materializer: &str) -> Result<(), MetaError> {
        let stream_key = stream_key(stream_name);
        for _attempt in 0..MAX_UPDATE_ATTEMPTS {
            let stored = self.read_stream(stream_name).await?;
            let mut record = stored.record;
            let was_registered = record.materializers.remove(materializer);
            if !was_registered {
                return Ok(());
            }
            let stream_is_unchanged = Condition::HasRevision {
                key: stream_key.clone(),
                revision: stored.revision,
            };
            let outcome = self
                .key_value_store
                .transact(
                    vec![stream_is_unchanged],
                    vec![
                        put_record(&stream_key, &record),
                        delete_key(&checkpoint_key(record.stream_id, materializer)),
                    ],
                )
                .await?;
            if outcome == TransactionOutcome::Applied {
                return Ok(());
            }
        }
        Err(too_much_contention(&stream_key))
    }

    /// Takes over a materializer by raising its fencing token.
    ///
    /// Whoever held the previous token can no longer commit. The caller resumes from the checkpoint returned and passes
    /// the token it carries to every commit.
    ///
    /// # Arguments
    ///
    /// * `stream_id` - The stream the materializer follows.
    /// * `materializer` - The materializer's name.
    ///
    /// # Returns
    ///
    /// The checkpoint as it is after the takeover: where to resume, the manifest of the artifacts up to there, and the
    /// fencing token the caller now holds.
    ///
    /// # Errors
    ///
    /// * [`MetaError::MaterializerNotRegistered`] if the materializer has no checkpoint on this stream.
    /// * [`MetaError::TooMuchContention`] or [`MetaError::Unavailable`] if the store could not do it now.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RetentionPolicy};
    /// # use bitacora_types::ReplicationSettings;
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// # let stream = metadata
    /// #     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    /// #     .await?;
    /// metadata.register_materializer("orders", "pages").await?;
    ///
    /// let first_runner = metadata.acquire_checkpoint(stream.stream_id, "pages").await?;
    /// let second_runner = metadata.acquire_checkpoint(stream.stream_id, "pages").await?;
    ///
    /// assert_eq!(first_runner.fencing_token, 1);
    /// assert_eq!(second_runner.fencing_token, 2);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn acquire_checkpoint(
        &self,
        stream_id: StreamId,
        materializer: &str,
    ) -> Result<CheckpointRecord, MetaError> {
        let checkpoint_key = checkpoint_key(stream_id, materializer);
        for _attempt in 0..MAX_UPDATE_ATTEMPTS {
            let stored = self.read_checkpoint(stream_id, materializer).await?;
            let mut checkpoint = stored.record;
            checkpoint.fencing_token += 1;
            let was_written = self
                .replace_record(&checkpoint_key, stored.revision, &checkpoint)
                .await?;
            if was_written {
                return Ok(checkpoint);
            }
        }
        Err(too_much_contention(&checkpoint_key))
    }

    /// Commits a checkpoint: "everything up to `checkpoint` is now in the artifacts `manifest` describes".
    ///
    /// The commit is valid only while `fencing_token` is still the materializer's current token. It is what lets the
    /// controller advance the stream's low-water mark. Committing the same checkpoint again succeeds.
    ///
    /// # Arguments
    ///
    /// * `stream_id` - The stream the materializer follows.
    /// * `materializer` - The materializer's name.
    /// * `fencing_token` - The token the caller got from [`MetadataStore::acquire_checkpoint`].
    /// * `checkpoint` - The last sequence number covered by the materializer's artifacts. It must not lie below the
    ///   checkpoint already committed.
    /// * `manifest` - Where the description of those artifacts is stored. Opaque to the log.
    ///
    /// # Errors
    ///
    /// * [`MetaError::MaterializerFenced`] if another runner took the materializer over. The caller must stop.
    /// * [`MetaError::InvalidState`] if the checkpoint would move backwards.
    /// * [`MetaError::MaterializerNotRegistered`] if the materializer has no checkpoint on this stream.
    /// * [`MetaError::TooMuchContention`] or [`MetaError::Unavailable`] if the store could not do it now.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RetentionPolicy};
    /// # use bitacora_types::{ReplicationSettings, SequenceNumber};
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// # let stream = metadata
    /// #     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    /// #     .await?;
    /// # metadata.register_materializer("orders", "pages").await?;
    /// let acquired = metadata.acquire_checkpoint(stream.stream_id, "pages").await?;
    ///
    /// metadata
    ///     .commit_checkpoint(
    ///         stream.stream_id,
    ///         "pages",
    ///         acquired.fencing_token,
    ///         SequenceNumber::new(40),
    ///         Some("manifests/40".to_owned()),
    ///     )
    ///     .await?;
    ///
    /// let committed = metadata.get_checkpoint(stream.stream_id, "pages").await?;
    /// assert_eq!(committed.checkpoint, SequenceNumber::new(40));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn commit_checkpoint(
        &self,
        stream_id: StreamId,
        materializer: &str,
        fencing_token: u64,
        checkpoint: SequenceNumber,
        manifest: Option<String>,
    ) -> Result<(), MetaError> {
        let checkpoint_key = checkpoint_key(stream_id, materializer);
        for _attempt in 0..MAX_UPDATE_ATTEMPTS {
            let stored = self.read_checkpoint(stream_id, materializer).await?;
            let mut record = stored.record;
            require_fencing_token(&record, fencing_token)?;
            require_no_step_back(&record, checkpoint)?;
            record.checkpoint = checkpoint;
            record.manifest.clone_from(&manifest);
            let was_written = self.replace_record(&checkpoint_key, stored.revision, &record).await?;
            if was_written {
                return Ok(());
            }
        }
        Err(too_much_contention(&checkpoint_key))
    }

    /// Reads one materializer's checkpoint.
    ///
    /// # Arguments
    ///
    /// * `stream_id` - The stream the materializer follows.
    /// * `materializer` - The materializer's name.
    ///
    /// # Returns
    ///
    /// The checkpoint as it is stored right now.
    ///
    /// # Errors
    ///
    /// * [`MetaError::MaterializerNotRegistered`] if the materializer has no checkpoint on this stream.
    /// * [`MetaError::CorruptRecord`] if the stored checkpoint cannot be decoded.
    /// * [`MetaError::Unavailable`] if the store cannot be reached.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetaError, MetadataStore, RetentionPolicy};
    /// # use bitacora_types::ReplicationSettings;
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// # let stream = metadata
    /// #     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    /// #     .await?;
    /// let never_registered = metadata.get_checkpoint(stream.stream_id, "pages").await;
    ///
    /// assert!(matches!(
    ///     never_registered,
    ///     Err(MetaError::MaterializerNotRegistered { .. })
    /// ));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_checkpoint(&self, stream_id: StreamId, materializer: &str) -> Result<CheckpointRecord, MetaError> {
        let stored = self.read_checkpoint(stream_id, materializer).await?;
        Ok(stored.record)
    }

    /// Lists the checkpoints of every materializer registered on a stream.
    ///
    /// The listing is one consistent view of the store.
    ///
    /// # Arguments
    ///
    /// * `stream_id` - The stream whose checkpoints are wanted.
    ///
    /// # Returns
    ///
    /// The checkpoints, sorted by materializer name. Empty if no materializer is registered, or if the stream does not
    /// exist.
    ///
    /// # Errors
    ///
    /// * [`MetaError::CorruptRecord`] if a stored checkpoint cannot be decoded.
    /// * [`MetaError::Unavailable`] if the store cannot be reached.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RetentionPolicy};
    /// # use bitacora_types::ReplicationSettings;
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// # let stream = metadata
    /// #     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    /// #     .await?;
    /// for materializer in ["pages", "blocks"] {
    ///     metadata.register_materializer("orders", materializer).await?;
    /// }
    ///
    /// let checkpoints = metadata.list_checkpoints(stream.stream_id).await?;
    ///
    /// assert_eq!(checkpoints[0].materializer, "blocks");
    /// assert_eq!(checkpoints[1].materializer, "pages");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_checkpoints(&self, stream_id: StreamId) -> Result<Vec<CheckpointRecord>, MetaError> {
        self.list_records(&checkpoints_key_prefix(stream_id)).await
    }
}

/// Builds the checkpoint of a materializer that was just registered on a stream whose low-water mark is
/// `low_water_mark`: right below the mark, with no runner and no artifacts yet.
fn new_checkpoint_record(materializer: &str, low_water_mark: SequenceNumber) -> CheckpointRecord {
    CheckpointRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        materializer: materializer.to_owned(),
        checkpoint: low_water_mark.preceding(),
        fencing_token: TOKEN_BEFORE_THE_FIRST_RUNNER,
        manifest: None,
    }
}

/// Checks that the checkpoint's fencing token is the one the caller holds.
///
/// # Errors
///
/// Returns [`MetaError::MaterializerFenced`], with both tokens, if it is not.
fn require_fencing_token(record: &CheckpointRecord, held_token: u64) -> Result<(), MetaError> {
    if record.fencing_token == held_token {
        Ok(())
    } else {
        Err(MetaError::MaterializerFenced {
            held_token,
            current_token: record.fencing_token,
        })
    }
}

/// Checks that committing `checkpoint` does not move the stored checkpoint backwards.
///
/// # Errors
///
/// Returns [`MetaError::InvalidState`] if `checkpoint` lies below the checkpoint already committed.
fn require_no_step_back(record: &CheckpointRecord, checkpoint: SequenceNumber) -> Result<(), MetaError> {
    if checkpoint < record.checkpoint {
        return Err(MetaError::InvalidState {
            reason: format!("checkpoint cannot move back from {} to {checkpoint}", record.checkpoint),
        });
    }
    Ok(())
}

#[cfg(test)]
#[path = "checkpoints_test.rs"]
mod tests;
