//! Typed metadata operations, each one a conditional write.
//!
//! Every mutation by a writer is conditional on "the epoch is still mine",
//! and every mutation by a materializer on its fencing token. This is what
//! makes a paused or partitioned process harmless when it wakes up (design,
//! section 8).

use std::collections::BTreeSet;
use std::sync::Arc;

use bitacora_types::{
    Epoch, ReplicaId, ReplicationSettings, SegmentId, SequenceNumber, StreamId, Timestamp,
};
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::key_value::{
    Condition, KeyValueStore, Mutation, Revision, StoreUnavailable, TransactionOutcome,
};
use crate::records::{
    CheckpointRecord, CopyRecord, CopyState, LeaderRecord, RECORD_SCHEMA_VERSION, ReplicaRecord,
    RetentionPolicy, SealRecord, SegmentRecord, StreamRecord, WriterLease,
};

/// How many times a read-modify-write is retried when other writers keep
/// changing the same key. Conflicts are rare: a stream's key changes on
/// rolls, failovers and controller housekeeping only.
const MAX_UPDATE_ATTEMPTS: u32 = 32;

/// The longest stream or materializer name accepted.
pub const MAX_NAME_LENGTH_BYTES: usize = 255;

const STREAM_KEY_PREFIX: &str = "streams/";
const CHECKPOINT_KEY_PREFIX: &str = "checkpoints/";
const REPLICA_KEY_PREFIX: &str = "replicas/";
const STREAM_ID_COUNTER_KEY: &str = "counters/stream-id";
const CONTROLLER_LEADER_KEY: &str = "controller/leader";

/// Why a metadata operation failed.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum MetaError {
    /// The store could not be reached. The operation may or may not have
    /// been applied; every operation here is safe to repeat.
    #[error(transparent)]
    Unavailable(#[from] StoreUnavailable),
    /// No stream has this name.
    #[error("stream {name:?} does not exist")]
    StreamNotFound {
        /// The name that was looked up.
        name: String,
    },
    /// A stream with this name exists already.
    #[error("stream {name:?} already exists")]
    StreamAlreadyExists {
        /// The name that is taken.
        name: String,
    },
    /// A stream or materializer name cannot be used in a key.
    #[error(
        "invalid name {name:?}: use 1 to {MAX_NAME_LENGTH_BYTES} ASCII letters, digits, '.', '_' or '-'"
    )]
    InvalidName {
        /// The rejected name.
        name: String,
    },
    /// Another writer has taken the stream. The caller must stop.
    #[error("epoch {held_epoch} was superseded by epoch {current_epoch}")]
    Fenced {
        /// The epoch the caller holds.
        held_epoch: Epoch,
        /// The stream's current epoch.
        current_epoch: Epoch,
    },
    /// The controller has not pre-allocated a replica set for the next
    /// segment yet.
    #[error("stream {name:?} has no standby segment to roll into")]
    NoStandbySegment {
        /// The stream that cannot roll.
        name: String,
    },
    /// The request contradicts what the store holds.
    #[error("invalid metadata operation: {reason}")]
    InvalidState {
        /// What the contradiction is.
        reason: String,
    },
    /// The materializer is not registered on the stream.
    #[error("materializer {materializer:?} is not registered on stream {stream_id}")]
    MaterializerNotRegistered {
        /// The stream that was looked at.
        stream_id: StreamId,
        /// The materializer that was looked up.
        materializer: String,
    },
    /// Another runner has taken over the materializer. The caller must stop.
    #[error("materializer fencing token {held_token} was superseded by {current_token}")]
    MaterializerFenced {
        /// The token the caller holds.
        held_token: u64,
        /// The materializer's current token.
        current_token: u64,
    },
    /// A stored record cannot be decoded.
    #[error("record at {key:?} cannot be decoded: {reason}")]
    CorruptRecord {
        /// The key of the record.
        key: String,
        /// Why decoding failed.
        reason: String,
    },
    /// Other writers changed the same key on every attempt.
    #[error("gave up updating {key:?} after {attempts} conflicting writes")]
    TooMuchContention {
        /// The contended key.
        key: String,
        /// How many attempts were made.
        attempts: u32,
    },
}

/// The metadata store, as bitacora uses it.
#[derive(Clone)]
pub struct MetadataStore {
    key_value_store: Arc<dyn KeyValueStore>,
}

impl MetadataStore {
    /// Wraps a key-value backend.
    #[must_use]
    pub fn new(key_value_store: Arc<dyn KeyValueStore>) -> Self {
        Self { key_value_store }
    }

    // ---- streams ----

    /// Creates a stream with no segments and epoch zero.
    ///
    /// `standby_replicas` is the replica set of its first segment, if the
    /// caller already placed it; otherwise the controller provides one.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::StreamAlreadyExists`] if the name is taken, or
    /// [`MetaError::InvalidName`] if it cannot be used in a key.
    pub async fn create_stream(
        &self,
        name: &str,
        replication: ReplicationSettings,
        retention: RetentionPolicy,
        standby_replicas: Option<Vec<ReplicaId>>,
    ) -> Result<StreamRecord, MetaError> {
        validate_name(name)?;
        let stream_key = stream_key(name);
        for _attempt in 0..MAX_UPDATE_ATTEMPTS {
            if self.key_value_store.get(&stream_key).await?.is_some() {
                return Err(MetaError::StreamAlreadyExists {
                    name: name.to_owned(),
                });
            }
            let counter = self.key_value_store.get(STREAM_ID_COUNTER_KEY).await?;
            let (last_stream_id, counter_condition) = match &counter {
                Some(stored) => (
                    decode::<u64>(STREAM_ID_COUNTER_KEY, &stored.value)?,
                    Condition::HasRevision {
                        key: STREAM_ID_COUNTER_KEY.to_owned(),
                        revision: stored.revision,
                    },
                ),
                None => (
                    0,
                    Condition::IsAbsent {
                        key: STREAM_ID_COUNTER_KEY.to_owned(),
                    },
                ),
            };
            let stream_id = last_stream_id + 1;
            let record = StreamRecord {
                schema_version: RECORD_SCHEMA_VERSION,
                stream_id: StreamId::new(stream_id),
                name: name.to_owned(),
                replication,
                retention,
                epoch: Epoch::ZERO,
                writer: None,
                segments: Vec::new(),
                standby_replicas: standby_replicas.clone(),
                truncate_before: None,
                low_water_mark: SequenceNumber::FIRST,
                materializers: BTreeSet::new(),
            };
            let outcome = self
                .key_value_store
                .transact(
                    vec![
                        counter_condition,
                        Condition::IsAbsent {
                            key: stream_key.clone(),
                        },
                    ],
                    vec![
                        put(STREAM_ID_COUNTER_KEY, &stream_id),
                        put(&stream_key, &record),
                    ],
                )
                .await?;
            if outcome == TransactionOutcome::Applied {
                return Ok(record);
            }
        }
        Err(too_much_contention(&stream_key))
    }

    /// Reads a stream's record.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::StreamNotFound`] if no stream has this name.
    pub async fn get_stream(&self, name: &str) -> Result<StreamRecord, MetaError> {
        Ok(self.read_stream(name).await?.0)
    }

    /// Lists every stream, sorted by name.
    ///
    /// # Errors
    ///
    /// Fails if the store is unavailable or holds an undecodable record.
    pub async fn list_streams(&self) -> Result<Vec<StreamRecord>, MetaError> {
        self.key_value_store
            .list(STREAM_KEY_PREFIX)
            .await?
            .iter()
            .map(|(key, stored)| decode(key, &stored.value))
            .collect()
    }

    /// Deletes a stream's record and its checkpoints. The controller then
    /// removes the segments left on replicas and in the object store.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::StreamNotFound`] if no stream has this name.
    pub async fn delete_stream(&self, name: &str) -> Result<StreamRecord, MetaError> {
        let stream_key = stream_key(name);
        for _attempt in 0..MAX_UPDATE_ATTEMPTS {
            let (record, revision) = self.read_stream(name).await?;
            let mut mutations = vec![Mutation::Delete {
                key: stream_key.clone(),
            }];
            for materializer in &record.materializers {
                mutations.push(Mutation::Delete {
                    key: checkpoint_key(record.stream_id, materializer),
                });
            }
            let outcome = self
                .key_value_store
                .transact(
                    vec![Condition::HasRevision {
                        key: stream_key.clone(),
                        revision,
                    }],
                    mutations,
                )
                .await?;
            if outcome == TransactionOutcome::Applied {
                return Ok(record);
            }
        }
        Err(too_much_contention(&stream_key))
    }

    /// Reads a stream's record, lets `update` change it, and writes it back
    /// only if nobody else wrote it in between, retrying otherwise.
    ///
    /// `update` may run several times and must not have side effects. It
    /// aborts the whole operation by returning an error. If it leaves the
    /// record unchanged, nothing is written.
    ///
    /// # Errors
    ///
    /// Returns whatever `update` returns, [`MetaError::StreamNotFound`], or
    /// [`MetaError::TooMuchContention`] after too many conflicting writes.
    pub async fn update_stream<Output>(
        &self,
        name: &str,
        mut update: impl FnMut(&mut StreamRecord) -> Result<Output, MetaError> + Send,
    ) -> Result<(StreamRecord, Output), MetaError> {
        let stream_key = stream_key(name);
        for _attempt in 0..MAX_UPDATE_ATTEMPTS {
            let (stored_record, revision) = self.read_stream(name).await?;
            let mut updated_record = stored_record.clone();
            let output = update(&mut updated_record)?;
            if updated_record == stored_record {
                return Ok((stored_record, output));
            }
            updated_record.schema_version = RECORD_SCHEMA_VERSION;
            let outcome = self
                .key_value_store
                .transact(
                    vec![Condition::HasRevision {
                        key: stream_key.clone(),
                        revision,
                    }],
                    vec![put(&stream_key, &updated_record)],
                )
                .await?;
            if outcome == TransactionOutcome::Applied {
                return Ok((updated_record, output));
            }
        }
        Err(too_much_contention(&stream_key))
    }

    // ---- writers ----

    /// Claims the stream for a new writer by raising its epoch from
    /// `expected_epoch` to the next one (design, section 9, step 1).
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::Fenced`] if the epoch is no longer
    /// `expected_epoch`: someone else is ahead and the caller must stop.
    pub async fn claim_epoch(
        &self,
        name: &str,
        expected_epoch: Epoch,
        writer_identity: &str,
        lease_expires_at: Timestamp,
    ) -> Result<StreamRecord, MetaError> {
        let (record, ()) = self
            .update_stream(name, |record| {
                require_epoch(record, expected_epoch)?;
                record.epoch = expected_epoch.next();
                record.writer = Some(WriterLease {
                    writer_identity: writer_identity.to_owned(),
                    expires_at: lease_expires_at,
                });
                Ok(())
            })
            .await?;
        Ok(record)
    }

    /// Extends the writer's lease.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::Fenced`] if `epoch` is no longer current.
    pub async fn renew_lease(
        &self,
        name: &str,
        epoch: Epoch,
        lease_expires_at: Timestamp,
    ) -> Result<(), MetaError> {
        self.update_stream(name, |record| {
            require_epoch(record, epoch)?;
            if let Some(lease) = &mut record.writer {
                lease.expires_at = lease_expires_at;
            }
            Ok(())
        })
        .await?;
        Ok(())
    }

    /// Gives the lease up, so that the next writer does not wait for it to
    /// lapse. The epoch stays: it only ever grows.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::Fenced`] if `epoch` is no longer current.
    pub async fn release_lease(&self, name: &str, epoch: Epoch) -> Result<(), MetaError> {
        self.update_stream(name, |record| {
            require_epoch(record, epoch)?;
            record.writer = None;
            Ok(())
        })
        .await?;
        Ok(())
    }

    /// Records that the stream's last segment ends at `sealed_end` (design,
    /// section 9, step 7). Sealing again at the same end succeeds.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::Fenced`] if `epoch` is no longer current, or
    /// [`MetaError::InvalidState`] if `segment_id` is not the last segment
    /// or is already sealed elsewhere.
    pub async fn seal_segment(
        &self,
        name: &str,
        epoch: Epoch,
        segment_id: SegmentId,
        sealed_end: SequenceNumber,
        sealed_at: Timestamp,
    ) -> Result<StreamRecord, MetaError> {
        let (record, ()) = self
            .update_stream(name, |record| {
                require_epoch(record, epoch)?;
                seal_last_segment(record, segment_id, sealed_end, sealed_at)
            })
            .await?;
        Ok(record)
    }

    /// Opens the next segment on the standby replica set (design, section
    /// 9, step 8). If the last segment is already open under `epoch`, it is
    /// returned as it is, so a repeated call opens only one segment.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::Fenced`] if `epoch` is no longer current,
    /// [`MetaError::NoStandbySegment`] if the controller has not placed the
    /// next segment yet, or [`MetaError::InvalidState`] if the last segment
    /// is still open under an older epoch.
    pub async fn open_segment(
        &self,
        name: &str,
        epoch: Epoch,
    ) -> Result<(StreamRecord, SegmentRecord), MetaError> {
        self.update_stream(name, |record| {
            require_epoch(record, epoch)?;
            open_next_segment(record, epoch)
        })
        .await
    }

    /// Seals the last segment at `sealed_end` and opens the next one, in one
    /// write (design, section 5, "A slow or failed replica", step 2). A
    /// repeated call returns the segment the first one opened.
    ///
    /// # Errors
    ///
    /// As [`MetadataStore::seal_segment`] and [`MetadataStore::open_segment`].
    pub async fn roll_segment(
        &self,
        name: &str,
        epoch: Epoch,
        segment_id: SegmentId,
        sealed_end: SequenceNumber,
        sealed_at: Timestamp,
    ) -> Result<(StreamRecord, SegmentRecord), MetaError> {
        self.update_stream(name, |record| {
            require_epoch(record, epoch)?;
            let was_rolled_already = record.last_segment().is_some_and(|last_segment| {
                last_segment.segment_id == segment_id.next()
                    && last_segment.epoch == epoch
                    && last_segment.is_open()
            });
            if !was_rolled_already {
                seal_last_segment(record, segment_id, sealed_end, sealed_at)?;
            }
            open_next_segment(record, epoch)
        })
        .await
    }

    // ---- materializers ----

    /// Registers a materializer, so that its checkpoint holds the stream's
    /// low-water mark back. It starts right below the current mark, because
    /// nothing earlier can be read any more. Registering twice returns the
    /// existing checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::StreamNotFound`] or [`MetaError::InvalidName`].
    pub async fn register_materializer(
        &self,
        stream_name: &str,
        materializer: &str,
    ) -> Result<CheckpointRecord, MetaError> {
        validate_name(materializer)?;
        let stream_key = stream_key(stream_name);
        for _attempt in 0..MAX_UPDATE_ATTEMPTS {
            let (mut record, revision) = self.read_stream(stream_name).await?;
            let checkpoint_key = checkpoint_key(record.stream_id, materializer);
            if record.materializers.contains(materializer) {
                return self
                    .read_checkpoint(record.stream_id, materializer)
                    .await
                    .map(|(checkpoint, _revision)| checkpoint);
            }
            let checkpoint = CheckpointRecord {
                schema_version: RECORD_SCHEMA_VERSION,
                materializer: materializer.to_owned(),
                checkpoint: record.low_water_mark.preceding(),
                fencing_token: 0,
                manifest: None,
            };
            record.materializers.insert(materializer.to_owned());
            // One transaction, conditional on the stream record: the mark
            // cannot advance between reading it and registering.
            let outcome = self
                .key_value_store
                .transact(
                    vec![Condition::HasRevision {
                        key: stream_key.clone(),
                        revision,
                    }],
                    vec![put(&stream_key, &record), put(&checkpoint_key, &checkpoint)],
                )
                .await?;
            if outcome == TransactionOutcome::Applied {
                return Ok(checkpoint);
            }
        }
        Err(too_much_contention(&stream_key))
    }

    /// Unregisters a materializer and deletes its checkpoint, so that it no
    /// longer pins the log. Unregistering an unknown materializer succeeds.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::StreamNotFound`] if the stream does not exist.
    pub async fn unregister_materializer(
        &self,
        stream_name: &str,
        materializer: &str,
    ) -> Result<(), MetaError> {
        let stream_key = stream_key(stream_name);
        for _attempt in 0..MAX_UPDATE_ATTEMPTS {
            let (mut record, revision) = self.read_stream(stream_name).await?;
            if !record.materializers.remove(materializer) {
                return Ok(());
            }
            let outcome = self
                .key_value_store
                .transact(
                    vec![Condition::HasRevision {
                        key: stream_key.clone(),
                        revision,
                    }],
                    vec![
                        put(&stream_key, &record),
                        Mutation::Delete {
                            key: checkpoint_key(record.stream_id, materializer),
                        },
                    ],
                )
                .await?;
            if outcome == TransactionOutcome::Applied {
                return Ok(());
            }
        }
        Err(too_much_contention(&stream_key))
    }

    /// Takes over a materializer by raising its fencing token, and returns
    /// the checkpoint to resume from. Whoever held the previous token can no
    /// longer commit.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::MaterializerNotRegistered`] if it is unknown.
    pub async fn acquire_checkpoint(
        &self,
        stream_id: StreamId,
        materializer: &str,
    ) -> Result<CheckpointRecord, MetaError> {
        let checkpoint_key = checkpoint_key(stream_id, materializer);
        for _attempt in 0..MAX_UPDATE_ATTEMPTS {
            let (mut checkpoint, revision) = self.read_checkpoint(stream_id, materializer).await?;
            checkpoint.fencing_token += 1;
            if self.replace(&checkpoint_key, revision, &checkpoint).await? {
                return Ok(checkpoint);
            }
        }
        Err(too_much_contention(&checkpoint_key))
    }

    /// Commits a checkpoint: "everything up to `checkpoint` is now in the
    /// artifacts `manifest` describes". Valid only while `fencing_token` is
    /// current. This is what lets the stream's low-water mark advance.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::MaterializerFenced`] if another runner took
    /// over, or [`MetaError::InvalidState`] if the checkpoint would move
    /// backwards.
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
            let (mut record, revision) = self.read_checkpoint(stream_id, materializer).await?;
            if record.fencing_token != fencing_token {
                return Err(MetaError::MaterializerFenced {
                    held_token: fencing_token,
                    current_token: record.fencing_token,
                });
            }
            if checkpoint < record.checkpoint {
                return Err(MetaError::InvalidState {
                    reason: format!(
                        "checkpoint cannot move back from {} to {checkpoint}",
                        record.checkpoint
                    ),
                });
            }
            record.checkpoint = checkpoint;
            record.manifest.clone_from(&manifest);
            if self.replace(&checkpoint_key, revision, &record).await? {
                return Ok(());
            }
        }
        Err(too_much_contention(&checkpoint_key))
    }

    /// Reads one materializer's checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::MaterializerNotRegistered`] if it is unknown.
    pub async fn get_checkpoint(
        &self,
        stream_id: StreamId,
        materializer: &str,
    ) -> Result<CheckpointRecord, MetaError> {
        Ok(self.read_checkpoint(stream_id, materializer).await?.0)
    }

    /// Lists the checkpoints of every materializer registered on a stream.
    ///
    /// # Errors
    ///
    /// Fails if the store is unavailable or holds an undecodable record.
    pub async fn list_checkpoints(
        &self,
        stream_id: StreamId,
    ) -> Result<Vec<CheckpointRecord>, MetaError> {
        self.key_value_store
            .list(&format!("{CHECKPOINT_KEY_PREFIX}{stream_id}/"))
            .await?
            .iter()
            .map(|(key, stored)| decode(key, &stored.value))
            .collect()
    }

    // ---- replica registry ----

    /// Registers a replica or renews its registration.
    ///
    /// # Errors
    ///
    /// Fails if the store is unavailable.
    pub async fn put_replica(&self, replica: &ReplicaRecord) -> Result<(), MetaError> {
        self.key_value_store
            .transact(
                Vec::new(),
                vec![put(&replica_key(&replica.replica_id), replica)],
            )
            .await?;
        Ok(())
    }

    /// Removes a replica from the registry, after which repair re-creates
    /// its sealed copies elsewhere.
    ///
    /// # Errors
    ///
    /// Fails if the store is unavailable.
    pub async fn remove_replica(&self, replica_id: &ReplicaId) -> Result<(), MetaError> {
        self.key_value_store
            .transact(
                Vec::new(),
                vec![Mutation::Delete {
                    key: replica_key(replica_id),
                }],
            )
            .await?;
        Ok(())
    }

    /// Lists every registered replica, sorted by identifier.
    ///
    /// # Errors
    ///
    /// Fails if the store is unavailable or holds an undecodable record.
    pub async fn list_replicas(&self) -> Result<Vec<ReplicaRecord>, MetaError> {
        self.key_value_store
            .list(REPLICA_KEY_PREFIX)
            .await?
            .iter()
            .map(|(key, stored)| decode(key, &stored.value))
            .collect()
    }

    // ---- controller leadership ----

    /// Tries to become, or stay, the active controller until `expires_at`.
    /// Succeeds if nobody leads, the previous leadership lapsed before
    /// `now`, or `candidate` leads already.
    ///
    /// Leadership is a liveness optimization: every controller write is
    /// itself conditional, so two controllers that both believe they lead
    /// cannot corrupt anything.
    ///
    /// # Errors
    ///
    /// Fails if the store is unavailable or holds an undecodable record.
    pub async fn try_lead(
        &self,
        candidate: &str,
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<bool, MetaError> {
        let claim = LeaderRecord {
            holder: candidate.to_owned(),
            expires_at,
        };
        let condition = match self.key_value_store.get(CONTROLLER_LEADER_KEY).await? {
            None => Condition::IsAbsent {
                key: CONTROLLER_LEADER_KEY.to_owned(),
            },
            Some(stored) => {
                let leader: LeaderRecord = decode(CONTROLLER_LEADER_KEY, &stored.value)?;
                if leader.holder != candidate && leader.expires_at > now {
                    return Ok(false);
                }
                Condition::HasRevision {
                    key: CONTROLLER_LEADER_KEY.to_owned(),
                    revision: stored.revision,
                }
            }
        };
        let outcome = self
            .key_value_store
            .transact(vec![condition], vec![put(CONTROLLER_LEADER_KEY, &claim)])
            .await?;
        Ok(outcome == TransactionOutcome::Applied)
    }

    /// Reads which controller instance leads, if any ever did. The record
    /// stays after its leadership lapsed; compare `expires_at` with the
    /// clock to tell.
    ///
    /// # Errors
    ///
    /// Fails if the store is unavailable or holds an undecodable record.
    pub async fn get_leader(&self) -> Result<Option<LeaderRecord>, MetaError> {
        self.key_value_store
            .get(CONTROLLER_LEADER_KEY)
            .await?
            .map(|stored| decode(CONTROLLER_LEADER_KEY, &stored.value))
            .transpose()
    }

    // ---- helpers ----

    async fn read_stream(&self, name: &str) -> Result<(StreamRecord, Revision), MetaError> {
        let stream_key = stream_key(name);
        let Some(stored) = self.key_value_store.get(&stream_key).await? else {
            return Err(MetaError::StreamNotFound {
                name: name.to_owned(),
            });
        };
        Ok((decode(&stream_key, &stored.value)?, stored.revision))
    }

    async fn read_checkpoint(
        &self,
        stream_id: StreamId,
        materializer: &str,
    ) -> Result<(CheckpointRecord, Revision), MetaError> {
        let checkpoint_key = checkpoint_key(stream_id, materializer);
        let Some(stored) = self.key_value_store.get(&checkpoint_key).await? else {
            return Err(MetaError::MaterializerNotRegistered {
                stream_id,
                materializer: materializer.to_owned(),
            });
        };
        Ok((decode(&checkpoint_key, &stored.value)?, stored.revision))
    }

    /// Replaces `key` with `record` if it is still at `revision`. Returns
    /// whether the write was applied.
    async fn replace<Record: Serialize + Sync>(
        &self,
        key: &str,
        revision: Revision,
        record: &Record,
    ) -> Result<bool, MetaError> {
        let outcome = self
            .key_value_store
            .transact(
                vec![Condition::HasRevision {
                    key: key.to_owned(),
                    revision,
                }],
                vec![put(key, record)],
            )
            .await?;
        Ok(outcome == TransactionOutcome::Applied)
    }
}

fn stream_key(name: &str) -> String {
    format!("{STREAM_KEY_PREFIX}{name}")
}

fn checkpoint_key(stream_id: StreamId, materializer: &str) -> String {
    format!("{CHECKPOINT_KEY_PREFIX}{stream_id}/{materializer}")
}

fn replica_key(replica_id: &ReplicaId) -> String {
    format!("{REPLICA_KEY_PREFIX}{replica_id}")
}

fn validate_name(name: &str) -> Result<(), MetaError> {
    let is_allowed =
        |character: char| character.is_ascii_alphanumeric() || "._-".contains(character);
    if name.is_empty() || name.len() > MAX_NAME_LENGTH_BYTES || !name.chars().all(is_allowed) {
        return Err(MetaError::InvalidName {
            name: name.to_owned(),
        });
    }
    Ok(())
}

fn put<Record: Serialize>(key: &str, record: &Record) -> Mutation {
    Mutation::Put {
        key: key.to_owned(),
        value: Bytes::from(serde_json::to_vec(record).expect("metadata records serialize to JSON")),
    }
}

fn decode<Record: DeserializeOwned>(key: &str, stored_value: &[u8]) -> Result<Record, MetaError> {
    serde_json::from_slice(stored_value).map_err(|json_error| MetaError::CorruptRecord {
        key: key.to_owned(),
        reason: json_error.to_string(),
    })
}

fn too_much_contention(key: &str) -> MetaError {
    MetaError::TooMuchContention {
        key: key.to_owned(),
        attempts: MAX_UPDATE_ATTEMPTS,
    }
}

fn require_epoch(record: &StreamRecord, held_epoch: Epoch) -> Result<(), MetaError> {
    if record.epoch == held_epoch {
        Ok(())
    } else {
        Err(MetaError::Fenced {
            held_epoch,
            current_epoch: record.epoch,
        })
    }
}

fn seal_last_segment(
    record: &mut StreamRecord,
    segment_id: SegmentId,
    sealed_end: SequenceNumber,
    sealed_at: Timestamp,
) -> Result<(), MetaError> {
    let Some(last_segment) = record
        .segments
        .last_mut()
        .filter(|segment| segment.segment_id == segment_id)
    else {
        return Err(MetaError::InvalidState {
            reason: format!("segment {segment_id} is not the stream's last segment"),
        });
    };
    match last_segment.sealed {
        Some(existing_seal) if existing_seal.end == sealed_end => Ok(()),
        Some(existing_seal) => Err(MetaError::InvalidState {
            reason: format!(
                "segment {segment_id} is already sealed at {}, not {sealed_end}",
                existing_seal.end
            ),
        }),
        None if sealed_end < last_segment.first_sequence.preceding() => {
            Err(MetaError::InvalidState {
                reason: format!(
                    "segment {segment_id} starts at {} and cannot end at {sealed_end}",
                    last_segment.first_sequence
                ),
            })
        }
        None => {
            last_segment.sealed = Some(SealRecord {
                end: sealed_end,
                at: sealed_at,
            });
            Ok(())
        }
    }
}

fn open_next_segment(record: &mut StreamRecord, epoch: Epoch) -> Result<SegmentRecord, MetaError> {
    if let Some(open_segment) = record.open_segment() {
        if open_segment.epoch == epoch {
            return Ok(open_segment.clone());
        }
        return Err(MetaError::InvalidState {
            reason: format!(
                "segment {} is still open under epoch {}",
                open_segment.segment_id, open_segment.epoch
            ),
        });
    }
    let Some(standby_replicas) = record.standby_replicas.take() else {
        return Err(MetaError::NoStandbySegment {
            name: record.name.clone(),
        });
    };
    let opened_segment = SegmentRecord {
        segment_id: record
            .last_segment()
            .map_or(SegmentId::FIRST, |last_segment| {
                last_segment.segment_id.next()
            }),
        epoch,
        first_sequence: record.next_segment_first_sequence(),
        sealed: None,
        copies: standby_replicas
            .into_iter()
            .map(|replica_id| CopyRecord {
                replica_id,
                state: CopyState::Unverified,
            })
            .collect(),
        tiered: None,
    };
    record.segments.push(opened_segment.clone());
    Ok(opened_segment)
}

#[cfg(test)]
mod tests {
    use crate::memory_store::MemoryKeyValueStore;

    use super::*;

    const STREAM: &str = "orders";
    const LEASE_EXPIRY: Timestamp = Timestamp::from_unix_millis(10_000);
    const SEALED_AT: Timestamp = Timestamp::from_unix_millis(5_000);

    fn replica_ids(names: &[&str]) -> Vec<ReplicaId> {
        names
            .iter()
            .map(|name| ReplicaId::new(*name).expect("valid identifier"))
            .collect()
    }

    fn standby() -> Vec<ReplicaId> {
        replica_ids(&["replica-a", "replica-b", "replica-c"])
    }

    fn new_store() -> (MetadataStore, MemoryKeyValueStore) {
        let key_value_store = MemoryKeyValueStore::new();
        (
            MetadataStore::new(Arc::new(key_value_store.clone())),
            key_value_store,
        )
    }

    /// A store with stream "orders", standby placed, no writer yet.
    async fn store_with_stream() -> MetadataStore {
        let (store, _key_value_store) = new_store();
        store
            .create_stream(
                STREAM,
                ReplicationSettings::DEFAULT,
                RetentionPolicy::default(),
                Some(standby()),
            )
            .await
            .expect("stream is created");
        store
    }

    /// A store where a writer holds epoch 1 and segment 1 is open.
    async fn store_with_open_segment() -> MetadataStore {
        let store = store_with_stream().await;
        store
            .claim_epoch(STREAM, Epoch::ZERO, "compute-1", LEASE_EXPIRY)
            .await
            .expect("epoch is claimed");
        store
            .open_segment(STREAM, Epoch::new(1))
            .await
            .expect("segment is opened");
        store
    }

    async fn set_standby(store: &MetadataStore) {
        store
            .update_stream(STREAM, |record| {
                record.standby_replicas = Some(standby());
                Ok(())
            })
            .await
            .expect("standby is placed");
    }

    // ---- streams ----

    #[tokio::test]
    async fn new_stream_has_no_segments_and_no_writer() {
        let store = store_with_stream().await;
        let record = store.get_stream(STREAM).await.expect("stream exists");
        assert!(
            record.epoch == Epoch::ZERO
                && record.segments.is_empty()
                && record.writer.is_none()
                && record.low_water_mark == SequenceNumber::FIRST,
            "{record:?}"
        );
    }

    #[tokio::test]
    async fn streams_get_distinct_identifiers() {
        let store = store_with_stream().await;
        let second = store
            .create_stream(
                "payments",
                ReplicationSettings::DEFAULT,
                RetentionPolicy::default(),
                None,
            )
            .await
            .expect("stream is created");
        let first = store.get_stream(STREAM).await.expect("stream exists");
        assert_ne!(first.stream_id, second.stream_id);
    }

    #[tokio::test]
    async fn creating_a_stream_twice_fails() {
        let store = store_with_stream().await;
        let second_attempt = store
            .create_stream(
                STREAM,
                ReplicationSettings::DEFAULT,
                RetentionPolicy::default(),
                None,
            )
            .await;
        assert_eq!(
            second_attempt,
            Err(MetaError::StreamAlreadyExists {
                name: STREAM.to_owned()
            })
        );
    }

    #[tokio::test]
    async fn names_that_would_break_keys_are_refused() {
        let (store, _key_value_store) = new_store();
        for name in [
            "",
            "a/b",
            "with space",
            &"n".repeat(MAX_NAME_LENGTH_BYTES + 1),
        ] {
            let created = store
                .create_stream(
                    name,
                    ReplicationSettings::DEFAULT,
                    RetentionPolicy::default(),
                    None,
                )
                .await;
            assert!(
                matches!(created, Err(MetaError::InvalidName { .. })),
                "name {name:?} was accepted"
            );
        }
    }

    #[tokio::test]
    async fn reading_a_missing_stream_fails() {
        let (store, _key_value_store) = new_store();
        assert_eq!(
            store.get_stream(STREAM).await,
            Err(MetaError::StreamNotFound {
                name: STREAM.to_owned()
            })
        );
    }

    #[tokio::test]
    async fn streams_are_listed_sorted_by_name() {
        let store = store_with_stream().await;
        store
            .create_stream(
                "billing",
                ReplicationSettings::DEFAULT,
                RetentionPolicy::default(),
                None,
            )
            .await
            .expect("stream is created");
        let names: Vec<String> = store
            .list_streams()
            .await
            .expect("list")
            .into_iter()
            .map(|record| record.name)
            .collect();
        assert_eq!(names, ["billing", "orders"]);
    }

    #[tokio::test]
    async fn deleted_stream_takes_its_checkpoints_with_it() {
        let store = store_with_stream().await;
        let stream_id = store.get_stream(STREAM).await.expect("exists").stream_id;
        store
            .register_materializer(STREAM, "pages")
            .await
            .expect("registered");
        store.delete_stream(STREAM).await.expect("deleted");
        assert_eq!(
            (
                store.get_stream(STREAM).await.is_err(),
                store.list_checkpoints(stream_id).await
            ),
            (true, Ok(Vec::new()))
        );
    }

    #[tokio::test]
    async fn update_that_changes_nothing_writes_nothing() {
        let (store, key_value_store) = new_store();
        store
            .create_stream(
                STREAM,
                ReplicationSettings::DEFAULT,
                RetentionPolicy::default(),
                None,
            )
            .await
            .expect("stream is created");
        let revision_before = key_value_store
            .get("streams/orders")
            .await
            .expect("get")
            .map(|stored| stored.revision);
        store
            .update_stream(STREAM, |_record| Ok(()))
            .await
            .expect("update");
        let revision_after = key_value_store
            .get("streams/orders")
            .await
            .expect("get")
            .map(|stored| stored.revision);
        assert_eq!(revision_before, revision_after);
    }

    #[tokio::test]
    async fn update_fails_when_the_store_is_unavailable() {
        let (store, key_value_store) = new_store();
        key_value_store.set_unavailable(true);
        assert!(matches!(
            store.get_stream(STREAM).await,
            Err(MetaError::Unavailable(_))
        ));
    }

    // ---- epochs and leases ----

    #[tokio::test]
    async fn claiming_raises_the_epoch_by_one() {
        let store = store_with_stream().await;
        let record = store
            .claim_epoch(STREAM, Epoch::ZERO, "compute-1", LEASE_EXPIRY)
            .await
            .expect("epoch is claimed");
        assert_eq!(
            (record.epoch, record.writer),
            (
                Epoch::new(1),
                Some(WriterLease {
                    writer_identity: "compute-1".to_owned(),
                    expires_at: LEASE_EXPIRY,
                })
            )
        );
    }

    #[tokio::test]
    async fn second_claim_from_the_same_epoch_loses() {
        let store = store_with_stream().await;
        store
            .claim_epoch(STREAM, Epoch::ZERO, "compute-1", LEASE_EXPIRY)
            .await
            .expect("epoch is claimed");
        let late_claim = store
            .claim_epoch(STREAM, Epoch::ZERO, "compute-2", LEASE_EXPIRY)
            .await;
        assert_eq!(
            late_claim,
            Err(MetaError::Fenced {
                held_epoch: Epoch::ZERO,
                current_epoch: Epoch::new(1),
            })
        );
    }

    #[tokio::test]
    async fn superseded_writer_cannot_renew_its_lease() {
        let store = store_with_open_segment().await;
        store
            .claim_epoch(STREAM, Epoch::new(1), "compute-2", LEASE_EXPIRY)
            .await
            .expect("epoch is claimed");
        let renewal = store
            .renew_lease(STREAM, Epoch::new(1), Timestamp::from_unix_millis(99_000))
            .await;
        assert!(matches!(renewal, Err(MetaError::Fenced { .. })));
    }

    #[tokio::test]
    async fn renewal_extends_the_lease() {
        let store = store_with_open_segment().await;
        let extended_expiry = Timestamp::from_unix_millis(99_000);
        store
            .renew_lease(STREAM, Epoch::new(1), extended_expiry)
            .await
            .expect("lease is renewed");
        let record = store.get_stream(STREAM).await.expect("stream exists");
        assert_eq!(
            record.writer.map(|lease| lease.expires_at),
            Some(extended_expiry)
        );
    }

    #[tokio::test]
    async fn released_lease_keeps_the_epoch() {
        let store = store_with_open_segment().await;
        store
            .release_lease(STREAM, Epoch::new(1))
            .await
            .expect("lease is released");
        let record = store.get_stream(STREAM).await.expect("stream exists");
        assert_eq!((record.writer, record.epoch), (None, Epoch::new(1)));
    }

    // ---- segments ----

    #[tokio::test]
    async fn first_segment_opens_on_the_standby_replicas() {
        let store = store_with_stream().await;
        store
            .claim_epoch(STREAM, Epoch::ZERO, "compute-1", LEASE_EXPIRY)
            .await
            .expect("epoch is claimed");
        let (record, opened) = store
            .open_segment(STREAM, Epoch::new(1))
            .await
            .expect("segment is opened");
        let opened_on: Vec<ReplicaId> = opened
            .copies
            .iter()
            .map(|copy| copy.replica_id.clone())
            .collect();
        assert!(
            opened.segment_id == SegmentId::FIRST
                && opened.first_sequence == SequenceNumber::FIRST
                && opened.epoch == Epoch::new(1)
                && opened_on == standby()
                && record.standby_replicas.is_none(),
            "{opened:?}"
        );
    }

    #[tokio::test]
    async fn opening_twice_under_one_epoch_opens_one_segment() {
        let store = store_with_open_segment().await;
        let (record, reopened) = store
            .open_segment(STREAM, Epoch::new(1))
            .await
            .expect("segment is returned");
        assert_eq!(
            (record.segments.len(), reopened.segment_id),
            (1, SegmentId::FIRST)
        );
    }

    #[tokio::test]
    async fn opening_without_a_standby_fails() {
        let store = store_with_open_segment().await;
        store
            .seal_segment(
                STREAM,
                Epoch::new(1),
                SegmentId::FIRST,
                SequenceNumber::new(50),
                SEALED_AT,
            )
            .await
            .expect("segment is sealed");
        assert_eq!(
            store.open_segment(STREAM, Epoch::new(1)).await,
            Err(MetaError::NoStandbySegment {
                name: STREAM.to_owned()
            })
        );
    }

    #[tokio::test]
    async fn new_writer_cannot_open_while_the_old_segment_is_unsealed() {
        let store = store_with_open_segment().await;
        store
            .claim_epoch(STREAM, Epoch::new(1), "compute-2", LEASE_EXPIRY)
            .await
            .expect("epoch is claimed");
        set_standby(&store).await;
        assert!(matches!(
            store.open_segment(STREAM, Epoch::new(2)).await,
            Err(MetaError::InvalidState { .. })
        ));
    }

    #[tokio::test]
    async fn sealing_records_the_end_and_the_time() {
        let store = store_with_open_segment().await;
        let record = store
            .seal_segment(
                STREAM,
                Epoch::new(1),
                SegmentId::FIRST,
                SequenceNumber::new(50),
                SEALED_AT,
            )
            .await
            .expect("segment is sealed");
        assert_eq!(
            record.segments[0].sealed,
            Some(SealRecord {
                end: SequenceNumber::new(50),
                at: SEALED_AT,
            })
        );
    }

    #[tokio::test]
    async fn sealing_again_at_the_same_end_succeeds() {
        let store = store_with_open_segment().await;
        for _attempt in 0..2 {
            store
                .seal_segment(
                    STREAM,
                    Epoch::new(1),
                    SegmentId::FIRST,
                    SequenceNumber::new(50),
                    SEALED_AT,
                )
                .await
                .expect("segment is sealed");
        }
    }

    #[tokio::test]
    async fn sealing_again_at_another_end_fails() {
        let store = store_with_open_segment().await;
        store
            .seal_segment(
                STREAM,
                Epoch::new(1),
                SegmentId::FIRST,
                SequenceNumber::new(50),
                SEALED_AT,
            )
            .await
            .expect("segment is sealed");
        let second_seal = store
            .seal_segment(
                STREAM,
                Epoch::new(1),
                SegmentId::FIRST,
                SequenceNumber::new(60),
                SEALED_AT,
            )
            .await;
        assert!(matches!(second_seal, Err(MetaError::InvalidState { .. })));
    }

    #[tokio::test]
    async fn segment_can_be_sealed_empty() {
        let store = store_with_open_segment().await;
        let record = store
            .seal_segment(
                STREAM,
                Epoch::new(1),
                SegmentId::FIRST,
                SequenceNumber::ZERO,
                SEALED_AT,
            )
            .await
            .expect("segment is sealed");
        assert_eq!(record.next_segment_first_sequence(), SequenceNumber::FIRST);
    }

    #[tokio::test]
    async fn sealing_below_the_segment_start_fails() {
        let store = store_with_open_segment().await;
        set_standby(&store).await;
        store
            .roll_segment(
                STREAM,
                Epoch::new(1),
                SegmentId::FIRST,
                SequenceNumber::new(50),
                SEALED_AT,
            )
            .await
            .expect("segment is rolled");
        let seal_below_start = store
            .seal_segment(
                STREAM,
                Epoch::new(1),
                SegmentId::new(2),
                SequenceNumber::new(40),
                SEALED_AT,
            )
            .await;
        assert!(matches!(
            seal_below_start,
            Err(MetaError::InvalidState { .. })
        ));
    }

    #[tokio::test]
    async fn superseded_writer_cannot_seal() {
        let store = store_with_open_segment().await;
        store
            .claim_epoch(STREAM, Epoch::new(1), "compute-2", LEASE_EXPIRY)
            .await
            .expect("epoch is claimed");
        let stale_seal = store
            .seal_segment(
                STREAM,
                Epoch::new(1),
                SegmentId::FIRST,
                SequenceNumber::new(50),
                SEALED_AT,
            )
            .await;
        assert_eq!(
            stale_seal,
            Err(MetaError::Fenced {
                held_epoch: Epoch::new(1),
                current_epoch: Epoch::new(2),
            })
        );
    }

    #[tokio::test]
    async fn roll_seals_and_opens_in_one_step() {
        let store = store_with_open_segment().await;
        set_standby(&store).await;
        let (record, opened) = store
            .roll_segment(
                STREAM,
                Epoch::new(1),
                SegmentId::FIRST,
                SequenceNumber::new(50),
                SEALED_AT,
            )
            .await
            .expect("segment is rolled");
        assert_eq!(
            (
                record.segments[0].sealed_end(),
                opened.segment_id,
                opened.first_sequence
            ),
            (
                Some(SequenceNumber::new(50)),
                SegmentId::new(2),
                SequenceNumber::new(51)
            )
        );
    }

    #[tokio::test]
    async fn failed_roll_leaves_the_segment_open() {
        let store = store_with_open_segment().await;
        let roll = store
            .roll_segment(
                STREAM,
                Epoch::new(1),
                SegmentId::FIRST,
                SequenceNumber::new(50),
                SEALED_AT,
            )
            .await;
        let record = store.get_stream(STREAM).await.expect("stream exists");
        assert!(
            matches!(roll, Err(MetaError::NoStandbySegment { .. })) && record.segments[0].is_open()
        );
    }

    #[tokio::test]
    async fn repeated_roll_returns_the_segment_already_opened() {
        let store = store_with_open_segment().await;
        set_standby(&store).await;
        let mut opened_segments = Vec::new();
        for _attempt in 0..2 {
            let (_record, opened) = store
                .roll_segment(
                    STREAM,
                    Epoch::new(1),
                    SegmentId::FIRST,
                    SequenceNumber::new(50),
                    SEALED_AT,
                )
                .await
                .expect("segment is rolled");
            opened_segments.push(opened.segment_id);
        }
        let record = store.get_stream(STREAM).await.expect("stream exists");
        assert_eq!(
            (opened_segments, record.segments.len()),
            (vec![SegmentId::new(2), SegmentId::new(2)], 2)
        );
    }

    #[tokio::test]
    async fn failover_seals_the_old_segment_and_opens_under_the_new_epoch() {
        let store = store_with_open_segment().await;
        store
            .claim_epoch(STREAM, Epoch::new(1), "compute-2", LEASE_EXPIRY)
            .await
            .expect("epoch is claimed");
        set_standby(&store).await;
        store
            .seal_segment(
                STREAM,
                Epoch::new(2),
                SegmentId::FIRST,
                SequenceNumber::new(77),
                SEALED_AT,
            )
            .await
            .expect("segment is sealed");
        let (_record, opened) = store
            .open_segment(STREAM, Epoch::new(2))
            .await
            .expect("segment is opened");
        assert_eq!(
            (opened.epoch, opened.first_sequence),
            (Epoch::new(2), SequenceNumber::new(78))
        );
    }

    // ---- materializers ----

    #[tokio::test]
    async fn registered_materializer_starts_below_the_low_water_mark() {
        let store = store_with_stream().await;
        store
            .update_stream(STREAM, |record| {
                record.low_water_mark = SequenceNumber::new(500);
                Ok(())
            })
            .await
            .expect("mark is set");
        let checkpoint = store
            .register_materializer(STREAM, "pages")
            .await
            .expect("registered");
        assert_eq!(
            (checkpoint.checkpoint, checkpoint.fencing_token),
            (SequenceNumber::new(499), 0)
        );
    }

    #[tokio::test]
    async fn registering_twice_keeps_the_existing_checkpoint() {
        let store = store_with_stream().await;
        let stream_id = store.get_stream(STREAM).await.expect("exists").stream_id;
        store
            .register_materializer(STREAM, "pages")
            .await
            .expect("registered");
        let token = store
            .acquire_checkpoint(stream_id, "pages")
            .await
            .expect("acquired")
            .fencing_token;
        store
            .commit_checkpoint(stream_id, "pages", token, SequenceNumber::new(40), None)
            .await
            .expect("committed");
        let again = store
            .register_materializer(STREAM, "pages")
            .await
            .expect("registered");
        assert_eq!(again.checkpoint, SequenceNumber::new(40));
    }

    #[tokio::test]
    async fn registration_is_recorded_on_the_stream() {
        let store = store_with_stream().await;
        store
            .register_materializer(STREAM, "pages")
            .await
            .expect("registered");
        let record = store.get_stream(STREAM).await.expect("stream exists");
        assert!(record.materializers.contains("pages"));
    }

    #[tokio::test]
    async fn unregistered_materializer_no_longer_has_a_checkpoint() {
        let store = store_with_stream().await;
        let stream_id = store.get_stream(STREAM).await.expect("exists").stream_id;
        store
            .register_materializer(STREAM, "pages")
            .await
            .expect("registered");
        store
            .unregister_materializer(STREAM, "pages")
            .await
            .expect("unregistered");
        assert_eq!(
            store.get_checkpoint(stream_id, "pages").await,
            Err(MetaError::MaterializerNotRegistered {
                stream_id,
                materializer: "pages".to_owned(),
            })
        );
    }

    #[tokio::test]
    async fn unregistering_an_unknown_materializer_succeeds() {
        let store = store_with_stream().await;
        assert_eq!(store.unregister_materializer(STREAM, "pages").await, Ok(()));
    }

    #[tokio::test]
    async fn each_acquisition_raises_the_fencing_token() {
        let store = store_with_stream().await;
        let stream_id = store.get_stream(STREAM).await.expect("exists").stream_id;
        store
            .register_materializer(STREAM, "pages")
            .await
            .expect("registered");
        let mut tokens = Vec::new();
        for _runner in 0..2 {
            let acquired = store
                .acquire_checkpoint(stream_id, "pages")
                .await
                .expect("acquired");
            tokens.push(acquired.fencing_token);
        }
        assert_eq!(tokens, [1, 2]);
    }

    #[tokio::test]
    async fn superseded_runner_cannot_commit() {
        let store = store_with_stream().await;
        let stream_id = store.get_stream(STREAM).await.expect("exists").stream_id;
        store
            .register_materializer(STREAM, "pages")
            .await
            .expect("registered");
        let old_token = store
            .acquire_checkpoint(stream_id, "pages")
            .await
            .expect("acquired")
            .fencing_token;
        store
            .acquire_checkpoint(stream_id, "pages")
            .await
            .expect("acquired");
        let stale_commit = store
            .commit_checkpoint(stream_id, "pages", old_token, SequenceNumber::new(40), None)
            .await;
        assert_eq!(
            stale_commit,
            Err(MetaError::MaterializerFenced {
                held_token: 1,
                current_token: 2,
            })
        );
    }

    #[tokio::test]
    async fn committed_checkpoint_is_read_back_with_its_manifest() {
        let store = store_with_stream().await;
        let stream_id = store.get_stream(STREAM).await.expect("exists").stream_id;
        store
            .register_materializer(STREAM, "pages")
            .await
            .expect("registered");
        let token = store
            .acquire_checkpoint(stream_id, "pages")
            .await
            .expect("acquired")
            .fencing_token;
        store
            .commit_checkpoint(
                stream_id,
                "pages",
                token,
                SequenceNumber::new(40),
                Some("pages/manifest-40".to_owned()),
            )
            .await
            .expect("committed");
        let checkpoint = store
            .get_checkpoint(stream_id, "pages")
            .await
            .expect("registered");
        assert_eq!(
            (checkpoint.checkpoint, checkpoint.manifest),
            (
                SequenceNumber::new(40),
                Some("pages/manifest-40".to_owned())
            )
        );
    }

    #[tokio::test]
    async fn checkpoint_cannot_move_backwards() {
        let store = store_with_stream().await;
        let stream_id = store.get_stream(STREAM).await.expect("exists").stream_id;
        store
            .register_materializer(STREAM, "pages")
            .await
            .expect("registered");
        let token = store
            .acquire_checkpoint(stream_id, "pages")
            .await
            .expect("acquired")
            .fencing_token;
        store
            .commit_checkpoint(stream_id, "pages", token, SequenceNumber::new(40), None)
            .await
            .expect("committed");
        let backwards = store
            .commit_checkpoint(stream_id, "pages", token, SequenceNumber::new(39), None)
            .await;
        assert!(matches!(backwards, Err(MetaError::InvalidState { .. })));
    }

    #[tokio::test]
    async fn checkpoints_of_one_stream_are_listed_together() {
        let store = store_with_stream().await;
        let stream_id = store.get_stream(STREAM).await.expect("exists").stream_id;
        for materializer in ["pages", "blocks"] {
            store
                .register_materializer(STREAM, materializer)
                .await
                .expect("registered");
        }
        let listed: Vec<String> = store
            .list_checkpoints(stream_id)
            .await
            .expect("list")
            .into_iter()
            .map(|checkpoint| checkpoint.materializer)
            .collect();
        assert_eq!(listed, ["blocks", "pages"]);
    }

    // ---- replica registry ----

    fn replica(name: &str, heartbeat_at: u64) -> ReplicaRecord {
        ReplicaRecord {
            schema_version: RECORD_SCHEMA_VERSION,
            replica_id: ReplicaId::new(name).expect("valid identifier"),
            address: format!("{name}:7000"),
            failure_domains: vec!["zone-a".to_owned()],
            capacity_bytes: 1_000,
            free_bytes: 900,
            is_draining: false,
            heartbeat_at: Timestamp::from_unix_millis(heartbeat_at),
        }
    }

    #[tokio::test]
    async fn registered_replicas_are_listed_sorted() {
        let (store, _key_value_store) = new_store();
        for name in ["replica-b", "replica-a"] {
            store.put_replica(&replica(name, 0)).await.expect("put");
        }
        let listed: Vec<String> = store
            .list_replicas()
            .await
            .expect("list")
            .into_iter()
            .map(|record| record.replica_id.to_string())
            .collect();
        assert_eq!(listed, ["replica-a", "replica-b"]);
    }

    #[tokio::test]
    async fn heartbeat_replaces_the_previous_registration() {
        let (store, _key_value_store) = new_store();
        store
            .put_replica(&replica("replica-a", 0))
            .await
            .expect("put");
        store
            .put_replica(&replica("replica-a", 3_000))
            .await
            .expect("put");
        let listed = store.list_replicas().await.expect("list");
        assert_eq!(
            listed
                .iter()
                .map(|record| record.heartbeat_at)
                .collect::<Vec<_>>(),
            [Timestamp::from_unix_millis(3_000)]
        );
    }

    #[tokio::test]
    async fn removed_replica_is_no_longer_listed() {
        let (store, _key_value_store) = new_store();
        store
            .put_replica(&replica("replica-a", 0))
            .await
            .expect("put");
        store
            .remove_replica(&ReplicaId::new("replica-a").expect("valid identifier"))
            .await
            .expect("remove");
        assert_eq!(store.list_replicas().await, Ok(Vec::new()));
    }

    // ---- controller leadership ----

    const NOW: Timestamp = Timestamp::from_unix_millis(1_000);
    const LEADERSHIP_EXPIRY: Timestamp = Timestamp::from_unix_millis(6_000);

    #[tokio::test]
    async fn first_candidate_becomes_the_leader() {
        let (store, _key_value_store) = new_store();
        assert_eq!(
            store.try_lead("controller-1", NOW, LEADERSHIP_EXPIRY).await,
            Ok(true)
        );
    }

    #[tokio::test]
    async fn leader_can_be_read_back() {
        let (store, _key_value_store) = new_store();
        let before_anyone_led = store.get_leader().await;
        store
            .try_lead("controller-1", NOW, LEADERSHIP_EXPIRY)
            .await
            .expect("store is available");
        assert_eq!(
            (before_anyone_led, store.get_leader().await),
            (
                Ok(None),
                Ok(Some(LeaderRecord {
                    holder: "controller-1".to_owned(),
                    expires_at: LEADERSHIP_EXPIRY,
                }))
            )
        );
    }

    #[tokio::test]
    async fn second_candidate_waits_while_the_leader_is_live() {
        let (store, _key_value_store) = new_store();
        store
            .try_lead("controller-1", NOW, LEADERSHIP_EXPIRY)
            .await
            .expect("store is available");
        assert_eq!(
            store.try_lead("controller-2", NOW, LEADERSHIP_EXPIRY).await,
            Ok(false)
        );
    }

    #[tokio::test]
    async fn leader_renews_its_own_leadership() {
        let (store, _key_value_store) = new_store();
        store
            .try_lead("controller-1", NOW, LEADERSHIP_EXPIRY)
            .await
            .expect("store is available");
        assert_eq!(
            store
                .try_lead("controller-1", NOW, Timestamp::from_unix_millis(9_000))
                .await,
            Ok(true)
        );
    }

    #[tokio::test]
    async fn lapsed_leadership_can_be_taken_over() {
        let (store, _key_value_store) = new_store();
        store
            .try_lead("controller-1", NOW, LEADERSHIP_EXPIRY)
            .await
            .expect("store is available");
        assert_eq!(
            store
                .try_lead(
                    "controller-2",
                    LEADERSHIP_EXPIRY,
                    Timestamp::from_unix_millis(11_000)
                )
                .await,
            Ok(true)
        );
    }
}
