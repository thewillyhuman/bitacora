//! Getting a metadata store back after it was lost: snapshots, restoring one, and adopting a rebuilt stream.
//!
//! # Why this file exists
//!
//! The metadata store is small and holds what nothing else does. If it is lost, the records on the replicas and in the
//! object store are still there but cannot be found. The design has two answers (section 14, "Routine procedures"): the
//! store is copied to the object store every few minutes, so that it can be restored from the last copy; and as a last
//! resort the segment lists are rebuilt from what the replicas and the object store hold. Both end with writes to the
//! metadata store that no ordinary operation makes: writing keys back wholesale, and writing a stream record that was
//! put together outside the store. Those writes are here, apart from everything else, because they are the only ones
//! that do not start from what the store already holds.
//!
//! # What it contains
//!
//! - `MetadataStore::snapshot`: the whole store as it is at one moment.
//! - `MetadataStore::restore`, with [`RestoreMode`] and [`RestoreOutcome`]: writing a snapshot back.
//! - `MetadataStore::adopt_stream`: writing a stream record that a rebuild produced.
//!
//! # How it works
//!
//! **Snapshot.** One listing of every key. A backend answers a listing from one consistent view, so a snapshot never
//! mixes two states: a stream's record and the checkpoints of its materializers are from the same instant.
//!
//! **Restore.** Every key of the snapshot is written with the bytes it had; the records are not decoded, so a snapshot
//! is restored the same way whatever version wrote it. A backend limits how much one transaction may change, so the
//! keys go in batches. That makes a restore several transactions, not one, and something to run while nothing else uses
//! the store. The mode says what to do about keys the store already holds: refuse to start, or end up exactly as the
//! snapshot was.
//!
//! **Adoption.** A rebuilt stream record is written under its name, replacing the record of the same stream if there is
//! one. The stream identifier counter is raised to the record's identifier if it is below, in the same transaction, so
//! that a stream created later never gets an identifier that data on replicas still carries.
//!
//! # Invariants
//!
//! - A restore into a store that must be empty changes nothing if the store is not empty, and stops as soon as it sees
//!   a key that somebody else wrote meanwhile.
//! - Adoption never lowers the stream identifier counter, never gives one identifier to two names, never gives one name
//!   to two identifiers, and never brings back a stream that is recorded as deleted.
//!
//! # See also
//!
//! - `../snapshot.rs`: what a snapshot is as bytes, and how snapshot objects are named.
//! - `bitacora-controller/src/controller/snapshot.rs`: who takes snapshots, and when.
//! - `bita/src/commands`: `bita metadata restore` and `bita metadata rebuild`, the callers.

use std::collections::BTreeSet;

use bitacora_types::Timestamp;

use super::{
    MAX_UPDATE_ATTEMPTS, MetadataStore, decode_record, delete_key, put_record, too_much_contention,
    unchanged_since_read,
};
use crate::error::MetaError;
use crate::key_value::{Condition, Mutation, TransactionOutcome, VersionedValue};
use crate::keys::{STREAM_ID_COUNTER_KEY, deleted_stream_key, stream_key, validate_name};
use crate::records::StreamRecord;
use crate::snapshot::{MetadataSnapshot, SnapshotEntry};

/// How many keys one transaction of a restore changes. etcd refuses a transaction with more operations than it is
/// configured for, 128 by default; this stays well below, with room for the conditions.
const RESTORE_BATCH_KEYS: usize = 48;

/// What a restore does about keys the store already holds.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_meta::MetaError> {
/// # use std::sync::Arc;
/// # use bitacora_meta::{MemoryKeyValueStore, MetaError, MetadataStore, RestoreMode, RetentionPolicy};
/// # use bitacora_types::{ReplicationSettings, Timestamp};
/// # let source = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
/// # let snapshot = source.snapshot(Timestamp::from_unix_millis(1_000)).await?;
/// # let in_use = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
/// in_use
///     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
///     .await?;
///
/// let refused = in_use.restore(&snapshot, RestoreMode::IntoEmptyStore).await;
/// let replaced = in_use.restore(&snapshot, RestoreMode::ReplaceEverything).await;
///
/// assert!(matches!(refused, Err(MetaError::InvalidState { .. })));
/// assert!(replaced.is_ok());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestoreMode {
    /// The store must be empty. If it is not, the restore fails and changes nothing.
    IntoEmptyStore,
    /// The store ends up as the snapshot was: keys of the snapshot are overwritten and every other key is deleted.
    ReplaceEverything,
}

/// What a restore changed.
///
/// # Examples
///
/// Restoring the snapshot of an empty store over a store with one stream writes nothing and deletes the stream's
/// record and the stream identifier counter:
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_meta::MetaError> {
/// # use std::sync::Arc;
/// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RestoreMode, RestoreOutcome, RetentionPolicy};
/// # use bitacora_types::{ReplicationSettings, Timestamp};
/// # let empty_store = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
/// # let store_in_use = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
/// # store_in_use
/// #     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
/// #     .await?;
/// let snapshot_of_an_empty_store = empty_store.snapshot(Timestamp::from_unix_millis(1_000)).await?;
///
/// let outcome = store_in_use
///     .restore(&snapshot_of_an_empty_store, RestoreMode::ReplaceEverything)
///     .await?;
///
/// assert_eq!(
///     outcome,
///     RestoreOutcome {
///         keys_written: 0,
///         keys_deleted: 2
///     }
/// );
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestoreOutcome {
    /// How many keys were written from the snapshot.
    pub keys_written: usize,
    /// How many keys that the snapshot does not hold were deleted.
    pub keys_deleted: usize,
}

/// The writes a restore will make, worked out before the first one is made.
struct RestorePlan {
    /// One write per key of the snapshot, then one delete per key that only the store holds.
    mutations: Vec<Mutation>,
    /// How many of the mutations are deletes.
    keys_deleted: usize,
}

impl MetadataStore {
    /// Reads the whole store as it is at one moment, for a backup in the object store (design, section 14).
    ///
    /// The snapshot is one listing of every key. Both backends answer a listing from one consistent view of the store,
    /// so a snapshot never mixes two states: a stream's record and the checkpoints of its materializers, for example,
    /// are from the same instant.
    ///
    /// # Arguments
    ///
    /// * `taken_at` - The current time, recorded in the snapshot. The store has no clock of its own.
    ///
    /// # Returns
    ///
    /// Every key of the store with the bytes stored under it, in key order.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::Unavailable`] if the store cannot be reached.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RetentionPolicy};
    /// # use bitacora_types::{ReplicationSettings, Timestamp};
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// metadata
    ///     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    ///     .await?;
    ///
    /// let snapshot = metadata.snapshot(Timestamp::from_unix_millis(1_000)).await?;
    ///
    /// let mut keys = Vec::new();
    /// for entry in &snapshot.entries {
    ///     keys.push(entry.key.as_str());
    /// }
    /// assert_eq!(keys, ["counters/stream-id", "streams/orders"]);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn snapshot(&self, taken_at: Timestamp) -> Result<MetadataSnapshot, MetaError> {
        let stored_entries = self.key_value_store.list("").await?;
        let mut entries = Vec::with_capacity(stored_entries.len());
        for (key, stored) in stored_entries {
            entries.push(SnapshotEntry {
                key,
                value: stored.value,
            });
        }
        Ok(MetadataSnapshot { taken_at, entries })
    }

    /// Writes the content of a snapshot back into the store, after the store was lost (design, section 14, "Disaster
    /// recovery").
    ///
    /// Every key of the snapshot is written with the bytes it had. With [`RestoreMode::ReplaceEverything`], keys the
    /// snapshot does not hold are deleted, so that the store ends up as the snapshot was.
    ///
    /// The restore is several transactions, not one: a backend limits how much one transaction may change. Run it while
    /// no writer, controller or materializer uses the store. If it fails half way, run it again with
    /// [`RestoreMode::ReplaceEverything`].
    ///
    /// # Arguments
    ///
    /// * `snapshot` - What to write back.
    /// * `mode` - What to do about keys the store already holds.
    ///
    /// # Returns
    ///
    /// How many keys were written and how many were deleted.
    ///
    /// # Errors
    ///
    /// * [`MetaError::InvalidState`] if the mode is [`RestoreMode::IntoEmptyStore`] and the store holds a key, in which
    ///   case nothing was changed, or gets one from somebody else while the restore runs, in which case the batches
    ///   written so far stay.
    /// * [`MetaError::Unavailable`] if the store cannot be reached. Part of the snapshot may have been written.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RestoreMode, RetentionPolicy};
    /// # use bitacora_types::{ReplicationSettings, Timestamp};
    /// # let lost_store = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// # lost_store
    /// #     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    /// #     .await?;
    /// let snapshot = lost_store.snapshot(Timestamp::from_unix_millis(1_000)).await?;
    /// let new_store = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    ///
    /// let outcome = new_store.restore(&snapshot, RestoreMode::IntoEmptyStore).await?;
    ///
    /// assert_eq!(outcome.keys_written, 2);
    /// assert_eq!(new_store.get_stream("orders").await?.name, "orders");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn restore(&self, snapshot: &MetadataSnapshot, mode: RestoreMode) -> Result<RestoreOutcome, MetaError> {
        let existing_entries = self.key_value_store.list("").await?;
        if mode == RestoreMode::IntoEmptyStore && !existing_entries.is_empty() {
            return Err(MetaError::InvalidState {
                reason: format!(
                    "the metadata store holds {} keys; restoring into it would mix two states",
                    existing_entries.len()
                ),
            });
        }
        let plan = plan_restore(snapshot, &existing_entries);
        for batch in plan.mutations.chunks(RESTORE_BATCH_KEYS) {
            let conditions = conditions_of_a_restore_batch(mode, batch);
            let outcome = self.key_value_store.transact(conditions, batch.to_vec()).await?;
            if outcome == TransactionOutcome::ConditionFailed {
                return Err(MetaError::InvalidState {
                    reason: "somebody else wrote to the metadata store while it was restored".to_owned(),
                });
            }
        }
        Ok(RestoreOutcome {
            keys_written: snapshot.entries.len(),
            keys_deleted: plan.keys_deleted,
        })
    }

    /// Writes a stream record that was put together outside the store, as `bita metadata rebuild` does from what
    /// replicas and the object store hold.
    ///
    /// The record replaces the stream's record if the stream is known, and creates it otherwise. The stream identifier
    /// counter is raised to the record's identifier if it is below, in the same transaction, so that a stream created
    /// later never reuses it.
    ///
    /// # Arguments
    ///
    /// * `record` - The stream record to write, complete. It is stored as given.
    ///
    /// # Errors
    ///
    /// * [`MetaError::InvalidName`] if the record's name cannot be used in a key.
    /// * [`MetaError::InvalidState`] if the name belongs to a stream with another identifier, if the identifier belongs
    ///   to a stream with another name, or if the stream is recorded as deleted.
    /// * [`MetaError::TooMuchContention`] or [`MetaError::Unavailable`] if the store could not do it now.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RetentionPolicy};
    /// # use bitacora_types::{Epoch, ReplicationSettings, StreamId};
    /// # let scratch = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// # let mut rebuilt_record = scratch
    /// #     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    /// #     .await?;
    /// rebuilt_record.stream_id = StreamId::new(7);
    /// rebuilt_record.epoch = Epoch::new(9);
    /// let empty_store = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    ///
    /// empty_store.adopt_stream(&rebuilt_record).await?;
    /// let created_later = empty_store
    ///     .create_stream("audit", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    ///     .await?;
    ///
    /// assert_eq!(empty_store.get_stream("orders").await?, rebuilt_record);
    /// assert_eq!(created_later.stream_id, StreamId::new(8));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn adopt_stream(&self, record: &StreamRecord) -> Result<(), MetaError> {
        validate_name(&record.name)?;
        for _attempt in 0..MAX_UPDATE_ATTEMPTS {
            let was_adopted = self.try_adopt_stream(record).await?;
            if was_adopted {
                return Ok(());
            }
        }
        Err(too_much_contention(&stream_key(&record.name)))
    }

    /// Makes one attempt to adopt a stream record: checks that it contradicts nothing the store holds, and writes it
    /// together with the raised identifier counter if nothing it checked changed meanwhile.
    ///
    /// # Returns
    ///
    /// `true` if the record was written, `false` if somebody else wrote in between and the attempt must be repeated.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::InvalidState`] if the record contradicts the store, and [`MetaError::Unavailable`] if the
    /// store cannot be reached.
    async fn try_adopt_stream(&self, record: &StreamRecord) -> Result<bool, MetaError> {
        let stream_key = stream_key(&record.name);
        let deleted_key = deleted_stream_key(record.stream_id);
        self.refuse_a_stream_recorded_as_deleted(record, &deleted_key).await?;
        self.refuse_an_identifier_known_under_another_name(record).await?;
        let stored_stream = self.key_value_store.get(&stream_key).await?;
        refuse_a_name_that_belongs_to_another_stream(record, &stream_key, stored_stream.as_ref())?;
        let stream_is_unchanged = unchanged_since_read(&stream_key, stored_stream.as_ref());
        let counter = self.read_stream_id_counter().await?;
        let raised_counter = counter.last_stream_id.max(record.stream_id.get());
        let is_not_deleted = Condition::IsAbsent { key: deleted_key };
        let outcome = self
            .key_value_store
            .transact(
                vec![counter.unchanged, stream_is_unchanged, is_not_deleted],
                vec![
                    put_record(STREAM_ID_COUNTER_KEY, &raised_counter),
                    put_record(&stream_key, record),
                ],
            )
            .await?;
        Ok(outcome == TransactionOutcome::Applied)
    }

    /// Checks that the stream of `record` is not recorded as deleted, by the note stored under `deleted_key`.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::InvalidState`] if the note exists: the controller is removing the stream's records, and
    /// adopting it would bring a stream back whose data is going away.
    async fn refuse_a_stream_recorded_as_deleted(
        &self,
        record: &StreamRecord,
        deleted_key: &str,
    ) -> Result<(), MetaError> {
        let deletion_note = self.key_value_store.get(deleted_key).await?;
        if deletion_note.is_some() {
            return Err(MetaError::InvalidState {
                reason: format!(
                    "stream {} is recorded as deleted; its records are being removed",
                    record.stream_id
                ),
            });
        }
        Ok(())
    }

    /// Checks that no stream with another name already has the identifier of `record`.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::InvalidState`], naming the other stream, if one does.
    async fn refuse_an_identifier_known_under_another_name(&self, record: &StreamRecord) -> Result<(), MetaError> {
        for known_stream in self.list_streams().await? {
            if known_stream.stream_id == record.stream_id && known_stream.name != record.name {
                return Err(MetaError::InvalidState {
                    reason: format!(
                        "stream {} is already known as {:?}",
                        record.stream_id, known_stream.name
                    ),
                });
            }
        }
        Ok(())
    }
}

/// Checks that the record stored under the name of `record`, if any, is the same stream.
///
/// # Errors
///
/// Returns [`MetaError::InvalidState`] if the name is taken by a stream with another identifier, and
/// [`MetaError::CorruptRecord`] if the stored record cannot be decoded.
fn refuse_a_name_that_belongs_to_another_stream(
    record: &StreamRecord,
    stream_key: &str,
    stored_stream: Option<&VersionedValue>,
) -> Result<(), MetaError> {
    let Some(stored) = stored_stream else {
        return Ok(());
    };
    let known_stream: StreamRecord = decode_record(stream_key, &stored.value)?;
    if known_stream.stream_id != record.stream_id {
        return Err(MetaError::InvalidState {
            reason: format!(
                "the name {:?} belongs to stream {}, not to stream {}",
                record.name, known_stream.stream_id, record.stream_id
            ),
        });
    }
    Ok(())
}

/// Works out the writes of a restore: every key of the snapshot is put, and every key the store holds that the snapshot
/// does not is deleted.
fn plan_restore(snapshot: &MetadataSnapshot, existing_entries: &[(String, VersionedValue)]) -> RestorePlan {
    let mut snapshot_keys = BTreeSet::new();
    let mut mutations = Vec::with_capacity(snapshot.entries.len());
    for entry in &snapshot.entries {
        snapshot_keys.insert(entry.key.as_str());
        mutations.push(Mutation::Put {
            key: entry.key.clone(),
            value: entry.value.clone(),
        });
    }
    let mut keys_deleted = 0;
    for (existing_key, _stored) in existing_entries {
        if !snapshot_keys.contains(existing_key.as_str()) {
            mutations.push(delete_key(existing_key));
            keys_deleted += 1;
        }
    }
    RestorePlan {
        mutations,
        keys_deleted,
    }
}

/// Returns what must hold for one batch of a restore to be applied.
///
/// A restore into a store that must be empty writes a key only if it is still absent, so that it never overwrites what
/// somebody else wrote since the restore began. A restore that replaces everything has no conditions.
fn conditions_of_a_restore_batch(mode: RestoreMode, batch: &[Mutation]) -> Vec<Condition> {
    let mut conditions = Vec::new();
    if mode == RestoreMode::IntoEmptyStore {
        for mutation in batch {
            if let Mutation::Put { key, .. } = mutation {
                conditions.push(Condition::IsAbsent { key: key.clone() });
            }
        }
    }
    conditions
}

#[cfg(test)]
#[path = "recovery_test.rs"]
mod tests;
