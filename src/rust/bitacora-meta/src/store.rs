//! The metadata store as bitacora uses it: typed operations, each one a conditional write.
//!
//! # Why this file exists
//!
//! A [`KeyValueStore`] holds bytes under keys. Writers, readers, the controller and materializers need more than that:
//! "claim this stream", "seal this segment", "commit this checkpoint", each of them safe when two processes try at once
//! and safe to repeat after a failure. Those operations are the same whichever system holds the bytes, so they are
//! written once, here, on top of the interface. This file holds the store itself and the few pieces every operation is
//! made of.
//!
//! # What it contains
//!
//! - [`MetadataStore`]: the handle every process of a cluster uses. Its operations are defined in the files under
//!   `store/`, one file per subject.
//! - `Stored`: a record together with the revision it was read at.
//! - `read_stream`, `read_checkpoint`, `list_records`, `replace_record`, `read_stream_id_counter`: the reads and the
//!   conditional write that the operations share.
//! - `put_record`, `delete_key`, `decode_record`, `unchanged_since_read`, `too_much_contention`: the small conversions
//!   between records and what a transaction is made of.
//!
//! # How it works
//!
//! Every operation that changes something has the same shape, a read-modify-write:
//!
//! 1. read the record, together with its revision;
//! 2. decide, from what was read, whether the change is allowed and what the new record is;
//! 3. write the new record in a transaction whose condition is that the key is still at the revision that was read;
//! 4. if the condition failed, somebody else wrote in between: start again from step 1.
//!
//! Step 2 is where the rules live: a writer's change is refused unless the stream's epoch is still the writer's, a
//! materializer's unless its fencing token is still current. Because the decision and the write are tied together by
//! the revision, a process that was paused or cut off cannot write from stale knowledge when it wakes up (design,
//! section 8).
//!
//! Conflicts are rare, because a stream's record changes on rolls, failovers and controller housekeeping only. The loop
//! gives up after `MAX_UPDATE_ATTEMPTS` conflicts in a row, so that a bug or a storm becomes an error instead of a
//! hang.
//!
//! Records are stored as JSON (see `records.rs`). A stored value that cannot be decoded is reported with its key; it is
//! never skipped, because a listing with a silent hole would be taken for the truth.
//!
//! # Invariants
//!
//! - There are no blind writes to a record whose content a decision depends on: every such write is conditional on the
//!   revision it was read at.
//! - An operation that returns an error other than `Unavailable` has changed nothing. The one exception is a restore,
//!   which is several transactions and documents what a failure half way leaves.
//! - Every operation is safe to repeat.
//!
//! # See also
//!
//! - `store/streams.rs`: creating, reading, updating and deleting streams.
//! - `store/writers.rs`: what a stream's writer does: epochs, leases, segments.
//! - `store/checkpoints.rs`: materializers and their checkpoints.
//! - `store/replicas.rs`: the replica registry.
//! - `store/leadership.rs`: which controller instance is active.
//! - `store/recovery.rs`: snapshots, restoring one, and adopting a rebuilt stream.
//! - `keys.rs`: where each record lives. `error.rs`: how an operation fails.

use std::sync::Arc;

use bitacora_types::StreamId;
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::MetaError;
use crate::key_value::{Condition, KeyValueStore, Mutation, Revision, TransactionOutcome, VersionedValue};
use crate::keys::{STREAM_ID_COUNTER_KEY, checkpoint_key, stream_key};
use crate::records::{CheckpointRecord, StreamRecord};

mod checkpoints;
mod leadership;
mod recovery;
mod replicas;
mod streams;
mod writers;

#[cfg(test)]
mod test_support;

pub use recovery::{RestoreMode, RestoreOutcome};

/// How many times a read-modify-write is retried when other writers keep changing the same key. Conflicts are rare: a
/// stream's key changes on rolls, failovers and controller housekeeping only.
const MAX_UPDATE_ATTEMPTS: u32 = 32;

/// The stream identifier counter of a store in which no stream was ever created. The first stream gets the identifier
/// one above it.
const NO_STREAM_ID_HANDED_OUT: u64 = 0;

/// The metadata store, as bitacora uses it.
///
/// It wraps a [`KeyValueStore`] and offers the operations of the protocol: streams, epochs and leases, segments,
/// checkpoints, the replica registry, controller leadership, and snapshots. Cloning is cheap, and clones share the
/// backend; every process holds one and hands clones to whatever needs it.
///
/// # Examples
///
/// A writer claiming a stream and opening its first segment:
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_meta::MetaError> {
/// use std::sync::Arc;
///
/// use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RetentionPolicy};
/// use bitacora_types::{Epoch, ReplicaId, ReplicationSettings, SequenceNumber, Timestamp};
///
/// let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
/// let standby_replicas = vec![
///     ReplicaId::new("replica-a").expect("a valid identifier"),
///     ReplicaId::new("replica-b").expect("a valid identifier"),
///     ReplicaId::new("replica-c").expect("a valid identifier"),
/// ];
/// metadata
///     .create_stream(
///         "orders",
///         ReplicationSettings::DEFAULT,
///         RetentionPolicy::default(),
///         Some(standby_replicas),
///     )
///     .await?;
///
/// let lease_expires_at = Timestamp::from_unix_millis(10_000);
/// let claimed = metadata
///     .claim_epoch("orders", Epoch::ZERO, "compute-1", lease_expires_at)
///     .await?;
/// let (_stream, opened_segment) = metadata.open_segment("orders", claimed.epoch).await?;
///
/// assert_eq!(claimed.epoch, Epoch::new(1));
/// assert_eq!(opened_segment.first_sequence, SequenceNumber::FIRST);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct MetadataStore {
    /// The backend that holds the records. Shared by every clone of the store.
    key_value_store: Arc<dyn KeyValueStore>,
}

/// A record together with the revision it was read at, which a later write names as its condition.
struct Stored<Record> {
    /// The decoded record.
    record: Record,
    /// The revision of the record's key when it was read.
    revision: Revision,
}

/// The stream identifier counter as it was read, with the condition that it is still so.
struct StreamIdCounter {
    /// The last stream identifier that was handed out.
    last_stream_id: u64,
    /// Holds only while nobody has changed the counter since it was read.
    unchanged: Condition,
}

impl MetadataStore {
    /// Wraps a key-value backend.
    ///
    /// Nothing is read or written yet.
    ///
    /// # Arguments
    ///
    /// * `key_value_store` - The backend that holds the records: the etcd backend in a deployment, the in-memory one in
    ///   tests and in the simulator.
    ///
    /// # Returns
    ///
    /// The store.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use bitacora_meta::{MemoryKeyValueStore, MetadataStore};
    ///
    /// let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// let another_handle = metadata.clone();
    /// # drop(another_handle);
    /// ```
    #[must_use]
    pub fn new(key_value_store: Arc<dyn KeyValueStore>) -> Self {
        Self { key_value_store }
    }

    /// Reads the record of the stream called `name`, with its revision.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::StreamNotFound`] if no stream has this name, [`MetaError::CorruptRecord`] if the stored
    /// record cannot be decoded, and [`MetaError::Unavailable`] if the store cannot be read.
    async fn read_stream(&self, name: &str) -> Result<Stored<StreamRecord>, MetaError> {
        let stream_key = stream_key(name);
        let Some(stored) = self.key_value_store.get(&stream_key).await? else {
            return Err(MetaError::StreamNotFound { name: name.to_owned() });
        };
        Ok(Stored {
            record: decode_record(&stream_key, &stored.value)?,
            revision: stored.revision,
        })
    }

    /// Reads one materializer's checkpoint, with its revision.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::MaterializerNotRegistered`] if there is no such checkpoint, [`MetaError::CorruptRecord`] if
    /// it cannot be decoded, and [`MetaError::Unavailable`] if the store cannot be read.
    async fn read_checkpoint(
        &self,
        stream_id: StreamId,
        materializer: &str,
    ) -> Result<Stored<CheckpointRecord>, MetaError> {
        let checkpoint_key = checkpoint_key(stream_id, materializer);
        let Some(stored) = self.key_value_store.get(&checkpoint_key).await? else {
            return Err(MetaError::MaterializerNotRegistered {
                stream_id,
                materializer: materializer.to_owned(),
            });
        };
        Ok(Stored {
            record: decode_record(&checkpoint_key, &stored.value)?,
            revision: stored.revision,
        })
    }

    /// Reads and decodes every record whose key starts with `key_prefix`, in key order.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::CorruptRecord`] for the first record that cannot be decoded, and [`MetaError::Unavailable`]
    /// if the store cannot be read.
    async fn list_records<Record: DeserializeOwned>(&self, key_prefix: &str) -> Result<Vec<Record>, MetaError> {
        let stored_entries = self.key_value_store.list(key_prefix).await?;
        let mut records = Vec::with_capacity(stored_entries.len());
        for (key, stored) in &stored_entries {
            records.push(decode_record(key, &stored.value)?);
        }
        Ok(records)
    }

    /// Replaces the record at `key` with `record` if the key is still at `revision`.
    ///
    /// # Returns
    ///
    /// `true` if the write was applied, `false` if somebody else wrote the key since it was read.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::Unavailable`] if the store cannot be reached. The write may or may not have been applied.
    async fn replace_record<Record: Serialize + Sync>(
        &self,
        key: &str,
        revision: Revision,
        record: &Record,
    ) -> Result<bool, MetaError> {
        let unchanged = Condition::HasRevision {
            key: key.to_owned(),
            revision,
        };
        let outcome = self
            .key_value_store
            .transact(vec![unchanged], vec![put_record(key, record)])
            .await?;
        Ok(outcome == TransactionOutcome::Applied)
    }

    /// Reads the last stream identifier that was handed out, with the condition that it is still the last.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::CorruptRecord`] if the counter cannot be decoded, and [`MetaError::Unavailable`] if the
    /// store cannot be read.
    async fn read_stream_id_counter(&self) -> Result<StreamIdCounter, MetaError> {
        let stored_counter = self.key_value_store.get(STREAM_ID_COUNTER_KEY).await?;
        let last_stream_id = match &stored_counter {
            Some(stored) => decode_record::<u64>(STREAM_ID_COUNTER_KEY, &stored.value)?,
            None => NO_STREAM_ID_HANDED_OUT,
        };
        Ok(StreamIdCounter {
            last_stream_id,
            unchanged: unchanged_since_read(STREAM_ID_COUNTER_KEY, stored_counter.as_ref()),
        })
    }
}

/// Builds the mutation that stores `record`, as JSON, under `key`.
///
/// # Panics
///
/// Panics if the record cannot be written as JSON, which no record of this crate can fail to be: they are plain data
/// with string keys.
fn put_record<Record: Serialize>(key: &str, record: &Record) -> Mutation {
    let encoded_record = serde_json::to_vec(record).expect("metadata records serialize to JSON");
    Mutation::Put {
        key: key.to_owned(),
        value: Bytes::from(encoded_record),
    }
}

/// Builds the mutation that removes `key`.
fn delete_key(key: &str) -> Mutation {
    Mutation::Delete { key: key.to_owned() }
}

/// Decodes the record stored under `key`.
///
/// # Errors
///
/// Returns [`MetaError::CorruptRecord`], naming the key and what the decoder found, if the bytes are not the JSON of a
/// `Record`.
fn decode_record<Record: DeserializeOwned>(key: &str, stored_value: &[u8]) -> Result<Record, MetaError> {
    match serde_json::from_slice(stored_value) {
        Ok(record) => Ok(record),
        Err(json_error) => Err(MetaError::CorruptRecord {
            key: key.to_owned(),
            reason: json_error.to_string(),
        }),
    }
}

/// Builds the condition "`key` is as it was when it was read": absent if the read found nothing, at the revision of the
/// read otherwise.
fn unchanged_since_read(key: &str, stored: Option<&VersionedValue>) -> Condition {
    match stored {
        Some(stored) => Condition::HasRevision {
            key: key.to_owned(),
            revision: stored.revision,
        },
        None => Condition::IsAbsent { key: key.to_owned() },
    }
}

/// Builds the error for a read-modify-write of `key` that lost against other writers on every attempt.
fn too_much_contention(key: &str) -> MetaError {
    MetaError::TooMuchContention {
        key: key.to_owned(),
        attempts: MAX_UPDATE_ATTEMPTS,
    }
}

#[cfg(test)]
#[path = "store_test.rs"]
mod tests;
