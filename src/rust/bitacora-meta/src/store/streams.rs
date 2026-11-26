//! Creating, reading, updating and deleting streams.
//!
//! # Why this file exists
//!
//! A stream exists because the metadata store has a record of it. Creating that record, finding it, changing it and
//! removing it are the operations everything else is built from: a writer's claim, a controller's verdict on a copy and
//! an operator's truncation request are all "read the stream record, change it, write it back if nobody else did". This
//! file holds those operations.
//!
//! # What it contains
//!
//! - `MetadataStore::create_stream`: a new stream with a fresh identifier.
//! - `MetadataStore::get_stream`, `MetadataStore::list_streams`: reads.
//! - `MetadataStore::update_stream`: the general read-modify-write of one stream record.
//! - `MetadataStore::delete_stream`, `MetadataStore::list_deleted_streams`, `MetadataStore::forget_deleted_stream`:
//!   deletion, and the note it leaves for the controller.
//!
//! # How it works
//!
//! **Identifiers.** A stream gets its identifier from a counter in the store. Creating a stream raises the counter and
//! writes the record in one transaction, conditional on the counter being unchanged and on the name being free. Two
//! creations at once therefore get two identifiers, and an identifier is never handed out twice, also not after its
//! stream was deleted: replicas and the object store may still hold data under it.
//!
//! **Updates.** `update_stream` reads the record, hands a copy to the caller's function, and writes the changed copy
//! back on the condition that the record is still at the revision that was read. If somebody else wrote in between, it
//! reads again and calls the function again. A function that changes nothing costs no write.
//!
//! **Deletion.** Deleting a stream removes its record and the checkpoints of its materializers, and writes a note under
//! the stream's identifier, all in one transaction. The record's data is still on replicas and in the object store at
//! that point. The note is what tells the controller to remove it, and the controller deletes the note when nothing is
//! left. Meanwhile the name is free again.
//!
//! # Invariants
//!
//! - Stream identifiers are unique for the life of the store, deleted streams included.
//! - A stream record and the checkpoints of its materializers are deleted together or not at all.
//! - `update_stream` writes only what the caller's function produced from the latest record.
//!
//! # See also
//!
//! - `../store.rs`: the read-modify-write shape and the helpers used here.
//! - `writers.rs`, `checkpoints.rs`: the operations built on `update_stream` and on the same shape.
//! - `bitacora-controller`: who acts on the deleted-stream notes.

use std::collections::BTreeSet;

use bitacora_types::{Epoch, ReplicaId, ReplicationSettings, SegmentId, SequenceNumber, StreamId};

use super::{MAX_UPDATE_ATTEMPTS, MetadataStore, delete_key, put_record, too_much_contention};
use crate::error::MetaError;
use crate::key_value::{Condition, TransactionOutcome};
use crate::keys::{
    DELETED_STREAM_KEY_PREFIX, STREAM_ID_COUNTER_KEY, STREAM_KEY_PREFIX, checkpoint_key, deleted_stream_key,
    stream_key, validate_name,
};
use crate::records::{DeletedStreamRecord, RECORD_SCHEMA_VERSION, RetentionPolicy, StreamRecord};

impl MetadataStore {
    /// Creates a stream with no segments, no writer and epoch zero.
    ///
    /// The stream gets the next unused identifier. The record and the identifier counter are written in one
    /// transaction, so two creations at once cannot share an identifier or a name.
    ///
    /// # Arguments
    ///
    /// * `name` - The stream's name: 1 to [`crate::MAX_NAME_LENGTH_BYTES`] ASCII letters, digits, `.`, `_` or `-`.
    /// * `replication` - How many replicas hold each segment and how many commit a record.
    /// * `retention` - How long records that nothing needs are kept.
    /// * `standby_replicas` - The replica set of the stream's first segment, if the caller already placed it. With
    ///   `None`, the controller places it on its next pass.
    ///
    /// # Returns
    ///
    /// The record that was written.
    ///
    /// # Errors
    ///
    /// * [`MetaError::InvalidName`] if the name cannot be used in a key.
    /// * [`MetaError::StreamAlreadyExists`] if the name is taken.
    /// * [`MetaError::TooMuchContention`] if other creations won every attempt.
    /// * [`MetaError::Unavailable`] if the store cannot be reached.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// use std::sync::Arc;
    ///
    /// use bitacora_meta::{MemoryKeyValueStore, MetaError, MetadataStore, RetentionPolicy};
    /// use bitacora_types::ReplicationSettings;
    ///
    /// let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    ///
    /// let created = metadata
    ///     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    ///     .await?;
    /// let created_again = metadata
    ///     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    ///     .await;
    ///
    /// assert_eq!(created.name, "orders");
    /// assert!(matches!(created_again, Err(MetaError::StreamAlreadyExists { .. })));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_stream(
        &self,
        name: &str,
        replication: ReplicationSettings,
        retention: RetentionPolicy,
        standby_replicas: Option<Vec<ReplicaId>>,
    ) -> Result<StreamRecord, MetaError> {
        validate_name(name)?;
        for _attempt in 0..MAX_UPDATE_ATTEMPTS {
            let created = self
                .try_create_stream(name, replication, retention, standby_replicas.clone())
                .await?;
            if let Some(record) = created {
                return Ok(record);
            }
        }
        Err(too_much_contention(&stream_key(name)))
    }

    /// Makes one attempt to create a stream: reads the identifier counter, and writes the counter and the record if
    /// neither the counter nor the name was touched meanwhile.
    ///
    /// # Returns
    ///
    /// The record that was written, or `None` if another creation got in between and the attempt must be repeated.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::StreamAlreadyExists`] if the name is taken, and [`MetaError::Unavailable`] if the store
    /// cannot be reached.
    async fn try_create_stream(
        &self,
        name: &str,
        replication: ReplicationSettings,
        retention: RetentionPolicy,
        standby_replicas: Option<Vec<ReplicaId>>,
    ) -> Result<Option<StreamRecord>, MetaError> {
        let stream_key = stream_key(name);
        if self.key_value_store.get(&stream_key).await?.is_some() {
            return Err(MetaError::StreamAlreadyExists { name: name.to_owned() });
        }
        let counter = self.read_stream_id_counter().await?;
        let stream_id = counter.last_stream_id + 1;
        let record = new_stream_record(StreamId::new(stream_id), name, replication, retention, standby_replicas);
        let name_is_free = Condition::IsAbsent {
            key: stream_key.clone(),
        };
        let outcome = self
            .key_value_store
            .transact(
                vec![counter.unchanged, name_is_free],
                vec![
                    put_record(STREAM_ID_COUNTER_KEY, &stream_id),
                    put_record(&stream_key, &record),
                ],
            )
            .await?;
        match outcome {
            TransactionOutcome::Applied => Ok(Some(record)),
            TransactionOutcome::ConditionFailed => Ok(None),
        }
    }

    /// Reads a stream's record.
    ///
    /// # Arguments
    ///
    /// * `name` - The stream's name.
    ///
    /// # Returns
    ///
    /// The record as it is stored right now.
    ///
    /// # Errors
    ///
    /// * [`MetaError::StreamNotFound`] if no stream has this name.
    /// * [`MetaError::CorruptRecord`] if the stored record cannot be decoded.
    /// * [`MetaError::Unavailable`] if the store cannot be reached.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RetentionPolicy};
    /// # use bitacora_types::{Epoch, ReplicationSettings};
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// metadata
    ///     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    ///     .await?;
    ///
    /// let stream = metadata.get_stream("orders").await?;
    ///
    /// assert_eq!(stream.epoch, Epoch::ZERO);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_stream(&self, name: &str) -> Result<StreamRecord, MetaError> {
        let stored = self.read_stream(name).await?;
        Ok(stored.record)
    }

    /// Lists every stream.
    ///
    /// The listing is one consistent view of the store.
    ///
    /// # Returns
    ///
    /// The records of all streams, sorted by name. Empty if there is none.
    ///
    /// # Errors
    ///
    /// * [`MetaError::CorruptRecord`] if a stored record cannot be decoded.
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
    /// for name in ["orders", "billing"] {
    ///     metadata
    ///         .create_stream(name, ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    ///         .await?;
    /// }
    ///
    /// let streams = metadata.list_streams().await?;
    ///
    /// assert_eq!(streams[0].name, "billing");
    /// assert_eq!(streams[1].name, "orders");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_streams(&self) -> Result<Vec<StreamRecord>, MetaError> {
        self.list_records(STREAM_KEY_PREFIX).await
    }

    /// Deletes a stream.
    ///
    /// Removes the stream's record and the checkpoints of its materializers, and leaves a note for the controller, in
    /// one transaction. The controller removes the segments left on replicas and in the object store and then calls
    /// [`MetadataStore::forget_deleted_stream`]. The name can be used for a new stream at once.
    ///
    /// # Arguments
    ///
    /// * `name` - The stream's name.
    ///
    /// # Returns
    ///
    /// The record the stream had when it was deleted.
    ///
    /// # Errors
    ///
    /// * [`MetaError::StreamNotFound`] if no stream has this name.
    /// * [`MetaError::TooMuchContention`] if other writers changed the record on every attempt.
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
    /// # metadata
    /// #     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    /// #     .await?;
    /// let deleted = metadata.delete_stream("orders").await?;
    ///
    /// let notes = metadata.list_deleted_streams().await?;
    /// assert_eq!(notes[0].stream_id, deleted.stream_id);
    /// assert!(metadata.get_stream("orders").await.is_err());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_stream(&self, name: &str) -> Result<StreamRecord, MetaError> {
        let stream_key = stream_key(name);
        for _attempt in 0..MAX_UPDATE_ATTEMPTS {
            let stored = self.read_stream(name).await?;
            let record = stored.record;
            let deletion_note = DeletedStreamRecord {
                stream_id: record.stream_id,
                name: record.name.clone(),
            };
            let mut mutations = vec![
                delete_key(&stream_key),
                put_record(&deleted_stream_key(record.stream_id), &deletion_note),
            ];
            for materializer in &record.materializers {
                mutations.push(delete_key(&checkpoint_key(record.stream_id, materializer)));
            }
            let unchanged = Condition::HasRevision {
                key: stream_key.clone(),
                revision: stored.revision,
            };
            let outcome = self.key_value_store.transact(vec![unchanged], mutations).await?;
            if outcome == TransactionOutcome::Applied {
                return Ok(record);
            }
        }
        Err(too_much_contention(&stream_key))
    }

    /// Lists the deleted streams whose segments may still exist.
    ///
    /// # Returns
    ///
    /// One note per deleted stream that the controller has not finished cleaning up, sorted by the text of the stream
    /// identifier. Empty if there is none.
    ///
    /// # Errors
    ///
    /// * [`MetaError::CorruptRecord`] if a stored note cannot be decoded.
    /// * [`MetaError::Unavailable`] if the store cannot be reached.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore};
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// let notes = metadata.list_deleted_streams().await?;
    ///
    /// assert!(notes.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_deleted_streams(&self) -> Result<Vec<DeletedStreamRecord>, MetaError> {
        self.list_records(DELETED_STREAM_KEY_PREFIX).await
    }

    /// Drops the note about a deleted stream, once nothing of the stream is left anywhere.
    ///
    /// Dropping a note that does not exist succeeds.
    ///
    /// # Arguments
    ///
    /// * `stream_id` - The identifier the deleted stream had.
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
    /// # use bitacora_types::ReplicationSettings;
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// # metadata
    /// #     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    /// #     .await?;
    /// let deleted = metadata.delete_stream("orders").await?;
    ///
    /// metadata.forget_deleted_stream(deleted.stream_id).await?;
    ///
    /// assert!(metadata.list_deleted_streams().await?.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn forget_deleted_stream(&self, stream_id: StreamId) -> Result<(), MetaError> {
        self.key_value_store
            .transact(Vec::new(), vec![delete_key(&deleted_stream_key(stream_id))])
            .await?;
        Ok(())
    }

    /// Reads a stream's record, lets `update` change it, and writes it back only if nobody else wrote it in between,
    /// retrying otherwise.
    ///
    /// This is the general read-modify-write of a stream record. `update` gets a copy of the latest record and changes
    /// it in place. It may run several times, once per attempt, and must have no side effects: everything it decides
    /// must follow from the record it is given. It aborts the whole operation by returning an error. If it leaves the
    /// record unchanged, nothing is written.
    ///
    /// `update` is a function, although this workspace prefers plain data to closures in public interfaces. It is the
    /// body of a transaction: the writer, the controller, the administration client and operator tools each pass their
    /// own change, a list of all possible changes would have to be extended here for every new one, and none of them
    /// could be expressed without also carrying the condition it depends on.
    ///
    /// # Arguments
    ///
    /// * `name` - The stream's name.
    /// * `update` - Changes the record it is given, or returns an error to change nothing. Its `Ok` value is returned
    ///   to the caller.
    ///
    /// # Returns
    ///
    /// The record as it is stored after the call, and what `update` returned for it.
    ///
    /// # Errors
    ///
    /// * Whatever `update` returns.
    /// * [`MetaError::StreamNotFound`] if no stream has this name.
    /// * [`MetaError::TooMuchContention`] if other writers changed the record on every attempt.
    /// * [`MetaError::Unavailable`] if the store cannot be reached.
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
    /// let requested_position = SequenceNumber::new(500);
    ///
    /// let (stream, previous_request) = metadata
    ///     .update_stream("orders", |record| {
    ///         let previous_request = record.truncate_before;
    ///         record.truncate_before = Some(requested_position);
    ///         Ok(previous_request)
    ///     })
    ///     .await?;
    ///
    /// assert_eq!(previous_request, None);
    /// assert_eq!(stream.truncate_before, Some(requested_position));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_stream<Output>(
        &self,
        name: &str,
        mut update: impl FnMut(&mut StreamRecord) -> Result<Output, MetaError> + Send,
    ) -> Result<(StreamRecord, Output), MetaError> {
        let stream_key = stream_key(name);
        for _attempt in 0..MAX_UPDATE_ATTEMPTS {
            let stored = self.read_stream(name).await?;
            let mut updated_record = stored.record.clone();
            let output = update(&mut updated_record)?;
            if updated_record == stored.record {
                return Ok((stored.record, output));
            }
            updated_record.schema_version = RECORD_SCHEMA_VERSION;
            let was_written = self
                .replace_record(&stream_key, stored.revision, &updated_record)
                .await?;
            if was_written {
                return Ok((updated_record, output));
            }
        }
        Err(too_much_contention(&stream_key))
    }
}

/// Builds the record of a stream that was just created: no segments, no writer, epoch zero, and nothing truncated.
fn new_stream_record(
    stream_id: StreamId,
    name: &str,
    replication: ReplicationSettings,
    retention: RetentionPolicy,
    standby_replicas: Option<Vec<ReplicaId>>,
) -> StreamRecord {
    StreamRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        stream_id,
        name: name.to_owned(),
        replication,
        retention,
        epoch: Epoch::ZERO,
        writer: None,
        segments: Vec::new(),
        next_segment_id: SegmentId::FIRST,
        standby_replicas,
        truncate_before: None,
        low_water_mark: SequenceNumber::FIRST,
        materializers: BTreeSet::new(),
    }
}

#[cfg(test)]
#[path = "streams_test.rs"]
mod tests;
