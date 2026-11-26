//! What the tests of the metadata store's operations share.
//!
//! # Why this file exists
//!
//! The operations are tested in one file per subject, and almost every test starts from the same few situations: an
//! empty store, a store with a stream, a store whose stream has a writer and an open segment. Building those in each
//! file would be five copies of the same setup that drift apart. They are built here once.
//!
//! # What it contains
//!
//! - [`STREAM`], [`LEASE_EXPIRY`], [`SEALED_AT`], [`NOW`], [`LEADERSHIP_EXPIRY`]: the names and moments the tests use.
//! - [`empty_store`], [`empty_store_with_backend`], [`store_with_stream`], [`store_with_open_segment`]: the starting
//!   situations.
//! - [`store_that_loses_every_conditional_write`]: a store on which no read-modify-write ever wins.
//! - [`create_stream`], [`place_standby`], [`seal_first_segment_at`], [`stream_id_of`]: single steps that several tests
//!   take.
//! - [`standby`], [`replica_id`], [`replica`]: values.
//!
//! # How it works
//!
//! Every store is a [`MetadataStore`] over a fresh [`MemoryKeyValueStore`]. A test that needs to look at the stored
//! bytes, or to switch the backend off, asks for [`empty_store_with_backend`] and gets both handles; they share the
//! same data.
//!
//! The helpers panic, with a message that says which step of the setup failed, when the store refuses something: a
//! setup that cannot be built is a broken test, not a case to handle.
//!
//! # Invariants
//!
//! - A fixture never shares state with another test: each call builds a new backend.
//!
//! # See also
//!
//! - The `*_test.rs` files next to this one: the users.

use std::sync::Arc;

use async_trait::async_trait;
use bitacora_types::{Epoch, ReplicaId, ReplicationSettings, SegmentId, SequenceNumber, StreamId, Timestamp};

use super::MetadataStore;
use crate::key_value::{Condition, KeyValueStore, Mutation, StoreUnavailable, TransactionOutcome, VersionedValue};
use crate::memory_store::MemoryKeyValueStore;
use crate::records::{RECORD_SCHEMA_VERSION, ReplicaRecord, RetentionPolicy, StreamRecord};

/// The stream most tests work on.
pub(crate) const STREAM: &str = "orders";

/// When the lease of a writer in these tests lapses.
pub(crate) const LEASE_EXPIRY: Timestamp = Timestamp::from_unix_millis(10_000);

/// The moment the tests seal segments at.
pub(crate) const SEALED_AT: Timestamp = Timestamp::from_unix_millis(5_000);

/// The current time of the tests that need one.
pub(crate) const NOW: Timestamp = Timestamp::from_unix_millis(1_000);

/// When a leadership taken at [`NOW`] lapses.
pub(crate) const LEADERSHIP_EXPIRY: Timestamp = Timestamp::from_unix_millis(6_000);

/// A metadata store together with the backend it writes to, for tests that look underneath.
pub(crate) struct StoreWithBackend {
    /// The store under test.
    pub(crate) store: MetadataStore,
    /// The backend of `store`. Reads the stored bytes and simulates outages.
    pub(crate) key_value_store: MemoryKeyValueStore,
}

/// Returns a store that holds nothing, with a handle to its backend.
pub(crate) fn empty_store_with_backend() -> StoreWithBackend {
    let key_value_store = MemoryKeyValueStore::new();
    StoreWithBackend {
        store: MetadataStore::new(Arc::new(key_value_store.clone())),
        key_value_store,
    }
}

/// Returns a store that holds nothing.
pub(crate) fn empty_store() -> MetadataStore {
    empty_store_with_backend().store
}

/// A backend on which every conditional write loses, as if another writer always got in between the read and the write.
///
/// Reads and unconditional writes go to the in-memory store it wraps.
struct ContendedKeyValueStore {
    /// The store that holds the data.
    key_value_store: MemoryKeyValueStore,
}

#[async_trait]
impl KeyValueStore for ContendedKeyValueStore {
    /// Reads from the wrapped store.
    async fn get(&self, key: &str) -> Result<Option<VersionedValue>, StoreUnavailable> {
        self.key_value_store.get(key).await
    }

    /// Lists from the wrapped store.
    async fn list(&self, key_prefix: &str) -> Result<Vec<(String, VersionedValue)>, StoreUnavailable> {
        self.key_value_store.list(key_prefix).await
    }

    /// Refuses every transaction that has a condition, and applies the others to the wrapped store.
    async fn transact(
        &self,
        conditions: Vec<Condition>,
        mutations: Vec<Mutation>,
    ) -> Result<TransactionOutcome, StoreUnavailable> {
        if conditions.is_empty() {
            return self.key_value_store.transact(conditions, mutations).await;
        }
        Ok(TransactionOutcome::ConditionFailed)
    }
}

/// Returns a store over the data of `key_value_store` on which every conditional write loses.
///
/// It stands for a store so busy, or a bug so persistent, that a read-modify-write never wins. Build the starting
/// situation through an ordinary store over the same backend first.
pub(crate) fn store_that_loses_every_conditional_write(key_value_store: &MemoryKeyValueStore) -> MetadataStore {
    MetadataStore::new(Arc::new(ContendedKeyValueStore {
        key_value_store: key_value_store.clone(),
    }))
}

/// Returns the replica identifier `name`.
pub(crate) fn replica_id(name: &str) -> ReplicaId {
    ReplicaId::new(name).expect("a valid identifier")
}

/// Returns the replica set the tests place for a stream's next segment.
pub(crate) fn standby() -> Vec<ReplicaId> {
    vec![
        replica_id("replica-a"),
        replica_id("replica-b"),
        replica_id("replica-c"),
    ]
}

/// Creates a stream called `name` with the default replication, no retention and no standby.
pub(crate) async fn create_stream(store: &MetadataStore, name: &str) -> StreamRecord {
    store
        .create_stream(name, ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
        .await
        .expect("the stream is created")
}

/// Returns a store with the stream [`STREAM`]: standby placed, no writer yet.
pub(crate) async fn store_with_stream() -> MetadataStore {
    let store = empty_store();
    store
        .create_stream(
            STREAM,
            ReplicationSettings::DEFAULT,
            RetentionPolicy::default(),
            Some(standby()),
        )
        .await
        .expect("the stream is created");
    store
}

/// Returns a store where `compute-1` holds epoch 1 of [`STREAM`] and segment 1 is open. The standby was used up by that
/// segment.
pub(crate) async fn store_with_open_segment() -> MetadataStore {
    let store = store_with_stream().await;
    store
        .claim_epoch(STREAM, Epoch::ZERO, "compute-1", LEASE_EXPIRY)
        .await
        .expect("the epoch is claimed");
    store
        .open_segment(STREAM, Epoch::new(1))
        .await
        .expect("the segment is opened");
    store
}

/// Places a standby replica set for the next segment of [`STREAM`], as the controller does.
pub(crate) async fn place_standby(store: &MetadataStore) {
    store
        .update_stream(STREAM, |record| {
            record.standby_replicas = Some(standby());
            Ok(())
        })
        .await
        .expect("the standby is placed");
}

/// Seals segment 1 of [`STREAM`] at `sealed_end`, as the writer of epoch 1.
pub(crate) async fn seal_first_segment_at(store: &MetadataStore, sealed_end: u64) -> StreamRecord {
    store
        .seal_segment(
            STREAM,
            Epoch::new(1),
            SegmentId::FIRST,
            SequenceNumber::new(sealed_end),
            SEALED_AT,
        )
        .await
        .expect("the segment is sealed")
}

/// Returns the identifier of [`STREAM`].
pub(crate) async fn stream_id_of(store: &MetadataStore) -> StreamId {
    store.get_stream(STREAM).await.expect("the stream exists").stream_id
}

/// Returns the registration of a replica called `name` that last renewed it at `heartbeat_at_millis`.
pub(crate) fn replica(name: &str, heartbeat_at_millis: u64) -> ReplicaRecord {
    ReplicaRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        replica_id: replica_id(name),
        address: format!("{name}:7000"),
        failure_domains: vec!["zone-a".to_owned()],
        capacity_bytes: 1_000,
        free_bytes: 900,
        is_draining: false,
        heartbeat_at: Timestamp::from_unix_millis(heartbeat_at_millis),
    }
}
