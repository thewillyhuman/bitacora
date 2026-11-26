//! A [`KeyValueStore`] that keeps everything in memory.
//!
//! # Why this file exists
//!
//! Almost every test in the workspace needs a metadata store: the unit tests of this crate, the tests of the client and
//! the controller, and above all the deterministic simulator, which must be able to run thousands of clusters in one
//! process and take the store away from them at will. A real etcd is too slow for that and cannot be made to fail on
//! demand. This is the backend they all use.
//!
//! # What it contains
//!
//! - [`MemoryKeyValueStore`]: the store, with [`MemoryKeyValueStore::set_unavailable`] to simulate an outage.
//!
//! # How it works
//!
//! The keys live in one sorted map behind one lock. Every write of a transaction gets the same revision, taken from a
//! counter that is shared by all keys and only grows. Sharing the counter is what keeps a key that was deleted and
//! created again from ever repeating a revision.
//!
//! No operation waits while it holds the lock, so the store is linearizable by construction: operations happen one at a
//! time, in the order they take the lock.
//!
//! # Invariants
//!
//! - It honours [`KeyValueStore`] exactly; the etcd backend is tested with the same cases, and a difference between the
//!   two is a bug in one of them.
//! - While it is unavailable, every operation fails and nothing changes.
//! - Clones share the same data, like two clients of one cluster.
//!
//! # See also
//!
//! - `key_value.rs`: the interface this implements.
//! - `bitacora-meta-etcd/tests/etcd/key_value_contract.rs`: the same cases against a real etcd.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;

use crate::key_value::{
    Condition, KeyValueStore, Mutation, Revision, StoreUnavailable, TransactionOutcome, VersionedValue,
};

/// The reason every operation gives while a test has switched the store off.
const INJECTED_OUTAGE_REASON: &str = "injected outage";

/// What the store holds, behind its lock.
#[derive(Debug, Default)]
struct MemoryStoreState {
    /// The stored values with their revisions. Sorted by key, which is the order a listing must have.
    entries_by_key: BTreeMap<String, VersionedValue>,
    /// The revision given to the most recent transaction. Shared by all keys, so a key that is deleted and created
    /// again never repeats a revision.
    last_revision: u64,
    /// While set, every operation of the interface fails with [`StoreUnavailable`].
    is_unavailable: bool,
}

/// A metadata store backend that keeps everything in memory.
///
/// Use it wherever a test or a simulation needs a metadata store. Nothing is durable: the data is gone when the last
/// clone is dropped. Clones share the same data, like two clients of one cluster.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_meta::MetaError> {
/// use std::sync::Arc;
///
/// use bitacora_meta::{MemoryKeyValueStore, MetadataStore};
///
/// let key_value_store = MemoryKeyValueStore::new();
/// let metadata = MetadataStore::new(Arc::new(key_value_store.clone()));
///
/// key_value_store.set_unavailable(true);
/// let during_the_outage = metadata.list_streams().await;
/// key_value_store.set_unavailable(false);
/// let after_the_outage = metadata.list_streams().await?;
///
/// assert!(during_the_outage.is_err());
/// assert!(after_the_outage.is_empty());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default)]
pub struct MemoryKeyValueStore {
    /// The data, shared by every clone of the store.
    state: Arc<Mutex<MemoryStoreState>>,
}

impl MemoryKeyValueStore {
    /// Creates a store that holds no keys and is available.
    ///
    /// # Returns
    ///
    /// The empty store.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::StoreUnavailable> {
    /// use bitacora_meta::{KeyValueStore, MemoryKeyValueStore};
    ///
    /// let key_value_store = MemoryKeyValueStore::new();
    ///
    /// assert!(key_value_store.list("").await?.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Simulates an outage, or ends one.
    ///
    /// While the store is unavailable, every operation of [`KeyValueStore`] fails with [`StoreUnavailable`] and changes
    /// nothing. The switch affects every clone.
    ///
    /// # Arguments
    ///
    /// * `is_unavailable` - `true` starts the outage, `false` ends it.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// use bitacora_meta::{KeyValueStore, MemoryKeyValueStore};
    ///
    /// let key_value_store = MemoryKeyValueStore::new();
    ///
    /// key_value_store.set_unavailable(true);
    ///
    /// assert!(key_value_store.get("streams/orders").await.is_err());
    /// # }
    /// ```
    pub fn set_unavailable(&self, is_unavailable: bool) {
        self.lock_state().is_unavailable = is_unavailable;
    }

    /// Locks the store's state, whether or not another holder of the lock panicked.
    ///
    /// Every critical section finishes its change before anything that can panic, so a poisoned lock still guards
    /// consistent data.
    fn lock_state(&self) -> MutexGuard<'_, MemoryStoreState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Locks the store's state for one operation of the interface, unless a test has switched the store off.
    fn lock_available_state(&self) -> Result<MutexGuard<'_, MemoryStoreState>, StoreUnavailable> {
        let state = self.lock_state();
        if state.is_unavailable {
            return Err(StoreUnavailable {
                reason: INJECTED_OUTAGE_REASON.to_owned(),
            });
        }
        Ok(state)
    }
}

/// Says whether `condition` holds for the stored entries.
fn holds(condition: &Condition, entries_by_key: &BTreeMap<String, VersionedValue>) -> bool {
    match condition {
        Condition::IsAbsent { key } => !entries_by_key.contains_key(key),
        Condition::HasRevision { key, revision } => match entries_by_key.get(key) {
            Some(stored) => stored.revision == *revision,
            None => false,
        },
    }
}

#[async_trait]
impl KeyValueStore for MemoryKeyValueStore {
    /// Looks the key up in the map.
    async fn get(&self, key: &str) -> Result<Option<VersionedValue>, StoreUnavailable> {
        let state = self.lock_available_state()?;
        Ok(state.entries_by_key.get(key).cloned())
    }

    /// Walks the sorted map from the prefix on, until a key no longer starts with it.
    async fn list(&self, key_prefix: &str) -> Result<Vec<(String, VersionedValue)>, StoreUnavailable> {
        let state = self.lock_available_state()?;
        let mut listed = Vec::new();
        for (key, stored) in state.entries_by_key.range(key_prefix.to_owned()..) {
            if !key.starts_with(key_prefix) {
                break;
            }
            listed.push((key.clone(), stored.clone()));
        }
        Ok(listed)
    }

    /// Checks every condition and applies every mutation while holding the lock, so nothing can happen in between. All
    /// keys written get one new revision.
    async fn transact(
        &self,
        conditions: Vec<Condition>,
        mutations: Vec<Mutation>,
    ) -> Result<TransactionOutcome, StoreUnavailable> {
        let mut state = self.lock_available_state()?;
        for condition in &conditions {
            if !holds(condition, &state.entries_by_key) {
                return Ok(TransactionOutcome::ConditionFailed);
            }
        }
        state.last_revision += 1;
        let revision = Revision::new(state.last_revision);
        for mutation in mutations {
            match mutation {
                Mutation::Put { key, value } => {
                    state.entries_by_key.insert(key, VersionedValue { value, revision });
                }
                Mutation::Delete { key } => {
                    state.entries_by_key.remove(&key);
                }
            }
        }
        Ok(TransactionOutcome::Applied)
    }
}

#[cfg(test)]
#[path = "memory_store_test.rs"]
mod tests;
