//! An in-memory [`KeyValueStore`], for tests, simulation and single-process
//! deployments that need no durability.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;

use crate::key_value::{
    Condition, KeyValueStore, Mutation, Revision, StoreUnavailable, TransactionOutcome, VersionedValue,
};

#[derive(Debug, Default)]
struct MemoryStoreState {
    entries_by_key: BTreeMap<String, VersionedValue>,
    /// The revision given to the most recent write. Shared by all keys, so a
    /// key that is deleted and created again never repeats a revision.
    last_revision: u64,
    is_unavailable: bool,
}

/// An in-memory metadata store. Clones share the same data, like two
/// clients of one cluster.
#[derive(Clone, Debug, Default)]
pub struct MemoryKeyValueStore {
    state: Arc<Mutex<MemoryStoreState>>,
}

impl MemoryKeyValueStore {
    /// Creates an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Simulates an outage: while unavailable, every operation fails and
    /// nothing changes.
    pub fn set_unavailable(&self, is_unavailable: bool) {
        self.lock().is_unavailable = is_unavailable;
    }

    fn lock(&self) -> MutexGuard<'_, MemoryStoreState> {
        // Every critical section finishes its change before anything that
        // can panic, so a poisoned lock still guards consistent data.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn available_state(&self) -> Result<MutexGuard<'_, MemoryStoreState>, StoreUnavailable> {
        let state = self.lock();
        if state.is_unavailable {
            return Err(StoreUnavailable {
                reason: "injected outage".to_owned(),
            });
        }
        Ok(state)
    }
}

fn holds(condition: &Condition, entries_by_key: &BTreeMap<String, VersionedValue>) -> bool {
    match condition {
        Condition::IsAbsent { key } => !entries_by_key.contains_key(key),
        Condition::HasRevision { key, revision } => entries_by_key
            .get(key)
            .is_some_and(|stored| stored.revision == *revision),
    }
}

#[async_trait]
impl KeyValueStore for MemoryKeyValueStore {
    async fn get(&self, key: &str) -> Result<Option<VersionedValue>, StoreUnavailable> {
        Ok(self.available_state()?.entries_by_key.get(key).cloned())
    }

    async fn list(&self, key_prefix: &str) -> Result<Vec<(String, VersionedValue)>, StoreUnavailable> {
        Ok(self
            .available_state()?
            .entries_by_key
            .range(key_prefix.to_owned()..)
            .take_while(|(key, _stored)| key.starts_with(key_prefix))
            .map(|(key, stored)| (key.clone(), stored.clone()))
            .collect())
    }

    async fn transact(
        &self,
        conditions: Vec<Condition>,
        mutations: Vec<Mutation>,
    ) -> Result<TransactionOutcome, StoreUnavailable> {
        let mut state = self.available_state()?;
        if !conditions
            .iter()
            .all(|condition| holds(condition, &state.entries_by_key))
        {
            return Ok(TransactionOutcome::ConditionFailed);
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
mod tests {
    use bytes::Bytes;

    use super::*;

    /// Unconditionally writes one key.
    async fn put(
        store: &MemoryKeyValueStore,
        key: &str,
        value: &'static [u8],
    ) -> Result<TransactionOutcome, StoreUnavailable> {
        store.transact(Vec::new(), vec![put_mutation(key, value)]).await
    }

    async fn revision_of(store: &MemoryKeyValueStore, key: &str) -> Revision {
        store
            .get(key)
            .await
            .expect("store is available")
            .expect("key exists")
            .revision
    }

    fn put_mutation(key: &str, value: &'static [u8]) -> Mutation {
        Mutation::Put {
            key: key.to_owned(),
            value: Bytes::from_static(value),
        }
    }

    #[tokio::test]
    async fn missing_key_reads_as_absent() {
        let store = MemoryKeyValueStore::new();
        assert_eq!(store.get("streams/orders").await, Ok(None));
    }

    #[tokio::test]
    async fn written_value_is_read_back() {
        let store = MemoryKeyValueStore::new();
        put(&store, "streams/orders", b"v1").await.expect("put");
        let stored = store.get("streams/orders").await.expect("get");
        assert_eq!(stored.map(|stored| stored.value), Some(Bytes::from_static(b"v1")));
    }

    #[tokio::test]
    async fn every_write_changes_the_revision() {
        let store = MemoryKeyValueStore::new();
        put(&store, "streams/orders", b"v1").await.expect("put");
        let first_revision = revision_of(&store, "streams/orders").await;
        put(&store, "streams/orders", b"v1").await.expect("put");
        assert!(revision_of(&store, "streams/orders").await > first_revision);
    }

    #[tokio::test]
    async fn recreated_key_never_repeats_a_revision() {
        let store = MemoryKeyValueStore::new();
        put(&store, "streams/orders", b"v1").await.expect("put");
        let first_revision = revision_of(&store, "streams/orders").await;
        store
            .transact(
                Vec::new(),
                vec![Mutation::Delete {
                    key: "streams/orders".to_owned(),
                }],
            )
            .await
            .expect("delete");
        put(&store, "streams/orders", b"v2").await.expect("put");
        assert!(revision_of(&store, "streams/orders").await > first_revision);
    }

    #[tokio::test]
    async fn transaction_applies_when_the_key_is_unchanged() {
        let store = MemoryKeyValueStore::new();
        put(&store, "streams/orders", b"v1").await.expect("put");
        let read_revision = revision_of(&store, "streams/orders").await;
        let outcome = store
            .transact(
                vec![Condition::HasRevision {
                    key: "streams/orders".to_owned(),
                    revision: read_revision,
                }],
                vec![put_mutation("streams/orders", b"v2")],
            )
            .await;
        assert_eq!(outcome, Ok(TransactionOutcome::Applied));
    }

    #[tokio::test]
    async fn transaction_fails_when_someone_wrote_in_between() {
        let store = MemoryKeyValueStore::new();
        put(&store, "streams/orders", b"v1").await.expect("put");
        let read_revision = revision_of(&store, "streams/orders").await;
        put(&store, "streams/orders", b"other").await.expect("put");
        let outcome = store
            .transact(
                vec![Condition::HasRevision {
                    key: "streams/orders".to_owned(),
                    revision: read_revision,
                }],
                vec![put_mutation("streams/orders", b"v2")],
            )
            .await;
        assert_eq!(outcome, Ok(TransactionOutcome::ConditionFailed));
    }

    #[tokio::test]
    async fn failed_transaction_changes_nothing() {
        let store = MemoryKeyValueStore::new();
        put(&store, "streams/orders", b"v1").await.expect("put");
        store
            .transact(
                vec![Condition::IsAbsent {
                    key: "streams/orders".to_owned(),
                }],
                vec![
                    put_mutation("streams/orders", b"v2"),
                    put_mutation("streams/other", b"v2"),
                ],
            )
            .await
            .expect("transact");
        assert_eq!(
            store.list("streams/").await.expect("list").len(),
            1,
            "the failed transaction must not create streams/other"
        );
    }

    #[tokio::test]
    async fn absence_condition_holds_for_a_missing_key() {
        let store = MemoryKeyValueStore::new();
        let outcome = store
            .transact(
                vec![Condition::IsAbsent {
                    key: "streams/orders".to_owned(),
                }],
                vec![put_mutation("streams/orders", b"v1")],
            )
            .await;
        assert_eq!(outcome, Ok(TransactionOutcome::Applied));
    }

    #[tokio::test]
    async fn revision_condition_fails_for_a_missing_key() {
        let store = MemoryKeyValueStore::new();
        let outcome = store
            .transact(
                vec![Condition::HasRevision {
                    key: "streams/orders".to_owned(),
                    revision: Revision::new(1),
                }],
                vec![put_mutation("streams/orders", b"v1")],
            )
            .await;
        assert_eq!(outcome, Ok(TransactionOutcome::ConditionFailed));
    }

    #[tokio::test]
    async fn transaction_writes_all_its_keys_at_one_revision() {
        let store = MemoryKeyValueStore::new();
        store
            .transact(Vec::new(), vec![put_mutation("a", b"1"), put_mutation("b", b"2")])
            .await
            .expect("transact");
        assert_eq!(revision_of(&store, "a").await, revision_of(&store, "b").await);
    }

    #[tokio::test]
    async fn list_returns_only_the_prefix_sorted_by_key() {
        let store = MemoryKeyValueStore::new();
        for key in ["streams/b", "replicas/x", "streams/a", "streamsX"] {
            put(&store, key, b"v").await.expect("put");
        }
        let listed_keys: Vec<String> = store
            .list("streams/")
            .await
            .expect("list")
            .into_iter()
            .map(|(key, _stored)| key)
            .collect();
        assert_eq!(listed_keys, ["streams/a", "streams/b"]);
    }

    #[tokio::test]
    async fn deleting_a_missing_key_is_not_an_error() {
        let store = MemoryKeyValueStore::new();
        let outcome = store
            .transact(
                Vec::new(),
                vec![Mutation::Delete {
                    key: "streams/orders".to_owned(),
                }],
            )
            .await;
        assert_eq!(outcome, Ok(TransactionOutcome::Applied));
    }

    #[tokio::test]
    async fn unavailable_store_fails_reads_and_writes_without_changing() {
        let store = MemoryKeyValueStore::new();
        put(&store, "streams/orders", b"v1").await.expect("put");
        store.set_unavailable(true);
        let read = store.get("streams/orders").await;
        let write = put(&store, "streams/orders", b"v2").await;
        store.set_unavailable(false);
        let stored = store.get("streams/orders").await.expect("get");
        assert!(
            read.is_err() && write.is_err() && stored.map(|stored| stored.value) == Some(Bytes::from_static(b"v1"))
        );
    }

    #[tokio::test]
    async fn clones_share_the_same_data() {
        let store = MemoryKeyValueStore::new();
        let other_client = store.clone();
        put(&store, "streams/orders", b"v1").await.expect("put");
        assert!(other_client.get("streams/orders").await.expect("get").is_some());
    }
}
