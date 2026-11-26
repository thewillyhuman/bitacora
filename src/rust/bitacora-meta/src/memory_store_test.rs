//! Tests of the in-memory key-value store, which are the tests of the [`KeyValueStore`] interface.
//!
//! # What is covered
//!
//! - Reads: a missing key, a written value, a listing by prefix.
//! - Revisions: every write changes the revision of its key, a recreated key never repeats one, and one transaction
//!   writes all its keys at one revision.
//! - Conditions: on absence and on a revision, when they hold and when they do not, and that a failed transaction
//!   changes nothing.
//! - The outage switch, and that clones share their data.
//!
//! The etcd backend runs the same cases against a real etcd, in `bitacora-meta-etcd/tests/etcd/key_value_contract.rs`.
//!
//! # How the fixtures work
//!
//! `put` writes one key without conditions. `revision_of` reads the revision a key has now. The tests use the key of a
//! stream record because that is what the store holds in practice; the values are arbitrary bytes.

use bytes::Bytes;

use super::*;

/// The key most tests read and write.
const STREAM_KEY: &str = "streams/orders";

/// A mutation that writes `value` under `key`.
fn put_mutation(key: &str, value: &'static [u8]) -> Mutation {
    Mutation::Put {
        key: key.to_owned(),
        value: Bytes::from_static(value),
    }
}

/// A mutation that deletes `key`.
fn delete_mutation(key: &str) -> Mutation {
    Mutation::Delete { key: key.to_owned() }
}

/// A condition that holds only while `key` does not exist.
fn is_absent(key: &str) -> Condition {
    Condition::IsAbsent { key: key.to_owned() }
}

/// A condition that holds only while `key` is at `revision`.
fn has_revision(key: &str, revision: Revision) -> Condition {
    Condition::HasRevision {
        key: key.to_owned(),
        revision,
    }
}

/// Writes one key without conditions.
async fn put(
    store: &MemoryKeyValueStore,
    key: &str,
    value: &'static [u8],
) -> Result<TransactionOutcome, StoreUnavailable> {
    store.transact(Vec::new(), vec![put_mutation(key, value)]).await
}

/// The revision `key` has now. The key must exist.
async fn revision_of(store: &MemoryKeyValueStore, key: &str) -> Revision {
    let stored = store
        .get(key)
        .await
        .expect("the store is available")
        .expect("the key was written by the test");
    stored.revision
}

/// The value `key` has now, or `None` if it does not exist.
async fn value_of(store: &MemoryKeyValueStore, key: &str) -> Option<Bytes> {
    let stored = store.get(key).await.expect("the store is available")?;
    Some(stored.value)
}

#[tokio::test]
async fn missing_key_reads_as_absent() {
    let store = MemoryKeyValueStore::new();

    let read = store.get(STREAM_KEY).await;

    assert_eq!(read, Ok(None));
}

#[tokio::test]
async fn written_value_is_read_back() {
    let store = MemoryKeyValueStore::new();

    put(&store, STREAM_KEY, b"v1").await.expect("the store is available");

    assert_eq!(value_of(&store, STREAM_KEY).await, Some(Bytes::from_static(b"v1")));
}

#[tokio::test]
async fn every_write_changes_the_revision() {
    let store = MemoryKeyValueStore::new();
    put(&store, STREAM_KEY, b"v1").await.expect("the store is available");
    let first_revision = revision_of(&store, STREAM_KEY).await;

    put(&store, STREAM_KEY, b"v1").await.expect("the store is available");

    assert!(revision_of(&store, STREAM_KEY).await > first_revision);
}

#[tokio::test]
async fn recreated_key_never_repeats_a_revision() {
    let store = MemoryKeyValueStore::new();
    put(&store, STREAM_KEY, b"v1").await.expect("the store is available");
    let first_revision = revision_of(&store, STREAM_KEY).await;
    store
        .transact(Vec::new(), vec![delete_mutation(STREAM_KEY)])
        .await
        .expect("the store is available");

    put(&store, STREAM_KEY, b"v2").await.expect("the store is available");

    assert!(revision_of(&store, STREAM_KEY).await > first_revision);
}

#[tokio::test]
async fn transaction_applies_when_the_key_is_unchanged() {
    let store = MemoryKeyValueStore::new();
    put(&store, STREAM_KEY, b"v1").await.expect("the store is available");
    let read_revision = revision_of(&store, STREAM_KEY).await;

    let outcome = store
        .transact(
            vec![has_revision(STREAM_KEY, read_revision)],
            vec![put_mutation(STREAM_KEY, b"v2")],
        )
        .await;

    assert_eq!(outcome, Ok(TransactionOutcome::Applied));
}

#[tokio::test]
async fn transaction_fails_when_someone_wrote_in_between() {
    let store = MemoryKeyValueStore::new();
    put(&store, STREAM_KEY, b"v1").await.expect("the store is available");
    let read_revision = revision_of(&store, STREAM_KEY).await;
    put(&store, STREAM_KEY, b"other").await.expect("the store is available");

    let outcome = store
        .transact(
            vec![has_revision(STREAM_KEY, read_revision)],
            vec![put_mutation(STREAM_KEY, b"v2")],
        )
        .await;

    assert_eq!(outcome, Ok(TransactionOutcome::ConditionFailed));
}

#[tokio::test]
async fn failed_transaction_changes_nothing() {
    let store = MemoryKeyValueStore::new();
    put(&store, STREAM_KEY, b"v1").await.expect("the store is available");

    store
        .transact(
            vec![is_absent(STREAM_KEY)],
            vec![put_mutation(STREAM_KEY, b"v2"), put_mutation("streams/other", b"v2")],
        )
        .await
        .expect("the store is available");

    let listed = store.list("streams/").await.expect("the store is available");
    assert_eq!(listed.len(), 1, "the failed transaction must not create streams/other");
    assert_eq!(value_of(&store, STREAM_KEY).await, Some(Bytes::from_static(b"v1")));
}

#[tokio::test]
async fn absence_condition_holds_for_a_missing_key() {
    let store = MemoryKeyValueStore::new();

    let outcome = store
        .transact(vec![is_absent(STREAM_KEY)], vec![put_mutation(STREAM_KEY, b"v1")])
        .await;

    assert_eq!(outcome, Ok(TransactionOutcome::Applied));
}

#[tokio::test]
async fn absence_condition_fails_for_an_existing_key() {
    let store = MemoryKeyValueStore::new();
    put(&store, STREAM_KEY, b"v1").await.expect("the store is available");

    let outcome = store
        .transact(vec![is_absent(STREAM_KEY)], vec![put_mutation(STREAM_KEY, b"v2")])
        .await;

    assert_eq!(outcome, Ok(TransactionOutcome::ConditionFailed));
}

#[tokio::test]
async fn revision_condition_fails_for_a_missing_key() {
    let store = MemoryKeyValueStore::new();

    let outcome = store
        .transact(
            vec![has_revision(STREAM_KEY, Revision::new(1))],
            vec![put_mutation(STREAM_KEY, b"v1")],
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
        .expect("the store is available");

    assert_eq!(revision_of(&store, "a").await, revision_of(&store, "b").await);
}

#[tokio::test]
async fn list_returns_only_the_prefix_sorted_by_key() {
    let store = MemoryKeyValueStore::new();
    for key in ["streams/b", "replicas/x", "streams/a", "streamsX"] {
        put(&store, key, b"v").await.expect("the store is available");
    }

    let listed = store.list("streams/").await.expect("the store is available");

    let mut listed_keys = Vec::new();
    for (key, _stored) in listed {
        listed_keys.push(key);
    }
    assert_eq!(listed_keys, ["streams/a", "streams/b"]);
}

#[tokio::test]
async fn list_of_the_empty_prefix_returns_every_key() {
    let store = MemoryKeyValueStore::new();
    for key in ["streams/a", "replicas/x"] {
        put(&store, key, b"v").await.expect("the store is available");
    }

    let listed = store.list("").await.expect("the store is available");

    assert_eq!(listed.len(), 2);
}

#[tokio::test]
async fn deleting_a_missing_key_is_not_an_error() {
    let store = MemoryKeyValueStore::new();

    let outcome = store.transact(Vec::new(), vec![delete_mutation(STREAM_KEY)]).await;

    assert_eq!(outcome, Ok(TransactionOutcome::Applied));
}

#[tokio::test]
async fn deleted_key_reads_as_absent() {
    let store = MemoryKeyValueStore::new();
    put(&store, STREAM_KEY, b"v1").await.expect("the store is available");

    store
        .transact(Vec::new(), vec![delete_mutation(STREAM_KEY)])
        .await
        .expect("the store is available");

    assert_eq!(store.get(STREAM_KEY).await, Ok(None));
}

#[tokio::test]
async fn unavailable_store_fails_reads() {
    let store = MemoryKeyValueStore::new();
    store.set_unavailable(true);

    let read = store.get(STREAM_KEY).await;
    let listing = store.list("").await;

    assert!(read.is_err(), "{read:?}");
    assert!(listing.is_err(), "{listing:?}");
}

#[tokio::test]
async fn unavailable_store_fails_writes_without_changing() {
    let store = MemoryKeyValueStore::new();
    put(&store, STREAM_KEY, b"v1").await.expect("the store is available");
    store.set_unavailable(true);

    let write = put(&store, STREAM_KEY, b"v2").await;

    store.set_unavailable(false);
    assert!(write.is_err(), "{write:?}");
    assert_eq!(value_of(&store, STREAM_KEY).await, Some(Bytes::from_static(b"v1")));
}

#[tokio::test]
async fn clones_share_the_same_data() {
    let store = MemoryKeyValueStore::new();
    let other_client = store.clone();

    put(&store, STREAM_KEY, b"v1").await.expect("the store is available");

    assert_eq!(
        value_of(&other_client, STREAM_KEY).await,
        Some(Bytes::from_static(b"v1"))
    );
}
