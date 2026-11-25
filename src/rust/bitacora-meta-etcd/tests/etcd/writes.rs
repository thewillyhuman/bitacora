//! Shorthand for the writes and reads that tests set the stage with.
//!
//! # Why this file exists
//!
//! The key-value interface has one way to write, a transaction, and one
//! way to learn a revision, a read. Spelled out in full they take more
//! lines than the behaviour a test is about. These functions name the
//! common cases once, for every test module.
//!
//! # What it contains
//!
//! - [`put`]: writes one key without conditions.
//! - [`put_mutation`] and [`delete_mutation`]: the parts of a transaction.
//! - [`revision_of`]: the current revision of a key that exists.
//!
//! # How it works
//!
//! Everything goes through the `KeyValueStore` interface, never through an
//! etcd client of its own: the tests then exercise the store for the
//! set-up too, and what they set up is what the store would have written.

use bitacora_meta::{KeyValueStore, Mutation, Revision, StoreUnavailable, TransactionOutcome};
use bitacora_meta_etcd::EtcdKeyValueStore;
use bytes::Bytes;

/// Writes `value` under `key`, whatever the key held before.
///
/// # Errors
///
/// Fails with [`StoreUnavailable`] when etcd cannot be reached; tests that
/// expect it to be reachable call `expect` on the result.
pub(crate) async fn put(
    store: &EtcdKeyValueStore,
    key: &str,
    value: &'static [u8],
) -> Result<TransactionOutcome, StoreUnavailable> {
    store
        .transact(Vec::new(), vec![put_mutation(key, value)])
        .await
}

/// Returns the mutation that writes `value` under `key`.
pub(crate) fn put_mutation(key: &str, value: &'static [u8]) -> Mutation {
    Mutation::Put {
        key: key.to_owned(),
        value: Bytes::from_static(value),
    }
}

/// Returns the mutation that removes `key`.
pub(crate) fn delete_mutation(key: &str) -> Mutation {
    Mutation::Delete {
        key: key.to_owned(),
    }
}

/// Reads the revision `key` has now.
///
/// # Panics
///
/// Panics when etcd cannot be reached or the key does not exist: a test
/// that asks for a revision has just written the key.
pub(crate) async fn revision_of(store: &EtcdKeyValueStore, key: &str) -> Revision {
    let stored = store.get(key).await.expect("etcd is reachable");
    stored.expect("the key exists").revision
}
