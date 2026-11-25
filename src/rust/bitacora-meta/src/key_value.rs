//! What bitacora requires from the system that holds its metadata.
//!
//! The requirements are deliberately few (design, section 8): linearizable
//! reads, and a transaction that applies a handful of writes only if a
//! handful of keys are unchanged. etcd provides this in version 0 and an
//! embedded store can provide it later, without any protocol change.

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

/// Identifies one version of one key. It changes on every write to the key
/// and never repeats, so "the key still has this revision" means "nobody
/// wrote it since I read it".
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Revision(u64);

impl Revision {
    /// Wraps a store-assigned revision number.
    #[must_use]
    pub const fn new(raw_revision: u64) -> Self {
        Self(raw_revision)
    }

    /// Returns the raw revision number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A value together with the revision it was read at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionedValue {
    /// The stored bytes.
    pub value: Bytes,
    /// The revision of the key when it was read.
    pub revision: Revision,
}

/// Something that must hold for a transaction to apply.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Condition {
    /// The key must not exist.
    IsAbsent {
        /// The key that must be absent.
        key: String,
    },
    /// The key must exist and still be at this revision.
    HasRevision {
        /// The key that must be unchanged.
        key: String,
        /// The revision it was read at.
        revision: Revision,
    },
}

/// One write of a transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mutation {
    /// Create or replace a key.
    Put {
        /// The key to write.
        key: String,
        /// Its new value.
        value: Bytes,
    },
    /// Remove a key. Removing an absent key is not an error.
    Delete {
        /// The key to remove.
        key: String,
    },
}

/// Whether a transaction's conditions held.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionOutcome {
    /// Every condition held and every mutation was applied.
    Applied,
    /// A condition did not hold and nothing was changed.
    ConditionFailed,
}

/// The store could not be reached or did not answer. A transaction that
/// fails this way may or may not have been applied; the caller reads again
/// to find out.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("metadata store is unavailable: {reason}")]
pub struct StoreUnavailable {
    /// What went wrong on the way.
    pub reason: String,
}

/// A linearizable key-value store with conditional transactions.
#[async_trait]
pub trait KeyValueStore: Send + Sync + 'static {
    /// Reads one key. The answer reflects every write acknowledged before
    /// the call started.
    async fn get(&self, key: &str) -> Result<Option<VersionedValue>, StoreUnavailable>;

    /// Reads every key that starts with `key_prefix`, sorted by key, as of
    /// one moment.
    async fn list(&self, key_prefix: &str) -> Result<Vec<(String, VersionedValue)>, StoreUnavailable>;

    /// Applies every mutation atomically, but only if every condition holds.
    async fn transact(
        &self,
        conditions: Vec<Condition>,
        mutations: Vec<Mutation>,
    ) -> Result<TransactionOutcome, StoreUnavailable>;
}
