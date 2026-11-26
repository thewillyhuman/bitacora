//! What bitacora requires from the system that holds its metadata.
//!
//! # Why this file exists
//!
//! The metadata store is the one place where bitacora needs consensus (design, section 8). bitacora does not implement
//! consensus; it uses a system that has it: etcd in version 0, perhaps an embedded store later. Everything
//! bitacora-specific must work the same on any of them, and must also run in tests and in the simulator without a
//! network. That needs the requirements written down as one small interface, with nothing in it that only one system
//! can do.
//!
//! # What it contains
//!
//! - [`KeyValueStore`]: the interface a backend implements. Three operations.
//! - [`Revision`] and [`VersionedValue`]: what a read returns, so that a later write can say "only if nobody wrote
//!   since".
//! - [`Condition`], [`Mutation`] and [`TransactionOutcome`]: the parts of a conditional transaction.
//! - [`StoreUnavailable`]: the one way a backend fails.
//!
//! # How it works
//!
//! The requirements are deliberately few: linearizable reads of one key and of a key prefix, and a transaction that
//! applies a handful of writes only if a handful of keys are unchanged. Every operation in `store.rs` is built from
//! those: read a record together with its revision, compute the new record, write it on the condition that the revision
//! is still the one that was read.
//!
//! A revision is an opaque number per key. All that is asked of it is that it changes on every write of the key and is
//! never repeated for that key, not even after the key was deleted and created again. Otherwise "still at this
//! revision" could be true although somebody wrote in between.
//!
//! There is one error. A backend cannot tell a caller anything more useful than "I could not do it, and I do not know
//! whether it happened"; what to do about that is the same whatever the cause.
//!
//! # Invariants
//!
//! - A read reflects every write that was acknowledged before the read started.
//! - A listing is one consistent view: it never mixes the state before a transaction with the state after it.
//! - A transaction applies all of its mutations or none.
//! - A transaction that fails as [`StoreUnavailable`] may or may not have been applied.
//!
//! # See also
//!
//! - `memory_store.rs`: the in-memory backend, whose tests are the tests of this interface.
//! - `bitacora-meta-etcd`: the etcd backend, tested against a real etcd with the same cases.
//! - `store.rs`: everything bitacora builds on the interface.

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

/// Identifies one version of one key.
///
/// It changes on every write to the key and never repeats, so "the key still has this revision" means "nobody wrote it
/// since I read it". The number itself means nothing outside the backend that assigned it; revisions of two different
/// keys cannot be compared.
///
/// # Examples
///
/// ```
/// use bitacora_meta::Revision;
///
/// let revision = Revision::new(41);
///
/// assert_eq!(revision.get(), 41);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Revision(u64);

impl Revision {
    /// Wraps a revision number that a backend assigned.
    ///
    /// # Arguments
    ///
    /// * `raw_revision` - The backend's own number for this version of the key.
    ///
    /// # Returns
    ///
    /// The revision. Only a backend has a reason to build one; everything else passes on what it read.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_meta::Revision;
    ///
    /// assert!(Revision::new(2) > Revision::new(1));
    /// ```
    #[must_use]
    pub const fn new(raw_revision: u64) -> Self {
        Self(raw_revision)
    }

    /// Returns the backend's own number for this revision.
    ///
    /// # Returns
    ///
    /// The number that was given to [`Revision::new`].
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_meta::Revision;
    ///
    /// assert_eq!(Revision::new(7).get(), 7);
    /// ```
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A value together with the revision it was read at.
///
/// The revision is what makes the value useful for more than looking at: a write that names it applies only if the key
/// was not written since.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_meta::StoreUnavailable> {
/// use bitacora_meta::{Condition, KeyValueStore, MemoryKeyValueStore, Mutation, TransactionOutcome};
/// use bytes::Bytes;
///
/// let store = MemoryKeyValueStore::new();
/// let first_write = Mutation::Put {
///     key: "streams/orders".to_owned(),
///     value: Bytes::from_static(b"v1"),
/// };
/// let second_write = Mutation::Put {
///     key: "streams/orders".to_owned(),
///     value: Bytes::from_static(b"v2"),
/// };
/// store.transact(Vec::new(), vec![first_write]).await?;
///
/// let stored = store.get("streams/orders").await?.expect("the key was just written");
/// let still_as_read = Condition::HasRevision {
///     key: "streams/orders".to_owned(),
///     revision: stored.revision,
/// };
/// let outcome = store.transact(vec![still_as_read], vec![second_write]).await?;
///
/// assert_eq!(stored.value, Bytes::from_static(b"v1"));
/// assert_eq!(outcome, TransactionOutcome::Applied);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionedValue {
    /// The stored bytes, exactly as they were written.
    pub value: Bytes,
    /// The revision of the key at the moment it was read.
    pub revision: Revision,
}

/// Something that must hold for a transaction to apply.
///
/// A transaction names the keys its decision was based on. If any of them changed since it was read, the decision may
/// be wrong, and the transaction is not applied.
///
/// # Examples
///
/// A record that must not replace another is created on the condition that its key is absent, so of two creations
/// only the first is applied:
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_meta::StoreUnavailable> {
/// use bitacora_meta::{Condition, KeyValueStore, MemoryKeyValueStore, Mutation, TransactionOutcome};
/// use bytes::Bytes;
///
/// let store = MemoryKeyValueStore::new();
/// let is_absent = Condition::IsAbsent {
///     key: "streams/orders".to_owned(),
/// };
/// let creation = Mutation::Put {
///     key: "streams/orders".to_owned(),
///     value: Bytes::from_static(b"v1"),
/// };
///
/// let first_creation = store.transact(vec![is_absent.clone()], vec![creation.clone()]).await?;
/// let second_creation = store.transact(vec![is_absent], vec![creation]).await?;
///
/// assert_eq!(first_creation, TransactionOutcome::Applied);
/// assert_eq!(second_creation, TransactionOutcome::ConditionFailed);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Condition {
    /// The key must not exist. Used to create a record that must not replace another.
    IsAbsent {
        /// The key that must be absent.
        key: String,
    },
    /// The key must exist and still be at this revision. Used to replace a record only if it is the one that was read.
    HasRevision {
        /// The key that must be unchanged.
        key: String,
        /// The revision it was read at.
        revision: Revision,
    },
}

/// One write of a transaction.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_meta::StoreUnavailable> {
/// use bitacora_meta::{KeyValueStore, MemoryKeyValueStore, Mutation};
/// use bytes::Bytes;
///
/// let store = MemoryKeyValueStore::new();
/// let write = Mutation::Put {
///     key: "streams/orders".to_owned(),
///     value: Bytes::from_static(b"v1"),
/// };
/// let removal = Mutation::Delete {
///     key: "streams/orders".to_owned(),
/// };
///
/// store.transact(Vec::new(), vec![write]).await?;
/// store.transact(Vec::new(), vec![removal]).await?;
///
/// assert_eq!(store.get("streams/orders").await?, None);
/// # Ok(())
/// # }
/// ```
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
///
/// # Examples
///
/// A transaction without conditions has none that can fail:
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_meta::StoreUnavailable> {
/// use bitacora_meta::{KeyValueStore, MemoryKeyValueStore, Mutation, TransactionOutcome};
///
/// let store = MemoryKeyValueStore::new();
/// let removal_of_a_missing_key = Mutation::Delete {
///     key: "streams/orders".to_owned(),
/// };
///
/// let outcome = store.transact(Vec::new(), vec![removal_of_a_missing_key]).await?;
///
/// assert_eq!(outcome, TransactionOutcome::Applied);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionOutcome {
    /// Every condition held and every mutation was applied.
    Applied,
    /// A condition did not hold and nothing was changed. The caller reads again and decides again.
    ConditionFailed,
}

/// The store could not be reached or did not answer.
///
/// A read that fails this way returned nothing. A transaction that fails this way may or may not have been applied; the
/// caller reads again to find out, which is why every operation built on this interface is safe to repeat.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// use bitacora_meta::{KeyValueStore, MemoryKeyValueStore};
///
/// let store = MemoryKeyValueStore::new();
/// store.set_unavailable(true);
///
/// let read_during_the_outage = store.get("streams/orders").await;
///
/// let failure = read_during_the_outage.expect_err("the store is switched off");
/// assert!(failure.to_string().starts_with("metadata store is unavailable: "));
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("metadata store is unavailable: {reason}")]
pub struct StoreUnavailable {
    /// What went wrong on the way, in the backend's own words, for the operator who reads the log.
    pub reason: String,
}

/// A linearizable key-value store with conditional transactions.
///
/// Implement it once per system that can hold bitacora's metadata. Use it through `Arc<dyn KeyValueStore>`, usually
/// wrapped in a [`crate::MetadataStore`], which is where records, epochs and leases are defined.
///
/// # Examples
///
/// Replacing a value only if nobody else wrote it in between:
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_meta::StoreUnavailable> {
/// use bitacora_meta::{Condition, KeyValueStore, MemoryKeyValueStore, Mutation, TransactionOutcome};
/// use bytes::Bytes;
///
/// let key_value_store = MemoryKeyValueStore::new();
/// let write_first_version = Mutation::Put {
///     key: "streams/orders".to_owned(),
///     value: Bytes::from_static(b"first"),
/// };
/// key_value_store.transact(Vec::new(), vec![write_first_version]).await?;
///
/// let read = key_value_store.get("streams/orders").await?.expect("the key was just written");
/// let unchanged_since_the_read = Condition::HasRevision {
///     key: "streams/orders".to_owned(),
///     revision: read.revision,
/// };
/// let write_second_version = Mutation::Put {
///     key: "streams/orders".to_owned(),
///     value: Bytes::from_static(b"second"),
/// };
/// let outcome = key_value_store
///     .transact(vec![unchanged_since_the_read], vec![write_second_version])
///     .await?;
///
/// assert_eq!(outcome, TransactionOutcome::Applied);
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait KeyValueStore: Send + Sync + 'static {
    /// Reads one key.
    ///
    /// The answer reflects every write that was acknowledged before the call started.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to read.
    ///
    /// # Returns
    ///
    /// The value and its revision, or `None` if the key does not exist.
    ///
    /// # Errors
    ///
    /// Returns [`StoreUnavailable`] if the store could not answer.
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
    /// let never_written = key_value_store.get("streams/orders").await?;
    ///
    /// assert_eq!(never_written, None);
    /// # Ok(())
    /// # }
    /// ```
    async fn get(&self, key: &str) -> Result<Option<VersionedValue>, StoreUnavailable>;

    /// Reads every key that starts with `key_prefix`, as of one moment.
    ///
    /// The prefix is compared character by character; there are no directories. An empty prefix lists the whole store.
    ///
    /// # Arguments
    ///
    /// * `key_prefix` - What the keys must start with.
    ///
    /// # Returns
    ///
    /// The keys with their values and revisions, sorted by key. Empty if no key matches.
    ///
    /// # Errors
    ///
    /// Returns [`StoreUnavailable`] if the store could not answer.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::StoreUnavailable> {
    /// use bitacora_meta::{KeyValueStore, MemoryKeyValueStore, Mutation};
    /// use bytes::Bytes;
    ///
    /// let key_value_store = MemoryKeyValueStore::new();
    /// for key in ["replicas/b", "streams/orders", "replicas/a"] {
    ///     let write = Mutation::Put {
    ///         key: key.to_owned(),
    ///         value: Bytes::from_static(b"record"),
    ///     };
    ///     key_value_store.transact(Vec::new(), vec![write]).await?;
    /// }
    ///
    /// let replicas = key_value_store.list("replicas/").await?;
    ///
    /// assert_eq!(replicas.len(), 2);
    /// assert_eq!(replicas[0].0, "replicas/a");
    /// # Ok(())
    /// # }
    /// ```
    async fn list(&self, key_prefix: &str) -> Result<Vec<(String, VersionedValue)>, StoreUnavailable>;

    /// Applies every mutation atomically, but only if every condition holds.
    ///
    /// All keys written by one transaction get their new revision at the same moment: no reader sees some of the
    /// mutations without the others.
    ///
    /// # Arguments
    ///
    /// * `conditions` - What must hold. An empty list always holds, which makes the transaction an unconditional write.
    /// * `mutations` - The writes and deletes to apply. A backend may limit how many one transaction carries; the
    ///   operations in this crate stay far below any such limit, or split their work.
    ///
    /// # Returns
    ///
    /// [`TransactionOutcome::Applied`] if every condition held and everything was written, or
    /// [`TransactionOutcome::ConditionFailed`] if one did not and nothing was.
    ///
    /// # Errors
    ///
    /// Returns [`StoreUnavailable`] if the store could not answer. The transaction may or may not have been applied.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::StoreUnavailable> {
    /// use bitacora_meta::{Condition, KeyValueStore, MemoryKeyValueStore, Mutation, TransactionOutcome};
    /// use bytes::Bytes;
    ///
    /// let key_value_store = MemoryKeyValueStore::new();
    /// let create = |value: &'static [u8]| {
    ///     let must_be_new = Condition::IsAbsent {
    ///         key: "streams/orders".to_owned(),
    ///     };
    ///     let write = Mutation::Put {
    ///         key: "streams/orders".to_owned(),
    ///         value: Bytes::from_static(value),
    ///     };
    ///     (vec![must_be_new], vec![write])
    /// };
    ///
    /// let (conditions, mutations) = create(b"first");
    /// let first = key_value_store.transact(conditions, mutations).await?;
    /// let (conditions, mutations) = create(b"second");
    /// let second = key_value_store.transact(conditions, mutations).await?;
    ///
    /// assert_eq!(first, TransactionOutcome::Applied);
    /// assert_eq!(second, TransactionOutcome::ConditionFailed);
    /// # Ok(())
    /// # }
    /// ```
    async fn transact(
        &self,
        conditions: Vec<Condition>,
        mutations: Vec<Mutation>,
    ) -> Result<TransactionOutcome, StoreUnavailable>;
}
