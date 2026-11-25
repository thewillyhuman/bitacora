//! The metadata store's key-value interface, implemented on etcd.
//!
//! # Why this file exists
//!
//! bitacora keeps its metadata behind [`bitacora_meta::KeyValueStore`], an
//! interface small enough that any linearizable store can implement it
//! (design, section 8). Tests and the simulator use the in-memory
//! implementation; a real cluster needs one that survives the loss of a
//! machine. This file is that implementation, on etcd.
//!
//! # What it contains
//!
//! - [`EtcdKeyValueStore`]: the store, and its `KeyValueStore`
//!   implementation.
//! - Private conversion functions between the interface's types and the
//!   requests and responses of the etcd client.
//!
//! # How it works
//!
//! Each method of the interface is exactly one etcd request:
//!
//! | interface  | etcd request                                             |
//! |------------|----------------------------------------------------------|
//! | `get`      | a linearizable range over one key                        |
//! | `list`     | a linearizable range over a prefix, sorted by key        |
//! | `transact` | a transaction: comparisons, then puts and deletes        |
//!
//! One request is one consistent view, so `list` needs no paging and no
//! snapshot bookkeeping. The price is that a listing arrives as a single
//! response; the size limit of the gRPC client is lifted for that reason
//! (see [`LARGEST_RESPONSE_BYTES`]).
//!
//! A [`Revision`] is the key's *mod revision*: the revision of the etcd
//! cluster at the key's last write. etcd raises its revision on every write
//! and never reuses one, which is exactly what the interface asks for: a
//! key that is deleted and created again gets a higher revision than before.
//!
//! Every key is sent with the configured prefix in front and comes back
//! without it, so callers never see the prefix.
//!
//! # Invariants
//!
//! - Every request is bounded by the request timeout, whether etcd is slow,
//!   unreachable or gone. A caller is never left waiting.
//! - Every failure is reported as [`StoreUnavailable`]. Nothing here panics
//!   on what etcd answers.
//! - Nothing is retried here. A transaction that fails may or may not have
//!   been applied; the layer above reads again to find out, and that is the
//!   only safe way to retry a conditional write.
//!
//! # See also
//!
//! - `settings.rs`: the endpoints, key prefix and timeouts a store is built
//!   from.
//! - `bitacora-meta/src/key_value.rs`: the interface and its contract.
//! - `bitacora-meta/src/memory_store.rs`: the in-memory implementation that
//!   this one must behave like.

use std::time::Duration;

use async_trait::async_trait;
use bitacora_meta::{
    Condition, KeyValueStore, Mutation, Revision, StoreUnavailable, TransactionOutcome, VersionedValue,
};
use bytes::Bytes;
use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, GetOptions, KeyValue, KvClient, SortOrder, SortTarget, Txn, TxnOp,
};

use crate::settings::{EtcdSettings, InvalidEtcdSettings};

/// The largest response the client accepts from etcd: no limit of its own.
///
/// The gRPC client would otherwise refuse responses over 4 MiB, and a
/// listing is one response. etcd already bounds how much it stores; a
/// second, smaller bound on the reading side would only turn a large
/// cluster into one whose streams can no longer be listed. etcd's own
/// client lifts the limit for the same reason.
const LARGEST_RESPONSE_BYTES: usize = usize::MAX;

/// The metadata store's key-value interface on an etcd cluster.
///
/// Use it wherever a [`KeyValueStore`] is expected in a real deployment;
/// tests that do not need etcd use `bitacora_meta::MemoryKeyValueStore`.
/// One store serves any number of concurrent requests and is meant to be
/// shared behind an `Arc` for the life of the process.
///
/// # Limits inherited from etcd
///
/// - A transaction that writes the same key twice is refused.
/// - A transaction has at most as many operations, and a request at most as
///   many bytes, as the cluster is configured to accept (128 operations and
///   1.5 MiB by default).
///
/// Both are reported as [`StoreUnavailable`], like every other failure.
///
/// # Examples
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use std::sync::Arc;
///
/// use bitacora_meta::MetadataStore;
/// use bitacora_meta_etcd::{EtcdKeyValueStore, EtcdSettings};
///
/// let settings = EtcdSettings::new(vec!["http://127.0.0.1:2379".to_owned()]);
/// let key_value_store = EtcdKeyValueStore::connect(settings).await?;
/// let metadata = MetadataStore::new(Arc::new(key_value_store));
/// let streams = metadata.list_streams().await?;
/// # Ok(())
/// # }
/// ```
pub struct EtcdKeyValueStore {
    /// The etcd client. It owns the connections to the endpoints and keeps
    /// them open for as long as the store lives.
    client: Client,

    /// The text put in front of every key on the way to etcd and removed on
    /// the way back.
    key_prefix: String,

    /// The longest one request may take before it fails as unavailable.
    request_timeout: Duration,
}

impl EtcdKeyValueStore {
    /// Prepares a store for the etcd cluster described by `settings`.
    ///
    /// No request is sent: connections are opened when the first request
    /// needs them and reopened after they break. A process can therefore
    /// start while etcd is still down; its requests fail with
    /// [`StoreUnavailable`] until etcd answers, the same way they do during
    /// any later outage. Must be called inside a tokio runtime, which the
    /// connections live on.
    ///
    /// # Arguments
    ///
    /// * `settings` - Endpoints, key prefix and timeouts. The store keeps
    ///   what it needs; the caller gives the value away.
    ///
    /// # Returns
    ///
    /// The store, ready for requests.
    ///
    /// # Errors
    ///
    /// Fails with [`InvalidEtcdSettings`] when there is no endpoint, when a
    /// timeout is zero, or when an endpoint is not a URL the client can use
    /// (`https` endpoints are not supported yet).
    pub async fn connect(settings: EtcdSettings) -> Result<Self, InvalidEtcdSettings> {
        settings.validate()?;
        let options = ConnectOptions::new().with_connect_timeout(settings.connect_timeout);
        let client = match Client::connect(&settings.endpoints, Some(options)).await {
            Ok(client) => client,
            Err(error) => return Err(InvalidEtcdSettings::because(error.to_string())),
        };
        Ok(Self {
            client,
            key_prefix: settings.key_prefix,
            request_timeout: settings.request_timeout,
        })
    }

    /// Returns a handle for one key-value request. The handle shares the
    /// store's connections; etcd's calls need exclusive access to a handle,
    /// so each request takes its own instead of locking a shared one.
    fn key_value_client(&self) -> KvClient {
        self.client
            .kv_client()
            .max_decoding_message_size(LARGEST_RESPONSE_BYTES)
    }

    /// Returns the etcd key that holds the store's `key`.
    fn etcd_key(&self, key: &str) -> String {
        format!("{}{key}", self.key_prefix)
    }

    /// Awaits one etcd call for at most the request timeout.
    ///
    /// `operation` names the call in the error, so an operator reading a
    /// log line knows what was being attempted.
    ///
    /// # Errors
    ///
    /// Fails with [`StoreUnavailable`] when etcd reports an error or does
    /// not answer in time. In the second case the call is abandoned, but
    /// etcd may still carry it out.
    async fn bounded<Response>(
        &self,
        operation: &str,
        call: impl Future<Output = Result<Response, etcd_client::Error>>,
    ) -> Result<Response, StoreUnavailable> {
        match tokio::time::timeout(self.request_timeout, call).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(error)) => Err(unavailable(format!("etcd {operation} failed: {error}"))),
            Err(_elapsed) => Err(unavailable(format!(
                "etcd did not answer the {operation} within {:?}",
                self.request_timeout
            ))),
        }
    }

    /// Converts one entry of an etcd response into the store's key (the
    /// etcd key without the prefix) and its value with its revision.
    ///
    /// # Errors
    ///
    /// Fails with [`StoreUnavailable`] when the entry is not one this store
    /// could have written: its key is outside the prefix or is not UTF-8,
    /// or its revision is negative.
    fn entry(&self, key_value: KeyValue) -> Result<(String, VersionedValue), StoreUnavailable> {
        let Ok(revision) = u64::try_from(key_value.mod_revision()) else {
            return Err(unavailable("etcd returned a negative revision".to_owned()));
        };
        let (etcd_key, value) = key_value.into_key_value();
        let Ok(etcd_key) = String::from_utf8(etcd_key) else {
            return Err(unavailable("etcd returned a key that is not UTF-8".to_owned()));
        };
        let Some(key) = etcd_key.strip_prefix(&self.key_prefix) else {
            return Err(unavailable(format!(
                "etcd returned the key {etcd_key:?}, which is outside the prefix {:?}",
                self.key_prefix
            )));
        };
        let versioned_value = VersionedValue {
            value: Bytes::from(value),
            revision: Revision::new(revision),
        };
        Ok((key.to_owned(), versioned_value))
    }

    /// Converts a condition into the etcd comparison that holds exactly
    /// when the condition does.
    fn comparison(&self, condition: Condition) -> Compare {
        match condition {
            // etcd reports create revision zero for a key that does not
            // exist, and a positive one for every key that does.
            Condition::IsAbsent { key } => Compare::create_revision(self.etcd_key(&key), CompareOp::Equal, 0),
            Condition::HasRevision { key, revision } => {
                let etcd_key = self.etcd_key(&key);
                match assignable_revision(revision) {
                    Some(revision) => Compare::mod_revision(etcd_key, CompareOp::Equal, revision),
                    None => never_true(etcd_key),
                }
            }
        }
    }

    /// Converts a mutation into the etcd operation that carries it out.
    fn operation(&self, mutation: Mutation) -> TxnOp {
        match mutation {
            Mutation::Put { key, value } => TxnOp::put(self.etcd_key(&key), value, None),
            Mutation::Delete { key } => TxnOp::delete(self.etcd_key(&key), None),
        }
    }
}

#[async_trait]
impl KeyValueStore for EtcdKeyValueStore {
    /// Reads one key with a linearizable range request: the answer reflects
    /// every write that completed before the call started.
    async fn get(&self, key: &str) -> Result<Option<VersionedValue>, StoreUnavailable> {
        let mut client = self.key_value_client();
        let call = client.get(self.etcd_key(key), None);
        let mut response = self.bounded("read", call).await?;

        match response.take_kvs().pop() {
            None => Ok(None),
            Some(key_value) => {
                let (_key, versioned_value) = self.entry(key_value)?;
                Ok(Some(versioned_value))
            }
        }
    }

    /// Reads every key under a prefix with one linearizable range request,
    /// so all entries are from the same revision of the cluster.
    async fn list(&self, key_prefix: &str) -> Result<Vec<(String, VersionedValue)>, StoreUnavailable> {
        let mut client = self.key_value_client();
        let options = GetOptions::new()
            .with_prefix()
            .with_sort(SortTarget::Key, SortOrder::Ascend);
        let call = client.get(self.etcd_key(key_prefix), Some(options));
        let mut response = self.bounded("listing", call).await?;

        let key_values = response.take_kvs();
        let mut entries = Vec::with_capacity(key_values.len());
        for key_value in key_values {
            entries.push(self.entry(key_value)?);
        }
        Ok(entries)
    }

    /// Runs one etcd transaction: if every comparison holds, every put and
    /// delete is applied at one revision; otherwise nothing is.
    ///
    /// An error does not say whether the transaction was applied: the
    /// answer may have been lost after etcd committed it. The caller reads
    /// again to find out.
    async fn transact(
        &self,
        conditions: Vec<Condition>,
        mutations: Vec<Mutation>,
    ) -> Result<TransactionOutcome, StoreUnavailable> {
        let mut comparisons = Vec::with_capacity(conditions.len());
        for condition in conditions {
            comparisons.push(self.comparison(condition));
        }
        let mut operations = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            operations.push(self.operation(mutation));
        }
        let transaction = Txn::new().when(comparisons).and_then(operations);

        let mut client = self.key_value_client();
        let call = client.txn(transaction);
        let response = self.bounded("transaction", call).await?;

        if response.succeeded() {
            Ok(TransactionOutcome::Applied)
        } else {
            Ok(TransactionOutcome::ConditionFailed)
        }
    }
}

/// Builds the error every failure of this store is reported as.
fn unavailable(reason: String) -> StoreUnavailable {
    StoreUnavailable { reason }
}

/// Returns `revision` as etcd counts revisions, or `None` when etcd can
/// never have given it to a key.
///
/// etcd's revisions are positive signed 64-bit numbers. Zero matters most:
/// etcd reports mod revision zero for a key that does not exist, so
/// comparing against zero would hold for a missing key, while the interface
/// says a revision condition on a missing key fails.
fn assignable_revision(revision: Revision) -> Option<i64> {
    match i64::try_from(revision.get()) {
        Ok(revision) if revision > 0 => Some(revision),
        _ => None,
    }
}

/// Returns a comparison on `etcd_key` that holds for no key at all, present
/// or absent: no mod revision is below zero.
fn never_true(etcd_key: String) -> Compare {
    Compare::mod_revision(etcd_key, CompareOp::Less, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns default settings for the given endpoints.
    fn settings(endpoints: &[&str]) -> EtcdSettings {
        let endpoints = endpoints.iter().map(|&endpoint| endpoint.to_owned());
        EtcdSettings::new(endpoints.collect())
    }

    #[test]
    fn revision_of_an_existing_key_is_assignable() {
        assert_eq!(assignable_revision(Revision::new(7)), Some(7));
    }

    #[test]
    fn revision_zero_is_not_assignable() {
        assert_eq!(assignable_revision(Revision::new(0)), None);
    }

    #[test]
    fn revision_beyond_what_etcd_counts_is_not_assignable() {
        assert_eq!(assignable_revision(Revision::new(u64::MAX)), None);
    }

    /// Connecting sends nothing, so a store can be prepared while etcd is
    /// down: nobody listens on the discard port used here.
    #[tokio::test]
    async fn connecting_does_not_need_etcd_to_be_up() {
        let store = EtcdKeyValueStore::connect(settings(&["http://127.0.0.1:9"])).await;

        assert!(store.is_ok());
    }

    #[tokio::test]
    async fn connecting_without_endpoints_is_refused() {
        let store = EtcdKeyValueStore::connect(settings(&[])).await;

        assert_eq!(store.err(), Some(InvalidEtcdSettings::because("there is no endpoint")));
    }

    #[tokio::test]
    async fn connecting_to_a_malformed_endpoint_is_refused() {
        let store = EtcdKeyValueStore::connect(settings(&["http://not a url"])).await;

        assert!(store.is_err());
    }

    /// TLS is not compiled in yet, and saying so at start beats failing
    /// every request later.
    #[tokio::test]
    async fn connecting_to_an_https_endpoint_is_refused() {
        let store = EtcdKeyValueStore::connect(settings(&["https://127.0.0.1:2379"])).await;

        assert!(store.is_err());
    }
}
