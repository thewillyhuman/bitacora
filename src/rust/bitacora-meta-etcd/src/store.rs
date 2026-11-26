//! The metadata store's key-value interface, implemented on etcd.
//!
//! # Why this file exists
//!
//! bitacora keeps its metadata behind [`bitacora_meta::KeyValueStore`], an interface small enough that any linearizable
//! store can implement it (design, section 8). Tests and the simulator use the in-memory implementation; a real cluster
//! needs one that survives the loss of a machine. This file is that implementation, on etcd.
//!
//! # What it contains
//!
//! - [`EtcdKeyValueStore`]: the store, and its `KeyValueStore` implementation.
//! - Private conversion functions between the interface's types and the requests and responses of the etcd client.
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
//! One request is one consistent view, so `list` needs no paging and no snapshot bookkeeping. The price is that a
//! listing arrives as a single response; the size limit of the gRPC client is lifted for that reason (see
//! [`LARGEST_RESPONSE_BYTES`]).
//!
//! A [`Revision`] is the key's *mod revision*: the revision of the etcd cluster at the key's last write. etcd raises
//! its revision on every write and never reuses one, which is exactly what the interface asks for: a key that is
//! deleted and created again gets a higher revision than before.
//!
//! A condition becomes an etcd comparison. "The key is absent" compares the key's create revision with zero, which is
//! what etcd reports for a key that does not exist. "The key is at this revision" compares its mod revision. A revision
//! that etcd can never have given to a key, zero included, becomes a comparison that holds for no key, so that the
//! condition fails instead of the request.
//!
//! Every key is sent with the configured prefix in front and comes back without it, so callers never see the prefix.
//!
//! A read that fails is made once more after a short pause. A read changes nothing, so making it again is safe, and
//! one more attempt gets a program past the moment it most often fails at: its first request, sent before any
//! connection is up. On a real cluster, about one tool in a hundred that had just started failed there, with etcd in
//! good health.
//!
//! # Invariants
//!
//! - Every attempt is bounded by the request timeout, whether etcd is slow, unreachable or gone, and a read makes at
//!   most [`READ_ATTEMPTS`] of them. A caller is never left waiting.
//! - Every failure is reported as [`StoreUnavailable`]. Nothing here panics on what etcd answers.
//! - A transaction is never made again here. One that fails may or may not have been applied; the layer above reads
//!   again to find out, and that is the only safe way to retry a conditional write.
//!
//! # See also
//!
//! - `settings.rs`: the endpoints, key prefix and timeouts a store is built from.
//! - `bitacora-meta/src/key_value.rs`: the interface and its contract.
//! - `bitacora-meta/src/memory_store.rs`: the in-memory implementation that this one must behave like.
//! - `tests/etcd/`: the tests of this file's requests, against a real etcd.

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
/// The gRPC client would otherwise refuse responses over 4 MiB, and a listing is one response. etcd already bounds how
/// much it stores; a second, smaller bound on the reading side would only turn a large cluster into one whose streams
/// can no longer be listed. etcd's own client lifts the limit for the same reason.
const LARGEST_RESPONSE_BYTES: usize = usize::MAX;

/// How many times a read is made before it is reported as unavailable: once, and once more if that failed.
const READ_ATTEMPTS: u32 = 2;

/// How long a read that failed waits before it is made again, so that a store that refuses at once is not asked in a
/// tight loop.
const READ_RETRY_PAUSE: Duration = Duration::from_millis(100);

/// The create revision etcd reports for a key that does not exist. Every key that exists has a positive one.
const CREATE_REVISION_OF_AN_ABSENT_KEY: i64 = 0;

/// The mod revision etcd reports for a key that does not exist. Every key that exists has a higher one, and no key has
/// a lower one.
const MOD_REVISION_OF_AN_ABSENT_KEY: i64 = 0;

/// The metadata store's key-value interface on an etcd cluster.
///
/// Use it wherever a [`KeyValueStore`] is expected in a real deployment; tests that do not need etcd use
/// `bitacora_meta::MemoryKeyValueStore`. One store serves any number of concurrent requests and is meant to be shared
/// behind an `Arc` for the life of the process.
///
/// # Limits inherited from etcd
///
/// - A transaction that writes the same key twice is refused.
/// - A transaction has at most as many operations, and a request at most as many bytes, as the cluster is configured to
///   accept (128 operations and 1.5 MiB by default).
///
/// Both are reported as [`StoreUnavailable`], like every other failure.
///
/// # Examples
///
/// The example is compiled but not run, because its last line needs an etcd to answer.
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
    /// The etcd client. It owns the connections to the endpoints and keeps them open for as long as the store lives.
    client: Client,

    /// The text put in front of every key on the way to etcd and removed on the way back.
    key_prefix: String,

    /// The longest one request may take before it fails as unavailable.
    request_timeout: Duration,
}

impl EtcdKeyValueStore {
    /// Prepares a store for the etcd cluster described by `settings`.
    ///
    /// No request is sent: connections are opened when the first request needs them and reopened after they break. A
    /// process can therefore start while etcd is still down; its requests fail with [`StoreUnavailable`] until etcd
    /// answers, the same way they do during any later outage. Must be called inside a tokio runtime, which the
    /// connections live on.
    ///
    /// # Arguments
    ///
    /// * `settings` - Endpoints, key prefix and timeouts. The store keeps what it needs; the caller gives the value
    ///   away.
    ///
    /// # Returns
    ///
    /// The store, ready for requests.
    ///
    /// # Errors
    ///
    /// Fails with [`InvalidEtcdSettings`] when there is no endpoint, when a timeout is zero, or when an endpoint is not
    /// a URL the client can use (`https` endpoints are not supported yet).
    ///
    /// # Examples
    ///
    /// Connecting contacts nobody, so this example runs without an etcd:
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// use bitacora_meta_etcd::{EtcdKeyValueStore, EtcdSettings};
    ///
    /// let settings = EtcdSettings::new(vec!["http://127.0.0.1:2379".to_owned()]);
    /// let settings_without_endpoints = EtcdSettings::new(Vec::new());
    ///
    /// let prepared = EtcdKeyValueStore::connect(settings).await;
    /// let refused = EtcdKeyValueStore::connect(settings_without_endpoints).await;
    ///
    /// assert!(prepared.is_ok());
    /// assert!(refused.is_err());
    /// # }
    /// ```
    pub async fn connect(settings: EtcdSettings) -> Result<Self, InvalidEtcdSettings> {
        settings.validate()?;
        let options = ConnectOptions::new().with_connect_timeout(settings.connect_timeout);
        let client = match Client::connect(&settings.endpoints, Some(options)).await {
            Ok(client) => client,
            Err(client_error) => return Err(InvalidEtcdSettings::because(client_error.to_string())),
        };
        Ok(Self {
            client,
            key_prefix: settings.key_prefix,
            request_timeout: settings.request_timeout,
        })
    }

    /// Returns a handle for one key-value request.
    ///
    /// The handle shares the store's connections; etcd's calls need exclusive access to a handle, so each request
    /// takes its own instead of locking a shared one.
    fn new_key_value_client(&self) -> KvClient {
        self.client
            .kv_client()
            .max_decoding_message_size(LARGEST_RESPONSE_BYTES)
    }

    /// Returns the etcd key that holds the store's `key`: the key with the store's prefix in front.
    fn to_etcd_key(&self, key: &str) -> String {
        format!("{}{key}", self.key_prefix)
    }

    /// Awaits one etcd call for at most the request timeout.
    ///
    /// `operation` names the call in the error, so an operator reading a log line knows what was being attempted.
    ///
    /// # Errors
    ///
    /// Fails with [`StoreUnavailable`] when etcd reports an error or does not answer in time. In the second case the
    /// call is abandoned, but etcd may still carry it out.
    async fn await_within_the_request_timeout<Response>(
        &self,
        operation: &str,
        call: impl Future<Output = Result<Response, etcd_client::Error>>,
    ) -> Result<Response, StoreUnavailable> {
        match tokio::time::timeout(self.request_timeout, call).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(etcd_error)) => Err(unavailable(format!("etcd {operation} failed: {etcd_error}"))),
            Err(_elapsed) => Err(unavailable(format!(
                "etcd did not answer the {operation} within {:?}",
                self.request_timeout
            ))),
        }
    }

    /// Converts one entry of an etcd response into the store's key (the etcd key without the prefix) and its value
    /// with its revision.
    ///
    /// # Errors
    ///
    /// Fails with [`StoreUnavailable`] when the entry is not one this store could have written: its key is outside the
    /// prefix or is not UTF-8, or its revision is negative.
    fn decode_entry(&self, key_value: KeyValue) -> Result<(String, VersionedValue), StoreUnavailable> {
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

    /// Converts a condition into the etcd comparison that holds exactly when the condition does.
    fn to_comparison(&self, condition: Condition) -> Compare {
        match condition {
            Condition::IsAbsent { key } => Compare::create_revision(
                self.to_etcd_key(&key),
                CompareOp::Equal,
                CREATE_REVISION_OF_AN_ABSENT_KEY,
            ),
            Condition::HasRevision { key, revision } => {
                let etcd_key = self.to_etcd_key(&key);
                match to_etcd_revision(revision) {
                    Some(etcd_revision) => Compare::mod_revision(etcd_key, CompareOp::Equal, etcd_revision),
                    None => comparison_that_never_holds(etcd_key),
                }
            }
        }
    }

    /// Converts a mutation into the etcd operation that carries it out.
    fn to_operation(&self, mutation: Mutation) -> TxnOp {
        match mutation {
            Mutation::Put { key, value } => TxnOp::put(self.to_etcd_key(&key), value, None),
            Mutation::Delete { key } => TxnOp::delete(self.to_etcd_key(&key), None),
        }
    }

    /// Makes one attempt to read `key`: a linearizable range request over the one key.
    async fn get_once(&self, key: &str) -> Result<Option<VersionedValue>, StoreUnavailable> {
        let mut client = self.new_key_value_client();
        let call = client.get(self.to_etcd_key(key), None);
        let mut response = self.await_within_the_request_timeout("read", call).await?;

        let Some(key_value) = response.take_kvs().pop() else {
            return Ok(None);
        };
        let (_key, versioned_value) = self.decode_entry(key_value)?;
        Ok(Some(versioned_value))
    }

    /// Makes one attempt to list the keys under `key_prefix`: one linearizable range request, sorted by key.
    async fn list_once(&self, key_prefix: &str) -> Result<Vec<(String, VersionedValue)>, StoreUnavailable> {
        let mut client = self.new_key_value_client();
        let options = GetOptions::new()
            .with_prefix()
            .with_sort(SortTarget::Key, SortOrder::Ascend);
        let call = client.get(self.to_etcd_key(key_prefix), Some(options));
        let mut response = self.await_within_the_request_timeout("listing", call).await?;

        let key_values = response.take_kvs();
        let mut entries = Vec::with_capacity(key_values.len());
        for key_value in key_values {
            entries.push(self.decode_entry(key_value)?);
        }
        Ok(entries)
    }
}

#[async_trait]
impl KeyValueStore for EtcdKeyValueStore {
    /// Reads one key with a linearizable range request, which reflects every write completed before the call started.
    /// A read that fails is made once more.
    async fn get(&self, key: &str) -> Result<Option<VersionedValue>, StoreUnavailable> {
        read_again_if_unavailable(|| self.get_once(key)).await
    }

    /// Reads every key under a prefix with one linearizable range request, so all entries are from the same revision.
    /// A listing that fails is made once more.
    async fn list(&self, key_prefix: &str) -> Result<Vec<(String, VersionedValue)>, StoreUnavailable> {
        read_again_if_unavailable(|| self.list_once(key_prefix)).await
    }

    /// Runs one etcd transaction: every put and delete is applied at one revision if every comparison holds. It is
    /// sent once, whatever happens to it.
    async fn transact(
        &self,
        conditions: Vec<Condition>,
        mutations: Vec<Mutation>,
    ) -> Result<TransactionOutcome, StoreUnavailable> {
        let mut comparisons = Vec::with_capacity(conditions.len());
        for condition in conditions {
            comparisons.push(self.to_comparison(condition));
        }
        let mut operations = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            operations.push(self.to_operation(mutation));
        }
        let transaction = Txn::new().when(comparisons).and_then(operations);

        let mut client = self.new_key_value_client();
        let call = client.txn(transaction);
        let response = self.await_within_the_request_timeout("transaction", call).await?;

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

/// Makes a read, and makes it again after [`READ_RETRY_PAUSE`] if the store was unavailable, [`READ_ATTEMPTS`] times
/// in all.
///
/// Only a read may go through here: it changes nothing, so making it twice does no harm. See the file header for
/// what the second attempt is for.
///
/// # Errors
///
/// Fails with the [`StoreUnavailable`] of the last attempt when none of them succeeded.
async fn read_again_if_unavailable<Response, Attempt>(
    mut read: impl FnMut() -> Attempt,
) -> Result<Response, StoreUnavailable>
where
    Attempt: Future<Output = Result<Response, StoreUnavailable>>,
{
    let mut attempts_made = 1;
    loop {
        match read().await {
            Ok(response) => return Ok(response),
            Err(last_failure) if attempts_made == READ_ATTEMPTS => return Err(last_failure),
            Err(_failure_before_the_last_attempt) => {
                attempts_made += 1;
                tokio::time::sleep(READ_RETRY_PAUSE).await;
            }
        }
    }
}

/// Returns `revision` as etcd counts revisions, or `None` when etcd can never have given it to a key.
///
/// etcd's revisions are positive signed 64-bit numbers. Zero matters most: etcd reports mod revision zero for a key
/// that does not exist, so comparing against zero would hold for a missing key, while the interface says a revision
/// condition on a missing key fails.
fn to_etcd_revision(revision: Revision) -> Option<i64> {
    match i64::try_from(revision.get()) {
        Ok(etcd_revision) if etcd_revision > MOD_REVISION_OF_AN_ABSENT_KEY => Some(etcd_revision),
        _zero_or_too_large => None,
    }
}

/// Returns a comparison on `etcd_key` that holds for no key at all, present or absent: no mod revision is below the
/// one of an absent key.
fn comparison_that_never_holds(etcd_key: String) -> Compare {
    Compare::mod_revision(etcd_key, CompareOp::Less, MOD_REVISION_OF_AN_ABSENT_KEY)
}

#[cfg(test)]
#[path = "store_test.rs"]
mod tests;
