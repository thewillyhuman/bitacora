//! A metadata store that records how it answers.
//!
//! # Why this file exists
//!
//! The metadata store is never on the append path, but every roll and every failover is a conditional
//! write to it, so its latency and availability decide how long those take and whether they can happen
//! at all (design, section 14, "Observability"). The crates that use it reach it through an injected
//! interface, [`KeyValueStore`]; this file wraps that interface and measures there.
//!
//! # What it contains
//!
//! - [`InstrumentedKeyValueStore`]: a [`KeyValueStore`] that forwards to another one and records metrics.
//!
//! # How it works
//!
//! Every call is forwarded unchanged and its result is returned unchanged. Each one is recorded in the
//! three request families of `metrics.rs` under the subsystem `metadata_store`:
//!
//! - `bitacora_metadata_store_requests_total{operation}`
//! - `bitacora_metadata_store_request_duration_seconds{operation}`
//! - `bitacora_metadata_store_request_failures_total{operation,error}`
//!
//! `operation` is `get`, `list` or `transact`. The store has one way to fail, so `error` is always
//! `unavailable`. A transaction whose condition did not hold was answered, and is not a failure.
//!
//! One more gauge makes the simplest alert possible: `bitacora_metadata_store_available` is 1 after a
//! request that was answered and 0 after one that failed. It starts at 1: a store is not reported down
//! before anybody asked it.
//!
//! # Invariants
//!
//! - The decorator never changes, delays or retries a call.
//!
//! # See also
//!
//! - `metrics.rs`: the naming scheme and the request families.
//! - `bitacora-meta/src/key_value.rs`: the interface that is wrapped.

use std::sync::Arc;

use async_trait::async_trait;
use bitacora_meta::{
    Condition, KeyValueStore, Mutation, StoreUnavailable, TransactionOutcome, VersionedValue,
};

use crate::metrics::{Gauge, MetricsRegistry, RequestMetrics};

/// Names the only kind of failure the store has, for the `error` label.
fn kind_of_store_failure(_failure: &StoreUnavailable) -> &'static str {
    "unavailable"
}

/// A [`KeyValueStore`] that forwards every call to another one and records metrics about them.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use bitacora_meta::{KeyValueStore, MemoryKeyValueStore, MetadataStore};
/// use bitacora_process::{InstrumentedKeyValueStore, MetricsRegistry};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let registry = MetricsRegistry::new();
/// // A daemon passes its etcd-backed store here.
/// let store: Arc<dyn KeyValueStore> = Arc::new(MemoryKeyValueStore::new());
/// let metadata = MetadataStore::new(Arc::new(InstrumentedKeyValueStore::new(store, &registry)));
///
/// let replicas = metadata.list_replicas().await;
///
/// assert!(replicas.is_ok());
/// assert!(registry.encode().contains(
///     r#"bitacora_metadata_store_requests_total{operation="list"} 1"#
/// ));
/// # }
/// ```
#[derive(Clone)]
pub struct InstrumentedKeyValueStore {
    /// The store that does the work.
    store: Arc<dyn KeyValueStore>,
    /// What is recorded about its requests.
    metrics: RequestMetrics,
    /// Whether the last request was answered.
    is_available: Gauge,
}

impl InstrumentedKeyValueStore {
    /// Wraps `store` and registers the metadata store metrics in `registry`.
    ///
    /// # Arguments
    ///
    /// * `store` - The store that does the work.
    /// * `registry` - The registry of the process. Wrap only one store per registry: a second one would
    ///   register the same metric names again.
    #[must_use]
    pub fn new(store: Arc<dyn KeyValueStore>, registry: &MetricsRegistry) -> Self {
        let is_available = registry.gauge(
            "metadata_store_available",
            "Whether the last request to the metadata store was answered (1) or failed (0)",
        );
        is_available.set_flag(true);
        Self {
            store,
            metrics: RequestMetrics::register(
                registry,
                "metadata_store",
                "Requests to the metadata store",
            ),
            is_available,
        }
    }

    /// Remembers whether the request that just finished was answered, and passes its outcome on.
    fn note_availability<Answer>(
        &self,
        outcome: Result<Answer, StoreUnavailable>,
    ) -> Result<Answer, StoreUnavailable> {
        self.is_available.set_flag(outcome.is_ok());
        outcome
    }
}

#[async_trait]
impl KeyValueStore for InstrumentedKeyValueStore {
    async fn get(&self, key: &str) -> Result<Option<VersionedValue>, StoreUnavailable> {
        let outcome = self
            .metrics
            .observe("get", self.store.get(key), kind_of_store_failure)
            .await;
        self.note_availability(outcome)
    }

    async fn list(
        &self,
        key_prefix: &str,
    ) -> Result<Vec<(String, VersionedValue)>, StoreUnavailable> {
        let outcome = self
            .metrics
            .observe("list", self.store.list(key_prefix), kind_of_store_failure)
            .await;
        self.note_availability(outcome)
    }

    async fn transact(
        &self,
        conditions: Vec<Condition>,
        mutations: Vec<Mutation>,
    ) -> Result<TransactionOutcome, StoreUnavailable> {
        let outcome = self
            .metrics
            .observe(
                "transact",
                self.store.transact(conditions, mutations),
                kind_of_store_failure,
            )
            .await;
        self.note_availability(outcome)
    }
}

#[cfg(test)]
#[path = "instrumented_key_value_store_test.rs"]
mod tests;
