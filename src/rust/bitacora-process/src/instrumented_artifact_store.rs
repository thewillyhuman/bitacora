//! An object store that records how it answers.
//!
//! # Why this file exists
//!
//! Tiering uploads sealed segments to the object store, and once the replicas drop their copies it is the
//! only place those records are read from. A slow or failing object store therefore shows up as uploads
//! that do not finish and as old data that cannot be read. The crates that use it reach it through an
//! injected interface, [`ArtifactStore`]; this file wraps that interface and measures there.
//!
//! # What it contains
//!
//! - [`InstrumentedArtifactStore`]: an [`ArtifactStore`] that forwards to another one and records
//!   metrics.
//! - `kind_of_artifact_error`: the value of the `error` label for each [`ArtifactError`].
//!
//! # How it works
//!
//! Every call is forwarded unchanged and its result is returned unchanged. Each one is recorded in the
//! three request families of `metrics.rs` under the subsystem `object_store`:
//!
//! - `bitacora_object_store_requests_total{operation}`
//! - `bitacora_object_store_request_duration_seconds{operation}`
//! - `bitacora_object_store_request_failures_total{operation,error}`
//!
//! `operation` is the name of the interface's method: `put_object`, `get_object`, `get_range`,
//! `delete_object`, `list_prefix`. `error` is `not_found`, `range_out_of_bounds`, `invalid_key` or
//! `unavailable`. Only the last one means the store itself is in trouble; the others are answers a
//! caller can get from a healthy store, and are counted so that a wrong key or a missing object is
//! visible too.
//!
//! `bitacora_object_store_transferred_bytes_total{direction}` counts the bytes of stored objects
//! (`uploaded`) and of the objects and ranges that were read (`downloaded`), for requests that
//! succeeded.
//!
//! # Invariants
//!
//! - The decorator never changes, delays or retries a call.
//!
//! # See also
//!
//! - `metrics.rs`: the naming scheme and the request families.
//! - `bitacora-artifact/src/store.rs`: the interface that is wrapped.

use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bitacora_artifact::{ArtifactError, ArtifactStore, ObjectInfo};
use bytes::Bytes;
use prometheus_client::metrics::counter::Counter as LibraryCounter;
use prometheus_client::metrics::family::Family;

use crate::metrics::{MetricsRegistry, RequestMetrics};

/// The label that says which way bytes travelled.
const DIRECTION_LABEL: &str = "direction";

/// The `direction` of bytes sent to the object store.
const UPLOADED: &str = "uploaded";

/// The `direction` of bytes received from the object store.
const DOWNLOADED: &str = "downloaded";

/// The label set of the transferred bytes counter.
type DirectionLabels = [(&'static str, &'static str); 1];

/// Names the kind of an object store failure, for the `error` label.
fn kind_of_artifact_error(error: &ArtifactError) -> &'static str {
    match error {
        ArtifactError::NotFound { .. } => "not_found",
        ArtifactError::RangeOutOfBounds { .. } => "range_out_of_bounds",
        ArtifactError::InvalidKey { .. } => "invalid_key",
        ArtifactError::Unavailable { .. } => "unavailable",
    }
}

/// An [`ArtifactStore`] that forwards every call to another one and records metrics about them.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use bitacora_artifact::{ArtifactStore, MemoryArtifactStore};
/// use bitacora_process::{InstrumentedArtifactStore, MetricsRegistry};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_artifact::ArtifactError> {
/// let registry = MetricsRegistry::new();
/// // A daemon passes the object store of its cluster configuration here.
/// let store = InstrumentedArtifactStore::new(Arc::new(MemoryArtifactStore::new()), &registry);
///
/// store.put_object("streams/7/segments/3", "segment".into()).await?;
///
/// assert!(registry.encode().contains(
///     r#"bitacora_object_store_transferred_bytes_total{direction="uploaded"} 7"#
/// ));
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct InstrumentedArtifactStore {
    /// The store that does the work.
    store: Arc<dyn ArtifactStore>,
    /// What is recorded about its requests.
    metrics: RequestMetrics,
    /// Bytes of successful requests, by direction.
    transferred_bytes: Family<DirectionLabels, LibraryCounter>,
}

impl InstrumentedArtifactStore {
    /// Wraps `store` and registers the object store metrics in `registry`.
    ///
    /// # Arguments
    ///
    /// * `store` - The store that does the work.
    /// * `registry` - The registry of the process. Wrap only one store per registry: a second one would
    ///   register the same metric names again.
    #[must_use]
    pub fn new(store: Arc<dyn ArtifactStore>, registry: &MetricsRegistry) -> Self {
        let transferred_bytes = Family::<DirectionLabels, LibraryCounter>::default();
        registry.register_bytes_family(
            "object_store_transferred",
            "Bytes sent to and received from the object store by successful requests, by direction",
            transferred_bytes.clone(),
        );
        Self {
            store,
            metrics: RequestMetrics::register(
                registry,
                "object_store",
                "Requests to the object store",
            ),
            transferred_bytes,
        }
    }

    /// Adds `byte_count` to the bytes transferred in `direction`.
    fn count_transferred(&self, direction: &'static str, byte_count: usize) {
        self.transferred_bytes
            .get_or_create(&[(DIRECTION_LABEL, direction)])
            .inc_by(byte_count as u64);
    }
}

#[async_trait]
impl ArtifactStore for InstrumentedArtifactStore {
    async fn put_object(&self, object_key: &str, contents: Bytes) -> Result<(), ArtifactError> {
        let object_bytes = contents.len();
        self.metrics
            .observe(
                "put_object",
                self.store.put_object(object_key, contents),
                kind_of_artifact_error,
            )
            .await?;
        self.count_transferred(UPLOADED, object_bytes);
        Ok(())
    }

    async fn get_object(&self, object_key: &str) -> Result<Bytes, ArtifactError> {
        let contents = self
            .metrics
            .observe(
                "get_object",
                self.store.get_object(object_key),
                kind_of_artifact_error,
            )
            .await?;
        self.count_transferred(DOWNLOADED, contents.len());
        Ok(contents)
    }

    async fn get_range(
        &self,
        object_key: &str,
        byte_range: Range<u64>,
    ) -> Result<Bytes, ArtifactError> {
        let contents = self
            .metrics
            .observe(
                "get_range",
                self.store.get_range(object_key, byte_range),
                kind_of_artifact_error,
            )
            .await?;
        self.count_transferred(DOWNLOADED, contents.len());
        Ok(contents)
    }

    async fn delete_object(&self, object_key: &str) -> Result<(), ArtifactError> {
        self.metrics
            .observe(
                "delete_object",
                self.store.delete_object(object_key),
                kind_of_artifact_error,
            )
            .await
    }

    async fn list_prefix(&self, key_prefix: &str) -> Result<Vec<ObjectInfo>, ArtifactError> {
        self.metrics
            .observe(
                "list_prefix",
                self.store.list_prefix(key_prefix),
                kind_of_artifact_error,
            )
            .await
    }
}

#[cfg(test)]
#[path = "instrumented_artifact_store_test.rs"]
mod tests;
