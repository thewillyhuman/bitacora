//! A replica that records the requests it serves.
//!
//! # Why this file exists
//!
//! An operator needs to see what a replica is asked to do, how long it takes and how it fails: a replica
//! that refuses appends because it is fenced, full or draining looks healthy from the outside. The
//! replica's own code must stay free of a metrics library, so that it runs unchanged in the simulator.
//! Every request already enters through the [`ReplicaApi`] interface; this file wraps that interface and
//! measures there.
//!
//! # What it contains
//!
//! - [`InstrumentedReplica`]: a [`ReplicaApi`] that forwards to another one and records metrics.
//! - `kind_of_replica_error`: the value of the `error` label for each [`ReplicaError`].
//!
//! # How it works
//!
//! Every call is forwarded unchanged and its result is returned unchanged. Each one is recorded in the
//! three request families of `metrics.rs` under the subsystem `replica`:
//!
//! - `bitacora_replica_requests_total{operation}`
//! - `bitacora_replica_request_duration_seconds{operation}`
//! - `bitacora_replica_request_failures_total{operation,error}`
//!
//! `operation` is the name of the interface's method: `append`, `read`, `fence`, `seal`,
//! `repair_segment`, `upload_segment`, `delete_segment`, `list_segments`, `status`, `drain`. `error` is
//! the variant of [`ReplicaError`] in snake case, for example `fenced` or `no_capacity`.
//!
//! The duration of a `read` includes the time a reader that follows the tail waits for new records, so
//! its upper quantiles show the configured wait, not a slow disk. Use `append` and the disk's flush
//! histogram to judge the write path.
//!
//! The daemon puts this decorator between the gRPC server and the replica, so the numbers are what
//! clients of this replica experience, without the network.
//!
//! # Invariants
//!
//! - The decorator never changes, delays or retries a call.
//! - Every [`ReplicaError`] variant has its own label value; none is folded into another.
//!
//! # See also
//!
//! - `metrics.rs`: the naming scheme and the request families.
//! - `bitacora-types/src/replica_api.rs`: the interface that is wrapped.

use std::sync::Arc;

use async_trait::async_trait;
use bitacora_types::SegmentAddress;
use bitacora_types::replica_api::{
    AppendRequest, AppendResponse, FenceRequest, FenceResponse, ReadRequest, ReadResponse, RepairRequest,
    RepairResponse, ReplicaApi, ReplicaError, ReplicaStatus, SealRequest, SealResponse, SegmentStatus, UploadRequest,
    UploadResponse,
};

use crate::metrics::{MetricsRegistry, RequestMetrics};

/// Names the kind of a replica's refusal or failure, for the `error` label.
fn kind_of_replica_error(error: &ReplicaError) -> &'static str {
    match error {
        ReplicaError::Fenced { .. } => "fenced",
        ReplicaError::Sealed { .. } => "sealed",
        ReplicaError::OutOfOrder { .. } => "out_of_order",
        ReplicaError::ConflictingSeal { .. } => "conflicting_seal",
        ReplicaError::SegmentNotFound { .. } => "segment_not_found",
        ReplicaError::InvalidRequest { .. } => "invalid_request",
        ReplicaError::NoCapacity { .. } => "no_capacity",
        ReplicaError::Draining => "draining",
        ReplicaError::Storage { .. } => "storage",
        ReplicaError::Unavailable { .. } => "unavailable",
    }
}

/// A [`ReplicaApi`] that forwards every call to another one and records metrics about them.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use bitacora_process::{InstrumentedReplica, MetricsRegistry};
/// use bitacora_types::replica_api::{ReplicaApi, UnreachableReplica};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let registry = MetricsRegistry::new();
/// // A daemon passes its `bitacora_replica::Replica` here.
/// let replica: Arc<dyn ReplicaApi> = Arc::new(UnreachableReplica::new("an example"));
/// let instrumented = InstrumentedReplica::new(replica, &registry);
///
/// let status = instrumented.status().await;
///
/// assert!(status.is_err());
/// assert!(registry.encode().contains(
///     r#"bitacora_replica_request_failures_total{operation="status",error="unavailable"} 1"#
/// ));
/// # }
/// ```
#[derive(Clone)]
pub struct InstrumentedReplica {
    /// The replica that does the work.
    replica: Arc<dyn ReplicaApi>,
    /// What is recorded about its requests.
    metrics: RequestMetrics,
}

impl InstrumentedReplica {
    /// Wraps `replica` and registers the replica request metrics in `registry`.
    ///
    /// # Arguments
    ///
    /// * `replica` - The replica that does the work.
    /// * `registry` - The registry of the process. Wrap only one replica per registry: a second one would
    ///   register the same metric names again.
    #[must_use]
    pub fn new(replica: Arc<dyn ReplicaApi>, registry: &MetricsRegistry) -> Self {
        Self {
            replica,
            metrics: RequestMetrics::register(registry, "replica", "Requests to the replica"),
        }
    }
}

#[async_trait]
impl ReplicaApi for InstrumentedReplica {
    async fn append(&self, request: AppendRequest) -> Result<AppendResponse, ReplicaError> {
        self.metrics
            .observe("append", self.replica.append(request), kind_of_replica_error)
            .await
    }

    async fn read(&self, request: ReadRequest) -> Result<ReadResponse, ReplicaError> {
        self.metrics
            .observe("read", self.replica.read(request), kind_of_replica_error)
            .await
    }

    async fn fence(&self, request: FenceRequest) -> Result<FenceResponse, ReplicaError> {
        self.metrics
            .observe("fence", self.replica.fence(request), kind_of_replica_error)
            .await
    }

    async fn seal(&self, request: SealRequest) -> Result<SealResponse, ReplicaError> {
        self.metrics
            .observe("seal", self.replica.seal(request), kind_of_replica_error)
            .await
    }

    async fn repair_segment(&self, request: RepairRequest) -> Result<RepairResponse, ReplicaError> {
        self.metrics
            .observe(
                "repair_segment",
                self.replica.repair_segment(request),
                kind_of_replica_error,
            )
            .await
    }

    async fn upload_segment(&self, request: UploadRequest) -> Result<UploadResponse, ReplicaError> {
        self.metrics
            .observe(
                "upload_segment",
                self.replica.upload_segment(request),
                kind_of_replica_error,
            )
            .await
    }

    async fn delete_segment(&self, segment: SegmentAddress) -> Result<(), ReplicaError> {
        self.metrics
            .observe(
                "delete_segment",
                self.replica.delete_segment(segment),
                kind_of_replica_error,
            )
            .await
    }

    async fn list_segments(&self) -> Result<Vec<SegmentStatus>, ReplicaError> {
        self.metrics
            .observe("list_segments", self.replica.list_segments(), kind_of_replica_error)
            .await
    }

    async fn status(&self) -> Result<ReplicaStatus, ReplicaError> {
        self.metrics
            .observe("status", self.replica.status(), kind_of_replica_error)
            .await
    }

    async fn drain(&self) -> Result<(), ReplicaError> {
        self.metrics
            .observe("drain", self.replica.drain(), kind_of_replica_error)
            .await
    }
}

#[cfg(test)]
#[path = "instrumented_replica_test.rs"]
mod tests;
