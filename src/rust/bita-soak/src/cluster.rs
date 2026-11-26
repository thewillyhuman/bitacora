//! The cluster a soak drives, as the tool holds it.
//!
//! # Why this file exists
//!
//! Every part of a soak needs the same four things of the cluster: a client to write and read with, the metadata
//! store to see which replicas are registered and who leads, a clock to tell whether a registration is fresh, and
//! the way to inject faults. On Kubernetes they come from the command line and the pod's service account; in the
//! tests of this crate they come from the in-process test cluster. This file is the one value both hand to the
//! rounds, so that a round is the same code in both.
//!
//! # What it contains
//!
//! - [`SoakedCluster`]: the four handles.
//!
//! # How it works
//!
//! The value is built once, by `main.rs` or by a test, and passed by reference. Its handles are shared ones: cloning
//! the client or the metadata store gives another handle to the same connections.
//!
//! # Invariants
//!
//! - The client, the metadata store and the injector are about the same cluster. Nothing checks that; whoever
//!   builds the value answers for it.
//!
//! # See also
//!
//! - `main.rs`: builds it for a cluster on Kubernetes.
//! - `test_support.rs`: builds it for the in-process test cluster.

use std::sync::Arc;

use bitacora::BitacoraClient;
use bitacora_meta::MetadataStore;
use bitacora_types::Clock;

use crate::faults::FaultInjector;

/// The cluster under test.
#[derive(Clone)]
pub(crate) struct SoakedCluster {
    /// Creates and deletes streams, and opens the writers and readers of the workload.
    pub(crate) client: BitacoraClient,
    /// Where the replicas register and the leading controller writes its name.
    pub(crate) metadata: MetadataStore,
    /// Says what time it is for the cluster, to judge whether a registration is fresh.
    pub(crate) clock: Arc<dyn Clock>,
    /// Injects the faults and removes the cuts.
    pub(crate) faults: Arc<dyn FaultInjector>,
}
