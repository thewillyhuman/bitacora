//! What every part of the client needs to reach the cluster.
//!
//! # Why this file exists
//!
//! The writer, the reader and the administration all talk to the same four things: the metadata store, the replicas,
//! the clock and, for tiered segments, the object store. None of them may reach those directly, or the client could not
//! run inside the deterministic simulator. They are injected once, when the client is built, and shared from here. The
//! one thing all three parts do with them, turning the replica set of a segment into handles that can be called, also
//! lives here so that it is written once.
//!
//! # What it contains
//!
//! - [`ClientContext`]: the injected interfaces.
//! - [`ConnectedReplica`]: one replica of a segment, ready to be talked to.
//! - [`ClientContext::connect_copies`]: from a segment's copies to connected replicas.
//!
//! # How it works
//!
//! A segment record names its replicas by identity. Their addresses are in the replica registry of the metadata store.
//! `connect_copies` reads the registry once and asks the connector for a handle per copy, passing both the identity and
//! the address: the transport then refuses to let another replica answer at that address (a node whose disk was
//! replaced comes back under a new identity).
//!
//! A copy whose replica is no longer registered still gets a handle, one that fails every request as unavailable.
//! Callers therefore never special-case a missing replica: it is one more replica that does not answer.
//!
//! # Invariants
//!
//! - The handles are returned in the order of the copies, so that an index into one is an index into the other. The
//!   writer and the reader both rely on it.
//! - Nothing here keeps state between calls. A caller that wants fresh addresses calls again.
//!
//! # See also
//!
//! - `client.rs`: where the context is built.
//! - `bitacora-types/src/replica_api.rs`: `ReplicaConnector` and `UnreachableReplica`.

use std::sync::Arc;

use bitacora_artifact::ArtifactStore;
use bitacora_meta::{CopyRecord, MetaError, MetadataStore, ReplicaRecord};
use bitacora_types::replica_api::{ReplicaApi, ReplicaConnector, UnreachableReplica};
use bitacora_types::{Clock, ReplicaId};

/// A replica of a segment, ready to be talked to.
#[derive(Clone)]
pub(crate) struct ConnectedReplica {
    /// The identity of the replica, as the segment record names it.
    pub replica_id: ReplicaId,
    /// The handle requests are sent through. It fails every request if the replica is not registered.
    pub client: Arc<dyn ReplicaApi>,
}

/// The injected interfaces of the client: metadata, network, clock and, optionally, the object store.
pub(crate) struct ClientContext {
    /// The metadata store: stream records, the replica registry, checkpoints.
    pub metadata: MetadataStore,
    /// How replicas are reached: gRPC in a deployment, in-process links in tests and simulation.
    pub connector: Arc<dyn ReplicaConnector>,
    /// The wall clock, used only for leases and seal times.
    pub clock: Arc<dyn Clock>,
    /// Where tiered segments are read from. A client without one can only read segments that replicas still hold.
    pub artifact_store: Option<Arc<dyn ArtifactStore>>,
}

impl ClientContext {
    /// Connects to the replicas that hold `copies`, in the same order.
    ///
    /// Reads the replica registry once. Connecting itself sends nothing: a handle is only an address to send to.
    ///
    /// # Arguments
    ///
    /// * `copies` - The replica set of a segment, as its record lists it.
    ///
    /// # Returns
    ///
    /// One handle per copy, in the order of `copies`. A replica that is no longer registered gets a handle that fails
    /// every request, so callers treat it like any other replica that is down.
    ///
    /// # Errors
    ///
    /// Returns the metadata store's error if the registry cannot be read.
    pub(crate) async fn connect_copies(&self, copies: &[CopyRecord]) -> Result<Vec<ConnectedReplica>, MetaError> {
        let registered_replicas = self.metadata.list_replicas().await?;
        let mut connected_replicas = Vec::with_capacity(copies.len());
        for copy in copies {
            connected_replicas.push(ConnectedReplica {
                replica_id: copy.replica_id.clone(),
                client: self.connect_replica(&registered_replicas, &copy.replica_id),
            });
        }
        Ok(connected_replicas)
    }

    /// Returns a handle to the replica `replica_id`: through the connector if the registry knows its address, and one
    /// that fails every request otherwise.
    fn connect_replica(&self, registered_replicas: &[ReplicaRecord], replica_id: &ReplicaId) -> Arc<dyn ReplicaApi> {
        for registered_replica in registered_replicas {
            if &registered_replica.replica_id == replica_id {
                return self
                    .connector
                    .connect(&registered_replica.replica_id, &registered_replica.address);
            }
        }
        Arc::new(UnreachableReplica::new(format!(
            "replica {replica_id} is not registered"
        )))
    }
}

#[cfg(test)]
#[path = "context_test.rs"]
mod tests;
