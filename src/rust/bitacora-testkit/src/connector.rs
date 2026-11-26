//! How the cluster's replicas are reached by address.
//!
//! # Why this file exists
//!
//! Writers, readers, the controller and the replicas themselves do not hold a replica: they hold its identity and its
//! address, read from the metadata store, and ask a `ReplicaConnector` for a way to call it. In production that opens a
//! connection. In a test cluster there are no sockets, so this file is the connector that answers with the node's link
//! instead, which is what makes every request of the code under test cross a link and meet its faults.
//!
//! It also keeps a promise that production keeps with a handshake: a caller that asks for replica X at an address is
//! never served by another replica that took the address over. A node that lost its disk comes back at the same address
//! under a new identity, and must not answer for the copies its old identity held.
//!
//! # What it contains
//!
//! - [`ClusterConnector`]: the address book of a cluster and its `ReplicaConnector` implementation.
//! - `AddressedLink`: a link together with the identity its caller expects behind it.
//!
//! # How it works
//!
//! The connector is a map from address to link behind a shared lock. It exists before the first node does, because each
//! replica is given it when it is opened (replicas repair their copies from each other), and it learns of each node as
//! the node starts. Clones share the one map.
//!
//! Connecting looks the address up. A known address yields an `AddressedLink`, which sends every request through the
//! link addressed to the expected identity; an unknown one yields a replica that is never reachable, as a connection to
//! a host that does not exist would be.
//!
//! # Invariants
//!
//! - An address is registered once, when its node starts, and stays registered: a node keeps its address and its link
//!   for life, through crashes and lost disks.
//! - A request made through the connector reaches a replica only if that replica has the identity the caller asked for.
//! - The lock guards one map operation and is never held across an `await`.
//!
//! # See also
//!
//! - `link.rs`: the link, its faults and the check of the expected identity.
//! - `cluster.rs`: `Cluster::connector`, which hands this out.
//! - `docs/decisions/0010-replica-identity.md`: why an address is not an identity.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use bitacora_replica::Replica;
use bitacora_types::replica_api::{
    AppendRequest, AppendResponse, FenceRequest, FenceResponse, ReadRequest, ReadResponse, RepairRequest,
    RepairResponse, ReplicaApi, ReplicaConnector, ReplicaError, ReplicaStatus, SealRequest, SealResponse,
    SegmentStatus, UnreachableReplica, UploadRequest, UploadResponse,
};
use bitacora_types::{ReplicaId, SegmentAddress};

use crate::link::Link;

/// Reaches the cluster's nodes through their links.
///
/// The replicas use it too, to repair copies from each other, so it has to exist before the first node does and learn
/// of nodes as they start. A clone shares the address book of the original.
#[derive(Clone, Default)]
pub(crate) struct ClusterConnector {
    /// The link of every node that has started, by the address the node is registered under. Shared, because every
    /// replica and every client of the cluster holds a clone of the connector.
    links_by_address: Arc<Mutex<BTreeMap<String, Arc<Link>>>>,
}

impl ClusterConnector {
    /// Records that the node at `address` is reached through `link`.
    ///
    /// Called once per node, when it starts. Registering an address again replaces its link.
    ///
    /// # Arguments
    ///
    /// * `address` - The address the node registers in the metadata store.
    /// * `link` - The network path to the node.
    pub(crate) fn register(&self, address: String, link: Arc<Link>) {
        self.lock_links().insert(address, link);
    }

    /// Looks up the link registered under `address`.
    fn link_at(&self, address: &str) -> Option<Arc<Link>> {
        self.lock_links().get(address).cloned()
    }

    /// Locks the address book. It is a map changed by one insertion at a time, so a lock poisoned by a panicking test
    /// thread still guards a consistent map and is used as it is.
    fn lock_links(&self) -> MutexGuard<'_, BTreeMap<String, Arc<Link>>> {
        self.links_by_address.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl ReplicaConnector for ClusterConnector {
    /// Answers with the link registered under `address`, addressed to `replica_id`, or with a replica that is never
    /// reachable if no node has that address.
    fn connect(&self, replica_id: &ReplicaId, address: &str) -> Arc<dyn ReplicaApi> {
        match self.link_at(address) {
            Some(link) => Arc::new(AddressedLink {
                link,
                expected_replica_id: replica_id.clone(),
            }),
            None => Arc::new(UnreachableReplica::new(format!("no node is reachable at {address}"))),
        }
    }
}

/// A [`Link`] used on behalf of a caller that expects one particular replica behind it. This is what the cluster's
/// connector hands out.
struct AddressedLink {
    /// The network path to the node at the address the caller asked for.
    link: Arc<Link>,
    /// The identity the caller asked for. Requests reach the replica behind the link only while it has this identity.
    expected_replica_id: ReplicaId,
}

impl AddressedLink {
    /// Carries one request to the replica the caller expects. See [`Link::carry_to_replica`].
    async fn carry_to_expected_replica(&self) -> Result<Arc<Replica>, ReplicaError> {
        self.link.carry_to_replica(Some(&self.expected_replica_id)).await
    }
}

#[async_trait]
impl ReplicaApi for AddressedLink {
    /// Appends through the link, if the expected replica is behind it.
    async fn append(&self, request: AppendRequest) -> Result<AppendResponse, ReplicaError> {
        let replica = self.carry_to_expected_replica().await?;
        self.link.carry_back(replica.append(request).await)
    }

    /// Reads through the link, if the expected replica is behind it.
    async fn read(&self, request: ReadRequest) -> Result<ReadResponse, ReplicaError> {
        let replica = self.carry_to_expected_replica().await?;
        self.link.carry_back(replica.read(request).await)
    }

    /// Fences through the link, if the expected replica is behind it.
    async fn fence(&self, request: FenceRequest) -> Result<FenceResponse, ReplicaError> {
        let replica = self.carry_to_expected_replica().await?;
        self.link.carry_back(replica.fence(request).await)
    }

    /// Seals through the link, if the expected replica is behind it.
    async fn seal(&self, request: SealRequest) -> Result<SealResponse, ReplicaError> {
        let replica = self.carry_to_expected_replica().await?;
        self.link.carry_back(replica.seal(request).await)
    }

    /// Asks for a repair through the link, if the expected replica is behind it.
    async fn repair_segment(&self, request: RepairRequest) -> Result<RepairResponse, ReplicaError> {
        let replica = self.carry_to_expected_replica().await?;
        self.link.carry_back(replica.repair_segment(request).await)
    }

    /// Asks for an upload through the link, if the expected replica is behind it.
    async fn upload_segment(&self, request: UploadRequest) -> Result<UploadResponse, ReplicaError> {
        let replica = self.carry_to_expected_replica().await?;
        self.link.carry_back(replica.upload_segment(request).await)
    }

    /// Deletes a segment copy through the link, if the expected replica is behind it.
    async fn delete_segment(&self, segment: SegmentAddress) -> Result<(), ReplicaError> {
        let replica = self.carry_to_expected_replica().await?;
        self.link.carry_back(replica.delete_segment(segment).await)
    }

    /// Lists the segment copies through the link, if the expected replica is behind it.
    async fn list_segments(&self) -> Result<Vec<SegmentStatus>, ReplicaError> {
        let replica = self.carry_to_expected_replica().await?;
        self.link.carry_back(replica.list_segments().await)
    }

    /// Asks for the replica's status through the link, if the expected replica is behind it.
    async fn status(&self) -> Result<ReplicaStatus, ReplicaError> {
        let replica = self.carry_to_expected_replica().await?;
        self.link.carry_back(replica.status().await)
    }

    /// Asks the replica to drain, through the link, if the expected replica is behind it.
    async fn drain(&self) -> Result<(), ReplicaError> {
        let replica = self.carry_to_expected_replica().await?;
        self.link.carry_back(replica.drain().await)
    }
}

#[cfg(test)]
#[path = "connector_test.rs"]
mod tests;
