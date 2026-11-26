//! The cluster: storage nodes, the stores they share and the clock they follow.
//!
//! # Why this file exists
//!
//! Tests of replication and recovery need several replicas that can crash, lose their network or corrupt their disk on
//! demand, a metadata store those replicas are registered in, and an object store their sealed segments are uploaded
//! to. Real processes make that slow and non-deterministic. This file builds the same topology inside one process, on
//! in-memory disks, in-memory stores and a virtual clock, so that a test can inject any fault at an exact point and get
//! the same result on every run.
//!
//! # What it contains
//!
//! - [`Cluster`]: the nodes plus the metadata store they register in, the object store they upload to, the clock, and
//!   the connector that reaches the nodes through their links.
//!
//! # How it works
//!
//! [`Cluster::start_with`] creates the stores and the connector, then starts the nodes one after the other, in order of
//! their numbers, and registers each in the metadata store as a replica would register itself. From then on the cluster
//! is a bag of handles: the test, or the code under test, takes what it needs ([`Cluster::metadata`],
//! [`Cluster::connector`], [`Cluster::clock`], [`Cluster::artifact_store`]) and the cluster itself does nothing more.
//!
//! In particular it has no timer. A real replica renews its registration by itself; here the test calls
//! [`Cluster::renew_registrations`] when it wants the registry to be fresh, and a node that is down then keeps its old
//! heartbeat and ages out, as it would in production.
//!
//! # Invariants
//!
//! - Nodes are started and registered in order of their numbers, and `nodes()[N]` is node `N` for the life of the
//!   cluster.
//! - Every node shares the one metadata store, the one object store, the one clock and the one connector.
//! - The cluster starts no task and no timer: nothing happens in it unless the caller makes it happen.
//!
//! # See also
//!
//! - `node.rs`: one node and what can happen to it.
//! - `layout.rs`: the failure domains of the nodes.
//! - `connector.rs`: how the nodes are reached by address.
//! - `clock.rs`: the cluster's wall clock.

use std::sync::Arc;

use bitacora_artifact::MemoryArtifactStore;
use bitacora_meta::{MemoryKeyValueStore, MetaError, MetadataStore};
use bitacora_replica::ReplicaSettings;
use bitacora_types::replica_api::ReplicaConnector;
use bitacora_types::{Clock, ReplicaId};

use crate::clock::VirtualClock;
use crate::connector::ClusterConnector;
use crate::layout::FailureDomainLayout;
use crate::node::{Node, NodeEnvironment};

/// The size of the segment files of a cluster started with [`Cluster::start`]. Small, because every node reserves it
/// for every open segment and tests open many.
const SMALL_SEGMENT_FILE_BYTES: u64 = 4 * 1024;

/// An in-process cluster: storage nodes, a metadata store and an object store.
///
/// Start one with [`Cluster::start`], hand its [`Cluster::metadata`], [`Cluster::connector`] and [`Cluster::clock`] to
/// the code under test, and use [`Cluster::nodes`] to make things go wrong.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use bitacora_testkit::Cluster;
///
/// let cluster = Cluster::start(3).await;
///
/// cluster.nodes()[2].crash();
///
/// let registered = cluster.metadata().list_replicas().await?;
/// assert_eq!(registered.len(), 3);
/// assert!(!cluster.nodes()[2].is_running());
/// # Ok(())
/// # }
/// ```
pub struct Cluster {
    /// The backend of the metadata store, kept to simulate its outages.
    key_value_store: MemoryKeyValueStore,
    /// The metadata store the nodes are registered in.
    metadata: MetadataStore,
    /// The cluster's wall clock. Shared, because clients and the controller are handed it too.
    clock: Arc<VirtualClock>,
    /// The storage nodes, in order of their numbers.
    nodes: Vec<Node>,
    /// The address book that reaches every node through its link.
    connector: ClusterConnector,
    /// The object store every node uploads sealed segments to.
    artifact_store: MemoryArtifactStore,
}

impl Cluster {
    /// Starts `node_count` nodes named `replica-0`, `replica-1`, ... with small segment files, each on a host of its
    /// own, and registers them.
    ///
    /// The same as [`Cluster::start_with`] with segment files of 4 KiB and [`FailureDomainLayout::HostPerNode`]. The
    /// host label matters when a node loses its disk: its old and its new identity are then registered side by side for
    /// a while, and the label is what keeps placement from taking them for two machines.
    ///
    /// # Arguments
    ///
    /// * `node_count` - How many storage nodes the cluster has.
    ///
    /// # Returns
    ///
    /// The cluster, with every node running and registered.
    ///
    /// # Panics
    ///
    /// Panics if a replica cannot be opened or registered, which the in-memory disks and stores do not cause.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_testkit::Cluster;
    ///
    /// let cluster = Cluster::start(3).await;
    ///
    /// assert_eq!(cluster.nodes().len(), 3);
    /// # }
    /// ```
    pub async fn start(node_count: usize) -> Self {
        let small_segments = ReplicaSettings {
            preallocated_segment_bytes: SMALL_SEGMENT_FILE_BYTES,
            ..ReplicaSettings::default()
        };
        Self::start_with(node_count, small_segments, FailureDomainLayout::HostPerNode).await
    }

    /// Starts `node_count` nodes with the given replica settings and failure domains, and registers them.
    ///
    /// Creates the metadata store, the object store and the clock, starts the nodes in order of their numbers, each on
    /// an empty in-memory disk, and writes each node's registration to the metadata store.
    ///
    /// # Arguments
    ///
    /// * `node_count` - How many storage nodes the cluster has.
    /// * `settings` - The settings every replica is opened with, now and at every restart.
    /// * `layout` - Which zones and hosts the nodes are in.
    ///
    /// # Returns
    ///
    /// The cluster, with every node running and registered.
    ///
    /// # Panics
    ///
    /// Panics if `layout` is [`FailureDomainLayout::HostsInZones`] with no zone, or if a replica cannot be opened or
    /// registered, which the in-memory disks and stores do not cause.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use bitacora_replica::ReplicaSettings;
    /// use bitacora_testkit::{Cluster, FailureDomainLayout};
    ///
    /// let two_zones = FailureDomainLayout::HostsInZones { zone_count: 2 };
    ///
    /// let cluster = Cluster::start_with(4, ReplicaSettings::default(), two_zones).await;
    ///
    /// let registered = cluster.metadata().list_replicas().await?;
    /// assert_eq!(registered[3].failure_domains, ["zone-1", "host-3"]);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn start_with(node_count: usize, settings: ReplicaSettings, layout: FailureDomainLayout) -> Self {
        let key_value_store = MemoryKeyValueStore::new();
        let metadata = MetadataStore::new(Arc::new(key_value_store.clone()));
        let connector = ClusterConnector::default();
        let artifact_store = MemoryArtifactStore::new();
        let environment = NodeEnvironment {
            settings,
            connector: connector.clone(),
            artifact_store: artifact_store.clone(),
        };
        let mut nodes = Vec::with_capacity(node_count);
        for node_number in 0..node_count {
            let failure_domains = layout.failure_domains_of(node_number);
            nodes.push(Node::start(node_number, failure_domains, environment.clone()).await);
        }
        let cluster = Self {
            key_value_store,
            metadata,
            clock: Arc::new(VirtualClock::new()),
            nodes,
            connector,
            artifact_store,
        };
        cluster
            .renew_registrations()
            .await
            .expect("a fresh metadata store is available");
        cluster
    }

    /// Hands out the metadata store's backend, for simulating outages.
    ///
    /// # Returns
    ///
    /// The in-memory key-value store under [`Cluster::metadata`]. Making it unavailable makes every metadata operation
    /// of the cluster fail until it is made available again.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_testkit::Cluster;
    ///
    /// let cluster = Cluster::start(1).await;
    ///
    /// cluster.key_value_store().set_unavailable(true);
    ///
    /// assert!(cluster.metadata().list_replicas().await.is_err());
    /// # }
    /// ```
    #[must_use]
    pub const fn key_value_store(&self) -> &MemoryKeyValueStore {
        &self.key_value_store
    }

    /// Hands out the object store every node uploads sealed segments to.
    ///
    /// It is shared, like a bucket, and outlives any node's disk. Use it to read what was uploaded and to simulate
    /// outages.
    ///
    /// # Returns
    ///
    /// The cluster's in-memory object store. Clones of it share its content.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use bitacora_artifact::ArtifactStore;
    /// use bitacora_testkit::Cluster;
    ///
    /// let cluster = Cluster::start(1).await;
    ///
    /// let uploaded = cluster.artifact_store().list_prefix("streams/").await?;
    ///
    /// assert!(uploaded.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub const fn artifact_store(&self) -> &MemoryArtifactStore {
        &self.artifact_store
    }

    /// Hands out the metadata store.
    ///
    /// # Returns
    ///
    /// The store the nodes are registered in, and in which the code under test keeps its streams.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use bitacora_testkit::Cluster;
    ///
    /// let cluster = Cluster::start(2).await;
    ///
    /// let registered = cluster.metadata().list_replicas().await?;
    ///
    /// assert_eq!(registered[1].address, "replica-1:7000");
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub const fn metadata(&self) -> &MetadataStore {
        &self.metadata
    }

    /// Hands out the cluster's wall clock.
    ///
    /// Give it to everything that reads the time in a test, so that heartbeats, leases and the checks on them agree. It
    /// advances with the runtime's clock.
    ///
    /// # Returns
    ///
    /// The clock, shared with the cluster.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use std::time::Duration;
    ///
    /// use bitacora_testkit::Cluster;
    ///
    /// let cluster = Cluster::start(1).await;
    /// let clock = cluster.clock();
    /// let started_at = clock.now();
    ///
    /// tokio::time::advance(Duration::from_secs(5)).await;
    ///
    /// assert_eq!(clock.now(), started_at.plus(Duration::from_secs(5)));
    /// # }
    /// ```
    #[must_use]
    pub fn clock(&self) -> Arc<dyn Clock> {
        Arc::clone(&self.clock) as Arc<dyn Clock>
    }

    /// Hands out the way to reach the cluster's replicas through their links.
    ///
    /// Give it to clients and to the controller. Every request made through it crosses the link of the node at the
    /// address and meets the link's faults, and reaches the replica only if it still has the identity the caller asked
    /// for.
    ///
    /// # Returns
    ///
    /// A connector that knows every node of the cluster by its address.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use bitacora_testkit::Cluster;
    ///
    /// let cluster = Cluster::start(1).await;
    /// let node = &cluster.nodes()[0];
    ///
    /// let replica = cluster.connector().connect(&node.replica_id(), node.address());
    ///
    /// assert_eq!(replica.status().await?.replica_id, node.replica_id());
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn connector(&self) -> Arc<dyn ReplicaConnector> {
        Arc::new(self.connector.clone())
    }

    /// Hands out the cluster's storage nodes.
    ///
    /// # Returns
    ///
    /// The nodes in the order they were started: the node at index `N` is node `N`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_testkit::Cluster;
    ///
    /// let cluster = Cluster::start(3).await;
    ///
    /// for node in cluster.nodes() {
    ///     node.set_reachable(false);
    /// }
    ///
    /// assert!(cluster.nodes()[0].link().fault().is_cut);
    /// # }
    /// ```
    #[must_use]
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// Reports the current identities of the nodes at `node_numbers`.
    ///
    /// # Arguments
    ///
    /// * `node_numbers` - Positions of the nodes, counted from zero. The answer is in the same order.
    ///
    /// # Returns
    ///
    /// One identity per number given.
    ///
    /// # Panics
    ///
    /// Panics if a number is not smaller than the node count.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_testkit::Cluster;
    ///
    /// let cluster = Cluster::start(3).await;
    ///
    /// let replica_ids = cluster.replica_ids(&[2, 0]);
    ///
    /// assert_eq!(replica_ids[0].to_string(), "replica-2");
    /// assert_eq!(replica_ids[1].to_string(), "replica-0");
    /// # }
    /// ```
    #[must_use]
    pub fn replica_ids(&self, node_numbers: &[usize]) -> Vec<ReplicaId> {
        node_numbers
            .iter()
            .map(|node_number| self.nodes[*node_number].replica_id())
            .collect()
    }

    /// Renews the registration of every running node, as replicas do on a timer.
    ///
    /// Writes one registry record per running node, in order of the node numbers, with the node's current identity, its
    /// disk space and a heartbeat at the present moment of [`Cluster::clock`]. A node that is down is skipped: it keeps
    /// its old heartbeat and so ages out. A node that lost its disk and was restarted registers under its new identity,
    /// next to the record of its old one.
    ///
    /// # Errors
    ///
    /// Returns the metadata store's error if a record cannot be written, for example [`MetaError::Unavailable`] during
    /// a simulated outage. Nodes before the failing one were renewed.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use std::time::Duration;
    ///
    /// use bitacora_testkit::Cluster;
    ///
    /// let cluster = Cluster::start(1).await;
    /// tokio::time::advance(Duration::from_secs(10)).await;
    ///
    /// cluster.renew_registrations().await?;
    ///
    /// let registered = cluster.metadata().list_replicas().await?;
    /// assert_eq!(registered[0].heartbeat_at, cluster.clock().now());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn renew_registrations(&self) -> Result<(), MetaError> {
        for node in &self.nodes {
            if !node.is_running() {
                continue;
            }
            let registration = node.registration(self.clock.as_ref()).await;
            self.metadata.put_replica(&registration).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "cluster_test.rs"]
mod tests;
