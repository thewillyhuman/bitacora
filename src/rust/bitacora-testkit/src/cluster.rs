//! Storage nodes and the cluster they form.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use bitacora_meta::{
    MemoryKeyValueStore, MetaError, MetadataStore, RECORD_SCHEMA_VERSION, ReplicaRecord,
};
use bitacora_replica::{Replica, ReplicaSettings};
use bitacora_segment::format::{Batch, decode_frames};
use bitacora_segment::{Disk, MemoryDisk};
use bitacora_types::replica_api::{
    ReadLimit, ReadRequest, ReplicaApi, ReplicaConnector, UnreachableReplica,
};
use bitacora_types::{Clock, ReplicaId, SegmentAddress, SequenceNumber};

use crate::clock::VirtualClock;
use crate::link::{AddressedLink, Link, LinkFault};

/// Reaches the cluster's nodes through their links.
///
/// The replicas use it too, to repair copies from each other, so it has to
/// exist before the first node does and learn of nodes as they start.
#[derive(Clone, Default)]
struct ClusterConnector {
    links_by_address: Arc<Mutex<BTreeMap<String, Arc<Link>>>>,
}

impl ClusterConnector {
    fn register(&self, address: String, link: Arc<Link>) {
        self.links_by_address
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(address, link);
    }
}

impl ReplicaConnector for ClusterConnector {
    fn connect(&self, replica_id: &ReplicaId, address: &str) -> Arc<dyn ReplicaApi> {
        let link = self
            .links_by_address
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(address)
            .cloned();
        match link {
            Some(link) => Arc::new(AddressedLink {
                link,
                expected_replica_id: replica_id.clone(),
            }),
            None => Arc::new(UnreachableReplica::new(format!(
                "no node is reachable at {address}"
            ))),
        }
    }
}

/// A node's disk and the identity that belongs to it.
struct Storage {
    replica_id: ReplicaId,
    disk: MemoryDisk,
    /// How many disks the node has gone through.
    disks_lost: u32,
}

/// One storage node: a replica process, the disk it lives on, and the
/// network path to it.
pub struct Node {
    node_number: usize,
    address: String,
    failure_domains: Vec<String>,
    storage: Mutex<Storage>,
    link: Arc<Link>,
    settings: ReplicaSettings,
    connector: ClusterConnector,
    has_failed_write: Arc<AtomicBool>,
}

impl Node {
    async fn start(
        node_number: usize,
        failure_domains: Vec<String>,
        settings: ReplicaSettings,
        connector: ClusterConnector,
    ) -> Self {
        let replica_id =
            ReplicaId::new(format!("replica-{node_number}")).expect("valid identifier");
        let disk = MemoryDisk::new();
        let has_failed_write = Arc::new(AtomicBool::new(false));
        let replica =
            open_replica(&disk, &replica_id, settings, &connector, &has_failed_write).await;
        let node = Self {
            node_number,
            address: format!("{replica_id}:7000"),
            failure_domains,
            storage: Mutex::new(Storage {
                replica_id,
                disk,
                disks_lost: 0,
            }),
            link: Arc::new(Link::new(replica)),
            settings,
            connector,
            has_failed_write,
        };
        node.connector
            .register(node.address.clone(), Arc::clone(&node.link));
        node
    }

    /// The identity of the node's current data directory. It changes when
    /// the node loses its disk.
    #[must_use]
    pub fn replica_id(&self) -> ReplicaId {
        self.storage().replica_id.clone()
    }

    fn storage(&self) -> std::sync::MutexGuard<'_, Storage> {
        // Plain data replaced in one step.
        self.storage.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The address the node is registered under.
    #[must_use]
    pub fn address(&self) -> &str {
        &self.address
    }

    /// The node's current disk, for injecting disk faults and inspecting
    /// files. The handle shares the disk's content.
    #[must_use]
    pub fn disk(&self) -> MemoryDisk {
        self.storage().disk.clone()
    }

    /// The network path to the node, for injecting network faults.
    #[must_use]
    pub const fn link(&self) -> &Arc<Link> {
        &self.link
    }

    /// The replica process, bypassing the network. `None` while the node is
    /// down.
    #[must_use]
    pub fn replica(&self) -> Option<Arc<Replica>> {
        self.link.replica()
    }

    /// Whether the replica process is running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.replica().is_some()
    }

    /// Cuts or restores the network path to the node, leaving any other
    /// fault of the link as it is.
    pub fn set_reachable(&self, is_reachable: bool) {
        self.link.set_fault(LinkFault {
            is_cut: !is_reachable,
            ..self.link.fault()
        });
    }

    /// Whether the disk refused a write or a flush since the node started.
    /// A real replica exits when that happens; whoever drives the cluster
    /// is expected to crash the node.
    #[must_use]
    pub fn has_failed_write(&self) -> bool {
        self.has_failed_write.load(Ordering::Relaxed)
    }

    /// Kills the replica process and loses everything that was not flushed.
    pub fn crash(&self) {
        self.crash_keeping(|_path, durable, _visible| durable.to_vec());
    }

    /// Kills the replica process; `surviving_content` decides, per file,
    /// what the disk holds afterwards. See [`MemoryDisk::crash_keeping`].
    pub fn crash_keeping(&self, surviving_content: impl FnMut(&str, &[u8], &[u8]) -> Vec<u8>) {
        if let Some(replica) = self.replica() {
            replica.stop();
        }
        self.link.set_replica(None);
        self.disk().crash_keeping(surviving_content);
    }

    /// Kills the replica process and replaces the node's disk with an empty
    /// one, as when a machine is rebuilt. Everything the node stored is
    /// gone, and once restarted it runs under a new identity, so that it can
    /// never claim copies it no longer holds. The old identity stays in the
    /// registry until it ages out or is removed.
    pub fn lose_disk(&self) {
        if let Some(replica) = self.replica() {
            replica.stop();
        }
        self.link.set_replica(None);
        let mut storage = self.storage();
        storage.disks_lost += 1;
        storage.replica_id = ReplicaId::new(format!(
            "replica-{}-disk-{}",
            self.node_number, storage.disks_lost
        ))
        .expect("valid identifier");
        storage.disk = MemoryDisk::new();
    }

    /// Starts the replica process again on the same disk. Does nothing if
    /// it is running.
    pub async fn restart(&self) {
        if self.is_running() {
            return;
        }
        self.has_failed_write.store(false, Ordering::Relaxed);
        let (disk, replica_id) = (self.disk(), self.replica_id());
        disk.fail_flushes(false);
        let replica = open_replica(
            &disk,
            &replica_id,
            self.settings,
            &self.connector,
            &self.has_failed_write,
        )
        .await;
        self.link.set_replica(Some(replica));
    }

    /// The end of this node's copy of `segment`, if it is running and has
    /// one.
    pub async fn local_end(&self, segment: SegmentAddress) -> Option<SequenceNumber> {
        self.replica()?
            .list_segments()
            .await
            .ok()?
            .into_iter()
            .find(|status| status.address == segment)
            .and_then(|status| status.local_end)
    }

    /// The sequence numbers this node would serve to a reader of `segment`.
    /// Empty if the node is down or has no copy.
    pub async fn readable_sequences(&self, segment: SegmentAddress) -> Vec<u64> {
        let Some(replica) = self.replica() else {
            return Vec::new();
        };
        let request = ReadRequest {
            segment,
            from_sequence: SequenceNumber::FIRST,
            max_bytes: usize::MAX,
            wait_for_records: None,
            limit: ReadLimit::DurableSequence,
        };
        let Ok(response) = replica.read(request).await else {
            return Vec::new();
        };
        decode_frames(segment, &response.batch_frames)
            .expect("a replica serves valid frames")
            .iter()
            .flat_map(Batch::records)
            .map(|record| record.sequence.get())
            .collect()
    }

    async fn registration(&self, clock: &dyn Clock) -> ReplicaRecord {
        let space = self
            .disk()
            .space()
            .await
            .expect("a memory disk always reports its space");
        ReplicaRecord {
            schema_version: RECORD_SCHEMA_VERSION,
            replica_id: self.replica_id(),
            address: self.address.clone(),
            failure_domains: self.failure_domains.clone(),
            capacity_bytes: space.capacity_bytes,
            free_bytes: space.free_bytes,
            is_draining: false,
            heartbeat_at: clock.now(),
        }
    }
}

async fn open_replica(
    disk: &MemoryDisk,
    replica_id: &ReplicaId,
    settings: ReplicaSettings,
    connector: &ClusterConnector,
    has_failed_write: &Arc<AtomicBool>,
) -> Arc<Replica> {
    let has_failed_write = Arc::clone(has_failed_write);
    let replica = Replica::open(
        Arc::new(disk.clone()),
        replica_id.clone(),
        settings,
        Arc::new(connector.clone()),
        Arc::new(move |_segment, _reason| has_failed_write.store(true, Ordering::Relaxed)),
    )
    .await
    .expect("a replica opens on a memory disk");
    Arc::new(replica)
}

/// An in-process cluster: storage nodes and a metadata store.
pub struct Cluster {
    key_value_store: MemoryKeyValueStore,
    metadata: MetadataStore,
    clock: Arc<VirtualClock>,
    nodes: Vec<Node>,
    connector: ClusterConnector,
}

impl Cluster {
    /// Starts `node_count` nodes named `replica-0`, `replica-1`, ... with
    /// small segment files, each on a host of its own, and registers them.
    ///
    /// The host label matters when a node loses its disk: its old and its
    /// new identity are then registered side by side for a while, and the
    /// label is what keeps placement from taking them for two machines.
    pub async fn start(node_count: usize) -> Self {
        let small_segments = ReplicaSettings {
            preallocated_segment_bytes: 4 * 1024,
            ..ReplicaSettings::default()
        };
        Self::start_with(node_count, small_segments, |node_number| {
            vec![format!("host-{node_number}")]
        })
        .await
    }

    /// Starts `node_count` nodes with the given replica settings, where
    /// `failure_domains_of` names each node's failure domains, widest first.
    pub async fn start_with(
        node_count: usize,
        settings: ReplicaSettings,
        failure_domains_of: impl Fn(usize) -> Vec<String>,
    ) -> Self {
        let key_value_store = MemoryKeyValueStore::new();
        let metadata = MetadataStore::new(Arc::new(key_value_store.clone()));
        let connector = ClusterConnector::default();
        let mut nodes = Vec::with_capacity(node_count);
        for node_number in 0..node_count {
            nodes.push(
                Node::start(
                    node_number,
                    failure_domains_of(node_number),
                    settings,
                    connector.clone(),
                )
                .await,
            );
        }
        let cluster = Self {
            key_value_store,
            metadata,
            clock: Arc::new(VirtualClock::new()),
            nodes,
            connector,
        };
        cluster
            .renew_registrations()
            .await
            .expect("a fresh metadata store is available");
        cluster
    }

    /// The metadata store's backend, for simulating outages.
    #[must_use]
    pub const fn key_value_store(&self) -> &MemoryKeyValueStore {
        &self.key_value_store
    }

    /// The metadata store.
    #[must_use]
    pub const fn metadata(&self) -> &MetadataStore {
        &self.metadata
    }

    /// The cluster's wall clock.
    #[must_use]
    pub fn clock(&self) -> Arc<dyn Clock> {
        Arc::clone(&self.clock) as Arc<dyn Clock>
    }

    /// The way to reach the cluster's replicas through their links.
    #[must_use]
    pub fn connector(&self) -> Arc<dyn ReplicaConnector> {
        Arc::new(self.connector.clone())
    }

    /// The cluster's storage nodes, in the order they were started.
    #[must_use]
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// The identifiers of the nodes at `node_numbers`.
    ///
    /// # Panics
    ///
    /// Panics if a number is not smaller than the node count.
    #[must_use]
    pub fn replica_ids(&self, node_numbers: &[usize]) -> Vec<ReplicaId> {
        node_numbers
            .iter()
            .map(|node_number| self.nodes[*node_number].replica_id())
            .collect()
    }

    /// Renews the registration of every running node, as replicas do on a
    /// timer. A node that is down keeps its old heartbeat and so ages out.
    ///
    /// # Errors
    ///
    /// Fails if the metadata store is unavailable.
    pub async fn renew_registrations(&self) -> Result<(), MetaError> {
        for node in self.nodes.iter().filter(|node| node.is_running()) {
            self.metadata
                .put_replica(&node.registration(self.clock.as_ref()).await)
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bitacora_types::replica_api::{AppendRequest, ReplicaError, SegmentDescriptor};
    use bitacora_types::{Epoch, SegmentId, StreamId};
    use bytes::Bytes;
    use tokio::time::Instant;

    use super::*;

    const SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(1), SegmentId::new(1));

    /// An append of `record_count` records starting at `first_sequence`,
    /// with everything before it declared durable.
    fn append(first_sequence: u64, record_count: u64) -> AppendRequest {
        let payloads: Vec<Bytes> = (0..record_count)
            .map(|number| Bytes::from(format!("record-{number}")))
            .collect();
        AppendRequest {
            segment: SegmentDescriptor {
                address: SEGMENT,
                epoch: Epoch::new(1),
                first_sequence: SequenceNumber::FIRST,
            },
            writer_epoch: Epoch::new(1),
            batch_frames: Batch::encode(SEGMENT, SequenceNumber::new(first_sequence), &payloads)
                .expect("valid batch")
                .frame()
                .clone(),
            durable_sequence: SequenceNumber::new(first_sequence - 1),
        }
    }

    async fn cluster_with_three_records() -> Cluster {
        let cluster = Cluster::start(1).await;
        cluster.nodes()[0]
            .link()
            .append(append(1, 3))
            .await
            .expect("append");
        cluster
    }

    #[tokio::test(start_paused = true)]
    async fn started_nodes_are_registered_under_their_addresses() {
        let cluster = Cluster::start(2).await;
        let registered: Vec<String> = cluster
            .metadata()
            .list_replicas()
            .await
            .expect("list")
            .into_iter()
            .map(|replica| replica.address)
            .collect();
        assert_eq!(registered, ["replica-0:7000", "replica-1:7000"]);
    }

    #[tokio::test(start_paused = true)]
    async fn connector_reaches_a_node_through_its_link() {
        let cluster = cluster_with_three_records().await;
        let node = &cluster.nodes()[0];
        let status = cluster
            .connector()
            .connect(&node.replica_id(), node.address())
            .status()
            .await;
        assert_eq!(
            status.map(|status| status.replica_id),
            Ok(node.replica_id())
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cut_link_fails_requests_without_touching_the_replica() {
        let cluster = cluster_with_three_records().await;
        let node = &cluster.nodes()[0];
        node.set_reachable(false);
        let refused = node.link().append(append(4, 1)).await;
        assert!(
            matches!(refused, Err(ReplicaError::Unavailable { .. }))
                && node.local_end(SEGMENT).await == Some(SequenceNumber::new(3))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn restored_link_carries_requests_again() {
        let cluster = cluster_with_three_records().await;
        let node = &cluster.nodes()[0];
        node.set_reachable(false);
        node.set_reachable(true);
        assert!(node.link().append(append(4, 1)).await.is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn link_that_loses_responses_still_applies_the_request() {
        let cluster = cluster_with_three_records().await;
        let node = &cluster.nodes()[0];
        node.link().set_fault(LinkFault {
            loses_responses: true,
            ..LinkFault::default()
        });
        let answer = node.link().append(append(4, 1)).await;
        assert!(
            matches!(answer, Err(ReplicaError::Unavailable { .. }))
                && node.local_end(SEGMENT).await == Some(SequenceNumber::new(4))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn slow_link_holds_requests_for_its_delay() {
        let cluster = cluster_with_three_records().await;
        let node = &cluster.nodes()[0];
        node.link().set_fault(LinkFault {
            delay: Duration::from_millis(300),
            ..LinkFault::default()
        });
        let started_at = Instant::now();
        node.link().append(append(4, 1)).await.expect("append");
        assert_eq!(started_at.elapsed(), Duration::from_millis(300));
    }

    #[tokio::test(start_paused = true)]
    async fn request_in_flight_when_the_link_is_cut_never_arrives() {
        let cluster = Arc::new(cluster_with_three_records().await);
        cluster.nodes()[0].link().set_fault(LinkFault {
            delay: Duration::from_millis(300),
            ..LinkFault::default()
        });
        let in_flight = {
            let cluster = Arc::clone(&cluster);
            tokio::spawn(async move { cluster.nodes()[0].link().append(append(4, 1)).await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        cluster.nodes()[0].set_reachable(false);
        let answer = in_flight.await.expect("task finishes");
        assert!(
            matches!(answer, Err(ReplicaError::Unavailable { .. }))
                && cluster.nodes()[0].local_end(SEGMENT).await == Some(SequenceNumber::new(3))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn crashed_node_is_down_until_restarted() {
        let cluster = cluster_with_three_records().await;
        let node = &cluster.nodes()[0];
        node.crash();
        let while_down = node.link().append(append(4, 1)).await;
        node.restart().await;
        assert!(
            matches!(while_down, Err(ReplicaError::Unavailable { .. }))
                && node.local_end(SEGMENT).await == Some(SequenceNumber::new(3))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn crash_can_leave_whatever_the_caller_decides_on_disk() {
        let cluster = cluster_with_three_records().await;
        let node = &cluster.nodes()[0];
        // Keep the segment header and the first bytes of the only batch.
        let surviving_bytes = bitacora_segment::format::SEGMENT_HEADER_BYTES + 16;
        node.crash_keeping(|_path, durable, _visible| {
            durable[..durable.len().min(surviving_bytes)].to_vec()
        });
        node.restart().await;
        assert_eq!(node.local_end(SEGMENT).await, Some(SequenceNumber::ZERO));
    }

    #[tokio::test(start_paused = true)]
    async fn node_that_lost_its_disk_returns_empty_under_a_new_identity() {
        let cluster = cluster_with_three_records().await;
        let node = &cluster.nodes()[0];
        let identity_before = node.replica_id();
        node.lose_disk();
        node.restart().await;
        let status = node.link().status().await.expect("status");
        assert!(
            status.replica_id != identity_before
                && status.replica_id == node.replica_id()
                && status.segment_count == 0,
            "{status:?}"
        );
    }

    /// A rebuilt node must not answer in the name of the node it replaced:
    /// it holds none of the copies the old identity held.
    #[tokio::test(start_paused = true)]
    async fn rebuilt_node_does_not_answer_for_its_old_identity() {
        let cluster = cluster_with_three_records().await;
        let node = &cluster.nodes()[0];
        let old_identity = node.replica_id();
        node.lose_disk();
        node.restart().await;
        let connector = cluster.connector();
        let as_old_identity = connector
            .connect(&old_identity, node.address())
            .list_segments()
            .await;
        let as_new_identity = connector
            .connect(&node.replica_id(), node.address())
            .list_segments()
            .await;
        assert!(
            matches!(as_old_identity, Err(ReplicaError::Unavailable { .. }))
                && as_new_identity == Ok(Vec::new()),
            "as the old identity: {as_old_identity:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn node_that_lost_its_disk_registers_next_to_its_old_identity() {
        let cluster = cluster_with_three_records().await;
        cluster.nodes()[0].lose_disk();
        cluster.nodes()[0].restart().await;
        cluster.renew_registrations().await.expect("renewed");
        let registered: Vec<String> = cluster
            .metadata()
            .list_replicas()
            .await
            .expect("list")
            .into_iter()
            .map(|replica| replica.replica_id.to_string())
            .collect();
        assert_eq!(registered, ["replica-0", "replica-0-disk-1"]);
    }

    #[tokio::test(start_paused = true)]
    async fn node_reports_a_failed_flush_until_it_is_restarted() {
        let cluster = cluster_with_three_records().await;
        let node = &cluster.nodes()[0];
        node.disk().fail_flushes(true);
        let failed = node.link().append(append(4, 1)).await;
        let reported = node.has_failed_write();
        node.crash();
        node.restart().await;
        assert!(
            matches!(failed, Err(ReplicaError::Storage { .. }))
                && reported
                && !node.has_failed_write()
                && node.link().append(append(4, 1)).await.is_ok()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn node_that_is_down_ages_out_of_the_registry() {
        let cluster = Cluster::start(2).await;
        cluster.nodes()[1].crash();
        tokio::time::advance(Duration::from_secs(60)).await;
        cluster
            .renew_registrations()
            .await
            .expect("the store is available");
        let now = cluster.clock().now();
        let alive: Vec<bool> = cluster
            .metadata()
            .list_replicas()
            .await
            .expect("list")
            .iter()
            .map(|replica| replica.is_alive(now, Duration::from_secs(15)))
            .collect();
        assert_eq!(alive, [true, false]);
    }

    #[tokio::test(start_paused = true)]
    async fn readable_sequences_follow_the_durable_sequence() {
        let cluster = cluster_with_three_records().await;
        let node = &cluster.nodes()[0];
        node.link().append(append(4, 2)).await.expect("append");
        assert_eq!(node.readable_sequences(SEGMENT).await, [1, 2, 3]);
    }
}
