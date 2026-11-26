//! One storage node: a replica process, the disk it lives on, and the things that can happen to the machine.
//!
//! # Why this file exists
//!
//! The replica crate knows how to be a replica while its process runs. What it cannot express is what happens to the
//! machine around it: the process is killed and the disk keeps only what was flushed, or a torn piece of what was not;
//! the disk is replaced by an empty one; the process starts again and has to make sense of what it finds. Recovery is
//! exactly the code that runs after those events, so tests need to cause them at will.
//!
//! This file is the machine. It owns the disk and the identity that belongs to it, starts and kills the replica
//! process, and offers a few ways to look at what the replica holds without going through the network.
//!
//! # What it contains
//!
//! - [`Node`]: the machine, with [`Node::crash`], [`Node::crash_with`], [`Node::lose_disk`] and [`Node::restart`], and
//!   [`Node::local_end`] and [`Node::readable_sequences`] to inspect its copies.
//! - [`NodeEnvironment`]: what a cluster gives every one of its nodes.
//! - `Storage`: a disk and the identity that belongs to it.
//!
//! # How it works
//!
//! A node is two pieces of state, each with one representation:
//!
//! | The node | Process behind its link | Storage |
//! | --- | --- | --- |
//! | runs | a running replica | the disk the replica is open on, and its identity |
//! | crashed | none | the same disk, holding what the crash left, and the same identity |
//! | lost its disk | none | a new, empty disk and a new identity |
//!
//! Whether a process runs is recorded in one place, the node's [`Link`], because the link is what delivers requests to
//! it. The disk and its identity are recorded here, in `Storage`, and are replaced together when the disk is lost.
//! Restarting is the same in both stopped states: open a replica on whatever the storage is now, and put it behind the
//! link.
//!
//! A node keeps its number, its address, its failure domains and its link for life. Its identity is `replica-N` on its
//! first disk and `replica-N-disk-M` on its M-th replacement, so the identity of a rebuilt node is new, as the design
//! demands, and still tells a reader which machine it is.
//!
//! # Invariants
//!
//! - A replica runs behind the link if and only if the node is running; nothing else records it.
//! - The identity changes exactly when the disk is replaced, and never comes back.
//! - Killing the process comes before anything is done to the disk, so the process never sees a disk that changed under
//!   it.
//! - The storage lock guards plain data replaced in one step and is never held across an `await`.
//! - The node does nothing by itself: it does not crash, restart or register on its own.
//!
//! # See also
//!
//! - `link.rs`: the network path to the node and the process behind it.
//! - `cluster.rs`: starts the nodes and registers them.
//! - `bitacora-segment`: `MemoryDisk` and `CrashMode`.
//! - `docs/decisions/0010-replica-identity.md`: why a rebuilt node has a new identity.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bitacora_artifact::MemoryArtifactStore;
use bitacora_meta::{RECORD_SCHEMA_VERSION, ReplicaRecord};
use bitacora_replica::{Replica, ReplicaSettings};
use bitacora_segment::format::decode_frames;
use bitacora_segment::{CrashMode, Disk, MemoryDisk};
use bitacora_types::replica_api::{ReadLimit, ReadRequest, ReplicaApi};
use bitacora_types::{Clock, ReplicaId, SegmentAddress, SequenceNumber};

use crate::connector::ClusterConnector;
use crate::link::{Link, LinkFault};

/// The port in every node's address. Nothing listens on it; it makes the address look like one.
const REPLICA_PORT: u16 = 7000;

/// What a cluster gives every one of its nodes, and what a node needs again each time its replica is opened.
#[derive(Clone)]
pub(crate) struct NodeEnvironment {
    /// The settings every replica of the cluster is opened with.
    pub(crate) settings: ReplicaSettings,
    /// How a replica reaches the other replicas, to repair its copies from them.
    pub(crate) connector: ClusterConnector,
    /// The cluster's object store, which the node's replica uploads sealed segments to.
    pub(crate) artifact_store: MemoryArtifactStore,
}

/// A node's disk and the identity that belongs to it. The two are replaced together when the disk is lost.
struct Storage {
    /// The identity of the data directory on `disk`. It is derived from the node's number and `lost_disk_count` when
    /// the disk is replaced, and kept here because it is read far more often than a disk is lost.
    replica_id: ReplicaId,
    /// The node's current disk. Handles to it share its content.
    disk: MemoryDisk,
    /// How many disks the node has lost. It numbers the identity of the next replacement.
    lost_disk_count: u32,
}

/// One storage node: a replica process, the disk it lives on, and the network path to it.
///
/// A cluster creates its nodes; get them from [`Cluster::nodes`](crate::Cluster::nodes). Use the node to make the
/// machine fail ([`Node::crash`], [`Node::lose_disk`], [`Node::set_reachable`]) and recover ([`Node::restart`]), its
/// [`Node::link`] to send requests the way the network would, and [`Node::replica`] and [`Node::disk`] to look at what
/// it holds.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() {
/// use bitacora_testkit::Cluster;
///
/// let cluster = Cluster::start(3).await;
/// let node = &cluster.nodes()[0];
///
/// node.crash();
/// let is_running_after_the_crash = node.is_running();
/// node.restart().await;
///
/// assert!(!is_running_after_the_crash);
/// assert!(node.is_running());
/// # }
/// ```
pub struct Node {
    /// The node's position in the cluster, counted from zero. It names the node's identities.
    node_number: usize,
    /// The address the node registers under, for life.
    address: String,
    /// The node's failure domains, widest first, as it registers them.
    failure_domains: Vec<String>,
    /// The node's disk and identity. A lock, because losing the disk replaces both through a shared reference.
    storage: Mutex<Storage>,
    /// The network path to the node, which also holds the replica process while one runs.
    link: Arc<Link>,
    /// What the node's replica is opened with, at the start and at every restart.
    environment: NodeEnvironment,
}

impl Node {
    /// Starts node `node_number` on a new, empty disk and makes it reachable through the cluster's connector.
    ///
    /// Opens a replica under the identity `replica-N`, puts it behind a new link, and registers the link under the
    /// node's address. It does not register the node in the metadata store; the cluster does.
    ///
    /// # Arguments
    ///
    /// * `node_number` - The node's position in the cluster, counted from zero.
    /// * `failure_domains` - The node's failure domains, widest first.
    /// * `environment` - What the cluster gives each of its nodes.
    ///
    /// # Returns
    ///
    /// The running node.
    ///
    /// # Panics
    ///
    /// Panics if the replica cannot be opened, which an empty in-memory disk does not cause.
    pub(crate) async fn start(node_number: usize, failure_domains: Vec<String>, environment: NodeEnvironment) -> Self {
        let replica_id = identity_on_disk(node_number, 0);
        let disk = MemoryDisk::new();
        let replica = open_replica(&disk, &replica_id, &environment).await;
        let node = Self {
            node_number,
            address: format!("{replica_id}:{REPLICA_PORT}"),
            failure_domains,
            storage: Mutex::new(Storage {
                replica_id,
                disk,
                lost_disk_count: 0,
            }),
            link: Arc::new(Link::new(replica)),
            environment,
        };
        node.environment
            .connector
            .register(node.address.clone(), Arc::clone(&node.link));
        node
    }

    /// Reports the identity of the node's current data directory.
    ///
    /// The identity belongs to the disk, not to the machine: it changes when the node loses its disk, whether or not
    /// the node was restarted since.
    ///
    /// # Returns
    ///
    /// `replica-N` for node `N` on its first disk, `replica-N-disk-M` on its M-th replacement.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_testkit::Cluster;
    ///
    /// let cluster = Cluster::start(1).await;
    /// let node = &cluster.nodes()[0];
    /// let first_identity = node.replica_id();
    ///
    /// node.lose_disk();
    ///
    /// assert_eq!(first_identity.to_string(), "replica-0");
    /// assert_eq!(node.replica_id().to_string(), "replica-0-disk-1");
    /// # }
    /// ```
    #[must_use]
    pub fn replica_id(&self) -> ReplicaId {
        self.lock_storage().replica_id.clone()
    }

    /// Locks the node's storage. It is plain data replaced in one step, so a lock poisoned by a panicking test thread
    /// still guards a consistent value and is used as it is.
    fn lock_storage(&self) -> MutexGuard<'_, Storage> {
        self.storage.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Reports the address the node is registered under in the metadata store.
    ///
    /// Pass it, with [`Node::replica_id`], to the cluster's connector to reach the node through its link. The address
    /// never changes, not even when the node loses its disk and its identity.
    ///
    /// # Returns
    ///
    /// The address, `replica-N:7000` for node `N`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_testkit::Cluster;
    ///
    /// let cluster = Cluster::start(2).await;
    ///
    /// assert_eq!(cluster.nodes()[1].address(), "replica-1:7000");
    /// # }
    /// ```
    #[must_use]
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Hands out the node's current disk, for injecting disk faults and inspecting files.
    ///
    /// The handle shares the disk's content with the replica: what is written, corrupted or made to fail through it is
    /// what the replica sees. After [`Node::lose_disk`] an earlier handle still refers to the lost disk; ask again for
    /// the new one.
    ///
    /// # Returns
    ///
    /// A handle to the disk the node's replica is, or will next be, opened on.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_testkit::Cluster;
    ///
    /// let cluster = Cluster::start(1).await;
    /// let node = &cluster.nodes()[0];
    ///
    /// let identity_file = node.disk().durable_content("replica.id");
    ///
    /// assert!(identity_file.is_some());
    /// # }
    /// ```
    #[must_use]
    pub fn disk(&self) -> MemoryDisk {
        self.lock_storage().disk.clone()
    }

    /// Hands out the network path to the node, for sending requests and injecting network faults.
    ///
    /// # Returns
    ///
    /// The node's link. It is the same link for the life of the node.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use bitacora_testkit::Cluster;
    /// use bitacora_types::replica_api::ReplicaApi;
    ///
    /// let cluster = Cluster::start(1).await;
    /// let node = &cluster.nodes()[0];
    ///
    /// let status = node.link().status().await?;
    ///
    /// assert_eq!(status.replica_id, node.replica_id());
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub const fn link(&self) -> &Arc<Link> {
        &self.link
    }

    /// Hands out the replica process, bypassing the network.
    ///
    /// Calls made on it are not affected by the faults of the node's link. Use it to look at what the replica holds;
    /// send the requests under test through [`Node::link`].
    ///
    /// # Returns
    ///
    /// The running replica, or `None` while the node is down.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_testkit::Cluster;
    ///
    /// let cluster = Cluster::start(1).await;
    /// let node = &cluster.nodes()[0];
    /// node.set_reachable(false);
    ///
    /// let replica = node.replica().expect("the node is cut off, not down");
    ///
    /// assert_eq!(replica.replica_id(), &node.replica_id());
    /// # }
    /// ```
    #[must_use]
    pub fn replica(&self) -> Option<Arc<Replica>> {
        self.link.running_replica()
    }

    /// Says whether the replica process is running.
    ///
    /// # Returns
    ///
    /// `true` from the start of the node until it crashes or loses its disk, and again after a restart. A node whose
    /// link is cut is still running.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_testkit::Cluster;
    ///
    /// let cluster = Cluster::start(1).await;
    /// let node = &cluster.nodes()[0];
    ///
    /// node.crash();
    ///
    /// assert!(!node.is_running());
    /// # }
    /// ```
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.replica().is_some()
    }

    /// Cuts or restores the network path to the node, leaving any other fault of the link as it is.
    ///
    /// # Arguments
    ///
    /// * `is_reachable` - `false` cuts the link, as a partition does; `true` restores it.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_testkit::Cluster;
    /// use bitacora_types::replica_api::ReplicaApi;
    ///
    /// let cluster = Cluster::start(1).await;
    /// let node = &cluster.nodes()[0];
    ///
    /// node.set_reachable(false);
    ///
    /// assert!(node.link().status().await.is_err());
    /// # }
    /// ```
    pub fn set_reachable(&self, is_reachable: bool) {
        self.link.set_fault(LinkFault {
            is_cut: !is_reachable,
            ..self.link.fault()
        });
    }

    /// Says whether the disk refused a write or a flush to the replica process that is running now.
    ///
    /// A real replica exits when that happens, because it can no longer trust what it sees of its files. The node does
    /// not crash itself: whoever drives the cluster is expected to look here and crash it.
    ///
    /// # Returns
    ///
    /// `true` once the running replica recorded a write failure. `false` before that, while the node is down, and after
    /// a restart, which gives the node a new process.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_testkit::Cluster;
    ///
    /// let cluster = Cluster::start(1).await;
    /// let node = &cluster.nodes()[0];
    ///
    /// if node.has_failed_write() {
    ///     node.crash();
    ///     node.restart().await;
    /// }
    ///
    /// assert!(!node.has_failed_write());
    /// # }
    /// ```
    #[must_use]
    pub fn has_failed_write(&self) -> bool {
        match self.replica() {
            Some(replica) => replica.write_failure().is_some(),
            None => false,
        }
    }

    /// Kills the replica process and loses everything that was not flushed.
    ///
    /// The same as [`Node::crash_with`] with [`CrashMode::LoseUnflushed`]. The node stays down until [`Node::restart`].
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_testkit::Cluster;
    /// use bitacora_types::replica_api::{ReplicaApi, ReplicaError};
    ///
    /// let cluster = Cluster::start(1).await;
    /// let node = &cluster.nodes()[0];
    ///
    /// node.crash();
    ///
    /// let answer = node.link().status().await;
    /// assert!(matches!(answer, Err(ReplicaError::Unavailable { .. })));
    /// # }
    /// ```
    pub fn crash(&self) {
        self.crash_with(CrashMode::LoseUnflushed);
    }

    /// Kills the replica process; `mode` decides what the disk holds afterwards.
    ///
    /// The process is stopped first and the disk crashed second, so the replica never sees the disk change. Requests
    /// the replica was working on are abandoned. Crashing a node that is already down crashes its disk again. The node
    /// stays down until [`Node::restart`].
    ///
    /// # Arguments
    ///
    /// * `mode` - What survives of the bytes that were written but not flushed. See [`MemoryDisk::crash_with`].
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_segment::CrashMode;
    /// use bitacora_testkit::Cluster;
    ///
    /// let cluster = Cluster::start(1).await;
    /// let node = &cluster.nodes()[0];
    ///
    /// node.crash_with(CrashMode::KeepFirstBytes(100));
    ///
    /// assert!(!node.is_running());
    /// # }
    /// ```
    pub fn crash_with(&self, mode: CrashMode) {
        self.link.stop_process();
        self.disk().crash_with(mode);
    }

    /// Kills the replica process and replaces the node's disk with an empty one, as when a machine is rebuilt.
    ///
    /// Everything the node stored is gone. The node gets a new identity at once, and runs under it when it is
    /// restarted, so that it can never claim copies it no longer holds. The old identity stays in the registry until it
    /// ages out or is removed. The node keeps its address, its link and its failure domains.
    ///
    /// # Panics
    ///
    /// Panics if the new identity is not a valid identifier, which the names built here always are.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use bitacora_testkit::Cluster;
    /// use bitacora_types::replica_api::ReplicaApi;
    ///
    /// let cluster = Cluster::start(1).await;
    /// let node = &cluster.nodes()[0];
    ///
    /// node.lose_disk();
    /// node.restart().await;
    ///
    /// let status = node.link().status().await?;
    /// assert_eq!(status.replica_id.to_string(), "replica-0-disk-1");
    /// # Ok(())
    /// # }
    /// ```
    pub fn lose_disk(&self) {
        self.link.stop_process();
        let mut storage = self.lock_storage();
        storage.lost_disk_count += 1;
        storage.replica_id = identity_on_disk(self.node_number, storage.lost_disk_count);
        storage.disk = MemoryDisk::new();
    }

    /// Starts the replica process again on the node's current disk. Does nothing if it is running.
    ///
    /// The disk is first told to accept flushes again: a restarted machine does not inherit the fault that killed its
    /// process. Then a replica is opened on what the disk holds, which runs the replica's recovery, and is put behind
    /// the link. The node is not registered again; see
    /// [`Cluster::renew_registrations`](crate::Cluster::renew_registrations).
    ///
    /// # Panics
    ///
    /// Panics if the replica cannot be opened on what the disk holds.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_testkit::Cluster;
    /// use bitacora_types::replica_api::ReplicaApi;
    ///
    /// let cluster = Cluster::start(1).await;
    /// let node = &cluster.nodes()[0];
    /// node.crash();
    ///
    /// node.restart().await;
    ///
    /// assert!(node.link().status().await.is_ok());
    /// # }
    /// ```
    pub async fn restart(&self) {
        if self.is_running() {
            return;
        }
        let (disk, replica_id) = (self.disk(), self.replica_id());
        disk.fail_flushes(false);
        let replica = open_replica(&disk, &replica_id, &self.environment).await;
        self.link.start_process(replica);
    }

    /// Reports where this node's copy of a segment ends.
    ///
    /// Asks the replica process directly, bypassing the link, so network faults on the node do not affect the answer.
    /// It lists the replica's segments and picks the one whose address matches.
    ///
    /// # Arguments
    ///
    /// * `segment` - Address of the segment to look up.
    ///
    /// # Returns
    ///
    /// `Some(sequence)` with the last sequence number stored locally. `None` when the node is down, when the replica
    /// cannot list its segments, or when it holds no records of `segment`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_testkit::Cluster;
    /// use bitacora_types::{SegmentAddress, SegmentId, StreamId};
    ///
    /// let cluster = Cluster::start(1).await;
    /// let segment = SegmentAddress::new(StreamId::new(1), SegmentId::new(1));
    ///
    /// match cluster.nodes()[0].local_end(segment).await {
    ///     Some(last_sequence) => println!("the copy ends at {last_sequence}"),
    ///     None => println!("the node is down or has no copy"),
    /// }
    /// # }
    /// ```
    pub async fn local_end(&self, segment: SegmentAddress) -> Option<SequenceNumber> {
        let replica = self.replica()?;
        let statuses = replica.list_segments().await.ok()?;
        for status in statuses {
            if status.address == segment {
                return status.local_end;
            }
        }
        None
    }

    /// Lists the sequence numbers this node would serve to a reader of a segment.
    ///
    /// Reads the whole segment from the replica process directly, bypassing the link, with the limit an ordinary reader
    /// gets: nothing above the durable sequence the replica last heard from the writer.
    ///
    /// # Arguments
    ///
    /// * `segment` - Address of the segment to read.
    ///
    /// # Returns
    ///
    /// The sequence number of every record served, in order. Empty if the node is down, has no copy, or refuses the
    /// read.
    ///
    /// # Panics
    ///
    /// Panics if the replica serves frames that cannot be decoded, which would be a bug in the replica.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_testkit::Cluster;
    /// use bitacora_types::{SegmentAddress, SegmentId, StreamId};
    ///
    /// let cluster = Cluster::start(1).await;
    /// let segment_nobody_wrote = SegmentAddress::new(StreamId::new(1), SegmentId::new(1));
    ///
    /// let readable = cluster.nodes()[0].readable_sequences(segment_nobody_wrote).await;
    ///
    /// assert!(readable.is_empty());
    /// # }
    /// ```
    pub async fn readable_sequences(&self, segment: SegmentAddress) -> Vec<u64> {
        let Some(replica) = self.replica() else {
            return Vec::new();
        };
        let whole_segment = ReadRequest {
            segment,
            from_sequence: SequenceNumber::FIRST,
            max_bytes: usize::MAX,
            wait_for_records: None,
            limit: ReadLimit::DurableSequence,
        };
        let Ok(response) = replica.read(whole_segment).await else {
            return Vec::new();
        };
        let batches = decode_frames(segment, &response.batch_frames).expect("a replica serves valid frames");
        let mut sequences = Vec::new();
        for batch in &batches {
            for record in batch.records() {
                sequences.push(record.sequence.get());
            }
        }
        sequences
    }

    /// Builds the record the node registers with, stamped with the current time.
    ///
    /// Measures the node's disk, as a replica does before each heartbeat. It writes nothing; the caller stores the
    /// record.
    ///
    /// # Arguments
    ///
    /// * `clock` - The wall clock the heartbeat time is read from.
    ///
    /// # Returns
    ///
    /// The registry record of the node's current identity: its address, failure domains, disk space, and a heartbeat at
    /// the present moment. The node is never registered as draining.
    ///
    /// # Panics
    ///
    /// Panics if the disk cannot report its space, which an in-memory disk always can.
    pub(crate) async fn registration(&self, clock: &dyn Clock) -> ReplicaRecord {
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

/// Names the identity of node `node_number` on the disk it has after losing `lost_disk_count` disks: `replica-N` on the
/// first disk, `replica-N-disk-M` on the M-th replacement.
fn identity_on_disk(node_number: usize, lost_disk_count: u32) -> ReplicaId {
    let name = if lost_disk_count == 0 {
        format!("replica-{node_number}")
    } else {
        format!("replica-{node_number}-disk-{lost_disk_count}")
    };
    ReplicaId::new(name).expect("a name of letters, digits and dashes is a valid identifier")
}

/// Opens a replica on `disk` under `replica_id`, able to reach its peers and the object store of the cluster.
///
/// # Panics
///
/// Panics if the replica cannot be opened. An in-memory disk does not fail, and what a crash leaves on it is what the
/// replica's recovery is written for.
async fn open_replica(disk: &MemoryDisk, replica_id: &ReplicaId, environment: &NodeEnvironment) -> Arc<Replica> {
    let replica = Replica::open(
        Arc::new(disk.clone()),
        replica_id.clone(),
        environment.settings,
        Arc::new(environment.connector.clone()),
    )
    .await
    .expect("a replica opens on a memory disk")
    .with_artifact_store(Arc::new(environment.artifact_store.clone()));
    Arc::new(replica)
}

#[cfg(test)]
#[path = "node_test.rs"]
mod tests;
