//! What the tasks of one simulated run share: the cluster, the history, and how clients are built.
//!
//! # Why this file exists
//!
//! A run is a dozen tasks: compute nodes, readers, the controller, the nemesis, the audit. They all act on one cluster
//! and write to one history, and each of them builds clients, which must all be wired to the same metadata store,
//! connector, clock and object store, with the same shortened timeouts. Without one place for that, every task would
//! carry four handles and its own copy of the settings, and two tasks could end up simulating different clusters.
//!
//! # What it contains
//!
//! - [`World`]: the cluster, the history, the node whose disk is allowed to fail, and what the audit is doing.
//! - [`AuditStep`]: the steps of the audit, so that a run that does not settle can say where it stopped.
//! - [`STREAM`]: the one stream every run writes to.
//!
//! # How it works
//!
//! [`World::start`] starts the cluster, begins the history, draws the unlucky node, creates the stream and registers
//! the key-value store's materializer on it. From then on the world is shared behind an `Arc` and never replaced. Tasks
//! reach the cluster and the history through its fields, and build clients with [`World::client`] and stores with
//! [`World::store_dependencies`].
//!
//! The clients' timeouts are far shorter than in production, so that request timeouts, segment rolls and lease expiries
//! all happen many times in a minute of simulated time.
//!
//! # Invariants
//!
//! - [`World::start`] awaits and draws in a fixed order: the cluster starts, the history begins, the unlucky node is
//!   drawn, the stream is created. That order is part of what a seed means.
//! - Silent corruption and disk loss happen on the unlucky node only. That keeps them within what replication is meant
//!   to survive: at most one copy of any record is destroyed.
//! - The numbers in this file shape every run. Changing one changes what every seed does.
//!
//! # See also
//!
//! - `simulation.rs`: creates the world and the tasks that share it.
//! - `audit.rs`: reports its progress through [`World::now_waiting_for`].
//! - `bitacora-testkit`: the cluster.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bitacora_artifact::ArtifactStore;
use bitacora_client::{BitacoraClient, ClientSettings, ReaderSettings, WriterSettings};
use bitacora_kv::{KvDependencies, KvStore};
use bitacora_meta::RetentionPolicy;
use bitacora_testkit::Cluster;
use bitacora_types::{DeterministicRandom, ReplicationSettings};

use crate::history::History;
use crate::kv_workload::declare_fillers;

/// The stream every simulation writes to.
pub(crate) const STREAM: &str = "simulated";

/// How long a simulated client waits for one request to a replica.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// How long a simulated writer goes without an answer from a replica before it replaces the replica.
const REPLICA_FAILURE_TIMEOUT: Duration = Duration::from_secs(4);

/// How long a simulated writer's lease lasts. Short, so that leases expire during a run.
const LEASE_DURATION: Duration = Duration::from_secs(6);

/// How often a simulated writer renews its lease.
const LEASE_RENEWAL_INTERVAL: Duration = Duration::from_secs(2);

/// The size at which a simulated writer rolls to a new segment. Small, so that a run seals hundreds of segments.
const TARGET_SEGMENT_BYTES: u64 = 8 * 1024;

/// The age at which a simulated writer rolls to a new segment whatever its size.
const TARGET_SEGMENT_AGE: Duration = Duration::from_secs(20);

/// How long a simulated writer may take to open before it gives up.
const WRITER_OPEN_TIMEOUT: Duration = Duration::from_secs(15);

/// How long a simulated reader waits at the tail of the stream before it asks again.
const READER_TAIL_WAIT: Duration = Duration::from_millis(500);

/// What the audit of a healed cluster is doing. A run that does not settle in time reports the step it was at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AuditStep {
    /// The audit has not begun.
    Starting,
    /// A new key-value store is being opened and made to commit one last record.
    CommittingFinalRecord,
    /// Every key is being read through that store.
    ReadingEveryKey,
    /// The whole stream is being read back.
    ReadingWholeStream,
    /// The controller is being given time to bring every sealed segment back to full health.
    WaitingForFullReplication,
}

impl AuditStep {
    /// Says what the audit is doing at this step, as it appears in the report of a stuck run.
    ///
    /// # Returns
    ///
    /// A phrase that completes the sentence "the healed cluster did not settle; it was ...".
    pub(crate) const fn description(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::CommittingFinalRecord => "opening a store and committing a final record",
            Self::ReadingEveryKey => "reading every key through the store",
            Self::ReadingWholeStream => "reading the whole stream back",
            Self::WaitingForFullReplication => "waiting for every sealed segment to have all its copies good",
        }
    }
}

/// What the tasks of a run share.
pub(crate) struct World {
    /// The simulated cluster: storage nodes, metadata store, object store, clock.
    pub(crate) cluster: Cluster,
    /// Where every client of the run records what it did and saw.
    pub(crate) history: History,
    /// What the audit is waiting for, to say so if it never happens. A lock, because the audit writes it and the report
    /// of a stuck run reads it, through a shared reference.
    audit_step: Mutex<AuditStep>,
    /// The one node whose disk may corrupt data or be lost. Keeping these faults on one node keeps them within what
    /// replication is meant to survive: at most one copy of any record is destroyed.
    pub(crate) unlucky_node: usize,
}

impl World {
    /// Starts the cluster of a run and prepares the stream the run writes to.
    ///
    /// Starts `node_count` storage nodes, begins the history, draws the unlucky node, creates [`STREAM`] with the
    /// default replication and retention, registers the key-value store's materializer on it, and declares the store's
    /// fillers to the history. The order of these steps is part of what a seed means.
    ///
    /// # Arguments
    ///
    /// * `node_count` - How many storage nodes the cluster has.
    /// * `random` - The random source of the run. One number is drawn from it, for the unlucky node.
    ///
    /// # Returns
    ///
    /// The world, with an empty stream and no client yet.
    ///
    /// # Panics
    ///
    /// Panics if the stream or the materializer cannot be created, which a fresh in-memory metadata store does not
    /// cause.
    pub(crate) async fn start(node_count: usize, random: &mut DeterministicRandom) -> Self {
        let world = Self {
            cluster: Cluster::start(node_count).await,
            history: History::new(),
            audit_step: Mutex::new(AuditStep::Starting),
            unlucky_node: random.index(node_count),
        };
        world
            .cluster
            .metadata()
            .create_stream(STREAM, ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
            .await
            .expect("a fresh metadata store accepts a stream");
        KvStore::initialize(world.cluster.metadata(), STREAM)
            .await
            .expect("a fresh stream accepts a materializer");
        declare_fillers(&world.history);
        world
    }

    /// Hands out the cluster's object store, as the clients, the controller and the key-value stores of the run are
    /// given it.
    ///
    /// Sealed segments are tiered to it, and the stores keep their layers and manifests in it.
    ///
    /// # Returns
    ///
    /// A handle that shares the content of the cluster's object store.
    pub(crate) fn artifact_store(&self) -> Arc<dyn ArtifactStore> {
        Arc::new(self.cluster.artifact_store().clone())
    }

    /// Builds a client of the cluster for `identity`.
    ///
    /// The client reaches replicas through the cluster's links, so it meets every fault the nemesis injects. Its
    /// timeouts are shorter than in production; see the file header.
    ///
    /// # Arguments
    ///
    /// * `identity` - The name the client's writers take leases under. Clients with one identity count as one writer.
    ///
    /// # Returns
    ///
    /// A client that can open writers and readers on the cluster and read tiered segments.
    pub(crate) fn client(&self, identity: &str) -> BitacoraClient {
        let settings = ClientSettings {
            writer: WriterSettings {
                writer_identity: identity.to_owned(),
                request_timeout: REQUEST_TIMEOUT,
                replica_failure_timeout: REPLICA_FAILURE_TIMEOUT,
                lease_duration: LEASE_DURATION,
                lease_renewal_interval: LEASE_RENEWAL_INTERVAL,
                target_segment_bytes: TARGET_SEGMENT_BYTES,
                target_segment_age: TARGET_SEGMENT_AGE,
                open_timeout: WRITER_OPEN_TIMEOUT,
                ..WriterSettings::default()
            },
            reader: ReaderSettings {
                tail_wait: READER_TAIL_WAIT,
                request_timeout: REQUEST_TIMEOUT,
                ..ReaderSettings::default()
            },
        };
        BitacoraClient::new(
            self.cluster.metadata().clone(),
            self.cluster.connector(),
            self.cluster.clock(),
            settings,
        )
        .with_artifact_store(self.artifact_store())
    }

    /// Gathers what a key-value store opened by `identity` is built from.
    ///
    /// # Arguments
    ///
    /// * `identity` - The name the store's writer takes its lease under.
    ///
    /// # Returns
    ///
    /// A new client for `identity`, the cluster's metadata store and its object store.
    pub(crate) fn store_dependencies(&self, identity: &str) -> KvDependencies {
        KvDependencies {
            client: self.client(identity),
            metadata: self.cluster.metadata().clone(),
            artifact_store: self.artifact_store(),
        }
    }

    /// Notes what the audit is waiting for from now on.
    ///
    /// # Arguments
    ///
    /// * `step` - The step the audit is about to begin.
    pub(crate) fn now_waiting_for(&self, step: AuditStep) {
        *self.lock_audit_step() = step;
    }

    /// Reports the step the audit last began.
    ///
    /// # Returns
    ///
    /// [`AuditStep::Starting`] until the audit notes its first step.
    pub(crate) fn audit_step(&self) -> AuditStep {
        *self.lock_audit_step()
    }

    /// Locks the audit's step. It is one value replaced whole, so a poisoned lock is used as it is.
    fn lock_audit_step(&self) -> MutexGuard<'_, AuditStep> {
        self.audit_step.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
#[path = "world_test.rs"]
mod tests;
