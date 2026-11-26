//! Fixtures shared by the tests of the commands.
//!
//! # Why this file exists
//!
//! Every command is a function over the cluster's interfaces, so its tests need a cluster. The test kit
//! provides one in process: real replicas on in-memory disks, an in-memory metadata store and object
//! store, and a virtual clock. This file wraps it in the structure the commands take, and adds the few
//! steps most tests start with.
//!
//! # What it contains
//!
//! - [`TestCluster`]: an in-process cluster together with the tool's handle to it.
//! - [`STREAM`]: the name of the stream the fixtures create.
//!
//! # How it works
//!
//! [`TestCluster::start`] builds the tool's [`Cluster`] from the test kit's metadata store, connector,
//! clock and object store, exactly the four things a real run gets from `Cluster::connect`. The
//! controller is not running in the background: a test that needs its work calls
//! [`TestCluster::run_controller_pass`], so that every test decides when that work happens.
//!
//! # Invariants
//!
//! - Tests using these fixtures run under Tokio's paused clock: no test waits in real time.
//!
//! # See also
//!
//! - `bitacora-testkit`: the in-process cluster.

use std::sync::Arc;
use std::time::Duration;

use bitacora::TakeoverPolicy;
use bitacora_controller::{Controller, ControllerSettings, PassReport};
use bytes::Bytes;

use bitacora_meta::{KeyValueStore, Mutation};

use super::stream;
use crate::cluster::Cluster;

/// The stream the fixtures create.
pub(crate) const STREAM: &str = "orders";

/// An in-process cluster and the tool's handle to it.
pub(crate) struct TestCluster {
    /// The cluster itself: nodes to crash, links to cut, stores to inspect.
    pub(crate) nodes: bitacora_testkit::Cluster,
    /// The interfaces the commands under test are given.
    pub(crate) cluster: Cluster,
}

impl TestCluster {
    /// Starts `node_count` replicas, registered and alive, with no stream.
    pub(crate) async fn start(node_count: usize) -> Self {
        let nodes = bitacora_testkit::Cluster::start(node_count).await;
        let cluster = Cluster {
            metadata: nodes.metadata().clone(),
            connector: nodes.connector(),
            clock: nodes.clock(),
            artifact_store: Some(Arc::new(nodes.artifact_store().clone())),
            request_timeout: Duration::from_secs(5),
        };
        Self { nodes, cluster }
    }

    /// Starts `node_count` replicas and creates [`STREAM`] with the default replication.
    pub(crate) async fn with_stream(node_count: usize) -> Self {
        let started = Self::start(node_count).await;
        stream::create(&started.cluster, STREAM, 3, 2, None)
            .await
            .expect("the stream is created");
        started
    }

    /// A controller over the cluster, tiering to its object store, with the given settings.
    pub(crate) fn controller(&self, settings: ControllerSettings) -> Controller {
        Controller::new(
            self.nodes.metadata().clone(),
            self.nodes.connector(),
            self.nodes.clock(),
            settings,
        )
        .with_artifact_store(Arc::new(self.nodes.artifact_store().clone()))
    }

    /// Runs one pass of a controller with the default settings: places standby segments, verifies and
    /// repairs copies.
    pub(crate) async fn run_controller_pass(&self) -> PassReport {
        self.controller(ControllerSettings::default())
            .run_pass()
            .await
            .expect("the pass runs")
    }

    /// Appends `count` records to [`STREAM`] through the tool, as `record-<n>` numbered from `first`,
    /// after a controller pass has made sure the writer has a segment to roll into.
    pub(crate) async fn append_numbered(&self, first: u64, count: u64) {
        self.run_controller_pass().await;
        let mut payloads = Vec::new();
        for number in first..first + count {
            payloads.push(Bytes::from(format!("record-{number}")));
        }
        stream::append(&self.cluster, STREAM, payloads, TakeoverPolicy::Force)
            .await
            .expect("the records commit");
    }

    /// Lets `duration` pass: timers fire, and the replicas renew their registrations as their own
    /// timers would.
    pub(crate) async fn let_pass(&self, duration: Duration) {
        tokio::time::advance(duration).await;
        self.nodes
            .renew_registrations()
            .await
            .expect("registrations are renewed");
    }

    /// Deletes every key of the metadata store, as the loss of every etcd member would. The replicas and
    /// the object store keep what they hold.
    pub(crate) async fn lose_the_metadata_store(&self) {
        let key_value_store = self.nodes.key_value_store();
        let stored_entries = key_value_store.list("").await.expect("the store is listed");
        let mut deletions = Vec::with_capacity(stored_entries.len());
        for (key, _stored) in stored_entries {
            deletions.push(Mutation::Delete { key });
        }
        key_value_store
            .transact(Vec::new(), deletions)
            .await
            .expect("the store is emptied");
    }

    /// Deletes every key of the metadata store and has every replica register again, as replicas that are running
    /// do within seconds of a real loss. Returns how many replicas registered.
    pub(crate) async fn lose_the_metadata_store_under_running_replicas(&self) -> usize {
        let registrations = self
            .cluster
            .metadata
            .list_replicas()
            .await
            .expect("the replicas are listed");
        self.lose_the_metadata_store().await;
        for registration in &registrations {
            self.cluster
                .metadata
                .put_replica(registration)
                .await
                .expect("the replica registers again");
        }
        registrations.len()
    }

    /// How many keys the metadata store holds.
    pub(crate) async fn metadata_key_count(&self) -> usize {
        self.nodes
            .key_value_store()
            .list("")
            .await
            .expect("the store is listed")
            .len()
    }
}
