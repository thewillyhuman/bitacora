//! Tests of how the cluster's replicas are reached by address.
//!
//! # What is covered
//!
//! - The connector reaches a node through its link, faults included.
//! - Nothing is reachable at an address no node has.
//! - A node that was rebuilt on an empty disk answers for its new identity and never for its old one.
//! - Every operation of the replica interface is refused to a caller that expects another replica.
//!
//! # How the fixtures work
//!
//! Each test starts from `cluster_with_three_records` and connects through `Cluster::connector`, as a client or the
//! controller would, with an identity and an address. "Rebuilt" means the node lost its disk and was restarted, which
//! gives it a new identity at the same address.

use bitacora_types::SequenceNumber;

use super::*;
use crate::Cluster;
use crate::test_support::{
    EVERY_OPERATION, SEGMENT, append_request, cluster_with_three_records, is_unavailable,
    operations_failed_as_unavailable,
};

/// The two identities of a node that was rebuilt, and the cluster it is in.
struct RebuiltNode {
    /// The cluster whose only node was rebuilt.
    cluster: Cluster,
    /// The identity the node had on the disk it lost.
    old_replica_id: ReplicaId,
    /// The identity the node runs under now.
    new_replica_id: ReplicaId,
}

impl RebuiltNode {
    /// Connects to the node's address as a caller that expects `replica_id` there.
    fn connect_as(&self, replica_id: &ReplicaId) -> Arc<dyn ReplicaApi> {
        self.cluster
            .connector()
            .connect(replica_id, self.cluster.nodes()[0].address())
    }
}

/// Starts a one-node cluster with three records, then loses the node's disk and restarts it.
async fn rebuild_the_only_node() -> RebuiltNode {
    let cluster = cluster_with_three_records().await;
    let old_replica_id = cluster.nodes()[0].replica_id();
    cluster.nodes()[0].lose_disk();
    cluster.nodes()[0].restart().await;
    let new_replica_id = cluster.nodes()[0].replica_id();
    RebuiltNode {
        cluster,
        old_replica_id,
        new_replica_id,
    }
}

#[tokio::test(start_paused = true)]
async fn connector_reaches_a_node_through_its_link() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];

    let status = cluster
        .connector()
        .connect(&node.replica_id(), node.address())
        .status()
        .await
        .expect("a healthy node answers");

    assert_eq!(status.replica_id, node.replica_id());
}

#[tokio::test(start_paused = true)]
async fn connection_meets_the_faults_of_the_link() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];
    let connection = cluster.connector().connect(&node.replica_id(), node.address());

    node.set_reachable(false);
    let answer = connection.status().await;

    assert!(is_unavailable(&answer), "{answer:?}");
}

#[tokio::test(start_paused = true)]
async fn nothing_is_reachable_at_an_address_no_node_has() {
    let cluster = cluster_with_three_records().await;
    let replica_id = cluster.nodes()[0].replica_id();

    let answer = cluster
        .connector()
        .connect(&replica_id, "replica-9:7000")
        .status()
        .await;

    assert!(is_unavailable(&answer), "{answer:?}");
}

/// A rebuilt node must not answer in the name of the node it replaced: it holds none of the copies the old identity
/// held.
#[tokio::test(start_paused = true)]
async fn rebuilt_node_does_not_answer_for_its_old_identity() {
    let rebuilt = rebuild_the_only_node().await;

    let as_old_identity = rebuilt.connect_as(&rebuilt.old_replica_id).list_segments().await;

    assert!(is_unavailable(&as_old_identity), "{as_old_identity:?}");
}

#[tokio::test(start_paused = true)]
async fn rebuilt_node_answers_for_its_new_identity() {
    let rebuilt = rebuild_the_only_node().await;

    let as_new_identity = rebuilt.connect_as(&rebuilt.new_replica_id).list_segments().await;

    assert_eq!(as_new_identity, Ok(Vec::new()));
}

/// A connection made before the node was rebuilt is as stale as an identity looked up before.
#[tokio::test(start_paused = true)]
async fn connection_made_before_a_rebuild_is_refused_after_it() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];
    let connection = cluster.connector().connect(&node.replica_id(), node.address());

    node.lose_disk();
    node.restart().await;
    let answer = connection.status().await;

    assert!(is_unavailable(&answer), "{answer:?}");
}

#[tokio::test(start_paused = true)]
async fn every_operation_is_refused_to_a_caller_that_expects_another_replica() {
    let rebuilt = rebuild_the_only_node().await;
    let connection = rebuilt.connect_as(&rebuilt.old_replica_id);

    let refused_operations = operations_failed_as_unavailable(connection.as_ref()).await;

    assert_eq!(refused_operations, EVERY_OPERATION);
}

#[tokio::test(start_paused = true)]
async fn request_reaches_the_replica_the_caller_expects() {
    let cluster = cluster_with_three_records().await;
    let node = &cluster.nodes()[0];
    let connection = cluster.connector().connect(&node.replica_id(), node.address());

    let answer = connection.append(append_request(4, 1)).await;

    assert!(answer.is_ok(), "{answer:?}");
    assert_eq!(node.local_end(SEGMENT).await, Some(SequenceNumber::new(4)));
}
