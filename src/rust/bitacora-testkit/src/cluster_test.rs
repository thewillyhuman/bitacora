//! Tests of the cluster as a whole: what it registers and what it hands out.
//!
//! # What is covered
//!
//! - Started nodes are registered under their addresses, each as a host of its own, or in the zones the layout says.
//! - Renewing registrations renews running nodes only: a node that is down ages out, and a node that lost its disk
//!   registers next to its old identity.
//! - Renewing fails while the metadata store is unavailable.
//! - The identities and the clock the cluster hands out.
//!
//! # How the fixtures work
//!
//! The tests read the registry through `registered_replicas`, in the metadata store's order, which is the order of the
//! identities. Time is paused, so a heartbeat is exactly as old as the test made it.

use std::time::Duration;

use bitacora_artifact::ArtifactStore as _;
use bitacora_types::replica_api::ReplicaApi as _;

use super::*;
use crate::test_support::{append_request, cluster_with_three_records, registered_replicas};

/// How long a registered replica counts as alive after its last heartbeat, in these tests.
const LIVENESS_WINDOW: Duration = Duration::from_secs(15);

/// A time long enough for a node that does not renew its registration to age out.
const LONGER_THAN_THE_LIVENESS_WINDOW: Duration = Duration::from_secs(60);

/// A segment file size well above the one `Cluster::start` uses.
const ROOMY_SEGMENT_FILE_BYTES: u64 = 64 * 1024;

/// Appends one record to the only node of `cluster`, renews its registration, and reports the free space it registered.
async fn free_bytes_after_one_append(cluster: &Cluster) -> u64 {
    cluster.nodes()[0]
        .link()
        .append(append_request(1, 1))
        .await
        .expect("a healthy node accepts the first batch of a segment");
    cluster
        .renew_registrations()
        .await
        .expect("the metadata store is available");
    registered_replicas(cluster).await[0].free_bytes
}

#[tokio::test(start_paused = true)]
async fn started_nodes_are_registered_under_their_addresses() {
    let cluster = Cluster::start(2).await;

    let mut addresses = Vec::new();
    for replica in registered_replicas(&cluster).await {
        addresses.push(replica.address);
    }

    assert_eq!(addresses, ["replica-0:7000", "replica-1:7000"]);
}

#[tokio::test(start_paused = true)]
async fn started_nodes_are_each_a_host_of_their_own() {
    let cluster = Cluster::start(2).await;

    let mut failure_domains = Vec::new();
    for replica in registered_replicas(&cluster).await {
        failure_domains.push(replica.failure_domains);
    }

    assert_eq!(failure_domains, [["host-0"], ["host-1"]]);
}

#[tokio::test(start_paused = true)]
async fn nodes_of_a_zoned_cluster_are_registered_in_their_zones() {
    let two_zones = FailureDomainLayout::HostsInZones { zone_count: 2 };

    let cluster = Cluster::start_with(3, ReplicaSettings::default(), two_zones).await;

    let mut failure_domains = Vec::new();
    for replica in registered_replicas(&cluster).await {
        failure_domains.push(replica.failure_domains);
    }
    assert_eq!(
        failure_domains,
        [["zone-0", "host-0"], ["zone-1", "host-1"], ["zone-0", "host-2"]]
    );
}

/// A replica reserves a whole segment file when it first writes to a segment, so the size the cluster was started with
/// shows in the free space its node registers.
#[tokio::test(start_paused = true)]
async fn replicas_are_opened_with_the_settings_the_cluster_was_started_with() {
    let roomy_segment_files = ReplicaSettings {
        preallocated_segment_bytes: ROOMY_SEGMENT_FILE_BYTES,
        ..ReplicaSettings::default()
    };
    let small = Cluster::start(1).await;
    let roomy = Cluster::start_with(1, roomy_segment_files, FailureDomainLayout::HostPerNode).await;

    let free_bytes_of_small = free_bytes_after_one_append(&small).await;
    let free_bytes_of_roomy = free_bytes_after_one_append(&roomy).await;

    assert!(
        free_bytes_of_roomy < free_bytes_of_small,
        "roomy: {free_bytes_of_roomy}, small: {free_bytes_of_small}"
    );
}

#[tokio::test(start_paused = true)]
async fn node_that_lost_its_disk_registers_next_to_its_old_identity() {
    let cluster = cluster_with_three_records().await;
    cluster.nodes()[0].lose_disk();
    cluster.nodes()[0].restart().await;

    cluster
        .renew_registrations()
        .await
        .expect("the metadata store is available");

    let mut identities = Vec::new();
    for replica in registered_replicas(&cluster).await {
        identities.push(replica.replica_id.to_string());
    }
    assert_eq!(identities, ["replica-0", "replica-0-disk-1"]);
}

#[tokio::test(start_paused = true)]
async fn node_that_is_down_ages_out_of_the_registry() {
    let cluster = Cluster::start(2).await;
    cluster.nodes()[1].crash();
    tokio::time::advance(LONGER_THAN_THE_LIVENESS_WINDOW).await;

    cluster
        .renew_registrations()
        .await
        .expect("the metadata store is available");

    let now = cluster.clock().now();
    let mut is_alive_by_node = Vec::new();
    for replica in registered_replicas(&cluster).await {
        is_alive_by_node.push(replica.is_alive(now, LIVENESS_WINDOW));
    }
    assert_eq!(is_alive_by_node, [true, false]);
}

#[tokio::test(start_paused = true)]
async fn renewing_registrations_fails_while_the_metadata_store_is_unavailable() {
    let cluster = Cluster::start(1).await;
    cluster.key_value_store().set_unavailable(true);

    let renewed = cluster.renew_registrations().await;

    assert!(renewed.is_err(), "{renewed:?}");
}

#[tokio::test(start_paused = true)]
async fn replica_ids_are_listed_in_the_order_asked_for() {
    let cluster = Cluster::start(3).await;

    let mut identities = Vec::new();
    for replica_id in cluster.replica_ids(&[2, 0]) {
        identities.push(replica_id.to_string());
    }

    assert_eq!(identities, ["replica-2", "replica-0"]);
}

#[tokio::test(start_paused = true)]
async fn replica_ids_follow_a_node_to_its_new_identity() {
    let cluster = Cluster::start(1).await;

    cluster.nodes()[0].lose_disk();

    assert_eq!(cluster.replica_ids(&[0]), [cluster.nodes()[0].replica_id()]);
}

#[tokio::test(start_paused = true)]
async fn cluster_clock_starts_at_its_documented_moment() {
    let cluster = Cluster::start(1).await;

    assert_eq!(cluster.clock().now(), VirtualClock::START);
}

#[tokio::test(start_paused = true)]
async fn object_store_of_a_new_cluster_is_empty() {
    let cluster = Cluster::start(1).await;

    let stored_objects = cluster
        .artifact_store()
        .list_prefix("")
        .await
        .expect("the object store is up");

    assert_eq!(stored_objects, Vec::new());
}
