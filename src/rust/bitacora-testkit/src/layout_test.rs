//! Tests of the labels a layout gives the nodes of a cluster.
//!
//! # What is covered
//!
//! - A layout of one host per node names the host and nothing else.
//! - A zoned layout deals the nodes out to the zones in turn, and still names each node's own host.
//! - A zoned layout without zones is refused.
//!
//! # How the fixtures work
//!
//! The labels are computed from a node number alone, so the tests need no cluster and no runtime.

use super::*;

/// The zoned layout the tests use: three zones.
const THREE_ZONES: FailureDomainLayout = FailureDomainLayout::HostsInZones { zone_count: 3 };

#[test]
fn layout_of_one_host_per_node_names_only_the_host() {
    let failure_domains = FailureDomainLayout::HostPerNode.failure_domains_of(2);

    assert_eq!(failure_domains, ["host-2"]);
}

#[test]
fn zoned_layout_names_the_zone_before_the_host() {
    let failure_domains = THREE_ZONES.failure_domains_of(1);

    assert_eq!(failure_domains, ["zone-1", "host-1"]);
}

#[test]
fn zoned_layout_deals_nodes_out_to_the_zones_in_turn() {
    let mut zones = Vec::new();
    for node_number in 0..6 {
        zones.push(THREE_ZONES.failure_domains_of(node_number)[0].clone());
    }

    assert_eq!(zones, ["zone-0", "zone-1", "zone-2", "zone-0", "zone-1", "zone-2"]);
}

#[test]
fn nodes_that_share_a_zone_are_still_hosts_of_their_own() {
    let first_in_zone_1 = THREE_ZONES.failure_domains_of(1);
    let second_in_zone_1 = THREE_ZONES.failure_domains_of(4);

    assert_eq!(first_in_zone_1[0], second_in_zone_1[0]);
    assert_ne!(first_in_zone_1[1], second_in_zone_1[1]);
}

#[test]
#[should_panic(expected = "at least one zone")]
fn zoned_layout_without_zones_is_refused() {
    let no_zones = FailureDomainLayout::HostsInZones { zone_count: 0 };

    let _ = no_zones.failure_domains_of(0);
}
