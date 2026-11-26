//! Where the nodes of a cluster are: their failure domains.
//!
//! # Why this file exists
//!
//! Placement spreads the copies of a segment over failure domains, so that losing one host or one zone loses one copy
//! at most. To test that, a cluster must say which host and which zone each of its nodes is in. The caller used to pass
//! a function from a node's number to its labels; every caller wrote one of the same two functions, and a reader of a
//! test had to find and read the function to learn the shape of the cluster. This file names the two shapes instead.
//!
//! # What it contains
//!
//! - [`FailureDomainLayout`]: the shapes a cluster can have, and the labels each gives a node.
//!
//! # How it works
//!
//! A node's failure domains are a list of labels, widest first, as a replica registers them
//! (`ReplicaRecord::failure_domains`). The host label is always `host-N` for node `N`: it is what tells placement that
//! the old and the new identity of a node that lost its disk are one machine. A zoned layout puts a `zone-Z` label in
//! front of it, dealing the nodes out to the zones in turn.
//!
//! # Invariants
//!
//! - The labels of a node depend on its number and the layout only, so a cluster started twice is laid out the same
//!   way.
//! - Every node is on a host of its own, whatever the layout.
//!
//! # See also
//!
//! - `cluster.rs`: `Cluster::start_with` takes the layout.
//! - `bitacora-meta`: placement, which reads the labels.

/// How the nodes of a cluster are spread over failure domains.
///
/// Pass one to [`Cluster::start_with`](crate::Cluster::start_with). [`Cluster::start`](crate::Cluster::start) uses
/// [`FailureDomainLayout::HostPerNode`].
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use bitacora_replica::ReplicaSettings;
/// use bitacora_testkit::{Cluster, FailureDomainLayout};
///
/// let layout = FailureDomainLayout::HostsInZones { zone_count: 3 };
/// let cluster = Cluster::start_with(6, ReplicaSettings::default(), layout).await;
///
/// let registered = cluster.metadata().list_replicas().await?;
/// assert_eq!(registered[4].failure_domains, ["zone-1", "host-4"]);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureDomainLayout {
    /// Every node is a host of its own and there is nothing wider: node `N` is in `["host-N"]`.
    HostPerNode,
    /// The nodes are dealt out to the zones in turn, and every node is a host of its own. Node `N` registers the
    /// labels `zone-Z` and `host-N`, where `Z` is `N` modulo `zone_count`: with six nodes and three zones, nodes 1 and
    /// 4 share zone 1.
    HostsInZones {
        /// How many zones the nodes are dealt out to. Must not be zero.
        zone_count: usize,
    },
}

impl FailureDomainLayout {
    /// Names the failure domains of one node, widest first.
    ///
    /// Formats the labels; nothing is remembered.
    ///
    /// # Arguments
    ///
    /// * `node_number` - The node's position in the cluster, counted from zero.
    ///
    /// # Returns
    ///
    /// The labels the node registers with, the widest domain first and its host last.
    ///
    /// # Panics
    ///
    /// Panics if the layout is [`FailureDomainLayout::HostsInZones`] with no zone.
    pub(crate) fn failure_domains_of(self, node_number: usize) -> Vec<String> {
        let host = format!("host-{node_number}");
        match self {
            Self::HostPerNode => vec![host],
            Self::HostsInZones { zone_count } => {
                assert!(zone_count > 0, "a zoned cluster has at least one zone");
                let zone_number = node_number % zone_count;
                vec![format!("zone-{zone_number}"), host]
            }
        }
    }
}

#[cfg(test)]
#[path = "layout_test.rs"]
mod tests;
