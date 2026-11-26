//! Finding what a fault may hit.
//!
//! # Why this file exists
//!
//! A fault names a pod or a zone, and the nemesis has to pick one that exists and that the cluster is meant to
//! survive losing. Everything it needs to know for that is already written down by the cluster itself: the replicas
//! register their address and their failure domains in the metadata store, the leading controller writes its name
//! there, and the members of the metadata store are listed by whatever manages the cluster. This file turns those
//! three lists into the next fault, as pure functions, so that the rules about what may be hit can be tested without a
//! cluster.
//!
//! # What it contains
//!
//! - [`ClusterView`]: the three lists, as the nemesis read them.
//! - [`pod_of_address`]: the pod behind a registered address.
//! - [`choose_fault`]: one fault of a kind, or nothing if the cluster offers no target the design claims to survive
//!   losing.
//! - [`replicas_cut_by`]: the replicas a cut hides, which is where the tool looks to see whether the cut is real.
//! - `zones_that_may_be_cut`: the zones whose loss leaves a write quorum of every stream and a majority of the
//!   metadata store.
//!
//! # How it works
//!
//! A replica's pod is the first label of the address it registered: the chart registers
//! `<pod>.<service>.<namespace>.svc.<domain>:<port>`, and the in-process test cluster `<name>:<port>`. A replica's
//! zone is its first failure domain when it has more than one; a replica with a single failure domain, its node, has
//! no zone, and a cluster of such replicas is never cut by zone.
//!
//! A zone may be cut only if the replicas are in at least three zones, so that a stream with three copies in three
//! zones keeps two, and only if the zone holds less than half of the members of the metadata store, so that the
//! store keeps its majority. A member of the metadata store is killed only if there are at least three.
//!
//! # Invariants
//!
//! - Pure: nothing here reads the cluster. The same view and the same random source give the same fault.
//! - A fault is only ever chosen among targets the view lists.
//! - No fault is chosen that takes away a write quorum or the majority of the metadata store by itself.
//!
//! # See also
//!
//! - `faults.rs`: what a fault is.
//! - `nemesis.rs`: reads the view and calls [`choose_fault`].

use std::collections::BTreeSet;

use bitacora_meta::ReplicaRecord;
use bitacora_types::{DeterministicRandom, ReplicaId};

use crate::faults::{EtcdMember, Fault, FaultKind};

/// The fewest zones the replicas must be in for one of them to be cut: a stream with a copy in each of three zones
/// keeps a write quorum of two.
const FEWEST_ZONES_FOR_A_ZONE_CUT: usize = 3;

/// The fewest members the metadata store must have for one of them to be killed: three keep a majority of two.
const FEWEST_ETCD_MEMBERS_FOR_A_KILL: usize = 3;

/// What the nemesis knows about the cluster when it chooses a fault.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ClusterView {
    /// The registered replicas.
    pub(crate) replicas: Vec<ReplicaRecord>,
    /// The name the leading controller wrote into the metadata store, which is the name of its pod. `None` while no
    /// controller leads.
    pub(crate) leading_controller: Option<String>,
    /// The members of the metadata store.
    pub(crate) etcd_members: Vec<EtcdMember>,
}

/// Names the pod behind a registered address: its first label.
///
/// # Arguments
///
/// * `address` - What a replica registered, such as `bitacora-replica-2.bitacora-replica.storage.svc:7000`.
///
/// # Returns
///
/// The text before the first dot or colon; the whole address if it has neither.
pub(crate) fn pod_of_address(address: &str) -> &str {
    match address.find(['.', ':']) {
        Some(end_of_first_label) => &address[..end_of_first_label],
        None => address,
    }
}

/// Names the zone a replica is in, if it registered one: its first failure domain when a narrower one follows.
fn zone_of(replica: &ReplicaRecord) -> Option<&str> {
    if replica.failure_domains.len() < 2 {
        return None;
    }
    replica.failure_domains.first().map(String::as_str)
}

/// Chooses one fault of `kind` for the cluster in `view`.
///
/// # Arguments
///
/// * `kind` - The kind of fault wanted.
/// * `view` - The cluster as the nemesis read it.
/// * `random` - Picks among the possible targets.
///
/// # Returns
///
/// The fault, or `None` if the cluster offers no target of that kind that the design claims to survive losing: no
/// replica, no leading controller, fewer than three members of the metadata store, or no zone that may be cut.
pub(crate) fn choose_fault(kind: FaultKind, view: &ClusterView, random: &mut DeterministicRandom) -> Option<Fault> {
    match kind {
        FaultKind::KillReplica => {
            let pod = pod_of_some_replica(view, random)?;
            Some(Fault::KillReplica { pod })
        }
        FaultKind::CutReplica => {
            let pod = pod_of_some_replica(view, random)?;
            Some(Fault::CutReplica { pod })
        }
        FaultKind::KillController => {
            let pod = view.leading_controller.clone()?;
            Some(Fault::KillController { pod })
        }
        FaultKind::KillEtcdMember => {
            if view.etcd_members.len() < FEWEST_ETCD_MEMBERS_FOR_A_KILL {
                return None;
            }
            let member = random.choose(&view.etcd_members)?;
            Some(Fault::KillEtcdMember {
                pod: member.pod.clone(),
            })
        }
        FaultKind::CutZone => {
            let zones = zones_that_may_be_cut(view);
            let zone = random.choose(&zones)?;
            Some(Fault::CutZone { zone: zone.clone() })
        }
    }
}

/// Picks one of the registered replicas and names its pod. `None` if no replica is registered.
fn pod_of_some_replica(view: &ClusterView, random: &mut DeterministicRandom) -> Option<String> {
    let replica = random.choose(&view.replicas)?;
    Some(pod_of_address(&replica.address).to_owned())
}

/// Says which replicas a cut hides from the rest of the cluster.
///
/// # Arguments
///
/// * `fault` - The fault.
/// * `view` - The cluster as the nemesis read it when it chose the fault.
///
/// # Returns
///
/// The replica in the pod of a replica cut, the replicas in the zone of a zone cut, and nothing for a kill.
pub(crate) fn replicas_cut_by(fault: &Fault, view: &ClusterView) -> BTreeSet<ReplicaId> {
    let mut cut_off = BTreeSet::new();
    for replica in &view.replicas {
        let is_behind_the_cut = match fault {
            Fault::CutReplica { pod } => pod_of_address(&replica.address) == pod,
            Fault::CutZone { zone } => zone_of(replica) == Some(zone.as_str()),
            Fault::KillReplica { .. } | Fault::KillController { .. } | Fault::KillEtcdMember { .. } => false,
        };
        if is_behind_the_cut {
            cut_off.insert(replica.replica_id.clone());
        }
    }
    cut_off
}

/// Lists the zones that may be cut off, in alphabetical order. See the file header for the rule.
fn zones_that_may_be_cut(view: &ClusterView) -> Vec<String> {
    let mut zones_of_replicas = BTreeSet::new();
    for replica in &view.replicas {
        let Some(zone) = zone_of(replica) else {
            // A replica outside every zone makes "a zone" mean less than the whole of a failure domain.
            return Vec::new();
        };
        zones_of_replicas.insert(zone);
    }
    if zones_of_replicas.len() < FEWEST_ZONES_FOR_A_ZONE_CUT {
        return Vec::new();
    }
    let mut zones = Vec::new();
    for zone in zones_of_replicas {
        if keeps_etcd_majority_without(zone, &view.etcd_members) {
            zones.push(zone.to_owned());
        }
    }
    zones
}

/// Whether the metadata store keeps a majority of its members when `zone` is cut off. A member whose zone is not
/// known is counted as lost, which errs on the side of not cutting.
fn keeps_etcd_majority_without(zone: &str, etcd_members: &[EtcdMember]) -> bool {
    let mut members_left = 0;
    for member in etcd_members {
        let is_lost = match &member.zone {
            Some(member_zone) => member_zone == zone,
            None => true,
        };
        if !is_lost {
            members_left += 1;
        }
    }
    members_left * 2 > etcd_members.len() || etcd_members.is_empty()
}

#[cfg(test)]
#[path = "targets_test.rs"]
mod tests;
