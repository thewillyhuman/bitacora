//! Telling whether the cluster is whole again.
//!
//! # Why this file exists
//!
//! While faults are injected, a cluster may refuse work; once they are gone it must recover (design, section 15). A
//! soak therefore has to know when the cluster has recovered: before it injects the next fault, so that it never
//! injects two at once, and after the last one, before it judges a round. This file is that question and the wait
//! for its answer.
//!
//! # What it contains
//!
//! - [`reason_it_is_unhealthy`]: the rule, as a pure function of what the metadata store holds.
//! - [`read_unhealthy_reason`]: the rule applied to the cluster as it is now.
//! - [`alive_replicas`]: the replicas a run starts with, which every later check asks for again.
//! - [`wait_until_healthy`]: asks again and again, for a bounded time.
//!
//! # How it works
//!
//! A cluster is healthy when every replica the run started with has renewed its registration within the liveness
//! window, and a controller holds a leadership that has not expired. Both are read from the metadata store, as an
//! operator would read them with `bita replica list` and `bita controller status`. A metadata store that does not
//! answer is itself a reason.
//!
//! # Invariants
//!
//! - The rule looks only at the metadata store. It never asks a replica or a controller directly.
//! - [`wait_until_healthy`] returns within its time limit plus one poll, healthy or not.
//!
//! # See also
//!
//! - `nemesis.rs`: asks before every fault.
//! - `round.rs`: waits after the last one.

use std::collections::BTreeSet;
use std::time::Duration;

use bitacora_meta::{LeaderRecord, ReplicaRecord};
use bitacora_types::{ReplicaId, Timestamp};
use tokio::time::{Instant, sleep};

use crate::cluster::SoakedCluster;

/// How old a replica's last registration may be before the replica counts as gone. The chart's replicas register
/// every five seconds, and its controller gives a replica no new segment after fifteen.
pub(crate) const LIVENESS_WINDOW: Duration = Duration::from_secs(15);

/// How long [`wait_until_healthy`] waits between two looks at the metadata store.
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Says why a cluster is not healthy, or that it is.
///
/// # Arguments
///
/// * `expected_replicas` - The replicas the run started with.
/// * `registered_replicas` - The registry of the metadata store.
/// * `leader` - The leadership record of the metadata store, if there is one.
/// * `now` - The time by the cluster's clock.
///
/// # Returns
///
/// `None` if every expected replica registered within [`LIVENESS_WINDOW`] and a leadership is in force. Otherwise
/// the first thing that is wrong, in words.
pub(crate) fn reason_it_is_unhealthy(
    expected_replicas: &BTreeSet<ReplicaId>,
    registered_replicas: &[ReplicaRecord],
    leader: Option<&LeaderRecord>,
    now: Timestamp,
) -> Option<String> {
    for expected in expected_replicas {
        let Some(registration) = registration_of(expected, registered_replicas) else {
            return Some(format!("replica {expected} is no longer registered"));
        };
        if !registration.is_alive(now, LIVENESS_WINDOW) {
            let silent_for = now.duration_since(registration.heartbeat_at);
            return Some(format!("replica {expected} has not registered for {silent_for:?}"));
        }
    }
    match leader {
        None => Some("no controller leads".to_owned()),
        Some(leader) if leader.expires_at <= now => {
            Some(format!("the leadership of controller {} has expired", leader.holder))
        }
        Some(_leader_in_force) => None,
    }
}

/// Finds the registration of `replica_id` in the registry.
///
/// # Returns
///
/// The registration, or `None` if the replica is not registered.
pub(crate) fn registration_of<'registry>(
    replica_id: &ReplicaId,
    registry: &'registry [ReplicaRecord],
) -> Option<&'registry ReplicaRecord> {
    registry
        .iter()
        .find(|registration| &registration.replica_id == replica_id)
}

/// Applies the rule to the cluster as it is now.
///
/// # Arguments
///
/// * `cluster` - The cluster under test.
/// * `expected_replicas` - The replicas the run started with.
///
/// # Returns
///
/// `None` if the cluster is healthy, otherwise why it is not. A metadata store that cannot be read is a reason.
pub(crate) async fn read_unhealthy_reason(
    cluster: &SoakedCluster,
    expected_replicas: &BTreeSet<ReplicaId>,
) -> Option<String> {
    let registered_replicas = match cluster.metadata.list_replicas().await {
        Ok(registered_replicas) => registered_replicas,
        Err(meta_error) => return Some(format!("the registry of replicas cannot be read: {meta_error}")),
    };
    let leader = match cluster.metadata.get_leader().await {
        Ok(leader) => leader,
        Err(meta_error) => return Some(format!("the leadership cannot be read: {meta_error}")),
    };
    reason_it_is_unhealthy(
        expected_replicas,
        &registered_replicas,
        leader.as_ref(),
        cluster.clock.now(),
    )
}

/// Lists the replicas that are registered and alive right now: what a run expects to find again after every fault.
///
/// # Errors
///
/// Returns the reason, in words, if the registry cannot be read.
pub(crate) async fn alive_replicas(cluster: &SoakedCluster) -> Result<BTreeSet<ReplicaId>, String> {
    let registered_replicas = match cluster.metadata.list_replicas().await {
        Ok(registered_replicas) => registered_replicas,
        Err(meta_error) => return Err(format!("the registry of replicas cannot be read: {meta_error}")),
    };
    let now = cluster.clock.now();
    let mut alive = BTreeSet::new();
    for registration in registered_replicas {
        if registration.is_alive(now, LIVENESS_WINDOW) {
            alive.insert(registration.replica_id);
        }
    }
    Ok(alive)
}

/// Waits until the cluster is healthy, for at most `time_limit`.
///
/// # Arguments
///
/// * `cluster` - The cluster under test.
/// * `expected_replicas` - The replicas the run started with.
/// * `time_limit` - How long to keep asking.
///
/// # Errors
///
/// Returns why the cluster was not healthy when it was last asked, if the time ran out.
pub(crate) async fn wait_until_healthy(
    cluster: &SoakedCluster,
    expected_replicas: &BTreeSet<ReplicaId>,
    time_limit: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + time_limit;
    loop {
        let Some(reason) = read_unhealthy_reason(cluster, expected_replicas).await else {
            return Ok(());
        };
        if Instant::now() >= deadline {
            return Err(reason);
        }
        sleep(POLL_INTERVAL).await;
    }
}

#[cfg(test)]
#[path = "health_test.rs"]
mod tests;
