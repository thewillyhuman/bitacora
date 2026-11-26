//! Faults of the network path to a node: a partition, a slow link, a link that loses answers.
//!
//! # Why this file exists
//!
//! Most of the log's protocol exists because of the network. A partition makes a writer replace a replica that is alive
//! and will come back with a stale copy. A slow link delivers a request after its sender gave up and moved on. A lost
//! answer leaves a writer not knowing whether its append was stored, which is the case its "unknown outcome" exists
//! for. Each of the three exercises a different part of the protocol, so the nemesis injects each on its own.
//!
//! # What it contains
//!
//! - [`disturb_link`]: puts the planned fault on the link of the planned node and schedules its removal.
//! - `link_fault_of`: the link fault a kind stands for.
//!
//! # How it works
//!
//! The fault replaces whatever was wrong with the link before, and the removal task sets the link back to healthy after
//! the planned duration, whatever is wrong with it by then. Faults on one link therefore do not stack: the last one
//! injected wins, and the first removal heals the link. A slow link draws its delay from the nemesis's random source.
//!
//! # Invariants
//!
//! - Only a slow link draws a random number, and exactly one.
//! - The history entry is `node N link gets <fault> for D`, with the fault rendered by its `Debug` implementation, and
//!   is part of the fingerprint of a run. That makes the names of `LinkFault` and of its fields part of the fingerprint
//!   too.
//!
//! # See also
//!
//! - `nemesis.rs`: the loop that calls this.
//! - `bitacora-testkit`: `Link` and `LinkFault`, which apply the fault.

use std::sync::Arc;
use std::time::Duration;

use bitacora_testkit::LinkFault;
use bitacora_types::DeterministicRandom;
use tokio::task::JoinSet;
use tokio::time::sleep;

use super::FaultPlan;
use crate::config::FaultKind;
use crate::world::World;

/// The shortest delay, in milliseconds, of a slow link.
const SHORTEST_LINK_DELAY_MILLIS: u64 = 1;

/// The longest delay, in milliseconds, of a slow link. Below the clients' request timeout, so that a slow link is slow
/// and not a partition.
const LONGEST_LINK_DELAY_MILLIS: u64 = 1_500;

/// Puts the planned fault on the link of the planned node and schedules its removal.
///
/// Records the fault, replaces the link's fault with it, and spawns a task that makes the link healthy again after the
/// planned duration.
///
/// # Arguments
///
/// * `world` - The run the fault is injected into.
/// * `plan` - The node whose link is hit, which of the three link faults it gets, and for how long.
/// * `random` - The nemesis's random source. A slow link draws its delay from it.
/// * `repairs` - The set that owns the removal task.
pub(super) fn disturb_link(
    world: &Arc<World>,
    plan: FaultPlan,
    random: &mut DeterministicRandom,
    repairs: &mut JoinSet<()>,
) {
    let fault = link_fault_of(plan.kind, random);
    world.history.fault(plan.describe(&format!("link gets {fault:?}")));
    world.cluster.nodes()[plan.node_number].link().set_fault(fault);
    let world = Arc::clone(world);
    repairs.spawn(async move {
        sleep(plan.lasts_for).await;
        world.cluster.nodes()[plan.node_number]
            .link()
            .set_fault(LinkFault::default());
    });
}

/// Builds the link fault `kind` stands for: a cut for a partition, a random delay for a slow link, and lost answers for
/// every other kind the caller passes, which is only ever [`FaultKind::LostResponses`].
fn link_fault_of(kind: FaultKind, random: &mut DeterministicRandom) -> LinkFault {
    match kind {
        FaultKind::Partition => LinkFault {
            is_cut: true,
            ..LinkFault::default()
        },
        FaultKind::SlowLink => LinkFault {
            delay: Duration::from_millis(random.between(SHORTEST_LINK_DELAY_MILLIS, LONGEST_LINK_DELAY_MILLIS)),
            ..LinkFault::default()
        },
        _lost_responses => LinkFault {
            loses_responses: true,
            ..LinkFault::default()
        },
    }
}

#[cfg(test)]
#[path = "link_test.rs"]
mod tests;
