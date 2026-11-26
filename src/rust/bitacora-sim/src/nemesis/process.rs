//! Faults that kill a replica process: a crash, and a crash that tears what was not flushed.
//!
//! # Why this file exists
//!
//! A crash is the fault the log's durability rule is written for: a replica may acknowledge an append only after its
//! flush returned, because everything else is gone when the process dies. A torn crash is the harder version, and the
//! one real disks produce: part of what was written but not flushed survives, and its last byte is damaged. A replica
//! that restarts must tell such a tail from records it acknowledged, and recover.
//!
//! # What it contains
//!
//! - [`crash_node`]: kills the replica of the planned node, plainly or with torn writes, and schedules its restart.
//!
//! # How it works
//!
//! The kind of the plan says whether the crash is torn. A torn crash hands the node's disk a random source forked from
//! the nemesis's, and the disk decides from it, per segment file, how much of the unflushed tail survives. Only segment
//! files are torn: the state files next to them are replaced atomically and are never half written. The restart is a
//! task that sleeps for the planned duration and starts the replica again on whatever the disk holds by then.
//!
//! # Invariants
//!
//! - A node that is already down is left alone, and nothing is recorded or drawn for it.
//! - A torn crash forks the nemesis's random source exactly once; a plain crash does not touch it.
//! - The history entry is `node N crashes for D` or `node N crashes with torn writes for D`, and is part of the
//!   fingerprint of a run.
//!
//! # See also
//!
//! - `nemesis.rs`: the loop that calls this.
//! - `bitacora-segment`: `CrashMode::TearUnflushedWrites`, which does the tearing.
//! - `background.rs`: the supervisor, which also restarts replicas, after a refused flush.

use std::sync::Arc;

use bitacora_segment::CrashMode;
use bitacora_types::DeterministicRandom;
use tokio::task::JoinSet;
use tokio::time::sleep;

use super::{FaultPlan, SEGMENT_FILE_SUFFIX};
use crate::config::FaultKind;
use crate::world::World;

/// Kills the replica process of the planned node and schedules its restart.
///
/// Does nothing if the node is already down. Otherwise it records the crash, kills the process (tearing the unflushed
/// tail of its segment files if the plan's kind is [`FaultKind::TornCrash`]), and spawns a task that restarts the
/// replica after the planned duration.
///
/// # Arguments
///
/// * `world` - The run the fault is injected into.
/// * `plan` - The node to crash, whether the crash is torn, and how long the node stays down.
/// * `random` - The nemesis's random source. A torn crash takes one fork from it.
/// * `repairs` - The set that owns the restart task.
pub(super) fn crash_node(
    world: &Arc<World>,
    plan: FaultPlan,
    random: &mut DeterministicRandom,
    repairs: &mut JoinSet<()>,
) {
    let node = &world.cluster.nodes()[plan.node_number];
    if !node.is_running() {
        return;
    }
    let is_torn = plan.kind == FaultKind::TornCrash;
    let what_happens = if is_torn { "crashes with torn writes" } else { "crashes" };
    world.history.fault(plan.describe(what_happens));
    if is_torn {
        node.crash_with(CrashMode::TearUnflushedWrites {
            random: random.fork(),
            path_suffix: SEGMENT_FILE_SUFFIX.to_owned(),
        });
    } else {
        node.crash();
    }
    let world = Arc::clone(world);
    repairs.spawn(async move {
        sleep(plan.lasts_for).await;
        world.cluster.nodes()[plan.node_number].restart().await;
    });
}

#[cfg(test)]
#[path = "process_test.rs"]
mod tests;
