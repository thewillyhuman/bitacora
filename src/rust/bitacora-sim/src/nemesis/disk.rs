//! Faults of a node's disk: a refused flush, a silently flipped bit, a disk lost for good.
//!
//! # Why this file exists
//!
//! Disks fail in three ways that a log must survive, and each needs a different defence. A disk that refuses a flush
//! leaves the replica unable to trust its files, so the replica must stop (design, section 7). A disk that flips a bit
//! says nothing at all; only checksums find it, and only if something rereads the data. A disk that is replaced takes
//! every copy on it along, and the node that comes back must not be taken for the one that held them. This file injects
//! the three.
//!
//! # What it contains
//!
//! - [`fail_next_flush`]: makes the planned node's disk refuse flushes.
//! - [`corrupt_a_stored_byte`]: flips one bit in one segment file of the unlucky node.
//! - [`lose_disk`]: replaces the unlucky node's disk with an empty one and schedules the node's return.
//!
//! # How it works
//!
//! A refused flush is not undone here: the replica reports the failure, the supervisor of `background.rs` kills and
//! restarts it, and restarting is what makes the disk accept flushes again.
//!
//! Bit rot and the lost disk are the two faults that destroy data that was acknowledged. They always hit the same node,
//! the run's unlucky node, so that together they destroy at most one copy of any record, which is what a replication
//! factor of three with a write quorum of two is meant to survive. The nemesis loses a disk at most once per run.
//!
//! # Invariants
//!
//! - Data that was flushed is only ever destroyed on the unlucky node.
//! - Bit rot draws two random numbers, the file and the offset, and none if the node holds no segment file or the
//!   chosen file is empty; in those cases nothing is recorded either.
//! - The history entries are `node N disk fails its next flush for D`, `node N silently corrupts byte B of <path>` and
//!   `node N loses its disk for good`, and are part of the fingerprint of a run.
//!
//! # See also
//!
//! - `nemesis.rs`: the loop that calls these, and the once-per-run rule of the lost disk.
//! - `background.rs`: the supervisor that restarts a replica after a refused flush, and the scrubber that finds flipped
//!   bits.
//! - `world.rs`: the unlucky node.

use std::sync::Arc;
use std::time::Duration;

use bitacora_segment::Disk;
use bitacora_types::DeterministicRandom;
use tokio::task::JoinSet;
use tokio::time::sleep;

use super::{FaultPlan, SEGMENT_FILE_SUFFIX};
use crate::world::World;

/// The bit that silent corruption flips in the byte it hits.
const FLIPPED_BIT: u8 = 0x40;

/// Makes the disk of the planned node refuse its next flush, and every flush until the node is restarted.
///
/// Records the fault and flips the disk's switch. Nothing is scheduled: the replica that meets the refusal reports it
/// and is restarted by the supervisor.
///
/// # Arguments
///
/// * `world` - The run the fault is injected into.
/// * `plan` - The node whose disk is hit. The planned duration appears in the history entry and is otherwise unused.
pub(super) fn fail_next_flush(world: &World, plan: FaultPlan) {
    world.history.fault(plan.describe("disk fails its next flush"));
    world.cluster.nodes()[plan.node_number].disk().fail_flushes(true);
}

/// Flips one bit of one byte in one segment file on the unlucky node's disk, without anyone being told.
///
/// Lists the files of the disk, draws one segment file and one offset into its flushed content, records the fault and
/// flips the bit. Does nothing, and records nothing, if the node holds no segment file or the file drawn is empty.
///
/// # Arguments
///
/// * `world` - The run the fault is injected into.
/// * `random` - The nemesis's random source. Up to two numbers are drawn from it.
pub(super) async fn corrupt_a_stored_byte(world: &World, random: &mut DeterministicRandom) {
    let disk = world.cluster.nodes()[world.unlucky_node].disk();
    let mut segment_files = Vec::new();
    for path in disk.list_files().await.unwrap_or_default() {
        if path.ends_with(SEGMENT_FILE_SUFFIX) {
            segment_files.push(path);
        }
    }
    let Some(path) = random.choose(&segment_files) else {
        return;
    };
    let file_bytes = match disk.durable_content(path) {
        Some(content) => content.len(),
        None => 0,
    };
    if file_bytes == 0 {
        return;
    }
    let offset = random.index(file_bytes);
    world.history.fault(format!(
        "node {} silently corrupts byte {offset} of {path}",
        world.unlucky_node
    ));
    disk.corrupt_byte(path, offset, FLIPPED_BIT);
}

/// Replaces the unlucky node's disk with an empty one, for good, and schedules the node's return.
///
/// Records the fault, kills the node's replica and discards its disk, and spawns a task that starts the node again,
/// empty and under a new identity, after `returns_after`.
///
/// # Arguments
///
/// * `world` - The run the fault is injected into.
/// * `returns_after` - How long the node stays away.
/// * `repairs` - The set that owns the task that brings the node back.
pub(super) fn lose_disk(world: &Arc<World>, returns_after: Duration, repairs: &mut JoinSet<()>) {
    let unlucky_node = world.unlucky_node;
    world
        .history
        .fault(format!("node {unlucky_node} loses its disk for good"));
    world.cluster.nodes()[unlucky_node].lose_disk();
    let world = Arc::clone(world);
    repairs.spawn(async move {
        sleep(returns_after).await;
        world.cluster.nodes()[unlucky_node].restart().await;
    });
}

#[cfg(test)]
#[path = "disk_test.rs"]
mod tests;
