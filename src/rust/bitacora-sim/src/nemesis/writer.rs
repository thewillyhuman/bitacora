//! Faults of the writer: the writing compute node crashes, or another one takes the stream from it.
//!
//! # Why this file exists
//!
//! The log has one writer at a time, and the hardest part of its protocol is the change from one writer to the next
//! (design, section 9). A writer that crashes leaves appends in flight whose outcome nobody knows, and the next writer
//! must recover the tail without losing what was committed. A writer that is replaced while it is still running is
//! worse: for a moment two processes believe they are the writer, and only fencing keeps the old one from committing.
//! The nemesis causes both.
//!
//! # What it contains
//!
//! - [`crash_writer`]: tells the designated compute node to crash.
//! - [`switch_writer`]: designates the next compute node as the writer, without telling the current one.
//!
//! # How it works
//!
//! The compute nodes are tasks of `kv_workload.rs`, and the nemesis reaches them through two kinds of channel that the
//! simulation hands it. One channel holds the number of the compute node that should be the writer; every compute node
//! watches it. One channel per compute node holds a counter; a change of its value tells that node to drop everything
//! and start over, as a crashed process would.
//!
//! A switch only changes the designation. The node that was the writer is not told: it goes on until the log refuses
//! it, which is the situation fencing exists for.
//!
//! # Invariants
//!
//! - Neither fault draws a random number.
//! - A crash is sent to the node that is designated at that moment, whether or not it has become the writer yet.
//! - The history entry of a crash is `compute node N crashes`, and the entry of a switch names the node that takes over
//!   and says that it takes the stream from a writer still running. Both are part of the fingerprint of a run.
//!
//! # See also
//!
//! - `nemesis.rs`: the loop that calls these.
//! - `kv_workload.rs`: the compute nodes, which watch the two channels.
//! - `simulation.rs`: creates the channels.

use tokio::sync::watch;

use crate::world::World;

/// Tells the compute node that is designated as the writer to crash.
///
/// Records the crash and bumps that node's crash counter. The node drops its store and every call in flight when it
/// notices, and starts over after a pause.
///
/// # Arguments
///
/// * `world` - The run the fault is injected into.
/// * `designated_writer` - Says which compute node should be the writer.
/// * `crash_signals` - One sender per compute node, by node number.
///
/// # Panics
///
/// Panics if the designated node has no crash signal, which cannot happen: both come from the same count of compute
/// nodes.
pub(super) fn crash_writer(
    world: &World,
    designated_writer: &watch::Sender<usize>,
    crash_signals: &[watch::Sender<u64>],
) {
    let compute_node = *designated_writer.borrow();
    world.history.fault(format!("compute node {compute_node} crashes"));
    crash_signals[compute_node].send_modify(|crash_count| *crash_count += 1);
}

/// Designates the next compute node as the writer while the current writer is still running.
///
/// Records the switch and changes the designation to the node after the current one, wrapping around. The current
/// writer is not told; the newly designated node notices and takes the stream.
///
/// # Arguments
///
/// * `world` - The run the fault is injected into.
/// * `designated_writer` - Says which compute node should be the writer; it is changed.
/// * `compute_node_count` - How many compute nodes the run has. Must not be zero.
///
/// # Panics
///
/// Panics if `compute_node_count` is zero.
pub(super) fn switch_writer(world: &World, designated_writer: &watch::Sender<usize>, compute_node_count: usize) {
    let next_writer = (*designated_writer.borrow() + 1) % compute_node_count;
    world.history.fault(format!(
        "compute node {next_writer} takes the stream from a writer still running"
    ));
    designated_writer.send_replace(next_writer);
}

#[cfg(test)]
#[path = "writer_test.rs"]
mod tests;
