//! The nemesis: the task that makes things go wrong during a simulated run.
//!
//! # Why this file exists
//!
//! A replicated log is only interesting when parts of it fail, and the bugs that matter need failures that overlap: a
//! writer is fenced while a replica is restarting behind a slow link. Nobody can write those scenarios down by hand,
//! and nobody would think of the ones that find bugs. So a simulated run has one task whose only job is to break things
//! at random moments, from the run's seed, and to undo each fault a random time later so that the next one lands on a
//! cluster that is half recovered (design, section 15, "Faults injected").
//!
//! # What it contains
//!
//! - [`run_nemesis`]: the loop that picks the next fault, waits, and injects it.
//! - `FaultPlan`: what was drawn for one fault: its kind, the node it hits and how long it lasts.
//! - The faults themselves are in the files of `nemesis/`, one file per thing that is hit: `process.rs` (crashes),
//!   `disk.rs` (refused flushes, bit rot, a lost disk), `link.rs` (partitions, delays, lost answers), `stores.rs`
//!   (outages of the metadata store and of the object store) and `writer.rs` (a crashed or a replaced writer).
//!
//! # How it works
//!
//! Each round of the loop sleeps for a random gap, then draws three numbers whether or not the fault will use them: the
//! kind, a node, a duration. Drawing all three every time means a round costs the same draws whatever kind comes up.
//! The round then calls the function of that kind, which records the fault in the history, applies it, and, if the
//! fault ends by itself, spawns a small task that undoes it after the duration.
//!
//! Those undo tasks live in a set owned by the nemesis. When the chaos phase ends the simulation aborts the nemesis,
//! the set is dropped with it, and whatever was still to be undone is left to the healing step.
//!
//! # Invariants
//!
//! - Per round the random source is asked, in this order, for the gap, the kind, the node and the duration, and then
//!   for whatever the fault itself draws. That order is part of what a seed means.
//! - Every fault is recorded in the history before it is applied, in words that are part of the fingerprint of a run.
//!   Do not reword them.
//! - What [`run_nemesis`] owns is dropped in a fixed order when the task is aborted: the undo tasks first, then the
//!   senders that tell the compute nodes to crash, then the sender that designates the writer. Dropping a sender wakes
//!   the tasks that wait on it, so that order decides the order in which tasks run next, which is part of what a seed
//!   means. The function keeps its parameters and locals in the order they had when the regression seeds were found; do
//!   not reorder them, and do not move them into a struct.
//! - A fault function never awaits before it has recorded and applied its fault.
//! - With no kind enabled the nemesis does nothing, forever.
//! - [`run_nemesis`] is one function of some forty lines for the reason above: it must own everything the faults share,
//!   in that order, and its one await per round must stay where it is. What each fault does is in the functions it
//!   calls.
//!
//! # See also
//!
//! - `config.rs`: `FaultMix`, which enables kinds, and `FaultKind`.
//! - `simulation.rs`: starts the nemesis and aborts it when the chaos phase is over.
//! - `audit.rs`: the healing that removes what the nemesis left behind.

mod disk;
mod link;
mod process;
mod stores;
mod writer;

use std::sync::Arc;
use std::time::Duration;

use bitacora_types::DeterministicRandom;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::sleep;

use crate::config::{FaultKind, SimulationConfig};
use crate::world::World;

/// The shortest time, in milliseconds, a fault lasts before it is undone.
const SHORTEST_FAULT_MILLIS: u64 = 100;

/// The longest time, in milliseconds, a fault lasts before it is undone. Longer than a writer's lease and than every
/// timeout of the simulated clients, so that some faults outlast them.
const LONGEST_FAULT_MILLIS: u64 = 12_000;

/// What the path of every segment file on a replica's disk ends with. The faults that damage stored data aim at these
/// files: the state files next to them are replaced atomically and are never half written.
const SEGMENT_FILE_SUFFIX: &str = ".seg";

/// One fault as the nemesis drew it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FaultPlan {
    /// Which kind of fault to inject.
    kind: FaultKind,
    /// The storage node the fault hits. Kinds that hit something else ignore it.
    node_number: usize,
    /// How long the fault lasts before it is undone. Kinds that are not undone by the nemesis ignore it.
    lasts_for: Duration,
}

impl FaultPlan {
    /// Words the history entry of a fault on the plan's node: `node N <what> for <duration>`.
    fn describe(&self, what: &str) -> String {
        format!("node {} {what} for {:?}", self.node_number, self.lasts_for)
    }
}

/// Injects faults at random moments and undoes each one after a random time, until the task is aborted.
///
/// See the file header for the loop, and for why the parameters and locals of this function must keep their order.
///
/// # Arguments
///
/// * `world` - The run the nemesis acts on.
/// * `config` - The configuration of the run: which kinds are enabled, how often faults come, and the sizes of the
///   cluster the node and the writer are drawn from.
/// * `random` - The nemesis's own random source.
/// * `designated_writer` - Says which compute node should be the writer. The nemesis changes it to switch writers.
/// * `crash_signals` - One sender per compute node; changing its value tells that node to crash.
///
/// # Panics
///
/// Panics if a kind is drawn from an empty list, which cannot happen: with no kind enabled the function waits forever
/// before it draws.
pub(crate) async fn run_nemesis(
    world: Arc<World>,
    config: SimulationConfig,
    mut random: DeterministicRandom,
    designated_writer: watch::Sender<usize>,
    crash_signals: Vec<watch::Sender<u64>>,
) {
    let kinds = config.faults.enabled_kinds();
    if kinds.is_empty() {
        std::future::pending::<()>().await;
    }
    let mean_gap_millis = u64::try_from(config.mean_time_between_faults.as_millis()).unwrap_or(u64::MAX);
    let mut repairs = JoinSet::new();
    let mut has_lost_a_disk = false;
    loop {
        // Uniform between zero and twice the mean, so that the mean is the mean.
        let gap_millis = random.between(0, mean_gap_millis.saturating_mul(2));
        sleep(Duration::from_millis(gap_millis)).await;
        let plan = FaultPlan {
            kind: *random.choose(&kinds).expect("some fault is enabled"),
            node_number: random.index(config.node_count),
            lasts_for: Duration::from_millis(random.between(SHORTEST_FAULT_MILLIS, LONGEST_FAULT_MILLIS)),
        };
        match plan.kind {
            FaultKind::NodeCrash | FaultKind::TornCrash => {
                process::crash_node(&world, plan, &mut random, &mut repairs);
            }
            FaultKind::Partition | FaultKind::SlowLink | FaultKind::LostResponses => {
                link::disturb_link(&world, plan, &mut random, &mut repairs);
            }
            FaultKind::FlushFailure => disk::fail_next_flush(&world, plan),
            FaultKind::MetadataOutage => stores::cut_off_metadata_store(&world, plan.lasts_for, &mut repairs),
            FaultKind::ObjectStoreOutage => stores::cut_off_object_store(&world, plan.lasts_for, &mut repairs),
            FaultKind::WriterCrash => writer::crash_writer(&world, &designated_writer, &crash_signals),
            FaultKind::SilentCorruption => disk::corrupt_a_stored_byte(&world, &mut random).await,
            FaultKind::DiskLoss => {
                // Once per run: a second lost disk on the same node would prove nothing more.
                if !has_lost_a_disk {
                    has_lost_a_disk = true;
                    disk::lose_disk(&world, plan.lasts_for, &mut repairs);
                }
            }
            FaultKind::WriterSwitch => {
                writer::switch_writer(&world, &designated_writer, config.compute_node_count);
            }
        }
    }
}

#[cfg(test)]
#[path = "nemesis_test.rs"]
mod tests;
