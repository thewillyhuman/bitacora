//! One simulated run of a whole cluster, from its seed to its report (design, section 15, layer 2).
//!
//! # Why this file exists
//!
//! The parts of a simulation are spread over this crate: the cluster and its background tasks, the clients, the
//! nemesis, the audit, the checkers. Something has to put them on one runtime, in an order that never varies, and take
//! them through the phases of a run. That order is what makes a seed reproduce a run exactly, so it is written in one
//! place, top to bottom, where a reader can see all of it.
//!
//! # What it contains
//!
//! - [`run`]: executes one configuration on a runtime of its own and returns the report.
//! - `simulate`: the run itself, as the single task everything else is spawned from.
//! - `start_clients`: starts the compute nodes and the readers.
//! - `inject_faults_for`: the chaos phase.
//!
//! # How it works
//!
//! Replicas, the controller, compute nodes and readers all run in one thread on a simulated clock, network and disk.
//! The runtime's clock is paused, so time advances only when every task is waiting, and a minute of simulated time
//! takes as long as its events take to compute. The runtime's own random choices are seeded from the run's seed as
//! well.
//!
//! A run has three phases:
//!
//! 1. **Chaos.** The nemesis injects faults while the compute nodes serve a key-value store on the stream and the
//!    readers follow it.
//! 2. **Healing.** The nemesis and the clients are stopped, every fault is removed and every node restarted.
//! 3. **Audit.** A final record is written, every key and the whole stream are read back, and the cluster is given a
//!    bounded time to become whole again.
//!
//! The history of the run is then judged by the log checker and by the linearizability check.
//!
//! Every task gets a random source of its own, forked from the run's in the order the tasks are started. A task that
//! draws more or fewer numbers therefore does not disturb the others.
//!
//! # Invariants
//!
//! - The same configuration always produces the same report.
//! - `simulate` draws, spawns and awaits in a fixed order: the world; the background tasks (one fork); per compute node
//!   its crash channel, its store dependencies and one fork, then its task; one fork and one task per reader; one fork
//!   for the nemesis. Changing that order changes what every seed does, and a seed that once found a bug may no longer
//!   find it.
//! - The nemesis is awaited after it is aborted, so that it cannot inject a fault into the healed cluster.
//! - The background tasks run through healing and the audit, and are stopped before the history is judged.
//! - Nothing here reads the system clock or any other source of randomness.
//!
//! # See also
//!
//! - `config.rs`: what a run is given. `report.rs`: what it returns.
//! - `world.rs`, `background.rs`, `kv_workload.rs`, `reader_workload.rs`, `nemesis.rs`, `audit.rs`: the parts.
//! - `tests/fault_matrix_test.rs`: where runs are tested, over many seeds. This file has no unit tests: its only
//!   behaviour is a whole run.

use std::sync::Arc;

use bitacora_types::DeterministicRandom;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};

use crate::audit::{audit, heal};
use crate::background::start_background_tasks;
use crate::config::SimulationConfig;
use crate::diagnosis::describe_stuck_cluster;
use crate::kv_workload::ComputeNode;
use crate::nemesis::run_nemesis;
use crate::reader_workload::run_reader;
use crate::report::{SimulationReport, judge};
use crate::world::{STREAM, World};

/// Where tiered segments live in the object store. The key-value store's artifacts are under another prefix.
const TIERED_SEGMENTS_KEY_PREFIX: &str = "streams/";

/// The clients of a run, and the channels the nemesis reaches the compute nodes through.
struct Clients {
    /// The tasks of the compute nodes and of the readers.
    tasks: JoinSet<()>,
    /// One sender per compute node, by node number. Changing its value tells that node to crash.
    crash_signals: Vec<watch::Sender<u64>>,
}

/// Runs one simulation to completion and returns what happened.
///
/// Builds a single-threaded runtime with a paused clock, seeded from the configuration, and runs the whole simulation
/// on it: the chaos phase, the healing and the audit. Then judges the history. It blocks the calling thread for as long
/// as the events of the run take to compute, which is seconds for the standard run; no real time is spent waiting. The
/// same configuration always produces the same report.
///
/// # Arguments
///
/// * `config` - What the run looks like.
///
/// # Returns
///
/// The history of the run, every invariant it breaks, and why the healed cluster did not recover if it did not.
///
/// # Panics
///
/// Panics if it is called from inside a Tokio runtime, because it blocks on a runtime of its own.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use bitacora_sim::{FaultMix, SimulationConfig, run};
///
/// let short_and_calm = SimulationConfig {
///     faults: FaultMix::NONE,
///     chaos_duration: Duration::from_secs(2),
///     ..SimulationConfig::with_seed(1)
/// };
///
/// let report = run(&short_and_calm);
///
/// assert!(report.is_clean(), "{}", report.failure_summary());
/// ```
#[must_use]
pub fn run(config: &SimulationConfig) -> SimulationReport {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        // Tokio makes a few random choices of its own, such as which waiter of a watch channel wakes first. Seeding
        // them is what makes a run replay exactly.
        .rng_seed(tokio::runtime::RngSeed::from_bytes(&config.seed.to_le_bytes()))
        .build()
        .expect("a current-thread runtime needs nothing from the system")
        .block_on(simulate(config.clone()))
}

/// Takes one run through its phases: start, chaos, healing, audit, verdict.
async fn simulate(config: SimulationConfig) -> SimulationReport {
    let mut random = DeterministicRandom::from_seed(config.seed);
    let world = Arc::new(World::start(config.node_count, &mut random).await);
    let mut background = start_background_tasks(&world, &mut random);
    let (designated_writer, _) = watch::channel(0_usize);
    let mut clients = start_clients(&world, &config, &mut random, &designated_writer);

    inject_faults_for(&world, &config, &mut random, designated_writer, clients.crash_signals).await;
    heal(&world, &mut clients.tasks).await;
    let settled = match timeout(config.settle_timeout, audit(&world)).await {
        Ok(audited) => audited,
        Err(_elapsed) => Err(describe_stuck_cluster(&world, config.settle_timeout).await),
    };
    background.abort_all();

    let mut report = judge(config.seed, &world.history.events(), settled);
    report.tiered_segment_reads = world
        .cluster
        .artifact_store()
        .served_range_reads(TIERED_SEGMENTS_KEY_PREFIX);
    report
}

/// Starts the clients of a run: the compute nodes, which serve the key-value store, and the readers.
///
/// Each compute node gets a crash channel, store dependencies under the identity `compute-N`, and a fork of `random`;
/// each reader gets a fork. The order of these steps is part of what a seed means.
fn start_clients(
    world: &Arc<World>,
    config: &SimulationConfig,
    random: &mut DeterministicRandom,
    designated_writer: &watch::Sender<usize>,
) -> Clients {
    let mut crash_signals = Vec::new();
    let mut tasks = JoinSet::new();
    for compute_node in 0..config.compute_node_count {
        let (crash_signal, crashes) = watch::channel(0_u64);
        crash_signals.push(crash_signal);
        let node = ComputeNode {
            node_number: compute_node,
            stream: STREAM,
            dependencies: world.store_dependencies(&format!("compute-{compute_node}")),
            history: world.history.clone(),
            injected_bug: config.injected_bug,
            random: random.fork(),
            puts_made: 0,
        };
        tasks.spawn(node.run(designated_writer.subscribe(), crashes));
    }
    for _reader in 0..config.reader_count {
        tasks.spawn(run_reader(Arc::clone(world), random.fork()));
    }
    Clients { tasks, crash_signals }
}

/// Runs the chaos phase: starts the nemesis, lets it work for the configured time, and stops it.
///
/// Returns only once the nemesis is gone, so that it cannot inject a fault into the healed cluster. The channels to the
/// compute nodes are handed to the nemesis and end with it.
async fn inject_faults_for(
    world: &Arc<World>,
    config: &SimulationConfig,
    random: &mut DeterministicRandom,
    designated_writer: watch::Sender<usize>,
    crash_signals: Vec<watch::Sender<u64>>,
) {
    let nemesis = tokio::spawn(run_nemesis(
        Arc::clone(world),
        config.clone(),
        random.fork(),
        designated_writer,
        crash_signals,
    ));
    sleep(config.chaos_duration).await;
    nemesis.abort();
    drop(nemesis.await);
}
