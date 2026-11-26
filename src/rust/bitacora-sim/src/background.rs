//! What a cluster does on its own while a simulation runs: register, control, supervise, scrub.
//!
//! # Why this file exists
//!
//! The test kit's cluster does nothing by itself: no replica renews its registration, nothing restarts a replica that
//! exited, nothing scrubs, and there is no controller. In production those are daemons and timers around the library
//! code. A simulation that left them out would test a cluster that cannot heal, and would report every fault as fatal.
//! This file plays those parts, for the whole length of a run, including the audit: the healed cluster needs its
//! controller to recover.
//!
//! # What it contains
//!
//! - [`start_background_tasks`]: starts the four tasks below and returns the set that owns them.
//! - `renew_registrations`: what every replica does on a timer.
//! - `run_controller`: one controller, with settings shortened for a one-minute run.
//! - `stop_replicas_whose_disk_failed`: the process supervisor: a replica whose disk refused a write exits and is
//!   started again a little later.
//! - `scrub_replicas`: what every replica does on a schedule: reread its sealed segments.
//!
//! # How it works
//!
//! Each task is a loop around a sleep. They are started in a fixed order, before any client, and stopped together when
//! the run has been audited. Only the supervisor draws random numbers, from a source of its own that is forked from the
//! run's.
//!
//! # Invariants
//!
//! - The tasks are spawned in the order registration, controller, supervisor, scrubber, and the supervisor's random
//!   source is forked at its spawn. The order of spawns and of forks is part of what a seed means.
//! - None of these tasks is a fault: they run the same from the first instant of a run to its last, and are not stopped
//!   for healing.
//! - A task never fails the run. What goes wrong in a round (the metadata store is down, a replica stops mid-scrub) is
//!   tried again in the next.
//! - The numbers in this file shape every run. Changing one changes what every seed does.
//!
//! # See also
//!
//! - `simulation.rs`: starts and stops the tasks.
//! - `nemesis.rs`: what goes wrong; this file is what goes right.
//! - `bitacora-controller`: the controller that runs here.
//! - `tests/fault_matrix_test.rs`: where these tasks are tested. They have no unit tests: each is a loop around calls
//!   that are tested in their own crates, and no run of the fault matrix settles without them.

use std::sync::Arc;
use std::time::Duration;

use bitacora_controller::{Controller, ControllerSettings};
use bitacora_meta::PlacementPolicy;
use bitacora_types::DeterministicRandom;
use tokio::task::JoinSet;
use tokio::time::sleep;

use crate::world::World;

/// How often replicas renew their registration.
const REGISTRATION_INTERVAL: Duration = Duration::from_secs(2);

/// How many registration intervals a replica may miss before placement takes it for dead.
const MISSED_REGISTRATIONS_BEFORE_DEAD: u32 = 3;

/// How often replicas scrub their sealed segments. Weekly in production; here often enough to find injected corruption
/// within a run.
const SCRUB_INTERVAL: Duration = Duration::from_secs(3);

/// How often the supervisor looks for replicas whose disk refused a write.
const SUPERVISOR_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The shortest time, in milliseconds, the supervisor waits before it restarts a replica that exited.
const SHORTEST_RESTART_DELAY_MILLIS: u64 = 100;

/// The longest time, in milliseconds, the supervisor waits before it restarts a replica that exited.
const LONGEST_RESTART_DELAY_MILLIS: u64 = 5_000;

/// How often the simulated controller makes a pass over the cluster.
const CONTROLLER_PASS_INTERVAL: Duration = Duration::from_millis(500);

/// How long the simulated controller's leadership lasts without renewal.
const CONTROLLER_LEADERSHIP_DURATION: Duration = Duration::from_secs(5);

/// How long the simulated controller waits for one request to a replica.
const CONTROLLER_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// How long a replica must be unreachable before the simulated controller gives its copies up for lost.
const REPLICA_LOST_AFTER: Duration = Duration::from_secs(30);

/// How long after a segment is sealed the simulated controller uploads it. Short enough that segments are uploaded, and
/// their local copies released, before truncation deletes them: the run must read from the object store too.
const TIERING_DELAY: Duration = Duration::from_secs(1);

/// How long after its upload a segment keeps its local copies in the simulation.
const HOT_READ_WINDOW: Duration = Duration::from_secs(2);

/// How long the simulated controller waits for one upload.
const UPLOAD_TIMEOUT: Duration = Duration::from_secs(5);

/// Starts everything a cluster does on its own, for the rest of the run.
///
/// Spawns the registration loop, the controller, the supervisor and the scrubber, in that order. The supervisor gets a
/// random source forked from `random`.
///
/// # Arguments
///
/// * `world` - The run the tasks belong to.
/// * `random` - The random source of the run. One fork is taken from it.
///
/// # Returns
///
/// The set that owns the four tasks. They run until the set is told to abort them or is dropped.
pub(crate) fn start_background_tasks(world: &Arc<World>, random: &mut DeterministicRandom) -> JoinSet<()> {
    let mut background = JoinSet::new();
    background.spawn(renew_registrations(Arc::clone(world)));
    background.spawn(run_controller(Arc::clone(world)));
    background.spawn(stop_replicas_whose_disk_failed(Arc::clone(world), random.fork()));
    background.spawn(scrub_replicas(Arc::clone(world)));
    background
}

/// Does what every replica does on a timer: renews its registration.
async fn renew_registrations(world: Arc<World>) {
    loop {
        // A metadata outage makes this fail; the next round tries again.
        drop(world.cluster.renew_registrations().await);
        sleep(REGISTRATION_INTERVAL).await;
    }
}

/// Runs the cluster's controller until the task is stopped.
async fn run_controller(world: Arc<World>) {
    let settings = ControllerSettings {
        pass_interval: CONTROLLER_PASS_INTERVAL,
        leadership_duration: CONTROLLER_LEADERSHIP_DURATION,
        placement: PlacementPolicy {
            liveness_window: REGISTRATION_INTERVAL * MISSED_REGISTRATIONS_BEFORE_DEAD,
            ..PlacementPolicy::default()
        },
        request_timeout: CONTROLLER_REQUEST_TIMEOUT,
        replica_lost_after: REPLICA_LOST_AFTER,
        tiering_delay: TIERING_DELAY,
        hot_read_window: HOT_READ_WINDOW,
        upload_timeout: UPLOAD_TIMEOUT,
        ..ControllerSettings::default()
    };
    let controller = Controller::new(
        world.cluster.metadata().clone(),
        world.cluster.connector(),
        world.cluster.clock(),
        settings,
    )
    .with_artifact_store(world.artifact_store());
    controller.run().await;
}

/// Plays the part of the process supervisor. A replica whose disk refuses a flush exits (design, section 7); this
/// notices, and restarts the replica a little later.
async fn stop_replicas_whose_disk_failed(world: Arc<World>, mut random: DeterministicRandom) {
    loop {
        sleep(SUPERVISOR_POLL_INTERVAL).await;
        for (node_number, node) in world.cluster.nodes().iter().enumerate() {
            if node.is_running() && node.has_failed_write() {
                world
                    .history
                    .fault(format!("node {node_number} exits after a failed flush"));
                node.crash();
                let restart_delay =
                    Duration::from_millis(random.between(SHORTEST_RESTART_DELAY_MILLIS, LONGEST_RESTART_DELAY_MILLIS));
                let world = Arc::clone(&world);
                tokio::spawn(async move {
                    sleep(restart_delay).await;
                    world.cluster.nodes()[node_number].restart().await;
                });
            }
        }
    }
}

/// Does what every replica does on a schedule: rereads its sealed segments and marks the copies that fail a checksum.
async fn scrub_replicas(world: Arc<World>) {
    loop {
        sleep(SCRUB_INTERVAL).await;
        for node in world.cluster.nodes() {
            if let Some(replica) = node.replica() {
                // A replica that stops mid-scrub is scrubbed next time.
                drop(replica.scrub_sealed_segments().await);
            }
        }
    }
}
