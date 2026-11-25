//! One simulated run of a whole cluster (design, section 15, layer 2).
//!
//! Replicas, the controller, writers and readers all run in one thread on a
//! simulated clock, network and disk, driven by a seed. A run has three
//! phases: chaos, during which a nemesis injects faults while clients work;
//! healing, which removes every fault; and an audit, which writes a final
//! record and reads the whole stream back. The history of the run is then
//! judged by the checker.

use std::sync::Arc;
use std::time::Duration;

use bitacora_client::{
    AppendError, BitacoraClient, ClientSettings, ReaderSettings, TakeoverPolicy, WriterSettings,
};
use bitacora_controller::{Controller, ControllerSettings};
use bitacora_meta::{PlacementPolicy, RetentionPolicy};
use bitacora_testkit::{Cluster, LinkFault};
use bitacora_types::{DeterministicRandom, ReplicationSettings, SequenceNumber, SequenceRange};
use bytes::Bytes;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep, timeout, timeout_at};

use crate::checker::{Violation, check, check_audit};
use crate::history::{AppendOutcome, Event, History, TimedEvent};

/// The stream every simulation writes to.
const STREAM: &str = "simulated";

/// How many appends one compute node keeps in flight.
const APPENDS_IN_FLIGHT: usize = 4;

/// How often replicas renew their registration.
const REGISTRATION_INTERVAL: Duration = Duration::from_secs(2);

/// Which kinds of fault a run injects (design, section 15, "Faults
/// injected").
// Every field is an independent switch; there is no hidden state machine.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FaultMix {
    /// Replica processes are killed and later restarted.
    pub node_crashes: bool,
    /// A killed replica's disk keeps part of what was not flushed, with a
    /// damaged last byte: a torn write.
    pub torn_writes: bool,
    /// Links to replicas are cut.
    pub partitions: bool,
    /// Links to replicas delay every request.
    pub slow_links: bool,
    /// Links deliver requests but lose the answers.
    pub lost_responses: bool,
    /// A replica's disk fails a flush, after which the replica stops.
    pub flush_failures: bool,
    /// The metadata store is unreachable for a while.
    pub metadata_outages: bool,
    /// The writing compute node is killed with appends in flight.
    pub writer_crashes: bool,
    /// Another compute node takes the stream while the old writer is still
    /// running, as after a pause that outlived its lease.
    pub writer_switches: bool,
}

impl FaultMix {
    /// No faults: the cluster only has to make progress.
    pub const NONE: Self = Self {
        node_crashes: false,
        torn_writes: false,
        partitions: false,
        slow_links: false,
        lost_responses: false,
        flush_failures: false,
        metadata_outages: false,
        writer_crashes: false,
        writer_switches: false,
    };

    /// Every fault the simulator knows.
    pub const ALL: Self = Self {
        node_crashes: true,
        torn_writes: true,
        partitions: true,
        slow_links: true,
        lost_responses: true,
        flush_failures: true,
        metadata_outages: true,
        writer_crashes: true,
        writer_switches: true,
    };

    fn enabled_kinds(self) -> Vec<FaultKind> {
        [
            (self.node_crashes, FaultKind::NodeCrash),
            (self.torn_writes, FaultKind::TornCrash),
            (self.partitions, FaultKind::Partition),
            (self.slow_links, FaultKind::SlowLink),
            (self.lost_responses, FaultKind::LostResponses),
            (self.flush_failures, FaultKind::FlushFailure),
            (self.metadata_outages, FaultKind::MetadataOutage),
            (self.writer_crashes, FaultKind::WriterCrash),
            (self.writer_switches, FaultKind::WriterSwitch),
        ]
        .into_iter()
        .filter_map(|(is_enabled, kind)| is_enabled.then_some(kind))
        .collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FaultKind {
    NodeCrash,
    TornCrash,
    Partition,
    SlowLink,
    LostResponses,
    FlushFailure,
    MetadataOutage,
    WriterCrash,
    WriterSwitch,
}

/// What one simulated run looks like.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimulationConfig {
    /// Determines every random choice of the run.
    pub seed: u64,
    /// How many storage nodes the cluster has.
    pub node_count: usize,
    /// How many compute nodes compete to be the writer.
    pub compute_node_count: usize,
    /// How many readers follow the stream.
    pub reader_count: usize,
    /// How long faults are injected, in simulated time.
    pub chaos_duration: Duration,
    /// How long the healed cluster gets to accept a final record and serve
    /// the whole stream back, in simulated time.
    pub settle_timeout: Duration,
    /// Which faults are injected.
    pub faults: FaultMix,
    /// The average simulated time between two faults.
    pub mean_time_between_faults: Duration,
}

impl SimulationConfig {
    /// The standard run for `seed`: five storage nodes, two compute nodes,
    /// two readers, a minute of every kind of fault.
    #[must_use]
    pub const fn with_seed(seed: u64) -> Self {
        Self {
            seed,
            node_count: 5,
            compute_node_count: 2,
            reader_count: 2,
            chaos_duration: Duration::from_mins(1),
            settle_timeout: Duration::from_mins(5),
            faults: FaultMix::ALL,
            mean_time_between_faults: Duration::from_millis(700),
        }
    }
}

/// What a run produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimulationReport {
    /// The seed that reproduces the run.
    pub seed: u64,
    /// Everything clients did and saw, in order.
    pub events: Vec<TimedEvent>,
    /// Every invariant the history breaks.
    pub violations: Vec<Violation>,
    /// Why the healed cluster did not recover, if it did not. Safety is
    /// required always; liveness only once every fault is gone.
    pub stuck: Option<String>,
    /// How many appends were reported as committed.
    pub committed_appends: usize,
    /// How many faults were injected.
    pub faults_injected: usize,
}

impl SimulationReport {
    /// Whether the run broke no invariant and the cluster recovered.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.violations.is_empty() && self.stuck.is_none()
    }

    /// A number that identifies the run's whole history. Two runs of the
    /// same seed on the same code have the same fingerprint, on any machine;
    /// comparing fingerprints is the quickest way to see whether a change
    /// altered behaviour.
    #[must_use]
    pub fn fingerprint(&self) -> u64 {
        // FNV-1a over the debug rendering of every event.
        const OFFSET_BASIS: u64 = 0xCBF2_9CE4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01B3;
        self.events
            .iter()
            .flat_map(|timed| format!("{timed:?}").into_bytes())
            .fold(OFFSET_BASIS, |hash, byte| {
                (hash ^ u64::from(byte)).wrapping_mul(PRIME)
            })
    }

    /// Describes what went wrong, with the seed that reproduces it.
    #[must_use]
    pub fn failure_summary(&self) -> String {
        use std::fmt::Write as _;
        let mut summary = format!(
            "simulation seed {} failed after {} committed appends and {} faults\n",
            self.seed, self.committed_appends, self.faults_injected
        );
        if let Some(reason) = &self.stuck {
            let _ = writeln!(summary, "stuck: {reason}");
        }
        for violation in self.violations.iter().take(10) {
            let _ = writeln!(summary, "violation: {violation:?}");
        }
        let _ = writeln!(summary, "last events:");
        let shown_from = self.events.len().saturating_sub(40);
        for timed in &self.events[shown_from..] {
            let _ = writeln!(summary, "  {:>10.3?} {:?}", timed.at, timed.event);
        }
        summary
    }
}

/// Runs one simulation to completion on its own single-threaded runtime
/// with a paused clock, and returns what happened.
///
/// The same configuration always produces the same report.
#[must_use]
pub fn run(config: &SimulationConfig) -> SimulationReport {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        // Tokio makes a few random choices of its own, such as which waiter
        // of a watch channel wakes first. Seeding them is what makes a run
        // replay exactly.
        .rng_seed(tokio::runtime::RngSeed::from_bytes(
            &config.seed.to_le_bytes(),
        ))
        .build()
        .expect("a current-thread runtime needs nothing from the system")
        .block_on(simulate(config.clone()))
}

/// What the tasks of a run share.
struct World {
    cluster: Cluster,
    history: History,
}

impl World {
    fn client(&self, identity: &str) -> BitacoraClient {
        BitacoraClient::new(
            self.cluster.metadata().clone(),
            self.cluster.connector(),
            self.cluster.clock(),
            ClientSettings {
                // Shorter than production, so that timeouts, rolls and
                // lease expiries all happen many times in a minute.
                writer: WriterSettings {
                    writer_identity: identity.to_owned(),
                    request_timeout: Duration::from_secs(2),
                    replica_failure_timeout: Duration::from_secs(4),
                    lease_duration: Duration::from_secs(6),
                    lease_renewal_interval: Duration::from_secs(2),
                    target_segment_bytes: 8 * 1024,
                    target_segment_age: Duration::from_secs(20),
                    open_timeout: Duration::from_secs(15),
                    ..WriterSettings::default()
                },
                reader: ReaderSettings {
                    tail_wait: Duration::from_millis(500),
                    request_timeout: Duration::from_secs(2),
                    ..ReaderSettings::default()
                },
            },
        )
    }
}

async fn simulate(config: SimulationConfig) -> SimulationReport {
    let mut random = DeterministicRandom::from_seed(config.seed);
    let world = Arc::new(World {
        cluster: Cluster::start(config.node_count).await,
        history: History::new(),
    });
    world
        .cluster
        .metadata()
        .create_stream(
            STREAM,
            ReplicationSettings::DEFAULT,
            RetentionPolicy::default(),
            None,
        )
        .await
        .expect("a fresh metadata store accepts a stream");

    let mut background = JoinSet::new();
    background.spawn(renew_registrations(Arc::clone(&world)));
    background.spawn(run_controller(Arc::clone(&world)));
    background.spawn(stop_replicas_whose_disk_failed(
        Arc::clone(&world),
        random.fork(),
    ));

    let (designated_writer, _) = watch::channel(0_usize);
    let mut crash_signals = Vec::new();
    let mut clients = JoinSet::new();
    for compute_node in 0..config.compute_node_count {
        let (crash_signal, crashes) = watch::channel(0_u64);
        crash_signals.push(crash_signal);
        clients.spawn(run_compute_node(
            Arc::clone(&world),
            compute_node,
            random.fork(),
            designated_writer.subscribe(),
            crashes,
        ));
    }
    for _reader in 0..config.reader_count {
        clients.spawn(run_reader(Arc::clone(&world), random.fork()));
    }

    // Chaos.
    let nemesis = tokio::spawn(run_nemesis(
        Arc::clone(&world),
        config.clone(),
        random.fork(),
        designated_writer,
        crash_signals,
    ));
    sleep(config.chaos_duration).await;
    nemesis.abort();
    // Wait for the nemesis to be gone, so that it cannot inject a fault
    // into the healed cluster.
    drop(nemesis.await);

    // Healing: every fault is removed and every process runs again.
    clients.abort_all();
    while clients.join_next().await.is_some() {}
    world.cluster.key_value_store().set_unavailable(false);
    for node in world.cluster.nodes() {
        node.link().set_fault(LinkFault::default());
        node.crash();
        node.restart().await;
    }
    world
        .history
        .fault("healed: every fault removed, every node restarted");

    // Audit.
    let settled = timeout(config.settle_timeout, audit(&world)).await;
    background.abort_all();

    judge(&config, &world.history.events(), settled)
}

/// Checks the history of a finished run and summarizes it.
fn judge(
    config: &SimulationConfig,
    events: &[TimedEvent],
    settled: Result<Result<u64, String>, tokio::time::error::Elapsed>,
) -> SimulationReport {
    let mut violations = check(events);
    let stuck = match settled {
        Ok(Ok(audit_session)) => {
            violations.extend(check_audit(events, audit_session));
            None
        }
        Ok(Err(reason)) => Some(reason),
        Err(_elapsed) => Some(format!(
            "the healed cluster did not settle within {:?}",
            config.settle_timeout
        )),
    };
    let count = |is_counted: fn(&Event) -> bool| {
        events
            .iter()
            .filter(|timed| is_counted(&timed.event))
            .count()
    };
    SimulationReport {
        seed: config.seed,
        events: events.to_vec(),
        violations,
        stuck,
        committed_appends: count(|event| {
            matches!(
                event,
                Event::AppendFinished {
                    outcome: AppendOutcome::Committed { .. },
                    ..
                }
            )
        }),
        faults_injected: count(|event| matches!(event, Event::Fault { .. })),
    }
}

/// Does what every replica does on a timer: renews its registration.
async fn renew_registrations(world: Arc<World>) {
    loop {
        // A metadata outage makes this fail; the next round tries again.
        drop(world.cluster.renew_registrations().await);
        sleep(REGISTRATION_INTERVAL).await;
    }
}

async fn run_controller(world: Arc<World>) {
    let controller = Controller::new(
        world.cluster.metadata().clone(),
        world.cluster.connector(),
        world.cluster.clock(),
        ControllerSettings {
            pass_interval: Duration::from_millis(500),
            leadership_duration: Duration::from_secs(5),
            placement: PlacementPolicy {
                liveness_window: REGISTRATION_INTERVAL * 3,
                ..PlacementPolicy::default()
            },
            request_timeout: Duration::from_secs(2),
            replica_lost_after: Duration::from_secs(30),
            ..ControllerSettings::default()
        },
    );
    controller.run().await;
}

/// A replica whose disk refuses a flush exits (design, section 7). This
/// plays the part of the process supervisor: it notices, and restarts the
/// replica a little later.
async fn stop_replicas_whose_disk_failed(world: Arc<World>, mut random: DeterministicRandom) {
    loop {
        sleep(Duration::from_millis(100)).await;
        for (node_number, node) in world.cluster.nodes().iter().enumerate() {
            if node.is_running() && node.has_failed_write() {
                world
                    .history
                    .fault(format!("node {node_number} exits after a failed flush"));
                node.crash();
                let restart_delay = Duration::from_millis(random.between(100, 5_000));
                let world = Arc::clone(&world);
                tokio::spawn(async move {
                    sleep(restart_delay).await;
                    world.cluster.nodes()[node_number].restart().await;
                });
            }
        }
    }
}

/// One compute node: while it is the designated writer it keeps a writer
/// open and appends records; when it is told to crash it drops everything
/// and starts over.
async fn run_compute_node(
    world: Arc<World>,
    compute_node: usize,
    mut random: DeterministicRandom,
    mut designated_writer: watch::Receiver<usize>,
    mut crashes: watch::Receiver<u64>,
) {
    let client = world.client(&format!("compute-{compute_node}"));
    let mut records_appended = 0_u64;
    loop {
        // Only the designated node tries to become the writer. A node that
        // loses the designation is not told: it finds out by being fenced.
        while *designated_writer.borrow_and_update() != compute_node {
            if designated_writer.changed().await.is_err() {
                return;
            }
        }
        let policy = if random.chance(1, 2) {
            TakeoverPolicy::Force
        } else {
            TakeoverPolicy::WaitForLeaseExpiry
        };
        let writer = match client.open_writer(STREAM, policy).await {
            Ok(writer) => Arc::new(writer),
            Err(_open_error) => {
                sleep(Duration::from_millis(random.between(100, 1_000))).await;
                continue;
            }
        };
        let epoch = writer.epoch();
        crashes.borrow_and_update();

        let mut appends: JoinSet<bool> = JoinSet::new();
        let was_crashed = loop {
            while appends.len() < APPENDS_IN_FLIGHT {
                records_appended += 1;
                let payload =
                    Bytes::from(format!("compute-{compute_node}-record-{records_appended}"));
                let operation = world.history.append_invoked(payload.clone());
                let writer = Arc::clone(&writer);
                let history = world.history.clone();
                appends.spawn(async move {
                    let (outcome, can_continue) = match writer.append(payload).await {
                        Ok(sequence) => (AppendOutcome::Committed { sequence, epoch }, true),
                        Err(AppendError::Closed | AppendError::Rejected(_)) => {
                            (AppendOutcome::NotAppended, false)
                        }
                        Err(AppendError::Fenced | AppendError::Indeterminate { .. }) => {
                            (AppendOutcome::Unknown, false)
                        }
                    };
                    history.append_finished(operation, outcome);
                    can_continue
                });
            }
            tokio::select! {
                biased;
                () = next_crash(&mut crashes) => break true,
                Some(finished) = appends.join_next() => {
                    if !finished.unwrap_or(false) {
                        break false;
                    }
                }
            }
            sleep(Duration::from_millis(random.between(0, 8))).await;
        };

        // A crashed node forgets its appends in flight: nobody ever learns
        // their outcome, so the history leaves them unfinished.
        appends.abort_all();
        while appends.join_next().await.is_some() {}
        drop(writer);
        let restart_delay = if was_crashed {
            random.between(100, 5_000)
        } else {
            random.between(10, 300)
        };
        sleep(Duration::from_millis(restart_delay)).await;
    }
}

/// Resolves when the compute node is told to crash. If nobody is left to
/// tell it, it never resolves.
async fn next_crash(crashes: &mut watch::Receiver<u64>) {
    if crashes.changed().await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// One reader: follows the stream in sessions, sometimes starting again
/// from an earlier position to exercise reads of sealed segments.
async fn run_reader(world: Arc<World>, mut random: DeterministicRandom) {
    let reader = world.client("reader").open_reader(STREAM);
    let mut read_through = SequenceNumber::ZERO;
    loop {
        let from_sequence = if random.chance(1, 4) {
            SequenceNumber::new(random.between(1, read_through.get().max(1)))
        } else {
            read_through.next()
        };
        let Ok(mut following) = reader.follow_from(from_sequence).await else {
            sleep(Duration::from_millis(random.between(100, 1_000))).await;
            continue;
        };
        let session = world.history.reader_started(from_sequence);
        let session_ends_at = Instant::now() + Duration::from_secs(random.between(5, 40));
        while let Ok(Some(Ok(record))) = timeout_at(session_ends_at, following.next()).await {
            world
                .history
                .record_read(session, record.sequence, record.payload);
            read_through = read_through.max(record.sequence);
        }
    }
}

/// Injects faults at random moments and undoes each one after a random
/// time, until it is stopped.
async fn run_nemesis(
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
    let mean_gap_millis =
        u64::try_from(config.mean_time_between_faults.as_millis()).unwrap_or(u64::MAX);
    let mut repairs = JoinSet::new();
    loop {
        sleep(Duration::from_millis(
            random.between(0, mean_gap_millis.saturating_mul(2)),
        ))
        .await;
        let kind = *random.choose(&kinds).expect("some fault is enabled");
        let node_number = random.index(config.node_count);
        let lasts_for = Duration::from_millis(random.between(100, 12_000));
        let describe = |what: &str| format!("node {node_number} {what} for {lasts_for:?}");
        let node = &world.cluster.nodes()[node_number];
        match kind {
            FaultKind::NodeCrash | FaultKind::TornCrash => {
                if !node.is_running() {
                    continue;
                }
                if kind == FaultKind::TornCrash {
                    world.history.fault(describe("crashes with torn writes"));
                    let mut torn = random.fork();
                    node.crash_keeping(|path, durable, visible| {
                        tear_unflushed_writes(path, durable, visible, &mut torn)
                    });
                } else {
                    world.history.fault(describe("crashes"));
                    node.crash();
                }
                let world = Arc::clone(&world);
                repairs.spawn(async move {
                    sleep(lasts_for).await;
                    world.cluster.nodes()[node_number].restart().await;
                });
            }
            FaultKind::Partition | FaultKind::SlowLink | FaultKind::LostResponses => {
                let fault = match kind {
                    FaultKind::Partition => LinkFault {
                        is_cut: true,
                        ..LinkFault::default()
                    },
                    FaultKind::SlowLink => LinkFault {
                        delay: Duration::from_millis(random.between(1, 1_500)),
                        ..LinkFault::default()
                    },
                    _ => LinkFault {
                        loses_responses: true,
                        ..LinkFault::default()
                    },
                };
                world
                    .history
                    .fault(describe(&format!("link gets {fault:?}")));
                node.link().set_fault(fault);
                let world = Arc::clone(&world);
                repairs.spawn(async move {
                    sleep(lasts_for).await;
                    world.cluster.nodes()[node_number]
                        .link()
                        .set_fault(LinkFault::default());
                });
            }
            FaultKind::FlushFailure => {
                world.history.fault(describe("disk fails its next flush"));
                node.disk().fail_flushes(true);
            }
            FaultKind::MetadataOutage => {
                world
                    .history
                    .fault(format!("metadata store is unreachable for {lasts_for:?}"));
                world.cluster.key_value_store().set_unavailable(true);
                let world = Arc::clone(&world);
                repairs.spawn(async move {
                    sleep(lasts_for).await;
                    world.cluster.key_value_store().set_unavailable(false);
                });
            }
            FaultKind::WriterCrash => {
                let compute_node = *designated_writer.borrow();
                world
                    .history
                    .fault(format!("compute node {compute_node} crashes"));
                crash_signals[compute_node].send_modify(|crash_count| *crash_count += 1);
            }
            FaultKind::WriterSwitch => {
                let next_writer = (*designated_writer.borrow() + 1) % config.compute_node_count;
                world.history.fault(format!(
                    "compute node {next_writer} takes the stream from a writer still running"
                ));
                designated_writer.send_replace(next_writer);
            }
        }
    }
}

/// Decides what a torn crash leaves in one file: everything that was
/// flushed, plus a random-length part of what was written after it, whose
/// last byte is damaged. State files are replaced atomically and never
/// torn.
fn tear_unflushed_writes(
    path: &str,
    durable: &[u8],
    visible: &[u8],
    random: &mut DeterministicRandom,
) -> Vec<u8> {
    let is_segment_file = path.rsplit('.').next() == Some("seg");
    let longest = durable.len().max(visible.len());
    let is_unflushed = |index: &usize| durable.get(*index) != visible.get(*index);
    let (true, Some(first_unflushed_byte)) = (is_segment_file, (0..longest).find(is_unflushed))
    else {
        return durable.to_vec();
    };
    let last_unflushed_byte = (0..longest)
        .rfind(is_unflushed)
        .unwrap_or(first_unflushed_byte);
    let torn_at =
        first_unflushed_byte + random.index(last_unflushed_byte - first_unflushed_byte + 1);

    let written_byte = |index: usize| visible.get(index).copied().unwrap_or(0);
    let mut survived = durable.to_vec();
    if survived.len() <= torn_at {
        survived.resize(torn_at + 1, 0);
    }
    for (index, survived_byte) in survived
        .iter_mut()
        .enumerate()
        .take(torn_at)
        .skip(first_unflushed_byte)
    {
        *survived_byte = written_byte(index);
    }
    survived[torn_at] = !written_byte(torn_at);
    survived
}

/// After healing: a fresh writer commits one last record, then the whole
/// stream is read back. Returns the session of that read, or why the
/// cluster could not do it.
async fn audit(world: &World) -> Result<u64, String> {
    let client = world.client("auditor");
    let mut attempt = 0_u64;
    let (final_sequence, _final_writer) = loop {
        attempt += 1;
        if let Ok(writer) = client.open_writer(STREAM, TakeoverPolicy::Force).await {
            let payload = Bytes::from(format!("final-record-{attempt}"));
            let operation = world.history.append_invoked(payload.clone());
            let appended = timeout(Duration::from_secs(20), writer.append(payload)).await;
            match appended {
                Ok(Ok(sequence)) => {
                    world.history.append_finished(
                        operation,
                        AppendOutcome::Committed {
                            sequence,
                            epoch: writer.epoch(),
                        },
                    );
                    // The writer stays open until the audit read is done,
                    // so its heartbeats keep the durable sequence current.
                    break (sequence, writer);
                }
                Ok(Err(_)) | Err(_) => {
                    world
                        .history
                        .append_finished(operation, AppendOutcome::Unknown);
                }
            }
        }
        sleep(Duration::from_millis(500)).await;
    };

    let reader = client.open_reader(STREAM);
    loop {
        // Whatever was truncated can no longer be read; the audit covers
        // the rest.
        if let Ok(low_water_mark) = reader.low_water_mark().await
            && let Ok(whole_stream) = SequenceRange::new(low_water_mark, final_sequence)
            && let Ok(records) = reader.read_range(whole_stream).await
            && records.last().map(|record| record.sequence) == Some(final_sequence)
        {
            let session = world.history.reader_started(low_water_mark);
            for record in records {
                world
                    .history
                    .record_read(session, record.sequence, record.payload);
            }
            return Ok(session);
        }
        sleep(Duration::from_millis(500)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn torn(durable: &[u8], visible: &[u8], seed: u64) -> Vec<u8> {
        tear_unflushed_writes(
            "7/3.seg",
            durable,
            visible,
            &mut DeterministicRandom::from_seed(seed),
        )
    }

    #[test]
    fn tearing_never_changes_what_was_flushed() {
        let durable = b"flushed-\0\0\0\0".to_vec();
        let visible = b"flushed-tail".to_vec();
        for seed in 0..50 {
            assert_eq!(
                torn(&durable, &visible, seed)[..8],
                durable[..8],
                "seed {seed}"
            );
        }
    }

    #[test]
    fn tearing_damages_the_unflushed_region() {
        let durable = b"flushed-\0\0\0\0".to_vec();
        let visible = b"flushed-tail".to_vec();
        for seed in 0..50 {
            let survived = torn(&durable, &visible, seed);
            assert!(
                survived != durable && survived != visible,
                "seed {seed} left an intact file"
            );
        }
    }

    #[test]
    fn tearing_handles_writes_past_the_flushed_length() {
        let durable = b"flushed-".to_vec();
        let visible = b"flushed-tail".to_vec();
        for seed in 0..50 {
            let survived = torn(&durable, &visible, seed);
            assert!(
                survived.len() > durable.len() && survived[..8] == durable[..],
                "seed {seed}"
            );
        }
    }

    #[test]
    fn file_without_unflushed_writes_is_left_alone() {
        let flushed = b"everything was flushed".to_vec();
        assert_eq!(torn(&flushed, &flushed, 1), flushed);
    }

    #[test]
    fn state_files_are_never_torn() {
        let survived = tear_unflushed_writes(
            "7/3.state",
            b"old-state",
            b"new-state",
            &mut DeterministicRandom::from_seed(1),
        );
        assert_eq!(survived, b"old-state");
    }

    #[test]
    fn fault_mix_without_faults_enables_nothing() {
        assert_eq!(FaultMix::NONE.enabled_kinds(), Vec::new());
    }

    #[test]
    fn full_fault_mix_enables_every_kind() {
        assert_eq!(FaultMix::ALL.enabled_kinds().len(), 9);
    }
}
