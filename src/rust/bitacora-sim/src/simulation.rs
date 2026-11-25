//! One simulated run of a whole cluster (design, section 15, layer 2).
//!
//! Replicas, the controller, compute nodes and readers all run in one thread
//! on a simulated clock, network and disk, driven by a seed. The compute
//! nodes serve a key-value store on the stream (see `kv_workload.rs`), so
//! the materializer framework runs under the same faults as the log. A run
//! has three phases: chaos, during which a nemesis injects faults while
//! clients work; healing, which removes every fault; and an audit, which
//! writes a final record, reads the whole stream back and reads every key.
//! The history of the run is then judged by the log checker and by the
//! linearizability check.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bitacora_artifact::ArtifactStore;
use bitacora_client::{
    BitacoraClient, ClientSettings, ReadError, ReaderSettings, TakeoverPolicy, WriterSettings,
};
use bitacora_controller::{Controller, ControllerSettings};
use bitacora_kv::{KV_MATERIALIZER, KvDependencies, KvStore};
use bitacora_meta::{CopyState, PlacementPolicy, RetentionPolicy, SegmentRecord, StreamRecord};
use bitacora_segment::Disk;
use bitacora_testkit::{Cluster, LinkFault};
use bitacora_types::replica_api::ReplicaApi;
use bitacora_types::{DeterministicRandom, ReplicationSettings, SequenceNumber, SequenceRange};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep, timeout, timeout_at};

use crate::checker::{Violation, check, check_audit};
use crate::history::{AppendOutcome, Event, History, TimedEvent};
use crate::kv_workload::{
    ComputeNode, InjectedBug, commit_final_record, declare_fillers, read_every_key, store_settings,
};
use crate::linearizability::check_linearizability;

/// The stream every simulation writes to.
const STREAM: &str = "simulated";

/// Where tiered segments live in the object store. The key-value store's
/// artifacts are under another prefix.
const TIERED_SEGMENTS_KEY_PREFIX: &str = "streams/";

/// How often replicas renew their registration.
const REGISTRATION_INTERVAL: Duration = Duration::from_secs(2);

/// How often replicas scrub their sealed segments. Weekly in production;
/// here often enough to find injected corruption within a run.
const SCRUB_INTERVAL: Duration = Duration::from_secs(3);

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
    /// One node's disk silently flips bits in stored segments.
    pub silent_corruption: bool,
    /// One node loses its disk for good, once, and returns empty.
    pub disk_loss: bool,
    /// The object store is unreachable for a while.
    pub object_store_outages: bool,
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
        silent_corruption: false,
        disk_loss: false,
        object_store_outages: false,
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
        silent_corruption: true,
        disk_loss: true,
        object_store_outages: true,
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
            (self.silent_corruption, FaultKind::SilentCorruption),
            (self.disk_loss, FaultKind::DiskLoss),
            (self.object_store_outages, FaultKind::ObjectStoreOutage),
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
    SilentCorruption,
    DiskLoss,
    ObjectStoreOutage,
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
    /// A mistake the compute nodes make on purpose. A correct run has
    /// none; the simulator's own tests set one to show it is caught.
    pub injected_bug: InjectedBug,
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
            injected_bug: InjectedBug::None,
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
    /// How many range reads of tiered segments the object store served:
    /// whether, and how much, the run read segments that had left the
    /// replicas. Reads of the key-value store's own artifacts do not count.
    pub tiered_segment_reads: u64,
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
    /// What the audit is waiting for, to say so if it never happens.
    audit_step: Mutex<&'static str>,
    /// The one node whose disk may corrupt data or be lost. Keeping these
    /// faults on one node keeps them within what replication is meant to
    /// survive: at most one copy of any record is destroyed.
    unlucky_node: usize,
}

impl World {
    /// The cluster's object store, as the clients, the controller and the
    /// key-value stores of the run are given it. Sealed segments are tiered
    /// to it, and the stores keep their layers and manifests in it.
    fn artifact_store(&self) -> Arc<dyn ArtifactStore> {
        Arc::new(self.cluster.artifact_store().clone())
    }

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
        .with_artifact_store(self.artifact_store())
    }

    /// What a key-value store opened by `identity` is built from.
    fn store_dependencies(&self, identity: &str) -> KvDependencies {
        KvDependencies {
            client: self.client(identity),
            metadata: self.cluster.metadata().clone(),
            artifact_store: self.artifact_store(),
        }
    }
}

async fn simulate(config: SimulationConfig) -> SimulationReport {
    let mut random = DeterministicRandom::from_seed(config.seed);
    let world = Arc::new(World {
        cluster: Cluster::start(config.node_count).await,
        history: History::new(),
        audit_step: Mutex::new("starting"),
        unlucky_node: random.index(config.node_count),
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
    KvStore::initialize(world.cluster.metadata(), STREAM)
        .await
        .expect("a fresh stream accepts a materializer");
    declare_fillers(&world.history);

    let mut background = JoinSet::new();
    background.spawn(renew_registrations(Arc::clone(&world)));
    background.spawn(run_controller(Arc::clone(&world)));
    background.spawn(stop_replicas_whose_disk_failed(
        Arc::clone(&world),
        random.fork(),
    ));
    background.spawn(scrub_replicas(Arc::clone(&world)));

    let (designated_writer, _) = watch::channel(0_usize);
    let mut crash_signals = Vec::new();
    let mut clients = JoinSet::new();
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
        clients.spawn(node.run(designated_writer.subscribe(), crashes));
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
    world.cluster.artifact_store().set_unavailable(false);
    for node in world.cluster.nodes() {
        node.link().set_fault(LinkFault::default());
        node.crash();
        node.restart().await;
    }
    world
        .history
        .fault("healed: every fault removed, every node restarted");

    // Audit.
    let settled = match timeout(config.settle_timeout, audit(&world)).await {
        Ok(audited) => audited,
        Err(_elapsed) => Err(describe_stuck_cluster(&world, config.settle_timeout).await),
    };
    background.abort_all();

    let mut report = judge(&config, &world.history.events(), settled);
    report.tiered_segment_reads = world
        .cluster
        .artifact_store()
        .served_range_reads(TIERED_SEGMENTS_KEY_PREFIX);
    report
}

/// Checks the history of a finished run and summarizes it.
fn judge(
    config: &SimulationConfig,
    events: &[TimedEvent],
    settled: Result<u64, String>,
) -> SimulationReport {
    let mut violations = check(events);
    violations.extend(check_linearizability(events));
    let stuck = match settled {
        Ok(audit_session) => {
            violations.extend(check_audit(events, audit_session));
            None
        }
        Err(reason) => Some(reason),
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
        // Not part of the history; the caller fills it in.
        tiered_segment_reads: 0,
    }
}

/// Says what the healed cluster was still waiting for when time ran out.
async fn describe_stuck_cluster(world: &World, waited: Duration) -> String {
    use std::fmt::Write as _;
    let step = *world
        .audit_step
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let mut description =
        format!("the healed cluster did not settle within {waited:?}; it was {step}");
    if let Ok(stream) = world.cluster.metadata().get_stream(STREAM).await {
        let _ = write!(
            description,
            "; epoch {}, low-water mark {}, standby {:?}, segments:",
            stream.epoch, stream.low_water_mark, stream.standby_replicas
        );
        for segment in &stream.segments {
            let copies: Vec<String> = segment
                .copies
                .iter()
                .map(|copy| format!("{}={:?}", copy.replica_id, copy.state))
                .collect();
            let _ = write!(
                description,
                " [{} from {} sealed {:?}: {}]",
                segment.segment_id,
                segment.first_sequence,
                segment.sealed_end(),
                copies.join(" ")
            );
        }
    }
    for node in world.cluster.nodes() {
        let held = match node.replica() {
            Some(replica) => replica.list_segments().await.map(|statuses| {
                statuses
                    .iter()
                    .map(|status| {
                        format!(
                            "{}:end={:?},fenced={},sealed={:?}{}",
                            status.address.segment_id,
                            status.local_end.map(SequenceNumber::get),
                            status.fenced_epoch,
                            status.sealed_end.map(SequenceNumber::get),
                            if status.is_damaged { ",DAMAGED" } else { "" }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" ")
            }),
            None => Ok("down".to_owned()),
        };
        let _ = write!(description, "; {} holds {held:?}", node.replica_id());
    }
    description
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
            // Short enough that segments are uploaded, and their local
            // copies released, before truncation deletes them: the run
            // must read from the object store too.
            tiering_delay: Duration::from_secs(1),
            hot_read_window: Duration::from_secs(2),
            upload_timeout: Duration::from_secs(5),
            ..ControllerSettings::default()
        },
    )
    .with_artifact_store(world.artifact_store());
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

/// Does what every replica does on a schedule: rereads its sealed segments
/// and marks the copies that fail a checksum.
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

/// One reader: follows the stream in sessions, sometimes starting again
/// from an earlier position to exercise reads of sealed segments.
async fn run_reader(world: Arc<World>, mut random: DeterministicRandom) {
    let reader = world.client("reader").open_reader(STREAM);
    let mut read_through = SequenceNumber::ZERO;
    // The lowest position known to be readable still.
    let mut readable_from = SequenceNumber::FIRST;
    loop {
        let resume_at = read_through.next().max(readable_from);
        let from_sequence = if random.chance(1, 4) {
            SequenceNumber::new(random.between(readable_from.get(), resume_at.get()))
        } else {
            resume_at
        };
        let Ok(mut following) = reader.follow_from(from_sequence).await else {
            sleep(Duration::from_millis(random.between(100, 1_000))).await;
            continue;
        };
        let session = world.history.reader_started(from_sequence);
        let session_ends_at = Instant::now() + Duration::from_secs(random.between(5, 40));
        loop {
            match timeout_at(session_ends_at, following.next()).await {
                Ok(Some(Ok(record))) => {
                    world
                        .history
                        .record_read(session, record.sequence, record.payload);
                    read_through = read_through.max(record.sequence);
                }
                Ok(Some(Err(ReadError::Truncated { low_water_mark }))) => {
                    readable_from = readable_from.max(low_water_mark);
                    break;
                }
                Ok(Some(Err(_)) | None) | Err(_) => break,
            }
        }
    }
}

/// Injects faults at random moments and undoes each one after a random
/// time, until it is stopped.
// One flat arm per kind of fault reads better than the same schedule spread
// over a dozen small functions.
#[allow(clippy::too_many_lines)]
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
    let mut has_lost_a_disk = false;
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
                let is_torn = kind == FaultKind::TornCrash;
                world.history.fault(describe(if is_torn {
                    "crashes with torn writes"
                } else {
                    "crashes"
                }));
                crash_node(&world, node_number, is_torn, &mut random);
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
            FaultKind::ObjectStoreOutage => {
                world
                    .history
                    .fault(format!("object store is unreachable for {lasts_for:?}"));
                world.cluster.artifact_store().set_unavailable(true);
                let world = Arc::clone(&world);
                repairs.spawn(async move {
                    sleep(lasts_for).await;
                    world.cluster.artifact_store().set_unavailable(false);
                });
            }
            FaultKind::WriterCrash => {
                let compute_node = *designated_writer.borrow();
                world
                    .history
                    .fault(format!("compute node {compute_node} crashes"));
                crash_signals[compute_node].send_modify(|crash_count| *crash_count += 1);
            }
            FaultKind::SilentCorruption => corrupt_a_stored_byte(&world, &mut random).await,
            FaultKind::DiskLoss => {
                if has_lost_a_disk {
                    continue;
                }
                has_lost_a_disk = true;
                let unlucky_node = world.unlucky_node;
                world
                    .history
                    .fault(format!("node {unlucky_node} loses its disk for good"));
                world.cluster.nodes()[unlucky_node].lose_disk();
                let world = Arc::clone(&world);
                repairs.spawn(async move {
                    sleep(lasts_for).await;
                    world.cluster.nodes()[unlucky_node].restart().await;
                });
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

/// Kills a node's replica process. A torn crash leaves part of what was
/// not flushed on its disk, damaged.
fn crash_node(world: &World, node_number: usize, is_torn: bool, random: &mut DeterministicRandom) {
    let node = &world.cluster.nodes()[node_number];
    if is_torn {
        let mut torn = random.fork();
        node.crash_keeping(|path, durable, visible| {
            tear_unflushed_writes(path, durable, visible, &mut torn)
        });
    } else {
        node.crash();
    }
}

/// Flips one bit of one byte in one segment file on the unlucky node's
/// disk, without anyone being told.
async fn corrupt_a_stored_byte(world: &World, random: &mut DeterministicRandom) {
    let disk = world.cluster.nodes()[world.unlucky_node].disk();
    let segment_files: Vec<String> = disk
        .list_files()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|path| path.rsplit('.').next() == Some("seg"))
        .collect();
    let Some(path) = random.choose(&segment_files) else {
        return;
    };
    let file_bytes = disk
        .durable_content(path)
        .map_or(0, |content| content.len());
    if file_bytes == 0 {
        return;
    }
    let offset = random.index(file_bytes);
    world.history.fault(format!(
        "node {} silently corrupts byte {offset} of {path}",
        world.unlucky_node
    ));
    disk.corrupt_byte(path, offset, 0x40);
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
    // What was written since the last flush need not be one run of bytes:
    // an append also rewrites the flushed mark in the header.
    let unflushed_bytes: Vec<usize> = (0..longest)
        .filter(|index| durable.get(*index) != visible.get(*index))
        .collect();
    if !is_segment_file || unflushed_bytes.is_empty() {
        return durable.to_vec();
    }
    let torn_position = random.index(unflushed_bytes.len());
    let torn_at = unflushed_bytes[torn_position];

    let written_byte = |index: usize| visible.get(index).copied().unwrap_or(0);
    let mut survived = durable.to_vec();
    if survived.len() <= torn_at {
        survived.resize(torn_at + 1, 0);
    }
    for index in &unflushed_bytes[..torn_position] {
        survived[*index] = written_byte(*index);
    }
    survived[torn_at] = !written_byte(torn_at);
    survived
}

/// After healing: a fresh key-value store commits one last record, the
/// whole stream is read back, and every key is read through the store.
/// Returns the session of the stream read, or why the cluster could not
/// do it.
async fn audit(world: &World) -> Result<u64, String> {
    let now_waiting_for = |step: &'static str| {
        *world
            .audit_step
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = step;
    };
    now_waiting_for("opening a store and committing a final record");
    let mut attempt = 0_u64;
    let (final_sequence, final_store) = loop {
        attempt += 1;
        let opened = KvStore::open(
            world.store_dependencies("auditor"),
            STREAM,
            store_settings(TakeoverPolicy::Force),
        )
        .await;
        if let Ok(store) = opened
            && let Some(sequence) = commit_final_record(&store, &world.history, attempt).await
        {
            // The store stays open until the audit is done, so its
            // writer's heartbeats keep the durable sequence current.
            break (sequence, store);
        }
        sleep(Duration::from_millis(500)).await;
    };

    now_waiting_for("reading every key through the store");
    while !read_every_key(&final_store, &world.history).await {
        sleep(Duration::from_millis(500)).await;
    }

    now_waiting_for("reading the whole stream back");
    let reader = world.client("auditor").open_reader(STREAM);
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
            check_truncation(world, low_water_mark).await?;
            now_waiting_for("waiting for every sealed segment to have all its copies good");
            wait_for_full_replication(world).await;
            return Ok(session);
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// I5 Safe truncation: the low-water mark must not have passed the
/// materializer's committed checkpoint.
async fn check_truncation(world: &World, low_water_mark: SequenceNumber) -> Result<(), String> {
    let metadata = world.cluster.metadata();
    let stream = metadata
        .get_stream(STREAM)
        .await
        .map_err(|meta_error| meta_error.to_string())?;
    let checkpoint = metadata
        .get_checkpoint(stream.stream_id, KV_MATERIALIZER)
        .await
        .map_err(|meta_error| meta_error.to_string())?
        .checkpoint;
    if low_water_mark > checkpoint.next() {
        world
            .history
            .truncated_past_checkpoint(checkpoint, low_water_mark);
    }
    Ok(())
}

/// Waits until the controller has brought every sealed segment back to
/// where it belongs (design, section 18, the exit gate of M3): a full set
/// of good copies, or the object store alone once its hot-read window is
/// over. It returns only then; the caller bounds the wait.
async fn wait_for_full_replication(world: &World) {
    loop {
        if let Ok(stream) = world.cluster.metadata().get_stream(STREAM).await
            && stream
                .segments
                .iter()
                .all(|segment| is_where_it_belongs(&stream, segment))
        {
            return;
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// Whether `segment` needs nothing more from the controller.
fn is_where_it_belongs(stream: &StreamRecord, segment: &SegmentRecord) -> bool {
    let Some(sealed_end) = segment.sealed_end() else {
        // An open segment is the writer's business.
        return true;
    };
    if sealed_end < stream.low_water_mark {
        // Truncated; only its deletion is pending.
        return true;
    }
    let is_released = segment.copies.is_empty()
        && segment
            .tiered
            .as_ref()
            .is_some_and(|tiered| tiered.are_local_copies_dropped);
    let has_every_good_copy = segment.copies.len() == stream.replication.replica_count()
        && segment
            .copies
            .iter()
            .all(|copy| copy.state == CopyState::Good);
    is_released || has_every_good_copy
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
    fn tearing_leaves_flushed_bytes_between_two_unflushed_writes_alone() {
        let durable = b"mark-A|flushed-data|\0\0\0\0".to_vec();
        let visible = b"mark-B|flushed-data|tail".to_vec();
        for seed in 0..50 {
            let survived = torn(&durable, &visible, seed);
            assert_eq!(
                survived[6..20],
                durable[6..20],
                "seed {seed} damaged flushed bytes"
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
        assert_eq!(FaultMix::ALL.enabled_kinds().len(), 12);
    }
}
