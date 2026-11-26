//! What one simulated run looks like: its size, its length and the faults it injects.
//!
//! # Why this file exists
//!
//! A run is a function of its configuration: the same configuration always produces the same history. That makes the
//! configuration the whole interface of the simulator. The fault matrix runs the standard configuration over many
//! seeds; a test that wants to know which kind of fault breaks something runs it with one kind switched on; a person
//! replaying a failure passes the seed. This file is that interface, kept apart from the machinery that acts on it.
//!
//! # What it contains
//!
//! - [`SimulationConfig`]: the seed, the size of the cluster, how long faults are injected and which.
//! - [`FaultMix`]: one switch per kind of fault (design, section 15, "Faults injected").
//! - [`FaultKind`]: the kinds, as the nemesis draws them.
//!
//! # How it works
//!
//! The configuration is plain data with public fields. [`SimulationConfig::with_seed`] is the standard run, and struct
//! update syntax changes what a test wants different. The nemesis asks the mix for the list of enabled kinds once, and
//! then draws from that list with the run's random source.
//!
//! # Invariants
//!
//! - [`FaultMix::enabled_kinds`] lists the kinds in the order of the fields of [`FaultMix`]. The nemesis picks a kind
//!   by its position in that list, so the order is part of what a seed means: changing it changes every run.
//! - The standard run of [`SimulationConfig::with_seed`] is what the regression seeds were found with. Changing a
//!   number in it changes what every seed does, and a seed that once found a bug may no longer find it.
//!
//! # See also
//!
//! - `simulation.rs`: runs a configuration.
//! - `nemesis.rs`: injects the faults the mix enables.
//! - `kv_workload.rs`: [`InjectedBug`], the deliberate mistakes a configuration can ask for.

use std::time::Duration;

use crate::kv_workload::InjectedBug;

/// How many storage nodes the standard run has: two more than the replication factor, so that a segment has somewhere
/// to be repaired to while a node is away.
const STANDARD_NODE_COUNT: usize = 5;

/// How many compute nodes compete to be the writer in the standard run: enough for one to take the stream from the
/// other.
const STANDARD_COMPUTE_NODE_COUNT: usize = 2;

/// How many readers follow the stream in the standard run: enough for readers to be compared with each other.
const STANDARD_READER_COUNT: usize = 2;

/// How long the standard run injects faults, in simulated time.
const STANDARD_CHAOS_DURATION: Duration = Duration::from_mins(1);

/// How long the healed cluster of the standard run gets to settle, in simulated time. Generous: a correct cluster needs
/// a small part of it, and a cluster that needs more is stuck.
const STANDARD_SETTLE_TIMEOUT: Duration = Duration::from_mins(5);

/// The average simulated time between two faults of the standard run. Short enough that faults overlap: most bugs need
/// two things to go wrong at once.
const STANDARD_MEAN_TIME_BETWEEN_FAULTS: Duration = Duration::from_millis(700);

/// Which kinds of fault a run injects (design, section 15, "Faults injected").
///
/// Every field is an independent switch. Start from [`FaultMix::NONE`] or [`FaultMix::ALL`] and change what the run
/// should differ in.
///
/// # Examples
///
/// ```
/// use bitacora_sim::FaultMix;
///
/// let only_partitions = FaultMix {
///     partitions: true,
///     ..FaultMix::NONE
/// };
///
/// assert_ne!(only_partitions, FaultMix::NONE);
/// ```
// Every field is an independent switch; there is no hidden state machine.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FaultMix {
    /// Replica processes are killed and later restarted.
    pub node_crashes: bool,
    /// A killed replica's disk keeps part of what was not flushed, with a damaged last byte: a torn write.
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
    /// Another compute node takes the stream while the old writer is still running, as after a pause that outlived its
    /// lease.
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

    /// Lists the kinds of fault the mix enables.
    ///
    /// # Returns
    ///
    /// One kind per switch that is on, in the order of the fields of [`FaultMix`]. The nemesis draws a kind by its
    /// position in this list, so the order is part of what a seed means. Empty for [`FaultMix::NONE`].
    pub(crate) fn enabled_kinds(self) -> Vec<FaultKind> {
        let switches = [
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
        ];
        let mut enabled_kinds = Vec::new();
        for (is_enabled, kind) in switches {
            if is_enabled {
                enabled_kinds.push(kind);
            }
        }
        enabled_kinds
    }
}

/// One kind of fault, as the nemesis draws it. Each corresponds to one switch of [`FaultMix`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FaultKind {
    /// A replica process is killed and loses what it had not flushed.
    NodeCrash,
    /// A replica process is killed and its disk keeps a damaged part of what was not flushed.
    TornCrash,
    /// The link to a node is cut.
    Partition,
    /// The link to a node delays every request.
    SlowLink,
    /// The link to a node delivers requests and loses the answers.
    LostResponses,
    /// A node's disk refuses its next flush.
    FlushFailure,
    /// The metadata store is unreachable.
    MetadataOutage,
    /// The writing compute node is killed.
    WriterCrash,
    /// Another compute node is told to take the stream.
    WriterSwitch,
    /// A bit flips in a stored segment file.
    SilentCorruption,
    /// A node loses its disk for good.
    DiskLoss,
    /// The object store is unreachable.
    ObjectStoreOutage,
}

/// What one simulated run looks like.
///
/// The same configuration always produces the same report. Start from [`SimulationConfig::with_seed`] and change what
/// the run should differ in.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use bitacora_sim::{FaultMix, SimulationConfig};
///
/// let short_and_calm = SimulationConfig {
///     faults: FaultMix::NONE,
///     chaos_duration: Duration::from_secs(20),
///     ..SimulationConfig::with_seed(1)
/// };
///
/// assert_eq!(short_and_calm.node_count, 5);
/// ```
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
    /// How long the healed cluster gets to accept a final record and serve the whole stream back, in simulated time.
    pub settle_timeout: Duration,
    /// Which faults are injected.
    pub faults: FaultMix,
    /// The average simulated time between two faults.
    pub mean_time_between_faults: Duration,
    /// A mistake the compute nodes make on purpose. A correct run has none; the simulator's own tests set one to show
    /// it is caught.
    pub injected_bug: InjectedBug,
}

impl SimulationConfig {
    /// Builds the standard run for `seed`: five storage nodes, two compute nodes, two readers, a minute of every kind
    /// of fault.
    ///
    /// # Arguments
    ///
    /// * `seed` - Determines every random choice of the run. Any number is a valid seed.
    ///
    /// # Returns
    ///
    /// The configuration the fault matrix runs, and the one the regression seeds were found with.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_sim::{FaultMix, SimulationConfig};
    ///
    /// let config = SimulationConfig::with_seed(42);
    ///
    /// assert_eq!(config.seed, 42);
    /// assert_eq!(config.faults, FaultMix::ALL);
    /// ```
    #[must_use]
    pub const fn with_seed(seed: u64) -> Self {
        Self {
            seed,
            node_count: STANDARD_NODE_COUNT,
            compute_node_count: STANDARD_COMPUTE_NODE_COUNT,
            reader_count: STANDARD_READER_COUNT,
            chaos_duration: STANDARD_CHAOS_DURATION,
            settle_timeout: STANDARD_SETTLE_TIMEOUT,
            faults: FaultMix::ALL,
            mean_time_between_faults: STANDARD_MEAN_TIME_BETWEEN_FAULTS,
            injected_bug: InjectedBug::None,
        }
    }
}

#[cfg(test)]
#[path = "config_test.rs"]
mod tests;
