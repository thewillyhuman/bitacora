//! The controller's settings.
//!
//! # Why this file exists
//!
//! The controller makes its decisions from a handful of numbers: how often it looks, how long a replica may be silent
//! before its copies are given up, how long a segment waits before it is uploaded. An operator sets them, a test sets
//! them much shorter, and every step of a pass reads them. They are kept together here, each with what it means and
//! why its default is what it is, so that nobody has to find them scattered over the steps.
//!
//! # What it contains
//!
//! - [`ControllerSettings`]: the settings, with the defaults of the design.
//! - The `DEFAULT_*` constants: each default and the reason for it.
//!
//! # How it works
//!
//! The settings are plain data, copied into the controller when it is built and never changed afterwards. A process
//! that hosts a controller builds them from its configuration; `bita-controller` does so from its configuration
//! file.
//!
//! # Invariants
//!
//! - The settings are not validated here: every value is a duration or a count, and every combination is safe, if
//!   not always sensible. The one combination with a rule is noted on the field: `replica_lost_after` must be much
//!   longer than the placement policy's liveness window, or a restart triggers re-replication.
//!
//! # See also
//!
//! - `bitacora-meta/src/placement.rs`: [`PlacementPolicy`], the part of the settings that placement uses.
//! - `bita-controller/example.toml`: the same settings as an operator writes them.

use std::time::Duration;

use bitacora_meta::PlacementPolicy;

/// The name a controller records with its leadership when the process that hosts it gives none.
const DEFAULT_IDENTITY: &str = "bitacora-controller";

/// How long the controller rests between two passes. Short enough that a consumed standby is replaced before the
/// writer rolls again, long enough that an idle cluster costs the metadata store next to nothing.
const DEFAULT_PASS_INTERVAL: Duration = Duration::from_secs(1);

/// How long leadership lasts without renewal: ten passes. A leader that misses that many is taken for gone.
const DEFAULT_LEADERSHIP_DURATION: Duration = Duration::from_secs(10);

/// How long one request to a replica or to the object store may take. Every request the controller makes is small;
/// one that takes longer than this is not going to answer.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a replica may be silent before its copies are given up. Longer than a restart or an upgrade of a node
/// takes, so that neither moves data.
const DEFAULT_REPLICA_LOST_AFTER: Duration = Duration::from_mins(5);

/// How long rebuilding one copy of a sealed segment may take. A segment is up to a gigabyte, and repair is rate
/// limited on the replica.
const DEFAULT_REPAIR_TIMEOUT: Duration = Duration::from_mins(30);

/// How many copies one pass rebuilds at most.
const DEFAULT_MAX_REPAIRS_PER_PASS: usize = 8;

/// How long after its seal a segment is uploaded (design, section 10, "Tiering").
const DEFAULT_TIERING_DELAY: Duration = Duration::from_mins(15);

/// How long after the upload the replicas keep their copies (design, section 10, "Tiering").
const DEFAULT_HOT_READ_WINDOW: Duration = Duration::from_hours(24);

/// How long uploading one segment may take. A segment is up to a gigabyte, and the replica reads the object back
/// before it answers.
const DEFAULT_UPLOAD_TIMEOUT: Duration = Duration::from_mins(30);

/// How many segments one pass uploads at most.
const DEFAULT_MAX_UPLOADS_PER_PASS: usize = 4;

/// How often the metadata store is copied to the object store (design, section 14, "Routine procedures").
const DEFAULT_METADATA_SNAPSHOT_INTERVAL: Duration = Duration::from_mins(5);

/// How many metadata snapshots are kept: one day of them at the default interval.
const DEFAULT_METADATA_SNAPSHOTS_KEPT: usize = 288;

/// How the controller paces itself and judges replicas.
///
/// Build it from [`ControllerSettings::default`] and change what the deployment needs. Tests and the simulator
/// shorten the durations so that everything happens within seconds.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use bitacora_controller::ControllerSettings;
///
/// let settings = ControllerSettings {
///     identity: "controller-a".to_owned(),
///     tiering_delay: Duration::from_mins(5),
///     ..ControllerSettings::default()
/// };
///
/// assert_eq!(settings.hot_read_window, Duration::from_hours(24));
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControllerSettings {
    /// A name for this controller instance, recorded with its leadership. Give every instance of a cluster its own:
    /// two instances with one name would both believe they hold the leadership the other took.
    pub identity: String,
    /// How long the controller rests between two passes.
    pub pass_interval: Duration,
    /// How long leadership lasts without renewal. Another instance takes over once it has lapsed.
    pub leadership_duration: Duration,
    /// Which replicas may receive new segments: how recently they must have registered, and how full they may be.
    pub placement: PlacementPolicy,
    /// How long one request to a replica or to the object store may take, other than a repair or an upload.
    pub request_timeout: Duration,
    /// How long a replica may be silent before its copies are given up and re-created elsewhere. Much longer than
    /// the liveness window of `placement`, so that a restart or an upgrade does not trigger re-replication.
    pub replica_lost_after: Duration,
    /// How long rebuilding one copy of a sealed segment may take.
    pub repair_timeout: Duration,
    /// How many copies one pass rebuilds at most, so that a pass stays short and placement keeps up while a failed
    /// node is re-replicated.
    pub max_repairs_per_pass: usize,
    /// How long after its seal a fully replicated segment is uploaded to the object store.
    pub tiering_delay: Duration,
    /// How long after the upload the replicas keep their copies, so that recent data is read from local disks.
    pub hot_read_window: Duration,
    /// How long uploading one segment may take.
    pub upload_timeout: Duration,
    /// How many segments one pass uploads at most, so that a pass stays short.
    pub max_uploads_per_pass: usize,
    /// How often the whole metadata store is copied to the object store.
    pub metadata_snapshot_interval: Duration,
    /// How many of the newest metadata snapshots are kept; older ones are deleted. At least one is always kept.
    pub metadata_snapshots_kept: usize,
}

impl Default for ControllerSettings {
    /// The settings of the design: a pass every second, fifteen minutes before a segment is uploaded, a hot-read
    /// window of a day, a metadata snapshot every five minutes.
    fn default() -> Self {
        Self {
            identity: DEFAULT_IDENTITY.to_owned(),
            pass_interval: DEFAULT_PASS_INTERVAL,
            leadership_duration: DEFAULT_LEADERSHIP_DURATION,
            placement: PlacementPolicy::default(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            replica_lost_after: DEFAULT_REPLICA_LOST_AFTER,
            repair_timeout: DEFAULT_REPAIR_TIMEOUT,
            max_repairs_per_pass: DEFAULT_MAX_REPAIRS_PER_PASS,
            tiering_delay: DEFAULT_TIERING_DELAY,
            hot_read_window: DEFAULT_HOT_READ_WINDOW,
            upload_timeout: DEFAULT_UPLOAD_TIMEOUT,
            max_uploads_per_pass: DEFAULT_MAX_UPLOADS_PER_PASS,
            metadata_snapshot_interval: DEFAULT_METADATA_SNAPSHOT_INTERVAL,
            metadata_snapshots_kept: DEFAULT_METADATA_SNAPSHOTS_KEPT,
        }
    }
}

#[cfg(test)]
#[path = "settings_test.rs"]
mod tests;
