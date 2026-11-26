//! The settings of a replica.
//!
//! # Why this file exists
//!
//! A replica makes a handful of choices that depend on the machine it runs on and on how the operator
//! wants it to behave under pressure: how much disk a segment reserves, when a filling disk stops taking
//! data, how hard a repair may pull. They are gathered in one plain value, so that a process reads them
//! from its configuration once, a test changes exactly the one it is about, and nothing in the replica
//! reaches for a constant that should have been a setting.
//!
//! # What it contains
//!
//! - [`ReplicaSettings`]: the settings, with the design's defaults.
//!
//! # How it works
//!
//! The value is `Copy` and has only public fields. A caller starts from [`ReplicaSettings::default`] and
//! overrides what it needs with struct update syntax. The replica never changes its settings after it is
//! opened.
//!
//! # Invariants
//!
//! - Nothing here is validated: the type holds numbers, and a process that reads them from a file checks
//!   them there. The replica behaves sensibly for any value: a threshold of 0 refuses everything, a
//!   threshold above 100 refuses nothing, a rate of 0 means no limit.
//!
//! # See also
//!
//! - `replica/admission.rs`: where the two disk thresholds are applied.
//! - `copy/repair.rs` and `upload.rs`: where the repair and upload settings are used.
//! - `bita-replica`: the daemon that fills these from its configuration file.

use std::time::Duration;

/// How much disk a new segment file reserves by default, and grows by when full: 64 MiB.
const DEFAULT_PREALLOCATED_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// The share of used disk from which a replica accepts no new segments by default (design, section 7,
/// "Capacity and admission").
const DEFAULT_REFUSE_NEW_SEGMENTS_AT_USED_PERCENT: u8 = 85;

/// The share of used disk from which a replica refuses every append by default.
const DEFAULT_REFUSE_APPENDS_AT_USED_PERCENT: u8 = 95;

/// How long a measurement of free space is reused by default: long enough that appends do not each ask
/// the file system, short enough that a filling disk is noticed.
const DEFAULT_SPACE_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

/// The longest a read waits for records by default.
const DEFAULT_LONGEST_READ_WAIT: Duration = Duration::from_secs(30);

/// The default size of one read from a peer during a repair: 1 MiB.
const DEFAULT_REPAIR_CHUNK_BYTES: usize = 1024 * 1024;

/// How many bytes per second one repair copies by default: 128 MiB.
const DEFAULT_REPAIR_BYTES_PER_SECOND: u64 = 128 * 1024 * 1024;

/// How long one read from a peer may take during a repair by default.
const DEFAULT_REPAIR_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The default size of one read while a sealed segment is uploaded: 8 MiB.
const DEFAULT_UPLOAD_CHUNK_BYTES: usize = 8 * 1024 * 1024;

/// How long one request to the object store may take by default.
const DEFAULT_OBJECT_STORE_TIMEOUT: Duration = Duration::from_mins(5);

/// How a replica uses its disk, its peers and the object store.
///
/// Build one from [`ReplicaSettings::default`] and override the fields that matter. The replica takes the
/// value when it is opened and never changes it.
///
/// # Examples
///
/// ```
/// use bitacora_replica::ReplicaSettings;
///
/// let small_segments = ReplicaSettings {
///     preallocated_segment_bytes: 4 * 1024,
///     ..ReplicaSettings::default()
/// };
///
/// assert_eq!(small_segments.refuse_appends_at_used_percent, 95);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaSettings {
    /// How much disk a new segment file reserves, and grows by when full, in bytes. Reserving ahead keeps
    /// the flush that acknowledges an append from also having to persist a change of the file's size.
    pub preallocated_segment_bytes: u64,
    /// From this share of used disk on, in percent, the replica accepts no new segments. Appends to
    /// segments it already holds go on.
    pub refuse_new_segments_at_used_percent: u8,
    /// From this share of used disk on, in percent, the replica refuses every append, which makes writers
    /// roll to another replica set.
    pub refuse_appends_at_used_percent: u8,
    /// How long a measurement of the disk's free space is reused before the disk is asked again.
    pub space_refresh_interval: Duration,
    /// The longest a read may wait for records to become readable, whatever the reader asked for.
    pub longest_read_wait: Duration,
    /// The target size, in bytes, of one read from a peer when a copy is repaired.
    pub repair_chunk_bytes: usize,
    /// How many bytes per second one repair may copy, so that it cannot starve appends of the disk and
    /// the network. Zero means no limit.
    pub repair_bytes_per_second: u64,
    /// How long one read from a peer may take during a repair before the repair gives up.
    pub repair_request_timeout: Duration,
    /// The target size, in bytes, of one read of a local copy, and of one read of the stored object,
    /// while a sealed segment is uploaded.
    pub upload_chunk_bytes: usize,
    /// How long one request to the object store may take before an upload gives up.
    pub object_store_timeout: Duration,
}

impl Default for ReplicaSettings {
    /// The settings of the design: 64 MiB segment files, no new segments from 85% of used disk and no
    /// appends from 95%, repairs limited to 128 MiB per second.
    fn default() -> Self {
        Self {
            preallocated_segment_bytes: DEFAULT_PREALLOCATED_SEGMENT_BYTES,
            refuse_new_segments_at_used_percent: DEFAULT_REFUSE_NEW_SEGMENTS_AT_USED_PERCENT,
            refuse_appends_at_used_percent: DEFAULT_REFUSE_APPENDS_AT_USED_PERCENT,
            space_refresh_interval: DEFAULT_SPACE_REFRESH_INTERVAL,
            longest_read_wait: DEFAULT_LONGEST_READ_WAIT,
            repair_chunk_bytes: DEFAULT_REPAIR_CHUNK_BYTES,
            repair_bytes_per_second: DEFAULT_REPAIR_BYTES_PER_SECOND,
            repair_request_timeout: DEFAULT_REPAIR_REQUEST_TIMEOUT,
            upload_chunk_bytes: DEFAULT_UPLOAD_CHUNK_BYTES,
            object_store_timeout: DEFAULT_OBJECT_STORE_TIMEOUT,
        }
    }
}

#[cfg(test)]
#[path = "settings_test.rs"]
mod tests;
