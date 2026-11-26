//! The settings of writers and readers, with the defaults of the design.
//!
//! # Why this file exists
//!
//! The write and read paths wait, retry and give up in many places: how long a replica may be silent before the writer
//! rolls away from it, how much may await commit before `append` holds its caller back, how long a follower lets a
//! replica hold a request. The design gives each of them a number (sections 5, 6 and 9). Tests and the simulator need
//! other numbers, so that a minute of virtual time sees every timeout fire many times. This file is the one place where
//! those numbers live, as plain data that a caller can read, copy and change.
//!
//! # What it contains
//!
//! - [`TakeoverPolicy`]: whether a new writer waits for the previous writer's lease.
//! - [`WriterSettings`]: flow control, failure detection, leases and segment limits of a writer.
//! - [`ReaderSettings`]: fetch sizes, waits and retries of a reader.
//!
//! # How it works
//!
//! Each settings type is a struct of public fields with a `Default` that holds the design's values. A caller builds one
//! with struct update syntax, changing only what it needs. Nothing here is validated: the values are used as they are,
//! and a value that makes no sense (a zero request timeout) makes the client fail fast at its first request instead of
//! being silently corrected.
//!
//! # Invariants
//!
//! - Every duration is a real duration, never a count of ticks: the client reads time through `tokio::time`, which is
//!   virtual under simulation.
//! - Sizes are in bytes and say so in their names.
//!
//! # See also
//!
//! - `client.rs`: [`crate::ClientSettings`], which groups the two.
//! - `writer.rs` and `reader.rs`: where the settings are used.

use std::time::Duration;

/// One kibibyte, the unit the size defaults are written in.
const KIBIBYTE: usize = 1024;

/// One mebibyte.
const MEBIBYTE: usize = 1024 * KIBIBYTE;

/// One gibibyte, as the 64-bit count that segment sizes use.
const GIBIBYTE: u64 = 1024 * 1024 * 1024;

/// What a new writer does when the previous writer's lease has not lapsed.
///
/// The lease is a courtesy between writers; correctness depends only on the epoch. The policy therefore only decides
/// how polite a takeover is, never whether it is safe.
///
/// # Examples
///
/// ```
/// use bitacora_client::TakeoverPolicy;
///
/// // An engine whose own failure detector already declared the old writer dead does not wait.
/// let policy = TakeoverPolicy::Force;
///
/// assert_ne!(policy, TakeoverPolicy::WaitForLeaseExpiry);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TakeoverPolicy {
    /// Wait for the lease to lapse, which prevents accidental takeover of a healthy writer. Opening fails with
    /// [`crate::OpenError::LeaseHeld`] if the lease outlasts the writer's `open_timeout`.
    WaitForLeaseExpiry,
    /// Take the stream at once. Use it when the engine's own high-availability logic already decided the old writer is
    /// gone.
    Force,
}

/// How a writer batches, detects failures and rolls (design, section 5).
///
/// # Examples
///
/// ```
/// use bitacora_client::WriterSettings;
///
/// let settings = WriterSettings {
///     writer_identity: "compute-7".to_owned(),
///     target_segment_bytes: 256 * 1024 * 1024,
///     ..WriterSettings::default()
/// };
///
/// assert_eq!(settings.max_request_bytes, 256 * 1024);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriterSettings {
    /// A name for this writer, recorded with its lease for operators.
    pub writer_identity: String,
    /// How many bytes of records may await commit before `append` waits. This bounds memory and is the compute node's
    /// backpressure signal.
    pub max_unacknowledged_bytes: usize,
    /// How far a replica may fall behind the durable sequence, in bytes of committed records, before the writer rolls
    /// away from it.
    pub max_replica_lag_bytes: usize,
    /// The target size of one append request. A request always carries at least one batch, however large.
    pub max_request_bytes: usize,
    /// How often an idle stream tells its replicas the durable sequence.
    pub heartbeat_interval: Duration,
    /// How long one request to a replica may take.
    pub request_timeout: Duration,
    /// How long a replica may be unreachable before the writer rolls away from it.
    pub replica_failure_timeout: Duration,
    /// How long before a failed request to a replica, or a roll that could not happen, is tried again.
    pub retry_interval: Duration,
    /// How long a lease lasts without renewal.
    pub lease_duration: Duration,
    /// How often the writer renews its lease. Shorter than `lease_duration`, so that one failed renewal does not let
    /// the lease lapse.
    pub lease_renewal_interval: Duration,
    /// The size, in bytes of committed batches, at which the writer rolls to a new segment.
    pub target_segment_bytes: u64,
    /// The age at which the writer rolls to a new segment.
    pub target_segment_age: Duration,
    /// How long opening a writer may take, including recovery of the previous writer's segment and waiting for a
    /// standby segment.
    pub open_timeout: Duration,
    /// How long recovery waits for replicas beyond the quorum it needs. Hearing from all of them avoids repairs later.
    pub straggler_grace: Duration,
}

impl Default for WriterSettings {
    /// The values of the design: 64 MiB of flow control and lag, requests of 256 KiB, a heartbeat every 50 ms, a
    /// replica given up after 10 s, a lease of 10 s renewed every 3 s, and segments of 1 GiB or one hour.
    fn default() -> Self {
        Self {
            writer_identity: "bitacora-writer".to_owned(),
            max_unacknowledged_bytes: 64 * MEBIBYTE,
            max_replica_lag_bytes: 64 * MEBIBYTE,
            max_request_bytes: 256 * KIBIBYTE,
            heartbeat_interval: Duration::from_millis(50),
            request_timeout: Duration::from_secs(5),
            replica_failure_timeout: Duration::from_secs(10),
            retry_interval: Duration::from_millis(100),
            lease_duration: Duration::from_secs(10),
            lease_renewal_interval: Duration::from_secs(3),
            target_segment_bytes: GIBIBYTE,
            target_segment_age: Duration::from_hours(1),
            open_timeout: Duration::from_secs(30),
            straggler_grace: Duration::from_millis(100),
        }
    }
}

/// How a reader fetches and waits (design, section 6).
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use bitacora_client::ReaderSettings;
///
/// let settings = ReaderSettings {
///     tail_wait: Duration::from_millis(250),
///     ..ReaderSettings::default()
/// };
///
/// assert_eq!(settings.max_attempts_per_read, 3);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReaderSettings {
    /// The target size of one read answer, from a replica or from the object store.
    pub max_response_bytes: usize,
    /// How long a following reader lets a replica hold a request when nothing new is committed.
    pub tail_wait: Duration,
    /// How long one request to a replica may take beyond its tail wait, and how long one request to the object store
    /// may take.
    pub request_timeout: Duration,
    /// How long before a read that every replica failed is tried again.
    pub retry_interval: Duration,
    /// How many times a bounded read goes around the replica set before it reports the records as unavailable.
    pub max_attempts_per_read: u32,
}

impl Default for ReaderSettings {
    /// The values of the design: answers of 1 MiB, a tail wait of one second, requests of five seconds, and three
    /// rounds, 100 ms apart, before a bounded read gives up.
    fn default() -> Self {
        Self {
            max_response_bytes: MEBIBYTE,
            tail_wait: Duration::from_secs(1),
            request_timeout: Duration::from_secs(5),
            retry_interval: Duration::from_millis(100),
            max_attempts_per_read: 3,
        }
    }
}
