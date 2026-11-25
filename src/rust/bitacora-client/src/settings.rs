//! The knobs of the client library, with the defaults of the design.

use std::time::Duration;

/// What a new writer does when the previous writer's lease has not lapsed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TakeoverPolicy {
    /// Wait for the lease to lapse, which prevents accidental takeover of a
    /// healthy writer.
    WaitForLeaseExpiry,
    /// Take the stream at once. Use it when the engine's own
    /// high-availability logic already decided the old writer is gone.
    Force,
}

/// How a writer batches, detects failures and rolls (design, section 5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriterSettings {
    /// A name for this writer, recorded with its lease for operators.
    pub writer_identity: String,
    /// How many bytes of records may await commit before `append` waits.
    /// This bounds memory and is the compute node's backpressure signal.
    pub max_unacknowledged_bytes: usize,
    /// How far a replica may fall behind before the writer rolls away from
    /// it.
    pub max_replica_lag_bytes: usize,
    /// The target size of one append request. A request always carries at
    /// least one batch, however large.
    pub max_request_bytes: usize,
    /// How often an idle stream tells its replicas the durable sequence.
    pub heartbeat_interval: Duration,
    /// How long one request to a replica may take.
    pub request_timeout: Duration,
    /// How long a replica may be unreachable before the writer rolls away
    /// from it.
    pub replica_failure_timeout: Duration,
    /// How long before a failed request to a replica is tried again.
    pub retry_interval: Duration,
    /// How long a lease lasts without renewal.
    pub lease_duration: Duration,
    /// How often the writer renews its lease.
    pub lease_renewal_interval: Duration,
    /// The size at which the writer rolls to a new segment.
    pub target_segment_bytes: u64,
    /// The age at which the writer rolls to a new segment.
    pub target_segment_age: Duration,
    /// How long opening a writer may take, including recovery of the
    /// previous writer's segment and waiting for a standby segment.
    pub open_timeout: Duration,
    /// How long recovery waits for replicas beyond the quorum it needs.
    /// Hearing from all of them avoids repairs later.
    pub straggler_grace: Duration,
}

impl Default for WriterSettings {
    fn default() -> Self {
        Self {
            writer_identity: "bitacora-writer".to_owned(),
            max_unacknowledged_bytes: 64 * 1024 * 1024,
            max_replica_lag_bytes: 64 * 1024 * 1024,
            max_request_bytes: 256 * 1024,
            heartbeat_interval: Duration::from_millis(50),
            request_timeout: Duration::from_secs(5),
            replica_failure_timeout: Duration::from_secs(10),
            retry_interval: Duration::from_millis(100),
            lease_duration: Duration::from_secs(10),
            lease_renewal_interval: Duration::from_secs(3),
            target_segment_bytes: 1024 * 1024 * 1024,
            target_segment_age: Duration::from_hours(1),
            open_timeout: Duration::from_secs(30),
            straggler_grace: Duration::from_millis(100),
        }
    }
}

/// How a reader fetches and waits (design, section 6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReaderSettings {
    /// The target size of one read answer.
    pub max_response_bytes: usize,
    /// How long a following reader lets a replica hold a request when
    /// nothing new is committed.
    pub tail_wait: Duration,
    /// How long one request to a replica may take beyond its tail wait.
    pub request_timeout: Duration,
    /// How long before a read that every replica failed is tried again.
    pub retry_interval: Duration,
    /// How many times a bounded read goes around the replica set before it
    /// reports the records as unavailable.
    pub max_attempts_per_read: u32,
}

impl Default for ReaderSettings {
    fn default() -> Self {
        Self {
            max_response_bytes: 1024 * 1024,
            tail_wait: Duration::from_secs(1),
            request_timeout: Duration::from_secs(5),
            retry_interval: Duration::from_millis(100),
            max_attempts_per_read: 3,
        }
    }
}
