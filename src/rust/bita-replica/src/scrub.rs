//! The schedule on which the replica rereads its sealed segments.
//!
//! # Why this file exists
//!
//! A sealed segment is written once and may then sit on a disk for months without being read. Disks
//! lose bits silently, and a copy that rotted is only found when somebody reads it, which may be the day
//! the other copies are gone. So a background scrubber rereads every sealed segment on a rolling
//! schedule, weekly by default, and a copy that fails a checksum is marked damaged so that the
//! controller replaces it from a peer (design, section 7, "Integrity"). The replica library knows how
//! to scrub; when to do it is the daemon's decision, and this file is the timer.
//!
//! # What it contains
//!
//! - [`run_scrub_schedule`]: scrubs after every interval, forever.
//!
//! # How it works
//!
//! The first scrub happens one interval after start, not at start: opening the data directory has just
//! read every segment file, so an immediate scrub would read them twice. After each scrub the number of
//! damaged copies goes to the daemon's gauge and to the log; the controller learns about damaged copies
//! by itself, when it next asks the replica what it holds.
//!
//! A scrub that fails as a whole, which only happens when the replica is stopping, is logged and the
//! schedule goes on.
//!
//! # Invariants
//!
//! - Scrubbing only reads. It never repairs or deletes a copy.
//!
//! # See also
//!
//! - `bitacora-replica/src/replica.rs`: `Replica::scrub_sealed_segments`, which does the reading.
//! - `daemon.rs`: starts the schedule and stops it at shutdown.

use std::sync::Arc;
use std::time::Duration;

use bitacora_replica::Replica;
use tokio::time::sleep;

use crate::metrics::ReplicaDaemonMetrics;

/// Scrubs the sealed segments of `replica` after every `interval`, until the future is dropped.
///
/// # Arguments
///
/// * `replica` - The replica whose sealed segments are reread.
/// * `interval` - How long to wait before the first scrub and between two scrubs.
/// * `metrics` - The daemon's metrics: each completed scrub is counted, and the number of damaged
///   copies it found is stored in the gauge.
pub(crate) async fn run_scrub_schedule(
    replica: Arc<Replica>,
    interval: Duration,
    metrics: ReplicaDaemonMetrics,
) {
    loop {
        sleep(interval).await;
        match replica.scrub_sealed_segments().await {
            Ok(damaged_copies) => {
                metrics.scrubs.add(1);
                metrics
                    .damaged_segment_copies
                    .set_unsigned(damaged_copies as u64);
                if damaged_copies == 0 {
                    tracing::info!("scrub found every sealed segment intact");
                } else {
                    tracing::warn!(
                        damaged_copies,
                        "scrub found damaged copies; the controller will replace them"
                    );
                }
            }
            Err(replica_error) => {
                tracing::warn!(%replica_error, "scrub did not complete");
            }
        }
    }
}

#[cfg(test)]
#[path = "scrub_test.rs"]
mod tests;
