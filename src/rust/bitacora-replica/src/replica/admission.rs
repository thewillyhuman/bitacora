//! Admission: whether the replica takes more data right now.
//!
//! # Why this file exists
//!
//! A disk that fills up while a writer depends on it is the worst way to learn that it was full. A replica
//! therefore turns data away while there is still room to finish what it has started (design, section 7,
//! "Capacity and admission"): past a first threshold it accepts no new segments, past a second it refuses
//! every append, which makes writers roll to replicas that have room. An operator who drains a replica
//! for maintenance wants the same effect at once. This file is where the replica decides, for each write
//! that arrives, which of these applies.
//!
//! # What it contains
//!
//! - `Replica::admit_write`: the decision, as an `Admission` for the copy or a refusal for the caller.
//! - [`SpaceMeasurement`]: a measurement of the disk's free space, and when it was taken.
//!
//! # How it works
//!
//! A draining replica refuses. Otherwise the share of used disk is compared with the two thresholds of
//! the settings: at or above the higher one the write is refused; below it the write is admitted, with a
//! note on whether a new segment file may still be created.
//!
//! Asking a file system for its free space is a system call, and appends arrive by the thousand. The
//! last measurement is therefore kept and reused for the refresh interval of the settings.
//!
//! # Invariants
//!
//! - Admission is decided before a request reaches a segment's task, so a refused write never queues
//!   behind others.
//! - Reads, fences, seals and deletions are never subject to admission: a full or draining replica still
//!   serves what it holds and still takes part in a failover.
//! - This runs for every append. With a recent measurement it costs one lock and no system call, and it
//!   allocates nothing. The lock is never held across an `await`.
//! - A measurement may be up to one refresh interval old, so the thresholds are soft by what can be
//!   written in that time. The gap between the two thresholds is what absorbs it.
//!
//! # See also
//!
//! - `../replica.rs`: the replica, and the draining switch.
//! - `../copy/append.rs`: what a copy does with an `Admission`.
//! - `../settings.rs`: the thresholds and the refresh interval.

use std::sync::atomic::Ordering;
use std::sync::{MutexGuard, PoisonError};

use bitacora_segment::DiskSpace;
use bitacora_types::replica_api::ReplicaError;
use tokio::time::Instant;

use super::{Replica, directory_failure};
use crate::copy::Admission;

/// A measurement of the disk's free space, and when it was taken.
#[derive(Clone, Copy, Debug)]
pub(super) struct SpaceMeasurement {
    /// When the disk was asked.
    measured_at: Instant,
    /// What it answered.
    space: DiskSpace,
}

impl Replica {
    /// Decides whether the replica takes more data right now: not while it is draining, not when its
    /// disk is nearly full.
    ///
    /// Asks the disk for its free space only if the last measurement is older than the refresh interval.
    ///
    /// # Returns
    ///
    /// What the write may do: whether it may create a new segment file, and how full the disk is.
    ///
    /// # Errors
    ///
    /// * [`ReplicaError::Draining`] if the replica is being drained.
    /// * [`ReplicaError::NoCapacity`] if the disk is at or past the threshold from which every append is
    ///   refused.
    /// * [`ReplicaError::Storage`] if the disk cannot be measured.
    pub(super) async fn admit_write(&self) -> Result<Admission, ReplicaError> {
        if self.is_draining.load(Ordering::Relaxed) {
            return Err(ReplicaError::Draining);
        }
        let used_percent = self.disk_space().await?.used_percent();
        if used_percent >= self.settings.refuse_appends_at_used_percent {
            return Err(ReplicaError::NoCapacity { used_percent });
        }
        Ok(Admission {
            may_create_segments: used_percent < self.settings.refuse_new_segments_at_used_percent,
            used_percent,
        })
    }

    /// Measures the disk's free space, reusing the last measurement while it is recent enough.
    async fn disk_space(&self) -> Result<DiskSpace, ReplicaError> {
        let now = Instant::now();
        if let Some(recent_space) = self.recent_space_measurement(now) {
            return Ok(recent_space);
        }
        let space = match self.disk.space().await {
            Ok(space) => space,
            Err(io_error) => return Err(directory_failure("measure", &io_error)),
        };
        *self.lock_space_measurement() = Some(SpaceMeasurement {
            measured_at: now,
            space,
        });
        Ok(space)
    }

    /// Returns the last measurement of free space if it was taken less than the refresh interval before
    /// `now`.
    fn recent_space_measurement(&self, now: Instant) -> Option<DiskSpace> {
        let last_measurement = (*self.lock_space_measurement())?;
        if now.duration_since(last_measurement.measured_at) < self.settings.space_refresh_interval {
            Some(last_measurement.space)
        } else {
            None
        }
    }

    /// Locks the last measurement, whether or not another holder of the lock panicked: the value is
    /// replaced whole, so it is never half-written.
    fn lock_space_measurement(&self) -> MutexGuard<'_, Option<SpaceMeasurement>> {
        self.last_space_measurement
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
#[path = "admission_test.rs"]
mod tests;
