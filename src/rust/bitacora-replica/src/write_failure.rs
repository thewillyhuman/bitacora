//! Telling the replica's host that the disk refused a write.
//!
//! # Why this file exists
//!
//! When a disk refuses a write or a flush, the replica must stop: the kernel may have dropped the pages
//! that were not written, so a retry could falsely succeed, and what the process sees of its files is no
//! longer what is on the disk (design, section 7, "Flush policy"). Stopping a process is not a decision
//! this crate can carry out. The daemon that hosts the replica exits and lets its supervisor restart it;
//! the simulator restarts the simulated node. This file is how the replica hands that fact to whoever
//! hosts it.
//!
//! # What it contains
//!
//! - [`WriteFailure`]: which segment's copy the disk refused a write to, and why.
//! - [`WriteFailureReport`]: where segment tasks record a refusal and where the replica reads it back.
//!
//! # How it works
//!
//! The replica owns one report and gives a clone to the task of every segment copy. A task that sees its
//! copy's write refused records it; the report keeps the first refusal and ignores later ones, because
//! the first one already means "stop". The host either looks (`Replica::write_failure`) or waits
//! (`Replica::write_failed`).
//!
//! The fact travels as plain data, not through a callback: the host decides what to do with it in its own
//! code, on its own task, and a test reads it like any other value.
//!
//! # Invariants
//!
//! - A recorded refusal is never cleared or replaced. A replica that has one stays a replica that has one
//!   until it is opened again.
//! - Recording never blocks and never fails, whether or not anybody is waiting.
//!
//! # See also
//!
//! - `actor.rs`: the segment task that records a refusal after the command that met it.
//! - `copy.rs`: how a copy remembers that its disk refused a write.
//! - `replica.rs`: `Replica::write_failure` and `Replica::write_failed`, which hosts use.

use bitacora_types::SegmentAddress;
use tokio::sync::watch;

/// A write or a flush that the disk refused.
///
/// Once a replica reports one, its host must stop it: see [`crate::Replica::write_failed`]. What had been
/// flushed before is still on the disk, and is read back and verified when the replica is opened again.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::sync::Arc;
///
/// use bitacora_replica::{Replica, ReplicaSettings};
/// use bitacora_segment::MemoryDisk;
/// use bitacora_segment::format::Batch;
/// use bitacora_types::replica_api::{AppendRequest, ReplicaApi, SegmentDescriptor, StaticReplicaConnector};
/// use bitacora_types::{Epoch, ReplicaId, SegmentAddress, SegmentId, SequenceNumber, StreamId};
/// use bytes::Bytes;
///
/// let disk = MemoryDisk::new();
/// let settings = ReplicaSettings {
///     preallocated_segment_bytes: 4096,
///     ..ReplicaSettings::default()
/// };
/// let replica = Replica::open(
///     Arc::new(disk.clone()),
///     ReplicaId::new("replica-a")?,
///     settings,
///     Arc::new(StaticReplicaConnector::new()),
/// )
/// .await?;
/// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
/// let first_sequence = SequenceNumber::new(100);
/// let batch = Batch::encode(segment, first_sequence, &[Bytes::from_static(b"a record")])?;
/// let append = AppendRequest {
///     segment: SegmentDescriptor {
///         address: segment,
///         epoch: Epoch::new(1),
///         first_sequence,
///     },
///     writer_epoch: Epoch::new(1),
///     batch_frames: batch.frame().clone(),
///     durable_sequence: SequenceNumber::ZERO,
/// };
///
/// // The disk refuses the flush that would have made the batch durable.
/// disk.fail_flushes(true);
/// let refused = replica.append(append).await;
///
/// assert!(refused.is_err());
/// let write_failure = replica.write_failure().ok_or("the refused flush is reported")?;
/// assert_eq!(write_failure.segment, segment);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteFailure {
    /// The segment whose copy the disk refused a write to.
    pub segment: SegmentAddress,
    /// What the disk reported, for the operator who reads the log of the stopped process.
    pub reason: String,
}

/// The place where the tasks of a replica record that the disk refused a write, and where the replica
/// reads it back for its host.
///
/// Clones share one record. It keeps the first refusal only.
#[derive(Clone, Debug)]
pub(crate) struct WriteFailureReport {
    /// The first refusal recorded, or `None` while there is none. A watch channel, so that a host can
    /// wait for it without polling.
    first_failure: watch::Sender<Option<WriteFailure>>,
}

impl WriteFailureReport {
    /// Creates a report with nothing recorded.
    ///
    /// # Returns
    ///
    /// The report. Clone it for every task that may have to record a refusal.
    pub(crate) fn new() -> Self {
        let (first_failure, _nobody_waits_yet) = watch::channel(None);
        Self { first_failure }
    }

    /// Records that the disk refused a write to the copy of `segment`.
    ///
    /// Only the first refusal is kept; a later one changes nothing. Allocates the reason once, which is
    /// fine: this runs at most once in the life of a process.
    ///
    /// # Arguments
    ///
    /// * `segment` - The segment whose copy was being written.
    /// * `reason` - What the disk reported.
    pub(crate) fn record(&self, segment: SegmentAddress, reason: &str) {
        self.first_failure
            .send_if_modified(|first_failure| keep_first_failure(first_failure, segment, reason));
    }

    /// Reads the first refusal recorded so far.
    ///
    /// # Returns
    ///
    /// The refusal, or `None` if the disk has refused nothing.
    pub(crate) fn first(&self) -> Option<WriteFailure> {
        self.first_failure.borrow().clone()
    }

    /// Waits until a refusal is recorded.
    ///
    /// Returns at once if one already is. Never returns if the disk never refuses a write.
    ///
    /// # Returns
    ///
    /// The first refusal recorded.
    ///
    /// # Panics
    ///
    /// Never in practice: the channel cannot close while this report holds its sender, and the wait ends
    /// only on a recorded refusal.
    pub(crate) async fn wait_for_first(&self) -> WriteFailure {
        let mut failures = self.first_failure.subscribe();
        let first_failure = failures
            .wait_for(Option::is_some)
            .await
            .expect("the sender lives in this report, which is borrowed for the whole wait");
        first_failure
            .clone()
            .expect("the wait ends only once a refusal is recorded")
    }
}

/// Stores a refusal in `first_failure` unless one is stored there already.
///
/// Returns whether it stored one, which is what tells the watch channel to wake those who wait.
fn keep_first_failure(first_failure: &mut Option<WriteFailure>, segment: SegmentAddress, reason: &str) -> bool {
    if first_failure.is_some() {
        return false;
    }
    *first_failure = Some(WriteFailure {
        segment,
        reason: reason.to_owned(),
    });
    true
}

#[cfg(test)]
#[path = "write_failure_test.rs"]
mod tests;
