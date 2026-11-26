//! Where a runner is, and why it stopped, readable from any task.
//!
//! # Why this file exists
//!
//! The runner works alone in its task, but others need to know how far it is: a store that answers "read your own
//! write" waits until the record it appended is applied, a test waits for a checkpoint, a daemon exports the lag as a
//! metric. And when the runner stops, whoever was waiting must learn why, or it waits for ever. This file is the one
//! way out of the runner's task for both, so that no front end has to build a channel of its own.
//!
//! # What it contains
//!
//! - [`Progress`]: the three positions a runner publishes.
//! - [`RunnerProgress`]: the handle others read and wait on.
//! - [`RunnerStopped`]: what a waiter gets when the runner stops first.
//! - [`ProgressPublisher`]: the runner's end.
//!
//! # How it works
//!
//! The positions travel over a watch channel: the runner replaces the value, any number of handles read the latest
//! one or wait for one that satisfies them. Nothing queues up, and a reader that is slow holds nobody back.
//!
//! The reason for stopping is kept next to the channel, in a cell that is written once. The runner writes it and
//! only then lets go of its end of the channel. A waiter is woken by the channel closing, and by then the reason is
//! there to be read.
//!
//! # Invariants
//!
//! - Reading the latest progress never waits.
//! - The stop reason is written at most once, and before the channel closes: a waiter that is woken by the runner
//!   stopping always finds it.
//! - A runner that did not stop by itself, because its task was cancelled or it was dropped before it ran, leaves no
//!   reason. Waiters are still woken.
//!
//! # See also
//!
//! - `runner.rs`: the loop that publishes.
//! - `progress_test.rs`: the tests of the two ends against each other.

use std::sync::{Arc, OnceLock};

use bitacora_types::SequenceNumber;
use tokio::sync::watch;

use super::RunnerError;

/// The runner whose progress was being watched has stopped.
///
/// # Examples
///
/// ```
/// use bitacora_materializer::{RunnerError, RunnerStopped};
///
/// let stopped = RunnerStopped { reason: Some(RunnerError::StreamEnded) };
/// assert_eq!(stopped.to_string(), "the materializer runner has stopped: the stream ended");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunnerStopped {
    /// Why the runner stopped: what `MaterializerRunner::run` returned. `None` if the runner did not
    /// stop by itself: its task was cancelled, or it was dropped before it ran.
    pub reason: Option<RunnerError>,
}

impl std::fmt::Display for RunnerStopped {
    /// Says that the runner stopped and, if it is known, why.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.reason {
            Some(reason) => write!(formatter, "the materializer runner has stopped: {reason}"),
            None => write!(formatter, "the materializer runner was stopped from outside"),
        }
    }
}

impl std::error::Error for RunnerStopped {}

/// Where a runner is.
///
/// # Examples
///
/// ```
/// use bitacora_materializer::Progress;
/// use bitacora_types::SequenceNumber;
///
/// let progress = Progress {
///     applied_sequence: SequenceNumber::new(90),
///     committed_checkpoint: SequenceNumber::new(50),
///     durable_sequence: SequenceNumber::new(100),
/// };
/// assert!(progress.committed_checkpoint <= progress.applied_sequence);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Progress {
    /// The last record the materializer has applied.
    pub applied_sequence: SequenceNumber,
    /// The last record covered by a committed checkpoint. The log may be truncated up to here.
    pub committed_checkpoint: SequenceNumber,
    /// The highest sequence number the runner knows to be committed in the log. It is a lower bound: a
    /// runner far behind the tail only knows about the segments it has read about.
    pub durable_sequence: SequenceNumber,
}

impl Progress {
    /// How many committed records the runner knows of and has not applied yet.
    ///
    /// # Returns
    ///
    /// The distance from the last record applied to the highest sequence known to be committed. It is a lower
    /// bound, like [`Progress::durable_sequence`].
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_materializer::Progress;
    /// use bitacora_types::SequenceNumber;
    ///
    /// let progress = Progress {
    ///     applied_sequence: SequenceNumber::new(90),
    ///     committed_checkpoint: SequenceNumber::new(50),
    ///     durable_sequence: SequenceNumber::new(100),
    /// };
    /// assert_eq!(progress.lag_records(), 10);
    /// ```
    #[must_use]
    pub const fn lag_records(&self) -> u64 {
        self.durable_sequence.records_since(self.applied_sequence)
    }
}

/// A cheap, cloneable way to see where a runner is, from any task.
///
/// Get one from `MaterializerRunner::progress` before the runner starts to run; the example there runs a whole
/// materializer and waits on its handle.
#[derive(Clone, Debug)]
pub struct RunnerProgress {
    /// The receiving end of the channel the runner publishes its progress on.
    receiver: watch::Receiver<Progress>,
    /// Why the runner stopped, once it has. The runner writes it before it lets go of the channel.
    stop_reason: Arc<OnceLock<RunnerError>>,
}

impl RunnerProgress {
    /// Where the runner is right now. Never waits.
    ///
    /// # Returns
    ///
    /// The positions the runner last published. For a runner that has stopped, the last ones it reached.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_materializer::RunnerProgress;
    ///
    /// /// How many records a runner still has to apply, for a metric.
    /// fn lag_of(progress: &RunnerProgress) -> u64 {
    ///     progress.latest().lag_records()
    /// }
    /// ```
    #[must_use]
    pub fn latest(&self) -> Progress {
        *self.receiver.borrow()
    }

    /// Why the runner stopped.
    ///
    /// # Returns
    ///
    /// What `MaterializerRunner::run` returned, from the moment it returns. `None` while the runner
    /// runs, and for a runner that did not stop by itself: one whose task was cancelled, or that was
    /// dropped before it ran.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_materializer::{RunnerError, RunnerProgress};
    ///
    /// /// Whether a new runner must be started because another instance took this one's place.
    /// fn was_replaced(progress: &RunnerProgress) -> bool {
    ///     matches!(progress.stop_reason(), Some(RunnerError::Fenced { .. }))
    /// }
    /// ```
    #[must_use]
    pub fn stop_reason(&self) -> Option<RunnerError> {
        self.stop_reason.get().cloned()
    }

    /// Waits until the runner has applied the record at `sequence`.
    ///
    /// # Arguments
    ///
    /// * `sequence` - The record to wait for. A record that is already applied is not waited for.
    ///
    /// # Returns
    ///
    /// The runner's progress at that moment.
    ///
    /// # Errors
    ///
    /// Returns [`RunnerStopped`], with the reason, if the runner stopped before it got there. The caller
    /// bounds the wait with a timeout if the record may never be committed.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_materializer::{RunnerProgress, RunnerStopped};
    /// use bitacora_types::SequenceNumber;
    ///
    /// /// Lets a reader see its own write: returns once the record it appended is part of the state.
    /// async fn read_your_write(
    ///     progress: &mut RunnerProgress,
    ///     appended_sequence: SequenceNumber,
    /// ) -> Result<(), RunnerStopped> {
    ///     progress.wait_until_applied(appended_sequence).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn wait_until_applied(&mut self, sequence: SequenceNumber) -> Result<Progress, RunnerStopped> {
        let reached = self
            .receiver
            .wait_for(|progress| progress.applied_sequence >= sequence)
            .await
            .map(|progress| *progress);
        match reached {
            Ok(progress) => Ok(progress),
            Err(_sender_dropped) => Err(self.stopped()),
        }
    }

    /// Waits until a checkpoint at or above `sequence` is committed.
    ///
    /// # Arguments
    ///
    /// * `sequence` - The record the committed checkpoint must cover.
    ///
    /// # Returns
    ///
    /// The runner's progress at that moment.
    ///
    /// # Errors
    ///
    /// Returns [`RunnerStopped`], with the reason, if the runner stopped before it got there.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_materializer::{RunnerProgress, RunnerStopped};
    /// use bitacora_types::SequenceNumber;
    ///
    /// /// Returns once the log may be truncated up to `sequence`.
    /// async fn wait_until_truncatable(
    ///     progress: &mut RunnerProgress,
    ///     sequence: SequenceNumber,
    /// ) -> Result<(), RunnerStopped> {
    ///     progress.wait_until_committed(sequence).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn wait_until_committed(&mut self, sequence: SequenceNumber) -> Result<Progress, RunnerStopped> {
        let reached = self
            .receiver
            .wait_for(|progress| progress.committed_checkpoint >= sequence)
            .await
            .map(|progress| *progress);
        match reached {
            Ok(progress) => Ok(progress),
            Err(_sender_dropped) => Err(self.stopped()),
        }
    }

    /// The error for a waiter whose runner let go of the progress channel.
    fn stopped(&self) -> RunnerStopped {
        RunnerStopped {
            reason: self.stop_reason(),
        }
    }
}

/// The runner's end of its progress: where it publishes its positions and leaves its reason for stopping.
#[derive(Debug)]
pub(super) struct ProgressPublisher {
    /// Where progress is published.
    sender: watch::Sender<Progress>,
    /// Where the reason for stopping is left for the handles, before the channel closes.
    stop_reason: Arc<OnceLock<RunnerError>>,
}

impl ProgressPublisher {
    /// Starts publishing for a runner that was just restored.
    ///
    /// # Arguments
    ///
    /// * `restored_checkpoint` - The checkpoint the runner restored from. Nothing above it is applied, committed or
    ///   known to be durable yet, so all three positions start there.
    ///
    /// # Returns
    ///
    /// The publisher. Handles taken from it see the starting position at once.
    pub(super) fn starting_at(restored_checkpoint: SequenceNumber) -> Self {
        let (sender, _first_receiver) = watch::channel(Progress {
            applied_sequence: restored_checkpoint,
            committed_checkpoint: restored_checkpoint,
            durable_sequence: restored_checkpoint,
        });
        Self {
            sender,
            stop_reason: Arc::new(OnceLock::new()),
        }
    }

    /// Gives a handle on what this publisher publishes.
    ///
    /// # Returns
    ///
    /// A handle that stays valid after the publisher is gone: it then shows the last positions and the reason.
    pub(super) fn handle(&self) -> RunnerProgress {
        RunnerProgress {
            receiver: self.sender.subscribe(),
            stop_reason: Arc::clone(&self.stop_reason),
        }
    }

    /// Replaces the positions every handle sees, and wakes the waiters they satisfy.
    ///
    /// # Arguments
    ///
    /// * `progress` - Where the runner is now.
    pub(super) fn publish(&self, progress: Progress) {
        self.sender.send_replace(progress);
    }

    /// Leaves the reason the runner stops for, where every handle finds it.
    ///
    /// Call it before the publisher is dropped: dropping it closes the channel, which is what wakes the waiters.
    /// Only the first reason is kept.
    ///
    /// # Arguments
    ///
    /// * `reason` - Why the runner stops.
    pub(super) fn leave_stop_reason(&self, reason: RunnerError) {
        // A second reason is dropped: the cell keeps the first, and a runner stops only once.
        drop(self.stop_reason.set(reason));
    }
}

#[cfg(test)]
#[path = "progress_test.rs"]
mod tests;
