//! The record of what clients did and saw during a simulation.
//!
//! Every append call and its result, and every record read, is logged here
//! (design, section 15, "Checking"). The checker judges the system from
//! this record alone, the way an outside observer would.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bitacora_types::{Epoch, SequenceNumber};
use bytes::Bytes;
use tokio::time::Instant;

/// How an append ended, as its caller learned it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppendOutcome {
    /// The writer reported the record committed at `sequence`.
    Committed {
        /// The sequence number the writer reported.
        sequence: SequenceNumber,
        /// The epoch of the writer that reported it.
        epoch: Epoch,
    },
    /// The writer was fenced or lost contact. The record may or may not be
    /// in the stream.
    Unknown,
    /// The writer refused the record before giving it a sequence number.
    /// The record must not be in the stream.
    NotAppended,
}

/// One thing a client did or saw.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// A client called `append`. An append that never gets a matching
    /// [`Event::AppendFinished`] belongs to a client that crashed, and its
    /// outcome is unknown.
    AppendInvoked {
        /// Identifies the call.
        operation: u64,
        /// The record's payload, unique within the simulation.
        payload: Bytes,
    },
    /// An `append` call returned.
    AppendFinished {
        /// The call that returned.
        operation: u64,
        /// What it returned.
        outcome: AppendOutcome,
    },
    /// A reader started reading at `from_sequence`.
    ReaderStarted {
        /// Identifies the reading session.
        session: u64,
        /// The first sequence number the reader asked for.
        from_sequence: SequenceNumber,
    },
    /// A reader was handed a record.
    RecordRead {
        /// The session that read it.
        session: u64,
        /// The record's sequence number.
        sequence: SequenceNumber,
        /// The record's payload.
        payload: Bytes,
    },
    /// A registered materializer was told that records above its committed
    /// checkpoint are gone.
    TruncatedPastCheckpoint {
        /// The materializer's committed checkpoint.
        checkpoint: SequenceNumber,
        /// The stream's low-water mark, above `checkpoint + 1`.
        low_water_mark: SequenceNumber,
    },
    /// The simulator did something to the cluster. Not judged; it explains
    /// the events around it when a seed fails.
    Fault {
        /// What was done.
        description: String,
    },
}

/// An [`Event`] and when, in simulated time, it happened.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimedEvent {
    /// How long after the start of the simulation.
    pub at: Duration,
    /// What happened.
    pub event: Event,
}

/// The history of one simulation. Clones record into the same history.
#[derive(Clone, Debug)]
pub struct History {
    started_at: Instant,
    recorded: Arc<Mutex<Recorded>>,
}

#[derive(Debug, Default)]
struct Recorded {
    events: Vec<TimedEvent>,
    next_operation: u64,
    next_session: u64,
}

impl History {
    /// Starts an empty history at the current simulated time.
    #[must_use]
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            recorded: Arc::new(Mutex::new(Recorded::default())),
        }
    }

    /// Records that a client is about to append `payload`, and returns the
    /// identifier to finish the call with.
    #[must_use]
    pub fn append_invoked(&self, payload: Bytes) -> u64 {
        let operation = {
            let mut recorded = self.lock();
            recorded.next_operation += 1;
            recorded.next_operation
        };
        self.record(Event::AppendInvoked { operation, payload });
        operation
    }

    /// Records how the append `operation` ended.
    pub fn append_finished(&self, operation: u64, outcome: AppendOutcome) {
        self.record(Event::AppendFinished { operation, outcome });
    }

    /// Records that a reader starts at `from_sequence`, and returns the
    /// identifier of its session.
    #[must_use]
    pub fn reader_started(&self, from_sequence: SequenceNumber) -> u64 {
        let session = {
            let mut recorded = self.lock();
            recorded.next_session += 1;
            recorded.next_session
        };
        self.record(Event::ReaderStarted {
            session,
            from_sequence,
        });
        session
    }

    /// Records that the reader of `session` was handed a record.
    pub fn record_read(&self, session: u64, sequence: SequenceNumber, payload: Bytes) {
        self.record(Event::RecordRead {
            session,
            sequence,
            payload,
        });
    }

    /// Records that the low-water mark passed a registered materializer's
    /// committed checkpoint.
    pub fn truncated_past_checkpoint(
        &self,
        checkpoint: SequenceNumber,
        low_water_mark: SequenceNumber,
    ) {
        self.record(Event::TruncatedPastCheckpoint {
            checkpoint,
            low_water_mark,
        });
    }

    /// Records something the simulator did to the cluster.
    pub fn fault(&self, description: impl Into<String>) {
        self.record(Event::Fault {
            description: description.into(),
        });
    }

    /// Returns everything recorded so far, in order.
    #[must_use]
    pub fn events(&self) -> Vec<TimedEvent> {
        self.lock().events.clone()
    }

    fn record(&self, event: Event) {
        let at = self.started_at.elapsed();
        self.lock().events.push(TimedEvent { at, event });
    }

    fn lock(&self) -> MutexGuard<'_, Recorded> {
        // Recording is a push onto a vector; a panic elsewhere cannot leave
        // it half done.
        self.recorded.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Default for History {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn events_are_kept_in_the_order_they_were_recorded() {
        let history = History::new();
        let operation = history.append_invoked(Bytes::from("a"));
        history.append_finished(operation, AppendOutcome::Unknown);
        let kinds: Vec<&str> = history
            .events()
            .iter()
            .map(|timed| match timed.event {
                Event::AppendInvoked { .. } => "invoked",
                Event::AppendFinished { .. } => "finished",
                _ => "other",
            })
            .collect();
        assert_eq!(kinds, ["invoked", "finished"]);
    }

    #[tokio::test(start_paused = true)]
    async fn operations_and_sessions_get_distinct_identifiers() {
        let history = History::new();
        let operations = [
            history.append_invoked(Bytes::from("a")),
            history.append_invoked(Bytes::from("b")),
        ];
        let sessions = [
            history.reader_started(SequenceNumber::FIRST),
            history.reader_started(SequenceNumber::FIRST),
        ];
        assert!(operations[0] != operations[1] && sessions[0] != sessions[1]);
    }

    #[tokio::test(start_paused = true)]
    async fn events_carry_the_simulated_time_they_happened_at() {
        let history = History::new();
        tokio::time::advance(Duration::from_secs(7)).await;
        history.fault("node 0 crashed");
        assert_eq!(history.events()[0].at, Duration::from_secs(7));
    }

    #[tokio::test(start_paused = true)]
    async fn clones_record_into_the_same_history() {
        let history = History::new();
        history.clone().fault("recorded through a clone");
        assert_eq!(history.events().len(), 1);
    }
}
