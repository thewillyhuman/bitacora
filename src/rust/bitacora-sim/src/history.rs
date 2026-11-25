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

/// One call to the key-value store, as its caller made it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvCall {
    /// `put(key, value)`.
    Put {
        /// The key written.
        key: Bytes,
        /// The value written, unique within the simulation.
        value: Bytes,
    },
    /// `delete(key)`.
    Delete {
        /// The key removed.
        key: Bytes,
    },
    /// `get(key)`, the linearizable read.
    Get {
        /// The key read.
        key: Bytes,
    },
}

impl KvCall {
    /// The key the call is about.
    #[must_use]
    pub const fn key(&self) -> &Bytes {
        match self {
            Self::Put { key, .. } | Self::Delete { key } | Self::Get { key } => key,
        }
    }
}

/// How a call to the key-value store ended, as its caller learned it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvOutcome {
    /// The write was reported as done.
    Written,
    /// The store could not tell whether the write was made. It may take
    /// effect at any time after the call, or never.
    WriteUnknown,
    /// The store refused the write. It must never take effect.
    NotWritten,
    /// The read returned this value, or `None` for a key that does not
    /// exist.
    Read {
        /// What the read returned.
        value: Option<Bytes>,
    },
    /// The read gave no answer.
    ReadFailed,
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
    /// A payload was declared a filler: a record that carries no information
    /// of its own and may appear in the stream any number of times, such as
    /// the barrier a key-value read appends. The log checker does not ask
    /// who appended a filler.
    FillerDeclared {
        /// The payload every such record has.
        payload: Bytes,
    },
    /// A client called an operation of the key-value store. A call that
    /// never gets a matching [`Event::KvReturned`] belongs to a client that
    /// crashed.
    KvInvoked {
        /// Identifies the call.
        operation: u64,
        /// What was called.
        call: KvCall,
    },
    /// An operation of the key-value store returned.
    KvReturned {
        /// The call that returned.
        operation: u64,
        /// What it returned.
        outcome: KvOutcome,
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
        self.record(Event::ReaderStarted { session, from_sequence });
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
    pub fn truncated_past_checkpoint(&self, checkpoint: SequenceNumber, low_water_mark: SequenceNumber) {
        self.record(Event::TruncatedPastCheckpoint {
            checkpoint,
            low_water_mark,
        });
    }

    /// Declares that records with this payload are fillers: they may appear
    /// in the stream any number of times without anyone having recorded
    /// their append.
    pub fn declare_filler(&self, payload: Bytes) {
        self.record(Event::FillerDeclared { payload });
    }

    /// Records that a client is about to call the key-value store, and
    /// returns the identifier to finish the call with.
    #[must_use]
    pub fn kv_invoked(&self, call: KvCall) -> u64 {
        let operation = {
            let mut recorded = self.lock();
            recorded.next_operation += 1;
            recorded.next_operation
        };
        self.record(Event::KvInvoked { operation, call });
        operation
    }

    /// Records how the key-value call `operation` ended.
    pub fn kv_returned(&self, operation: u64, outcome: KvOutcome) {
        self.record(Event::KvReturned { operation, outcome });
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
