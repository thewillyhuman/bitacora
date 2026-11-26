//! The record of what clients did and saw during a simulation.
//!
//! # Why this file exists
//!
//! A simulated run has to be judged by something that the bugs under test cannot reach. If the checker asked the
//! replicas or the metadata store what happened, a bug that makes them agree on a wrong answer would pass. So the
//! simulator writes down only what an outside observer could know: which calls clients made, what they were told, and
//! which records readers were handed (design, section 15, "Checking"). The checker judges the system from this record
//! alone.
//!
//! The record has a second use. Rendered as text and hashed, it is the fingerprint of a run: two runs of one seed on
//! the same code produce the same events at the same simulated times, and a change that alters behaviour shows as a
//! changed fingerprint.
//!
//! # What it contains
//!
//! - [`Event`]: one thing a client did or saw, or one thing the simulator did to the cluster.
//! - [`AppendOutcome`], [`KvCall`], [`KvOutcome`]: the parts of an event.
//! - [`TimedEvent`]: an event and the simulated time it happened at.
//! - [`History`]: the growing list of events of one run, shared by every task of the run.
//!
//! # How it works
//!
//! A [`History`] is a list behind a lock, with the runtime instant the run started at. Every task of a run holds a
//! clone and appends to the one list. A call is recorded in two events: one before it is made, which hands out the
//! identifier of the call, and one after it returned. A call whose second event never comes belongs to a client that
//! crashed, and nobody knows how it ended.
//!
//! # Invariants
//!
//! - Events are kept in the order they were recorded, each stamped with the simulated time of its recording.
//! - Identifiers of calls are unique within a history, and so are identifiers of reading sessions.
//! - The names of [`Event`], [`AppendOutcome`], [`KvCall`], [`KvOutcome`] and [`TimedEvent`], of their variants and of
//!   their fields, and the `Debug` rendering of everything inside them, are part of the fingerprint of a run
//!   (`report.rs`). Renaming or reshaping any of them changes every fingerprint ever written down, although no
//!   behaviour changed. Do not.
//! - Recording allocates nothing beyond the slot of the event in the list: payloads are `Bytes`, which are shared, not
//!   copied.
//! - The lock is held for one push and never across an `await`.
//!
//! # See also
//!
//! - `checker.rs`: judges the appends and reads recorded here.
//! - `linearizability.rs`: judges the key-value calls recorded here.
//! - `report.rs`: the fingerprint computed from the events.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bitacora_types::{Epoch, SequenceNumber};
use bytes::Bytes;
use tokio::time::Instant;

/// How an append ended, as its caller learned it.
///
/// The names of this type, its variants and its fields are part of the fingerprint of a run; see the file header.
///
/// # Examples
///
/// ```
/// use bitacora_sim::AppendOutcome;
/// use bitacora_types::{Epoch, SequenceNumber};
///
/// let outcome = AppendOutcome::Committed {
///     sequence: SequenceNumber::new(12),
///     epoch: Epoch::new(3),
/// };
///
/// assert_ne!(outcome, AppendOutcome::Unknown);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppendOutcome {
    /// The writer reported the record committed at `sequence`.
    Committed {
        /// The sequence number the writer reported.
        sequence: SequenceNumber,
        /// The epoch of the writer that reported it.
        epoch: Epoch,
    },
    /// The writer was fenced or lost contact. The record may or may not be in the stream.
    Unknown,
    /// The writer refused the record before giving it a sequence number. The record must not be in the stream.
    NotAppended,
}

/// One call to the key-value store, as its caller made it.
///
/// The names of this type, its variants and its fields are part of the fingerprint of a run; see the file header.
///
/// # Examples
///
/// ```
/// use bitacora_sim::KvCall;
/// use bytes::Bytes;
///
/// let call = KvCall::Put {
///     key: Bytes::from_static(b"colour"),
///     value: Bytes::from_static(b"red"),
/// };
///
/// assert_eq!(call.key(), &Bytes::from_static(b"colour"));
/// ```
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
    /// Reports the key the call is about.
    ///
    /// # Returns
    ///
    /// The key written, removed or read. Every call is about exactly one key.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_sim::KvCall;
    /// use bytes::Bytes;
    ///
    /// let call = KvCall::Get {
    ///     key: Bytes::from_static(b"colour"),
    /// };
    ///
    /// assert_eq!(call.key(), &Bytes::from_static(b"colour"));
    /// ```
    #[must_use]
    pub const fn key(&self) -> &Bytes {
        match self {
            Self::Put { key, .. } | Self::Delete { key } | Self::Get { key } => key,
        }
    }
}

/// How a call to the key-value store ended, as its caller learned it.
///
/// The names of this type, its variants and its fields are part of the fingerprint of a run; see the file header.
///
/// # Examples
///
/// ```
/// use bitacora_sim::KvOutcome;
/// use bytes::Bytes;
///
/// let found = KvOutcome::Read {
///     value: Some(Bytes::from_static(b"red")),
/// };
/// let absent = KvOutcome::Read { value: None };
///
/// assert_ne!(found, absent);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvOutcome {
    /// The write was reported as done.
    Written,
    /// The store could not tell whether the write was made. It may take effect at any time after the call, or never.
    WriteUnknown,
    /// The store refused the write. It must never take effect.
    NotWritten,
    /// The read returned this value, or `None` for a key that does not exist.
    Read {
        /// What the read returned.
        value: Option<Bytes>,
    },
    /// The read gave no answer.
    ReadFailed,
}

/// One thing a client did or saw.
///
/// The names of this type, its variants and its fields are part of the fingerprint of a run; see the file header.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() {
/// use bitacora_sim::{Event, History};
///
/// let history = History::new();
/// history.fault("node 2 crashes");
///
/// let recorded = &history.events()[0].event;
///
/// assert!(matches!(recorded, Event::Fault { .. }));
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// A client called `append`. An append that never gets a matching [`Event::AppendFinished`] belongs to a client
    /// that crashed, and its outcome is unknown.
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
    /// A registered materializer was told that records above its committed checkpoint are gone.
    TruncatedPastCheckpoint {
        /// The materializer's committed checkpoint.
        checkpoint: SequenceNumber,
        /// The stream's low-water mark, above `checkpoint + 1`.
        low_water_mark: SequenceNumber,
    },
    /// A payload was declared a filler: a record that carries no information of its own and may appear in the stream
    /// any number of times, such as the barrier a key-value read appends. The log checker does not ask who appended a
    /// filler.
    FillerDeclared {
        /// The payload every such record has.
        payload: Bytes,
    },
    /// A client called an operation of the key-value store. A call that never gets a matching [`Event::KvReturned`]
    /// belongs to a client that crashed.
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
    /// The simulator did something to the cluster. Not judged; it explains the events around it when a seed fails.
    Fault {
        /// What was done.
        description: String,
    },
}

/// An [`Event`] and when, in simulated time, it happened.
///
/// The names of this type and its fields are part of the fingerprint of a run; see the file header.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() {
/// use std::time::Duration;
///
/// use bitacora_sim::History;
///
/// let history = History::new();
/// tokio::time::advance(Duration::from_secs(3)).await;
/// history.fault("node 2 crashes");
///
/// let timed = &history.events()[0];
///
/// assert_eq!(timed.at, Duration::from_secs(3));
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimedEvent {
    /// How long after the start of the simulation.
    pub at: Duration,
    /// What happened.
    pub event: Event,
}

/// The history of one simulation. Clones record into the same history.
///
/// Create one at the start of a run, hand a clone to every task that acts as a client, and read [`History::events`]
/// when the run is over. A call is recorded twice: before it is made ([`History::append_invoked`],
/// [`History::kv_invoked`]) and after it returned ([`History::append_finished`], [`History::kv_returned`]).
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() {
/// use bitacora_sim::{AppendOutcome, History, check};
/// use bitacora_types::{Epoch, SequenceNumber};
/// use bytes::Bytes;
///
/// let history = History::new();
/// let payload = Bytes::from_static(b"first record");
/// let sequence = SequenceNumber::FIRST;
///
/// let operation = history.append_invoked(payload.clone());
/// let epoch = Epoch::new(1);
/// history.append_finished(operation, AppendOutcome::Committed { sequence, epoch });
/// let session = history.reader_started(sequence);
/// history.record_read(session, sequence, payload);
///
/// assert!(check(&history.events()).is_empty());
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct History {
    /// The runtime instant the simulation started at. Events are stamped with the time elapsed since.
    started_at: Instant,
    /// The events and the counters that number calls and sessions. Shared, because every clone records into the same
    /// history.
    recorded: Arc<Mutex<Recorded>>,
}

/// What a history has recorded so far.
#[derive(Debug, Default)]
struct Recorded {
    /// Every event, in the order it was recorded.
    events: Vec<TimedEvent>,
    /// The identifier given to the last call. Appends and key-value calls share the numbering.
    next_operation: u64,
    /// The identifier given to the last reading session.
    next_session: u64,
}

impl History {
    /// Starts an empty history at the current simulated time.
    ///
    /// # Returns
    ///
    /// A history with no events. The events recorded into it are stamped with the runtime time elapsed since this call.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_sim::History;
    ///
    /// let history = History::new();
    ///
    /// assert!(history.events().is_empty());
    /// # }
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            recorded: Arc::new(Mutex::new(Recorded::default())),
        }
    }

    /// Records that a client is about to append `payload`.
    ///
    /// Call it before the append is made, and [`History::append_finished`] with the identifier it returns once the
    /// append returned.
    ///
    /// # Arguments
    ///
    /// * `payload` - The payload of the record. It must be unique within the history: the checker tells records apart
    ///   by their payload.
    ///
    /// # Returns
    ///
    /// The identifier of the call, unique among the appends and key-value calls of this history.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_sim::History;
    /// use bytes::Bytes;
    ///
    /// let history = History::new();
    ///
    /// let first = history.append_invoked(Bytes::from_static(b"a"));
    /// let second = history.append_invoked(Bytes::from_static(b"b"));
    ///
    /// assert_ne!(first, second);
    /// # }
    /// ```
    #[must_use]
    pub fn append_invoked(&self, payload: Bytes) -> u64 {
        let operation = self.next_operation();
        self.record(Event::AppendInvoked { operation, payload });
        operation
    }

    /// Records how the append `operation` ended.
    ///
    /// # Arguments
    ///
    /// * `operation` - The identifier [`History::append_invoked`] returned for the call.
    /// * `outcome` - What the caller of the append was told.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_sim::{AppendOutcome, History};
    /// use bytes::Bytes;
    ///
    /// let history = History::new();
    /// let operation = history.append_invoked(Bytes::from_static(b"a"));
    ///
    /// history.append_finished(operation, AppendOutcome::Unknown);
    ///
    /// assert_eq!(history.events().len(), 2);
    /// # }
    /// ```
    pub fn append_finished(&self, operation: u64, outcome: AppendOutcome) {
        self.record(Event::AppendFinished { operation, outcome });
    }

    /// Records that a reader starts at `from_sequence`.
    ///
    /// # Arguments
    ///
    /// * `from_sequence` - The first sequence number the reader asked for. The checker requires the session to be
    ///   handed exactly this record first, and the following ones in order.
    ///
    /// # Returns
    ///
    /// The identifier of the reading session, to pass to [`History::record_read`].
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_sim::History;
    /// use bitacora_types::SequenceNumber;
    ///
    /// let history = History::new();
    ///
    /// let first_session = history.reader_started(SequenceNumber::FIRST);
    /// let second_session = history.reader_started(SequenceNumber::FIRST);
    ///
    /// assert_ne!(first_session, second_session);
    /// # }
    /// ```
    #[must_use]
    pub fn reader_started(&self, from_sequence: SequenceNumber) -> u64 {
        let session = {
            let mut recorded = self.lock_recorded();
            recorded.next_session += 1;
            recorded.next_session
        };
        self.record(Event::ReaderStarted { session, from_sequence });
        session
    }

    /// Records that the reader of `session` was handed a record.
    ///
    /// # Arguments
    ///
    /// * `session` - The identifier [`History::reader_started`] returned.
    /// * `sequence` - The sequence number of the record.
    /// * `payload` - The payload of the record, as the reader got it.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_sim::{Event, History};
    /// use bitacora_types::SequenceNumber;
    /// use bytes::Bytes;
    ///
    /// let history = History::new();
    /// let session = history.reader_started(SequenceNumber::FIRST);
    ///
    /// history.record_read(session, SequenceNumber::FIRST, Bytes::from_static(b"a"));
    ///
    /// assert!(matches!(history.events()[1].event, Event::RecordRead { .. }));
    /// # }
    /// ```
    pub fn record_read(&self, session: u64, sequence: SequenceNumber, payload: Bytes) {
        self.record(Event::RecordRead {
            session,
            sequence,
            payload,
        });
    }

    /// Records that the low-water mark passed a registered materializer's committed checkpoint.
    ///
    /// The checker reports every such event as a violation of safe truncation (I5).
    ///
    /// # Arguments
    ///
    /// * `checkpoint` - The materializer's committed checkpoint.
    /// * `low_water_mark` - The stream's low-water mark, which is above `checkpoint + 1`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_sim::{History, check};
    /// use bitacora_types::SequenceNumber;
    ///
    /// let history = History::new();
    ///
    /// history.truncated_past_checkpoint(SequenceNumber::new(10), SequenceNumber::new(15));
    ///
    /// assert_eq!(check(&history.events()).len(), 1);
    /// # }
    /// ```
    pub fn truncated_past_checkpoint(&self, checkpoint: SequenceNumber, low_water_mark: SequenceNumber) {
        self.record(Event::TruncatedPastCheckpoint {
            checkpoint,
            low_water_mark,
        });
    }

    /// Declares that records with this payload are fillers.
    ///
    /// A filler may appear in the stream any number of times without anyone having recorded its append: the log checker
    /// does not look for the append behind it. Readers must still agree on where each one is.
    ///
    /// # Arguments
    ///
    /// * `payload` - The payload every such record has.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_sim::{History, check};
    /// use bitacora_types::SequenceNumber;
    /// use bytes::Bytes;
    ///
    /// let history = History::new();
    /// let barrier = Bytes::from_static(b"barrier");
    ///
    /// history.declare_filler(barrier.clone());
    /// let session = history.reader_started(SequenceNumber::FIRST);
    /// history.record_read(session, SequenceNumber::FIRST, barrier);
    ///
    /// assert!(check(&history.events()).is_empty());
    /// # }
    /// ```
    pub fn declare_filler(&self, payload: Bytes) {
        self.record(Event::FillerDeclared { payload });
    }

    /// Records that a client is about to call the key-value store.
    ///
    /// Call it before the call is made, and [`History::kv_returned`] with the identifier it returns once the call
    /// returned.
    ///
    /// # Arguments
    ///
    /// * `call` - What is about to be called.
    ///
    /// # Returns
    ///
    /// The identifier of the call, unique among the appends and key-value calls of this history.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_sim::{History, KvCall, KvOutcome, check_linearizability};
    /// use bytes::Bytes;
    ///
    /// let history = History::new();
    /// let key = Bytes::from_static(b"colour");
    ///
    /// let get = history.kv_invoked(KvCall::Get { key });
    /// history.kv_returned(get, KvOutcome::Read { value: None });
    ///
    /// assert!(check_linearizability(&history.events()).is_empty());
    /// # }
    /// ```
    #[must_use]
    pub fn kv_invoked(&self, call: KvCall) -> u64 {
        let operation = self.next_operation();
        self.record(Event::KvInvoked { operation, call });
        operation
    }

    /// Records how the key-value call `operation` ended.
    ///
    /// # Arguments
    ///
    /// * `operation` - The identifier [`History::kv_invoked`] returned for the call.
    /// * `outcome` - What the caller was told.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_sim::{Event, History, KvCall, KvOutcome};
    /// use bytes::Bytes;
    ///
    /// let history = History::new();
    /// let key = Bytes::from_static(b"colour");
    /// let delete = history.kv_invoked(KvCall::Delete { key });
    ///
    /// history.kv_returned(delete, KvOutcome::Written);
    ///
    /// assert!(matches!(history.events()[1].event, Event::KvReturned { .. }));
    /// # }
    /// ```
    pub fn kv_returned(&self, operation: u64, outcome: KvOutcome) {
        self.record(Event::KvReturned { operation, outcome });
    }

    /// Records something the simulator did to the cluster.
    ///
    /// The checkers do not judge these events. They are there for the person reading a failed seed, and they are
    /// counted in the report.
    ///
    /// # Arguments
    ///
    /// * `description` - What was done, in words.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_sim::History;
    ///
    /// let history = History::new();
    ///
    /// history.fault("node 2 crashes for 3s");
    ///
    /// assert_eq!(history.events().len(), 1);
    /// # }
    /// ```
    pub fn fault(&self, description: impl Into<String>) {
        self.record(Event::Fault {
            description: description.into(),
        });
    }

    /// Returns everything recorded so far, in order.
    ///
    /// Copies the list; the history can go on recording.
    ///
    /// # Returns
    ///
    /// Every event recorded through this history or one of its clones, in the order it was recorded.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_sim::History;
    ///
    /// let history = History::new();
    /// history.clone().fault("recorded through a clone");
    ///
    /// let events = history.events();
    ///
    /// assert_eq!(events.len(), 1);
    /// # }
    /// ```
    #[must_use]
    pub fn events(&self) -> Vec<TimedEvent> {
        self.lock_recorded().events.clone()
    }

    /// Hands out the identifier of the next call. Appends and key-value calls share the numbering.
    fn next_operation(&self) -> u64 {
        let mut recorded = self.lock_recorded();
        recorded.next_operation += 1;
        recorded.next_operation
    }

    /// Appends `event` to the history, stamped with the simulated time elapsed since the history began.
    fn record(&self, event: Event) {
        let at = self.started_at.elapsed();
        self.lock_recorded().events.push(TimedEvent { at, event });
    }

    /// Locks the recorded events. Recording is a push onto a vector; a panic elsewhere cannot leave it half done, so a
    /// poisoned lock is used as it is.
    fn lock_recorded(&self) -> MutexGuard<'_, Recorded> {
        self.recorded.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Default for History {
    /// Starts an empty history at the current simulated time, as [`History::new`] does.
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "history_test.rs"]
mod tests;
