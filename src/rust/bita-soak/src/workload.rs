//! The clients of one stream: a writer and a reader that write down everything they see.
//!
//! # Why this file exists
//!
//! A soak judges a cluster by what its clients saw, so it needs clients, and they must behave as an application
//! would under faults: keep writing, find out that the stream was lost, take it back, go on. They must also be
//! honest witnesses. The checker believes the history, so an append that failed without saying how must be written
//! down as "unknown", never as done or as refused. This file is those clients.
//!
//! # What it contains
//!
//! - [`SoakedStream`]: the name of a stream of a round and the history of what its clients saw.
//! - [`WorkloadSettings`]: how fast a writer appends and how large its records are.
//! - [`WorkloadCounters`]: what the history does not say, counted as it happens.
//! - [`run_writer`]: appends at a steady pace until it is told to stop, reopening its writer whenever it loses the
//!   stream.
//! - [`run_reader`]: follows the stream from its first record and records every record it is handed.
//!
//! # How it works
//!
//! Every stream of a round has a history of its own, because the checker judges one stream. The writer makes one
//! append at a time, at the pace it was given: a soak is about days of correct behaviour, not about throughput. Each
//! payload starts with the name of the stream and a counter, so it is unique in the history, and is padded to the
//! record size.
//!
//! How an append ended is recorded as the client library reported it. A commit is recorded with the sequence number
//! and the epoch of the writer. A writer that was fenced or lost contact does not know whether its record is in the
//! stream: that is recorded as unknown, the writer is dropped, and a new one takes the stream by force, which is
//! what an application's failover does. The reader follows the stream; when its session ends, for whatever reason,
//! the next one starts after the last record it was handed.
//!
//! # Invariants
//!
//! - An append is recorded before it is made and after it returned, and never as anything but what the client
//!   library said.
//! - A writer has one append in flight. When it is told to stop, it finishes that append, closes its writer and ends.
//! - Payloads are unique within a stream's history.
//! - The reader never skips: a session starts right after the last record the previous one was handed.
//!
//! # See also
//!
//! - `round.rs`: starts and stops these clients.
//! - `bitacora-sim/src/history.rs`: the history they record into.
//! - `audit.rs`: the read of the whole stream once they have stopped.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bitacora::{AppendError, BitacoraClient, SequenceNumber, StreamWriter, TakeoverPolicy};
use bitacora_sim::{AppendOutcome, History};
use bitacora_types::Epoch;
use bytes::Bytes;
use tokio::sync::watch;
use tokio::time::{Interval, MissedTickBehavior, interval, sleep, timeout};

/// How long a client waits before it tries again to open a writer or to follow the stream.
const RETRY_PAUSE: Duration = Duration::from_secs(1);

/// How long a writer that was told to stop may take to close. Closing seals the open segment, which needs the
/// cluster; a cluster that cannot do it in this time is left to the audit, which takes the stream over anyway.
const CLOSE_TIME_LIMIT: Duration = Duration::from_secs(30);

/// The byte a payload is padded with up to the record size.
const PAYLOAD_PADDING: u8 = b'.';

/// One stream of a round, and what its clients saw.
#[derive(Clone, Debug)]
pub(crate) struct SoakedStream {
    /// The name of the stream.
    pub(crate) name: String,
    /// Everything the writer, the reader and the audit of this stream did and saw. Clones record into the same
    /// history.
    pub(crate) history: History,
}

impl SoakedStream {
    /// A stream called `name` whose history starts now.
    pub(crate) fn new(name: String) -> Self {
        Self {
            name,
            history: History::new(),
        }
    }
}

/// How the writers of a round write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WorkloadSettings {
    /// How many appends a writer makes per second, at most. Must not be zero.
    pub(crate) appends_per_second: u64,
    /// The size of every record's payload, in bytes. A payload is never shorter than its unique prefix.
    pub(crate) record_bytes: usize,
}

/// What the clients of a round count as it happens, because no history says it.
#[derive(Debug, Default)]
pub(crate) struct WorkloadCounters {
    /// How often a writer was opened. Every opening after a stream's first is a takeover.
    writer_openings: AtomicU64,
}

impl WorkloadCounters {
    /// How often a writer was opened so far, over all streams.
    pub(crate) fn writer_openings(&self) -> u64 {
        self.writer_openings.load(Ordering::Relaxed)
    }
}

/// How a writer's time with the stream ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriterEnd {
    /// The writer was told to stop.
    Stopped,
    /// The writer lost the stream or the cluster, and another one must take over.
    Lost,
}

/// Appends to `stream` at the pace of `settings` until `stop` says to, reopening the writer whenever it is lost.
///
/// # Arguments
///
/// * `client` - The client of the cluster under test.
/// * `stream` - The stream to write, and the history to record into.
/// * `settings` - The pace and the record size.
/// * `counters` - Counts the openings of writers.
/// * `stop` - Becomes `true`, or is dropped, when the writer must end.
pub(crate) async fn run_writer(
    client: BitacoraClient,
    stream: SoakedStream,
    settings: WorkloadSettings,
    counters: Arc<WorkloadCounters>,
    mut stop: watch::Receiver<bool>,
) {
    let mut pace = pace_of(settings.appends_per_second);
    let mut next_record_number = 0;
    while let Some(writer) = open_writer_unless_stopped(&client, &stream.name, &mut stop).await {
        counters.writer_openings.fetch_add(1, Ordering::Relaxed);
        let mut session = WriterSession {
            writer: &writer,
            stream: &stream,
            record_bytes: settings.record_bytes,
            next_record_number,
        };
        let end = session.append_until_lost_or_stopped(&mut pace, &mut stop).await;
        next_record_number = session.next_record_number;
        if end == WriterEnd::Stopped {
            // A cluster that cannot seal the segment in time is left to the audit; see `CLOSE_TIME_LIMIT`.
            let _closed_in_time = timeout(CLOSE_TIME_LIMIT, writer.close()).await;
            return;
        }
    }
}

/// The clock of a writer that makes `appends_per_second` appends a second. A writer that was held up does not catch
/// up with a burst: its next append is one period after the late one.
fn pace_of(appends_per_second: u64) -> Interval {
    let period = Duration::from_secs(1) / u32::try_from(appends_per_second.max(1)).unwrap_or(u32::MAX);
    let mut pace = interval(period);
    pace.set_missed_tick_behavior(MissedTickBehavior::Delay);
    pace
}

/// Takes the stream by force, trying again until it works or `stop` says to end.
///
/// Returns `None` once the writer must end.
async fn open_writer_unless_stopped(
    client: &BitacoraClient,
    stream_name: &str,
    stop: &mut watch::Receiver<bool>,
) -> Option<StreamWriter> {
    loop {
        if is_stopped(stop) {
            return None;
        }
        if let Ok(writer) = client.open_writer(stream_name, TakeoverPolicy::Force).await {
            return Some(writer);
        }
        tokio::select! {
            () = sleep(RETRY_PAUSE) => {}
            _stop_changed = stop.changed() => {}
        }
    }
}

/// Whether `stop` says to end: its value is `true`, or whoever would say so is gone.
fn is_stopped(stop: &watch::Receiver<bool>) -> bool {
    *stop.borrow() || stop.has_changed().is_err()
}

/// One writer's time with its stream.
struct WriterSession<'writer> {
    /// The writer that holds the stream.
    writer: &'writer StreamWriter,
    /// The stream, and the history to record into.
    stream: &'writer SoakedStream,
    /// The size of every payload.
    record_bytes: usize,
    /// The number of the next record of this stream, counted over all of its writers.
    next_record_number: u64,
}

impl WriterSession<'_> {
    /// Appends one record per tick of `pace` until the writer is lost or `stop` says to end.
    async fn append_until_lost_or_stopped(
        &mut self,
        pace: &mut Interval,
        stop: &mut watch::Receiver<bool>,
    ) -> WriterEnd {
        loop {
            tokio::select! {
                _tick = pace.tick() => {}
                _stop_changed = stop.changed() => {}
            }
            if is_stopped(stop) {
                return WriterEnd::Stopped;
            }
            if self.append_next_record().await == WriterEnd::Lost {
                return WriterEnd::Lost;
            }
        }
    }

    /// Appends the next record and records how the append ended.
    ///
    /// Returns [`WriterEnd::Lost`] if the writer can no longer be used, and [`WriterEnd::Stopped`] otherwise, which
    /// here only means "go on".
    async fn append_next_record(&mut self) -> WriterEnd {
        let payload = payload_of(&self.stream.name, self.next_record_number, self.record_bytes);
        self.next_record_number += 1;
        let operation = self.stream.history.append_invoked(payload.clone());
        let appended = self.writer.append(payload).await;
        let (outcome, end) = outcome_of(&appended, self.writer.epoch());
        self.stream.history.append_finished(operation, outcome);
        end
    }
}

/// Says what the history records for an append that ended as `appended`, made by a writer of `epoch`, and whether
/// the writer can go on.
///
/// A writer that was fenced or lost contact does not know where its record is: the outcome is unknown and the writer
/// is lost. A record that was refused before it got a sequence number was not appended; a closed writer is lost too,
/// while one that only refused a record can go on.
fn outcome_of(appended: &Result<SequenceNumber, AppendError>, epoch: Epoch) -> (AppendOutcome, WriterEnd) {
    match appended {
        Ok(sequence) => {
            let sequence = *sequence;
            (AppendOutcome::Committed { sequence, epoch }, WriterEnd::Stopped)
        }
        Err(AppendError::Fenced | AppendError::Indeterminate { .. }) => (AppendOutcome::Unknown, WriterEnd::Lost),
        Err(AppendError::Closed) => (AppendOutcome::NotAppended, WriterEnd::Lost),
        Err(AppendError::Rejected(_)) => (AppendOutcome::NotAppended, WriterEnd::Stopped),
    }
}

/// Builds the payload of record `record_number` of `stream_name`: the two, which no other record of the run shares,
/// padded to `record_bytes`.
pub(crate) fn payload_of(stream_name: &str, record_number: u64, record_bytes: usize) -> Bytes {
    let mut payload = format!("{stream_name}/{record_number}").into_bytes();
    if payload.len() < record_bytes {
        payload.resize(record_bytes, PAYLOAD_PADDING);
    }
    Bytes::from(payload)
}

/// Follows `stream` from its first record and records every record it is handed, until the task is aborted.
///
/// # Arguments
///
/// * `client` - The client of the cluster under test.
/// * `stream` - The stream to read, and the history to record into.
pub(crate) async fn run_reader(client: BitacoraClient, stream: SoakedStream) {
    let reader = client.open_reader(&stream.name);
    let mut read_through = SequenceNumber::ZERO;
    loop {
        let from_sequence = read_through.next();
        if let Ok(mut following) = reader.follow_from(from_sequence).await {
            let session = stream.history.reader_started(from_sequence);
            while let Some(Ok(record)) = following.next().await {
                read_through = record.sequence;
                stream.history.record_read(session, record.sequence, record.payload);
            }
        }
        // The session could not start or has ended, for whatever reason: the next one goes on where it stopped.
        sleep(RETRY_PAUSE).await;
    }
}

#[cfg(test)]
#[path = "workload_test.rs"]
mod tests;
