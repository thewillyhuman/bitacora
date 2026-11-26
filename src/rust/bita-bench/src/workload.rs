//! The workloads: what the benchmark does to a cluster, and what it measures while doing it.
//!
//! # Why this file exists
//!
//! The design's performance targets (section 16) are about two things: how long an append takes to
//! commit, and how many bytes per second a stream takes. Checking them, and noticing when a change makes
//! them worse, needs a tool that drives a stream through the same client library an application uses and
//! measures every append. This file is that driving and measuring; everything around it only parses
//! options and prints.
//!
//! # What it contains
//!
//! - [`AppendWorkload`] and [`run_append_workloads`]: appends to one or more streams, with a chosen
//!   number of appends in flight, measuring append-to-commit latency.
//! - [`stream_names`] and [`create_missing_streams`]: which streams an append workload writes, and
//!   creating them before a run.
//! - [`ReadWorkload`] and [`run_read_workload`]: a range scan of a stream, measuring records and bytes
//!   per second.
//! - [`WorkloadLimit`]: when a workload stops.
//! - [`AppendMeasurement`], [`ReadMeasurement`]: what was measured.
//! - [`WorkloadError`]: why a workload could not run at all.
//! - Private steps: `run_append_workload` (one stream: open the writer, run the workers, close the
//!   writer), `run_append_worker` (the measured loop) and `SharedAppendState` (what the workers of a
//!   stream share and the questions they ask it); `first_sequence_to_read` and `records_to_ask_for` for
//!   the scan.
//!
//! # How it works
//!
//! **Appends.** A stream has one writer, so an append workload opens one writer per stream and shares it
//! among `appends_in_flight` workers. Each worker takes the number of the next send from a shared
//! counter, waits until that send is due if the workload is paced, appends, and waits for the append to
//! commit. The time from the start of the append to its commit goes into the worker's own histogram; the
//! histograms are merged at the end. So the number of workers is the number of appends in flight, and
//! recording a latency takes no lock and no allocation.
//!
//! When the workload is paced, latency is measured from the moment the send was due, not from the moment
//! it went out. If the cluster stalls, the sends that pile up behind the stall are then counted as slow,
//! which they were for whoever wanted to append at that rate. Measuring from the actual send would hide
//! exactly the stalls a benchmark is supposed to find.
//!
//! A workload limited by a record count sends whole appends: the count is rounded up to a multiple of the
//! records per append. One limited by a duration stops starting appends when the time is up, and waits
//! for those in flight.
//!
//! The first append that fails stops the workload. What was measured until then is still reported,
//! together with the failure.
//!
//! **Reads.** The read workload scans the stream in ranges from a starting sequence number until it
//! reaches the end of the stream or its limit, and counts records and payload bytes.
//!
//! # Invariants
//!
//! - Time is read through Tokio's clock only, so the workloads run under a paused clock in tests.
//! - A writer opened by a workload is closed before the workload returns.
//! - The payload of every record is one shared buffer; no payload is allocated per record.
//! - `run_append_worker` is the measured loop. It takes no lock, and what it awaits per append is the
//!   wait for a paced send and the append itself, in that order; the steps around them are plain
//!   functions. Both waits stay in that one function, so that this can be read off it.
//!
//! # See also
//!
//! - `histogram.rs`: where latencies are counted.
//! - `pacing.rs`: when a paced send is due.
//! - `report.rs`: what is printed from a measurement.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use bitacora::{
    BitacoraClient, LogRecord, OpenError, ReadError, SequenceNumber, SequenceRange, StreamOptions, StreamReader,
    StreamWriter, TakeoverPolicy,
};
use bytes::Bytes;
use thiserror::Error;
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep_until};

use crate::histogram::LatencyHistogram;
use crate::pacing::Pace;

/// The byte every benchmark payload is filled with. Payloads are opaque to bitacora and nothing is
/// compressed, so their content does not change what is measured.
const PAYLOAD_BYTE: u8 = b'b';

/// How many sequence numbers one request of the read workload asks for.
const READ_RANGE_RECORDS: u64 = 10_000;

/// When a workload stops.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WorkloadLimit {
    /// After this many records.
    Records(u64),
    /// After this long.
    Duration(Duration),
}

/// An append workload on one stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AppendWorkload {
    /// The stream to append to. With more than one stream, the streams are `<stream>-0`, `<stream>-1`
    /// and so on.
    pub(crate) stream: String,
    /// How many streams are written at once, each by a writer of its own with the full number of appends
    /// in flight.
    pub(crate) stream_count: u64,
    /// The payload size of every record.
    pub(crate) record_bytes: usize,
    /// How many records one append carries.
    pub(crate) records_per_append: usize,
    /// How many appends per stream are in flight at any moment.
    pub(crate) appends_in_flight: usize,
    /// When the workload stops, per stream.
    pub(crate) limit: WorkloadLimit,
    /// The rate to pace each stream's appends to, in records per second; `None` appends as fast as the
    /// cluster commits.
    pub(crate) target_records_per_second: Option<u64>,
    /// Whether to wait for a live writer's lease or to take the stream at once.
    pub(crate) takeover: TakeoverPolicy,
}

/// What an append workload measured.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AppendMeasurement {
    /// How many appends committed.
    pub(crate) appends: u64,
    /// How many records those appends carried.
    pub(crate) records: u64,
    /// How many payload bytes those records carried.
    pub(crate) payload_bytes: u64,
    /// How long the workload ran, from the first send to the last commit.
    pub(crate) elapsed: Duration,
    /// The append-to-commit latency of every append that committed.
    pub(crate) latencies: LatencyHistogram,
    /// Why the workload stopped early, if it did.
    pub(crate) failure: Option<String>,
}

impl AppendMeasurement {
    /// The measurement of a workload that has not appended anything yet.
    fn nothing_measured() -> Self {
        Self {
            appends: 0,
            records: 0,
            payload_bytes: 0,
            elapsed: Duration::ZERO,
            latencies: LatencyHistogram::new(),
            failure: None,
        }
    }

    /// Adds what another stream measured: counts are summed, latencies merged, the run time is the
    /// longer of the two, and the failure that was added first is kept.
    fn add(&mut self, measured: Self) {
        self.appends += measured.appends;
        self.records += measured.records;
        self.payload_bytes += measured.payload_bytes;
        self.elapsed = self.elapsed.max(measured.elapsed);
        self.latencies.merge(&measured.latencies);
        if self.failure.is_none() {
            self.failure = measured.failure;
        }
    }
}

/// A read workload: a scan of one stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReadWorkload {
    /// The stream to read.
    pub(crate) stream: String,
    /// The first sequence number to read; `None` starts at the oldest record still kept.
    pub(crate) from: Option<u64>,
    /// When the scan stops, unless it reaches the end of the stream first.
    pub(crate) limit: WorkloadLimit,
}

/// What a read workload measured.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReadMeasurement {
    /// How many records were read.
    pub(crate) records: u64,
    /// How many payload bytes those records carried.
    pub(crate) payload_bytes: u64,
    /// How long the scan ran.
    pub(crate) elapsed: Duration,
    /// Why the scan stopped early, if it did.
    pub(crate) failure: Option<String>,
}

impl ReadMeasurement {
    /// The measurement of a scan that has not read anything yet.
    const fn nothing_read() -> Self {
        Self {
            records: 0,
            payload_bytes: 0,
            elapsed: Duration::ZERO,
            failure: None,
        }
    }

    /// Counts the records one request returned, and their payload bytes.
    fn count(&mut self, found: &[LogRecord]) {
        for record in found {
            self.payload_bytes += record.payload.len() as u64;
        }
        self.records += found.len() as u64;
    }
}

/// Why a workload could not run.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub(crate) enum WorkloadError {
    /// The workload's numbers make no workload.
    #[error("invalid workload: {reason}")]
    InvalidWorkload {
        /// Which number is wrong.
        reason: String,
    },
    /// The writer role of a stream could not be taken.
    #[error("cannot become the writer of stream {stream:?}: {reason}")]
    CannotOpen {
        /// The stream that could not be opened.
        stream: String,
        /// Why not.
        reason: OpenError,
    },
    /// The stream to read could not be found.
    #[error("cannot read stream {stream:?}: {reason}")]
    CannotRead {
        /// The stream that could not be read.
        stream: String,
        /// Why not.
        reason: ReadError,
    },
    /// A stream that the workload was told to create could not be created.
    #[error("cannot create stream {stream:?}: {reason}")]
    CannotCreate {
        /// The stream that could not be created.
        stream: String,
        /// What the metadata store answered.
        reason: String,
    },
}

/// What the workers of one stream share.
struct SharedAppendState {
    /// The stream's writer.
    writer: StreamWriter,
    /// The payload of every record.
    payload: Bytes,
    /// How many records one append carries.
    records_per_append: usize,
    /// The number of the next send; every worker takes its sends from here.
    next_send: AtomicU64,
    /// How many sends the workload makes, if it is limited by a record count.
    send_limit: Option<u64>,
    /// When the workload stops starting sends, if it is limited by a duration.
    deadline: Option<Instant>,
    /// When each send is due, if the workload is paced.
    pace: Option<Pace>,
    /// When the workload started.
    started_at: Instant,
    /// Set by the worker whose append failed, so that the others stop.
    has_failed: AtomicBool,
}

impl SharedAppendState {
    /// What the workers of one stream share for a workload that starts at `started_at`.
    fn new(writer: StreamWriter, workload: &AppendWorkload, started_at: Instant) -> Self {
        let records_per_append = workload.records_per_append as u64;
        let (send_limit, deadline) = match workload.limit {
            WorkloadLimit::Records(records) => (Some(records.div_ceil(records_per_append)), None),
            WorkloadLimit::Duration(duration) => (None, Some(started_at + duration)),
        };
        let target_rate = workload.target_records_per_second;
        let pace = target_rate.map(|rate| Pace::new(rate, records_per_append));
        Self {
            writer,
            payload: Bytes::from(vec![PAYLOAD_BYTE; workload.record_bytes]),
            records_per_append: workload.records_per_append,
            next_send: AtomicU64::new(0),
            send_limit,
            deadline,
            pace,
            started_at,
            has_failed: AtomicBool::new(false),
        }
    }

    /// When the send with this number is due, if the workload is paced.
    fn due_at(&self, send_number: u64) -> Option<Instant> {
        self.pace.map(|pace| self.started_at + pace.due_after(send_number))
    }

    /// Whether a send would start at or after the deadline of a workload that is limited by a duration.
    /// A send starts when it is due, and now if the workload is not paced or the send is overdue.
    fn is_too_late_to_start(&self, due_at: Option<Instant>) -> bool {
        let starts_at = due_at.unwrap_or_else(Instant::now).max(Instant::now());
        self.deadline.is_some_and(|deadline| starts_at >= deadline)
    }
}

/// What one worker measured.
struct WorkerMeasurement {
    /// The latency of every append this worker saw commit.
    latencies: LatencyHistogram,
    /// Why this worker stopped early, if an append of its own failed.
    failure: Option<String>,
}

impl WorkerMeasurement {
    /// The measurement of a worker that has not appended anything yet.
    fn nothing_measured() -> Self {
        Self {
            latencies: LatencyHistogram::new(),
            failure: None,
        }
    }
}

/// Runs an append workload on every one of its streams at once and adds the measurements up.
///
/// # Arguments
///
/// * `client` - The client to open writers from.
/// * `workload` - What to do. Its limit, its appends in flight and its rate apply to each stream.
///
/// # Returns
///
/// The measurements of all streams together: appends, records and bytes summed, latencies merged, and the
/// longest run time. If any stream failed, the first failure is reported and the rest is what was
/// measured anyway.
///
/// # Errors
///
/// * [`WorkloadError::InvalidWorkload`] if a number of the workload is zero.
/// * [`WorkloadError::CannotOpen`] if the writer role of a stream could not be taken. Streams that were
///   opened are closed again.
pub(crate) async fn run_append_workloads(
    client: &BitacoraClient,
    workload: &AppendWorkload,
) -> Result<AppendMeasurement, WorkloadError> {
    validate(workload)?;
    let mut streams = JoinSet::new();
    for stream_name in stream_names(workload) {
        let client = client.clone();
        let workload = workload.clone();
        streams.spawn(async move { run_append_workload(&client, &workload, &stream_name).await });
    }
    let mut total = AppendMeasurement::nothing_measured();
    let mut first_error = None;
    while let Some(joined) = streams.join_next().await {
        match joined.expect("a stream's workload does not panic") {
            Ok(measured) => total.add(measured),
            Err(workload_error) => first_error = first_error.or(Some(workload_error)),
        }
    }
    match first_error {
        Some(workload_error) => Err(workload_error),
        None => Ok(total),
    }
}

/// The names of the streams an append workload writes: the stream itself if there is one, otherwise the
/// stream's name followed by `-0`, `-1` and so on.
pub(crate) fn stream_names(workload: &AppendWorkload) -> Vec<String> {
    if workload.stream_count == 1 {
        return vec![workload.stream.clone()];
    }
    let mut names = Vec::new();
    for stream_number in 0..workload.stream_count {
        names.push(format!("{}-{stream_number}", workload.stream));
    }
    names
}

/// Creates the streams of an append workload that do not exist yet, with the default replication of
/// three replicas and a write quorum of two.
///
/// # Arguments
///
/// * `client` - The client to administer streams with.
/// * `workload` - The workload whose streams are needed.
///
/// # Errors
///
/// Returns [`WorkloadError::CannotCreate`] if a stream does not exist and cannot be created.
pub(crate) async fn create_missing_streams(
    client: &BitacoraClient,
    workload: &AppendWorkload,
) -> Result<(), WorkloadError> {
    let admin = client.admin();
    for stream_name in stream_names(workload) {
        if admin.describe_stream(&stream_name).await.is_ok() {
            continue;
        }
        let created = admin.create_stream(&stream_name, StreamOptions::default()).await;
        if let Err(reason) = created {
            return Err(WorkloadError::CannotCreate {
                stream: stream_name,
                reason: reason.to_string(),
            });
        }
    }
    Ok(())
}

/// Checks that a workload's numbers make a workload.
fn validate(workload: &AppendWorkload) -> Result<(), WorkloadError> {
    let zero_value = if workload.stream_count == 0 {
        Some("the number of streams")
    } else if workload.records_per_append == 0 {
        Some("the records per append")
    } else if workload.appends_in_flight == 0 {
        Some("the appends in flight")
    } else if workload.target_records_per_second == Some(0) {
        Some("the target rate")
    } else {
        None
    };
    match zero_value {
        Some(name) => Err(WorkloadError::InvalidWorkload {
            reason: format!("{name} must be at least 1"),
        }),
        None => Ok(()),
    }
}

/// Runs the append workload on one stream: opens its writer, runs the workers, closes the writer.
///
/// The run time is taken from the moment the writer is open to the moment the last worker returns, so
/// neither the takeover nor the closing of the writer counts.
async fn run_append_workload(
    client: &BitacoraClient,
    workload: &AppendWorkload,
    stream_name: &str,
) -> Result<AppendMeasurement, WorkloadError> {
    let writer = match client.open_writer(stream_name, workload.takeover).await {
        Ok(writer) => writer,
        Err(reason) => {
            return Err(WorkloadError::CannotOpen {
                stream: stream_name.to_owned(),
                reason,
            });
        }
    };
    let started_at = Instant::now();
    let shared = Arc::new(SharedAppendState::new(writer, workload, started_at));
    let mut measured = run_append_workers(&shared, workload.appends_in_flight).await;
    let elapsed = started_at.elapsed();
    if let Some(close_failure) = close_writer_of(shared).await {
        measured.failure = measured.failure.or(Some(close_failure));
    }
    let appends = measured.latencies.count();
    let records = appends * workload.records_per_append as u64;
    Ok(AppendMeasurement {
        appends,
        records,
        payload_bytes: records * workload.record_bytes as u64,
        elapsed,
        latencies: measured.latencies,
        failure: measured.failure,
    })
}

/// Runs `worker_count` workers on one stream until all of them have returned, and merges what they
/// measured. If several appends failed, the failure of the worker that returned first is kept.
async fn run_append_workers(shared: &Arc<SharedAppendState>, worker_count: usize) -> WorkerMeasurement {
    let mut workers = JoinSet::new();
    for _worker in 0..worker_count {
        workers.spawn(run_append_worker(Arc::clone(shared)));
    }
    let mut total = WorkerMeasurement::nothing_measured();
    while let Some(joined) = workers.join_next().await {
        let measured = joined.expect("a worker does not panic");
        total.latencies.merge(&measured.latencies);
        total.failure = total.failure.or(measured.failure);
    }
    total
}

/// Closes the writer of a stream whose workers have all returned.
///
/// # Returns
///
/// The failure to report if the writer did not close cleanly, otherwise `None`.
async fn close_writer_of(shared: Arc<SharedAppendState>) -> Option<String> {
    // Every worker has returned, so this is the last reference and the writer can be taken out.
    let Ok(shared) = Arc::try_unwrap(shared) else {
        return None;
    };
    match shared.writer.close().await {
        Ok(_closed) => None,
        Err(close_error) => Some(format!("the writer did not close cleanly: {close_error}")),
    }
}

/// One worker: appends, one append at a time, until the workload is over or an append fails.
async fn run_append_worker(shared: Arc<SharedAppendState>) -> WorkerMeasurement {
    let mut measured = WorkerMeasurement::nothing_measured();
    while !shared.has_failed.load(Ordering::Relaxed) {
        let send_number = shared.next_send.fetch_add(1, Ordering::Relaxed);
        if shared.send_limit.is_some_and(|limit| send_number >= limit) {
            break;
        }
        let due_at = shared.due_at(send_number);
        if shared.is_too_late_to_start(due_at) {
            break;
        }
        if let Some(due_at) = due_at {
            sleep_until(due_at).await;
        }
        // A paced append is measured from when it was due: see the file header.
        let measured_from = due_at.unwrap_or_else(Instant::now);
        let appended = if shared.records_per_append == 1 {
            shared.writer.append(shared.payload.clone()).await.err()
        } else {
            let payloads = vec![shared.payload.clone(); shared.records_per_append];
            shared.writer.append_batch(payloads).await.err()
        };
        if let Some(append_error) = appended {
            shared.has_failed.store(true, Ordering::Relaxed);
            measured.failure = Some(format!("an append failed: {append_error}"));
            break;
        }
        measured.latencies.record(measured_from.elapsed());
    }
    measured
}

/// Scans a stream from a starting sequence number and counts what it reads.
///
/// # Arguments
///
/// * `client` - The client to open the reader from.
/// * `workload` - The stream, where to start, and when to stop.
///
/// # Returns
///
/// How many records and payload bytes were read and how long it took. The scan ends at the end of the
/// stream if that comes before the limit. A read that fails ends the scan and is reported as the
/// measurement's failure.
///
/// # Errors
///
/// Returns [`WorkloadError::CannotRead`] if the stream does not exist or its metadata cannot be read.
pub(crate) async fn run_read_workload(
    client: &BitacoraClient,
    workload: &ReadWorkload,
) -> Result<ReadMeasurement, WorkloadError> {
    let reader = client.open_reader(&workload.stream);
    let mut next_sequence = first_sequence_to_read(&reader, workload).await?;
    let started_at = Instant::now();
    let mut measured = ReadMeasurement::nothing_read();
    loop {
        let records_wanted = records_to_ask_for(workload.limit, measured.records, started_at);
        if records_wanted == 0 {
            break;
        }
        let last_sequence = next_sequence.advanced_by(records_wanted - 1);
        let range =
            SequenceRange::new(next_sequence, last_sequence).expect("the last sequence is not before the first");
        let found = match reader.read_range(range).await {
            Ok(found) => found,
            Err(read_error) => {
                measured.failure = Some(format!("a read failed: {read_error}"));
                break;
            }
        };
        measured.count(&found);
        let found_count = found.len() as u64;
        next_sequence = next_sequence.advanced_by(found_count);
        if found_count < records_wanted {
            // The range reached past the last committed record: the stream ends here.
            break;
        }
    }
    measured.elapsed = started_at.elapsed();
    Ok(measured)
}

/// Where a scan starts: at the sequence number the workload names, or at the oldest record the stream
/// still keeps.
///
/// # Errors
///
/// Returns [`WorkloadError::CannotRead`] if the stream does not exist or its metadata cannot be read.
async fn first_sequence_to_read(
    reader: &StreamReader,
    workload: &ReadWorkload,
) -> Result<SequenceNumber, WorkloadError> {
    if let Some(sequence) = workload.from {
        return Ok(SequenceNumber::new(sequence.max(1)));
    }
    match reader.low_water_mark().await {
        Ok(low_water_mark) => Ok(low_water_mark),
        Err(reason) => Err(WorkloadError::CannotRead {
            stream: workload.stream.clone(),
            reason,
        }),
    }
}

/// How many records the next request of a scan asks for: a full range, less if the record limit leaves
/// less, and none once the limit is reached.
fn records_to_ask_for(limit: WorkloadLimit, records_read: u64, started_at: Instant) -> u64 {
    match limit {
        WorkloadLimit::Records(record_limit) => READ_RANGE_RECORDS.min(record_limit - records_read),
        WorkloadLimit::Duration(duration) if started_at.elapsed() >= duration => 0,
        WorkloadLimit::Duration(_) => READ_RANGE_RECORDS,
    }
}

#[cfg(test)]
#[path = "workload_test.rs"]
mod tests;
