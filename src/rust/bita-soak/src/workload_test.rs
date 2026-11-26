//! Tests of the clients of a stream.
//!
//! # What is covered
//!
//! - Payloads: unique per record, padded to the record size, never cut short.
//! - How the outcome of an append is recorded: commits with their sequence number, the two kinds of unknown, and
//!   the two kinds of refusal.
//! - A writer on the in-process cluster: it appends at its pace, stops when it is told to, and takes the stream
//!   back after another writer took it.
//! - A reader: it is handed every record the writer committed, in order, across sessions.
//!
//! # How the fixtures work
//!
//! `cluster_with_stream` starts the in-process cluster of `test_support.rs` and creates the stream [`STREAM`].
//! `Running` is a writer and a reader started on it, with the handle that stops the writer. Time is virtual: a test
//! that lets ten seconds pass does not wait.

use bitacora::{StreamOptions, TakeoverPolicy};
use bitacora_segment::format::EncodeError;
use bitacora_sim::{Event, check};

use super::*;
use crate::report::HistoryCounts;
use crate::test_support::InProcessCluster;

/// The stream the tests write.
const STREAM: &str = "soak-test-1-0";

/// Five appends a second, in records of 32 bytes.
const SETTINGS: WorkloadSettings = WorkloadSettings {
    appends_per_second: 5,
    record_bytes: 32,
};

/// The in-process cluster with [`STREAM`] created.
async fn cluster_with_stream() -> InProcessCluster {
    let cluster = InProcessCluster::start().await;
    cluster
        .soaked
        .client
        .admin()
        .create_stream(STREAM, StreamOptions::default())
        .await
        .expect("the stream is created");
    cluster
}

/// Lets the workload run for three seconds and then opens another writer on [`STREAM`] by force, between two
/// appends of the workload's writer, which finds out with its next append.
async fn take_the_stream_away(cluster: &InProcessCluster) -> bitacora::StreamWriter {
    sleep(Duration::from_millis(3_100)).await;
    cluster
        .soaked
        .client
        .open_writer(STREAM, TakeoverPolicy::Force)
        .await
        .expect("the stream can be taken by force")
}

/// A writer and a reader at work on [`STREAM`].
struct Running {
    /// The stream and the history both record into.
    stream: SoakedStream,
    /// What the writer counts.
    counters: Arc<WorkloadCounters>,
    /// Tells the writer to stop.
    stop_writer: watch::Sender<bool>,
    /// The writer's task.
    writer: tokio::task::JoinHandle<()>,
    /// The reader's task.
    reader: tokio::task::JoinHandle<()>,
}

impl Running {
    /// Starts a writer and a reader on [`STREAM`].
    fn start(cluster: &InProcessCluster) -> Self {
        let stream = SoakedStream::new(STREAM.to_owned());
        let counters = Arc::new(WorkloadCounters::default());
        let (stop_writer, writer_stopped) = watch::channel(false);
        let writer = tokio::spawn(run_writer(
            cluster.soaked.client.clone(),
            stream.clone(),
            SETTINGS,
            Arc::clone(&counters),
            writer_stopped,
        ));
        let reader = tokio::spawn(run_reader(cluster.soaked.client.clone(), stream.clone()));
        Self {
            stream,
            counters,
            stop_writer,
            writer,
            reader,
        }
    }

    /// Tells the writer to stop, waits for it, gives the reader a moment to catch up, and returns what both saw.
    async fn stop(self) -> Vec<bitacora_sim::TimedEvent> {
        self.stop_writer.send(true).expect("the writer listens");
        self.writer.await.expect("the writer ends by itself");
        sleep(Duration::from_secs(3)).await;
        self.reader.abort();
        self.stream.history.events()
    }
}

#[test]
fn payload_starts_with_the_stream_and_the_number_of_the_record() {
    let payload = payload_of("soak-7-1-0", 12, 32);

    assert!(payload.starts_with(b"soak-7-1-0/12"), "{payload:?}");
}

#[test]
fn payload_is_padded_to_the_record_size() {
    let payload = payload_of("soak-7-1-0", 12, 32);

    assert_eq!(payload.len(), 32);
}

/// A record size smaller than the unique part would make two records the same.
#[test]
fn payload_longer_than_the_record_size_is_not_cut() {
    let payload = payload_of("soak-7-1-0", 12, 4);

    assert_eq!(payload, Bytes::from_static(b"soak-7-1-0/12"));
}

#[test]
fn payloads_of_two_records_differ() {
    assert_ne!(payload_of("soak-7-1-0", 1, 32), payload_of("soak-7-1-0", 11, 32));
}

#[test]
fn commit_is_recorded_with_its_sequence_number_and_the_epoch_of_the_writer() {
    let (outcome, end) = outcome_of(&Ok(SequenceNumber::new(12)), Epoch::new(3));

    let expected = AppendOutcome::Committed {
        sequence: SequenceNumber::new(12),
        epoch: Epoch::new(3),
    };
    assert_eq!((outcome, end), (expected, WriterEnd::Stopped));
}

/// The record of a fenced writer may be in the stream, recovered by the writer that took over, or not.
#[test]
fn append_of_a_fenced_writer_is_unknown_and_the_writer_is_lost() {
    let recorded = outcome_of(&Err(AppendError::Fenced), Epoch::new(3));

    assert_eq!(recorded, (AppendOutcome::Unknown, WriterEnd::Lost));
}

#[test]
fn append_whose_outcome_the_writer_did_not_learn_is_unknown_and_the_writer_is_lost() {
    let lost_contact = AppendError::Indeterminate {
        reason: "no replica answered".to_owned(),
    };

    let recorded = outcome_of(&Err(lost_contact), Epoch::new(3));

    assert_eq!(recorded, (AppendOutcome::Unknown, WriterEnd::Lost));
}

#[test]
fn append_to_a_closed_writer_was_not_appended_and_the_writer_is_lost() {
    let recorded = outcome_of(&Err(AppendError::Closed), Epoch::new(3));

    assert_eq!(recorded, (AppendOutcome::NotAppended, WriterEnd::Lost));
}

#[test]
fn record_the_writer_refused_was_not_appended_and_the_writer_goes_on() {
    let refused = AppendError::Rejected(EncodeError::NoRecords);

    let recorded = outcome_of(&Err(refused), Epoch::new(3));

    assert_eq!(recorded, (AppendOutcome::NotAppended, WriterEnd::Stopped));
}

#[tokio::test(start_paused = true)]
async fn writer_appends_at_its_pace() {
    let cluster = cluster_with_stream().await;
    let running = Running::start(&cluster);
    sleep(Duration::from_secs(10)).await;

    let events = running.stop().await;

    let committed = HistoryCounts::of(&events).appends_committed;
    assert!(
        (40..=51).contains(&committed),
        "{committed} appends in ten seconds at five a second"
    );
}

#[tokio::test(start_paused = true)]
async fn commits_are_recorded_with_the_epoch_of_the_writer() {
    let cluster = cluster_with_stream().await;
    let running = Running::start(&cluster);
    sleep(Duration::from_secs(2)).await;

    let events = running.stop().await;

    let mut epochs = Vec::new();
    for timed in &events {
        if let Event::AppendFinished {
            outcome: AppendOutcome::Committed { epoch, .. },
            ..
        } = &timed.event
        {
            epochs.push(*epoch);
        }
    }
    epochs.dedup();
    assert_eq!(epochs, [Epoch::new(1)]);
}

#[tokio::test(start_paused = true)]
async fn writer_opens_the_stream_once_while_nothing_goes_wrong() {
    let cluster = cluster_with_stream().await;
    let running = Running::start(&cluster);
    sleep(Duration::from_secs(5)).await;
    let counters = Arc::clone(&running.counters);

    running.stop().await;

    assert_eq!(counters.writer_openings(), 1);
}

/// What an application's failover does: the stream was taken away, so take it back and go on.
#[tokio::test(start_paused = true)]
async fn writer_takes_the_stream_back_after_another_writer_took_it() {
    let cluster = cluster_with_stream().await;
    let running = Running::start(&cluster);
    let intruder = take_the_stream_away(&cluster).await;
    sleep(Duration::from_secs(5)).await;
    drop(intruder);
    let counters = Arc::clone(&running.counters);

    let events = running.stop().await;

    assert_eq!(counters.writer_openings(), 2);
    assert_eq!(check(&events), []);
}

#[tokio::test(start_paused = true)]
async fn append_cut_short_by_a_takeover_is_recorded_as_unknown() {
    let cluster = cluster_with_stream().await;
    let running = Running::start(&cluster);
    let intruder = take_the_stream_away(&cluster).await;
    sleep(Duration::from_secs(5)).await;
    drop(intruder);

    let events = running.stop().await;

    assert_eq!(HistoryCounts::of(&events).appends_unknown, 1);
}

#[tokio::test(start_paused = true)]
async fn reader_is_handed_every_record_the_writer_committed() {
    let cluster = cluster_with_stream().await;
    let running = Running::start(&cluster);
    sleep(Duration::from_secs(10)).await;

    let events = running.stop().await;

    let counts = HistoryCounts::of(&events);
    assert_eq!(counts.records_read, counts.appends_committed);
    assert_eq!(check(&events), []);
}

#[tokio::test(start_paused = true)]
async fn writer_that_is_told_to_stop_before_it_could_open_ends() {
    let cluster = InProcessCluster::start().await;
    let stream = SoakedStream::new("no-such-stream".to_owned());
    let (stop_writer, writer_stopped) = watch::channel(false);
    let writer = tokio::spawn(run_writer(
        cluster.soaked.client.clone(),
        stream,
        SETTINGS,
        Arc::new(WorkloadCounters::default()),
        writer_stopped,
    ));
    sleep(Duration::from_secs(5)).await;

    stop_writer.send(true).expect("the writer listens");

    assert!(timeout(Duration::from_secs(10), writer).await.is_ok());
}
