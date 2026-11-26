//! Tests of the key-value store against an in-process cluster.
//!
//! # What is covered
//!
//! - One store alone: puts, deletes, batches, reads of history, scans, refused writes.
//! - Restarting: a reopened store has everything, with and without a checkpoint to restore from.
//! - Failover: a replaced store can neither write nor answer a linearizable read; writes in flight during
//!   a takeover are applied once or not at all, and the new store sees exactly what was committed.
//! - Truncation: the store keeps answering, and can be reopened, after the log below its checkpoint is
//!   gone.
//!
//! # How the fixtures work
//!
//! [`Harness`] is the in-process cluster of `bitacora-testkit` with one stream prepared for a store, and
//! an in-memory artifact store. Nothing is mocked: stores write through the real client, the real
//! materializer runner and the real page store. Time is Tokio's paused clock. The controller only runs
//! when a test calls [`Harness::run_controller_pass`].

use std::collections::BTreeMap;

use bitacora_artifact::MemoryArtifactStore;
use bitacora_client::{ClientSettings, WriterSettings};
use bitacora_controller::{Controller, ControllerSettings};
use bitacora_meta::RetentionPolicy;
use bitacora_testkit::Cluster;
use bitacora_types::{MAX_RECORD_PAYLOAD_BYTES, ReplicationSettings, SequenceRange};
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};

use super::*;

/// The stream the stores under test live on.
const STREAM: &str = "inventory";

/// How long a test waits for something that must happen. Simulated time.
const PATIENCE: Duration = Duration::from_secs(300);

/// How often the stores under test checkpoint.
const CHECKPOINT_AFTER: Duration = Duration::from_secs(10);

/// A key or value from text.
fn bytes(text: &str) -> Bytes {
    Bytes::copy_from_slice(text.as_bytes())
}

/// An in-process cluster with [`STREAM`] prepared for a key-value store.
struct Harness {
    /// The replicas and the metadata store.
    cluster: Cluster,
    /// Where the stores keep their layers.
    artifact_store: MemoryArtifactStore,
}

impl Harness {
    /// Starts three replicas, creates the stream, places its first segment and prepares it for a store.
    async fn start() -> Self {
        let cluster = Cluster::start(3).await;
        cluster
            .metadata()
            .create_stream(STREAM, ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
            .await
            .expect("a fresh metadata store accepts a stream");
        KvStore::initialize(cluster.metadata(), STREAM)
            .await
            .expect("the stream exists");
        let harness = Self {
            cluster,
            artifact_store: MemoryArtifactStore::new(),
        };
        harness.run_controller_pass().await;
        harness
    }

    /// What a store whose writer uses `writer_settings` is built from.
    fn dependencies(&self, writer_settings: WriterSettings) -> KvDependencies {
        let client = BitacoraClient::new(
            self.cluster.metadata().clone(),
            self.cluster.connector(),
            self.cluster.clock(),
            ClientSettings {
                writer: writer_settings,
                ..ClientSettings::default()
            },
        );
        KvDependencies {
            client,
            metadata: self.cluster.metadata().clone(),
            artifact_store: Arc::new(self.artifact_store.clone()),
        }
    }

    /// Opens a store that takes the stream at once and checkpoints every [`CHECKPOINT_AFTER`].
    async fn open(&self) -> KvStore {
        self.open_with(WriterSettings::default()).await
    }

    /// Opens a store whose writer uses `writer_settings`. The controller runs first: a writer opens
    /// its segment on the standby replicas, and the store opened before used the last ones up.
    async fn open_with(&self, writer_settings: WriterSettings) -> KvStore {
        self.run_controller_pass().await;
        KvStore::open(self.dependencies(writer_settings), STREAM, test_settings())
            .await
            .expect("the store opens")
    }

    /// Opens a store whose every record fills a segment of its own, given values of two kilobytes.
    async fn open_with_small_segments(&self) -> KvStore {
        self.open_with(WriterSettings {
            target_segment_bytes: 1024,
            // Only size rolls the segment, however far a test moves the clock.
            target_segment_age: Duration::from_hours(100_000),
            ..WriterSettings::default()
        })
        .await
    }

    /// Runs one pass of the controller: placement of the standby segment, reconciliation, truncation.
    async fn run_controller_pass(&self) {
        Controller::new(
            self.cluster.metadata().clone(),
            self.cluster.connector(),
            self.cluster.clock(),
            ControllerSettings::default(),
        )
        .run_pass()
        .await
        .expect("the metadata store is available");
    }

    /// The first sequence number the log still holds.
    async fn low_water_mark(&self) -> SequenceNumber {
        self.cluster
            .metadata()
            .get_stream(STREAM)
            .await
            .expect("the stream exists")
            .low_water_mark
    }

    /// How often each payload occurs in the stream, from its first record to `last_sequence`.
    async fn payload_counts(&self, last_sequence: SequenceNumber) -> BTreeMap<Bytes, usize> {
        let whole_stream =
            SequenceRange::new(SequenceNumber::FIRST, last_sequence).expect("the stream holds at least one record");
        let records = self
            .dependencies(WriterSettings::default())
            .client
            .open_reader(STREAM)
            .read_range(whole_stream)
            .await
            .expect("the stream is readable");
        let mut counts = BTreeMap::new();
        for record in records {
            *counts.entry(record.payload).or_insert(0) += 1;
        }
        counts
    }
}

/// The settings of the stores under test.
fn test_settings() -> KvSettings {
    KvSettings {
        takeover: TakeoverPolicy::Force,
        checkpoint_after: CHECKPOINT_AFTER,
        ..KvSettings::default()
    }
}

/// Puts `value` under `key` and returns the sequence number of the put.
async fn put(store: &KvStore, key: &str, value: &str) -> SequenceNumber {
    store.put(bytes(key), bytes(value)).await.expect("the put commits")
}

/// Waits until `store` has committed a checkpoint at or above `sequence`.
async fn checkpointed(store: &KvStore, sequence: SequenceNumber) {
    timeout(PATIENCE, store.progress().wait_until_committed(sequence))
        .await
        .expect("the checkpoint is committed in time")
        .expect("the materializer is running");
}

// ---- one store ----

#[tokio::test(start_paused = true)]
async fn key_never_written_does_not_exist() {
    let harness = Harness::start().await;
    let store = harness.open().await;

    assert_eq!(store.get(b"apples").await, Ok(None));
}

#[tokio::test(start_paused = true)]
async fn value_that_was_put_is_read_back() {
    let harness = Harness::start().await;
    let store = harness.open().await;

    put(&store, "apples", "12").await;

    assert_eq!(store.get(b"apples").await, Ok(Some(bytes("12"))));
}

#[tokio::test(start_paused = true)]
async fn later_put_replaces_the_value() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    put(&store, "apples", "12").await;

    put(&store, "apples", "7").await;

    assert_eq!(store.get(b"apples").await, Ok(Some(bytes("7"))));
}

#[tokio::test(start_paused = true)]
async fn deleted_key_does_not_exist() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    put(&store, "apples", "12").await;

    store.delete(bytes("apples")).await.expect("the delete commits");

    assert_eq!(store.get(b"apples").await, Ok(None));
}

#[tokio::test(start_paused = true)]
async fn deleting_a_key_that_does_not_exist_succeeds() {
    let harness = Harness::start().await;
    let store = harness.open().await;

    let deleted = store.delete(bytes("apples")).await;

    assert!(deleted.is_ok());
}

#[tokio::test(start_paused = true)]
async fn key_with_an_empty_value_exists() {
    let harness = Harness::start().await;
    let store = harness.open().await;

    put(&store, "apples", "").await;

    assert_eq!(store.get(b"apples").await, Ok(Some(Bytes::new())));
}

#[tokio::test(start_paused = true)]
async fn puts_to_other_keys_leave_a_key_alone() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    put(&store, "apples", "12").await;

    for key_number in 0..200 {
        put(&store, &format!("other-{key_number}"), "x").await;
    }

    assert_eq!(store.get(b"apples").await, Ok(Some(bytes("12"))));
}

#[tokio::test(start_paused = true)]
async fn operations_of_one_write_take_effect_at_the_same_sequence() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    let before = put(&store, "pears", "1").await;

    let sequence = store
        .write(vec![
            KvOperation::Put {
                key: bytes("apples"),
                value: bytes("12"),
            },
            KvOperation::Delete { key: bytes("pears") },
        ])
        .await
        .expect("the write commits");

    assert_eq!(sequence, before.next());
    assert_eq!(store.get_as_of(b"apples", sequence).await, Ok(Some(bytes("12"))));
    assert_eq!(store.get_as_of(b"pears", sequence).await, Ok(None));
}

#[tokio::test(start_paused = true)]
async fn read_of_history_sees_the_value_of_that_time() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    let first_put = put(&store, "apples", "12").await;
    put(&store, "apples", "7").await;

    let value_then = store.get_as_of(b"apples", first_put).await;

    assert_eq!(value_then, Ok(Some(bytes("12"))));
}

#[tokio::test(start_paused = true)]
async fn read_of_history_before_a_key_existed_finds_it_absent() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    let before = put(&store, "pears", "1").await;
    put(&store, "apples", "12").await;

    assert_eq!(store.get_as_of(b"apples", before).await, Ok(None));
}

#[tokio::test(start_paused = true)]
async fn scan_returns_the_keys_of_the_range_in_order() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    let mut last_put = SequenceNumber::ZERO;
    for key in ["e", "b", "d", "a", "c"] {
        last_put = put(&store, key, &format!("value-{key}")).await;
    }

    let scanned = store.scan(b"b", b"e", last_put).await;

    assert_eq!(
        scanned,
        Ok(vec![
            (bytes("b"), bytes("value-b")),
            (bytes("c"), bytes("value-c")),
            (bytes("d"), bytes("value-d")),
        ])
    );
}

#[tokio::test(start_paused = true)]
async fn scan_of_history_does_not_see_later_writes() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    let first_put = put(&store, "a", "1").await;
    put(&store, "b", "2").await;

    let scanned = store.scan(b"a", b"z", first_put).await;

    assert_eq!(scanned, Ok(vec![(bytes("a"), bytes("1"))]));
}

#[tokio::test(start_paused = true)]
async fn value_too_large_for_one_record_is_not_written() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    let too_large = Bytes::from(vec![0; MAX_RECORD_PAYLOAD_BYTES + 1]);

    let written = store.put(bytes("apples"), too_large).await;

    assert!(matches!(written, Err(KvError::NotWritten { .. })));
    assert_eq!(store.get(b"apples").await, Ok(None));
}

#[tokio::test(start_paused = true)]
async fn store_on_a_stream_that_was_not_prepared_does_not_open() {
    let harness = Harness::start().await;
    harness
        .cluster
        .metadata()
        .unregister_materializer(STREAM, KV_MATERIALIZER)
        .await
        .expect("the stream exists");

    let opened = KvStore::open(harness.dependencies(WriterSettings::default()), STREAM, test_settings()).await;

    assert!(matches!(
        opened.err(),
        Some(KvError::Materializer(RunnerError::Metadata(
            MetaError::MaterializerNotRegistered { .. }
        )))
    ));
}

// ---- restarting ----

#[tokio::test(start_paused = true)]
async fn reopened_store_has_what_was_written_before() {
    let harness = Harness::start().await;
    let first_store = harness.open().await;
    put(&first_store, "apples", "12").await;
    drop(first_store);

    let reopened = harness.open().await;

    assert_eq!(reopened.get(b"apples").await, Ok(Some(bytes("12"))));
}

#[tokio::test(start_paused = true)]
async fn reopened_store_starts_from_its_checkpoint() {
    let harness = Harness::start().await;
    let first_store = harness.open().await;
    let last_put = put(&first_store, "apples", "12").await;
    checkpointed(&first_store, last_put).await;
    drop(first_store);

    let reopened = harness.open().await;

    assert_eq!(reopened.progress().latest().committed_checkpoint, last_put);
}

/// Opens a store, puts keys 0 to 19 with the value "before", checkpoints, puts keys 15 to 24 with the value
/// "after", and closes the store. Whoever opens the stream next restores the checkpoint and applies the rest.
async fn write_before_and_after_a_checkpoint(harness: &Harness) {
    let first_store = harness.open().await;
    let mut last_put = SequenceNumber::ZERO;
    for key_number in 0..20 {
        last_put = put(&first_store, &format!("key-{key_number}"), "before").await;
    }
    checkpointed(&first_store, last_put).await;
    for key_number in 15..25 {
        put(&first_store, &format!("key-{key_number}"), "after").await;
    }
}

#[tokio::test(start_paused = true)]
async fn reopened_store_has_what_was_checkpointed() {
    let harness = Harness::start().await;
    write_before_and_after_a_checkpoint(&harness).await;

    let reopened = harness.open().await;

    assert_eq!(reopened.get(b"key-3").await, Ok(Some(bytes("before"))));
}

#[tokio::test(start_paused = true)]
async fn reopened_store_has_what_was_written_after_its_checkpoint() {
    let harness = Harness::start().await;
    write_before_and_after_a_checkpoint(&harness).await;

    let reopened = harness.open().await;

    assert_eq!(reopened.get(b"key-17").await, Ok(Some(bytes("after"))));
    assert_eq!(reopened.get(b"key-24").await, Ok(Some(bytes("after"))));
}

#[tokio::test(start_paused = true)]
async fn reopened_store_still_reads_history_from_before_the_restart() {
    let harness = Harness::start().await;
    let first_store = harness.open().await;
    let first_put = put(&first_store, "apples", "12").await;
    let second_put = put(&first_store, "apples", "7").await;
    checkpointed(&first_store, second_put).await;
    drop(first_store);

    let reopened = harness.open().await;

    assert_eq!(reopened.get_as_of(b"apples", first_put).await, Ok(Some(bytes("12"))));
}

// ---- failover ----

#[tokio::test(start_paused = true)]
async fn new_store_sees_what_the_replaced_one_committed() {
    let harness = Harness::start().await;
    let replaced = harness.open().await;
    put(&replaced, "apples", "12").await;

    let successor = harness.open().await;

    assert_eq!(successor.get(b"apples").await, Ok(Some(bytes("12"))));
}

#[tokio::test(start_paused = true)]
async fn replaced_store_cannot_tell_whether_its_write_was_made() {
    let harness = Harness::start().await;
    let replaced = harness.open().await;
    let _successor = harness.open().await;

    let written = replaced.put(bytes("apples"), bytes("12")).await;

    assert!(matches!(written, Err(KvError::OutcomeUnknown { .. })));
}

#[tokio::test(start_paused = true)]
async fn replaced_store_gives_no_answer_rather_than_a_stale_one() {
    let harness = Harness::start().await;
    let replaced = harness.open().await;
    put(&replaced, "apples", "12").await;
    let successor = harness.open().await;
    put(&successor, "apples", "7").await;

    let read = replaced.get(b"apples").await;

    assert!(matches!(read, Err(KvError::NotCurrent { .. })), "{read:?}");
}

#[tokio::test(start_paused = true)]
async fn replaced_store_still_reads_history_it_has_applied() {
    let harness = Harness::start().await;
    let replaced = harness.open().await;
    let first_put = put(&replaced, "apples", "12").await;
    replaced
        .get_as_of(b"apples", first_put)
        .await
        .expect("the put is applied");
    let successor = harness.open().await;
    put(&successor, "apples", "7").await;

    let value_then = replaced.get_as_of(b"apples", first_put).await;

    assert_eq!(value_then, Ok(Some(bytes("12"))));
}

#[tokio::test(start_paused = true)]
async fn replaced_store_whose_materializer_was_stopped_says_so() {
    let harness = Harness::start().await;
    let replaced = harness.open().await;
    let successor = harness.open().await;
    let last_put = put(&successor, "apples", "7").await;
    // The replaced store's materializer applies the put and learns at its next checkpoint that it was
    // replaced.
    sleep(CHECKPOINT_AFTER * 2).await;

    let read = replaced.get_as_of(b"apples", last_put.advanced_by(1_000)).await;

    assert!(
        matches!(read, Err(KvError::Materializer(RunnerError::Fenced { .. }))),
        "{read:?}"
    );
}

/// What became of the puts that were in flight while another store took over.
struct InterruptedWrites {
    /// The store that took over.
    successor: KvStore,
    /// Every put by key: the value it wrote and whether it was reported as done.
    was_reported_done_by_key: BTreeMap<String, bool>,
}

/// What one writer task learned about one of its puts.
struct PutOutcome {
    /// The key the put wrote.
    key: String,
    /// Whether the store reported the put as committed.
    was_reported_done: bool,
}

/// How many tasks write to the store that is about to be replaced.
const CONCURRENT_WRITERS: u32 = 4;

/// How many puts one writer has finished when the takeover starts.
const PUTS_BEFORE_THE_TAKEOVER: u32 = 10;

/// Has four tasks put key after key on one store, each until a put fails, and opens a second store
/// once they are well under way. Every writer starts its next put as soon as the last one returned, so
/// the takeover always finds puts in flight.
async fn take_over_in_the_middle_of_writes(harness: &Harness) -> InterruptedWrites {
    let replaced = Arc::new(harness.open().await);
    let well_under_way = Arc::new(Notify::new());
    let mut writers = JoinSet::new();
    for writer_number in 0..CONCURRENT_WRITERS {
        let replaced = Arc::clone(&replaced);
        let well_under_way = Arc::clone(&well_under_way);
        writers.spawn(async move {
            let mut outcomes = Vec::new();
            for key_number in 0_u32.. {
                let key = format!("key-{writer_number}-{key_number}");
                let written = replaced.put(bytes(&key), bytes("written")).await;
                outcomes.push(PutOutcome {
                    key,
                    was_reported_done: written.is_ok(),
                });
                if written.is_err() {
                    break;
                }
                if writer_number == 0 && key_number == PUTS_BEFORE_THE_TAKEOVER {
                    well_under_way.notify_one();
                }
            }
            outcomes
        });
    }
    well_under_way.notified().await;
    let successor = harness.open().await;

    let mut was_reported_done_by_key = BTreeMap::new();
    while let Some(finished) = writers.join_next().await {
        for outcome in finished.expect("a writer does not panic") {
            was_reported_done_by_key.insert(outcome.key, outcome.was_reported_done);
        }
    }
    InterruptedWrites {
        successor,
        was_reported_done_by_key,
    }
}

#[tokio::test(start_paused = true)]
async fn takeover_finds_writes_done_and_writes_in_flight() {
    let harness = Harness::start().await;

    let interrupted = take_over_in_the_middle_of_writes(&harness).await;

    let mut done = 0;
    let mut interrupted_in_flight = 0;
    for was_reported_done in interrupted.was_reported_done_by_key.values() {
        if *was_reported_done {
            done += 1;
        } else {
            interrupted_in_flight += 1;
        }
    }
    assert!(done > 0, "no write finished before the takeover");
    assert_eq!(interrupted_in_flight, CONCURRENT_WRITERS);
}

#[tokio::test(start_paused = true)]
async fn write_reported_done_before_a_takeover_is_seen_by_the_new_store() {
    let harness = Harness::start().await;
    let interrupted = take_over_in_the_middle_of_writes(&harness).await;

    for (key, was_reported_done) in &interrupted.was_reported_done_by_key {
        if *was_reported_done {
            let read = interrupted.successor.get(key.as_bytes()).await;

            assert_eq!(read, Ok(Some(bytes("written"))), "{key}");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn write_interrupted_by_a_takeover_is_applied_once_or_not_at_all() {
    let harness = Harness::start().await;
    let interrupted = take_over_in_the_middle_of_writes(&harness).await;
    let end_of_stream = interrupted
        .successor
        .put(bytes("marker"), bytes("end"))
        .await
        .expect("the new store writes");

    let payload_counts = harness.payload_counts(end_of_stream).await;

    for (key, was_reported_done) in &interrupted.was_reported_done_by_key {
        let payload = KvRecord::put(bytes(key), bytes("written")).encode();
        let times_in_the_log = payload_counts.get(&payload).copied().unwrap_or(0);
        let expected_at_least = usize::from(*was_reported_done);

        assert!(
            (expected_at_least..=1).contains(&times_in_the_log),
            "{key} is in the log {times_in_the_log} times"
        );
    }
}

// ---- truncation ----

/// A value large enough that, in a store of small segments, every put fills a segment.
fn segment_filling_value(marker: u8) -> Bytes {
    Bytes::from(vec![marker; 2048])
}

/// A store whose log was truncated below its checkpoint.
struct TruncatedStore {
    /// The store, still open.
    store: KvStore,
    /// The sequence of the last put the store made before its checkpoint.
    last_put: SequenceNumber,
}

/// Writes ten keys, one segment each, checkpoints, and lets the controller truncate the log below the
/// checkpoint.
async fn store_whose_log_was_truncated(harness: &Harness) -> TruncatedStore {
    let store = harness.open_with_small_segments().await;
    let mut last_put = SequenceNumber::ZERO;
    for key_number in 0..10_u8 {
        harness.run_controller_pass().await;
        last_put = store
            .put(bytes(&format!("key-{key_number}")), segment_filling_value(key_number))
            .await
            .expect("the put commits");
        sleep(Duration::from_secs(1)).await;
    }
    checkpointed(&store, last_put).await;
    harness.run_controller_pass().await;
    TruncatedStore { store, last_put }
}

#[tokio::test(start_paused = true)]
async fn log_below_the_checkpoint_of_the_store_is_truncated() {
    let harness = Harness::start().await;

    let truncated = store_whose_log_was_truncated(&harness).await;

    let low_water_mark = harness.low_water_mark().await;
    assert!(low_water_mark > SequenceNumber::FIRST, "{low_water_mark}");
    assert!(low_water_mark <= truncated.last_put.next(), "{low_water_mark}");
}

#[tokio::test(start_paused = true)]
async fn store_reads_every_key_after_its_log_was_truncated() {
    let harness = Harness::start().await;

    let store = store_whose_log_was_truncated(&harness).await.store;

    for key_number in 0..10_u8 {
        let read = store.get(format!("key-{key_number}").as_bytes()).await;

        assert_eq!(read, Ok(Some(segment_filling_value(key_number))));
    }
}

#[tokio::test(start_paused = true)]
async fn store_reads_history_whose_records_were_truncated() {
    let harness = Harness::start().await;

    let store = store_whose_log_was_truncated(&harness).await.store;

    // The fifth put is long gone from the log; the layers still know what the store held then.
    let fifth_put = SequenceNumber::new(5);
    assert!(harness.low_water_mark().await > fifth_put);
    assert_eq!(store.get_as_of(b"key-9", fifth_put).await, Ok(None));
}

#[tokio::test(start_paused = true)]
async fn store_reopens_after_its_log_was_truncated() {
    let harness = Harness::start().await;
    let store = store_whose_log_was_truncated(&harness).await.store;
    drop(store);

    let reopened = harness.open().await;

    assert_eq!(reopened.get(b"key-0").await, Ok(Some(segment_filling_value(0))));
}

#[tokio::test(start_paused = true)]
async fn writes_and_reads_go_on_while_the_log_is_being_truncated() {
    let harness = Harness::start().await;
    let store = store_whose_log_was_truncated(&harness).await.store;

    for round in 0..5_u8 {
        harness.run_controller_pass().await;
        store
            .put(bytes("counter"), segment_filling_value(round))
            .await
            .expect("the put commits");
        sleep(CHECKPOINT_AFTER).await;
    }

    assert_eq!(store.get(b"counter").await, Ok(Some(segment_filling_value(4))));
}
