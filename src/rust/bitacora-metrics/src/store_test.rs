//! Tests of the metrics store against an in-process cluster.
//!
//! # What is covered
//!
//! - One store alone: appends, read-your-writes, refused batches, samples out of order and twice, series
//!   that come and go.
//! - Blocks: cut by the checkpoint timer and by a full builder, merged, read back after a restart, with
//!   one sample and with none.
//! - Queries while blocks are cut and merged: every answer is exact up to the record it names.
//! - Restarting: with and without a checkpoint, with samples still in the head when the store stopped.
//! - Failover: a replaced store, a takeover in the middle of appends, the catalog under a new token.
//! - The orphan sweeper: it leaves every block of the committed catalog in place.
//! - Truncation of the log below the checkpoint while queries run, and reopening afterwards.
//! - A damaged block.
//! - Determinism: the same records give the same answers whenever the checkpoints happen.
//!
//! # How the fixtures work
//!
//! `Harness`, from `test_support.rs`, is the in-process cluster of `bitacora-testkit` with one stream
//! prepared for a store and an in-memory artifact store. Nothing is mocked: stores write through the real
//! client and the real materializer runner. Time is Tokio's paused clock. `SmallBlocks` cuts a block
//! after eight samples or one second of them and merges two neighbouring blocks.

use std::collections::BTreeMap;

use bitacora_artifact::MemoryArtifactStore;
use bitacora_client::WriterSettings;
use bitacora_materializer::{namespace_prefix, sweep_orphans};
use bitacora_types::SequenceRange;
use bytes::Bytes;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::sleep;

use super::*;
use crate::record::decode_record;
use crate::sample::{Labels, Sample};
use crate::selector::Matcher;
use crate::test_support::{
    CHECKPOINT_AFTER, Harness, PATIENCE, RECORD_WITHOUT_SERIES, STREAM, SmallBlocks, UnmergedBlocks, batch, job,
    sample, samples_at, series,
};

/// A checkpoint interval no test reaches: only a full builder cuts a block.
const NEVER: Duration = Duration::from_hours(100_000);

/// Appends `samples` and returns the sequence number of the record.
async fn append<P: BlockPolicy>(store: &MetricsStore<P>, samples: &[SeriesSamples]) -> SequenceNumber {
    store.append(samples).await.expect("the append commits")
}

/// Everything the store holds once it has applied the record at `sequence`.
async fn everything_at<P: BlockPolicy>(store: &MetricsStore<P>, sequence: SequenceNumber) -> QueryResult {
    store
        .query(&Selector::all(), TimeRange::ALL, QueryPosition::AtLeast(sequence))
        .await
        .expect("an answer")
}

/// Waits until `store` has committed a checkpoint at or above `sequence`.
async fn checkpointed<P: BlockPolicy>(store: &MetricsStore<P>, sequence: SequenceNumber) {
    timeout(PATIENCE, store.progress().wait_until_committed(sequence))
        .await
        .expect("the checkpoint is committed in time")
        .expect("the materializer is running");
}

/// Appends `samples`, then waits for the checkpoint timer and the checkpoint that covers the record.
async fn append_and_checkpoint<P: BlockPolicy>(store: &MetricsStore<P>, samples: &[SeriesSamples]) -> SequenceNumber {
    let appended_at = append(store, samples).await;
    sleep(CHECKPOINT_AFTER).await;
    checkpointed(store, appended_at).await;
    appended_at
}

/// The records each block of `store` covers, as pairs of first and last sequence number.
fn covered_records<P: BlockPolicy>(store: &MetricsStore<P>) -> Vec<(u64, u64)> {
    let mut covered = Vec::new();
    for entry in store.blocks() {
        covered.push((entry.covered_range.first().get(), entry.covered_range.last().get()));
    }
    covered
}

/// The keys the blocks of `store` have in the artifact store.
async fn block_object_keys<P: BlockPolicy>(harness: &Harness, store: &MetricsStore<P>) -> Vec<String> {
    let prefix = namespace_prefix(harness.stream_id().await, METRICS_MATERIALIZER);
    let mut object_keys = Vec::new();
    for entry in store.blocks() {
        object_keys.push(format!("{prefix}{}", entry.artifact.artifact_key));
    }
    object_keys
}

// ---- one store ----

#[tokio::test(start_paused = true)]
async fn new_store_answers_nothing() {
    let harness = Harness::start().await;
    let store = harness.open().await;

    let answer = store
        .query(&Selector::all(), TimeRange::ALL, QueryPosition::Applied)
        .await;

    let nothing = QueryResult {
        as_of: SequenceNumber::ZERO,
        series: Vec::new(),
    };
    assert_eq!(answer, Ok(nothing));
}

#[tokio::test(start_paused = true)]
async fn appended_samples_are_read_back_by_a_query_that_asks_for_their_record() {
    let harness = Harness::start().await;
    let store = harness.open().await;

    let appended_at = append(&store, &batch("api", &[10, 20])).await;

    let answer = everything_at(&store, appended_at).await;
    assert_eq!(answer.series, [series("api", samples_at(&[10, 20]))]);
}

#[tokio::test(start_paused = true)]
async fn answer_names_a_record_at_or_after_the_one_asked_for() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    append(&store, &batch("api", &[10])).await;

    let appended_at = append(&store, &batch("api", &[20])).await;

    assert!(everything_at(&store, appended_at).await.as_of >= appended_at);
}

#[tokio::test(start_paused = true)]
async fn each_append_is_one_record() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    let first = append(&store, &batch("api", &[10])).await;

    let second = append(&store, &batch("api", &[20])).await;

    assert_eq!(second, first.next());
}

#[tokio::test(start_paused = true)]
async fn samples_of_several_series_in_one_batch_are_applied_together() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    let two_series = [series("api", samples_at(&[10])), series("web", samples_at(&[10]))];

    let appended_at = append(&store, &two_series).await;

    assert_eq!(everything_at(&store, appended_at).await.series, two_series);
}

#[tokio::test(start_paused = true)]
async fn batch_without_a_sample_is_refused() {
    let harness = Harness::start().await;
    let store = harness.open().await;

    let appended = store.append(&batch("api", &[])).await;

    assert_eq!(appended, Err(MetricsError::InvalidBatch(InvalidBatch::NoSamples)));
}

#[tokio::test(start_paused = true)]
async fn query_for_a_record_that_never_comes_gives_no_answer() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    let appended_at = append(&store, &batch("api", &[10])).await;
    let never_appended = appended_at.advanced_by(5);

    let answer = store
        .query(&Selector::all(), TimeRange::ALL, QueryPosition::AtLeast(never_appended))
        .await;

    let not_applied = MetricsError::NotApplied {
        sequence: never_appended,
        waited: MetricsSettings::default().query_wait,
    };
    assert_eq!(answer, Err(not_applied));
}

#[tokio::test(start_paused = true)]
async fn only_the_selected_series_and_time_range_are_answered() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    append(&store, &batch("api", &[10, 20, 30])).await;
    let appended_at = append(&store, &batch("web", &[10, 20, 30])).await;
    let only_web = Selector::new(vec![Matcher::equal("job", "web")]);
    let range = TimeRange::new(15, 30).expect("valid range");

    let answer = store
        .query(&only_web, range, QueryPosition::AtLeast(appended_at))
        .await
        .expect("an answer");

    assert_eq!(answer.series, [series("web", samples_at(&[20, 30]))]);
}

#[tokio::test(start_paused = true)]
async fn samples_out_of_order_and_twice_are_answered_once_in_time_order() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    append_and_checkpoint(&store, &batch("api", &[50, 60])).await;
    append_and_checkpoint(&store, &batch("api", &[10, 50, 55])).await;

    let appended_at = append(&store, &batch("api", &[5, 60, 57, 70])).await;

    let answer = everything_at(&store, appended_at).await;
    assert_eq!(answer.series, [series("api", samples_at(&[5, 10, 50, 55, 57, 60, 70]))]);
}

#[tokio::test(start_paused = true)]
async fn later_record_replaces_the_sample_of_a_timestamp_that_is_in_a_block() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    append_and_checkpoint(&store, &batch("api", &[10, 20])).await;

    let appended_at = append(&store, &[series("api", vec![sample(20, 7.0)])]).await;

    let answer = everything_at(&store, appended_at).await;
    assert_eq!(answer.series, [series("api", vec![sample(10, 10.0), sample(20, 7.0)])]);
}

#[tokio::test(start_paused = true)]
async fn series_that_stopped_and_series_that_started_are_answered_for_their_own_times() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    append_and_checkpoint(&store, &batch("old", &[10, 20])).await;
    append_and_checkpoint(&store, &batch("steady", &[10, 40])).await;
    let appended_at = append(&store, &batch("new", &[40, 50])).await;
    let later = TimeRange::new(30, 100).expect("valid range");

    let answer = store
        .query(&Selector::all(), later, QueryPosition::AtLeast(appended_at))
        .await
        .expect("an answer");

    let expected = [
        series("new", samples_at(&[40, 50])),
        series("steady", samples_at(&[40])),
    ];
    assert_eq!(answer.series, expected);
}

// ---- blocks ----

#[tokio::test(start_paused = true)]
async fn checkpoint_cuts_a_block_from_the_records_applied_since_the_last_one() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    append(&store, &batch("api", &[10])).await;

    append_and_checkpoint(&store, &batch("api", &[20])).await;

    assert_eq!(covered_records(&store), [(1, 2)]);
}

#[tokio::test(start_paused = true)]
async fn full_builder_cuts_a_block_without_waiting_for_the_timer() {
    let harness = Harness::start().await;
    let store = harness.open_with::<SmallBlocks>(WriterSettings::default(), NEVER).await;

    let appended_at = append(&store, &batch("api", &[1, 2, 3, 4, 5, 6, 7, 8])).await;

    checkpointed(&store, appended_at).await;
    assert_eq!(covered_records(&store), [(1, 1)]);
}

#[tokio::test(start_paused = true)]
async fn answer_is_the_same_before_and_after_the_samples_move_into_a_block() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    let appended_at = append(&store, &batch("api", &[10, 20])).await;
    let from_the_head = everything_at(&store, appended_at).await;

    sleep(CHECKPOINT_AFTER).await;
    checkpointed(&store, appended_at).await;

    assert_eq!(everything_at(&store, appended_at).await, from_the_head);
}

#[tokio::test(start_paused = true)]
async fn block_of_one_sample_is_read_back() {
    let harness = Harness::start().await;
    let store = harness.open().await;

    let appended_at = append_and_checkpoint(&store, &batch("api", &[10])).await;

    assert_eq!(
        everything_at(&store, appended_at).await.series,
        [series("api", samples_at(&[10]))]
    );
}

#[tokio::test(start_paused = true)]
async fn record_without_samples_becomes_a_block_without_series() {
    let harness = Harness::start().await;
    // A producer in another language may append a record that holds no series; this crate's own
    // encoder refuses to. The record is format version 1 with a series count of 0.
    let other_producer = harness
        .dependencies(WriterSettings::default())
        .client
        .open_writer(STREAM, TakeoverPolicy::Force)
        .await
        .expect("the writer opens");
    let appended_at = other_producer
        .append(Bytes::from_static(&RECORD_WITHOUT_SERIES))
        .await
        .expect("the record commits");
    drop(other_producer);
    let store = harness.open().await;

    sleep(CHECKPOINT_AFTER).await;
    checkpointed(&store, appended_at).await;

    let nothing = QueryResult {
        as_of: appended_at,
        series: Vec::new(),
    };
    assert_eq!(everything_at(&store, appended_at).await, nothing);
    assert_eq!(covered_records(&store), [(1, 1)]);
}

#[tokio::test(start_paused = true)]
async fn range_from_the_end_of_one_block_to_the_start_of_the_next_includes_both_ends() {
    let harness = Harness::start().await;
    let store = harness
        .open_with::<UnmergedBlocks>(WriterSettings::default(), CHECKPOINT_AFTER)
        .await;
    append_and_checkpoint(&store, &batch("api", &[10, 20])).await;
    let appended_at = append_and_checkpoint(&store, &batch("api", &[30, 40])).await;
    let range = TimeRange::new(20, 30).expect("valid range");

    let answer = store
        .query(&Selector::all(), range, QueryPosition::AtLeast(appended_at))
        .await
        .expect("an answer");

    assert_eq!(answer.series, [series("api", samples_at(&[20, 30]))]);
}

#[tokio::test(start_paused = true)]
async fn two_neighbouring_blocks_are_merged_into_one() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    append_and_checkpoint(&store, &batch("api", &[10])).await;

    append_and_checkpoint(&store, &batch("api", &[20])).await;

    assert_eq!(covered_records(&store), [(1, 2)]);
}

#[tokio::test(start_paused = true)]
async fn blocks_that_were_merged_are_deleted_from_the_store() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    append_and_checkpoint(&store, &batch("api", &[10])).await;
    let first_block = block_object_keys(&harness, &store).await;

    append_and_checkpoint(&store, &batch("api", &[20])).await;

    let first_block_now = harness.artifact_store.get_object(&first_block[0]).await;
    assert!(first_block_now.is_err());
}

#[tokio::test(start_paused = true)]
async fn query_spans_merged_blocks_and_the_head() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    append_and_checkpoint(&store, &batch("api", &[10, 20])).await;
    // A query before the merge, so that the store holds the index of a block that is about to go.
    store
        .query(&Selector::all(), TimeRange::ALL, QueryPosition::Applied)
        .await
        .expect("an answer");
    append_and_checkpoint(&store, &batch("api", &[30, 40])).await;

    let appended_at = append(&store, &batch("api", &[50])).await;

    let expected = QueryResult {
        as_of: appended_at,
        series: vec![series("api", samples_at(&[10, 20, 30, 40, 50]))],
    };
    assert_eq!(everything_at(&store, appended_at).await, expected);
}

/// How many records the writer of the consistency test appends.
const CONSISTENCY_RECORDS: u64 = 60;

#[tokio::test(start_paused = true)]
async fn every_answer_given_while_blocks_are_cut_and_merged_is_exact_up_to_its_record() {
    let harness = Harness::start().await;
    let store = Arc::new(
        harness
            .open_with::<SmallBlocks>(WriterSettings::default(), Duration::from_millis(700))
            .await,
    );
    let writer_store = Arc::clone(&store);
    // Record N holds one sample, at time N. An answer exact up to record N holds the times 1 to N.
    let writer = tokio::spawn(async move {
        for record_number in 1..=CONSISTENCY_RECORDS {
            let timestamp_millis = i64::try_from(record_number).expect("a small number");
            append(&writer_store, &batch("api", &[timestamp_millis])).await;
            sleep(Duration::from_millis(100)).await;
        }
    });

    let mut answers_checked = 0_u64;
    while !writer.is_finished() {
        let answer = store
            .query(&Selector::all(), TimeRange::ALL, QueryPosition::Applied)
            .await
            .expect("an answer");
        let mut sample_count = 0;
        for answered in &answer.series {
            sample_count += answered.samples.len() as u64;
        }

        assert_eq!(
            sample_count,
            answer.as_of.get(),
            "blocks: {:?}",
            covered_records(&store)
        );

        answers_checked += 1;
        sleep(Duration::from_millis(7)).await;
    }
    writer.await.expect("the writer does not panic");
    assert!(answers_checked > CONSISTENCY_RECORDS, "{answers_checked}");
    // Blocks were cut all along: the checkpoint timer fires every seven records.
    let blocks_reach = store.blocks().last().map(|entry| entry.covered_range.last().get());
    assert!(blocks_reach >= Some(CONSISTENCY_RECORDS - 10), "{blocks_reach:?}");
}

// ---- restarting ----

#[tokio::test(start_paused = true)]
async fn reopened_store_has_everything_without_a_checkpoint() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    let appended_at = append(&store, &batch("api", &[10, 20])).await;
    drop(store);

    let reopened = harness.open().await;

    assert_eq!(
        everything_at(&reopened, appended_at).await.series,
        [series("api", samples_at(&[10, 20]))]
    );
}

#[tokio::test(start_paused = true)]
async fn reopened_store_reads_its_blocks_and_applies_the_rest_of_the_log_again() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    append_and_checkpoint(&store, &batch("api", &[10, 20])).await;
    // These samples are only in the head when the store stops.
    let appended_at = append(&store, &batch("api", &[20, 30])).await;
    everything_at(&store, appended_at).await;
    drop(store);

    let reopened = harness.open().await;

    let expected = QueryResult {
        as_of: appended_at,
        series: vec![series("api", samples_at(&[10, 20, 30]))],
    };
    assert_eq!(everything_at(&reopened, appended_at).await, expected);
}

#[tokio::test(start_paused = true)]
async fn reopened_store_starts_from_the_blocks_of_the_last_checkpoint() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    append_and_checkpoint(&store, &batch("api", &[10, 20])).await;
    let blocks_before = store.blocks();
    append(&store, &batch("api", &[30])).await;
    drop(store);

    let reopened = harness.open().await;

    assert_eq!(reopened.blocks(), blocks_before);
}

#[tokio::test(start_paused = true)]
async fn store_that_was_reopened_twice_without_a_checkpoint_counts_no_sample_twice() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    let appended_at = append(&store, &batch("api", &[10, 20])).await;
    drop(store);
    let reopened = harness.open().await;
    everything_at(&reopened, appended_at).await;
    drop(reopened);

    let reopened_again = harness.open().await;

    assert_eq!(
        everything_at(&reopened_again, appended_at).await.series,
        [series("api", samples_at(&[10, 20]))]
    );
}

#[tokio::test(start_paused = true)]
async fn store_on_a_stream_that_was_never_prepared_does_not_open() {
    let harness = Harness::start().await;
    harness
        .cluster
        .metadata()
        .create_stream(
            "unprepared",
            bitacora_types::ReplicationSettings::DEFAULT,
            bitacora_meta::RetentionPolicy::default(),
            None,
        )
        .await
        .expect("the stream is created");
    harness.run_controller_pass().await;

    let opened = MetricsStore::<SmallBlocks>::open(
        harness.dependencies(WriterSettings::default()),
        "unprepared",
        MetricsSettings {
            takeover: TakeoverPolicy::Force,
            ..MetricsSettings::default()
        },
    )
    .await;

    assert!(matches!(
        opened,
        Err(MetricsError::Materializer(RunnerError::Metadata(_)))
    ));
}

// ---- failover ----

#[tokio::test(start_paused = true)]
async fn new_store_sees_what_the_replaced_one_committed() {
    let harness = Harness::start().await;
    let replaced = harness.open().await;
    let appended_at = append(&replaced, &batch("api", &[10])).await;

    let successor = harness.open().await;

    assert_eq!(
        everything_at(&successor, appended_at).await.series,
        [series("api", samples_at(&[10]))]
    );
}

#[tokio::test(start_paused = true)]
async fn replaced_store_cannot_tell_whether_its_append_was_made() {
    let harness = Harness::start().await;
    let replaced = harness.open().await;
    let _successor = harness.open().await;

    let appended = replaced.append(&batch("api", &[10])).await;

    assert!(
        matches!(appended, Err(MetricsError::OutcomeUnknown { .. })),
        "{appended:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn replaced_store_whose_materializer_was_stopped_says_so() {
    let harness = Harness::start().await;
    let replaced = harness.open().await;
    let successor = harness.open().await;
    let appended_at = append(&successor, &batch("api", &[10])).await;
    // The replaced store's materializer applies the record and learns at its next checkpoint that it
    // was replaced.
    sleep(CHECKPOINT_AFTER * 2).await;

    let answer = replaced
        .query(
            &Selector::all(),
            TimeRange::ALL,
            QueryPosition::AtLeast(appended_at.advanced_by(1_000)),
        )
        .await;

    assert!(
        matches!(answer, Err(MetricsError::Materializer(RunnerError::Fenced { .. }))),
        "{answer:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn new_store_goes_on_from_the_blocks_of_the_replaced_one() {
    let harness = Harness::start().await;
    let replaced = harness
        .open_with::<UnmergedBlocks>(WriterSettings::default(), CHECKPOINT_AFTER)
        .await;
    append_and_checkpoint(&replaced, &batch("api", &[10])).await;
    let successor = harness
        .open_with::<UnmergedBlocks>(WriterSettings::default(), CHECKPOINT_AFTER)
        .await;

    append_and_checkpoint(&successor, &batch("api", &[20])).await;

    assert_eq!(covered_records(&successor), [(1, 1), (2, 2)]);
}

/// What became of the appends that were in flight while another store took over.
struct InterruptedAppends {
    /// The store that took over.
    successor: MetricsStore<SmallBlocks>,
    /// Every sample a writer tried to append, by its timestamp, and whether the append was reported
    /// as done.
    was_reported_done_by_timestamp: BTreeMap<i64, bool>,
}

/// How many tasks append to the store that is about to be replaced.
const CONCURRENT_WRITERS: i64 = 4;

/// How many appends one writer has finished when the takeover starts.
const APPENDS_BEFORE_THE_TAKEOVER: i64 = 10;

/// Has four tasks append sample after sample to one store, each until an append fails, and opens a second
/// store once they are well under way. Every writer starts its next append as soon as the last one
/// returned, so the takeover always finds appends in flight. Every sample has a timestamp of its own; the
/// store cuts a block every eight samples, so blocks are cut and merged all along.
async fn take_over_in_the_middle_of_appends(harness: &Harness) -> InterruptedAppends {
    let replaced = Arc::new(harness.open().await);
    let well_under_way = Arc::new(Notify::new());
    let mut writers = JoinSet::new();
    for writer_number in 0..CONCURRENT_WRITERS {
        let replaced = Arc::clone(&replaced);
        let well_under_way = Arc::clone(&well_under_way);
        writers.spawn(async move {
            let mut outcomes = Vec::new();
            for append_number in 0_i64.. {
                let timestamp_millis = append_number * CONCURRENT_WRITERS + writer_number;
                let appended = replaced.append(&batch("api", &[timestamp_millis])).await;
                outcomes.push((timestamp_millis, appended.is_ok()));
                if appended.is_err() {
                    break;
                }
                if writer_number == 0 && append_number == APPENDS_BEFORE_THE_TAKEOVER {
                    well_under_way.notify_one();
                }
            }
            outcomes
        });
    }
    well_under_way.notified().await;
    let successor = harness.open().await;

    let mut was_reported_done_by_timestamp = BTreeMap::new();
    while let Some(finished) = writers.join_next().await {
        for (timestamp_millis, was_reported_done) in finished.expect("a writer does not panic") {
            was_reported_done_by_timestamp.insert(timestamp_millis, was_reported_done);
        }
    }
    InterruptedAppends {
        successor,
        was_reported_done_by_timestamp,
    }
}

/// The timestamps of the samples the series `job="api"` has in `answer`.
fn answered_timestamps(answer: &QueryResult) -> Vec<i64> {
    let mut timestamps = Vec::new();
    for answered in &answer.series {
        for answered_sample in &answered.samples {
            timestamps.push(answered_sample.timestamp_millis);
        }
    }
    timestamps
}

#[tokio::test(start_paused = true)]
async fn takeover_finds_appends_done_and_appends_in_flight() {
    let harness = Harness::start().await;

    let interrupted = take_over_in_the_middle_of_appends(&harness).await;

    let mut done = 0;
    let mut interrupted_in_flight = 0;
    for was_reported_done in interrupted.was_reported_done_by_timestamp.values() {
        if *was_reported_done {
            done += 1;
        } else {
            interrupted_in_flight += 1;
        }
    }
    assert!(done > 0, "no append finished before the takeover");
    assert_eq!(interrupted_in_flight, CONCURRENT_WRITERS);
}

#[tokio::test(start_paused = true)]
async fn sample_reported_as_appended_before_a_takeover_is_answered_by_the_new_store() {
    let harness = Harness::start().await;
    let interrupted = take_over_in_the_middle_of_appends(&harness).await;
    let marker = append(&interrupted.successor, &batch("marker", &[0])).await;
    let only_api = Selector::new(vec![Matcher::equal("job", "api")]);

    let answer = interrupted
        .successor
        .query(&only_api, TimeRange::ALL, QueryPosition::AtLeast(marker))
        .await
        .expect("an answer");

    let answered = answered_timestamps(&answer);
    for (timestamp_millis, was_reported_done) in &interrupted.was_reported_done_by_timestamp {
        if *was_reported_done {
            assert!(answered.contains(timestamp_millis), "sample {timestamp_millis} is lost");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn new_store_answers_no_sample_that_nobody_appended() {
    let harness = Harness::start().await;
    let interrupted = take_over_in_the_middle_of_appends(&harness).await;
    let marker = append(&interrupted.successor, &batch("marker", &[0])).await;
    let only_api = Selector::new(vec![Matcher::equal("job", "api")]);

    let answer = interrupted
        .successor
        .query(&only_api, TimeRange::ALL, QueryPosition::AtLeast(marker))
        .await
        .expect("an answer");

    for timestamp_millis in answered_timestamps(&answer) {
        assert!(
            interrupted
                .was_reported_done_by_timestamp
                .contains_key(&timestamp_millis),
            "sample {timestamp_millis} was never appended"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn answer_after_a_takeover_is_what_the_log_holds() {
    let harness = Harness::start().await;
    let interrupted = take_over_in_the_middle_of_appends(&harness).await;
    let marker = append(&interrupted.successor, &batch("marker", &[0])).await;
    let whole_stream = SequenceRange::new(SequenceNumber::FIRST, marker).expect("a record was appended");
    let records = harness
        .dependencies(WriterSettings::default())
        .client
        .open_reader(STREAM)
        .read_range(whole_stream)
        .await
        .expect("the stream is readable");
    let mut in_the_log: BTreeMap<Labels, BTreeMap<i64, Sample>> = BTreeMap::new();
    for record in &records {
        for appended in decode_record(&record.payload).expect("a metrics record") {
            for appended_sample in appended.samples {
                in_the_log
                    .entry(appended.labels.clone())
                    .or_default()
                    .insert(appended_sample.timestamp_millis, appended_sample);
            }
        }
    }
    let mut expected = Vec::new();
    for (labels, samples_by_time) in in_the_log {
        expected.push(SeriesSamples {
            labels,
            samples: samples_by_time.into_values().collect(),
        });
    }

    let answer = everything_at(&interrupted.successor, marker).await;

    assert_eq!(answer.series, expected);
}

// ---- the orphan sweeper ----

#[tokio::test(start_paused = true)]
async fn sweeper_leaves_every_block_of_the_committed_catalog_in_place() {
    let harness = Harness::start().await;
    let interrupted = take_over_in_the_middle_of_appends(&harness).await;
    let marker = append(&interrupted.successor, &batch("marker", &[0])).await;
    sleep(CHECKPOINT_AFTER).await;
    checkpointed(&interrupted.successor, marker).await;
    let before_the_sweep = everything_at(&interrupted.successor, marker).await;

    sweep_orphans(
        harness.cluster.metadata(),
        Arc::new(harness.artifact_store.clone()),
        harness.stream_id().await,
        METRICS_MATERIALIZER,
    )
    .await
    .expect("the sweep runs");

    for object_key in block_object_keys(&harness, &interrupted.successor).await {
        let block = harness.artifact_store.get_object(&object_key).await;

        assert!(block.is_ok(), "block {object_key} was swept");
    }
    assert_eq!(everything_at(&interrupted.successor, marker).await, before_the_sweep);
}

#[tokio::test(start_paused = true)]
async fn sweeper_deletes_the_block_a_replaced_store_cut_after_it_was_replaced() {
    let harness = Harness::start().await;
    let replaced = harness.open().await;
    let appended_at = append(&replaced, &batch("api", &[10])).await;
    everything_at(&replaced, appended_at).await;
    // The successor takes the materializer over and stops before it checkpoints, so nothing but the
    // sweeper is left to clean up after the replaced store.
    drop(harness.open().await);
    // The replaced store cuts a block at its next checkpoint and learns, too late, that it may not
    // commit it.
    sleep(CHECKPOINT_AFTER * 2).await;
    let orphans = block_object_keys(&harness, &replaced).await;
    assert_eq!(orphans.len(), 1);
    assert!(harness.artifact_store.get_object(&orphans[0]).await.is_ok());

    sweep_orphans(
        harness.cluster.metadata(),
        Arc::new(harness.artifact_store.clone()),
        harness.stream_id().await,
        METRICS_MATERIALIZER,
    )
    .await
    .expect("the sweep runs");

    let orphan_now = harness.artifact_store.get_object(&orphans[0]).await;
    assert!(orphan_now.is_err(), "orphan {} is still there", orphans[0]);
}

// ---- truncation ----

/// A batch large enough that, in a store of small segments, every append fills a segment: 150 samples
/// of the series `job="filler"`, starting at `first_timestamp_millis`.
fn segment_filling_batch(first_timestamp_millis: i64) -> Vec<SeriesSamples> {
    let mut timestamps = Vec::new();
    for sample_number in 0..150 {
        timestamps.push(first_timestamp_millis + sample_number);
    }
    batch("filler", &timestamps)
}

/// Opens a store whose every record fills a segment of its own.
async fn open_with_small_segments(harness: &Harness) -> MetricsStore<SmallBlocks> {
    let writer_settings = WriterSettings {
        target_segment_bytes: 1024,
        // Only size rolls the segment, however far a test moves the clock.
        target_segment_age: Duration::from_hours(100_000),
        ..WriterSettings::default()
    };
    harness.open_with(writer_settings, CHECKPOINT_AFTER).await
}

/// How many records the truncation tests append before the log is truncated.
const RECORDS_BEFORE_TRUNCATION: i64 = 10;

/// Appends ten records, one segment each, checkpoints, and lets the controller truncate the log below
/// the checkpoint. Returns the store and the sequence number of the last record.
async fn store_whose_log_was_truncated(harness: &Harness) -> (MetricsStore<SmallBlocks>, SequenceNumber) {
    let store = open_with_small_segments(harness).await;
    let mut last_append = SequenceNumber::ZERO;
    for record_number in 0..RECORDS_BEFORE_TRUNCATION {
        harness.run_controller_pass().await;
        last_append = append(&store, &segment_filling_batch(record_number * 1_000)).await;
        sleep(Duration::from_secs(1)).await;
    }
    sleep(CHECKPOINT_AFTER).await;
    checkpointed(&store, last_append).await;
    harness.run_controller_pass().await;
    (store, last_append)
}

/// How many samples `answer` holds, over all series.
fn sample_count(answer: &QueryResult) -> usize {
    let mut sample_count = 0;
    for answered in &answer.series {
        sample_count += answered.samples.len();
    }
    sample_count
}

#[tokio::test(start_paused = true)]
async fn log_below_the_checkpoint_of_the_store_is_truncated() {
    let harness = Harness::start().await;

    let (_store, last_append) = store_whose_log_was_truncated(&harness).await;

    let low_water_mark = harness.low_water_mark().await;
    assert!(low_water_mark > SequenceNumber::FIRST, "{low_water_mark}");
    assert!(low_water_mark <= last_append.next(), "{low_water_mark}");
}

#[tokio::test(start_paused = true)]
async fn store_answers_every_sample_after_its_log_was_truncated() {
    let harness = Harness::start().await;

    let (store, last_append) = store_whose_log_was_truncated(&harness).await;

    assert_eq!(sample_count(&everything_at(&store, last_append).await), 1_500);
}

#[tokio::test(start_paused = true)]
async fn store_reopens_after_its_log_was_truncated() {
    let harness = Harness::start().await;
    let (store, last_append) = store_whose_log_was_truncated(&harness).await;
    drop(store);

    let reopened = harness.open().await;

    assert_eq!(sample_count(&everything_at(&reopened, last_append).await), 1_500);
}

#[tokio::test(start_paused = true)]
async fn queries_are_answered_in_full_while_the_log_is_being_truncated() {
    let harness = Harness::start().await;
    let (store, _last_append) = store_whose_log_was_truncated(&harness).await;
    let store = Arc::new(store);
    let querying_store = Arc::clone(&store);
    // An answer exact up to record N holds the 150 samples of each of the N records.
    let queries = tokio::spawn(async move {
        let mut answers_checked = 0_u32;
        loop {
            let answer = querying_store
                .query(&Selector::all(), TimeRange::ALL, QueryPosition::Applied)
                .await
                .expect("an answer");
            let expected_samples = usize::try_from(answer.as_of.get()).expect("a small number") * 150;
            assert_eq!(sample_count(&answer), expected_samples);
            answers_checked += 1;
            if answer.as_of.get() == 15 {
                return answers_checked;
            }
            sleep(Duration::from_millis(250)).await;
        }
    });

    for record_number in RECORDS_BEFORE_TRUNCATION..15 {
        harness.run_controller_pass().await;
        append(&store, &segment_filling_batch(record_number * 1_000)).await;
        sleep(CHECKPOINT_AFTER).await;
    }

    let answers_checked = timeout(PATIENCE, queries)
        .await
        .expect("the queries finish")
        .expect("no answer was wrong");
    assert!(answers_checked > 5, "{answers_checked}");
    assert!(harness.low_water_mark().await > SequenceNumber::new(10));
}

// ---- damage ----

#[tokio::test(start_paused = true)]
async fn damaged_block_fails_the_query_that_reads_it() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    let appended_at = append_and_checkpoint(&store, &batch("api", &[10, 20])).await;
    let object_keys = block_object_keys(&harness, &store).await;
    harness.artifact_store.corrupt_byte(&object_keys[0], 5, 0x20);

    let answer = store
        .query(&Selector::all(), TimeRange::ALL, QueryPosition::AtLeast(appended_at))
        .await;

    assert!(
        matches!(
            answer,
            Err(MetricsError::Block(MaterializerError::DamagedArtifact { .. }))
        ),
        "{answer:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn damaged_block_does_not_keep_the_store_from_answering_from_the_head() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    append_and_checkpoint(&store, &batch("api", &[10, 20])).await;
    let object_keys = block_object_keys(&harness, &store).await;
    harness.artifact_store.corrupt_byte(&object_keys[0], 5, 0x20);
    let appended_at = append(&store, &batch("api", &[500])).await;
    let later = TimeRange::new(400, 600).expect("valid range");

    let answer = store
        .query(&Selector::all(), later, QueryPosition::AtLeast(appended_at))
        .await
        .expect("an answer");

    assert_eq!(answer.series, [series("api", samples_at(&[500]))]);
}

// ---- determinism ----

/// The records the determinism tests append: 30 records of three samples each, for two series, with
/// samples out of order and timestamps written twice.
fn determinism_records() -> Vec<Vec<SeriesSamples>> {
    let mut records = Vec::new();
    for record_number in 0..30_i64 {
        let job_name = if record_number % 3 == 0 { "web" } else { "api" };
        let base = record_number * 100;
        records.push(batch(job_name, &[base + 50, base, base - 100]));
    }
    records
}

/// Starts a cluster, appends [`determinism_records`] to a store that checkpoints every
/// `checkpoint_after`, pausing `pause` after each record, and returns everything the store answers and
/// the artifact store it wrote to.
async fn answer_with_checkpoints_every(
    checkpoint_after: Duration,
    pause: Duration,
) -> (QueryResult, MemoryArtifactStore) {
    let harness = Harness::start().await;
    let store = harness
        .open_with::<SmallBlocks>(WriterSettings::default(), checkpoint_after)
        .await;
    let mut last_append = SequenceNumber::ZERO;
    for record in determinism_records() {
        last_append = append(&store, &record).await;
        sleep(pause).await;
    }
    let answer = everything_at(&store, last_append).await;
    (answer, harness.artifact_store.clone())
}

#[tokio::test(start_paused = true)]
async fn same_records_give_the_same_answer_whenever_the_checkpoints_happen() {
    let (checkpointed_often, _blocks) =
        answer_with_checkpoints_every(Duration::from_secs(2), Duration::from_secs(1)).await;

    let (checkpointed_rarely, _blocks) =
        answer_with_checkpoints_every(Duration::from_secs(45), Duration::from_millis(300)).await;

    assert_eq!(checkpointed_often, checkpointed_rarely);
}

#[tokio::test(start_paused = true)]
async fn same_records_give_the_same_answer_as_a_single_pass_over_them() {
    let (answer, _blocks) = answer_with_checkpoints_every(Duration::from_secs(2), Duration::from_secs(1)).await;

    let mut expected: BTreeMap<Labels, BTreeMap<i64, Sample>> = BTreeMap::new();
    for record in determinism_records() {
        for appended in record {
            for appended_sample in appended.samples {
                expected
                    .entry(appended.labels.clone())
                    .or_default()
                    .insert(appended_sample.timestamp_millis, appended_sample);
            }
        }
    }
    let mut expected_series = Vec::new();
    for (labels, samples_by_time) in expected {
        expected_series.push(SeriesSamples {
            labels,
            samples: samples_by_time.into_values().collect(),
        });
    }
    assert_eq!(answer.series, expected_series);
    assert_eq!(expected_series[0].labels, job("api"));
}
