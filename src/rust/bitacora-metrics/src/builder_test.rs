//! Tests of the builder that turns records into blocks.
//!
//! # What is covered
//!
//! - Adding records: what a record that is not a metrics record does.
//! - When the builder asks for a seal: by number of samples, by span of time, and never by itself.
//! - Sealing: what the block holds, what the descriptor says, and that the builder is empty afterwards.
//! - Merging blocks: a later block wins, the result is the block a single seal would have written, and
//!   damaged or missing inputs are reported.
//! - A failed seal leaves the builder as it was.
//!
//! # How the fixtures work
//!
//! The builder is driven directly, as the framework's catalog drives it, against the in-memory artifact
//! store of `test_support.rs`. `SmallBlocks` cuts a block after eight samples or one second of them.

use bitacora_artifact::ArtifactError;
use bitacora_types::SequenceNumber;
use bytes::Bytes;

use super::*;
use crate::block::BlockSummary;
use crate::sample::{SeriesSamples, TimeRange};
use crate::selector::Selector;
use crate::test_support::{SmallBlocks, TestArtifacts, batch, job, record, sample, samples_at};

/// The range of records `first..=last`.
fn records(first: u64, last: u64) -> SequenceRange {
    SequenceRange::new(SequenceNumber::new(first), SequenceNumber::new(last)).expect("valid range")
}

/// A builder of small blocks that holds nothing.
fn small_builder() -> BlockBuilder<SmallBlocks> {
    BlockBuilder::default()
}

/// Seals what `builder` holds as the block of the records `first..=last` and returns its catalog entry.
async fn seal(
    builder: &mut BlockBuilder<SmallBlocks>,
    artifacts: &TestArtifacts,
    first: u64,
    last: u64,
) -> CatalogEntry {
    let covered_range = records(first, last);
    let artifact = builder
        .seal_artifact(covered_range, &artifacts.namespace)
        .await
        .expect("sealed");
    CatalogEntry {
        covered_range,
        artifact,
    }
}

/// Everything the block `entry` names holds.
async fn block_contents(artifacts: &TestArtifacts, entry: &CatalogEntry) -> BTreeMap<Labels, Vec<Sample>> {
    let block = artifacts
        .namespace
        .get_artifact(&entry.artifact.artifact_key)
        .await
        .expect("stored");
    decode_whole_block(&block).expect("valid block")
}

#[test]
fn new_builder_does_not_ask_for_a_seal() {
    assert!(!small_builder().should_seal());
}

#[test]
fn builder_below_both_limits_does_not_ask_for_a_seal() {
    let mut builder = small_builder();

    builder
        .add_records(&[record(1, &batch("api", &[0, 100, 200, 300, 400, 500, 999]))])
        .expect("valid record");

    assert!(!builder.should_seal());
}

#[test]
fn builder_asks_for_a_seal_once_it_holds_the_most_samples_a_block_may() {
    let mut builder = small_builder();

    builder
        .add_records(&[record(1, &batch("api", &[1, 2, 3, 4, 5, 6, 7, 8]))])
        .expect("valid record");

    assert!(builder.should_seal());
}

#[test]
fn samples_of_several_series_and_records_count_together() {
    let mut builder = small_builder();

    builder
        .add_records(&[
            record(1, &batch("api", &[1, 2, 3, 4])),
            record(2, &batch("web", &[1, 2, 3, 4])),
        ])
        .expect("valid records");

    assert!(builder.should_seal());
}

#[test]
fn replaced_sample_does_not_count_towards_a_full_block() {
    let mut builder = small_builder();

    builder
        .add_records(&[
            record(1, &batch("api", &[1, 2, 3, 4])),
            record(2, &batch("api", &[1, 2, 3, 4])),
        ])
        .expect("valid records");

    assert!(!builder.should_seal());
}

#[test]
fn builder_asks_for_a_seal_once_its_samples_span_the_longest_time_a_block_may() {
    let mut builder = small_builder();

    builder
        .add_records(&[record(1, &batch("api", &[5_000, 6_000]))])
        .expect("valid record");

    assert!(builder.should_seal());
}

#[test]
fn span_of_time_is_measured_over_all_series() {
    let mut builder = small_builder();

    builder
        .add_records(&[record(1, &batch("api", &[5_000])), record(2, &batch("web", &[4_000]))])
        .expect("valid records");

    assert!(builder.should_seal());
}

#[test]
fn payload_that_is_not_a_metrics_record_is_refused_with_its_sequence() {
    let mut builder = small_builder();
    let not_a_record = LogRecord {
        sequence: SequenceNumber::new(9),
        payload: Bytes::from_static(b"\x07 something else"),
    };

    let added = builder.add_records(&[not_a_record]);

    assert!(matches!(
        added,
        Err(MaterializerError::InvalidRecord { sequence, .. }) if sequence == SequenceNumber::new(9)
    ));
}

#[test]
fn compaction_follows_the_policy() {
    assert_eq!(small_builder().compaction_policy(), SmallBlocks::COMPACTION);
}

#[tokio::test]
async fn sealed_block_holds_the_samples_that_were_added() {
    let artifacts = TestArtifacts::new();
    let mut builder = small_builder();
    builder
        .add_records(&[record(1, &batch("api", &[10, 20])), record(2, &batch("web", &[15]))])
        .expect("valid records");

    let entry = seal(&mut builder, &artifacts, 1, 2).await;

    let mut expected = BTreeMap::new();
    expected.insert(job("api"), samples_at(&[10, 20]));
    expected.insert(job("web"), samples_at(&[15]));
    assert_eq!(block_contents(&artifacts, &entry).await, expected);
}

#[tokio::test]
async fn descriptor_states_the_size_of_the_block() {
    let artifacts = TestArtifacts::new();
    let mut builder = small_builder();
    builder
        .add_records(&[record(1, &batch("api", &[10, 20]))])
        .expect("valid record");

    let entry = seal(&mut builder, &artifacts, 1, 1).await;

    let block = artifacts
        .namespace
        .get_artifact(&entry.artifact.artifact_key)
        .await
        .expect("stored");
    assert_eq!(entry.artifact.size_bytes, block.len() as u64);
}

#[tokio::test]
async fn descriptor_carries_the_summary_of_the_block() {
    let artifacts = TestArtifacts::new();
    let mut builder = small_builder();
    builder
        .add_records(&[record(1, &batch("api", &[10, 20])), record(2, &batch("web", &[15]))])
        .expect("valid records");

    let entry = seal(&mut builder, &artifacts, 1, 2).await;

    let expected = BlockSummary {
        series_count: 2,
        sample_count: 3,
        min_time_millis: 10,
        max_time_millis: 20,
    };
    assert_eq!(BlockSummary::decode(&entry.artifact.summary), Some(expected));
}

#[tokio::test]
async fn descriptor_of_a_merged_block_carries_the_summary_of_the_merged_block() {
    let artifacts = TestArtifacts::new();
    let mut builder = small_builder();
    let inputs = two_blocks(&mut builder, &artifacts).await;

    let merged = builder
        .merge_artifacts(&inputs, &artifacts.namespace)
        .await
        .expect("merged");

    let expected = BlockSummary {
        series_count: 2,
        sample_count: 4,
        min_time_millis: 10,
        max_time_millis: 30,
    };
    assert_eq!(BlockSummary::decode(&merged.summary), Some(expected));
}

#[tokio::test]
async fn builder_holds_nothing_after_a_seal() {
    let artifacts = TestArtifacts::new();
    let mut builder = small_builder();
    builder
        .add_records(&[record(1, &batch("api", &[1, 2, 3, 4, 5, 6, 7, 8]))])
        .expect("valid record");

    seal(&mut builder, &artifacts, 1, 1).await;

    let snapshot = builder.head().snapshot(&Selector::all(), TimeRange::ALL);
    assert_eq!(snapshot.held_records, None);
    assert_eq!(snapshot.samples_by_series.len(), 0);
}

#[tokio::test]
async fn builder_names_the_record_that_fills_a_block() {
    let mut builder = small_builder();

    builder
        .add_records(&[
            record(1, &batch("api", &[1, 2, 3, 4])),
            record(2, &batch("api", &[5, 6, 7, 8])),
            record(3, &batch("api", &[9])),
        ])
        .expect("valid records");

    assert_eq!(builder.seal_boundary(), Some(SequenceNumber::new(2)));
}

#[tokio::test]
async fn builder_that_is_not_full_names_no_boundary() {
    let mut builder = small_builder();

    builder
        .add_records(&[record(1, &batch("api", &[1, 2, 3, 4]))])
        .expect("valid record");

    assert_eq!(builder.seal_boundary(), None);
    assert!(!builder.should_seal());
}

#[tokio::test]
async fn block_holds_the_records_up_to_the_boundary_and_no_more() {
    let artifacts = TestArtifacts::new();
    let mut builder = small_builder();
    builder
        .add_records(&[
            record(1, &batch("api", &[1, 2, 3, 4])),
            record(2, &batch("api", &[5, 6, 7, 8])),
            record(3, &batch("web", &[9])),
        ])
        .expect("valid records");

    let entry = seal(&mut builder, &artifacts, 1, 2).await;

    let mut expected = BTreeMap::new();
    expected.insert(job("api"), samples_at(&[1, 2, 3, 4, 5, 6, 7, 8]));
    assert_eq!(block_contents(&artifacts, &entry).await, expected);
}

#[tokio::test]
async fn records_after_the_boundary_stay_in_the_builder() {
    let artifacts = TestArtifacts::new();
    let mut builder = small_builder();
    builder
        .add_records(&[
            record(1, &batch("api", &[1, 2, 3, 4, 5, 6, 7, 8])),
            record(2, &batch("web", &[9])),
        ])
        .expect("valid records");

    seal(&mut builder, &artifacts, 1, 1).await;

    let snapshot = builder.head().snapshot(&Selector::all(), TimeRange::ALL);
    assert_eq!(snapshot.held_records, Some(records(2, 2)));
}

/// Saves what `builder` holds, as the records `first..=last`, and returns the entry a checkpoint would
/// name.
async fn save(
    builder: &mut BlockBuilder<SmallBlocks>,
    artifacts: &TestArtifacts,
    first: u64,
    last: u64,
) -> CatalogEntry {
    let artifact = builder
        .save_pending_records(&artifacts.namespace)
        .await
        .expect("saved")
        .expect("this builder saves its head");
    CatalogEntry {
        covered_range: records(first, last),
        artifact,
    }
}

#[tokio::test]
async fn saved_head_holds_the_samples_of_the_builder() {
    let artifacts = TestArtifacts::new();
    let mut builder = small_builder();
    builder
        .add_records(&[record(1, &batch("api", &[10, 20]))])
        .expect("valid record");

    let saved = save(&mut builder, &artifacts, 1, 1).await;

    let mut expected = BTreeMap::new();
    expected.insert(job("api"), samples_at(&[10, 20]));
    assert_eq!(block_contents(&artifacts, &saved).await, expected);
}

#[tokio::test]
async fn saving_leaves_the_builder_as_it_was() {
    let artifacts = TestArtifacts::new();
    let mut builder = small_builder();
    builder
        .add_records(&[record(1, &batch("api", &[10, 20]))])
        .expect("valid record");

    save(&mut builder, &artifacts, 1, 1).await;

    let snapshot = builder.head().snapshot(&Selector::all(), TimeRange::ALL);
    assert_eq!(snapshot.held_records, Some(records(1, 1)));
}

#[tokio::test]
async fn saved_head_carries_its_summary() {
    let artifacts = TestArtifacts::new();
    let mut builder = small_builder();
    builder
        .add_records(&[record(1, &batch("api", &[10, 20]))])
        .expect("valid record");

    let saved = save(&mut builder, &artifacts, 1, 1).await;

    let expected = BlockSummary {
        series_count: 1,
        sample_count: 2,
        min_time_millis: 10,
        max_time_millis: 20,
    };
    assert_eq!(BlockSummary::decode(&saved.artifact.summary), Some(expected));
}

#[tokio::test]
async fn builder_restored_from_a_saved_head_cuts_the_block_an_uninterrupted_builder_cuts() {
    let artifacts = TestArtifacts::new();
    let first_records = [record(1, &batch("api", &[1, 2, 3])), record(2, &batch("web", &[4, 5]))];
    let later_records = [record(3, &batch("api", &[6, 7, 8])), record(4, &batch("api", &[9]))];
    let mut interrupted = small_builder();
    interrupted.add_records(&first_records).expect("valid records");
    let saved = save(&mut interrupted, &artifacts, 1, 2).await;
    let mut restored = small_builder();
    restored
        .restore_pending_records(&saved, &artifacts.namespace)
        .await
        .expect("restored");
    restored.add_records(&later_records).expect("valid records");
    let mut uninterrupted = small_builder();
    uninterrupted.add_records(&first_records).expect("valid records");
    uninterrupted.add_records(&later_records).expect("valid records");

    let block_after_a_restart = seal(&mut restored, &artifacts, 1, 3).await;

    let block_without_one = seal(&mut uninterrupted, &artifacts, 1, 3).await;
    assert_eq!(restored.seal_boundary(), uninterrupted.seal_boundary());
    assert_eq!(
        block_contents(&artifacts, &block_after_a_restart).await,
        block_contents(&artifacts, &block_without_one).await
    );
}

#[tokio::test]
async fn saved_head_without_samples_restores_a_builder_that_holds_its_records() {
    let artifacts = TestArtifacts::new();
    let mut builder = small_builder();
    builder.add_records(&[record(1, &[])]).expect("a record without series");
    let saved = save(&mut builder, &artifacts, 1, 1).await;
    let mut restored = small_builder();

    restored
        .restore_pending_records(&saved, &artifacts.namespace)
        .await
        .expect("restored");

    let snapshot = restored.head().snapshot(&Selector::all(), TimeRange::ALL);
    assert_eq!(snapshot.held_records, Some(records(1, 1)));
}

#[tokio::test]
async fn damaged_saved_head_is_refused() {
    let artifacts = TestArtifacts::new();
    let mut builder = small_builder();
    builder
        .add_records(&[record(1, &batch("api", &[10, 20]))])
        .expect("valid record");
    let saved = save(&mut builder, &artifacts, 1, 1).await;
    artifacts
        .store
        .corrupt_byte(&TestArtifacts::object_key(&saved.artifact.artifact_key), 5, 0x20);
    let mut restored = small_builder();

    let refused = restored.restore_pending_records(&saved, &artifacts.namespace).await;

    assert!(
        matches!(refused, Err(MaterializerError::DamagedArtifact { .. })),
        "{refused:?}"
    );
}

#[tokio::test]
async fn saved_head_that_is_gone_is_reported_and_not_taken_for_an_empty_one() {
    let artifacts = TestArtifacts::new();
    let mut builder = small_builder();
    builder
        .add_records(&[record(1, &batch("api", &[10, 20]))])
        .expect("valid record");
    let mut saved = save(&mut builder, &artifacts, 1, 1).await;
    saved.artifact.artifact_key = "00000000000000000001/0000000099-head".to_owned();
    let mut restored = small_builder();

    let refused = restored.restore_pending_records(&saved, &artifacts.namespace).await;

    assert!(refused.is_err(), "{refused:?}");
}

#[tokio::test]
async fn builder_does_not_ask_for_a_seal_right_after_one() {
    let artifacts = TestArtifacts::new();
    let mut builder = small_builder();
    builder
        .add_records(&[record(1, &batch("api", &[1, 2, 3, 4, 5, 6, 7, 8]))])
        .expect("valid record");

    seal(&mut builder, &artifacts, 1, 1).await;

    assert!(!builder.should_seal());
}

#[tokio::test]
async fn record_without_a_sample_seals_into_a_block_without_series() {
    let artifacts = TestArtifacts::new();
    let mut builder = small_builder();
    builder
        .add_records(&[record(1, &batch("api", &[]))])
        .expect("valid record");

    let entry = seal(&mut builder, &artifacts, 1, 1).await;

    assert_eq!(block_contents(&artifacts, &entry).await, BTreeMap::new());
}

#[tokio::test]
async fn failed_seal_leaves_the_builder_as_it_was() {
    let artifacts = TestArtifacts::new();
    let mut builder = small_builder();
    builder
        .add_records(&[record(1, &batch("api", &[10, 20]))])
        .expect("valid record");
    artifacts.store.set_unavailable(true);
    let failed = builder.seal_artifact(records(1, 1), &artifacts.namespace).await;
    assert!(failed.is_err());
    artifacts.store.set_unavailable(false);

    let entry = seal(&mut builder, &artifacts, 1, 1).await;

    let mut expected = BTreeMap::new();
    expected.insert(job("api"), samples_at(&[10, 20]));
    assert_eq!(block_contents(&artifacts, &entry).await, expected);
}

/// Seals two blocks: records 1 to 2 with `api` at 10, 20 and `web` at 10, then record 3 with `api` at 20
/// (value 7) and 30.
async fn two_blocks(builder: &mut BlockBuilder<SmallBlocks>, artifacts: &TestArtifacts) -> Vec<CatalogEntry> {
    builder
        .add_records(&[record(1, &batch("api", &[10, 20])), record(2, &batch("web", &[10]))])
        .expect("valid records");
    let older = seal(builder, artifacts, 1, 2).await;
    let rewritten = [SeriesSamples {
        labels: job("api"),
        samples: vec![sample(20, 7.0), sample(30, 30.0)],
    }];
    builder.add_records(&[record(3, &rewritten)]).expect("valid record");
    let newer = seal(builder, artifacts, 3, 3).await;
    vec![older, newer]
}

#[tokio::test]
async fn merged_block_holds_the_samples_of_its_inputs_with_the_later_block_winning() {
    let artifacts = TestArtifacts::new();
    let mut builder = small_builder();
    let inputs = two_blocks(&mut builder, &artifacts).await;

    let artifact = builder
        .merge_artifacts(&inputs, &artifacts.namespace)
        .await
        .expect("merged");

    let merged_entry = CatalogEntry {
        covered_range: records(1, 3),
        artifact,
    };
    let mut expected = BTreeMap::new();
    expected.insert(job("api"), vec![sample(10, 10.0), sample(20, 7.0), sample(30, 30.0)]);
    expected.insert(job("web"), samples_at(&[10]));
    assert_eq!(block_contents(&artifacts, &merged_entry).await, expected);
}

#[tokio::test]
async fn merged_block_is_the_block_one_seal_of_all_the_records_would_have_written() {
    let artifacts = TestArtifacts::new();
    let mut builder = small_builder();
    let inputs = two_blocks(&mut builder, &artifacts).await;
    let merged = builder
        .merge_artifacts(&inputs, &artifacts.namespace)
        .await
        .expect("merged");
    let rewritten = [SeriesSamples {
        labels: job("api"),
        samples: vec![sample(20, 7.0), sample(30, 30.0)],
    }];
    let mut one_builder = small_builder();
    one_builder
        .add_records(&[
            record(1, &batch("api", &[10, 20])),
            record(2, &batch("web", &[10])),
            record(3, &rewritten),
        ])
        .expect("valid records");

    let sealed_at_once = seal(&mut one_builder, &artifacts, 1, 3).await;

    let merged_bytes = artifacts.namespace.get_artifact(&merged.artifact_key).await;
    let sealed_bytes = artifacts
        .namespace
        .get_artifact(&sealed_at_once.artifact.artifact_key)
        .await;
    assert_eq!(merged_bytes, sealed_bytes);
}

#[tokio::test]
async fn merge_leaves_what_the_builder_holds_alone() {
    let artifacts = TestArtifacts::new();
    let mut builder = small_builder();
    let inputs = two_blocks(&mut builder, &artifacts).await;
    builder
        .add_records(&[record(4, &batch("db", &[40]))])
        .expect("valid record");

    builder
        .merge_artifacts(&inputs, &artifacts.namespace)
        .await
        .expect("merged");

    let entry = seal(&mut builder, &artifacts, 4, 4).await;
    let mut expected = BTreeMap::new();
    expected.insert(job("db"), samples_at(&[40]));
    assert_eq!(block_contents(&artifacts, &entry).await, expected);
}

#[tokio::test]
async fn merge_reports_a_damaged_input_by_its_key() {
    let artifacts = TestArtifacts::new();
    let mut builder = small_builder();
    let inputs = two_blocks(&mut builder, &artifacts).await;
    let damaged_key = inputs[1].artifact.artifact_key.clone();
    artifacts
        .store
        .corrupt_byte(&TestArtifacts::object_key(&damaged_key), 2, 0x40);

    let merged = builder.merge_artifacts(&inputs, &artifacts.namespace).await;

    assert!(matches!(
        merged,
        Err(MaterializerError::DamagedArtifact { artifact_key, .. }) if artifact_key == damaged_key
    ));
}

#[tokio::test]
async fn merge_of_a_missing_input_fails() {
    let artifacts = TestArtifacts::new();
    let mut builder = small_builder();
    let mut inputs = two_blocks(&mut builder, &artifacts).await;
    inputs[0].artifact.artifact_key = "00000000000000000001/0000000099-block".to_owned();

    let merged = builder.merge_artifacts(&inputs, &artifacts.namespace).await;

    assert!(matches!(
        merged,
        Err(MaterializerError::ArtifactStore(ArtifactError::NotFound { .. }))
    ));
}
