//! Tests of the reader that answers from committed blocks without being the store.
//!
//! # What is covered
//!
//! - What a reader sees: nothing before the first checkpoint, the committed blocks afterwards, never the
//!   store's head.
//! - Refreshing: a later checkpoint is seen once the reader looks again.
//! - Blocks that are merged and deleted under a reader.
//! - Streams a reader cannot be opened on.
//! - A checkpoint a reader cannot read: the metadata store is down, the manifest is damaged or stays
//!   gone. The reader says which, and keeps what it knew.
//! - A reader writes nothing.
//!
//! # How the fixtures work
//!
//! `Harness`, from `test_support.rs`, runs a real store on an in-process cluster. The reader under test
//! shares only the metadata store and the artifact store with it, as a query node in another process
//! would.

use bitacora_artifact::ArtifactStore;
use bitacora_client::WriterSettings;
use tokio::time::{sleep, timeout};

use super::*;
use crate::builder::BlockPolicy;
use crate::sample::SeriesSamples;
use crate::store::MetricsStore;
use crate::test_support::{BlockPerRecord, CHECKPOINT_AFTER, Harness, PATIENCE, STREAM, batch, samples_at, series};

/// Opens a reader on the stream of `harness`.
async fn open_reader(harness: &Harness) -> BlockReader {
    BlockReader::open(
        harness.cluster.metadata().clone(),
        Arc::new(harness.artifact_store.clone()),
        STREAM,
    )
    .await
    .expect("the reader opens")
}

/// Appends `samples` to `store` and waits for the checkpoint that covers the record.
async fn append_and_checkpoint<Policy: BlockPolicy>(
    store: &MetricsStore<Policy>,
    samples: &[SeriesSamples],
) -> SequenceNumber {
    let appended_at = store.append(samples).await.expect("the append commits");
    sleep(CHECKPOINT_AFTER).await;
    timeout(PATIENCE, store.progress().wait_until_committed(appended_at))
        .await
        .expect("the checkpoint is committed in time")
        .expect("the materializer is running");
    appended_at
}

/// Everything the reader answers.
async fn everything(reader: &mut BlockReader) -> QueryResult {
    reader.query(&Selector::all(), TimeRange::ALL).await.expect("an answer")
}

/// A reader on a stream whose store committed one checkpoint and was then closed, so that nothing commits
/// another one behind the test's back.
async fn reader_after_the_only_checkpoint(harness: &Harness) -> BlockReader {
    let store = harness.open().await;
    append_and_checkpoint(&store, &batch("api", &[10, 20])).await;
    let reader = open_reader(harness).await;
    drop(store);
    reader
}

/// The key, in the object store, of the manifest the store last committed.
async fn committed_manifest_object_key(harness: &Harness) -> String {
    let stream_id = harness.stream_id().await;
    let committed = harness
        .cluster
        .metadata()
        .get_checkpoint(stream_id, METRICS_MATERIALIZER)
        .await
        .expect("a metrics store was prepared on the stream");
    let manifest_key = committed.manifest.expect("a checkpoint was committed");
    let prefix = bitacora_materializer::namespace_prefix(stream_id, METRICS_MATERIALIZER);
    format!("{prefix}{manifest_key}")
}

/// Deletes the committed manifest from the object store, as nothing in the system ever does.
async fn delete_the_committed_manifest(harness: &Harness) {
    let manifest_object_key = committed_manifest_object_key(harness).await;
    harness
        .artifact_store
        .delete_object(&manifest_object_key)
        .await
        .expect("the manifest is deleted behind the reader's back");
}

#[tokio::test(start_paused = true)]
async fn reader_of_a_stream_without_a_checkpoint_answers_nothing() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    store.append(&batch("api", &[10])).await.expect("the append commits");

    let mut reader = open_reader(&harness).await;

    let nothing = QueryResult {
        as_of: SequenceNumber::ZERO,
        series: Vec::new(),
    };
    assert_eq!(everything(&mut reader).await, nothing);
}

#[tokio::test(start_paused = true)]
async fn reader_answers_from_the_committed_blocks() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    let appended_at = append_and_checkpoint(&store, &batch("api", &[10, 20])).await;

    let mut reader = open_reader(&harness).await;

    let expected = QueryResult {
        as_of: appended_at,
        series: vec![series("api", samples_at(&[10, 20]))],
    };
    assert_eq!(everything(&mut reader).await, expected);
}

#[tokio::test(start_paused = true)]
async fn reader_sees_the_blocks_the_store_sees() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    append_and_checkpoint(&store, &batch("api", &[1, 2, 3, 4, 5, 6, 7, 8])).await;

    let reader = open_reader(&harness).await;

    assert_eq!(store.blocks().len(), 1);
    assert_eq!(reader.blocks(), store.blocks());
}

/// A block is cut when it is full, so what a checkpoint covers beyond the last block is in the head the
/// store saved. A reader that left it out would lag the store by up to a whole block.
#[tokio::test(start_paused = true)]
async fn reader_reads_the_head_the_checkpoint_saved_after_the_blocks() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    append_and_checkpoint(&store, &batch("api", &[10, 20])).await;

    let reader = open_reader(&harness).await;

    assert_eq!(store.blocks().len(), 0);
    assert_eq!(reader.blocks().len(), 1);
    assert_eq!(reader.blocks()[0].covered_range.last(), reader.checkpoint());
}

#[tokio::test(start_paused = true)]
async fn reader_answers_from_the_blocks_and_the_saved_head_together() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    store
        .append(&batch("api", &[1, 2, 3, 4, 5, 6, 7, 8]))
        .await
        .expect("the append commits");
    let saved_at = append_and_checkpoint(&store, &batch("api", &[20])).await;

    let mut reader = open_reader(&harness).await;

    let expected = QueryResult {
        as_of: saved_at,
        series: vec![series("api", samples_at(&[1, 2, 3, 4, 5, 6, 7, 8, 20]))],
    };
    assert_eq!(everything(&mut reader).await, expected);
}

#[tokio::test(start_paused = true)]
async fn reader_does_not_see_what_is_only_in_the_head_of_the_store() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    let checkpointed_at = append_and_checkpoint(&store, &batch("api", &[10])).await;
    store.append(&batch("api", &[20])).await.expect("the append commits");

    let mut reader = open_reader(&harness).await;

    let expected = QueryResult {
        as_of: checkpointed_at,
        series: vec![series("api", samples_at(&[10]))],
    };
    assert_eq!(everything(&mut reader).await, expected);
}

#[tokio::test(start_paused = true)]
async fn reader_answers_as_of_the_checkpoint_it_last_saw_until_it_looks_again() {
    let harness = Harness::start().await;
    let store = harness
        .open_with::<BlockPerRecord>(WriterSettings::default(), CHECKPOINT_AFTER)
        .await;
    let first_checkpoint = append_and_checkpoint(&store, &batch("api", &[10])).await;
    let mut reader = open_reader(&harness).await;

    append_and_checkpoint(&store, &batch("api", &[20])).await;

    assert_eq!(everything(&mut reader).await.as_of, first_checkpoint);
}

#[tokio::test(start_paused = true)]
async fn refreshed_reader_sees_the_later_checkpoint() {
    let harness = Harness::start().await;
    let store = harness
        .open_with::<BlockPerRecord>(WriterSettings::default(), CHECKPOINT_AFTER)
        .await;
    append_and_checkpoint(&store, &batch("api", &[10])).await;
    let mut reader = open_reader(&harness).await;
    let second_checkpoint = append_and_checkpoint(&store, &batch("api", &[20])).await;

    let refreshed_to = reader.refresh().await;

    assert_eq!(refreshed_to, Ok(second_checkpoint));
    assert_eq!(
        everything(&mut reader).await.series,
        [series("api", samples_at(&[10, 20]))]
    );
}

#[tokio::test(start_paused = true)]
async fn reader_whose_block_was_merged_away_looks_again_and_answers() {
    let harness = Harness::start().await;
    // This store merges two neighbouring blocks and deletes the ones it merged.
    let store = harness.open().await;
    append_and_checkpoint(&store, &batch("api", &[10])).await;
    let mut reader = open_reader(&harness).await;
    let second_checkpoint = append_and_checkpoint(&store, &batch("api", &[20])).await;

    let answer = everything(&mut reader).await;

    let expected = QueryResult {
        as_of: second_checkpoint,
        series: vec![series("api", samples_at(&[10, 20]))],
    };
    assert_eq!(answer, expected);
}

#[tokio::test(start_paused = true)]
async fn reader_does_not_open_on_a_stream_that_does_not_exist() {
    let harness = Harness::start().await;

    let opened = BlockReader::open(
        harness.cluster.metadata().clone(),
        Arc::new(harness.artifact_store.clone()),
        "no-such-stream",
    )
    .await;

    assert!(matches!(opened, Err(BlockReaderError::Metadata(_))));
}

#[tokio::test(start_paused = true)]
async fn reader_does_not_open_on_a_stream_without_a_metrics_store() {
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

    let opened = BlockReader::open(
        harness.cluster.metadata().clone(),
        Arc::new(harness.artifact_store.clone()),
        "unprepared",
    )
    .await;

    assert!(matches!(opened, Err(BlockReaderError::Metadata(_))));
}

#[tokio::test(start_paused = true)]
async fn refresh_reports_a_metadata_store_that_is_down() {
    let harness = Harness::start().await;
    let mut reader = reader_after_the_only_checkpoint(&harness).await;
    harness.cluster.key_value_store().set_unavailable(true);

    let refreshed = reader.refresh().await;

    assert!(matches!(refreshed, Err(BlockReaderError::Metadata(_))), "{refreshed:?}");
}

#[tokio::test(start_paused = true)]
async fn refresh_reports_a_damaged_manifest() {
    let harness = Harness::start().await;
    let mut reader = reader_after_the_only_checkpoint(&harness).await;
    let manifest_object_key = committed_manifest_object_key(&harness).await;
    harness.artifact_store.corrupt_byte(&manifest_object_key, 14, 0x01);

    let refreshed = reader.refresh().await;

    assert!(
        matches!(
            refreshed,
            Err(BlockReaderError::Block(MaterializerError::InvalidManifest { .. }))
        ),
        "{refreshed:?}"
    );
}

/// The store deletes a manifest only after it committed a newer one, so a reader that finds its manifest
/// gone looks again. A manifest that is gone every time is something else, and the reader must stop.
#[tokio::test(start_paused = true)]
async fn refresh_gives_up_when_the_committed_manifest_stays_gone() {
    let harness = Harness::start().await;
    let mut reader = reader_after_the_only_checkpoint(&harness).await;
    delete_the_committed_manifest(&harness).await;

    let refreshed = reader.refresh().await;

    assert_eq!(refreshed, Err(BlockReaderError::CheckpointKeptChanging));
}

#[tokio::test(start_paused = true)]
async fn reader_keeps_the_blocks_it_knew_when_a_refresh_fails() {
    let harness = Harness::start().await;
    let mut reader = reader_after_the_only_checkpoint(&harness).await;
    let checkpoint_before = reader.checkpoint();
    let blocks_before = reader.blocks().to_vec();
    delete_the_committed_manifest(&harness).await;

    reader.refresh().await.expect_err("the manifest is gone");

    assert_eq!(reader.checkpoint(), checkpoint_before);
    assert_eq!(reader.blocks(), blocks_before);
}

#[tokio::test(start_paused = true)]
async fn reader_still_answers_from_its_blocks_after_a_refresh_failed() {
    let harness = Harness::start().await;
    let mut reader = reader_after_the_only_checkpoint(&harness).await;
    let answer_before = everything(&mut reader).await;
    delete_the_committed_manifest(&harness).await;
    reader.refresh().await.expect_err("the manifest is gone");

    let answer_after = everything(&mut reader).await;

    assert_eq!(answer_after, answer_before);
}

#[tokio::test(start_paused = true)]
async fn reader_writes_nothing_to_the_artifact_store() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    append_and_checkpoint(&store, &batch("api", &[10, 20])).await;
    let objects_before = harness.artifact_store.list_prefix("").await;

    let mut reader = open_reader(&harness).await;
    everything(&mut reader).await;

    assert_eq!(harness.artifact_store.list_prefix("").await, objects_before);
}

#[tokio::test(start_paused = true)]
async fn damaged_block_fails_the_query_of_a_reader() {
    let harness = Harness::start().await;
    let store = harness.open().await;
    append_and_checkpoint(&store, &batch("api", &[10, 20])).await;
    let mut reader = open_reader(&harness).await;
    let prefix = bitacora_materializer::namespace_prefix(harness.stream_id().await, METRICS_MATERIALIZER);
    let object_key = format!("{prefix}{}", reader.blocks()[0].artifact.artifact_key);
    harness.artifact_store.corrupt_byte(&object_key, 5, 0x20);

    let answer = reader.query(&Selector::all(), TimeRange::ALL).await;

    assert!(matches!(
        answer,
        Err(BlockReaderError::Block(MaterializerError::DamagedArtifact { .. }))
    ));
}
