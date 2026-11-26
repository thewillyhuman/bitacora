//! Tests of how a query is answered from the head and the blocks.
//!
//! # What is covered
//!
//! - How the head fits a catalog: every relative position of the two.
//! - Answers from the head alone, from blocks alone and from both, and the record each is exact up to.
//! - Which sample wins when records, blocks or the head disagree; duplicates; samples that arrive out of
//!   order.
//! - Time ranges that end exactly on the first or last sample of a block; series that come and go.
//! - What is fetched: nothing from blocks outside the time range, by the summary in their catalog
//!   entry; an index only once.
//! - Damage and blocks that are gone.
//!
//! # How the fixtures work
//!
//! [`View`] is the store's materializer, the framework's catalog over the block builder, driven by hand: a test says which record is applied and when a
//! checkpoint cuts a block, with no cluster and no timing involved. The blocks go to the in-memory
//! artifact store of `test_support.rs`. `UnmergedBlocks` keeps every block as it was cut.

use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use bitacora_artifact::{ArtifactStore, MemoryArtifactStore, ObjectInfo};
use bitacora_materializer::Materializer;
use bytes::Bytes;

use super::*;
use crate::materializer::{METRICS_MATERIALIZER, MetricsMaterializer};
use crate::selector::Matcher;
use crate::test_support::{
    TEST_STREAM_ID, TestArtifacts, UnmergedBlocks, batch, job, record, sample, samples_at, series,
};

/// The record position `sequence`.
fn at(sequence: u64) -> SequenceNumber {
    SequenceNumber::new(sequence)
}

/// The store's materializer, driven by hand, and what a query against it needs.
struct View {
    /// Where the blocks go.
    artifacts: TestArtifacts,
    /// The materializer: a catalog of blocks and a head.
    materializer: MetricsMaterializer<UnmergedBlocks>,
    /// The cache of block indexes the queries share.
    cache: BlockIndexCache,
}

impl View {
    /// A view of a stream nothing was applied from yet.
    async fn new() -> Self {
        let artifacts = TestArtifacts::new();
        let materializer = MetricsMaterializer::restore_from_checkpoint(None, &artifacts.namespace)
            .await
            .expect("an empty view");
        Self {
            artifacts,
            materializer,
            cache: BlockIndexCache::default(),
        }
    }

    /// Applies the record at `sequence`, which holds `samples`.
    fn apply(&mut self, sequence: u64, samples: &[SeriesSamples]) {
        self.materializer
            .apply_records(&[record(sequence, samples)])
            .expect("a valid record");
    }

    /// Writes a checkpoint: everything applied since the last one becomes a block.
    async fn checkpoint(&mut self) {
        self.materializer
            .write_checkpoint(&self.artifacts.namespace)
            .await
            .expect("a checkpoint");
    }

    /// Answers a query against the view's own head and catalog.
    async fn query(&self, selector: &Selector, time_range: TimeRange) -> Result<QueryResult, QueryFailure> {
        self.query_with_head(&self.materializer.builder().head(), selector, time_range)
            .await
    }

    /// Answers a query against the view's catalog and some other head.
    async fn query_with_head(
        &self,
        head: &SharedHead,
        selector: &Selector,
        time_range: TimeRange,
    ) -> Result<QueryResult, QueryFailure> {
        query_head_and_blocks(
            head,
            &self.materializer.view(),
            &self.artifacts.namespace,
            &self.cache,
            selector,
            time_range,
        )
        .await
    }

    /// Everything the view holds.
    async fn everything(&self) -> QueryResult {
        self.query(&Selector::all(), TimeRange::ALL).await.expect("an answer")
    }

    /// The keys of the view's blocks in the store, in log order.
    fn block_object_keys(&self) -> Vec<String> {
        let mut object_keys = Vec::new();
        for entry in self.materializer.view().entries() {
            object_keys.push(TestArtifacts::object_key(&entry.artifact.artifact_key));
        }
        object_keys
    }
}

/// A head that holds the given records and nothing else.
fn head_holding(decoded_records: &[(SequenceNumber, Vec<SeriesSamples>)]) -> SharedHead {
    let head = SharedHead::default();
    head.add_records(decoded_records);
    head
}

/// No record: what a head that never sealed says it has given up.
const NEVER_SEALED: SequenceNumber = SequenceNumber::ZERO;

#[test]
fn empty_head_adds_nothing_to_the_blocks() {
    assert_eq!(how_the_head_fits(at(5), None, NEVER_SEALED), HeadFit::CoveredByBlocks);
}

#[test]
fn head_right_after_the_catalog_goes_on_top() {
    let fit = how_the_head_fits(at(5), Some((at(6), at(9))), at(5));

    assert_eq!(fit, HeadFit::OnTopOfBlocks { last_held: at(9) });
}

#[test]
fn head_without_any_block_goes_on_top() {
    let fit = how_the_head_fits(SequenceNumber::ZERO, Some((at(1), at(3))), NEVER_SEALED);

    assert_eq!(fit, HeadFit::OnTopOfBlocks { last_held: at(3) });
}

#[test]
fn head_that_starts_inside_the_catalog_and_ends_beyond_it_goes_on_top() {
    let fit = how_the_head_fits(at(5), Some((at(3), at(9))), at(2));

    assert_eq!(fit, HeadFit::OnTopOfBlocks { last_held: at(9) });
}

#[test]
fn head_that_ends_where_the_catalog_ends_is_covered() {
    let fit = how_the_head_fits(at(5), Some((at(3), at(5))), at(2));

    assert_eq!(fit, HeadFit::CoveredByBlocks);
}

#[test]
fn head_that_ends_before_the_catalog_ends_is_covered() {
    let fit = how_the_head_fits(at(5), Some((at(1), at(4))), NEVER_SEALED);

    assert_eq!(fit, HeadFit::CoveredByBlocks);
}

#[test]
fn head_that_starts_later_than_right_after_the_catalog_leaves_a_gap() {
    let fit = how_the_head_fits(at(5), Some((at(7), at(9))), NEVER_SEALED);

    assert_eq!(fit, HeadFit::Gap);
}

#[test]
fn empty_head_that_sealed_beyond_the_catalog_leaves_a_gap() {
    // The head has just given records 6 to 9 up to a block that the catalog does not list yet.
    assert_eq!(how_the_head_fits(at(5), None, at(9)), HeadFit::Gap);
}

#[test]
fn empty_head_whose_last_seal_the_catalog_lists_is_covered() {
    assert_eq!(how_the_head_fits(at(9), None, at(9)), HeadFit::CoveredByBlocks);
}

#[test]
fn head_that_sealed_beyond_the_catalog_and_holds_later_records_leaves_a_gap() {
    let fit = how_the_head_fits(at(5), Some((at(10), at(12))), at(9));

    assert_eq!(fit, HeadFit::Gap);
}

#[test]
fn catalog_without_blocks_ends_before_the_first_record() {
    assert_eq!(end_of(&[]), SequenceNumber::ZERO);
}

#[test]
fn series_without_a_sample_is_left_out_of_an_answer() {
    let mut merged = BTreeMap::new();
    merged.insert(job("api"), samples_at(&[10]));
    merged.insert(job("idle"), Vec::new());

    assert_eq!(into_series(merged), [series("api", samples_at(&[10]))]);
}

#[tokio::test]
async fn nothing_applied_gives_an_empty_answer_as_of_no_record() {
    let view = View::new().await;

    let nothing = QueryResult {
        as_of: SequenceNumber::ZERO,
        series: Vec::new(),
    };
    assert_eq!(view.everything().await, nothing);
}

#[tokio::test]
async fn applied_record_is_answered_from_the_head() {
    let mut view = View::new().await;

    view.apply(1, &batch("api", &[10, 20]));

    let expected = QueryResult {
        as_of: at(1),
        series: vec![series("api", samples_at(&[10, 20]))],
    };
    assert_eq!(view.everything().await, expected);
}

#[tokio::test]
async fn sealed_record_is_answered_from_its_block() {
    let mut view = View::new().await;
    view.apply(1, &batch("api", &[10, 20]));

    view.checkpoint().await;

    let expected = QueryResult {
        as_of: at(1),
        series: vec![series("api", samples_at(&[10, 20]))],
    };
    assert_eq!(view.everything().await, expected);
}

#[tokio::test]
async fn answer_joins_blocks_and_head_in_time_order() {
    let mut view = View::new().await;
    view.apply(1, &batch("api", &[10, 20]));
    view.checkpoint().await;
    view.apply(2, &batch("api", &[30]));
    view.checkpoint().await;
    view.apply(3, &batch("api", &[40, 50]));

    let answer = view.everything().await;

    assert_eq!(answer.series, [series("api", samples_at(&[10, 20, 30, 40, 50]))]);
}

#[tokio::test]
async fn answer_with_blocks_and_head_is_exact_up_to_the_last_applied_record() {
    let mut view = View::new().await;
    view.apply(1, &batch("api", &[10]));
    view.checkpoint().await;
    view.apply(2, &batch("api", &[20]));
    view.apply(3, &batch("api", &[30]));

    assert_eq!(view.everything().await.as_of, at(3));
}

#[tokio::test]
async fn sample_in_the_head_replaces_the_one_a_block_has_for_the_same_time() {
    let mut view = View::new().await;
    view.apply(1, &batch("api", &[10, 20]));
    view.checkpoint().await;

    view.apply(2, &[series("api", vec![sample(20, 7.0)])]);

    let answer = view.everything().await;
    assert_eq!(answer.series, [series("api", vec![sample(10, 10.0), sample(20, 7.0)])]);
}

#[tokio::test]
async fn sample_in_a_later_block_replaces_the_one_an_earlier_block_has() {
    let mut view = View::new().await;
    view.apply(1, &batch("api", &[10, 20]));
    view.checkpoint().await;
    view.apply(2, &[series("api", vec![sample(20, 7.0)])]);

    view.checkpoint().await;

    let answer = view.everything().await;
    assert_eq!(answer.series, [series("api", vec![sample(10, 10.0), sample(20, 7.0)])]);
}

#[tokio::test]
async fn sample_appended_twice_is_answered_once() {
    let mut view = View::new().await;
    view.apply(1, &batch("api", &[10, 20]));
    view.checkpoint().await;
    view.apply(2, &batch("api", &[10, 20]));
    view.checkpoint().await;

    view.apply(3, &batch("api", &[20]));

    assert_eq!(view.everything().await.series, [series("api", samples_at(&[10, 20]))]);
}

#[tokio::test]
async fn samples_that_arrive_out_of_order_are_answered_in_time_order() {
    let mut view = View::new().await;
    view.apply(1, &batch("api", &[50, 60]));
    view.checkpoint().await;
    view.apply(2, &batch("api", &[10, 55]));
    view.checkpoint().await;

    view.apply(3, &batch("api", &[5, 57, 70]));

    let answer = view.everything().await;
    assert_eq!(answer.series, [series("api", samples_at(&[5, 10, 50, 55, 57, 60, 70]))]);
}

/// A view with two blocks: record 1 with `api` at 10 and 20, record 2 with `api` at 30 and 40.
async fn view_with_two_blocks() -> View {
    let mut view = View::new().await;
    view.apply(1, &batch("api", &[10, 20]));
    view.checkpoint().await;
    view.apply(2, &batch("api", &[30, 40]));
    view.checkpoint().await;
    view
}

#[tokio::test]
async fn range_from_the_last_sample_of_one_block_to_the_first_of_the_next_includes_both() {
    let view = view_with_two_blocks().await;
    let range = TimeRange::new(20, 30).expect("valid range");

    let answer = view.query(&Selector::all(), range).await.expect("an answer");

    assert_eq!(answer.series, [series("api", samples_at(&[20, 30]))]);
}

#[tokio::test]
async fn range_between_two_blocks_is_empty() {
    let view = view_with_two_blocks().await;
    let range = TimeRange::new(21, 29).expect("valid range");

    let answer = view.query(&Selector::all(), range).await.expect("an answer");

    assert_eq!(answer.series, []);
}

#[tokio::test]
async fn range_that_ends_on_the_first_sample_of_a_block_includes_that_sample() {
    let view = view_with_two_blocks().await;
    let range = TimeRange::new(25, 30).expect("valid range");

    let answer = view.query(&Selector::all(), range).await.expect("an answer");

    assert_eq!(answer.series, [series("api", samples_at(&[30]))]);
}

#[tokio::test]
async fn range_of_one_instant_returns_the_sample_at_that_instant() {
    let view = view_with_two_blocks().await;
    let range = TimeRange::new(40, 40).expect("valid range");

    let answer = view.query(&Selector::all(), range).await.expect("an answer");

    assert_eq!(answer.series, [series("api", samples_at(&[40]))]);
}

#[tokio::test]
async fn empty_answer_is_still_exact_up_to_the_last_record() {
    let view = view_with_two_blocks().await;
    let range = TimeRange::new(21, 29).expect("valid range");

    let answer = view.query(&Selector::all(), range).await.expect("an answer");

    assert_eq!(answer.as_of, at(2));
}

#[tokio::test]
async fn only_the_selected_series_are_answered() {
    let mut view = View::new().await;
    view.apply(1, &batch("api", &[10]));
    view.checkpoint().await;
    view.apply(2, &batch("web", &[10]));
    let only_web = Selector::new(vec![Matcher::equal("job", "web")]);

    let answer = view.query(&only_web, TimeRange::ALL).await.expect("an answer");

    assert_eq!(answer.series, [series("web", samples_at(&[10]))]);
}

#[tokio::test]
async fn series_are_answered_in_ascending_order_of_labels() {
    let mut view = View::new().await;
    view.apply(1, &batch("web", &[10]));
    view.checkpoint().await;
    view.apply(2, &batch("api", &[10]));

    let answer = view.everything().await;

    assert_eq!(
        answer.series,
        [series("api", samples_at(&[10])), series("web", samples_at(&[10]))]
    );
}

/// A view in which a series stops and another starts: `old` has samples in the first block only, `new`
/// in the head only, `steady` everywhere.
async fn view_with_series_churn() -> View {
    let mut view = View::new().await;
    view.apply(1, &batch("old", &[10, 20]));
    view.apply(2, &batch("steady", &[10, 20]));
    view.checkpoint().await;
    view.apply(3, &batch("steady", &[30, 40]));
    view.checkpoint().await;
    view.apply(4, &batch("steady", &[50]));
    view.apply(5, &batch("new", &[50, 60]));
    view
}

#[tokio::test]
async fn series_that_stopped_and_series_that_started_are_all_in_a_wide_range() {
    let view = view_with_series_churn().await;

    let answer = view.everything().await;

    let expected = [
        series("new", samples_at(&[50, 60])),
        series("old", samples_at(&[10, 20])),
        series("steady", samples_at(&[10, 20, 30, 40, 50])),
    ];
    assert_eq!(answer.series, expected);
}

#[tokio::test]
async fn series_that_stopped_is_not_in_a_later_range() {
    let view = view_with_series_churn().await;
    let later = TimeRange::new(30, 100).expect("valid range");

    let answer = view.query(&Selector::all(), later).await.expect("an answer");

    let expected = [
        series("new", samples_at(&[50, 60])),
        series("steady", samples_at(&[30, 40, 50])),
    ];
    assert_eq!(answer.series, expected);
}

#[tokio::test]
async fn series_that_started_later_is_not_in_an_earlier_range() {
    let view = view_with_series_churn().await;
    let earlier = TimeRange::new(0, 20).expect("valid range");

    let answer = view.query(&Selector::all(), earlier).await.expect("an answer");

    let expected = [
        series("old", samples_at(&[10, 20])),
        series("steady", samples_at(&[10, 20])),
    ];
    assert_eq!(answer.series, expected);
}

#[tokio::test]
async fn block_with_only_records_that_hold_no_sample_answers_nothing() {
    let mut view = View::new().await;
    view.apply(1, &batch("api", &[]));
    view.checkpoint().await;

    let nothing = QueryResult {
        as_of: at(1),
        series: Vec::new(),
    };
    assert_eq!(view.everything().await, nothing);
}

#[tokio::test]
async fn block_of_one_sample_answers_that_sample() {
    let mut view = View::new().await;
    view.apply(1, &batch("api", &[10]));
    view.checkpoint().await;

    assert_eq!(view.everything().await.series, [series("api", samples_at(&[10]))]);
}

#[tokio::test]
async fn query_reads_blocks_with_range_reads_only() {
    let view = view_with_two_blocks().await;

    view.everything().await;

    // Per block: the footer, the index, one chunk.
    assert_eq!(view.artifacts.range_reads(), 6);
}

#[tokio::test]
async fn index_of_a_block_is_fetched_once() {
    let view = view_with_two_blocks().await;
    view.everything().await;
    let reads_before = view.artifacts.range_reads();

    view.everything().await;

    // One chunk per block; the indexes come from the cache.
    assert_eq!(view.artifacts.range_reads() - reads_before, 2);
}

#[tokio::test]
async fn nothing_is_fetched_from_a_block_outside_the_time_range() {
    let view = view_with_two_blocks().await;
    let second_block_only = TimeRange::new(30, 40).expect("valid range");

    view.query(&Selector::all(), second_block_only)
        .await
        .expect("an answer");

    // The footer, the index and one chunk of the second block. The first block is left alone: its
    // catalog entry says that it ends at 20.
    assert_eq!(view.artifacts.range_reads(), 3);
}

#[tokio::test]
async fn block_whose_catalog_entry_has_no_summary_is_read_by_its_footer() {
    let view = view_with_two_blocks().await;
    let mut entries = view.materializer.view().entries();
    for entry in &mut entries {
        entry.artifact.summary = Bytes::new();
    }
    let second_block_only = TimeRange::new(30, 40).expect("valid range");

    let mut merged = BTreeMap::new();
    query_blocks(
        &view.artifacts.namespace,
        &view.cache,
        &entries,
        &Selector::all(),
        second_block_only,
        &mut merged,
    )
    .await
    .expect("read");

    assert_eq!(into_series(merged), [series("api", samples_at(&[30, 40]))]);
}

#[tokio::test]
async fn catalog_entry_of_a_block_carries_the_summary_in_its_footer() {
    let view = view_with_two_blocks().await;
    let entries = view.materializer.view().entries();

    let index = view
        .cache
        .index_of(&view.artifacts.namespace, &entries[1])
        .await
        .expect("a valid index");

    assert_eq!(
        BlockSummary::decode(&entries[1].artifact.summary),
        Some(index.footer.summary)
    );
}

#[tokio::test]
async fn nothing_is_fetched_from_a_cached_block_outside_the_time_range() {
    let view = view_with_two_blocks().await;
    view.everything().await;
    let reads_before = view.artifacts.range_reads();
    let second_block_only = TimeRange::new(30, 40).expect("valid range");

    view.query(&Selector::all(), second_block_only)
        .await
        .expect("an answer");

    assert_eq!(view.artifacts.range_reads() - reads_before, 1);
}

#[tokio::test]
async fn no_chunk_is_fetched_for_a_series_that_is_not_selected() {
    let mut view = View::new().await;
    view.apply(1, &batch("api", &[10]));
    view.apply(2, &batch("web", &[10]));
    view.checkpoint().await;
    view.everything().await;
    let reads_before = view.artifacts.range_reads();
    let no_such_job = Selector::new(vec![Matcher::equal("job", "cache")]);

    view.query(&no_such_job, TimeRange::ALL).await.expect("an answer");

    assert_eq!(view.artifacts.range_reads(), reads_before);
}

#[tokio::test]
async fn head_that_overlaps_the_catalog_answers_every_sample_once() {
    let mut view = View::new().await;
    view.apply(1, &batch("api", &[10]));
    view.apply(2, &batch("api", &[20]));
    view.checkpoint().await;
    // A head that still holds record 2, which a block already has, and record 3 beyond it.
    let overlapping = head_holding(&[(at(2), batch("api", &[20])), (at(3), batch("api", &[30]))]);

    let answer = view
        .query_with_head(&overlapping, &Selector::all(), TimeRange::ALL)
        .await;

    let expected = QueryResult {
        as_of: at(3),
        series: vec![series("api", samples_at(&[10, 20, 30]))],
    };
    assert_eq!(answer, Ok(expected));
}

#[tokio::test]
async fn head_the_catalog_has_overtaken_is_not_used() {
    let mut view = View::new().await;
    view.apply(1, &batch("api", &[10]));
    view.apply(2, &[series("api", vec![sample(10, 7.0)])]);
    view.checkpoint().await;
    // A head as a query would have read it before record 2 was applied and both were sealed.
    let overtaken = head_holding(&[(at(1), batch("api", &[10]))]);

    let answer = view.query_with_head(&overtaken, &Selector::all(), TimeRange::ALL).await;

    let expected = QueryResult {
        as_of: at(2),
        series: vec![series("api", vec![sample(10, 7.0)])],
    };
    assert_eq!(answer, Ok(expected));
}

#[tokio::test]
async fn query_gives_no_answer_rather_than_one_with_a_hole() {
    let mut view = View::new().await;
    view.apply(1, &batch("api", &[10]));
    view.checkpoint().await;
    // A head that starts at record 3: record 2 is in neither place.
    let after_a_hole = head_holding(&[(at(3), batch("api", &[30]))]);

    let answer = view
        .query_with_head(&after_a_hole, &Selector::all(), TimeRange::ALL)
        .await;

    assert_eq!(answer, Err(QueryFailure::CatalogKeptChanging));
}

#[tokio::test]
async fn failed_checkpoint_does_not_change_the_answer() {
    let mut view = View::new().await;
    view.apply(1, &batch("api", &[10]));
    view.artifacts.store.set_unavailable(true);
    let failed = view.materializer.write_checkpoint(&view.artifacts.namespace).await;
    assert!(failed.is_err());
    view.artifacts.store.set_unavailable(false);

    let answer = view.everything().await;

    let expected = QueryResult {
        as_of: at(1),
        series: vec![series("api", samples_at(&[10]))],
    };
    assert_eq!(answer, expected);
}

#[tokio::test]
async fn damaged_chunk_fails_the_query_that_reads_it() {
    let view = view_with_two_blocks().await;
    let object_keys = view.block_object_keys();
    view.artifacts.store.corrupt_byte(&object_keys[0], 6, 0x04);

    let answer = view.query(&Selector::all(), TimeRange::ALL).await;

    assert!(matches!(
        answer,
        Err(QueryFailure::Block(MaterializerError::DamagedArtifact { .. }))
    ));
}

#[tokio::test]
async fn damaged_chunk_does_not_fail_a_query_for_another_time_range() {
    let view = view_with_two_blocks().await;
    let object_keys = view.block_object_keys();
    view.artifacts.store.corrupt_byte(&object_keys[0], 6, 0x04);
    let second_block_only = TimeRange::new(30, 40).expect("valid range");

    let answer = view
        .query(&Selector::all(), second_block_only)
        .await
        .expect("an answer");

    assert_eq!(answer.series, [series("api", samples_at(&[30, 40]))]);
}

#[tokio::test]
async fn block_that_is_gone_and_stays_gone_fails_the_query() {
    let view = view_with_two_blocks().await;
    let object_keys = view.block_object_keys();
    view.artifacts
        .store
        .delete_object(&object_keys[0])
        .await
        .expect("deleted");

    let answer = view.query(&Selector::all(), TimeRange::ALL).await;

    assert_eq!(answer, Err(QueryFailure::CatalogKeptChanging));
}

/// An artifact store whose first range read fails as if the object were gone, and that serves every
/// read after it. It stands for what a query meets when a block is merged away between the moment the
/// query read the catalog and the moment it reads the block: its next attempt finds everything.
struct GoneOnce {
    /// The store that holds the objects.
    objects: MemoryArtifactStore,
    /// Whether the one failure was already given.
    has_failed: AtomicBool,
}

#[async_trait]
impl ArtifactStore for GoneOnce {
    async fn put_object(&self, object_key: &str, contents: Bytes) -> Result<(), ArtifactError> {
        self.objects.put_object(object_key, contents).await
    }

    async fn get_object(&self, object_key: &str) -> Result<Bytes, ArtifactError> {
        self.objects.get_object(object_key).await
    }

    async fn get_range(&self, object_key: &str, byte_range: Range<u64>) -> Result<Bytes, ArtifactError> {
        if !self.has_failed.swap(true, Ordering::Relaxed) {
            return Err(ArtifactError::NotFound {
                object_key: object_key.to_owned(),
            });
        }
        self.objects.get_range(object_key, byte_range).await
    }

    async fn delete_object(&self, object_key: &str) -> Result<(), ArtifactError> {
        self.objects.delete_object(object_key).await
    }

    async fn list_prefix(&self, key_prefix: &str) -> Result<Vec<ObjectInfo>, ArtifactError> {
        self.objects.list_prefix(key_prefix).await
    }
}

#[tokio::test]
async fn query_that_finds_a_block_gone_reads_the_catalog_again_and_answers() {
    let mut view = view_with_two_blocks().await;
    view.apply(3, &batch("api", &[50]));
    let gone_once = GoneOnce {
        objects: view.artifacts.store.clone(),
        has_failed: AtomicBool::new(false),
    };
    let artifacts = ArtifactNamespace::read_only(Arc::new(gone_once), TEST_STREAM_ID, METRICS_MATERIALIZER);

    let answer = query_head_and_blocks(
        &view.materializer.builder().head(),
        &view.materializer.view(),
        &artifacts,
        &view.cache,
        &Selector::all(),
        TimeRange::ALL,
    )
    .await;

    let expected = QueryResult {
        as_of: at(3),
        series: vec![series("api", samples_at(&[10, 20, 30, 40, 50]))],
    };
    assert_eq!(answer, Ok(expected));
}

#[tokio::test]
async fn index_of_a_block_the_catalog_no_longer_names_is_forgotten() {
    let view = view_with_two_blocks().await;
    view.everything().await;
    let entries = view.materializer.view().entries();

    let mut merged = BTreeMap::new();
    query_blocks(
        &view.artifacts.namespace,
        &view.cache,
        &entries[1..],
        &Selector::all(),
        TimeRange::ALL,
        &mut merged,
    )
    .await
    .expect("read");

    assert_eq!(view.cache.lock().len(), 1);
}
