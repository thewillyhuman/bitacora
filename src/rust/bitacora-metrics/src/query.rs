//! Answering a query from the head and the blocks.
//!
//! # Why this file exists
//!
//! The samples of a series are spread over several places: older ones in blocks in the object store,
//! one block per stretch of the log, and the newest in the head in memory. A caller asks one question,
//! "the samples of these series in this span of time", and must get one answer that is the same wherever
//! the samples happen to be at that moment. Blocks are sealed, merged and deleted while queries run, so
//! this file is also where a query makes sure that what it read from the head and what it read from the
//! catalog belong to the same moment.
//!
//! # What it contains
//!
//! - [`QueryResult`]: the answer, and the record it is exact up to.
//! - `BlockIndexCache`: the indexes of the blocks in use, fetched once each.
//! - `query_blocks`: the samples a list of blocks holds for a selector and a time range.
//! - `query_head_and_blocks`: the same, with the head on top, as one consistent answer.
//! - `QueryFailure`: why no answer could be given.
//!
//! # How it works
//!
//! **What an answer contains.** A query sees every sample of every record up to one record, called
//! `as_of`, and nothing of any later record. For each series and timestamp it returns the sample of the
//! latest such record. Sources are therefore read in log order, the blocks first and the head last, and
//! each one replaces what an earlier one said about the same series and timestamp.
//!
//! **One block.** The block's index is fetched (two range reads) and kept in the cache: blocks never
//! change, so an index is valid for as long as the block exists. A block whose samples all lie outside
//! the query's time range is skipped. Otherwise the index picks the series, and only their chunks are
//! fetched.
//!
//! **Head and catalog together.** The head is read first and the catalog second, and a block can be
//! sealed in between. The head says which records its samples come from; the catalog says up to which
//! record its blocks reach. Three cases follow, and `how_the_head_fits` tells them apart:
//!
//! - The head holds records beyond the catalog and starts no later than right after it. The answer is
//!   the blocks with the head on top. Records that both hold do no harm: the head's sample for a series
//!   and timestamp comes from the latest record up to the head's end, which is the one that must win.
//! - The catalog reaches at least as far as the head, or the head is empty. Everything the head held is
//!   in a block by now, and the blocks may know later records than the head did. The answer is the
//!   blocks alone.
//! - The head starts further than right after the catalog. Records would be missing between the two.
//!   The materializer publishes a block before it drops the block's records from the head, so this is
//!   not expected; the query looks again rather than give an answer with a hole.
//!
//! **A block that disappears.** After blocks are merged, the ones they were merged from are deleted. A
//! query that still holds the older catalog then fails to read one of them. That is not an error: the
//! same samples are in the merged block. The query takes the catalog again and starts over, a bounded
//! number of times.
//!
//! # Invariants
//!
//! - An answer holds, for every selected series, exactly the samples in the time range that the records
//!   up to `as_of` leave: one per timestamp, the latest record's, in strictly increasing time order.
//! - No sample is returned from a block before the checksum that covers it was verified.
//!
//! # See also
//!
//! - `block.rs`: reading one block.
//! - `head.rs`: the samples in memory and the record numbers a query compares.
//! - `store.rs` and `reader.rs`: the two callers.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bitacora_artifact::ArtifactError;
use bitacora_materializer::{ArtifactNamespace, CatalogEntry, CatalogView, MaterializerError};
use bitacora_types::SequenceNumber;
use thiserror::Error;

use crate::block::{BlockIndex, load_block_index, read_series};
use crate::head::SharedHead;
use crate::sample::{Labels, Sample, SeriesSamples, TimeRange, merge_series_later_wins};
use crate::selector::Selector;

/// How many times a query starts over because the catalog changed under it, before it gives up.
const MAX_QUERY_ATTEMPTS: usize = 8;

/// The answer to a query.
///
/// # Examples
///
/// ```
/// use bitacora_metrics::QueryResult;
/// use bitacora_types::SequenceNumber;
///
/// let nothing_yet = QueryResult { as_of: SequenceNumber::ZERO, series: Vec::new() };
/// assert!(nothing_yet.series.is_empty());
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryResult {
    /// The answer holds every sample of the records up to this one, and nothing of later records.
    pub as_of: SequenceNumber,
    /// The selected series that have a sample in the time range, in ascending order of labels, each with
    /// its samples in the range in strictly increasing time order.
    pub series: Vec<SeriesSamples>,
}

/// Why a query could not be answered.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub(crate) enum QueryFailure {
    /// A block could not be read or is damaged.
    #[error(transparent)]
    Block(#[from] MaterializerError),
    /// The catalog changed under the query every time it tried.
    #[error("the catalog of blocks changed {MAX_QUERY_ATTEMPTS} times while the query was reading it")]
    CatalogKeptChanging,
}

/// The indexes of the blocks in use, each fetched once.
#[derive(Debug, Default)]
pub(crate) struct BlockIndexCache {
    /// The index of each block, by artifact key. Never held locked across an await.
    indexes_by_key: Mutex<BTreeMap<String, Arc<BlockIndex>>>,
}

impl BlockIndexCache {
    /// The index of the block `entry` names, from the cache or fetched and then cached.
    async fn index_of(
        &self,
        artifacts: &ArtifactNamespace,
        entry: &CatalogEntry,
    ) -> Result<Arc<BlockIndex>, MaterializerError> {
        let artifact_key = &entry.artifact.artifact_key;
        let cached = self.lock().get(artifact_key).cloned();
        if let Some(index) = cached {
            return Ok(index);
        }
        let index = Arc::new(load_block_index(artifacts, artifact_key, entry.artifact.size_bytes).await?);
        self.lock().insert(artifact_key.clone(), Arc::clone(&index));
        Ok(index)
    }

    /// Forgets the index of every block that `entries` no longer names, once the cache holds more
    /// indexes than there are blocks. A cache that is no larger than the catalog is left alone, so that
    /// the common query, against an unchanged catalog, pays one comparison.
    fn keep_only(&self, entries: &[CatalogEntry]) {
        let mut indexes_by_key = self.lock();
        if indexes_by_key.len() <= entries.len() {
            return;
        }
        let mut named_keys = BTreeSet::new();
        for entry in entries {
            named_keys.insert(entry.artifact.artifact_key.as_str());
        }
        indexes_by_key.retain(|artifact_key, _index| named_keys.contains(artifact_key.as_str()));
    }

    /// Locks the cache, whether or not another holder of the lock panicked: every change is one insert
    /// or one removal.
    fn lock(&self) -> MutexGuard<'_, BTreeMap<String, Arc<BlockIndex>>> {
        self.indexes_by_key.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Reads the selected samples in `time_range` out of `entries`, oldest block first, into `merged`.
///
/// # Arguments
///
/// * `artifacts` - Where the blocks are stored.
/// * `cache` - The cache of block indexes.
/// * `entries` - The blocks, in log order.
/// * `selector` - Which series to read.
/// * `time_range` - Which samples of them to read.
/// * `merged` - Where the samples go, by series. A later block replaces what an earlier one said about
///   the same series and timestamp.
///
/// # Errors
///
/// Returns [`MaterializerError`] if a block cannot be read or is damaged.
pub(crate) async fn query_blocks(
    artifacts: &ArtifactNamespace,
    cache: &BlockIndexCache,
    entries: &[CatalogEntry],
    selector: &Selector,
    time_range: TimeRange,
    merged: &mut BTreeMap<Labels, Vec<Sample>>,
) -> Result<(), MaterializerError> {
    cache.keep_only(entries);
    for entry in entries {
        let index = cache.index_of(artifacts, entry).await?;
        if !index.footer.summary.overlaps(time_range) {
            continue;
        }
        let selected_series = index.select(selector, time_range);
        if selected_series.is_empty() {
            continue;
        }
        let block_series = read_series(
            artifacts,
            &entry.artifact.artifact_key,
            &index,
            &selected_series,
            time_range,
        )
        .await?;
        for series in &block_series {
            merge_series_later_wins(merged, &series.labels, &series.samples);
        }
    }
    Ok(())
}

/// Turns the merged samples into the series of an answer, leaving out series without a sample.
pub(crate) fn into_series(merged: BTreeMap<Labels, Vec<Sample>>) -> Vec<SeriesSamples> {
    let mut series = Vec::with_capacity(merged.len());
    for (labels, samples) in merged {
        if !samples.is_empty() {
            series.push(SeriesSamples { labels, samples });
        }
    }
    series
}

/// The last record the blocks of a catalog reach; [`SequenceNumber::ZERO`] if the catalog is empty.
fn end_of(entries: &[CatalogEntry]) -> SequenceNumber {
    match entries.last() {
        Some(last_entry) => last_entry.covered_range.last(),
        None => SequenceNumber::ZERO,
    }
}

/// How what a query took from the head goes together with the catalog it read afterwards.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HeadFit {
    /// The head holds records beyond the catalog: the answer is the blocks with the head on top, exact
    /// up to the head's last record.
    OnTopOfBlocks {
        /// The last record the head held.
        last_held: SequenceNumber,
    },
    /// The catalog reaches at least as far as the head did: the answer is the blocks alone.
    CoveredByBlocks,
    /// Records are missing between the end of the catalog and the start of the head.
    Gap,
}

/// Tells how the head fits the catalog; see the file documentation.
///
/// # Arguments
///
/// * `catalog_end` - The last record the catalog's blocks reach; [`SequenceNumber::ZERO`] without blocks.
/// * `held_records` - The first and the last record the head held, if it held any.
fn how_the_head_fits(catalog_end: SequenceNumber, held_records: Option<(SequenceNumber, SequenceNumber)>) -> HeadFit {
    let Some((first_held, last_held)) = held_records else {
        return HeadFit::CoveredByBlocks;
    };
    if last_held <= catalog_end {
        return HeadFit::CoveredByBlocks;
    }
    if first_held > catalog_end.next() {
        return HeadFit::Gap;
    }
    HeadFit::OnTopOfBlocks { last_held }
}

/// Whether a read failed because the block is gone, which happens when a merge replaced it.
const fn is_block_gone(error: &MaterializerError) -> bool {
    matches!(error, MaterializerError::ArtifactStore(ArtifactError::NotFound { .. }))
}

/// Answers a query from the blocks of a catalog and the head on top of them.
///
/// See the file documentation for what the answer contains and how the head and the catalog are made to
/// fit together.
///
/// # Arguments
///
/// * `head` - The samples in memory.
/// * `catalog` - The blocks, as the materializer publishes them.
/// * `artifacts` - Where the blocks are stored.
/// * `cache` - The cache of block indexes.
/// * `selector` - Which series to return.
/// * `time_range` - Which samples of them to return.
///
/// # Returns
///
/// The answer, exact up to the last record the head had applied when the query read it, or up to the
/// end of the catalog if that is later.
///
/// # Errors
///
/// Returns [`QueryFailure::Block`] if a block cannot be read or is damaged, and
/// [`QueryFailure::CatalogKeptChanging`] if the catalog changed under the query at every attempt.
pub(crate) async fn query_head_and_blocks(
    head: &SharedHead,
    catalog: &CatalogView,
    artifacts: &ArtifactNamespace,
    cache: &BlockIndexCache,
    selector: &Selector,
    time_range: TimeRange,
) -> Result<QueryResult, QueryFailure> {
    for _attempt in 0..MAX_QUERY_ATTEMPTS {
        let head_snapshot = head.snapshot(selector, time_range);
        let entries = catalog.entries();
        let catalog_end = end_of(&entries);
        let head_fit = how_the_head_fits(catalog_end, head_snapshot.held_records);
        if head_fit == HeadFit::Gap {
            // Let the materializer finish what it is publishing.
            tokio::task::yield_now().await;
            continue;
        }
        let mut merged = BTreeMap::new();
        match query_blocks(artifacts, cache, &entries, selector, time_range, &mut merged).await {
            Ok(()) => {}
            // A merge replaced the block since the catalog was read. Read the catalog again.
            Err(block_error) if is_block_gone(&block_error) => continue,
            Err(block_error) => return Err(QueryFailure::Block(block_error)),
        }
        let mut as_of = catalog_end;
        if let HeadFit::OnTopOfBlocks { last_held } = head_fit {
            for (labels, samples) in &head_snapshot.samples_by_series {
                merge_series_later_wins(&mut merged, labels, samples);
            }
            as_of = last_held;
        }
        return Ok(QueryResult {
            as_of,
            series: into_series(merged),
        });
    }
    Err(QueryFailure::CatalogKeptChanging)
}

#[cfg(test)]
#[path = "query_test.rs"]
mod tests;
