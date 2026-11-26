//! Reading a block out of the object store, a part at a time.
//!
//! # Why this file exists
//!
//! A block can be large and the object store is far away, so a query must not download a block to read a few
//! series of it. `block.rs` lays a block out so that it can be read in parts; this file does the reading: the two
//! range reads that give a block's index, and the range reads that give the chunks of the series a query selected.
//! It is the only part of the block code that waits for anything, and it is kept apart so that everything else stays
//! a pure function over bytes.
//!
//! # What it contains
//!
//! - [`load_block_index`]: the footer, the series index and the postings of a block.
//! - [`read_series`]: the samples of selected series in a time range.
//! - [`damaged_artifact`]: a damaged block as the error the framework knows.
//!
//! # How it works
//!
//! The catalog records a block's size, which locates the footer: the last bytes of the block. The footer says where
//! the series index and the postings are, and they are fetched in one read. A block's index therefore costs two
//! requests, whatever the block's size.
//!
//! The chunks of the selected series are then fetched. Chunks lie in the block in the order of the series index, so
//! the chunks of series that follow each other lie next to each other. Chunks that are closer together than
//! [`JOINED_READ_GAP_BYTES`] are fetched in one read, gap included: a request to an object store costs far more
//! than the bytes of a gap do.
//!
//! # Invariants
//!
//! - Nothing is returned before the checksum that covers it was verified: the footer's, the index's and each
//!   chunk's.
//! - A read that returns fewer bytes than were asked for is reported as damage, never used as far as it goes.
//! - Nothing is written.
//!
//! # See also
//!
//! - `block.rs`: the layout of a block, and decoding what was fetched.
//! - `query.rs`: the caller, which keeps the indexes it loaded.
//! - `block_test.rs`: where the reads are tested, against an artifact store that counts them.

use bitacora_materializer::{ArtifactNamespace, MaterializerError};

use super::{BLOCK_FOOTER_BYTES, BlockFooter, BlockIndex, DamagedBlock, decode_verified_chunk};
use crate::sample::{SeriesSamples, TimeRange};

/// Two selected chunks this close together are fetched in one read, gap included. A request to an
/// object store costs far more than these bytes do.
const JOINED_READ_GAP_BYTES: u64 = 64 * 1024;

/// Wraps a [`DamagedBlock`] as the error the framework knows, naming the artifact.
///
/// # Arguments
///
/// * `artifact_key` - The key of the block that is damaged.
/// * `damaged_block` - What is wrong with it.
///
/// # Returns
///
/// [`MaterializerError::DamagedArtifact`] with the key and the reason.
pub(crate) fn damaged_artifact(artifact_key: &str, damaged_block: DamagedBlock) -> MaterializerError {
    MaterializerError::DamagedArtifact {
        artifact_key: artifact_key.to_owned(),
        reason: damaged_block.reason,
    }
}

/// Fetches and verifies the footer, the series index and the postings of a block: two range reads.
///
/// # Arguments
///
/// * `artifacts` - Where the block is stored.
/// * `artifact_key` - The block's key.
/// * `size_bytes` - The block's size, as the catalog records it. It locates the footer.
///
/// # Returns
///
/// The block's index, verified: what a query needs to choose series and to locate their chunks.
///
/// # Errors
///
/// Returns [`MaterializerError::ArtifactStore`] if the block cannot be read, and
/// [`MaterializerError::DamagedArtifact`] if what was read is not the index of a block of that size.
pub(crate) async fn load_block_index(
    artifacts: &ArtifactNamespace,
    artifact_key: &str,
    size_bytes: u64,
) -> Result<BlockIndex, MaterializerError> {
    let damaged = |damaged_block| damaged_artifact(artifact_key, damaged_block);
    let Some(footer_start) = size_bytes.checked_sub(BLOCK_FOOTER_BYTES as u64) else {
        return Err(damaged(DamagedBlock::because("it is too short to hold a footer")));
    };
    let footer_bytes = artifacts
        .get_artifact_range(artifact_key, footer_start..size_bytes)
        .await?;
    let footer = BlockFooter::decode(&footer_bytes).map_err(damaged)?;
    if footer.block_bytes() != Some(size_bytes) {
        return Err(damaged(DamagedBlock::because("it is not the size its footer states")));
    }
    let index_range = footer.index_range().expect("the block's size was just derived from it");
    let index_bytes = artifacts.get_artifact_range(artifact_key, index_range).await?;
    BlockIndex::decode(footer, &index_bytes).map_err(damaged)
}

/// Fetches, verifies and decodes the samples of the selected series of a block that lie in a time range.
///
/// Chunks that lie next to each other, or nearly so, are fetched in one range read.
///
/// # Arguments
///
/// * `artifacts` - Where the block is stored.
/// * `artifact_key` - The block's key.
/// * `index` - The block's index, from [`load_block_index`].
/// * `selected_series` - The numbers of the series to read, ascending, from [`BlockIndex::select`].
/// * `time_range` - Only samples in this range are returned.
///
/// # Returns
///
/// The selected series that have a sample in the range, in ascending order of labels, each with its
/// samples in the range.
///
/// # Errors
///
/// Returns [`MaterializerError::ArtifactStore`] if a chunk cannot be read, and
/// [`MaterializerError::DamagedArtifact`] if one fails its checksum or does not decode.
pub(crate) async fn read_series(
    artifacts: &ArtifactNamespace,
    artifact_key: &str,
    index: &BlockIndex,
    selected_series: &[u32],
    time_range: TimeRange,
) -> Result<Vec<SeriesSamples>, MaterializerError> {
    let mut series_in_range = Vec::new();
    let mut run_start = 0;
    while run_start < selected_series.len() {
        let run_end = end_of_joined_read(index, selected_series, run_start);
        let first_entry = &index.series[selected_series[run_start] as usize];
        let last_entry = &index.series[selected_series[run_end - 1] as usize];
        let joined_range = first_entry.chunk_offset..last_entry.chunk_range().end;
        let joined_bytes = artifacts.get_artifact_range(artifact_key, joined_range.clone()).await?;
        if joined_bytes.len() as u64 != joined_range.end - joined_range.start {
            return Err(damaged_artifact(
                artifact_key,
                DamagedBlock::because("a read returned fewer bytes than were asked for"),
            ));
        }
        for series_number in &selected_series[run_start..run_end] {
            let entry = &index.series[*series_number as usize];
            let chunk_start = usize::try_from(entry.chunk_offset - joined_range.start).expect("within one read");
            let chunk = &joined_bytes[chunk_start..chunk_start + entry.chunk_bytes as usize];
            let samples = decode_verified_chunk(entry, chunk, Some(time_range))
                .map_err(|damaged_block| damaged_artifact(artifact_key, damaged_block))?;
            if !samples.is_empty() {
                series_in_range.push(SeriesSamples {
                    labels: entry.labels.clone(),
                    samples,
                });
            }
        }
        run_start = run_end;
    }
    Ok(series_in_range)
}

/// Where the run of selected series that starts at `run_start` ends, if their chunks are fetched in one
/// read: before the first series whose chunk lies further than [`JOINED_READ_GAP_BYTES`] from the last.
fn end_of_joined_read(index: &BlockIndex, selected_series: &[u32], run_start: usize) -> usize {
    let mut run_end = run_start + 1;
    while run_end < selected_series.len() {
        let previous_entry = &index.series[selected_series[run_end - 1] as usize];
        let next_entry = &index.series[selected_series[run_end] as usize];
        if next_entry.chunk_offset - previous_entry.chunk_range().end > JOINED_READ_GAP_BYTES {
            break;
        }
        run_end += 1;
    }
    run_end
}
