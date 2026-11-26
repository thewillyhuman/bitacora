//! Querying the committed blocks from a process that is not the store.
//!
//! # Why this file exists
//!
//! The store that owns a stream answers queries from its head and its blocks. But the blocks are plain
//! objects in the object store, and the design asks that other processes can use them too: "the catalog
//! of artifacts [must] be readable by query nodes that are not the materializer" (section 19, "Common
//! thread"). A query node of that kind has no head. It reads the checkpoint the materializer last
//! committed, learns from it which blocks exist, and answers from those. This file is that query node,
//! and it is written against the framework's public interface only, to show that the interface allows
//! it.
//!
//! # What it contains
//!
//! - [`BlockReader`]: the committed blocks of one stream, and queries over them.
//! - [`BlockReaderError`]: why the blocks could not be found or read.
//!
//! # How it works
//!
//! Finding the blocks takes three steps, each through a public function of another crate: the stream's
//! identifier from the metadata store; the checkpoint the materializer last committed, with its manifest,
//! from the framework (`read_committed_checkpoint`, which copes with the materializer committing a newer
//! one at that moment); and the catalog out of the manifest's details.
//!
//! A block is cut when it is full, not when a checkpoint happens, so the catalog usually ends before the
//! checkpoint. The samples in between were in the store's head at the checkpoint, and the store saved
//! them next to the catalog as one artifact in the layout of a block. The reader reads that artifact
//! after the blocks, as one more block. Without it a reader would lag the store by up to a whole block.
//!
//! A reader sees what was committed when it last looked. [`BlockReader::refresh`] looks again; nothing
//! tells a reader that the catalog changed, so a reader that wants to stay current refreshes on a timer.
//! When a query finds that one of its blocks is gone, which happens after the materializer merged it into
//! a larger one, the reader refreshes by itself and tries once more.
//!
//! An answer is exact up to the committed checkpoint, which is older than what the store itself would
//! answer: the samples the store applied after its last checkpoint are in its memory alone.
//!
//! # Invariants
//!
//! - The reader never writes: not to the object store, not to the metadata store.
//! - An answer with `as_of` = N holds exactly what the records up to N make of the selected series,
//!   where N is the checkpoint the reader last saw.
//!
//! # See also
//!
//! - `store.rs`: the store, which also sees the head.
//! - `query.rs`: how blocks are read.

use std::collections::BTreeMap;
use std::sync::Arc;

use bitacora_artifact::{ArtifactError, ArtifactStore};
use bitacora_materializer::{
    ArtifactNamespace, CatalogEntry, CommittedCheckpointError, MaterializerError, decode_catalog,
    read_committed_checkpoint,
};
use bitacora_meta::{MetaError, MetadataStore};
use bitacora_types::{SequenceNumber, StreamId};
use thiserror::Error;

use crate::materializer::METRICS_MATERIALIZER;
use crate::query::{BlockIndexCache, QueryResult, into_series, query_blocks};
use crate::sample::TimeRange;
use crate::selector::Selector;

/// How many times a query reads the catalog again and starts over because a block it was about to read
/// had been merged away.
const MAX_QUERY_ATTEMPTS: usize = 4;

/// Why the committed blocks could not be found or read.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum BlockReaderError {
    /// The metadata store refused or could not be reached, the stream does not exist, or no metrics
    /// store was ever prepared on it.
    #[error(transparent)]
    Metadata(#[from] MetaError),
    /// The manifest or a block could not be read, or is damaged.
    #[error(transparent)]
    Block(#[from] MaterializerError),
    /// The materializer committed new checkpoints faster than the reader could read one.
    #[error("the committed checkpoint kept changing while it was being read")]
    CheckpointKeptChanging,
}

impl From<CommittedCheckpointError> for BlockReaderError {
    /// Sorts what the framework could not read into what the reader reports.
    fn from(error: CommittedCheckpointError) -> Self {
        match error {
            CommittedCheckpointError::Metadata(meta_error) => Self::Metadata(meta_error),
            CommittedCheckpointError::Manifest(read_error) => Self::Block(read_error),
            CommittedCheckpointError::ManifestKeptDisappearing { .. } => Self::CheckpointKeptChanging,
        }
    }
}

/// The blocks the materializer has committed, as of one checkpoint.
#[derive(Clone, Debug)]
struct CommittedBlocks {
    /// The checkpoint the blocks belong to: they hold every record up to it.
    checkpoint: SequenceNumber,
    /// The blocks, in log order.
    entries: Vec<CatalogEntry>,
}

/// Answers queries from the committed blocks of a metrics stream, without being its store.
///
/// # Examples
///
/// ```no_run
/// # use std::sync::Arc;
/// # use bitacora_artifact::ArtifactStore;
/// # use bitacora_meta::MetadataStore;
/// # use bitacora_metrics::{BlockReader, Selector, TimeRange};
/// # async fn example(
/// #     metadata: MetadataStore,
/// #     artifact_store: Arc<dyn ArtifactStore>,
/// # ) -> Result<(), bitacora_metrics::BlockReaderError> {
/// let mut reader = BlockReader::open(metadata, artifact_store, "tenant-7").await?;
///
/// let answer = reader.query(&Selector::all(), TimeRange::ALL).await?;
/// println!("{} series as of record {}", answer.series.len(), answer.as_of);
///
/// reader.refresh().await?;
/// # Ok(())
/// # }
/// ```
pub struct BlockReader {
    /// The metadata store, for the committed checkpoint.
    metadata: MetadataStore,
    /// The stream whose blocks are read.
    stream_id: StreamId,
    /// Where the manifest and the blocks are read from.
    artifacts: ArtifactNamespace,
    /// The indexes of the blocks in use.
    block_indexes: BlockIndexCache,
    /// The blocks as of the checkpoint last seen.
    committed: CommittedBlocks,
}

impl BlockReader {
    /// Opens a reader on the committed blocks of `stream_name`.
    ///
    /// Reads the stream record and the committed checkpoint. No block is fetched yet.
    ///
    /// # Arguments
    ///
    /// * `metadata` - The metadata store.
    /// * `artifact_store` - The object store the stream's metrics store keeps its blocks in.
    /// * `stream_name` - The stream.
    ///
    /// # Errors
    ///
    /// Returns [`BlockReaderError::Metadata`] if the stream does not exist, no metrics store was
    /// prepared on it, or the metadata store cannot be reached, and [`BlockReaderError::Block`] if the
    /// committed manifest cannot be read.
    pub async fn open(
        metadata: MetadataStore,
        artifact_store: Arc<dyn ArtifactStore>,
        stream_name: &str,
    ) -> Result<Self, BlockReaderError> {
        let stream = metadata.get_stream(stream_name).await?;
        let artifacts = ArtifactNamespace::read_only(artifact_store, stream.stream_id, METRICS_MATERIALIZER);
        let mut reader = Self {
            metadata,
            stream_id: stream.stream_id,
            artifacts,
            block_indexes: BlockIndexCache::default(),
            // Replaced by the refresh below, before anybody can ask.
            committed: CommittedBlocks {
                checkpoint: SequenceNumber::ZERO,
                entries: Vec::new(),
            },
        };
        reader.refresh().await?;
        Ok(reader)
    }

    /// Looks for the checkpoint the materializer has committed by now, and the blocks that go with it.
    ///
    /// # Returns
    ///
    /// The checkpoint the reader now answers as of.
    ///
    /// # Errors
    ///
    /// Returns [`BlockReaderError`] if the checkpoint or its manifest cannot be read. The reader then
    /// keeps answering from the blocks it knew.
    pub async fn refresh(&mut self) -> Result<SequenceNumber, BlockReaderError> {
        let committed =
            read_committed_checkpoint(&self.metadata, &self.artifacts, self.stream_id, METRICS_MATERIALIZER).await?;
        let mut entries = Vec::new();
        // Without a manifest nothing was ever checkpointed: there is no block.
        if let Some(manifest) = committed.manifest {
            let stored_catalog = decode_catalog(&manifest.details)?;
            entries = stored_catalog.entries;
            // What the store had applied and not cut into a block yet. It is saved in the layout of a
            // block and covers the records from the last block to the checkpoint.
            if let Some(saved_head) = stored_catalog.pending {
                entries.push(saved_head);
            }
        }
        self.committed = CommittedBlocks {
            checkpoint: committed.checkpoint,
            entries,
        };
        Ok(committed.checkpoint)
    }

    /// The checkpoint the reader answers as of.
    #[must_use]
    pub const fn checkpoint(&self) -> SequenceNumber {
        self.committed.checkpoint
    }

    /// What the reader answers from, in log order: the blocks of the catalog and, after them, the part
    /// of the store's head that the checkpoint saved, if it saved one. That part has the layout of a
    /// block and is read like one.
    #[must_use]
    pub fn blocks(&self) -> &[CatalogEntry] {
        &self.committed.entries
    }

    /// Returns the samples of the selected series in a span of time, from the committed blocks.
    ///
    /// If a block is gone because the materializer merged it since the reader last looked, the reader
    /// refreshes and tries again.
    ///
    /// # Arguments
    ///
    /// * `selector` - Which series.
    /// * `time_range` - Which samples of them, both ends included.
    ///
    /// # Returns
    ///
    /// The answer, exact up to the checkpoint the reader saw last.
    ///
    /// # Errors
    ///
    /// Returns [`BlockReaderError::Block`] if a block cannot be read or is damaged, and what
    /// [`BlockReader::refresh`] returns if a refresh was needed and failed.
    pub async fn query(&mut self, selector: &Selector, time_range: TimeRange) -> Result<QueryResult, BlockReaderError> {
        for _attempt in 0..MAX_QUERY_ATTEMPTS {
            let mut merged = BTreeMap::new();
            let read = query_blocks(
                &self.artifacts,
                &self.block_indexes,
                &self.committed.entries,
                selector,
                time_range,
                &mut merged,
            )
            .await;
            match read {
                Ok(()) => {
                    return Ok(QueryResult {
                        as_of: self.committed.checkpoint,
                        series: into_series(merged),
                    });
                }
                Err(MaterializerError::ArtifactStore(ArtifactError::NotFound { .. })) => {
                    self.refresh().await?;
                }
                Err(block_error) => return Err(BlockReaderError::Block(block_error)),
            }
        }
        Err(BlockReaderError::CheckpointKeptChanging)
    }
}

#[cfg(test)]
#[path = "reader_test.rs"]
mod tests;
