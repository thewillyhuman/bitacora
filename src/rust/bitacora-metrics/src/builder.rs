//! The builder: how records become blocks.
//!
//! # Why this file exists
//!
//! The materializer framework runs everything that is the same for every workload: following the log,
//! checkpointing, fencing, keeping the catalog of artifacts. What it cannot know is how the records of
//! this workload become a file. For workloads that only ever add data and cut immutable files, it asks
//! for exactly that and nothing else, through `ArtifactBuilder` (design, section 11, "Immutable
//! artifacts"). This file is the metrics store's answer: records are batches of samples, and a file is a
//! block.
//!
//! # What it contains
//!
//! - [`BlockPolicy`] and [`StandardBlocks`]: when a block is cut and when blocks are merged.
//! - [`BlockBuilder`]: the `ArtifactBuilder` of the metrics store.
//!
//! # How it works
//!
//! - **Adding records.** A record's payload is decoded (`record.rs`) and its samples go into the head
//!   (`head.rs`), which the builder owns and the store's queries read through a handle
//!   ([`BlockBuilder::head`]). A payload that is not a metrics record stops the materializer: the record
//!   will never change, and skipping it would make the state depend on more than the log.
//! - **Asking for a seal.** The builder asks once it holds [`BlockPolicy::MAX_BLOCK_SAMPLES`] samples, or
//!   samples that span [`BlockPolicy::MAX_BLOCK_SPAN_MILLIS`]. Both are counted from the samples
//!   themselves, never from a clock, so the same records ask at the same point every time.
//! - **Sealing.** Everything in the head becomes one block (`block.rs`), stored as one artifact. Only
//!   once the block is stored does the head drop its samples, and it remembers the last record it gave
//!   up, so that a query can tell that the catalog it sees does not list the block yet.
//! - **Merging.** The catalog hands over neighbouring blocks, oldest first. Each is read whole and
//!   verified, their series are merged with the later block winning a shared timestamp, and the result is
//!   written as one block. A merge reads blocks, never the log: the log below a checkpoint may be gone.
//!
//! The thresholds are constants of a type, [`BlockPolicy`], and not fields of the builder, because the
//! framework creates a builder with `Default` and has no place to pass settings through. Tests use a
//! policy with small numbers; see `docs/decisions/0015-metrics-materializer.md`.
//!
//! # Invariants
//!
//! - The head holds exactly the samples of the records added since the last successful seal.
//! - A failed seal or merge leaves the builder as it was.
//! - Merging blocks gives the block that sealing all their records at once would have given.
//!
//! # See also
//!
//! - `block.rs`: the file a seal writes.
//! - `head.rs`: the samples the builder holds.
//! - `materializer.rs`: the materializer this builder is part of.
//! - `query.rs`: how the head and the catalog are read together.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use bitacora_materializer::{
    ArtifactBuilder, ArtifactDescriptor, ArtifactNamespace, CatalogEntry, CompactionPolicy, MaterializerError,
};
use bitacora_types::{LogRecord, SequenceRange};
use bytes::Bytes;

use crate::block::{EncodedBlock, damaged_artifact, decode_whole_block, encode_block};
use crate::head::SharedHead;
use crate::record::decode_record;
use crate::sample::{Labels, Sample, merge_series_later_wins};

/// The label of the artifacts that are blocks, for people reading a listing of the object store.
const BLOCK_LABEL: &str = "block";

/// When a block is cut and when blocks are merged.
///
/// It is a trait with constants, implemented by marker types, because the framework creates the builder
/// with `Default`: a type is the only thing a caller can choose.
pub trait BlockPolicy: Send + Sync + 'static {
    /// A block is cut once the builder holds this many samples.
    const MAX_BLOCK_SAMPLES: u64;
    /// A block is cut once the samples the builder holds span this many milliseconds, by their own
    /// timestamps.
    const MAX_BLOCK_SPAN_MILLIS: u64;
    /// When small blocks are merged into larger ones.
    const COMPACTION: CompactionPolicy;
}

/// The policy for real use: a block holds up to a million samples or two hours of them, and four
/// neighbouring blocks of similar size are merged into one.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StandardBlocks;

impl BlockPolicy for StandardBlocks {
    const MAX_BLOCK_SAMPLES: u64 = 1_000_000;
    const MAX_BLOCK_SPAN_MILLIS: u64 = 2 * 60 * 60 * 1_000;
    const COMPACTION: CompactionPolicy = CompactionPolicy {
        merge_fan_in: 4,
        smallest_tier_bytes: 1024 * 1024,
    };
}

/// Turns batches of samples into blocks: the `ArtifactBuilder` of the metrics store.
///
/// The framework owns the builder, inside an `ArtifactCatalog`. It is not used directly; see
/// [`MetricsStore`](crate::MetricsStore).
pub struct BlockBuilder<P: BlockPolicy = StandardBlocks> {
    /// The samples of the records added since the last seal, shared with the store's queries.
    head: Arc<SharedHead>,
    /// The policy is a type; nothing of it is stored.
    policy: PhantomData<P>,
}

impl<P: BlockPolicy> Default for BlockBuilder<P> {
    /// A builder that holds nothing.
    fn default() -> Self {
        Self {
            head: Arc::new(SharedHead::default()),
            policy: PhantomData,
        }
    }
}

impl<P: BlockPolicy> BlockBuilder<P> {
    /// A handle on the head, for queries.
    pub(crate) fn head(&self) -> Arc<SharedHead> {
        Arc::clone(&self.head)
    }
}

#[async_trait]
impl<P: BlockPolicy> ArtifactBuilder for BlockBuilder<P> {
    /// Decodes the records and adds their samples to the head. If one of them is not a metrics record,
    /// none is added.
    fn add_records(&mut self, committed_records: &[LogRecord]) -> Result<(), MaterializerError> {
        let mut decoded_records = Vec::with_capacity(committed_records.len());
        for record in committed_records {
            match decode_record(&record.payload) {
                Ok(batch) => decoded_records.push((record.sequence, batch)),
                Err(record_error) => {
                    return Err(MaterializerError::InvalidRecord {
                        sequence: record.sequence,
                        reason: record_error.to_string(),
                    });
                }
            }
        }
        self.head.add_records(&decoded_records);
        Ok(())
    }

    /// Asks for a seal once the samples held are as many, or span as much time, as the policy allows.
    fn should_seal(&self) -> bool {
        let fill = self.head.fill();
        fill.sample_count >= P::MAX_BLOCK_SAMPLES || fill.time_span_millis >= P::MAX_BLOCK_SPAN_MILLIS
    }

    /// Writes everything in the head as one block and then empties the head.
    async fn seal_artifact(
        &mut self,
        covered_range: SequenceRange,
        artifacts: &ArtifactNamespace,
    ) -> Result<ArtifactDescriptor, MaterializerError> {
        let descriptor = store_encoded_block(self.head.encode_block(), artifacts).await?;
        self.head.drop_sealed(covered_range.last());
        Ok(descriptor)
    }

    /// Reads the input blocks whole, merges their series with the later block winning, and writes one
    /// block.
    async fn merge_artifacts(
        &mut self,
        inputs: &[CatalogEntry],
        artifacts: &ArtifactNamespace,
    ) -> Result<ArtifactDescriptor, MaterializerError> {
        let mut merged: BTreeMap<Labels, Vec<Sample>> = BTreeMap::new();
        for input in inputs {
            let artifact_key = &input.artifact.artifact_key;
            let block = artifacts.get_artifact(artifact_key).await?;
            let samples_by_series = match decode_whole_block(&block) {
                Ok(samples_by_series) => samples_by_series,
                Err(damaged_block) => return Err(damaged_artifact(artifact_key, damaged_block)),
            };
            for (labels, samples) in &samples_by_series {
                merge_series_later_wins(&mut merged, labels, samples);
            }
        }
        store_encoded_block(encode_block(&merged), artifacts).await
    }

    /// The policy's compaction settings.
    fn compaction_policy(&self) -> CompactionPolicy {
        P::COMPACTION
    }
}

/// Stores an encoded block as a new artifact.
async fn store_encoded_block(
    encoded: EncodedBlock,
    artifacts: &ArtifactNamespace,
) -> Result<ArtifactDescriptor, MaterializerError> {
    let size_bytes = encoded.bytes.len() as u64;
    let artifact_key = artifacts.put_artifact(BLOCK_LABEL, encoded.bytes).await?;
    tracing::info!(
        %artifact_key,
        size_bytes,
        series = encoded.summary.series_count,
        samples = encoded.summary.sample_count,
        min_time_millis = encoded.summary.min_time_millis,
        max_time_millis = encoded.summary.max_time_millis,
        "block stored"
    );
    Ok(ArtifactDescriptor {
        artifact_key,
        size_bytes,
        summary: Bytes::new(),
    })
}

#[cfg(test)]
#[path = "builder_test.rs"]
mod tests;
