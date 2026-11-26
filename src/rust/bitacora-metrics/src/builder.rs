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
//! - **Cutting.** The record that brings the head to [`BlockPolicy::MAX_BLOCK_SAMPLES`] samples, or to
//!   samples that span [`BlockPolicy::MAX_BLOCK_SPAN_MILLIS`], ends a block: the head closes that part
//!   and starts the next (`head.rs`). Both limits are counted from the samples themselves, never from a
//!   clock. The builder tells the framework that a block is due and after which record it ends, and the
//!   framework seals exactly those records. The same records therefore give the same blocks, whenever
//!   checkpoints happen and whichever instance runs.
//! - **Saving at a checkpoint.** A checkpoint must make everything applied durable, and the part of the
//!   head that is still filling is no block yet. It is written as an artifact in the layout of a block
//!   and named by the checkpoint next to the catalog. The next instance reads it back into its head and
//!   goes on where this one was. A process that only reads blocks reads it as one more block.
//! - **Describing.** Every block is returned to the framework with its summary: how many series and
//!   samples it holds and which span of time they cover. The framework keeps it in the catalog entry.
//! - **Sealing.** The oldest full part of the head becomes one block (`block.rs`), stored as one
//!   artifact. Only once the block is stored does the head drop that part, and it remembers the last
//!   record it gave up, so that a query can tell that the catalog it sees does not list the block yet.
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
//! - The head holds exactly the samples of the records added and not sealed yet.
//! - Where a block ends depends on the records alone.
//! - A failed seal, save or merge leaves the builder as it was.
//! - A builder restored from its saved head cuts the blocks the builder that saved would have cut.
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
use bitacora_types::{LogRecord, SequenceNumber, SequenceRange};

use crate::block::{EncodedBlock, damaged_artifact, decode_whole_block, encode_block};
use crate::head::{BlockLimits, DecodedRecord, HeadSamples, SharedHead};
use crate::record::decode_record;
use crate::sample::{Labels, Sample, merge_series_later_wins};

/// The label of the artifacts that are blocks, for people reading a listing of the object store.
const BLOCK_LABEL: &str = "block";

/// The label of the artifacts that hold the part of the head a checkpoint saved.
const SAVED_HEAD_LABEL: &str = "head";

/// When a block is cut and when blocks are merged.
///
/// It is a trait with constants, implemented by marker types, because the framework creates the builder
/// with `Default`: a type is the only thing a caller can choose.
///
/// # Examples
///
/// A policy for a store whose blocks hold at most an hour of samples:
///
/// ```
/// use bitacora_materializer::CompactionPolicy;
/// use bitacora_metrics::{BlockPolicy, MetricsStore, StandardBlocks};
///
/// struct HourlyBlocks;
///
/// impl BlockPolicy for HourlyBlocks {
///     const MAX_BLOCK_SAMPLES: u64 = StandardBlocks::MAX_BLOCK_SAMPLES;
///     const MAX_BLOCK_SPAN_MILLIS: u64 = 60 * 60 * 1_000;
///     const COMPACTION: CompactionPolicy = StandardBlocks::COMPACTION;
/// }
///
/// /// A store that cuts its blocks by that policy.
/// type HourlyStore = MetricsStore<HourlyBlocks>;
/// ```
pub trait BlockPolicy: Send + Sync + 'static {
    /// A block ends with the record that brings it to this many samples.
    const MAX_BLOCK_SAMPLES: u64;
    /// A block ends with the record that makes its samples span this many milliseconds, by their own
    /// timestamps.
    const MAX_BLOCK_SPAN_MILLIS: u64;
    /// When small blocks are merged into larger ones.
    const COMPACTION: CompactionPolicy;
}

/// The policy for real use: a block holds up to a million samples or two hours of them, and four
/// neighbouring blocks of similar size are merged into one.
///
/// # Examples
///
/// ```
/// use bitacora_metrics::{BlockPolicy, StandardBlocks};
///
/// assert_eq!(StandardBlocks::MAX_BLOCK_SAMPLES, 1_000_000);
/// assert_eq!(StandardBlocks::MAX_BLOCK_SPAN_MILLIS, 2 * 60 * 60 * 1_000);
/// ```
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
pub struct BlockBuilder<Policy: BlockPolicy = StandardBlocks> {
    /// The samples of the records added since the last seal, shared with the store's queries.
    head: Arc<SharedHead>,
    /// The policy is a type; nothing of it is stored.
    policy: PhantomData<Policy>,
}

impl<Policy: BlockPolicy> Default for BlockBuilder<Policy> {
    /// A builder that holds nothing.
    fn default() -> Self {
        Self {
            head: Arc::new(SharedHead::default()),
            policy: PhantomData,
        }
    }
}

impl<Policy: BlockPolicy> BlockBuilder<Policy> {
    /// Gives a handle on the head, for queries.
    ///
    /// # Returns
    ///
    /// The head this builder adds to and seals from. The store reads it while the runner owns the builder.
    pub(crate) fn head(&self) -> Arc<SharedHead> {
        Arc::clone(&self.head)
    }

    /// The limits of one block, from the policy.
    const fn limits() -> BlockLimits {
        BlockLimits {
            max_samples: Policy::MAX_BLOCK_SAMPLES,
            max_span_millis: Policy::MAX_BLOCK_SPAN_MILLIS,
        }
    }
}

#[async_trait]
impl<Policy: BlockPolicy> ArtifactBuilder for BlockBuilder<Policy> {
    /// Decodes the records and adds their samples to the head. If one of them is not a metrics record,
    /// none is added.
    fn add_records(&mut self, committed_records: &[LogRecord]) -> Result<(), MaterializerError> {
        let mut decoded_records = Vec::with_capacity(committed_records.len());
        for record in committed_records {
            match decode_record(&record.payload) {
                Ok(batch) => decoded_records.push(DecodedRecord {
                    sequence: record.sequence,
                    batch,
                }),
                Err(record_error) => {
                    return Err(MaterializerError::InvalidRecord {
                        sequence: record.sequence,
                        reason: record_error.to_string(),
                    });
                }
            }
        }
        self.head.add_records(&decoded_records, Self::limits());
        Ok(())
    }

    /// Asks for a seal while a part of the head is full.
    fn should_seal(&self) -> bool {
        self.head.oldest_full_part_end().is_some()
    }

    /// The record that filled the oldest full part of the head: the next block ends there.
    fn seal_boundary(&self) -> Option<SequenceNumber> {
        self.head.oldest_full_part_end()
    }

    /// Writes the parts of the head up to the end of `covered_range` as one block, then drops them from
    /// the head.
    ///
    /// # Panics
    ///
    /// Panics if no part of the head ends where `covered_range` ends. The framework seals up to the
    /// boundary this builder named, or everything it holds; anything else is a mistake in the framework.
    async fn seal_artifact(
        &mut self,
        covered_range: SequenceRange,
        artifacts: &ArtifactNamespace,
    ) -> Result<ArtifactDescriptor, MaterializerError> {
        let encoded = self
            .head
            .encode_through(covered_range.last())
            .expect("the framework seals at the end of a part of the head");
        let descriptor = store_encoded_block(encoded, BLOCK_LABEL, artifacts).await?;
        self.head.drop_through(covered_range.last());
        Ok(descriptor)
    }

    /// Writes the part of the head that is still filling as an artifact in the layout of a block. The
    /// head keeps it.
    async fn save_pending_records(
        &mut self,
        artifacts: &ArtifactNamespace,
    ) -> Result<Option<ArtifactDescriptor>, MaterializerError> {
        let saved = store_encoded_block(self.head.encode_filling_part(), SAVED_HEAD_LABEL, artifacts).await?;
        Ok(Some(saved))
    }

    /// Reads the saved part back and makes it the filling part of this builder's head.
    async fn restore_pending_records(
        &mut self,
        saved: &CatalogEntry,
        artifacts: &ArtifactNamespace,
    ) -> Result<(), MaterializerError> {
        let artifact_key = &saved.artifact.artifact_key;
        let saved_block = artifacts.get_artifact(artifact_key).await?;
        let samples_by_series = match decode_whole_block(&saved_block) {
            Ok(samples_by_series) => samples_by_series,
            Err(damaged_block) => return Err(damaged_artifact(artifact_key, damaged_block)),
        };
        self.head
            .restore_filling_part(HeadSamples::from_saved(samples_by_series, saved.covered_range));
        Ok(())
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
        store_encoded_block(encode_block(&merged), BLOCK_LABEL, artifacts).await
    }

    /// The policy's compaction settings.
    fn compaction_policy(&self) -> CompactionPolicy {
        Policy::COMPACTION
    }
}

/// Stores an encoded block as a new artifact with the given label.
async fn store_encoded_block(
    encoded: EncodedBlock,
    label: &str,
    artifacts: &ArtifactNamespace,
) -> Result<ArtifactDescriptor, MaterializerError> {
    let size_bytes = encoded.bytes.len() as u64;
    let artifact_key = artifacts.put_artifact(label, encoded.bytes).await?;
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
        summary: encoded.summary.encode(),
    })
}

#[cfg(test)]
#[path = "builder_test.rs"]
mod tests;
