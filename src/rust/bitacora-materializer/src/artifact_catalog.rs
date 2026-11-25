//! The adapter for append-only data that compacts into immutable files.
//!
//! # Why this file exists
//!
//! Some workloads never update anything in place: metrics blocks, index segments, log archives. They
//! collect records and, now and then, cut an immutable file that covers a range of the log. Such a
//! workload should only have to say how records become a file. It implements [`ArtifactBuilder`], and
//! [`ArtifactCatalog`] turns that into a full [`Materializer`]: it decides which range each file covers,
//! keeps the list of files, and merges small files into larger ones (design, section 11, "Immutable
//! artifacts").
//!
//! # What it contains
//!
//! - [`ArtifactBuilder`]: what a workload implements.
//! - [`ArtifactCatalog`]: the adapter; a [`Materializer`] the runner can run.
//! - [`CatalogView`]: a cloneable handle that shows the catalog while the runner works.
//! - [`CatalogEntry`] and [`ArtifactDescriptor`]: one artifact and the range it covers.
//! - [`CompactionPolicy`]: when small artifacts are merged.
//! - [`decode_catalog`]: the catalog out of a committed manifest, for a process that only queries.
//!
//! # How it works
//!
//! The catalog is a list of artifacts in log order. Each covers a range of sequence numbers; together
//! they cover every record from the first one applied to the last checkpoint, without gap or overlap.
//!
//! - Applying records hands them to the builder, which buffers them however it likes.
//! - Writing a checkpoint seals whatever the builder holds into one artifact that covers exactly the
//!   records added since the last seal, and appends it to the catalog. A checkpoint must persist
//!   everything applied, so it always seals, however little there is. The builder can ask for a
//!   checkpoint earlier through [`ArtifactBuilder::should_seal`].
//! - Checkpoints that come often therefore make small artifacts. After sealing, a checkpoint looks for
//!   the oldest run of [`CompactionPolicy::merge_fan_in`] neighbours in the same size tier and has the
//!   builder merge them into one artifact that covers their ranges together. At most one merge happens
//!   per checkpoint, which bounds the work a checkpoint does. The merged artifact is larger, lands in a
//!   higher tier, and is merged again once enough peers exist: size-tiered compaction.
//!
//! Compaction is a merge of existing artifacts, not a rebuild from the log. It cannot be a rebuild: the
//! log below the committed checkpoint may already be truncated, and the artifacts are then the only copy.
//!
//! The artifacts a merge replaces are simply no longer named by the next manifest. The framework deletes
//! them after that manifest is committed (see `sweeper.rs`); the catalog never deletes anything.
//!
//! # Invariants
//!
//! - Catalog entries are in log order, each starts right after the previous one ends, and none is empty.
//! - The catalog covers exactly the records applied up to the last checkpoint written.
//! - A replaced artifact stays readable until the manifest that drops it is committed.
//!
//! # See also
//!
//! - `contract.rs`: the contract this adapter implements.
//! - `page_store.rs`: the other adapter, for workloads that update pages.

use std::sync::{Arc, PoisonError, RwLock};

use async_trait::async_trait;
use bitacora_types::{LogRecord, SequenceNumber, SequenceRange};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::contract::{CheckpointManifest, Materializer, MaterializerError};
use crate::namespace::ArtifactNamespace;

/// The format of the details an artifact catalog puts in its checkpoint manifest.
const DETAILS_FORMAT_VERSION: u32 = 1;

/// Where one immutable artifact is and how large it is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactDescriptor {
    /// The artifact's key, as returned by [`ArtifactNamespace::put_artifact`].
    pub artifact_key: String,
    /// The artifact's size. Compaction groups artifacts of similar size.
    pub size_bytes: u64,
}

/// One artifact of the catalog and the records it covers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogEntry {
    /// The records the artifact was built from, both ends included.
    pub covered_range: SequenceRange,
    /// The artifact.
    pub artifact: ArtifactDescriptor,
}

/// When small artifacts are merged into larger ones.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionPolicy {
    /// How many neighbouring artifacts of one size tier are merged into one. At least two.
    pub merge_fan_in: usize,
    /// Artifacts smaller than this are in the lowest tier. Each tier above holds artifacts up to
    /// `merge_fan_in` times larger than the one below.
    pub smallest_tier_bytes: u64,
}

impl Default for CompactionPolicy {
    /// Four neighbours are merged; the lowest tier ends at 1 MiB.
    fn default() -> Self {
        Self {
            merge_fan_in: 4,
            smallest_tier_bytes: 1024 * 1024,
        }
    }
}

impl CompactionPolicy {
    /// The size tier of an artifact: 0 below [`CompactionPolicy::smallest_tier_bytes`], and one more for
    /// every factor of [`CompactionPolicy::merge_fan_in`] above it.
    fn tier_of(&self, size_bytes: u64) -> u32 {
        let fan_in = u64::try_from(self.merge_fan_in.max(2)).unwrap_or(u64::MAX);
        let mut tier = 0;
        let mut tier_end = self.smallest_tier_bytes.max(1);
        while size_bytes >= tier_end {
            tier += 1;
            tier_end = tier_end.saturating_mul(fan_in);
            if tier_end == u64::MAX {
                break;
            }
        }
        tier
    }
}

/// What a workload with append-only data implements, instead of the whole [`Materializer`] contract.
///
/// The builder is created with `Default` and restored empty: whatever it had buffered when the process
/// stopped was not sealed, and the runner hands those records over again.
///
/// # Examples
///
/// A builder whose artifacts are the payloads of their records, one per line:
///
/// ```
/// use async_trait::async_trait;
/// use bitacora_materializer::{
///     ArtifactBuilder, ArtifactDescriptor, ArtifactNamespace, CatalogEntry, MaterializerError,
/// };
/// use bitacora_types::{LogRecord, SequenceRange};
/// use bytes::{BufMut, Bytes, BytesMut};
///
/// #[derive(Default)]
/// struct LineFiles {
///     pending_lines: BytesMut,
/// }
///
/// #[async_trait]
/// impl ArtifactBuilder for LineFiles {
///     fn add_records(&mut self, committed_records: &[LogRecord]) -> Result<(), MaterializerError> {
///         for record in committed_records {
///             self.pending_lines.put_slice(&record.payload);
///             self.pending_lines.put_u8(b'\n');
///         }
///         Ok(())
///     }
///
///     fn should_seal(&self) -> bool {
///         self.pending_lines.len() >= 64 * 1024
///     }
///
///     async fn seal_artifact(
///         &mut self,
///         _covered_range: SequenceRange,
///         artifacts: &ArtifactNamespace,
///     ) -> Result<ArtifactDescriptor, MaterializerError> {
///         let lines = Bytes::copy_from_slice(&self.pending_lines);
///         let size_bytes = lines.len() as u64;
///         let artifact_key = artifacts.put_artifact("lines", lines).await?;
///         self.pending_lines.clear();
///         Ok(ArtifactDescriptor { artifact_key, size_bytes })
///     }
///
///     async fn merge_artifacts(
///         &mut self,
///         inputs: &[CatalogEntry],
///         artifacts: &ArtifactNamespace,
///     ) -> Result<ArtifactDescriptor, MaterializerError> {
///         let mut merged = BytesMut::new();
///         for input in inputs {
///             merged.put_slice(&artifacts.get_artifact(&input.artifact.artifact_key).await?);
///         }
///         let size_bytes = merged.len() as u64;
///         let artifact_key = artifacts.put_artifact("lines", merged.freeze()).await?;
///         Ok(ArtifactDescriptor { artifact_key, size_bytes })
///     }
/// }
/// ```
#[async_trait]
pub trait ArtifactBuilder: Default + Send + 'static {
    /// Takes committed records in, in order. They stay in the builder until the next seal.
    ///
    /// # Errors
    ///
    /// Returns [`MaterializerError::InvalidRecord`] if a record can never be added. The runner stops.
    fn add_records(&mut self, committed_records: &[LogRecord]) -> Result<(), MaterializerError>;

    /// Has enough accumulated to cut an artifact? If so the runner checkpoints without waiting for its
    /// own thresholds, which seals what the builder holds.
    fn should_seal(&self) -> bool;

    /// Writes one immutable artifact from every record added since the last seal, and empties the
    /// builder.
    ///
    /// # Arguments
    ///
    /// * `covered_range` - Exactly the records added since the last seal.
    /// * `artifacts` - Where the artifact goes.
    ///
    /// # Errors
    ///
    /// Returns [`MaterializerError`]. After an error the builder must still hold the records: the seal
    /// is tried again with the same or a longer range.
    async fn seal_artifact(
        &mut self,
        covered_range: SequenceRange,
        artifacts: &ArtifactNamespace,
    ) -> Result<ArtifactDescriptor, MaterializerError>;

    /// Writes one artifact that holds what `inputs` hold together. The inputs are left as they are.
    ///
    /// # Arguments
    ///
    /// * `inputs` - Two or more neighbouring artifacts, in log order. The merged artifact covers from the
    ///   start of the first to the end of the last.
    /// * `artifacts` - Where the inputs are read from and the merged artifact goes.
    ///
    /// # Errors
    ///
    /// Returns [`MaterializerError`] if an input cannot be read or the merged artifact cannot be stored.
    /// The catalog then keeps the inputs.
    async fn merge_artifacts(
        &mut self,
        inputs: &[CatalogEntry],
        artifacts: &ArtifactNamespace,
    ) -> Result<ArtifactDescriptor, MaterializerError>;

    /// When small artifacts are merged. The default is [`CompactionPolicy::default`].
    fn compaction_policy(&self) -> CompactionPolicy {
        CompactionPolicy::default()
    }
}

/// The catalog as the adapter and its views share it.
#[derive(Debug, Default)]
struct SharedCatalog {
    /// The artifacts in log order. Never held locked across an await.
    entries: RwLock<Vec<CatalogEntry>>,
}

impl SharedCatalog {
    /// A copy of the catalog as it is now.
    fn snapshot(&self) -> Vec<CatalogEntry> {
        self.entries
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Replaces the catalog.
    fn replace(&self, entries: Vec<CatalogEntry>) {
        *self.entries.write().unwrap_or_else(PoisonError::into_inner) = entries;
    }
}

/// Shows the catalog of an [`ArtifactCatalog`], from any task, while the runner works.
#[derive(Clone, Debug)]
pub struct CatalogView {
    /// Shared with the adapter.
    shared: Arc<SharedCatalog>,
}

impl CatalogView {
    /// Every artifact of the catalog, in log order: gapless and without overlap.
    #[must_use]
    pub fn entries(&self) -> Vec<CatalogEntry> {
        self.shared.snapshot()
    }

    /// The artifact that covers the record at `sequence`.
    ///
    /// # Returns
    ///
    /// `None` if the record is below the first artifact or was not sealed into one yet.
    #[must_use]
    pub fn entry_covering(&self, sequence: SequenceNumber) -> Option<CatalogEntry> {
        let covers_the_record = |entry: &CatalogEntry| {
            entry.covered_range.first() <= sequence && sequence <= entry.covered_range.last()
        };
        self.shared.snapshot().into_iter().find(covers_the_record)
    }
}

/// The adapter that turns an [`ArtifactBuilder`] into a [`Materializer`] with a catalog of artifacts.
///
/// The runner owns the adapter. Take a [`CatalogView`] from it with [`ArtifactCatalog::view`] before
/// handing it to the runner. See [`ArtifactBuilder`] for an example of a builder.
pub struct ArtifactCatalog<B: ArtifactBuilder> {
    /// The workload's builder.
    builder: B,
    /// The catalog, shared with the views.
    shared: Arc<SharedCatalog>,
    /// The first record added to the builder since the last seal, or `None` if the builder is empty.
    first_pending_sequence: Option<SequenceNumber>,
    /// The last record added to the builder.
    applied_sequence: SequenceNumber,
}

impl<B: ArtifactBuilder> ArtifactCatalog<B> {
    /// A handle that shows this adapter's catalog, from any task.
    #[must_use]
    pub fn view(&self) -> CatalogView {
        CatalogView {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Seals what the builder holds into one more catalog entry, if it holds anything.
    async fn seal_pending(
        &mut self,
        artifacts: &ArtifactNamespace,
    ) -> Result<(), MaterializerError> {
        let Some(first_pending_sequence) = self.first_pending_sequence else {
            return Ok(());
        };
        let covered_range = SequenceRange::new(first_pending_sequence, self.applied_sequence)
            .expect("the first pending record is at or before the last one applied");
        let artifact = self.builder.seal_artifact(covered_range, artifacts).await?;
        let mut entries = self.shared.snapshot();
        entries.push(CatalogEntry {
            covered_range,
            artifact,
        });
        self.shared.replace(entries);
        self.first_pending_sequence = None;
        Ok(())
    }

    /// Merges the oldest run of same-tier neighbours into one artifact, if there is such a run.
    async fn compact_once(
        &mut self,
        artifacts: &ArtifactNamespace,
    ) -> Result<(), MaterializerError> {
        let mut entries = self.shared.snapshot();
        let policy = self.builder.compaction_policy();
        let Some((run_start, run_end)) = oldest_run_to_merge(&entries, policy) else {
            return Ok(());
        };
        let inputs = &entries[run_start..run_end];
        let covered_range = SequenceRange::new(
            inputs[0].covered_range.first(),
            inputs[inputs.len() - 1].covered_range.last(),
        )
        .expect("catalog entries are in log order");
        let artifact = self.builder.merge_artifacts(inputs, artifacts).await?;
        let merged_entry = CatalogEntry {
            covered_range,
            artifact,
        };
        // Dropping what `splice` returns is what removes the inputs.
        drop(entries.splice(run_start..run_end, [merged_entry]));
        self.shared.replace(entries);
        Ok(())
    }
}

/// The positions, start included and end excluded, of the oldest `merge_fan_in` neighbouring entries
/// that are all in the same size tier.
fn oldest_run_to_merge(
    entries: &[CatalogEntry],
    policy: CompactionPolicy,
) -> Option<(usize, usize)> {
    let merge_fan_in = policy.merge_fan_in.max(2);
    let mut run_start = 0;
    for position in 1..=entries.len() {
        let run_goes_on = position < entries.len()
            && policy.tier_of(entries[position].artifact.size_bytes)
                == policy.tier_of(entries[run_start].artifact.size_bytes);
        if position - run_start == merge_fan_in {
            return Some((run_start, position));
        }
        if !run_goes_on {
            run_start = position;
        }
    }
    None
}

/// Whether every entry starts right after the previous one ends.
fn is_gapless(entries: &[CatalogEntry]) -> bool {
    for pair in entries.windows(2) {
        if pair[1].covered_range.first() != pair[0].covered_range.last().next() {
            return false;
        }
    }
    true
}

/// Serializes a catalog as the details of a checkpoint manifest.
fn encode_catalog(entries: &[CatalogEntry]) -> Bytes {
    let mut details = BytesMut::new();
    details.put_u32_le(DETAILS_FORMAT_VERSION);
    details.put_u32_le(u32::try_from(entries.len()).expect("far fewer than 2^32 artifacts"));
    for entry in entries {
        details.put_u64_le(entry.covered_range.first().get());
        details.put_u64_le(entry.covered_range.last().get());
        details.put_u64_le(entry.artifact.size_bytes);
        details.put_u32_le(
            u32::try_from(entry.artifact.artifact_key.len()).expect("an artifact key is short"),
        );
        details.put_slice(entry.artifact.artifact_key.as_bytes());
    }
    details.freeze()
}

/// Reads the catalog out of the details of a checkpoint manifest written by an [`ArtifactCatalog`].
///
/// A process that only queries the artifacts does not run the materializer. It reads the committed
/// manifest (see [`decode_manifest`](crate::decode_manifest)) and passes its details here.
///
/// # Arguments
///
/// * `details` - [`CheckpointManifest::details`] of a manifest an artifact catalog wrote.
///
/// # Returns
///
/// The catalog as of that checkpoint, in log order: gapless and without overlap.
///
/// # Errors
///
/// Returns [`MaterializerError::InvalidManifest`] if the bytes were not written by this version of the
/// catalog, or describe artifacts that leave a gap or overlap.
///
/// # Examples
///
/// ```
/// use bitacora_materializer::decode_catalog;
/// use bytes::Bytes;
///
/// let not_a_catalog = Bytes::from_static(b"something else");
/// assert!(decode_catalog(&not_a_catalog).is_err());
/// ```
pub fn decode_catalog(details: &Bytes) -> Result<Vec<CatalogEntry>, MaterializerError> {
    let invalid = |reason: &str| MaterializerError::InvalidManifest {
        reason: format!("the artifact catalog is not valid: {reason}"),
    };
    let cut_short = |_cut_short| invalid("it is cut short");
    let mut unread = details.clone();
    if unread.try_get_u32_le().map_err(cut_short)? != DETAILS_FORMAT_VERSION {
        return Err(invalid("it was written by another format"));
    }
    let entry_count = unread.try_get_u32_le().map_err(cut_short)?;
    let mut entries = Vec::new();
    for _entry in 0..entry_count {
        let first = SequenceNumber::new(unread.try_get_u64_le().map_err(cut_short)?);
        let last = SequenceNumber::new(unread.try_get_u64_le().map_err(cut_short)?);
        let size_bytes = unread.try_get_u64_le().map_err(cut_short)?;
        let key_bytes = unread.try_get_u32_le().map_err(cut_short)? as usize;
        if unread.remaining() < key_bytes {
            return Err(invalid("it is cut short"));
        }
        let Ok(artifact_key) = String::from_utf8(unread.split_to(key_bytes).to_vec()) else {
            return Err(invalid("an artifact key is not text"));
        };
        let Ok(covered_range) = SequenceRange::new(first, last) else {
            return Err(invalid("an artifact covers no record"));
        };
        entries.push(CatalogEntry {
            covered_range,
            artifact: ArtifactDescriptor {
                artifact_key,
                size_bytes,
            },
        });
    }
    if unread.has_remaining() {
        return Err(invalid("it holds more than its lengths say"));
    }
    if !is_gapless(&entries) {
        return Err(invalid("its artifacts leave a gap or overlap"));
    }
    Ok(entries)
}

#[async_trait]
impl<B: ArtifactBuilder> Materializer for ArtifactCatalog<B> {
    /// Hands the records to the builder and remembers where the unsealed ones start.
    fn apply_records(&mut self, committed_records: &[LogRecord]) -> Result<(), MaterializerError> {
        let (Some(first_record), Some(last_record)) =
            (committed_records.first(), committed_records.last())
        else {
            return Ok(());
        };
        self.builder.add_records(committed_records)?;
        if self.first_pending_sequence.is_none() {
            self.first_pending_sequence = Some(first_record.sequence);
        }
        self.applied_sequence = last_record.sequence;
        Ok(())
    }

    /// Asks for a checkpoint when the builder has enough for an artifact.
    fn wants_checkpoint(&self) -> bool {
        self.first_pending_sequence.is_some() && self.builder.should_seal()
    }

    /// Seals what the builder holds, merges one run of small artifacts if there is one, and names every
    /// artifact of the catalog.
    async fn write_checkpoint(
        &mut self,
        artifacts: &ArtifactNamespace,
    ) -> Result<CheckpointManifest, MaterializerError> {
        self.seal_pending(artifacts).await?;
        self.compact_once(artifacts).await?;
        let entries = self.shared.snapshot();
        let mut artifact_keys = Vec::with_capacity(entries.len());
        for entry in &entries {
            artifact_keys.push(entry.artifact.artifact_key.clone());
        }
        Ok(CheckpointManifest {
            artifact_keys,
            details: encode_catalog(&entries),
        })
    }

    /// Reads the catalog out of the manifest and starts with an empty builder.
    async fn restore_from_checkpoint(
        last_checkpoint: Option<CheckpointManifest>,
        _artifacts: &ArtifactNamespace,
    ) -> Result<Self, MaterializerError> {
        let entries = match last_checkpoint {
            Some(manifest) => decode_catalog(&manifest.details)?,
            None => Vec::new(),
        };
        let applied_sequence = entries
            .last()
            .map_or(SequenceNumber::ZERO, |entry| entry.covered_range.last());
        Ok(Self {
            builder: B::default(),
            shared: Arc::new(SharedCatalog {
                entries: RwLock::new(entries),
            }),
            first_pending_sequence: None,
            applied_sequence,
        })
    }
}

#[cfg(test)]
#[path = "artifact_catalog_test.rs"]
mod tests;
