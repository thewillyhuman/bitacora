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
//!
//! Two parts of the adapter are in files of their own, and are part of this module:
//! `artifact_catalog/compaction.rs` holds [`CompactionPolicy`] and the rule that picks what is merged,
//! `artifact_catalog/stored.rs` holds [`decode_catalog`] and [`StoredCatalog`]: the catalog out of a committed
//! manifest, for the next instance and for a process that only queries.
//!
//! # How it works
//!
//! The catalog is a list of artifacts in log order. Each covers a range of sequence numbers, without gap
//! or overlap, from the first record applied onwards.
//!
//! - Applying records hands them to the builder, which buffers them however it likes.
//! - The builder decides when an artifact is cut ([`ArtifactBuilder::should_seal`]) and where
//!   ([`ArtifactBuilder::seal_boundary`]). A builder that decides both from the records alone cuts the
//!   same artifacts whenever checkpoints happen and whichever instance runs, which is what makes its
//!   artifacts a function of the log (invariant I6).
//! - Writing a checkpoint first seals every artifact the builder asks for. A checkpoint must also
//!   persist the records the builder holds beyond those, because the log may be truncated up to the
//!   checkpoint. A builder that can save them does so ([`ArtifactBuilder::save_pending_records`]): the
//!   saved artifact is named by the manifest next to the catalog, is not part of the catalog, and is
//!   read back by the next instance ([`ArtifactBuilder::restore_pending_records`]). A builder that
//!   cannot save has everything it holds sealed instead, however little there is; its artifacts then
//!   follow the pace of the checkpoints.
//! - After sealing, a checkpoint looks for the oldest run of [`CompactionPolicy::merge_fan_in`]
//!   neighbours in the same size tier and has the builder merge them into one artifact that covers
//!   their ranges together. At most one merge happens per checkpoint, which bounds the work a
//!   checkpoint does. The merged artifact is larger, lands in a higher tier, and is merged again once
//!   enough peers exist: size-tiered compaction.
//!
//! **What is not sealed yet.** The records applied since the last seal are in the builder and nowhere
//! else. A workload that answers queries needs them: the newest data is the data most asked for. The
//! adapter therefore shows its builder ([`ArtifactCatalog::builder`]), and a builder that wants to be
//! read while the runner works keeps what it holds behind a shared handle of its own and gives that
//! handle out. The adapter adds an artifact to the catalog right after [`ArtifactBuilder::seal_artifact`]
//! returns, with nothing awaited in between. A reader on another thread can still look between the two
//! steps; a builder that drops what it sealed inside `seal_artifact` lets its readers tell, for example
//! by remembering the last record it sealed, so that a reader who finds the catalog short of that record
//! looks again.
//!
//! **What a reader knows before it fetches.** A catalog entry holds the range of records an artifact
//! covers, its key, its size, and a summary: bytes the builder returns with the artifact and only the
//! builder's readers understand ([`ArtifactDescriptor::summary`]). A query that can tell from the
//! summary that an artifact holds nothing it wants never fetches a byte of it.
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
//! - The catalog, and after it the saved pending records if there are any, cover exactly the records
//!   applied up to the last checkpoint written.
//! - Saving pending records changes neither the builder nor the catalog.
//! - A replaced artifact stays readable until the manifest that drops it is committed.
//!
//! # See also
//!
//! - `artifact_catalog/compaction.rs`: which artifacts are merged, and when.
//! - `artifact_catalog/stored.rs`: the catalog as a manifest holds it.
//! - `docs/protocol/checkpoint-manifest.md`: the layout of the stored catalog, for readers in other
//!   languages.
//! - `contract.rs`: the contract this adapter implements.
//! - `page_store.rs`: the other adapter, for workloads that update pages.

mod compaction;
mod stored;

use std::sync::{Arc, PoisonError, RwLock};

use async_trait::async_trait;
use bitacora_types::{LogRecord, SequenceNumber, SequenceRange};
use bytes::Bytes;

pub use self::compaction::CompactionPolicy;
use self::compaction::oldest_run_to_merge;
use self::stored::encode_catalog;
pub use self::stored::{StoredCatalog, decode_catalog};
use crate::contract::{CheckpointManifest, Materializer, MaterializerError};
use crate::namespace::ArtifactNamespace;

/// Where one immutable artifact is, how large it is, and what its builder says about it.
///
/// # Examples
///
/// ```
/// use bitacora_materializer::ArtifactDescriptor;
/// use bytes::Bytes;
///
/// let artifact = ArtifactDescriptor {
///     artifact_key: "00000000000000000003/0000000000-lines".to_owned(),
///     size_bytes: 4_096,
///     summary: Bytes::new(),
/// };
/// assert!(artifact.summary.is_empty());
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactDescriptor {
    /// The artifact's key, as returned by [`ArtifactNamespace::put_artifact`].
    pub artifact_key: String,
    /// The artifact's size. Compaction groups artifacts of similar size.
    pub size_bytes: u64,
    /// What every reader of the catalog should know about the artifact without fetching it: for
    /// example the span of time its data covers, so that a query for another time leaves it alone. The
    /// builder writes it and the builder's readers understand it; the adapter stores it in the catalog
    /// and never looks inside. Empty if the builder has nothing to say. Keep it small: every summary is
    /// in every manifest.
    pub summary: Bytes,
}

/// One artifact of the catalog and the records it covers.
///
/// # Examples
///
/// ```
/// use bitacora_materializer::{ArtifactDescriptor, CatalogEntry};
/// use bitacora_types::{SequenceNumber, SequenceRange};
/// use bytes::Bytes;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let entry = CatalogEntry {
///     covered_range: SequenceRange::new(SequenceNumber::new(1), SequenceNumber::new(500))?,
///     artifact: ArtifactDescriptor {
///         artifact_key: "00000000000000000003/0000000000-lines".to_owned(),
///         size_bytes: 4_096,
///         summary: Bytes::new(),
///     },
/// };
/// assert_eq!(entry.covered_range.last(), SequenceNumber::new(500));
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogEntry {
    /// The records the artifact was built from, both ends included.
    pub covered_range: SequenceRange,
    /// The artifact.
    pub artifact: ArtifactDescriptor,
}

/// What a workload with append-only data implements, instead of the whole [`Materializer`] contract.
///
/// The builder is created with `Default`. What it held when the last checkpoint was written is either in
/// sealed artifacts or, for a builder that saves its pending records, handed back to
/// [`ArtifactBuilder::restore_pending_records`]. Whatever was added after that checkpoint is handed over
/// again by the runner.
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
///         Ok(ArtifactDescriptor { artifact_key, size_bytes, summary: Bytes::new() })
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
///         Ok(ArtifactDescriptor { artifact_key, size_bytes, summary: Bytes::new() })
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
    /// own thresholds, and the checkpoint seals: once, or for as long as the answer stays yes.
    fn should_seal(&self) -> bool;

    /// Where the next artifact ends, for a builder that cuts artifacts at boundaries of its own.
    ///
    /// The adapter asks when [`ArtifactBuilder::should_seal`] says yes. A builder that answers from
    /// the records alone, for example "after the record that brought the samples to a million", cuts
    /// the same artifacts whenever checkpoints happen.
    ///
    /// # Returns
    ///
    /// The last record the next artifact covers; it must be one of the records the builder holds.
    /// `None`, the default, means everything the builder holds.
    ///
    /// # Panics
    ///
    /// The adapter panics if the record is not one the builder holds: that is a mistake in the
    /// builder, and sealing anything else would put records in the wrong artifact.
    fn seal_boundary(&self) -> Option<SequenceNumber> {
        None
    }

    /// Writes one immutable artifact from the oldest records the builder holds, and drops those
    /// records from the builder.
    ///
    /// # Arguments
    ///
    /// * `covered_range` - The records the artifact must hold: from the first record not sealed yet to
    ///   [`ArtifactBuilder::seal_boundary`], or to the last record added if the builder names no
    ///   boundary or cannot save its pending records.
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

    /// Saves the records the builder holds, without sealing them, so that a checkpoint can be written
    /// while an artifact is still filling.
    ///
    /// The adapter calls it at a checkpoint, after every artifact that was due has been sealed, if the
    /// builder still holds records. The builder keeps what it holds.
    ///
    /// # Returns
    ///
    /// The artifact that [`ArtifactBuilder::restore_pending_records`] rebuilds the builder's present
    /// state from. `None`, the default, means that this builder cannot save: the adapter then seals
    /// what the builder holds, with [`ArtifactBuilder::seal_artifact`].
    ///
    /// # Errors
    ///
    /// Returns [`MaterializerError`] if the artifact cannot be stored. The checkpoint is tried again.
    async fn save_pending_records(
        &mut self,
        _artifacts: &ArtifactNamespace,
    ) -> Result<Option<ArtifactDescriptor>, MaterializerError> {
        Ok(None)
    }

    /// Puts a new builder into the state that [`ArtifactBuilder::save_pending_records`] saved.
    ///
    /// The adapter calls it once, on a builder fresh from `Default`, when the checkpoint it restores
    /// from holds saved records. Afterwards the builder must behave exactly as the one that saved did:
    /// ask for the same seals at the same boundaries, and seal the same artifacts.
    ///
    /// # Arguments
    ///
    /// * `saved` - The saved artifact and the records it holds.
    /// * `artifacts` - Where the artifact is read from.
    ///
    /// # Errors
    ///
    /// Returns [`MaterializerError`] if the artifact cannot be read or is damaged. The default, for a
    /// builder that never saves, refuses: such a checkpoint was not written by this builder.
    async fn restore_pending_records(
        &mut self,
        saved: &CatalogEntry,
        _artifacts: &ArtifactNamespace,
    ) -> Result<(), MaterializerError> {
        Err(MaterializerError::InvalidManifest {
            reason: format!(
                "the checkpoint holds saved records in {:?}, and this builder does not save any",
                saved.artifact.artifact_key
            ),
        })
    }

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
        self.entries.read().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// Replaces the catalog.
    fn replace(&self, entries: Vec<CatalogEntry>) {
        *self.entries.write().unwrap_or_else(PoisonError::into_inner) = entries;
    }
}

/// Shows the catalog of an [`ArtifactCatalog`], from any task, while the runner works.
///
/// Take one with [`ArtifactCatalog::view`]; the example there reads a catalog through its view.
#[derive(Clone, Debug)]
pub struct CatalogView {
    /// Shared with the adapter.
    shared: Arc<SharedCatalog>,
}

impl CatalogView {
    /// Lists every sealed artifact of the catalog.
    ///
    /// # Returns
    ///
    /// The entries as they are now, in log order: gapless and without overlap. The records the builder holds
    /// beyond them are not listed, saved or not. The list is a copy; the catalog may change right after.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_materializer::CatalogView;
    ///
    /// /// How many bytes the artifacts of a catalog take in the object store.
    /// fn stored_bytes(catalog: &CatalogView) -> u64 {
    ///     let mut stored_bytes = 0;
    ///     for entry in catalog.entries() {
    ///         stored_bytes += entry.artifact.size_bytes;
    ///     }
    ///     stored_bytes
    /// }
    /// ```
    #[must_use]
    pub fn entries(&self) -> Vec<CatalogEntry> {
        self.shared.snapshot()
    }

    /// Finds the artifact that covers the record at `sequence`.
    ///
    /// # Arguments
    ///
    /// * `sequence` - The record to look up.
    ///
    /// # Returns
    ///
    /// The entry whose range holds the record. `None` if the record is below the first artifact or was not
    /// sealed into one yet.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_materializer::CatalogView;
    /// use bitacora_types::SequenceNumber;
    ///
    /// /// Whether the record at `sequence` is in a sealed artifact already.
    /// fn is_sealed(catalog: &CatalogView, sequence: SequenceNumber) -> bool {
    ///     catalog.entry_covering(sequence).is_some()
    /// }
    /// ```
    #[must_use]
    pub fn entry_covering(&self, sequence: SequenceNumber) -> Option<CatalogEntry> {
        let covers_the_record =
            |entry: &CatalogEntry| entry.covered_range.first() <= sequence && sequence <= entry.covered_range.last();
        self.shared.snapshot().into_iter().find(covers_the_record)
    }
}

/// The adapter that turns an [`ArtifactBuilder`] into a [`Materializer`] with a catalog of artifacts.
///
/// The runner owns the adapter. Take a [`CatalogView`] from it with [`ArtifactCatalog::view`], and
/// whatever the builder shares of itself through [`ArtifactCatalog::builder`], before the runner starts
/// to run; `MaterializerRunner::materializer` gives the adapter. See [`ArtifactBuilder`] for an example
/// of a builder.
pub struct ArtifactCatalog<Builder: ArtifactBuilder> {
    /// The workload's builder.
    builder: Builder,
    /// The catalog, shared with the views.
    shared: Arc<SharedCatalog>,
    /// The first record added to the builder since the last seal, or `None` if the builder is empty.
    first_pending_sequence: Option<SequenceNumber>,
    /// The last record added to the builder.
    applied_sequence: SequenceNumber,
    /// The artifact the builder's pending records were last saved to, for as long as the builder holds
    /// exactly those records. A checkpoint that finds it names it again instead of saving once more.
    saved_pending: Option<CatalogEntry>,
}

impl<Builder: ArtifactBuilder> ArtifactCatalog<Builder> {
    /// Gives a handle that shows this adapter's catalog, from any task.
    ///
    /// # Returns
    ///
    /// A view that can be cloned. It stays usable after the runner that owns the adapter has stopped; the
    /// catalog then simply no longer changes.
    ///
    /// # Examples
    ///
    /// ```
    /// # use async_trait::async_trait;
    /// # use bitacora_materializer::{ArtifactBuilder, ArtifactDescriptor, CatalogEntry, MaterializerError};
    /// # use bitacora_types::{LogRecord, SequenceRange};
    /// # #[derive(Default)]
    /// # struct LineFiles;
    /// # #[async_trait]
    /// # impl ArtifactBuilder for LineFiles {
    /// #     fn add_records(&mut self, _committed_records: &[LogRecord]) -> Result<(), MaterializerError> {
    /// #         Ok(())
    /// #     }
    /// #     fn should_seal(&self) -> bool {
    /// #         false
    /// #     }
    /// #     async fn seal_artifact(
    /// #         &mut self,
    /// #         _covered_range: SequenceRange,
    /// #         _artifacts: &ArtifactNamespace,
    /// #     ) -> Result<ArtifactDescriptor, MaterializerError> {
    /// #         unimplemented!("not sealed in this example")
    /// #     }
    /// #     async fn merge_artifacts(
    /// #         &mut self,
    /// #         _inputs: &[CatalogEntry],
    /// #         _artifacts: &ArtifactNamespace,
    /// #     ) -> Result<ArtifactDescriptor, MaterializerError> {
    /// #         unimplemented!("not merged in this example")
    /// #     }
    /// # }
    /// use std::sync::Arc;
    ///
    /// use bitacora_artifact::MemoryArtifactStore;
    /// use bitacora_materializer::{ArtifactCatalog, ArtifactNamespace, Materializer};
    /// use bitacora_types::StreamId;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), MaterializerError> {
    /// let store = Arc::new(MemoryArtifactStore::new());
    /// let artifacts = ArtifactNamespace::for_runner(store, StreamId::new(7), "line-files", 1);
    /// let catalog = ArtifactCatalog::<LineFiles>::restore_from_checkpoint(None, &artifacts).await?;
    ///
    /// let view = catalog.view();
    /// assert!(view.entries().is_empty());
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn view(&self) -> CatalogView {
        CatalogView {
            shared: Arc::clone(&self.shared),
        }
    }

    /// The workload's builder, to take from it whatever it shares with its readers.
    ///
    /// The adapter keeps changing the builder for as long as the runner works, so there is nothing to
    /// change through this reference. A builder whose unsealed records must be readable from other
    /// tasks holds them behind a shared handle and has a method that clones the handle; this is where a
    /// front end calls that method. See the file documentation for what such a reader must mind.
    ///
    /// # Returns
    ///
    /// The builder the adapter was restored with.
    ///
    /// # Examples
    ///
    /// ```
    /// # use async_trait::async_trait;
    /// # use bitacora_materializer::{
    /// #     ArtifactBuilder, ArtifactCatalog, ArtifactDescriptor, ArtifactNamespace, CatalogEntry, Materializer,
    /// #     MaterializerError,
    /// # };
    /// # use bitacora_types::{LogRecord, SequenceRange};
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicU64, Ordering};
    ///
    /// /// A builder that lets others see how many records it holds.
    /// #[derive(Default)]
    /// struct Counting {
    ///     pending_records: Arc<AtomicU64>,
    /// }
    ///
    /// impl Counting {
    ///     fn pending_records(&self) -> Arc<AtomicU64> {
    ///         Arc::clone(&self.pending_records)
    ///     }
    /// }
    /// # #[async_trait]
    /// # impl ArtifactBuilder for Counting {
    /// #     fn add_records(&mut self, committed_records: &[LogRecord]) -> Result<(), MaterializerError> {
    /// #         self.pending_records.fetch_add(committed_records.len() as u64, Ordering::Relaxed);
    /// #         Ok(())
    /// #     }
    /// #     fn should_seal(&self) -> bool {
    /// #         false
    /// #     }
    /// #     async fn seal_artifact(
    /// #         &mut self,
    /// #         _covered_range: SequenceRange,
    /// #         _artifacts: &ArtifactNamespace,
    /// #     ) -> Result<ArtifactDescriptor, MaterializerError> {
    /// #         unimplemented!("not sealed in this example")
    /// #     }
    /// #     async fn merge_artifacts(
    /// #         &mut self,
    /// #         _inputs: &[CatalogEntry],
    /// #         _artifacts: &ArtifactNamespace,
    /// #     ) -> Result<ArtifactDescriptor, MaterializerError> {
    /// #         unimplemented!("not merged in this example")
    /// #     }
    /// # }
    ///
    /// # async fn example(artifacts: &ArtifactNamespace) -> Result<(), MaterializerError> {
    /// let catalog = ArtifactCatalog::<Counting>::restore_from_checkpoint(None, artifacts).await?;
    /// let pending_records = catalog.builder().pending_records();
    /// assert_eq!(pending_records.load(Ordering::Relaxed), 0);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub const fn builder(&self) -> &Builder {
        &self.builder
    }

    /// Seals artifacts for as long as the builder asks for one and holds records.
    async fn seal_what_is_due(&mut self, artifacts: &ArtifactNamespace) -> Result<(), MaterializerError> {
        while let Some(first_pending_sequence) = self.first_pending_sequence {
            if !self.builder.should_seal() {
                break;
            }
            let last_sealed = self.builder.seal_boundary().unwrap_or(self.applied_sequence);
            assert!(
                first_pending_sequence <= last_sealed && last_sealed <= self.applied_sequence,
                "the builder asked to seal up to record {last_sealed}, which is not among the records \
                 {first_pending_sequence} to {} that it holds",
                self.applied_sequence
            );
            self.seal_through(last_sealed, artifacts).await?;
        }
        Ok(())
    }

    /// Seals the builder's records up to `last_sealed` into one more catalog entry, if the builder holds
    /// any.
    async fn seal_through(
        &mut self,
        last_sealed: SequenceNumber,
        artifacts: &ArtifactNamespace,
    ) -> Result<(), MaterializerError> {
        let Some(first_pending_sequence) = self.first_pending_sequence else {
            return Ok(());
        };
        let covered_range = SequenceRange::new(first_pending_sequence, last_sealed)
            .expect("the first pending record is at or before the last one sealed");
        let artifact = self.builder.seal_artifact(covered_range, artifacts).await?;
        let mut entries = self.shared.snapshot();
        entries.push(CatalogEntry {
            covered_range,
            artifact,
        });
        self.shared.replace(entries);
        self.first_pending_sequence = if last_sealed == self.applied_sequence {
            None
        } else {
            Some(last_sealed.next())
        };
        self.saved_pending = None;
        Ok(())
    }

    /// Makes the records the builder still holds durable: saved, if the builder can save, and sealed
    /// otherwise.
    ///
    /// # Returns
    ///
    /// The saved artifact and the records it holds, or `None` if nothing is pending any more.
    async fn save_or_seal_pending(
        &mut self,
        artifacts: &ArtifactNamespace,
    ) -> Result<Option<CatalogEntry>, MaterializerError> {
        let Some(first_pending_sequence) = self.first_pending_sequence else {
            return Ok(None);
        };
        let covered_range = SequenceRange::new(first_pending_sequence, self.applied_sequence)
            .expect("the first pending record is at or before the last one applied");
        if let Some(saved) = &self.saved_pending
            && saved.covered_range == covered_range
        {
            return Ok(Some(saved.clone()));
        }
        let Some(artifact) = self.builder.save_pending_records(artifacts).await? else {
            self.seal_through(self.applied_sequence, artifacts).await?;
            return Ok(None);
        };
        let saved = CatalogEntry {
            covered_range,
            artifact,
        };
        self.saved_pending = Some(saved.clone());
        Ok(Some(saved))
    }

    /// Merges the oldest run of same-tier neighbours into one artifact, if there is such a run.
    async fn compact_once(&mut self, artifacts: &ArtifactNamespace) -> Result<(), MaterializerError> {
        let mut entries = self.shared.snapshot();
        let policy = self.builder.compaction_policy();
        let Some(run) = oldest_run_to_merge(&entries, policy) else {
            return Ok(());
        };
        let inputs = &entries[run.clone()];
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
        drop(entries.splice(run, [merged_entry]));
        self.shared.replace(entries);
        Ok(())
    }
}

#[async_trait]
impl<Builder: ArtifactBuilder> Materializer for ArtifactCatalog<Builder> {
    /// Hands the records to the builder and remembers where the unsealed ones start.
    fn apply_records(&mut self, committed_records: &[LogRecord]) -> Result<(), MaterializerError> {
        let (Some(first_record), Some(last_record)) = (committed_records.first(), committed_records.last()) else {
            return Ok(());
        };
        self.builder.add_records(committed_records)?;
        if self.first_pending_sequence.is_none() {
            self.first_pending_sequence = Some(first_record.sequence);
        }
        self.applied_sequence = last_record.sequence;
        // The builder holds more than was saved.
        self.saved_pending = None;
        Ok(())
    }

    /// Asks for a checkpoint when the builder has enough for an artifact.
    fn wants_checkpoint(&self) -> bool {
        self.first_pending_sequence.is_some() && self.builder.should_seal()
    }

    /// Seals the artifacts that are due, saves or seals what the builder holds beyond them, merges one
    /// run of small artifacts if there is one, and names every artifact the checkpoint needs.
    async fn write_checkpoint(
        &mut self,
        artifacts: &ArtifactNamespace,
    ) -> Result<CheckpointManifest, MaterializerError> {
        self.seal_what_is_due(artifacts).await?;
        let pending = self.save_or_seal_pending(artifacts).await?;
        self.compact_once(artifacts).await?;
        let entries = self.shared.snapshot();
        let mut artifact_keys = Vec::with_capacity(entries.len() + 1);
        for entry in &entries {
            artifact_keys.push(entry.artifact.artifact_key.clone());
        }
        if let Some(pending) = &pending {
            artifact_keys.push(pending.artifact.artifact_key.clone());
        }
        Ok(CheckpointManifest {
            artifact_keys,
            details: encode_catalog(&entries, pending.as_ref()),
        })
    }

    /// Reads the catalog out of the manifest and gives a new builder the pending records the manifest
    /// holds, if any.
    async fn restore_from_checkpoint(
        last_checkpoint: Option<CheckpointManifest>,
        artifacts: &ArtifactNamespace,
    ) -> Result<Self, MaterializerError> {
        let stored = match last_checkpoint {
            Some(manifest) => decode_catalog(&manifest.details)?,
            None => StoredCatalog::default(),
        };
        let mut builder = Builder::default();
        let mut first_pending_sequence = None;
        let mut applied_sequence = stored
            .entries
            .last()
            .map_or(SequenceNumber::ZERO, |entry| entry.covered_range.last());
        if let Some(pending) = &stored.pending {
            builder.restore_pending_records(pending, artifacts).await?;
            first_pending_sequence = Some(pending.covered_range.first());
            applied_sequence = pending.covered_range.last();
        }
        Ok(Self {
            builder,
            shared: Arc::new(SharedCatalog {
                entries: RwLock::new(stored.entries),
            }),
            first_pending_sequence,
            applied_sequence,
            saved_pending: stored.pending,
        })
    }
}

#[cfg(test)]
#[path = "artifact_catalog_test.rs"]
mod tests;
