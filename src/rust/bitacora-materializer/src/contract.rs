//! The contract between a materializer and the framework that runs it.
//!
//! # Why this file exists
//!
//! A materializer is a deterministic function from a prefix of the log to queryable state (design, section
//! 11). Everything around that function is the same for every workload: tailing the log, deciding when to
//! checkpoint, committing checkpoints under a fencing token, storing artifacts. This file is the line
//! between the two. A workload implements [`Materializer`] once; the runner in `runner.rs` does the rest.
//!
//! # What it contains
//!
//! - [`Materializer`]: the three operations a workload implements, plus one optional hint.
//! - [`CheckpointManifest`]: what a materializer says about a checkpoint it wrote.
//! - [`MaterializerError`]: why applying, checkpointing or restoring failed.
//!
//! # How it works
//!
//! The runner owns the instance. It calls [`Materializer::restore_from_checkpoint`] once, then
//! [`Materializer::apply_records`] for every committed record in order, and
//! [`Materializer::write_checkpoint`] when enough bytes or time have passed. After a checkpoint is written
//! the runner commits it in the metadata store; only then may the log below it be truncated.
//!
//! A materializer never sees the raw artifact store. It gets an [`ArtifactNamespace`]: a view that can add
//! new artifacts and read existing ones, and nothing else. That is how "artifacts are immutable and have
//! unique names" is enforced instead of hoped for.
//!
//! # Rules every materializer must follow
//!
//! - **Determinism.** State at sequence N depends only on records 1 to N. No clocks, no randomness, no
//!   reads of external systems inside `apply_records`.
//! - **Replay safety.** After a crash the runner restores the last committed checkpoint and applies the
//!   records above it again. State must come out the same.
//! - **Manifests only shrink or grow by what was just written.** A manifest may name artifacts that the
//!   previous committed manifest named, and artifacts this instance wrote since. An artifact that a
//!   manifest stops naming is garbage for good; the sweeper deletes it.
//!
//! # See also
//!
//! - `namespace.rs`: the view of the artifact store a materializer writes through.
//! - `runner.rs`: the loop that drives a materializer.
//! - `sweeper.rs`: what happens to artifacts no manifest names.

use async_trait::async_trait;
use bitacora_artifact::ArtifactError;
use bitacora_types::{LogRecord, SequenceNumber};
use bytes::Bytes;
use thiserror::Error;

use crate::namespace::ArtifactNamespace;

/// Why a materializer could not apply records, write a checkpoint or restore from one.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum MaterializerError {
    /// A committed record cannot be applied. The record will never change, so trying again cannot help:
    /// the runner stops.
    #[error("record {sequence} cannot be applied: {reason}")]
    InvalidRecord {
        /// The record that was refused.
        sequence: SequenceNumber,
        /// What is wrong with it.
        reason: String,
    },
    /// An artifact does not hold what its name promises: it fails a checksum, is cut short, or comes from
    /// a format this code does not know.
    #[error("artifact {artifact_key:?} is damaged: {reason}")]
    DamagedArtifact {
        /// The artifact that was read, as named within the namespace.
        artifact_key: String,
        /// What is wrong with it.
        reason: String,
    },
    /// A checkpoint manifest cannot be understood, or names something that does not fit together.
    #[error("checkpoint manifest cannot be used: {reason}")]
    InvalidManifest {
        /// What is wrong with it.
        reason: String,
    },
    /// The label given for a new artifact cannot be part of an object key.
    #[error("artifact label {label:?} is not valid: use 1 to 64 ASCII letters, digits, '.', '_' or '-'")]
    InvalidLabel {
        /// The rejected label.
        label: String,
    },
    /// The view of the artifacts was opened for reading only and cannot add artifacts.
    #[error("this view of the artifacts is read-only")]
    ReadOnly,
    /// The artifact store failed or does not hold the artifact.
    #[error(transparent)]
    ArtifactStore(#[from] ArtifactError),
}

impl MaterializerError {
    /// Whether doing the same thing again later may succeed.
    ///
    /// # Returns
    ///
    /// `true` only for an artifact store that could not be reached. Everything else is a fact about the
    /// records or the artifacts, which waiting does not change.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_artifact::ArtifactError;
    /// use bitacora_materializer::MaterializerError;
    ///
    /// let outage = MaterializerError::ArtifactStore(ArtifactError::Unavailable {
    ///     reason: "connection refused".to_owned(),
    /// });
    /// assert!(outage.is_transient());
    /// assert!(!MaterializerError::ReadOnly.is_transient());
    /// ```
    #[must_use]
    pub const fn is_transient(&self) -> bool {
        matches!(self, Self::ArtifactStore(ArtifactError::Unavailable { .. }))
    }
}

/// What a materializer says about a checkpoint it has just written: which artifacts make it up, and how
/// they fit together.
///
/// The framework stores the manifest next to the artifacts and commits a pointer to it in the metadata
/// store. It reads `artifact_keys` to know which artifacts are still in use; `details` is the
/// materializer's own business.
///
/// # Examples
///
/// ```
/// use bitacora_materializer::CheckpointManifest;
/// use bytes::Bytes;
///
/// let manifest = CheckpointManifest {
///     artifact_keys: vec!["00000000000000000001/0000000000-snapshot".to_owned()],
///     details: Bytes::from_static(b"one snapshot, format 1"),
/// };
/// assert_eq!(manifest.artifact_keys.len(), 1);
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CheckpointManifest {
    /// Every artifact the checkpoint needs, as named by [`ArtifactNamespace::put_artifact`]. An artifact
    /// missing from this list is deleted by the sweeper, so the list must be complete.
    pub artifact_keys: Vec<String>,
    /// Whatever else the materializer needs to restore: for example which artifact covers which sequence
    /// range. The framework stores and returns these bytes unchanged.
    pub details: Bytes,
}

/// A deterministic function from a prefix of the log to queryable state. Implemented once per workload;
/// the runner owns the instance.
///
/// See the file documentation for the rules an implementation must follow.
///
/// # Examples
///
/// A materializer that counts records and checkpoints the count:
///
/// ```
/// use async_trait::async_trait;
/// use bitacora_materializer::{ArtifactNamespace, CheckpointManifest, Materializer, MaterializerError};
/// use bitacora_types::LogRecord;
/// use bytes::Bytes;
///
/// struct RecordCounter {
///     record_count: u64,
/// }
///
/// #[async_trait]
/// impl Materializer for RecordCounter {
///     fn apply_records(&mut self, committed_records: &[LogRecord]) -> Result<(), MaterializerError> {
///         self.record_count += committed_records.len() as u64;
///         Ok(())
///     }
///
///     async fn write_checkpoint(
///         &mut self,
///         artifacts: &ArtifactNamespace,
///     ) -> Result<CheckpointManifest, MaterializerError> {
///         let contents = Bytes::copy_from_slice(&self.record_count.to_le_bytes());
///         let artifact_key = artifacts.put_artifact("count", contents).await?;
///         Ok(CheckpointManifest { artifact_keys: vec![artifact_key], details: Bytes::new() })
///     }
///
///     async fn restore_from_checkpoint(
///         last_checkpoint: Option<CheckpointManifest>,
///         artifacts: &ArtifactNamespace,
///     ) -> Result<Self, MaterializerError> {
///         let Some(manifest) = last_checkpoint else {
///             return Ok(Self { record_count: 0 });
///         };
///         let contents = artifacts.get_artifact(&manifest.artifact_keys[0]).await?;
///         let record_count = u64::from_le_bytes(contents[..8].try_into().expect("eight bytes"));
///         Ok(Self { record_count })
///     }
/// }
/// ```
#[async_trait]
pub trait Materializer: Send + 'static {
    /// Applies committed records, in order.
    ///
    /// The runner calls this with consecutive records and never skips or repeats one within a run. After a
    /// restart the records above the restored checkpoint come again.
    ///
    /// # Arguments
    ///
    /// * `committed_records` - One or more consecutive records, the first right after the last one applied.
    ///
    /// # Errors
    ///
    /// Returns [`MaterializerError::InvalidRecord`] if a record can never be applied. The runner then
    /// stops: skipping a record would break determinism.
    fn apply_records(&mut self, committed_records: &[LogRecord]) -> Result<(), MaterializerError>;

    /// Whether the materializer would like to checkpoint now, before the runner's own thresholds are
    /// reached: for example because it has buffered as much as it wants to hold in memory.
    ///
    /// The runner asks after every call to [`Materializer::apply_records`]. The default never asks.
    fn wants_checkpoint(&self) -> bool {
        false
    }

    /// Persists everything applied so far and describes where it is.
    ///
    /// Everything is written through `artifacts`, under names nobody else uses, so a crash before the
    /// checkpoint is committed leaves only orphan artifacts. The runner may call this again without having
    /// committed the previous manifest, when the commit could not be made; the new manifest then replaces
    /// the old one.
    ///
    /// # Arguments
    ///
    /// * `artifacts` - Where the checkpoint's artifacts go. Borrowed for the call.
    ///
    /// # Returns
    ///
    /// The manifest of the checkpoint: every artifact needed to restore the state as of the last record
    /// applied.
    ///
    /// # Errors
    ///
    /// Returns [`MaterializerError`]. If [`MaterializerError::is_transient`] the runner tries again later,
    /// with nothing lost; otherwise it stops.
    async fn write_checkpoint(
        &mut self,
        artifacts: &ArtifactNamespace,
    ) -> Result<CheckpointManifest, MaterializerError>;

    /// Rebuilds the state from the last committed checkpoint.
    ///
    /// # Arguments
    ///
    /// * `last_checkpoint` - The manifest of the last committed checkpoint, or `None` if none was ever
    ///   committed: the state is then empty.
    /// * `artifacts` - Where the checkpoint's artifacts are. It may be cloned and kept, for example to
    ///   serve reads from artifacts later.
    ///
    /// # Errors
    ///
    /// Returns [`MaterializerError`] if an artifact cannot be read or does not hold what the manifest
    /// says.
    async fn restore_from_checkpoint(
        last_checkpoint: Option<CheckpointManifest>,
        artifacts: &ArtifactNamespace,
    ) -> Result<Self, MaterializerError>
    where
        Self: Sized;
}
