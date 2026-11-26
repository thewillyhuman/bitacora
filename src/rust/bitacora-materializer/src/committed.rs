//! Reading what a materializer last committed, from a process that is not its runner.
//!
//! # Why this file exists
//!
//! The artifacts of a materializer are plain objects, and the design asks that other processes can use
//! them: query nodes, followers, tools (section 19, "Common thread"). Such a process does not run the
//! materializer. It has to find the checkpoint the runner last committed and the manifest that goes with
//! it, and it has to cope with the runner committing a newer one at that very moment. Every such process
//! needs the same few steps and the same care; this file holds them once.
//!
//! # What it contains
//!
//! - [`read_committed_checkpoint`]: the checkpoint a materializer last committed, with its manifest.
//! - [`CommittedCheckpoint`]: what was found.
//! - [`CommittedCheckpointError`]: why nothing could be read.
//!
//! # How it works
//!
//! The checkpoint record in the metadata store names the manifest; the manifest is an object in the
//! materializer's namespace. They are read one after the other, and the runner may commit in between: it
//! writes a new manifest, points the record at it, and deletes the old one. A reader that finds the
//! manifest it was pointed at gone therefore reads the record again. After a few rounds it gives up,
//! because a manifest that stays gone means something else is wrong.
//!
//! The manifest states the checkpoint it belongs to. That is the checkpoint returned: it is the one the
//! artifacts are consistent with, whatever the record says by the time the caller looks.
//!
//! # Invariants
//!
//! - Nothing is written, to the metadata store or to the object store.
//! - What is returned was committed: a manifest that no checkpoint record ever named is never read.
//!
//! # See also
//!
//! - `manifest.rs`: the stored form of a manifest.
//! - `runner.rs`: the writer of what this file reads.
//! - `artifact_catalog.rs`: `decode_catalog`, for the details of a manifest that a catalog wrote.

use bitacora_artifact::ArtifactError;
use bitacora_meta::{MetaError, MetadataStore};
use bitacora_types::{SequenceNumber, StreamId};
use thiserror::Error;

use crate::contract::{CheckpointManifest, MaterializerError};
use crate::manifest::decode_manifest;
use crate::namespace::ArtifactNamespace;

/// How many times the checkpoint record is read again because the manifest it named was gone.
const MAX_ATTEMPTS: usize = 4;

/// What a materializer last committed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedCheckpoint {
    /// The last record the checkpoint covers. The log may be truncated up to here.
    pub checkpoint: SequenceNumber,
    /// What the materializer wrote for that checkpoint: the artifacts it needs and its own details.
    /// `None` if the materializer is registered and has never committed a checkpoint.
    pub manifest: Option<CheckpointManifest>,
}

/// Why the committed checkpoint could not be read.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum CommittedCheckpointError {
    /// The metadata store refused or could not be reached, or the materializer is not registered on the
    /// stream.
    #[error(transparent)]
    Metadata(#[from] MetaError),
    /// The manifest could not be read, or is damaged.
    #[error(transparent)]
    Manifest(#[from] MaterializerError),
    /// Every manifest the checkpoint record named was gone by the time it was read.
    #[error("the manifest of the committed checkpoint was gone on each of {attempts} attempts to read it")]
    ManifestKeptDisappearing {
        /// How many times the record was read and its manifest looked for.
        attempts: usize,
    },
}

/// Reads the checkpoint a materializer last committed, and its manifest.
///
/// Reads the checkpoint record from the metadata store and the manifest it names from the object store.
/// If the runner commits a newer checkpoint in between and deletes that manifest, the record is read
/// again, a few times at most.
///
/// # Arguments
///
/// * `metadata` - The metadata store.
/// * `artifacts` - The materializer's namespace, for reading: `ArtifactNamespace::read_only` of the same
///   stream and materializer.
/// * `stream_id` - The stream the materializer follows.
/// * `materializer` - The materializer's name, as registered on the stream.
///
/// # Returns
///
/// The committed checkpoint and its manifest; a checkpoint without a manifest if the materializer has
/// never checkpointed.
///
/// # Errors
///
/// * [`CommittedCheckpointError::Metadata`] if the metadata store cannot be read or the materializer is
///   not registered.
/// * [`CommittedCheckpointError::Manifest`] if the manifest cannot be read or is damaged.
/// * [`CommittedCheckpointError::ManifestKeptDisappearing`] if the named manifest was gone every time.
///
/// # Examples
///
/// ```no_run
/// # async fn example(
/// #     metadata: bitacora_meta::MetadataStore,
/// #     artifact_store: std::sync::Arc<dyn bitacora_artifact::ArtifactStore>,
/// # ) -> Result<(), Box<dyn std::error::Error>> {
/// use bitacora_materializer::{ArtifactNamespace, decode_catalog, read_committed_checkpoint};
///
/// let stream = metadata.get_stream("metrics").await?;
/// let artifacts = ArtifactNamespace::read_only(artifact_store, stream.stream_id, "bitacora-metrics");
///
/// let committed = read_committed_checkpoint(&metadata, &artifacts, stream.stream_id, "bitacora-metrics").await?;
///
/// if let Some(manifest) = committed.manifest {
///     let catalog = decode_catalog(&manifest.details)?;
///     println!("{} artifacts as of record {}", catalog.entries.len(), committed.checkpoint);
/// }
/// # Ok(())
/// # }
/// ```
pub async fn read_committed_checkpoint(
    metadata: &MetadataStore,
    artifacts: &ArtifactNamespace,
    stream_id: StreamId,
    materializer: &str,
) -> Result<CommittedCheckpoint, CommittedCheckpointError> {
    for _attempt in 0..MAX_ATTEMPTS {
        let record = metadata.get_checkpoint(stream_id, materializer).await?;
        let Some(manifest_key) = record.manifest else {
            return Ok(CommittedCheckpoint {
                checkpoint: record.checkpoint,
                manifest: None,
            });
        };
        let stored_manifest = match artifacts.get_artifact(&manifest_key).await {
            Ok(stored_manifest) => stored_manifest,
            // The runner committed a newer checkpoint and deleted this manifest. Look again.
            Err(MaterializerError::ArtifactStore(ArtifactError::NotFound { .. })) => continue,
            Err(read_error) => return Err(CommittedCheckpointError::Manifest(read_error)),
        };
        let (checkpoint, manifest) = decode_manifest(&stored_manifest)?;
        return Ok(CommittedCheckpoint {
            checkpoint,
            manifest: Some(manifest),
        });
    }
    Err(CommittedCheckpointError::ManifestKeptDisappearing { attempts: MAX_ATTEMPTS })
}

#[cfg(test)]
#[path = "committed_test.rs"]
mod tests;
