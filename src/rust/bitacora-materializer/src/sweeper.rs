//! Deletes the artifacts that no checkpoint will ever name again.
//!
//! # Why this file exists
//!
//! Artifacts are written before the checkpoint that names them is committed (design, section 11, step 4
//! and 5). A runner that crashes in between leaves artifacts nobody refers to. So does every checkpoint
//! that drops an artifact from its manifest, for example after compaction. Without a sweeper the artifact
//! store only grows. With a careless one, a checkpoint that is about to be committed loses its artifacts.
//!
//! # What it contains
//!
//! - [`sweep_orphans`]: the sweep anyone may run at any time, for example the controller or an operator.
//! - [`delete_unreferenced`]: the deletion both sweeps share, used by the runner after its own commits.
//! - [`SweepReport`] and [`SweepError`]: what a sweep did, or why it did nothing.
//!
//! # How it works
//!
//! Every artifact key starts with the fencing token of the runner that wrote it (see `namespace.rs`). A
//! sweep reads the materializer's checkpoint record once: the current token C and the pointer to the
//! committed manifest. It then lists the namespace and deletes an artifact only if both hold:
//!
//! 1. the committed manifest does not name it, and it is not the manifest itself;
//! 2. its token is lower than C.
//!
//! Why that is safe. Only the runner that holds C can commit, and whatever it commits names artifacts of
//! the manifest it restored from, which was committed, or artifacts it wrote itself, which carry token C.
//! An artifact with a lower token that the committed manifest does not name therefore cannot be named by
//! any future manifest. This stays true if the runner commits, or another runner takes over, while the
//! sweep is under way: neither can bring such an artifact back.
//!
//! Artifacts with token C that no manifest names may belong to the checkpoint the runner is writing right
//! now. An outside sweep never touches them. The runner itself deletes them right after it has committed
//! and before it writes anything new, when it knows nothing of its own is in flight; it passes its own
//! token as the limit to [`delete_unreferenced`].
//!
//! # Invariants
//!
//! - An artifact named by the committed manifest is never deleted.
//! - An artifact whose writer may still commit it is never deleted.
//! - An object whose key the namespace did not produce is never deleted: what the sweeper does not
//!   understand, it leaves alone.
//! - If the committed manifest cannot be read, nothing is deleted.
//!
//! # See also
//!
//! - `namespace.rs`: how artifact keys carry the fencing token.
//! - `runner.rs`: the sweep after every commit.

use std::collections::BTreeSet;
use std::sync::Arc;

use bitacora_artifact::ArtifactStore;
use bitacora_meta::{MetaError, MetadataStore};
use bitacora_types::StreamId;
use thiserror::Error;

use crate::contract::MaterializerError;
use crate::manifest::decode_manifest;
use crate::namespace::{ArtifactNamespace, token_of_artifact};

/// What a sweep did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// How many artifacts were deleted.
    pub artifacts_deleted: usize,
    /// How many objects of the namespace were left in place: artifacts in use, artifacts that may still be
    /// committed, and objects the sweeper does not understand.
    pub artifacts_kept: usize,
}

/// Why a sweep deleted nothing, or stopped part of the way.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum SweepError {
    /// The materializer's checkpoint could not be read from the metadata store.
    #[error(transparent)]
    Metadata(#[from] MetaError),
    /// The committed manifest could not be read, or the artifact store failed.
    #[error(transparent)]
    Artifacts(#[from] MaterializerError),
}

/// Deletes every artifact of a materializer that no future checkpoint can name.
///
/// It reads the checkpoint record and the committed manifest, lists the namespace, and deletes the
/// artifacts of replaced runners that the manifest does not name. It is safe to run at any time, from any
/// process, next to a live runner, and to run twice.
///
/// # Arguments
///
/// * `metadata` - The metadata store, to read the materializer's checkpoint.
/// * `artifact_store` - The artifact store to sweep.
/// * `stream_id` - The stream the materializer follows.
/// * `materializer` - The materializer's name, as registered on the stream.
///
/// # Returns
///
/// How many artifacts were deleted and how many were kept.
///
/// # Errors
///
/// Returns [`SweepError::Metadata`] if the materializer is not registered or the metadata store cannot be
/// reached, and [`SweepError::Artifacts`] if the committed manifest cannot be read or the artifact store
/// failed. Artifacts deleted before a failure stay deleted; they were garbage.
///
/// # Examples
///
/// ```no_run
/// # use std::sync::Arc;
/// # use bitacora_artifact::MemoryArtifactStore;
/// # use bitacora_materializer::sweep_orphans;
/// # use bitacora_meta::MetadataStore;
/// # use bitacora_types::StreamId;
/// # async fn example(metadata: MetadataStore) -> Result<(), bitacora_materializer::SweepError> {
/// let artifact_store = Arc::new(MemoryArtifactStore::new());
/// let report = sweep_orphans(&metadata, artifact_store, StreamId::new(7), "bitacora-kv").await?;
/// println!("deleted {} orphan artifacts", report.artifacts_deleted);
/// # Ok(())
/// # }
/// ```
pub async fn sweep_orphans(
    metadata: &MetadataStore,
    artifact_store: Arc<dyn ArtifactStore>,
    stream_id: StreamId,
    materializer: &str,
) -> Result<SweepReport, SweepError> {
    let checkpoint = metadata.get_checkpoint(stream_id, materializer).await?;
    let namespace = ArtifactNamespace::read_only(artifact_store, stream_id, materializer);
    let referenced = referenced_artifacts(&namespace, checkpoint.manifest.as_deref()).await?;
    let report = delete_unreferenced(&namespace, &referenced, checkpoint.fencing_token).await?;
    Ok(report)
}

/// The artifacts a committed checkpoint needs: its manifest and everything the manifest names.
///
/// # Arguments
///
/// * `namespace` - The materializer's artifacts.
/// * `manifest_key` - The pointer in the checkpoint record, or `None` if no checkpoint was ever committed
///   with a manifest.
///
/// # Errors
///
/// Returns [`MaterializerError`] if the manifest cannot be read or decoded.
pub(crate) async fn referenced_artifacts(
    namespace: &ArtifactNamespace,
    manifest_key: Option<&str>,
) -> Result<BTreeSet<String>, MaterializerError> {
    let mut referenced = BTreeSet::new();
    let Some(manifest_key) = manifest_key else {
        return Ok(referenced);
    };
    let stored_manifest = namespace.get_artifact(manifest_key).await?;
    let (_checkpoint, manifest) = decode_manifest(&stored_manifest)?;
    referenced.insert(manifest_key.to_owned());
    referenced.extend(manifest.artifact_keys);
    Ok(referenced)
}

/// Deletes the artifacts of a namespace that are not in `referenced` and were written with a fencing
/// token lower than `tokens_below`.
///
/// # Arguments
///
/// * `namespace` - The materializer's artifacts.
/// * `referenced` - The artifact keys that must stay: the committed manifest and what it names.
/// * `tokens_below` - Artifacts written with this token or a higher one are kept whether referenced or
///   not. An outside sweep passes the current token; the runner that holds token C and has nothing in
///   flight passes C + 1.
///
/// # Errors
///
/// Returns [`MaterializerError::ArtifactStore`] if the namespace cannot be listed or a deletion failed.
pub(crate) async fn delete_unreferenced(
    namespace: &ArtifactNamespace,
    referenced: &BTreeSet<String>,
    tokens_below: u64,
) -> Result<SweepReport, MaterializerError> {
    let mut report = SweepReport::default();
    let objects = namespace.store().list_prefix(namespace.prefix()).await?;
    for object in objects {
        let Some(artifact_key) = object.object_key.strip_prefix(namespace.prefix()) else {
            continue;
        };
        let was_written_by_a_replaced_runner =
            token_of_artifact(artifact_key).is_some_and(|token| token < tokens_below);
        if referenced.contains(artifact_key) || !was_written_by_a_replaced_runner {
            report.artifacts_kept += 1;
            continue;
        }
        namespace.store().delete_object(&object.object_key).await?;
        report.artifacts_deleted += 1;
    }
    Ok(report)
}

#[cfg(test)]
#[path = "sweeper_test.rs"]
mod tests;
