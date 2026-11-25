//! A materializer's own corner of the artifact store.
//!
//! # Why this file exists
//!
//! Checkpoints are only safe if artifacts are immutable and uniquely named: an instance that has been
//! replaced must not be able to overwrite what its successor wrote, and a crash between writing a
//! checkpoint and committing it must leave nothing but files nobody refers to (design, section 11, "What
//! the runner does"). Handing a materializer the whole artifact store would make that a convention. This
//! file makes it a property of the interface: through an [`ArtifactNamespace`] a materializer can add an
//! artifact under a fresh name and read artifacts back. It cannot overwrite, delete or list.
//!
//! # What it contains
//!
//! - [`ArtifactNamespace`]: the view. Writable for the runner that holds the fencing token, read-only for
//!   anyone else.
//! - [`namespace_prefix`] and [`token_of_artifact`]: how artifact keys map to object keys, for the sweeper.
//!
//! # How it works
//!
//! All objects of one materializer of one stream live under `materializers/<stream id>/<name>/`. Within
//! that prefix an artifact is named
//!
//! ```text
//! <fencing token, 20 digits>/<number, 10 digits>-<label>
//! ```
//!
//! The fencing token is raised in the metadata store every time a runner takes over, so no two runners
//! ever share one. The number counts the artifacts one runner has written. Together they make every name
//! unique without a clock or a random source. The label is for people reading a listing.
//!
//! The token in the name is also what tells the sweeper who wrote an artifact, and therefore whether its
//! writer can still be about to commit it (see `sweeper.rs`).
//!
//! # Invariants
//!
//! - An artifact key is never written twice: a view hands out each number once, and no two views that can
//!   write share a token.
//! - Artifact keys are relative to the namespace. A manifest that names them stays valid wherever the
//!   namespace lives.
//!
//! # See also
//!
//! - `contract.rs`: where a materializer receives the view.
//! - `sweeper.rs`: the only code that deletes from a namespace.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bitacora_artifact::ArtifactStore;
use bitacora_types::StreamId;
use bytes::Bytes;

use crate::contract::MaterializerError;

/// The longest label an artifact may carry.
const MAX_LABEL_BYTES: usize = 64;

/// The object key prefix under which every artifact of one materializer of one stream lives.
///
/// # Arguments
///
/// * `stream_id` - The stream the materializer follows. Stream identifiers are never reused, so neither
///   is a prefix.
/// * `materializer` - The materializer's name, as registered on the stream.
///
/// # Examples
///
/// ```
/// use bitacora_materializer::namespace_prefix;
/// use bitacora_types::StreamId;
///
/// assert_eq!(namespace_prefix(StreamId::new(7), "bitacora-kv"), "materializers/7/bitacora-kv/");
/// ```
#[must_use]
pub fn namespace_prefix(stream_id: StreamId, materializer: &str) -> String {
    format!("materializers/{}/{materializer}/", stream_id.get())
}

/// The fencing token of the runner that wrote an artifact, read from the artifact's key.
///
/// # Returns
///
/// `None` if the key was not produced by [`ArtifactNamespace::put_artifact`]. Callers leave such objects
/// alone.
///
/// # Examples
///
/// ```
/// use bitacora_materializer::token_of_artifact;
///
/// assert_eq!(token_of_artifact("00000000000000000042/0000000003-delta"), Some(42));
/// assert_eq!(token_of_artifact("notes.txt"), None);
/// ```
#[must_use]
pub fn token_of_artifact(artifact_key: &str) -> Option<u64> {
    let (token_directory, _artifact_name) = artifact_key.split_once('/')?;
    token_directory.parse().ok()
}

/// What a view needs in order to add artifacts.
#[derive(Debug)]
struct ArtifactWriter {
    /// The fencing token of the runner this view belongs to. It is the first part of every key written.
    fencing_token: u64,
    /// The number the next artifact gets. Shared by the clones of a view, so that they never hand out the
    /// same number.
    next_artifact_number: AtomicU64,
}

/// A view of the artifact store that holds the artifacts of one materializer of one stream.
///
/// The runner creates one for the materializer it runs and passes it to
/// [`Materializer::write_checkpoint`](crate::Materializer::write_checkpoint) and
/// [`Materializer::restore_from_checkpoint`](crate::Materializer::restore_from_checkpoint). Clones share
/// the same store and the same numbering, so a materializer may keep one to serve reads.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use bitacora_artifact::MemoryArtifactStore;
/// use bitacora_materializer::ArtifactNamespace;
/// use bitacora_types::StreamId;
/// use bytes::Bytes;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_materializer::MaterializerError> {
/// let store = Arc::new(MemoryArtifactStore::new());
/// let artifacts = ArtifactNamespace::for_runner(store, StreamId::new(7), "word-count", 3);
///
/// let artifact_key = artifacts.put_artifact("snapshot", Bytes::from_static(b"state")).await?;
/// assert_eq!(artifact_key, "00000000000000000003/0000000000-snapshot");
/// assert_eq!(artifacts.get_artifact(&artifact_key).await?, Bytes::from_static(b"state"));
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ArtifactNamespace {
    /// The store the artifacts live in.
    store: Arc<dyn ArtifactStore>,
    /// The object key prefix of the namespace, ending in `/`.
    prefix: String,
    /// Present if this view may add artifacts.
    writer: Option<Arc<ArtifactWriter>>,
}

impl ArtifactNamespace {
    /// Opens the namespace for the runner that holds `fencing_token`. The view can add artifacts.
    ///
    /// # Arguments
    ///
    /// * `store` - The artifact store.
    /// * `stream_id` - The stream the materializer follows.
    /// * `materializer` - The materializer's name, as registered on the stream.
    /// * `fencing_token` - The token the runner was given when it took the materializer over. The caller
    ///   guarantees that nobody else writes with the same token.
    #[must_use]
    pub fn for_runner(
        store: Arc<dyn ArtifactStore>,
        stream_id: StreamId,
        materializer: &str,
        fencing_token: u64,
    ) -> Self {
        Self {
            store,
            prefix: namespace_prefix(stream_id, materializer),
            writer: Some(Arc::new(ArtifactWriter {
                fencing_token,
                next_artifact_number: AtomicU64::new(0),
            })),
        }
    }

    /// Opens the namespace for reading only: for a follower that shares the artifacts of the active
    /// instance and never checkpoints.
    ///
    /// # Arguments
    ///
    /// * `store` - The artifact store.
    /// * `stream_id` - The stream the materializer follows.
    /// * `materializer` - The materializer's name, as registered on the stream.
    #[must_use]
    pub fn read_only(store: Arc<dyn ArtifactStore>, stream_id: StreamId, materializer: &str) -> Self {
        Self {
            store,
            prefix: namespace_prefix(stream_id, materializer),
            writer: None,
        }
    }

    /// Stores a new artifact, durably, under a name no artifact has had before.
    ///
    /// # Arguments
    ///
    /// * `label` - What the artifact is, for people reading a listing: 1 to 64 ASCII letters, digits,
    ///   `.`, `_` or `-`. It need not be unique.
    /// * `contents` - The whole artifact.
    ///
    /// # Returns
    ///
    /// The artifact's key within the namespace. Put it in the checkpoint manifest and pass it to
    /// [`ArtifactNamespace::get_artifact`] to read the artifact back.
    ///
    /// # Errors
    ///
    /// Returns [`MaterializerError::ReadOnly`] on a read-only view, [`MaterializerError::InvalidLabel`]
    /// for a label that cannot be part of a key, or [`MaterializerError::ArtifactStore`] if the store
    /// failed. After a failure the artifact may or may not exist; it is an orphan either way, because its
    /// key was never returned.
    pub async fn put_artifact(&self, label: &str, contents: Bytes) -> Result<String, MaterializerError> {
        let Some(writer) = &self.writer else {
            return Err(MaterializerError::ReadOnly);
        };
        if !is_valid_label(label) {
            return Err(MaterializerError::InvalidLabel {
                label: label.to_owned(),
            });
        }
        let artifact_number = writer.next_artifact_number.fetch_add(1, Ordering::Relaxed);
        let artifact_key = format!("{:020}/{artifact_number:010}-{label}", writer.fencing_token);
        self.store.put_object(&self.object_key(&artifact_key), contents).await?;
        Ok(artifact_key)
    }

    /// Reads a whole artifact.
    ///
    /// # Arguments
    ///
    /// * `artifact_key` - A key returned by [`ArtifactNamespace::put_artifact`], by this view or by an
    ///   earlier instance of the same materializer.
    ///
    /// # Errors
    ///
    /// Returns [`MaterializerError::ArtifactStore`] if the artifact does not exist or the store failed.
    pub async fn get_artifact(&self, artifact_key: &str) -> Result<Bytes, MaterializerError> {
        let contents = self.store.get_object(&self.object_key(artifact_key)).await?;
        Ok(contents)
    }

    /// Reads part of an artifact, without fetching the rest.
    ///
    /// # Arguments
    ///
    /// * `artifact_key` - A key returned by [`ArtifactNamespace::put_artifact`].
    /// * `byte_range` - The bytes to read. The range must lie inside the artifact.
    ///
    /// # Errors
    ///
    /// Returns [`MaterializerError::ArtifactStore`] if the artifact does not exist, the range lies outside
    /// it, or the store failed.
    pub async fn get_artifact_range(
        &self,
        artifact_key: &str,
        byte_range: Range<u64>,
    ) -> Result<Bytes, MaterializerError> {
        let contents = self.store.get_range(&self.object_key(artifact_key), byte_range).await?;
        Ok(contents)
    }

    /// The fencing token this view writes with, or `None` for a read-only view.
    #[must_use]
    pub fn fencing_token(&self) -> Option<u64> {
        self.writer.as_ref().map(|writer| writer.fencing_token)
    }

    /// The object key prefix of the namespace, ending in `/`. The sweeper lists it.
    pub(crate) fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The store the namespace lives in, for the sweeper, which is the only code that lists and deletes.
    pub(crate) fn store(&self) -> &dyn ArtifactStore {
        self.store.as_ref()
    }

    /// The object key of an artifact of this namespace.
    fn object_key(&self, artifact_key: &str) -> String {
        format!("{}{artifact_key}", self.prefix)
    }
}

impl std::fmt::Debug for ArtifactNamespace {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ArtifactNamespace")
            .field("prefix", &self.prefix)
            .field("fencing_token", &self.fencing_token())
            .finish_non_exhaustive()
    }
}

/// Whether `label` can be the readable part of an artifact key.
fn is_valid_label(label: &str) -> bool {
    if label.is_empty() || label.len() > MAX_LABEL_BYTES {
        return false;
    }
    label
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[cfg(test)]
#[path = "namespace_test.rs"]
mod tests;
