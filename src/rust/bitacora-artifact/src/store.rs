//! The artifact store interface: immutable objects addressed by key.
//!
//! # Why this file exists
//!
//! Two parts of bitacora keep data in an object store. The log uploads sealed segments there, where they
//! become the cold read path and the backup (design, section 10, "Tiering"). Materializers write their
//! layer files, index segments and checkpoints there (design, section 11, "Artifact store"). Both must run
//! unchanged against memory in the simulator, a directory on a developer's machine and a real object store
//! in production. That needs one small interface that all three can honour exactly, and one definition of
//! what a key is, so that a key valid in a test is valid everywhere.
//!
//! # What it contains
//!
//! - [`ArtifactStore`]: the interface. Six operations on whole, immutable objects.
//! - [`ObjectUpload`]: an object on its way into a store piece by piece, for objects too large to hold.
//! - [`ArtifactError`]: why an operation failed.
//! - [`ObjectInfo`]: what a listing says about one object.
//! - [`validate_key`]: the rule every backend applies to a key before it uses it.
//! - `slice_range`: the range check and cut that backends holding whole objects share.
//!
//! # How it works
//!
//! An object is a run of bytes stored under a key. A key is a `/`-separated relative path such as
//! `streams/7/segments/00000000000000000003`. Objects are written whole and never modified; writing an
//! existing key replaces the object atomically. There are no directories in the interface: a listing takes
//! a key prefix and compares it character by character, as object stores do.
//!
//! The design's interface has four operations. This one adds `get_object`, because reading a whole object
//! must not need a listing first to learn its size, and `start_upload`, because a sealed segment is a
//! gigabyte and whoever stores it must not have to hold it. An upload takes the object in pieces and makes
//! it appear under its key at the end, in one step, so the rule that a reader never sees part of an object
//! holds for it as for a `put_object`. How a store gets there is its own business: the in-memory store
//! keeps the pieces, the directory store appends them to a scratch file and renames it, an object store
//! service gets them as the parts of a multipart upload.
//!
//! Errors are few on purpose. A caller can do something different for exactly four situations: the object
//! is not there, the range is wrong, the key is wrong, or the store could not do it right now. Everything a
//! backend can fail with maps to one of them.
//!
//! # Invariants
//!
//! - A reader sees either the old object or the new one under a key, never a mixture and never a prefix,
//!   however the object was stored.
//! - An upload that is not completed stores nothing under its key.
//! - Every operation is safe to repeat. After [`ArtifactError::Unavailable`] the operation may or may not
//!   have taken effect.
//! - A key that [`validate_key`] accepts cannot name anything outside the store, in any backend.
//! - A listing is sorted by key.
//!
//! # See also
//!
//! - `contract.rs`: the same rules as an executable check that every backend runs against itself.
//! - `memory_store.rs`: the backend for tests and simulation.
//! - `local_store.rs`: the backend that keeps objects in a local directory.

use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

/// The character that separates the components of an object key, on every platform. A backend that maps
/// keys to file system paths translates it; it never appears as anything else in a key.
pub(crate) const KEY_SEPARATOR: char = '/';

/// The key component that means "this directory" in a file system path. A key must not contain it.
const CURRENT_DIRECTORY_COMPONENT: &str = ".";

/// The key component that means "the directory above" in a file system path. A key must not contain it,
/// or a backend that maps keys to paths could be led outside its own directory.
const PARENT_DIRECTORY_COMPONENT: &str = "..";

/// Why an artifact store operation failed.
///
/// The variants are the situations a caller can react to differently. A backend maps whatever its storage
/// reports to one of them.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ArtifactError {
    /// No object has this key. Retrying does not help unless somebody stores the object.
    #[error("object {object_key:?} does not exist")]
    NotFound {
        /// The key that was looked up.
        object_key: String,
    },
    /// The requested bytes lie outside the object, or the range ends before it starts. Retrying does not
    /// help: objects never change size.
    #[error("range {start}..{end} is outside object {object_key:?} of {size_bytes} bytes")]
    RangeOutOfBounds {
        /// The object that was read.
        object_key: String,
        /// The offset of the first byte asked for.
        start: u64,
        /// The offset one past the last byte asked for.
        end: u64,
        /// How large the object really is, in bytes.
        size_bytes: u64,
    },
    /// The key cannot be used: it is empty, absolute, or has a component that would escape the store.
    /// This is a mistake in the caller, not a state of the store.
    #[error("object key {object_key:?} is not valid")]
    InvalidKey {
        /// The rejected key.
        object_key: String,
    },
    /// The store could not be reached or failed. The operation may or may not have taken effect; every
    /// operation is safe to repeat.
    #[error("artifact store is unavailable: {reason}")]
    Unavailable {
        /// What went wrong, in the backend's own words, for the operator who reads the log.
        reason: String,
    },
}

/// What a listing says about one object.
///
/// A listing does not return contents. The size is what a reader needs to plan range requests, for example
/// to find the footer at the end of a tiered segment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectInfo {
    /// The object's full key, not shortened by the prefix that was listed.
    pub object_key: String,
    /// The object's size in bytes.
    pub size_bytes: u64,
}

/// Immutable objects addressed by key: where tiered log segments and materializer artifacts live.
///
/// Keys are `/`-separated relative paths (see [`validate_key`]). An object is written whole and never
/// modified. Writing an existing key replaces the object atomically, so a reader sees either the old
/// object or the new one.
///
/// Implement it once per kind of storage, and run [`crate::contract::check`] against the implementation
/// in its tests. Use it through `&dyn ArtifactStore` or `Arc<dyn ArtifactStore>`.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_artifact::ArtifactError> {
/// use bitacora_artifact::{ArtifactStore, MemoryArtifactStore};
/// use bytes::Bytes;
///
/// let artifact_store = MemoryArtifactStore::new();
/// artifact_store
///     .put_object("checkpoints/pages/41", Bytes::from_static(b"page images"))
///     .await?;
///
/// let first_word = artifact_store.get_range("checkpoints/pages/41", 0..4).await?;
/// assert_eq!(first_word, Bytes::from_static(b"page"));
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait ArtifactStore: Send + Sync {
    /// Stores `contents` under `object_key`, durably, replacing any object already there.
    ///
    /// The object appears under its key only once all of it is stored. Does I/O and returns when the
    /// object is durable.
    ///
    /// # Arguments
    ///
    /// * `object_key` - Where to store the object. Must pass [`validate_key`].
    /// * `contents` - The whole object. May be empty; an empty object is a real object.
    ///
    /// # Errors
    ///
    /// * [`ArtifactError::InvalidKey`] if the key is not a plain relative path.
    /// * [`ArtifactError::Unavailable`] if the store failed. The object may or may not have been stored.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_artifact::ArtifactError> {
    /// # use bitacora_artifact::{ArtifactStore, MemoryArtifactStore};
    /// # use bytes::Bytes;
    /// # let artifact_store = MemoryArtifactStore::new();
    /// artifact_store
    ///     .put_object("streams/7/segments/3", Bytes::from_static(b"first version"))
    ///     .await?;
    /// artifact_store
    ///     .put_object("streams/7/segments/3", Bytes::from_static(b"second"))
    ///     .await?;
    ///
    /// let stored = artifact_store.get_object("streams/7/segments/3").await?;
    /// assert_eq!(stored, Bytes::from_static(b"second"));
    /// # Ok(())
    /// # }
    /// ```
    async fn put_object(&self, object_key: &str, contents: Bytes) -> Result<(), ArtifactError>;

    /// Reads the whole object stored under `object_key`.
    ///
    /// Holds the whole object in memory. For a part of a large object use [`ArtifactStore::get_range`].
    ///
    /// # Arguments
    ///
    /// * `object_key` - The key of the object to read. Must pass [`validate_key`].
    ///
    /// # Returns
    ///
    /// The object's contents, exactly as they were stored.
    ///
    /// # Errors
    ///
    /// * [`ArtifactError::InvalidKey`] if the key is not a plain relative path.
    /// * [`ArtifactError::NotFound`] if no object has this key.
    /// * [`ArtifactError::Unavailable`] if the store failed.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// # use bitacora_artifact::{ArtifactError, ArtifactStore, MemoryArtifactStore};
    /// # let artifact_store = MemoryArtifactStore::new();
    /// let missing = artifact_store.get_object("never/written").await;
    ///
    /// assert!(matches!(missing, Err(ArtifactError::NotFound { .. })));
    /// # }
    /// ```
    async fn get_object(&self, object_key: &str) -> Result<Bytes, ArtifactError>;

    /// Reads the bytes of an object that lie in `byte_range`.
    ///
    /// This is how a large object is read without downloading it: a reader of a tiered segment fetches
    /// the footer, then the index, then only the blocks it needs.
    ///
    /// # Arguments
    ///
    /// * `object_key` - The key of the object to read. Must pass [`validate_key`].
    /// * `byte_range` - The offsets of the bytes wanted: from `start` up to, not including, `end`. It must
    ///   lie inside the object. An empty range at or before the end of the object is valid.
    ///
    /// # Returns
    ///
    /// Exactly `end - start` bytes.
    ///
    /// # Errors
    ///
    /// * [`ArtifactError::InvalidKey`] if the key is not a plain relative path.
    /// * [`ArtifactError::NotFound`] if no object has this key.
    /// * [`ArtifactError::RangeOutOfBounds`] if the range ends past the object or before it starts. The
    ///   error carries the object's real size.
    /// * [`ArtifactError::Unavailable`] if the store failed.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_artifact::ArtifactError> {
    /// # use bitacora_artifact::{ArtifactError, ArtifactStore, MemoryArtifactStore};
    /// # use bytes::Bytes;
    /// # let artifact_store = MemoryArtifactStore::new();
    /// artifact_store
    ///     .put_object("layers/0001", Bytes::from_static(b"0123456789"))
    ///     .await?;
    ///
    /// let middle = artifact_store.get_range("layers/0001", 3..7).await?;
    /// let past_the_end = artifact_store.get_range("layers/0001", 5..11).await;
    ///
    /// assert_eq!(middle, Bytes::from_static(b"3456"));
    /// assert!(matches!(
    ///     past_the_end,
    ///     Err(ArtifactError::RangeOutOfBounds { size_bytes: 10, .. })
    /// ));
    /// # Ok(())
    /// # }
    /// ```
    async fn get_range(&self, object_key: &str, byte_range: Range<u64>) -> Result<Bytes, ArtifactError>;

    /// Deletes the object stored under `object_key`.
    ///
    /// Deleting an object that does not exist succeeds, so that a deletion can be repeated after a failure
    /// without first asking whether it already happened.
    ///
    /// # Arguments
    ///
    /// * `object_key` - The key of the object to delete. Must pass [`validate_key`].
    ///
    /// # Errors
    ///
    /// * [`ArtifactError::InvalidKey`] if the key is not a plain relative path.
    /// * [`ArtifactError::Unavailable`] if the store failed. The object may or may not have been deleted.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_artifact::ArtifactError> {
    /// # use bitacora_artifact::{ArtifactStore, MemoryArtifactStore};
    /// # let artifact_store = MemoryArtifactStore::new();
    /// artifact_store.delete_object("never/written").await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn delete_object(&self, object_key: &str) -> Result<(), ArtifactError>;

    /// Lists every object whose key starts with `key_prefix`.
    ///
    /// The prefix is compared character by character, not component by component: `logs/segment-1` lists
    /// both `logs/segment-1` and `logs/segment-10`. End a prefix with `/` to list what is below a path.
    ///
    /// # Arguments
    ///
    /// * `key_prefix` - The beginning of the keys wanted. The empty prefix lists every object. It need not
    ///   be a valid key itself; a prefix no valid key can start with lists nothing.
    ///
    /// # Returns
    ///
    /// One [`ObjectInfo`] per matching object, sorted by key. Empty if nothing matches.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactError::Unavailable`] if the store failed.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_artifact::ArtifactError> {
    /// # use bitacora_artifact::{ArtifactStore, MemoryArtifactStore};
    /// # use bytes::Bytes;
    /// # let artifact_store = MemoryArtifactStore::new();
    /// artifact_store
    ///     .put_object("streams/7/segments/2", Bytes::from_static(b"22"))
    ///     .await?;
    /// artifact_store
    ///     .put_object("streams/7/segments/1", Bytes::from_static(b"1"))
    ///     .await?;
    /// artifact_store
    ///     .put_object("streams/8/segments/1", Bytes::from_static(b"other stream"))
    ///     .await?;
    ///
    /// let listed = artifact_store.list_prefix("streams/7/").await?;
    ///
    /// assert_eq!(listed.len(), 2);
    /// assert_eq!(listed[0].object_key, "streams/7/segments/1");
    /// assert_eq!(listed[1].size_bytes, 2);
    /// # Ok(())
    /// # }
    /// ```
    async fn list_prefix(&self, key_prefix: &str) -> Result<Vec<ObjectInfo>, ArtifactError>;

    /// Starts storing an object that is handed over piece by piece.
    ///
    /// This is how an object too large to hold is stored: a sealed segment is a gigabyte. The pieces go
    /// to the returned [`ObjectUpload`], and the object appears under `object_key` when the upload is
    /// completed, not before. For an object that is in memory anyway, use [`ArtifactStore::put_object`].
    ///
    /// # Arguments
    ///
    /// * `object_key` - Where the object will be stored. Must pass [`validate_key`].
    ///
    /// # Returns
    ///
    /// The upload. Nothing is stored under the key until it is completed.
    ///
    /// # Errors
    ///
    /// * [`ArtifactError::InvalidKey`] if the key is not a plain relative path.
    /// * [`ArtifactError::Unavailable`] if the store failed.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_artifact::ArtifactError> {
    /// # use bitacora_artifact::{ArtifactError, ArtifactStore, MemoryArtifactStore};
    /// # use bytes::Bytes;
    /// # let artifact_store = MemoryArtifactStore::new();
    /// let mut upload = artifact_store.start_upload("streams/7/segments/3").await?;
    /// upload.write(Bytes::from_static(b"first frames, ")).await?;
    /// upload.write(Bytes::from_static(b"last frames")).await?;
    ///
    /// let before_completion = artifact_store.get_object("streams/7/segments/3").await;
    /// upload.complete().await?;
    /// let after_completion = artifact_store.get_object("streams/7/segments/3").await?;
    ///
    /// assert!(matches!(before_completion, Err(ArtifactError::NotFound { .. })));
    /// assert_eq!(after_completion, Bytes::from_static(b"first frames, last frames"));
    /// # Ok(())
    /// # }
    /// ```
    async fn start_upload(&self, object_key: &str) -> Result<Box<dyn ObjectUpload>, ArtifactError>;
}

/// One object on its way into an artifact store, handed over piece by piece.
///
/// Get one from [`ArtifactStore::start_upload`], hand it the object's bytes in order with
/// [`ObjectUpload::write`], and end with [`ObjectUpload::complete`]. Until then the store holds nothing
/// under the key: a reader finds the object that was there before, or none.
///
/// A store may hold pieces back to send them on in parts of a size its service wants. What an upload keeps
/// in memory is therefore at most one such part and the piece it was just given, whatever the size of the
/// object. The in-memory store is the exception: its objects live in memory.
///
/// An upload that will not be completed should be ended with [`ObjectUpload::abort`], which lets the store
/// remove what it already took and says whether it could. An upload that is dropped stores nothing under
/// its key either. This is what happens to the upload of a caller that is itself given up on, such as a
/// request that is cancelled, so a store removes what a dropped upload took as well, in the background and
/// as far as it can: nobody hears whether it worked, and what is left is for the store's own housekeeping
/// to find.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_artifact::ArtifactError> {
/// use bitacora_artifact::{ArtifactStore, MemoryArtifactStore};
/// use bytes::Bytes;
///
/// let artifact_store = MemoryArtifactStore::new();
/// let mut upload = artifact_store.start_upload("layers/0001").await?;
/// for piece in [&b"one "[..], b"two ", b"three"] {
///     upload.write(Bytes::from_static(piece)).await?;
/// }
/// upload.complete().await?;
///
/// let stored = artifact_store.get_object("layers/0001").await?;
/// assert_eq!(stored, Bytes::from_static(b"one two three"));
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait ObjectUpload: Send {
    /// Takes the next bytes of the object.
    ///
    /// Does I/O when the store has enough to send on, and returns when the piece is safe with the upload:
    /// sent, or kept for the next part. After a failure the upload cannot go on; abort it.
    ///
    /// # Arguments
    ///
    /// * `piece` - The bytes that follow those already handed over. Any size; an empty piece adds nothing.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactError::Unavailable`] if the store failed.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_artifact::ArtifactError> {
    /// # use bitacora_artifact::{ArtifactStore, MemoryArtifactStore};
    /// # use bytes::Bytes;
    /// # let artifact_store = MemoryArtifactStore::new();
    /// let mut upload = artifact_store.start_upload("layers/0001").await?;
    ///
    /// upload.write(Bytes::from_static(b"a piece")).await?;
    /// upload.write(Bytes::new()).await?;
    /// # upload.abort().await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn write(&mut self, piece: Bytes) -> Result<(), ArtifactError>;

    /// Stores the object under its key: everything that was handed over, durably, replacing any object
    /// already there.
    ///
    /// The object appears whole or not at all. An upload that was handed nothing stores an empty object.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactError::Unavailable`] if the store failed. The object may or may not have been
    /// stored, as after a failed [`ArtifactStore::put_object`]; uploading again is safe. The upload is
    /// over either way, so the store removes what it took before it returns the error, as an abort would.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_artifact::ArtifactError> {
    /// # use bitacora_artifact::{ArtifactStore, MemoryArtifactStore};
    /// # use bytes::Bytes;
    /// # let artifact_store = MemoryArtifactStore::new();
    /// let upload = artifact_store.start_upload("layers/empty").await?;
    ///
    /// upload.complete().await?;
    ///
    /// assert_eq!(artifact_store.get_object("layers/empty").await?, Bytes::new());
    /// # Ok(())
    /// # }
    /// ```
    async fn complete(self: Box<Self>) -> Result<(), ArtifactError>;

    /// Gives the upload up and lets the store remove what it took.
    ///
    /// Nothing is stored under the key. Call it for an upload that failed or that turned out to have
    /// nothing right to store.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactError::Unavailable`] if the store could not remove what it took. Nothing is
    /// stored under the key even then; what is left is for the store's housekeeping.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_artifact::ArtifactError> {
    /// # use bitacora_artifact::{ArtifactError, ArtifactStore, MemoryArtifactStore};
    /// # use bytes::Bytes;
    /// # let artifact_store = MemoryArtifactStore::new();
    /// let mut upload = artifact_store.start_upload("layers/0001").await?;
    /// upload.write(Bytes::from_static(b"a copy that turned out damaged")).await?;
    ///
    /// upload.abort().await?;
    ///
    /// let stored = artifact_store.get_object("layers/0001").await;
    /// assert!(matches!(stored, Err(ArtifactError::NotFound { .. })));
    /// # Ok(())
    /// # }
    /// ```
    async fn abort(self: Box<Self>) -> Result<(), ArtifactError>;
}

/// Checks that an object key is a plain relative path, so that no backend ever has to wonder where it
/// points.
///
/// A valid key is one or more components separated by `/`. A component is any non-empty text other than
/// `.` and `..`. That rules out the empty key, a key that starts or ends with `/`, a doubled `/`, and
/// every way of naming something above the store's root. Every backend calls this before it uses a key.
///
/// # Arguments
///
/// * `object_key` - The key to check.
///
/// # Errors
///
/// Returns [`ArtifactError::InvalidKey`], carrying the key, if it is not a plain relative path.
///
/// # Examples
///
/// ```
/// use bitacora_artifact::validate_key;
///
/// assert!(validate_key("streams/7/segments/3").is_ok());
/// assert!(validate_key("streams/../secrets").is_err());
/// assert!(validate_key("/absolute").is_err());
/// ```
pub fn validate_key(object_key: &str) -> Result<(), ArtifactError> {
    // Splitting the empty key yields one empty component, so the empty key is refused here too.
    let is_plain_relative_path = object_key.split(KEY_SEPARATOR).all(is_valid_key_component);
    if is_plain_relative_path {
        Ok(())
    } else {
        Err(ArtifactError::InvalidKey {
            object_key: object_key.to_owned(),
        })
    }
}

/// Whether one `/`-separated component of a key is allowed: non-empty and not a directory reference.
fn is_valid_key_component(component: &str) -> bool {
    !component.is_empty() && component != CURRENT_DIRECTORY_COMPONENT && component != PARENT_DIRECTORY_COMPONENT
}

/// Cuts `byte_range` out of an object's contents, as every backend that holds whole objects does.
///
/// Checks the range against the object first. Cutting does not copy: the result shares the object's
/// memory.
///
/// # Arguments
///
/// * `object_key` - The key of the object, used only to describe an error.
/// * `contents` - The whole object.
/// * `byte_range` - The offsets of the bytes wanted, from `start` up to, not including, `end`.
///
/// # Returns
///
/// The bytes of `contents` in the range. Empty for an empty range inside the object.
///
/// # Errors
///
/// Returns [`ArtifactError::RangeOutOfBounds`], carrying the object's size, if the range ends past the
/// object or before it starts.
///
/// # Panics
///
/// Panics if the object is larger than 2^64 bytes, which no object in memory can be.
pub(crate) fn slice_range(object_key: &str, contents: &Bytes, byte_range: &Range<u64>) -> Result<Bytes, ArtifactError> {
    let size_bytes = u64::try_from(contents.len()).expect("lengths fit in 64 bits");
    let is_inside_the_object = byte_range.start <= byte_range.end && byte_range.end <= size_bytes;
    if !is_inside_the_object {
        return Err(ArtifactError::RangeOutOfBounds {
            object_key: object_key.to_owned(),
            start: byte_range.start,
            end: byte_range.end,
            size_bytes,
        });
    }
    let start = usize::try_from(byte_range.start).expect("checked against the length");
    let end = usize::try_from(byte_range.end).expect("checked against the length");
    Ok(contents.slice(start..end))
}

#[cfg(test)]
#[path = "store_test.rs"]
mod tests;
