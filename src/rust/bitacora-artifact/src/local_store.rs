//! The artifact store that keeps objects in a directory of the local file system.
//!
//! # Why this file exists
//!
//! A developer running a whole cluster on one machine, and the test that starts real processes, need an
//! object store that survives a restart and that several processes can share, without running an object
//! storage service next to the cluster. A directory does that. It has none of the durability of a real
//! object store, since one disk holds everything, so it is for development and tests, not production.
//!
//! # What it contains
//!
//! - [`LocalArtifactStore`]: the store.
//! - `StoreDirectories`: where the store keeps things, and the blocking file operations on them.
//! - `RangeReadError`: why a range could not be read from a file.
//! - `run_blocking` and the error mappers: the plumbing between async callers, blocking system calls and
//!   [`ArtifactError`].
//!
//! # How it works
//!
//! ```text
//! <root directory>/
//!   objects/<object key>     one file per object; the key's `/` become directories
//!   scratch/put-<number>     an object being written, not yet visible
//! ```
//!
//! An object is written to a scratch file, flushed, and only then renamed to its place below `objects/`,
//! and the directory that received the name is flushed too. A rename within one file system is atomic, so
//! a reader finds either the whole old object, the whole new object, or nothing; and an object that has a
//! name has all of its contents on disk. The scratch directory is outside `objects/`, so a half-written
//! object can never be read or listed, and no object key can collide with a scratch file. Whatever a
//! crashed write left in `scratch/` is removed when the store is opened.
//!
//! A listing walks only the directory that the prefix names, not the whole store: the objects whose key
//! starts with `streams/7/seg` can only be below `objects/streams/7/`.
//!
//! This is the only file of the crate that touches real files, which is why it alone opts out of the
//! workspace's I/O lints. System calls block, so each operation runs on Tokio's blocking pool and the
//! async threads never wait on a disk.
//!
//! # Invariants
//!
//! - A file below `objects/` is always a complete object.
//! - Keys are validated before they become paths, so no operation leaves the root directory.
//! - A root directory belongs to one store at a time: opening it clears `scratch/`, which would pull the
//!   rug from under another process's writes in progress. Several *handles* (clones) of one opened store
//!   are fine.
//!
//! # See also
//!
//! - `store.rs`: the interface this implements, and the rule for keys.
//! - `memory_store.rs`: the backend for tests that need no files.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::fs::{self, File};
use std::io::{self, Read as _, Seek as _, SeekFrom, Write as _};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;

use crate::store::{ArtifactError, ArtifactStore, KEY_SEPARATOR, ObjectInfo, validate_key};

/// The directory, below the store's root, that holds one file per object.
const OBJECTS_DIRECTORY_NAME: &str = "objects";

/// The directory, below the store's root, where an object is built before it is renamed into place. It is
/// outside [`OBJECTS_DIRECTORY_NAME`], so a half-written object can never be read, listed or collide with
/// an object key.
const SCRATCH_DIRECTORY_NAME: &str = "scratch";

/// What the name of every scratch file starts with. A number follows, unique within this process.
const SCRATCH_FILE_PREFIX: &str = "put-";

/// An artifact store in a directory of the local file system.
///
/// Use it for a development cluster on one machine, or in a test that needs objects to survive a restart.
/// Every process that should see the same objects is given the same directory. Open it once per process
/// with [`LocalArtifactStore::open`] and clone the handle freely: clones share the directory.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use bitacora_artifact::{ArtifactStore, LocalArtifactStore};
/// use bytes::Bytes;
///
/// let root_directory = tempfile::tempdir()?;
/// let artifact_store = LocalArtifactStore::open(root_directory.path()).await?;
/// artifact_store
///     .put_object("streams/7/segments/3", Bytes::from_static(b"segment"))
///     .await?;
///
/// let reopened = LocalArtifactStore::open(root_directory.path()).await?;
/// let stored = reopened.get_object("streams/7/segments/3").await?;
///
/// assert_eq!(stored, Bytes::from_static(b"segment"));
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct LocalArtifactStore {
    /// Where the store keeps things. Shared, so that an operation can take it along to the blocking pool
    /// and so that clones of the store write distinct scratch files.
    directories: Arc<StoreDirectories>,
}

impl LocalArtifactStore {
    /// Opens a directory as an artifact store.
    ///
    /// Creates the directory and its `objects/` and `scratch/` subdirectories if they do not exist, and
    /// removes whatever interrupted writes left in `scratch/`. Objects already below `objects/` are kept.
    /// Does blocking file system work on Tokio's blocking pool.
    ///
    /// A root directory belongs to one store at a time: do not open a directory that another running
    /// process has open.
    ///
    /// # Arguments
    ///
    /// * `root_directory` - The directory to keep the objects in. It is created if needed.
    ///
    /// # Returns
    ///
    /// A handle on the store. Clone it to share it.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactError::Unavailable`] if the directories cannot be created, cleared or resolved,
    /// for example because the path lies below a regular file or permissions forbid it.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use bitacora_artifact::{ArtifactStore, LocalArtifactStore};
    ///
    /// let root_directory = tempfile::tempdir()?;
    /// let artifact_store = LocalArtifactStore::open(root_directory.path()).await?;
    ///
    /// assert!(artifact_store.list_prefix("").await?.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn open(root_directory: impl Into<PathBuf>) -> Result<Self, ArtifactError> {
        let root_directory = root_directory.into();
        let prepared = run_blocking(move || StoreDirectories::prepare(&root_directory)).await;
        match prepared {
            Ok(directories) => Ok(Self {
                directories: Arc::new(directories),
            }),
            Err(io_error) => Err(unavailable(&io_error)),
        }
    }
}

/// The two directories of a local store and the blocking file operations on them.
///
/// The methods are plain blocking functions that return `io::Result`. The [`ArtifactStore`] implementation
/// below runs them on the blocking pool and turns their errors into [`ArtifactError`].
#[derive(Debug)]
struct StoreDirectories {
    /// The resolved path of the directory that holds one file per object.
    objects_directory: PathBuf,
    /// The resolved path of the directory where objects are built before they are renamed into place.
    scratch_directory: PathBuf,
    /// The number the next scratch file gets. Counting makes scratch names unique within this process,
    /// which is all that is needed because a root directory belongs to one process at a time.
    next_scratch_number: AtomicU64,
}

impl StoreDirectories {
    /// Creates the store's directories below `root_directory` and empties the scratch directory.
    ///
    /// The paths are resolved (symbolic links followed, made absolute) once, here, so that a path computed
    /// later from an object key can be compared with the objects directory by prefix.
    ///
    /// # Errors
    ///
    /// Fails with the file system's error if a directory cannot be created, cleared or resolved.
    fn prepare(root_directory: &Path) -> io::Result<Self> {
        let objects_directory = root_directory.join(OBJECTS_DIRECTORY_NAME);
        let scratch_directory = root_directory.join(SCRATCH_DIRECTORY_NAME);
        fs::create_dir_all(&objects_directory)?;
        // What is in the scratch directory was never renamed into place: an interrupted write.
        if scratch_directory.exists() {
            fs::remove_dir_all(&scratch_directory)?;
        }
        fs::create_dir_all(&scratch_directory)?;
        Ok(Self {
            objects_directory: objects_directory.canonicalize()?,
            scratch_directory: scratch_directory.canonicalize()?,
            next_scratch_number: AtomicU64::new(0),
        })
    }

    /// The path of the file that holds the object stored under `object_key`.
    ///
    /// The key must have passed [`validate_key`]: that is what guarantees the path stays below the
    /// objects directory.
    fn object_path(&self, object_key: &str) -> PathBuf {
        self.objects_directory.join(object_key)
    }

    /// A path in the scratch directory that no other write of this process uses.
    fn next_scratch_path(&self) -> PathBuf {
        let scratch_number = self.next_scratch_number.fetch_add(1, Ordering::Relaxed);
        self.scratch_directory
            .join(format!("{SCRATCH_FILE_PREFIX}{scratch_number}"))
    }

    /// Stores an object: builds it in the scratch directory, flushes it, renames it into place and flushes
    /// the directory that received the name.
    ///
    /// # Errors
    ///
    /// Fails with the file system's error. A common one: the key names a path below an existing object,
    /// or the path of an existing directory, which a file system cannot represent.
    fn store_object(&self, object_key: &str, contents: &[u8]) -> io::Result<()> {
        let object_path = self.object_path(object_key);
        let parent_directory = object_path
            .parent()
            .expect("an object path is below the objects directory");
        fs::create_dir_all(parent_directory)?;

        let scratch_path = self.next_scratch_path();
        let mut scratch_file = File::create(&scratch_path)?;
        scratch_file.write_all(contents)?;
        // The contents must be on disk before the name is: a reader that finds the name must find all of
        // the object.
        scratch_file.sync_all()?;
        fs::rename(&scratch_path, &object_path)?;
        // The new name is an entry of the parent directory, which has to be flushed like any other data.
        File::open(parent_directory)?.sync_all()
    }

    /// Reads the whole file of an object.
    ///
    /// # Errors
    ///
    /// Fails with the file system's error; its kind is `NotFound` if no object has this key.
    fn read_object(&self, object_key: &str) -> io::Result<Bytes> {
        let contents = fs::read(self.object_path(object_key))?;
        Ok(Bytes::from(contents))
    }

    /// Reads the bytes of an object's file that lie in `byte_range`, after checking the range against the
    /// file's size.
    ///
    /// # Errors
    ///
    /// * [`RangeReadError::OutOfBounds`], with the file's size, if the range ends past the file or before
    ///   it starts.
    /// * [`RangeReadError::Io`] with the file system's error otherwise; its kind is `NotFound` if no
    ///   object has this key.
    fn read_object_range(
        &self,
        object_key: &str,
        byte_range: &Range<u64>,
    ) -> Result<Bytes, RangeReadError> {
        let mut object_file = File::open(self.object_path(object_key))?;
        let size_bytes = object_file.metadata()?.len();
        let is_inside_the_object =
            byte_range.start <= byte_range.end && byte_range.end <= size_bytes;
        if !is_inside_the_object {
            return Err(RangeReadError::OutOfBounds { size_bytes });
        }
        let Ok(range_bytes) = usize::try_from(byte_range.end - byte_range.start) else {
            return Err(RangeReadError::Io(io::Error::other(
                "range does not fit in memory",
            )));
        };
        let mut range_contents = vec![0; range_bytes];
        object_file.seek(SeekFrom::Start(byte_range.start))?;
        object_file.read_exact(&mut range_contents)?;
        Ok(Bytes::from(range_contents))
    }

    /// Removes the file of an object. Succeeds if there is none.
    ///
    /// # Errors
    ///
    /// Fails with the file system's error for anything other than "no such file".
    fn remove_object(&self, object_key: &str) -> io::Result<()> {
        match fs::remove_file(self.object_path(object_key)) {
            Ok(()) => Ok(()),
            Err(io_error) if io_error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(io_error) => Err(io_error),
        }
    }

    /// Lists the objects whose key starts with `key_prefix`, sorted by key.
    ///
    /// Walks the directory that the prefix names and everything below it, and keeps the files whose key
    /// starts with the prefix. The walk uses an explicit list of directories still to visit, so a deep
    /// tree cannot exhaust the stack.
    ///
    /// # Errors
    ///
    /// Fails with the file system's error if a directory or a file's metadata cannot be read.
    fn list_objects(&self, key_prefix: &str) -> io::Result<Vec<ObjectInfo>> {
        let Some(first_directory) = self.directory_named_by(key_prefix) else {
            return Ok(Vec::new());
        };
        let mut listed = Vec::new();
        let mut directories_to_walk = vec![first_directory];
        while let Some(directory) = directories_to_walk.pop() {
            self.list_directory(
                &directory,
                key_prefix,
                &mut listed,
                &mut directories_to_walk,
            )?;
        }
        listed.sort_by(|left, right| left.object_key.cmp(&right.object_key));
        Ok(listed)
    }

    /// The directory that holds every object whose key starts with `key_prefix`: the path the prefix
    /// spells up to its last separator.
    ///
    /// # Returns
    ///
    /// The directory to start a listing from, or `None` if no valid key can start with the prefix, for
    /// example because the prefix climbs out of the store with `..`. The directory need not exist.
    fn directory_named_by(&self, key_prefix: &str) -> Option<PathBuf> {
        let directory_key = match key_prefix.rfind(KEY_SEPARATOR) {
            Some(last_separator) => &key_prefix[..last_separator],
            None => "",
        };
        if directory_key.is_empty() {
            return Some(self.objects_directory.clone());
        }
        if validate_key(directory_key).is_err() {
            return None;
        }
        Some(self.objects_directory.join(directory_key))
    }

    /// Looks at the entries of one directory: files whose key starts with `key_prefix` are added to
    /// `listed`, and subdirectories are added to `directories_to_walk`.
    ///
    /// A directory that does not exist has no entries; that is not an error, because the prefix of a
    /// listing may name a path nobody stored anything below.
    ///
    /// # Errors
    ///
    /// Fails with the file system's error if the directory or an entry's metadata cannot be read, or if a
    /// file's path is not a valid key.
    fn list_directory(
        &self,
        directory: &Path,
        key_prefix: &str,
        listed: &mut Vec<ObjectInfo>,
        directories_to_walk: &mut Vec<PathBuf>,
    ) -> io::Result<()> {
        let entries = match fs::read_dir(directory) {
            Ok(entries) => entries,
            Err(io_error) if io_error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(io_error) => return Err(io_error),
        };
        for entry in entries {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                directories_to_walk.push(entry.path());
                continue;
            }
            let object_key = self.object_key_of(&entry.path())?;
            if object_key.starts_with(key_prefix) {
                listed.push(ObjectInfo {
                    object_key,
                    size_bytes: metadata.len(),
                });
            }
        }
        Ok(())
    }

    /// The key of the object whose file is at `object_path`: the path below the objects directory, with
    /// `/` between its components whatever the platform's separator is.
    ///
    /// # Errors
    ///
    /// Fails if the path is not below the objects directory or has a component that is not valid UTF-8.
    /// Neither can come from an object this store wrote.
    fn object_key_of(&self, object_path: &Path) -> io::Result<String> {
        let Ok(relative_path) = object_path.strip_prefix(&self.objects_directory) else {
            return Err(not_an_object_key(object_path));
        };
        let mut object_key = String::new();
        for path_component in relative_path.components() {
            let Some(key_component) = path_component.as_os_str().to_str() else {
                return Err(not_an_object_key(object_path));
            };
            if !object_key.is_empty() {
                object_key.push(KEY_SEPARATOR);
            }
            object_key.push_str(key_component);
        }
        Ok(object_key)
    }
}

/// The error for a file below the objects directory whose path cannot be turned into an object key.
fn not_an_object_key(object_path: &Path) -> io::Error {
    io::Error::other(format!(
        "{} is not a valid object key",
        object_path.display()
    ))
}

/// Why a range could not be read from an object's file.
#[derive(Debug)]
enum RangeReadError {
    /// The range ends past the end of the file, or before it starts.
    OutOfBounds {
        /// How large the file really is, in bytes.
        size_bytes: u64,
    },
    /// The file system failed. The kind is `NotFound` if there is no such object.
    Io(io::Error),
}

impl From<io::Error> for RangeReadError {
    /// Wraps a file system error, so that `?` can be used on file operations while reading a range.
    fn from(io_error: io::Error) -> Self {
        Self::Io(io_error)
    }
}

/// Runs a blocking file system operation on Tokio's blocking pool, so that the async threads never wait
/// on a disk.
///
/// # Arguments
///
/// * `operation` - The blocking work. It is moved to another thread, so it owns everything it uses.
///
/// # Returns
///
/// What the operation returned.
///
/// # Errors
///
/// Returns the operation's own error, or an I/O error converted to the operation's error type if the
/// blocking task could not be run to its end, which happens when the runtime shuts down or the operation
/// panics.
async fn run_blocking<Output, Failure>(
    operation: impl FnOnce() -> Result<Output, Failure> + Send + 'static,
) -> Result<Output, Failure>
where
    Output: Send + 'static,
    Failure: From<io::Error> + Send + 'static,
{
    match tokio::task::spawn_blocking(operation).await {
        Ok(operation_result) => operation_result,
        Err(join_error) => Err(Failure::from(io::Error::other(join_error))),
    }
}

/// The error for a file system failure that says nothing more specific: the store could not do it.
fn unavailable(io_error: &io::Error) -> ArtifactError {
    ArtifactError::Unavailable {
        reason: io_error.to_string(),
    }
}

/// The error for a file system failure while reading `object_key`: "no such file" means the object does
/// not exist, anything else that the store could not do it.
fn not_found_or_unavailable(object_key: &str, io_error: &io::Error) -> ArtifactError {
    if io_error.kind() == io::ErrorKind::NotFound {
        ArtifactError::NotFound {
            object_key: object_key.to_owned(),
        }
    } else {
        unavailable(io_error)
    }
}

/// The error for a failed range read of `object_key`, with the range that was asked for.
fn range_read_failed(
    object_key: &str,
    byte_range: &Range<u64>,
    range_read_error: &RangeReadError,
) -> ArtifactError {
    match range_read_error {
        RangeReadError::OutOfBounds { size_bytes } => ArtifactError::RangeOutOfBounds {
            object_key: object_key.to_owned(),
            start: byte_range.start,
            end: byte_range.end,
            size_bytes: *size_bytes,
        },
        RangeReadError::Io(io_error) => not_found_or_unavailable(object_key, io_error),
    }
}

#[async_trait]
impl ArtifactStore for LocalArtifactStore {
    /// Writes the object to a scratch file and renames it into place. Returns once the object and its
    /// name are flushed.
    async fn put_object(&self, object_key: &str, contents: Bytes) -> Result<(), ArtifactError> {
        validate_key(object_key)?;
        let directories = Arc::clone(&self.directories);
        let owned_key = object_key.to_owned();
        let stored = run_blocking(move || directories.store_object(&owned_key, &contents)).await;
        stored.map_err(|io_error| unavailable(&io_error))
    }

    /// Reads the object's whole file.
    async fn get_object(&self, object_key: &str) -> Result<Bytes, ArtifactError> {
        validate_key(object_key)?;
        let directories = Arc::clone(&self.directories);
        let owned_key = object_key.to_owned();
        let read = run_blocking(move || directories.read_object(&owned_key)).await;
        read.map_err(|io_error| not_found_or_unavailable(object_key, &io_error))
    }

    /// Seeks into the object's file and reads only the range, so a large object is never read whole.
    async fn get_range(
        &self,
        object_key: &str,
        byte_range: Range<u64>,
    ) -> Result<Bytes, ArtifactError> {
        validate_key(object_key)?;
        let directories = Arc::clone(&self.directories);
        let owned_key = object_key.to_owned();
        let requested_range = byte_range.clone();
        let read =
            run_blocking(move || directories.read_object_range(&owned_key, &requested_range)).await;
        read.map_err(|range_read_error| {
            range_read_failed(object_key, &byte_range, &range_read_error)
        })
    }

    /// Removes the object's file. Directories that become empty are left in place.
    async fn delete_object(&self, object_key: &str) -> Result<(), ArtifactError> {
        validate_key(object_key)?;
        let directories = Arc::clone(&self.directories);
        let owned_key = object_key.to_owned();
        let removed = run_blocking(move || directories.remove_object(&owned_key)).await;
        removed.map_err(|io_error| unavailable(&io_error))
    }

    /// Walks the directory the prefix names. The cost grows with what is below that directory, not with
    /// the size of the store.
    async fn list_prefix(&self, key_prefix: &str) -> Result<Vec<ObjectInfo>, ArtifactError> {
        let directories = Arc::clone(&self.directories);
        let owned_prefix = key_prefix.to_owned();
        let listed = run_blocking(move || directories.list_objects(&owned_prefix)).await;
        listed.map_err(|io_error| unavailable(&io_error))
    }
}

#[cfg(test)]
#[path = "local_store_test.rs"]
mod tests;
