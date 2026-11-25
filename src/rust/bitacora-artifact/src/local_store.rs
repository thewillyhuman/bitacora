//! The [`ArtifactStore`] adapter for a directory of the local file system.
//!
//! It is meant for development clusters and tests: a single directory has
//! none of the durability of an object store. This is the only module of
//! the crate that touches real files, which is why it alone opts out of the
//! workspace's I/O lints. Blocking system calls run on Tokio's blocking pool.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::fs::{self, File};
use std::io::{self, Read as _, Seek as _, SeekFrom, Write as _};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;

use crate::store::{ArtifactError, ArtifactStore, ObjectInfo, validate_key};

/// Where objects live, below the store's root directory.
const OBJECTS_DIRECTORY: &str = "objects";

/// Where an object is built before it is renamed into place. It is outside
/// [`OBJECTS_DIRECTORY`], so a half-written object can never be read, listed
/// or collide with an object key.
const SCRATCH_DIRECTORY: &str = "scratch";

/// An artifact store in a directory of the local file system.
#[derive(Clone, Debug)]
pub struct LocalArtifactStore {
    directories: Arc<Directories>,
}

#[derive(Debug)]
struct Directories {
    objects: PathBuf,
    scratch: PathBuf,
    /// Makes scratch file names unique within this process.
    next_scratch_number: AtomicU64,
}

impl LocalArtifactStore {
    /// Opens `root_directory` as an artifact store, creating it if needed,
    /// and removes what interrupted writes left behind.
    ///
    /// A root directory belongs to one process at a time.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactError::Unavailable`] if the directory cannot be
    /// prepared.
    pub async fn open(root_directory: impl Into<PathBuf>) -> Result<Self, ArtifactError> {
        let root_directory = root_directory.into();
        run_blocking(move || {
            let objects = root_directory.join(OBJECTS_DIRECTORY);
            let scratch = root_directory.join(SCRATCH_DIRECTORY);
            fs::create_dir_all(&objects)?;
            if scratch.exists() {
                fs::remove_dir_all(&scratch)?;
            }
            fs::create_dir_all(&scratch)?;
            Ok(Self {
                directories: Arc::new(Directories {
                    objects: objects.canonicalize()?,
                    scratch: scratch.canonicalize()?,
                    next_scratch_number: AtomicU64::new(0),
                }),
            })
        })
        .await
        .map_err(unavailable)
    }
}

impl Directories {
    /// The file that holds the object. Keys are validated first, so the
    /// path cannot leave the objects directory.
    fn object_path(&self, object_key: &str) -> PathBuf {
        self.objects.join(object_key)
    }

    fn new_scratch_path(&self) -> PathBuf {
        let scratch_number = self.next_scratch_number.fetch_add(1, Ordering::Relaxed);
        self.scratch.join(format!("put-{scratch_number}"))
    }

    fn put(&self, object_key: &str, contents: &[u8]) -> io::Result<()> {
        let object_path = self.object_path(object_key);
        let parent_directory = object_path
            .parent()
            .expect("an object path is below the objects directory");
        fs::create_dir_all(parent_directory)?;

        let scratch_path = self.new_scratch_path();
        let mut scratch_file = File::create(&scratch_path)?;
        scratch_file.write_all(contents)?;
        // The object must be on disk before its name is: a reader that
        // finds the name must find all of the contents.
        scratch_file.sync_all()?;
        fs::rename(&scratch_path, &object_path)?;
        File::open(parent_directory)?.sync_all()
    }

    fn read_range(&self, object_key: &str, byte_range: &Range<u64>) -> Result<Bytes, RangeError> {
        let mut object_file = File::open(self.object_path(object_key))?;
        let size_bytes = object_file.metadata()?.len();
        if byte_range.start > byte_range.end || byte_range.end > size_bytes {
            return Err(RangeError::OutOfBounds { size_bytes });
        }
        let range_bytes = usize::try_from(byte_range.end - byte_range.start)
            .map_err(|_| io::Error::other("range does not fit in memory"))?;
        let mut contents = vec![0; range_bytes];
        object_file.seek(SeekFrom::Start(byte_range.start))?;
        object_file.read_exact(&mut contents)?;
        Ok(Bytes::from(contents))
    }

    fn delete(&self, object_key: &str) -> io::Result<()> {
        match fs::remove_file(self.object_path(object_key)) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
            _removed_or_absent => Ok(()),
        }
    }

    /// Lists the objects whose key starts with `key_prefix`, by walking
    /// only the directory that the prefix names.
    fn list(&self, key_prefix: &str) -> io::Result<Vec<ObjectInfo>> {
        let directory_of_prefix = key_prefix
            .rfind('/')
            .map_or("", |last_separator| &key_prefix[..last_separator]);
        if !directory_of_prefix.is_empty() && validate_key(directory_of_prefix).is_err() {
            // No valid key starts with such a prefix.
            return Ok(Vec::new());
        }
        let mut listed = Vec::new();
        let mut directories_to_walk = vec![self.objects.join(directory_of_prefix)];
        while let Some(directory) = directories_to_walk.pop() {
            let entries = match fs::read_dir(&directory) {
                Ok(entries) => entries,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            for entry in entries {
                let entry = entry?;
                let metadata = entry.metadata()?;
                if metadata.is_dir() {
                    directories_to_walk.push(entry.path());
                    continue;
                }
                let object_key = self.key_of(&entry.path())?;
                if object_key.starts_with(key_prefix) {
                    listed.push(ObjectInfo {
                        object_key,
                        size_bytes: metadata.len(),
                    });
                }
            }
        }
        listed.sort_by(|left, right| left.object_key.cmp(&right.object_key));
        Ok(listed)
    }

    fn key_of(&self, object_path: &Path) -> io::Result<String> {
        let relative_path = object_path
            .strip_prefix(&self.objects)
            .map_err(io::Error::other)?;
        let components: Option<Vec<&str>> = relative_path
            .components()
            .map(|component| component.as_os_str().to_str())
            .collect();
        components
            .map(|components| components.join("/"))
            .ok_or_else(|| {
                io::Error::other(format!(
                    "{} is not a valid object key",
                    object_path.display()
                ))
            })
    }
}

/// Why a range could not be read from a file.
enum RangeError {
    OutOfBounds { size_bytes: u64 },
    Io(io::Error),
}

impl From<io::Error> for RangeError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Runs a blocking file system operation off the async threads.
async fn run_blocking<Output: Send + 'static>(
    operation: impl FnOnce() -> io::Result<Output> + Send + 'static,
) -> io::Result<Output> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(io::Error::other)?
}

// By value, so that it can be passed to `map_err` as it is.
#[allow(clippy::needless_pass_by_value)]
fn unavailable(error: io::Error) -> ArtifactError {
    ArtifactError::Unavailable {
        reason: error.to_string(),
    }
}

fn not_found_or_unavailable(object_key: &str, error: io::Error) -> ArtifactError {
    if error.kind() == io::ErrorKind::NotFound {
        ArtifactError::NotFound {
            object_key: object_key.to_owned(),
        }
    } else {
        unavailable(error)
    }
}

#[async_trait]
impl ArtifactStore for LocalArtifactStore {
    async fn put_object(&self, object_key: &str, contents: Bytes) -> Result<(), ArtifactError> {
        validate_key(object_key)?;
        let directories = Arc::clone(&self.directories);
        let owned_key = object_key.to_owned();
        run_blocking(move || directories.put(&owned_key, &contents))
            .await
            .map_err(unavailable)
    }

    async fn get_object(&self, object_key: &str) -> Result<Bytes, ArtifactError> {
        validate_key(object_key)?;
        let object_path = self.directories.object_path(object_key);
        run_blocking(move || fs::read(object_path).map(Bytes::from))
            .await
            .map_err(|error| not_found_or_unavailable(object_key, error))
    }

    async fn get_range(
        &self,
        object_key: &str,
        byte_range: Range<u64>,
    ) -> Result<Bytes, ArtifactError> {
        validate_key(object_key)?;
        let directories = Arc::clone(&self.directories);
        let owned_key = object_key.to_owned();
        let requested_range = byte_range.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            directories.read_range(&owned_key, &requested_range)
        })
        .await
        .map_err(|join_error| unavailable(io::Error::other(join_error)))?;
        outcome.map_err(|range_error| match range_error {
            RangeError::OutOfBounds { size_bytes } => ArtifactError::RangeOutOfBounds {
                object_key: object_key.to_owned(),
                start: byte_range.start,
                end: byte_range.end,
                size_bytes,
            },
            RangeError::Io(error) => not_found_or_unavailable(object_key, error),
        })
    }

    async fn delete_object(&self, object_key: &str) -> Result<(), ArtifactError> {
        validate_key(object_key)?;
        let directories = Arc::clone(&self.directories);
        let owned_key = object_key.to_owned();
        run_blocking(move || directories.delete(&owned_key))
            .await
            .map_err(unavailable)
    }

    async fn list_prefix(&self, key_prefix: &str) -> Result<Vec<ObjectInfo>, ArtifactError> {
        let directories = Arc::clone(&self.directories);
        let owned_prefix = key_prefix.to_owned();
        run_blocking(move || directories.list(&owned_prefix))
            .await
            .map_err(unavailable)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::contract;

    async fn open_store() -> (TempDir, LocalArtifactStore) {
        let root_directory = TempDir::new().expect("temporary directory");
        let store = LocalArtifactStore::open(root_directory.path())
            .await
            .expect("store opens");
        (root_directory, store)
    }

    #[tokio::test]
    async fn local_store_honours_the_artifact_store_contract() {
        let (_root_directory, store) = open_store().await;
        contract::check(&store).await;
    }

    #[tokio::test]
    async fn objects_survive_reopening_the_store() {
        let (root_directory, store) = open_store().await;
        store
            .put_object("streams/1/a", Bytes::from_static(b"kept"))
            .await
            .expect("put");
        drop(store);
        let reopened = LocalArtifactStore::open(root_directory.path())
            .await
            .expect("store reopens");
        assert_eq!(
            reopened.get_object("streams/1/a").await,
            Ok(Bytes::from_static(b"kept"))
        );
    }

    #[tokio::test]
    async fn interrupted_write_is_neither_listed_nor_kept() {
        let (root_directory, store) = open_store().await;
        let leftover = root_directory.path().join(SCRATCH_DIRECTORY).join("put-7");
        fs::write(&leftover, b"half an object").expect("leftover is planted");
        drop(store);
        let reopened = LocalArtifactStore::open(root_directory.path())
            .await
            .expect("store reopens");
        assert!(reopened.list_prefix("").await == Ok(Vec::new()) && !leftover.exists());
    }

    #[tokio::test]
    async fn object_key_may_look_like_a_scratch_name() {
        let (_root_directory, store) = open_store().await;
        store
            .put_object("scratch/put-0", Bytes::from_static(b"an object"))
            .await
            .expect("put");
        store
            .put_object("other", Bytes::from_static(b"another"))
            .await
            .expect("put");
        assert_eq!(
            store.get_object("scratch/put-0").await,
            Ok(Bytes::from_static(b"an object"))
        );
    }

    #[tokio::test]
    async fn prefix_that_ends_inside_a_name_matches_by_characters() {
        let (_root_directory, store) = open_store().await;
        for object_key in ["logs/segment-1", "logs/segment-20", "logs/other"] {
            store
                .put_object(object_key, Bytes::from_static(b"x"))
                .await
                .expect("put");
        }
        let listed_keys: Vec<String> = store
            .list_prefix("logs/segment-")
            .await
            .expect("list")
            .into_iter()
            .map(|object| object.object_key)
            .collect();
        assert_eq!(listed_keys, ["logs/segment-1", "logs/segment-20"]);
    }

    #[tokio::test]
    async fn prefix_that_no_key_can_have_lists_nothing() {
        let (_root_directory, store) = open_store().await;
        store
            .put_object("a", Bytes::from_static(b"x"))
            .await
            .expect("put");
        assert_eq!(store.list_prefix("../").await, Ok(Vec::new()));
    }

    #[tokio::test]
    async fn object_cannot_be_stored_where_a_directory_is() {
        let (_root_directory, store) = open_store().await;
        store
            .put_object("a/b", Bytes::from_static(b"x"))
            .await
            .expect("put");
        assert!(matches!(
            store.put_object("a", Bytes::from_static(b"y")).await,
            Err(ArtifactError::Unavailable { .. })
        ));
    }
}
