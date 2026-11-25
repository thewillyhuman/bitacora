//! The [`Disk`] adapter for a directory of the local file system.
//!
//! This is the only module of the crate that touches real files, which is
//! why it alone opts out of the workspace's I/O lints. Blocking system calls
//! run on Tokio's blocking pool so they never stall request handling.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt as _;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use fs4::FileExt as _;

use crate::disk::{Disk, DiskFile, DiskSpace};

/// Suffix of the scratch file a file is built under before it is renamed
/// into place. A leftover one is an interrupted write and is never listed.
const TEMPORARY_SUFFIX: &str = ".tmp";

/// A data directory on the local file system.
#[derive(Clone, Debug)]
pub struct LocalDisk {
    root_directory: Arc<PathBuf>,
}

impl LocalDisk {
    /// Opens `root_directory` as a data directory, creating it if needed.
    ///
    /// # Errors
    ///
    /// Fails if the directory cannot be created or resolved.
    pub async fn open(root_directory: impl Into<PathBuf>) -> io::Result<Self> {
        let root_directory = root_directory.into();
        run_blocking(move || {
            fs::create_dir_all(&root_directory)?;
            let root_directory = root_directory.canonicalize()?;
            Ok(Self {
                root_directory: Arc::new(root_directory),
            })
        })
        .await
    }

    /// Maps a disk-relative path to a file system path, refusing anything
    /// that could escape the data directory.
    fn resolve(&self, path: &str) -> io::Result<PathBuf> {
        let relative_path = Path::new(path);
        let stays_inside = !path.is_empty()
            && relative_path
                .components()
                .all(|component| matches!(component, Component::Normal(_)));
        if !stays_inside {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("path {path:?} is not a plain relative path"),
            ));
        }
        Ok(self.root_directory.join(relative_path))
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

/// Makes the entries of `directory` durable. Creating or renaming a file
/// changes its parent directory, which has to be flushed like any other data.
fn sync_directory(directory: &Path) -> io::Result<()> {
    File::open(directory)?.sync_all()
}

/// The scratch name a file is built under before it is renamed into place.
fn temporary_path_for(file_path: &Path) -> PathBuf {
    let mut temporary_name = file_path.as_os_str().to_owned();
    temporary_name.push(TEMPORARY_SUFFIX);
    PathBuf::from(temporary_name)
}

fn parent_of(file_path: &Path) -> io::Result<&Path> {
    file_path
        .parent()
        .ok_or_else(|| io::Error::other("a resolved path always has a parent"))
}

/// Creates the directories above `file_path` and makes them durable.
fn create_parent_directories(root_directory: &Path, file_path: &Path) -> io::Result<()> {
    let parent_directory = parent_of(file_path)?;
    fs::create_dir_all(parent_directory)?;
    let mut directory = parent_directory;
    while directory.starts_with(root_directory) && directory != root_directory {
        sync_directory(parent_of(directory)?)?;
        directory = parent_of(directory)?;
    }
    Ok(())
}

fn collect_files(
    directory: &Path,
    root_directory: &Path,
    found: &mut Vec<String>,
) -> io::Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry_path = entry?.path();
        if entry_path.is_dir() {
            collect_files(&entry_path, root_directory, found)?;
            continue;
        }
        let relative_path = entry_path
            .strip_prefix(root_directory)
            .map_err(io::Error::other)?;
        let listed_path = relative_path
            .components()
            .map(|component| component.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");
        if !listed_path.ends_with(TEMPORARY_SUFFIX) {
            found.push(listed_path);
        }
    }
    Ok(())
}

#[async_trait]
impl Disk for LocalDisk {
    async fn create_file(
        &self,
        path: &str,
        initial_content: Bytes,
        preallocated_bytes: u64,
    ) -> io::Result<Box<dyn DiskFile>> {
        let file_path = self.resolve(path)?;
        let root_directory = Arc::clone(&self.root_directory);
        run_blocking(move || {
            create_parent_directories(&root_directory, &file_path)?;
            if file_path.exists() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("file exists: {}", file_path.display()),
                ));
            }
            // Build the file under a scratch name and rename it into place
            // once it is durable, so that the real name only ever points at
            // a file that starts with its initial content.
            let temporary_path = temporary_path_for(&file_path);
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temporary_path)?;
            if preallocated_bytes > 0 {
                // Reserving the blocks now means a later flush only has to
                // persist data, not a size change or a block allocation.
                file.allocate(preallocated_bytes)?;
            }
            file.write_all_at(&initial_content, 0)?;
            file.sync_all()?;
            fs::rename(&temporary_path, &file_path)?;
            sync_directory(parent_of(&file_path)?)?;
            Ok(Box::new(LocalDiskFile {
                file: Arc::new(file),
            }) as Box<dyn DiskFile>)
        })
        .await
    }

    async fn open_file(&self, path: &str) -> io::Result<Box<dyn DiskFile>> {
        let file_path = self.resolve(path)?;
        run_blocking(move || {
            let file = OpenOptions::new().read(true).write(true).open(&file_path)?;
            Ok(Box::new(LocalDiskFile {
                file: Arc::new(file),
            }) as Box<dyn DiskFile>)
        })
        .await
    }

    async fn write_file_atomically(&self, path: &str, contents: Bytes) -> io::Result<()> {
        let file_path = self.resolve(path)?;
        let root_directory = Arc::clone(&self.root_directory);
        run_blocking(move || {
            create_parent_directories(&root_directory, &file_path)?;
            let temporary_path = temporary_path_for(&file_path);
            let temporary_file = File::create(&temporary_path)?;
            temporary_file.write_all_at(&contents, 0)?;
            // Flush before the rename: a rename is atomic, but it must never
            // expose a name that points at content not yet on disk.
            temporary_file.sync_all()?;
            fs::rename(&temporary_path, &file_path)?;
            sync_directory(parent_of(&file_path)?)
        })
        .await
    }

    async fn read_file(&self, path: &str) -> io::Result<Option<Bytes>> {
        let file_path = self.resolve(path)?;
        run_blocking(move || match fs::read(&file_path) {
            Ok(contents) => Ok(Some(Bytes::from(contents))),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        })
        .await
    }

    async fn remove_file(&self, path: &str) -> io::Result<()> {
        let file_path = self.resolve(path)?;
        run_blocking(move || match fs::remove_file(&file_path) {
            Ok(()) => sync_directory(parent_of(&file_path)?),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        })
        .await
    }

    async fn rename_file(&self, from_path: &str, to_path: &str) -> io::Result<()> {
        let source_path = self.resolve(from_path)?;
        let target_path = self.resolve(to_path)?;
        run_blocking(move || {
            fs::rename(&source_path, &target_path)?;
            // The move is two directory changes when the files are in two
            // directories, and neither is durable until its directory is.
            sync_directory(parent_of(&target_path)?)?;
            sync_directory(parent_of(&source_path)?)
        })
        .await
    }

    async fn list_files(&self) -> io::Result<Vec<String>> {
        let root_directory = Arc::clone(&self.root_directory);
        run_blocking(move || {
            let mut found = Vec::new();
            collect_files(&root_directory, &root_directory, &mut found)?;
            found.sort();
            Ok(found)
        })
        .await
    }

    async fn space(&self) -> io::Result<DiskSpace> {
        let root_directory = Arc::clone(&self.root_directory);
        run_blocking(move || {
            Ok(DiskSpace {
                capacity_bytes: fs4::total_space(root_directory.as_path())?,
                free_bytes: fs4::available_space(root_directory.as_path())?,
            })
        })
        .await
    }
}

/// A handle to one file of a [`LocalDisk`].
#[derive(Debug)]
struct LocalDiskFile {
    file: Arc<File>,
}

#[async_trait]
impl DiskFile for LocalDiskFile {
    async fn read_at(&self, offset_bytes: u64, length_bytes: usize) -> io::Result<Bytes> {
        let file = Arc::clone(&self.file);
        run_blocking(move || {
            let mut contents = BytesMut::zeroed(length_bytes);
            let mut filled_bytes = 0;
            while filled_bytes < length_bytes {
                let read_offset =
                    offset_bytes + u64::try_from(filled_bytes).expect("lengths fit in 64 bits");
                match file.read_at(&mut contents[filled_bytes..], read_offset) {
                    Ok(0) => break,
                    Ok(read_bytes) => filled_bytes += read_bytes,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(error) => return Err(error),
                }
            }
            contents.truncate(filled_bytes);
            Ok(contents.freeze())
        })
        .await
    }

    async fn write_at(&mut self, offset_bytes: u64, data: Bytes) -> io::Result<()> {
        let file = Arc::clone(&self.file);
        run_blocking(move || file.write_all_at(&data, offset_bytes)).await
    }

    async fn sync_data(&mut self) -> io::Result<()> {
        let file = Arc::clone(&self.file);
        run_blocking(move || file.sync_data()).await
    }

    async fn set_length(&mut self, length_bytes: u64) -> io::Result<()> {
        let file = Arc::clone(&self.file);
        run_blocking(move || {
            file.set_len(length_bytes)?;
            file.sync_all()
        })
        .await
    }

    async fn length_bytes(&self) -> io::Result<u64> {
        let file = Arc::clone(&self.file);
        run_blocking(move || Ok(file.metadata()?.len())).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PATH: &str = "7/3.seg";

    async fn disk_in(directory: &tempfile::TempDir) -> LocalDisk {
        LocalDisk::open(directory.path().join("data"))
            .await
            .expect("data directory")
    }

    #[tokio::test]
    async fn written_bytes_are_read_back_after_reopening() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let disk = disk_in(&directory).await;
        let mut file = disk
            .create_file(PATH, Bytes::new(), 0)
            .await
            .expect("new file");
        file.write_at(3, Bytes::from_static(b"abc"))
            .await
            .expect("write");
        file.sync_data().await.expect("flush");

        let reopened = disk_in(&directory)
            .await
            .open_file(PATH)
            .await
            .expect("open");
        assert_eq!(
            reopened.read_at(0, 64).await.expect("read"),
            Bytes::from_static(b"\0\0\0abc")
        );
    }

    #[tokio::test]
    async fn new_file_starts_with_its_initial_content() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let disk = disk_in(&directory).await;
        disk.create_file(PATH, Bytes::from_static(b"header"), 8)
            .await
            .expect("new file");
        let reopened = disk.open_file(PATH).await.expect("open");
        assert_eq!(
            reopened.read_at(0, 64).await.expect("read"),
            Bytes::from_static(b"header\0\0")
        );
    }

    #[tokio::test]
    async fn rename_replaces_the_file_that_was_there() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let disk = disk_in(&directory).await;
        disk.write_file_atomically("7/3.seg", Bytes::from_static(b"old copy"))
            .await
            .expect("write");
        disk.write_file_atomically("7/3.seg.repair", Bytes::from_static(b"new copy"))
            .await
            .expect("write");

        disk.rename_file("7/3.seg.repair", "7/3.seg")
            .await
            .expect("rename");

        assert_eq!(
            disk.read_file("7/3.seg").await.expect("read"),
            Some(Bytes::from_static(b"new copy"))
        );
        assert_eq!(disk.read_file("7/3.seg.repair").await.expect("read"), None);
    }

    #[tokio::test]
    async fn rename_of_a_missing_file_fails_and_leaves_the_target_alone() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let disk = disk_in(&directory).await;
        disk.write_file_atomically("7/3.seg", Bytes::from_static(b"old copy"))
            .await
            .expect("write");

        let renamed = disk.rename_file("7/3.seg.repair", "7/3.seg").await;

        assert_eq!(
            renamed.map_err(|error| error.kind()),
            Err(io::ErrorKind::NotFound)
        );
        assert_eq!(
            disk.read_file("7/3.seg").await.expect("read"),
            Some(Bytes::from_static(b"old copy"))
        );
    }

    #[tokio::test]
    async fn creation_leaves_no_scratch_file_behind() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let disk = disk_in(&directory).await;
        disk.create_file(PATH, Bytes::from_static(b"header"), 0)
            .await
            .expect("new file");
        let scratch_path = directory.path().join("data").join("7").join("3.seg.tmp");
        assert!(!scratch_path.exists());
    }

    #[tokio::test]
    async fn new_file_has_its_preallocated_length() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let disk = disk_in(&directory).await;
        let file = disk
            .create_file(PATH, Bytes::new(), 4096)
            .await
            .expect("new file");
        assert_eq!(file.length_bytes().await.expect("length"), 4096);
    }

    #[tokio::test]
    async fn creating_an_existing_file_fails() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let disk = disk_in(&directory).await;
        disk.create_file(PATH, Bytes::new(), 0)
            .await
            .expect("new file");
        let second_attempt = disk.create_file(PATH, Bytes::new(), 0).await;
        assert_eq!(
            second_attempt.err().map(|error| error.kind()),
            Some(io::ErrorKind::AlreadyExists)
        );
    }

    #[tokio::test]
    async fn opening_a_missing_file_fails() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let opened = disk_in(&directory).await.open_file(PATH).await;
        assert_eq!(
            opened.err().map(|error| error.kind()),
            Some(io::ErrorKind::NotFound)
        );
    }

    #[tokio::test]
    async fn reading_past_the_end_returns_what_exists() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let disk = disk_in(&directory).await;
        let mut file = disk
            .create_file(PATH, Bytes::new(), 0)
            .await
            .expect("new file");
        file.write_at(0, Bytes::from_static(b"abc"))
            .await
            .expect("write");
        assert_eq!(
            file.read_at(1, 64).await.expect("read"),
            Bytes::from_static(b"bc")
        );
    }

    #[tokio::test]
    async fn atomic_write_replaces_the_previous_content() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let disk = disk_in(&directory).await;
        for contents in [&b"first"[..], &b"second"[..]] {
            disk.write_file_atomically("7/3.state", Bytes::from_static(contents))
                .await
                .expect("write");
        }
        assert_eq!(
            disk.read_file("7/3.state").await.expect("read"),
            Some(Bytes::from_static(b"second"))
        );
    }

    #[tokio::test]
    async fn reading_a_missing_small_file_returns_none() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let disk = disk_in(&directory).await;
        assert_eq!(disk.read_file("7/3.state").await.expect("read"), None);
    }

    #[tokio::test]
    async fn files_are_listed_sorted_without_scratch_files() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let disk = disk_in(&directory).await;
        for path in ["7/3.seg", "2/9.seg", "7/3.state.tmp"] {
            disk.create_file(path, Bytes::new(), 0)
                .await
                .expect("new file");
        }
        assert_eq!(
            disk.list_files().await.expect("list"),
            ["2/9.seg", "7/3.seg"]
        );
    }

    #[tokio::test]
    async fn removed_file_is_no_longer_listed() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let disk = disk_in(&directory).await;
        disk.create_file(PATH, Bytes::new(), 0)
            .await
            .expect("new file");
        disk.remove_file(PATH).await.expect("remove");
        assert_eq!(disk.list_files().await.expect("list"), Vec::<String>::new());
    }

    #[tokio::test]
    async fn removing_a_missing_file_succeeds() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let disk = disk_in(&directory).await;
        assert!(disk.remove_file(PATH).await.is_ok());
    }

    #[tokio::test]
    async fn shrinking_a_file_cuts_its_content() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let disk = disk_in(&directory).await;
        let mut file = disk
            .create_file(PATH, Bytes::new(), 0)
            .await
            .expect("new file");
        file.write_at(0, Bytes::from_static(b"keep-drop"))
            .await
            .expect("write");
        file.set_length(4).await.expect("shrink");
        assert_eq!(
            file.read_at(0, 64).await.expect("read"),
            Bytes::from_static(b"keep")
        );
    }

    #[tokio::test]
    async fn paths_that_escape_the_data_directory_are_refused() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let disk = disk_in(&directory).await;
        for escaping_path in ["../outside", "/etc/passwd", "7/../../outside", ""] {
            let created = disk.create_file(escaping_path, Bytes::new(), 0).await;
            assert_eq!(
                created.err().map(|error| error.kind()),
                Some(io::ErrorKind::InvalidInput),
                "path {escaping_path:?} was accepted"
            );
        }
    }

    #[tokio::test]
    async fn disk_reports_free_space_within_its_capacity() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let space = disk_in(&directory).await.space().await.expect("space");
        assert!(space.capacity_bytes > 0 && space.free_bytes <= space.capacity_bytes);
    }
}
