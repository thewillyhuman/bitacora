//! The [`Disk`] adapter for a directory of the local file system.
//!
//! # Why this file exists
//!
//! In production a replica keeps its segment copies in a data directory on a local disk. Everything else
//! in the workspace reaches a disk through the [`Disk`] interface, so that it can run on an in-memory disk
//! under test. Something has to implement that interface on real files, and get right the parts a file
//! system makes easy to get wrong: a file's name is durable only once its directory is flushed, a new file
//! must not be visible under its name before its content is on disk, and a blocking system call must not
//! stall the threads that serve requests. This file is that implementation, and the only place in the crate
//! that touches real files.
//!
//! # What it contains
//!
//! - [`LocalDisk`]: a data directory, as a [`Disk`].
//! - `LocalDiskFile`: one open file of it, as a [`DiskFile`].
//!
//! # How it works
//!
//! A path given to the disk is relative to the data directory, with `/` between its components. It is
//! refused unless every component is a plain name, so no path can leave the directory.
//!
//! Durability follows three rules:
//!
//! - **New and replaced files are built under a scratch name** (the final name plus `.tmp`), flushed, and
//!   renamed into place. A rename is atomic, so the real name only ever points at complete content. A
//!   scratch file left by a crash is never listed.
//! - **Every change of a name is followed by a flush of its directory.** Creating, renaming or removing a
//!   file changes the directory, which has to reach the disk like any other data.
//! - **Blocks are reserved when a file is created.** A later flush then only persists data, not a change
//!   of size or a block allocation, which keeps flushes short and avoids running out of space in the
//!   middle of an append.
//!
//! Every system call is blocking, so each operation runs on Tokio's blocking pool and the async threads
//! stay free.
//!
//! # Invariants
//!
//! - No operation touches anything outside the data directory.
//! - A file that exists under its final name starts with the content it was created with.
//! - This is the only module of the crate that uses `std::fs`. The workspace's lints forbid direct file
//!   access everywhere else; this module opts out below, because implementing the [`Disk`] interface on
//!   real files is exactly its job.
//!
//! # See also
//!
//! - `disk.rs`: the interface and what each operation promises.
//! - `memory_disk.rs`: the implementation for tests, which models the crashes this one must survive.

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

/// Suffix of the scratch file a file is built under before it is renamed into place. A leftover one is an
/// interrupted write and is never listed.
const TEMPORARY_SUFFIX: &str = ".tmp";

/// What separates the components of a path given to a [`Disk`], on every platform.
const DISK_PATH_SEPARATOR: &str = "/";

/// A data directory on the local file system.
///
/// Clones share the directory. The directory belongs to one replica process at a time; nothing here locks
/// it against a second one.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> std::io::Result<()> {
/// use bitacora_segment::{Disk, LocalDisk};
/// use bytes::Bytes;
///
/// let temporary_directory = tempfile::tempdir()?;
/// let disk = LocalDisk::open(temporary_directory.path().join("data")).await?;
///
/// disk.write_file_atomically("replica.id", Bytes::from_static(b"replica-a"))
///     .await?;
///
/// assert_eq!(disk.read_file("replica.id").await?, Some(Bytes::from_static(b"replica-a")));
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct LocalDisk {
    /// The data directory, as an absolute path without symbolic links. Every path the disk is given is
    /// resolved below it.
    root_directory: Arc<PathBuf>,
}

impl LocalDisk {
    /// Opens a directory as a data directory, creating it if needed.
    ///
    /// # Arguments
    ///
    /// * `root_directory` - The directory to use. Missing directories on the way to it are created.
    ///
    /// # Returns
    ///
    /// The disk. Opening reads and writes no file.
    ///
    /// # Errors
    ///
    /// Fails with the underlying error if the directory cannot be created or resolved.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> std::io::Result<()> {
    /// use bitacora_segment::{Disk, LocalDisk};
    ///
    /// let temporary_directory = tempfile::tempdir()?;
    /// let disk = LocalDisk::open(temporary_directory.path().join("data")).await?;
    ///
    /// assert!(disk.list_files().await?.is_empty());
    /// # Ok(())
    /// # }
    /// ```
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

    /// Maps a disk-relative path to a file system path, refusing anything that could escape the data
    /// directory.
    ///
    /// Fails with [`io::ErrorKind::InvalidInput`] if the path is empty, absolute, or has a component that
    /// is not a plain name, such as `..`.
    fn resolve(&self, path: &str) -> io::Result<PathBuf> {
        let relative_path = Path::new(path);
        let mut stays_inside = !path.is_empty();
        for component in relative_path.components() {
            if !matches!(component, Component::Normal(_)) {
                stays_inside = false;
            }
        }
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
///
/// Fails with the operation's own error, or with an error that wraps the panic if the operation panicked.
async fn run_blocking<Output: Send + 'static>(
    operation: impl FnOnce() -> io::Result<Output> + Send + 'static,
) -> io::Result<Output> {
    match tokio::task::spawn_blocking(operation).await {
        Ok(outcome) => outcome,
        Err(join_error) => Err(io::Error::other(join_error)),
    }
}

/// Makes the entries of `directory` durable. Creating, renaming or removing a file changes its parent
/// directory, which has to be flushed like any other data.
fn sync_directory(directory: &Path) -> io::Result<()> {
    File::open(directory)?.sync_all()
}

/// The scratch name a file is built under before it is renamed into place.
fn temporary_path_for(file_path: &Path) -> PathBuf {
    let mut temporary_name = file_path.as_os_str().to_owned();
    temporary_name.push(TEMPORARY_SUFFIX);
    PathBuf::from(temporary_name)
}

/// The directory that holds `file_path`. A resolved path always has one: it lies below the data directory.
fn parent_of(file_path: &Path) -> io::Result<&Path> {
    match file_path.parent() {
        Some(parent_directory) => Ok(parent_directory),
        None => Err(io::Error::other("a resolved path always has a parent")),
    }
}

/// Creates the directories above `file_path` and makes them durable, up to the data directory.
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

/// Wraps an open file in the handle the [`Disk`] interface returns.
fn handle_to(file: File) -> Box<dyn DiskFile> {
    Box::new(LocalDiskFile { file: Arc::new(file) })
}

/// Creates the file at `file_path` with its initial content and preallocated size, durably and atomically.
///
/// The file is built under a scratch name and renamed into place once it is durable, so that the real name
/// only ever points at a file that starts with its initial content.
fn create_file_durably(
    root_directory: &Path,
    file_path: &Path,
    initial_content: &[u8],
    preallocated_bytes: u64,
) -> io::Result<Box<dyn DiskFile>> {
    create_parent_directories(root_directory, file_path)?;
    if file_path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("file exists: {}", file_path.display()),
        ));
    }
    let temporary_path = temporary_path_for(file_path);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temporary_path)?;
    if preallocated_bytes > 0 {
        // Reserving the blocks now means a later flush only has to persist data, not a size change or a
        // block allocation.
        file.allocate(preallocated_bytes)?;
    }
    file.write_all_at(initial_content, 0)?;
    file.sync_all()?;
    fs::rename(&temporary_path, file_path)?;
    sync_directory(parent_of(file_path)?)?;
    Ok(handle_to(file))
}

/// Replaces the whole content of the file at `file_path`, durably and atomically.
fn replace_file_durably(root_directory: &Path, file_path: &Path, contents: &[u8]) -> io::Result<()> {
    create_parent_directories(root_directory, file_path)?;
    let temporary_path = temporary_path_for(file_path);
    let temporary_file = File::create(&temporary_path)?;
    temporary_file.write_all_at(contents, 0)?;
    // Flush before the rename: a rename is atomic, but it must never expose a name that points at content
    // not yet on disk.
    temporary_file.sync_all()?;
    fs::rename(&temporary_path, file_path)?;
    sync_directory(parent_of(file_path)?)
}

/// Adds to `found` the disk-relative path of every file below `directory`, scratch files excepted.
fn collect_files(directory: &Path, root_directory: &Path, found: &mut Vec<String>) -> io::Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry_path = entry?.path();
        if entry_path.is_dir() {
            collect_files(&entry_path, root_directory, found)?;
            continue;
        }
        let listed_path = disk_path_of(&entry_path, root_directory)?;
        if !listed_path.ends_with(TEMPORARY_SUFFIX) {
            found.push(listed_path);
        }
    }
    Ok(())
}

/// Turns a file system path below the data directory back into the path the [`Disk`] interface uses:
/// relative to the data directory, with `/` between components.
fn disk_path_of(file_path: &Path, root_directory: &Path) -> io::Result<String> {
    let relative_path = match file_path.strip_prefix(root_directory) {
        Ok(relative_path) => relative_path,
        Err(not_below_the_root) => return Err(io::Error::other(not_below_the_root)),
    };
    let mut component_names = Vec::new();
    for component in relative_path.components() {
        component_names.push(component.as_os_str().to_string_lossy());
    }
    Ok(component_names.join(DISK_PATH_SEPARATOR))
}

#[async_trait]
impl Disk for LocalDisk {
    /// Builds the file under a scratch name with its blocks reserved, flushes it, renames it into place
    /// and flushes the directory.
    async fn create_file(
        &self,
        path: &str,
        initial_content: Bytes,
        preallocated_bytes: u64,
    ) -> io::Result<Box<dyn DiskFile>> {
        let file_path = self.resolve(path)?;
        let root_directory = Arc::clone(&self.root_directory);
        run_blocking(move || create_file_durably(&root_directory, &file_path, &initial_content, preallocated_bytes))
            .await
    }

    /// Opens the file for reading and writing.
    async fn open_file(&self, path: &str) -> io::Result<Box<dyn DiskFile>> {
        let file_path = self.resolve(path)?;
        run_blocking(move || {
            let file = OpenOptions::new().read(true).write(true).open(&file_path)?;
            Ok(handle_to(file))
        })
        .await
    }

    /// Writes the new content under a scratch name, flushes it, renames it over the file and flushes the
    /// directory.
    async fn write_file_atomically(&self, path: &str, contents: Bytes) -> io::Result<()> {
        let file_path = self.resolve(path)?;
        let root_directory = Arc::clone(&self.root_directory);
        run_blocking(move || replace_file_durably(&root_directory, &file_path, &contents)).await
    }

    /// Reads the whole file into memory.
    async fn read_file(&self, path: &str) -> io::Result<Option<Bytes>> {
        let file_path = self.resolve(path)?;
        run_blocking(move || match fs::read(&file_path) {
            Ok(contents) => Ok(Some(Bytes::from(contents))),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        })
        .await
    }

    /// Removes the file and flushes its directory.
    async fn remove_file(&self, path: &str) -> io::Result<()> {
        let file_path = self.resolve(path)?;
        run_blocking(move || match fs::remove_file(&file_path) {
            Ok(()) => sync_directory(parent_of(&file_path)?),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        })
        .await
    }

    /// Renames the file and flushes the directories of both names.
    async fn rename_file(&self, from_path: &str, to_path: &str) -> io::Result<()> {
        let source_path = self.resolve(from_path)?;
        let target_path = self.resolve(to_path)?;
        run_blocking(move || {
            fs::rename(&source_path, &target_path)?;
            // The move is two directory changes when the files are in two directories, and neither is
            // durable until its directory is.
            sync_directory(parent_of(&target_path)?)?;
            sync_directory(parent_of(&source_path)?)
        })
        .await
    }

    /// Walks the data directory and sorts what it finds, leaving scratch files out.
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

    /// Asks the file system that holds the data directory for its size and its available space.
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
    /// The open file. Shared, so that each operation can take it onto the blocking pool without reopening
    /// it; positional reads and writes need no shared cursor.
    file: Arc<File>,
}

/// Reads up to `length_bytes` of `file` starting at `offset_bytes`, retrying interrupted and short reads
/// until the buffer is full or the file ends.
fn read_fully_at(file: &File, offset_bytes: u64, length_bytes: usize) -> io::Result<Bytes> {
    let mut contents = BytesMut::zeroed(length_bytes);
    let mut filled_bytes = 0;
    while filled_bytes < length_bytes {
        let read_offset = offset_bytes + u64::try_from(filled_bytes).expect("lengths fit in 64 bits");
        match file.read_at(&mut contents[filled_bytes..], read_offset) {
            // The file ends here.
            Ok(0) => break,
            Ok(read_bytes) => filled_bytes += read_bytes,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    contents.truncate(filled_bytes);
    Ok(contents.freeze())
}

#[async_trait]
impl DiskFile for LocalDiskFile {
    /// Reads with positional reads until the request is filled or the file ends.
    async fn read_at(&self, offset_bytes: u64, length_bytes: usize) -> io::Result<Bytes> {
        let file = Arc::clone(&self.file);
        run_blocking(move || read_fully_at(&file, offset_bytes, length_bytes)).await
    }

    /// Writes all of the data with a positional write.
    async fn write_at(&mut self, offset_bytes: u64, data: Bytes) -> io::Result<()> {
        let file = Arc::clone(&self.file);
        run_blocking(move || file.write_all_at(&data, offset_bytes)).await
    }

    /// Calls `fdatasync`: the data reaches the disk, and so does the size if it changed.
    async fn sync_data(&mut self) -> io::Result<()> {
        let file = Arc::clone(&self.file);
        run_blocking(move || file.sync_data()).await
    }

    /// Sets the length and flushes data and metadata, so the new length is durable.
    async fn set_length(&mut self, length_bytes: u64) -> io::Result<()> {
        let file = Arc::clone(&self.file);
        run_blocking(move || {
            file.set_len(length_bytes)?;
            file.sync_all()
        })
        .await
    }

    /// Asks the file system for the file's length.
    async fn length_bytes(&self) -> io::Result<u64> {
        let file = Arc::clone(&self.file);
        run_blocking(move || Ok(file.metadata()?.len())).await
    }
}

#[cfg(test)]
#[path = "local_disk_test.rs"]
mod tests;
