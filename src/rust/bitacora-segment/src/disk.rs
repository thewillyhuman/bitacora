//! The injected disk interface.
//!
//! # Why this file exists
//!
//! A replica's whole promise is that a record it acknowledged survives a power loss. That promise can only
//! be tested if a test can cut the power at any instant and see what the disk kept, and a real file system
//! does not let a test do that. So segment code never opens a file itself (design, section 15). It
//! receives a [`Disk`]: the local file system in production, an in-memory model that keeps "written" and
//! "flushed" apart under test. This file is that interface, and with it the list of everything bitacora
//! asks of a disk.
//!
//! # What it contains
//!
//! - [`Disk`]: one data directory. Create, open, replace, read, remove, rename and list files, and measure
//!   free space.
//! - [`DiskFile`]: one open file. Read and write at an offset, flush, and set the length.
//! - [`DiskSpace`]: how much room a disk has.
//!
//! # How it works
//!
//! The interface is small on purpose, and every operation says what a crash may leave behind, because that
//! is the only thing callers care about:
//!
//! - A file is **created** together with its first content, atomically. A file that exists under its name
//!   is never one whose creation was interrupted.
//! - A **write** at an offset is not durable until the file is **flushed**. A crash may keep any part of
//!   the writes since the last flush, or none.
//! - A small file is **replaced** whole and atomically: a crash leaves the old content or the new one.
//! - A file is **renamed** over another atomically, which is how a large file is replaced without a moment
//!   at which neither version exists.
//!
//! Paths are relative to the data directory and use `/` as separator on every platform.
//!
//! # Invariants
//!
//! - Nothing is durable before the operation that makes it durable has returned successfully.
//! - After a flush fails, the durable content of that file is unknown. The caller must stop using its view
//!   of the file: the kernel may have dropped the dirty pages, and a retry could falsely succeed.
//! - Both implementations behave the same for everything a caller can observe without a crash.
//!
//! # See also
//!
//! - `local_disk.rs`: the implementation on a directory of the local file system.
//! - `memory_disk.rs`: the in-memory implementation that models crashes.
//! - `file.rs`: the segment file, the main user of this interface.

use std::io;

use async_trait::async_trait;
use bytes::Bytes;

/// The share of a disk that counts as in use when the disk reports no capacity at all, in percent. A disk
/// that cannot say how large it is must not be handed more data.
const USED_PERCENT_WITHOUT_CAPACITY: u8 = 100;

/// How many percent make a whole.
const PERCENT_OF_THE_WHOLE: u128 = 100;

/// How much room a disk has.
///
/// # Examples
///
/// ```
/// use bitacora_segment::DiskSpace;
///
/// let space = DiskSpace {
///     capacity_bytes: 1_000,
///     free_bytes: 149,
/// };
///
/// assert_eq!(space.used_percent(), 85);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiskSpace {
    /// The total size of the disk, in bytes.
    pub capacity_bytes: u64,
    /// How much of it is still unused, in bytes.
    pub free_bytes: u64,
}

impl DiskSpace {
    /// Returns the share of the disk in use.
    ///
    /// A replica compares this with its thresholds to stop taking new segments, and then appends, before
    /// the disk is full.
    ///
    /// # Returns
    ///
    /// The used share in percent, rounded down, from 0 to 100. A disk that reports no capacity counts as
    /// full.
    ///
    /// # Panics
    ///
    /// Never in practice: the used bytes are at most the capacity, so the share is at most 100 and fits
    /// in a byte.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_segment::DiskSpace;
    ///
    /// let empty_disk = DiskSpace {
    ///     capacity_bytes: 1_000,
    ///     free_bytes: 1_000,
    /// };
    ///
    /// assert_eq!(empty_disk.used_percent(), 0);
    /// ```
    #[must_use]
    pub fn used_percent(self) -> u8 {
        if self.capacity_bytes == 0 {
            return USED_PERCENT_WITHOUT_CAPACITY;
        }
        let used_bytes = u128::from(self.capacity_bytes.saturating_sub(self.free_bytes));
        let percent = used_bytes * PERCENT_OF_THE_WHOLE / u128::from(self.capacity_bytes);
        u8::try_from(percent).expect("a share of the whole is at most 100")
    }
}

/// One data directory. Paths are relative to it and use `/` as separator.
///
/// Implemented by [`crate::LocalDisk`] for real files and by [`crate::MemoryDisk`] for tests and
/// simulation. Code takes a `&dyn Disk` or an `Arc<dyn Disk>` and never names either.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> std::io::Result<()> {
/// use bitacora_segment::{Disk, MemoryDisk};
/// use bytes::Bytes;
///
/// let disk = MemoryDisk::new();
/// disk.write_file_atomically("replica.id", Bytes::from_static(b"replica-a"))
///     .await?;
///
/// assert_eq!(disk.read_file("replica.id").await?, Some(Bytes::from_static(b"replica-a")));
/// assert_eq!(disk.list_files().await?, ["replica.id"]);
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait Disk: Send + Sync + 'static {
    /// Creates a file that starts with given content and is zero-filled up to a preallocated size, and
    /// makes all of it durable.
    ///
    /// Creation is atomic: after a crash the file either does not exist or starts with `initial_content`.
    /// A file that exists under its name can therefore never be mistaken for one whose creation was
    /// interrupted.
    ///
    /// # Arguments
    ///
    /// * `path` - Where to create the file, relative to the data directory. Missing directories are
    ///   created.
    /// * `initial_content` - The bytes the file starts with, for example a header.
    /// * `preallocated_bytes` - The size to reserve. The file is at least this long, the rest zeros, so
    ///   that later flushes do not also have to persist a change of size.
    ///
    /// # Returns
    ///
    /// A handle to the new file, open for reading and writing.
    ///
    /// # Errors
    ///
    /// Fails with [`io::ErrorKind::AlreadyExists`] if the file exists, or with the underlying error.
    async fn create_file(
        &self,
        path: &str,
        initial_content: Bytes,
        preallocated_bytes: u64,
    ) -> io::Result<Box<dyn DiskFile>>;

    /// Opens an existing file for reading and writing.
    ///
    /// # Arguments
    ///
    /// * `path` - The file to open, relative to the data directory.
    ///
    /// # Returns
    ///
    /// A handle to the file.
    ///
    /// # Errors
    ///
    /// Fails with [`io::ErrorKind::NotFound`] if the file does not exist, or with the underlying error.
    async fn open_file(&self, path: &str) -> io::Result<Box<dyn DiskFile>>;

    /// Replaces the whole content of a small file.
    ///
    /// After it returns, a crash leaves either the old content or the new one, never a mix, and the new
    /// content is durable. The file is created if it does not exist.
    ///
    /// # Arguments
    ///
    /// * `path` - The file to replace, relative to the data directory.
    /// * `contents` - The new content. Meant for small files: it is written in one piece.
    ///
    /// # Errors
    ///
    /// Fails with the underlying error; the old content is then intact.
    async fn write_file_atomically(&self, path: &str, contents: Bytes) -> io::Result<()>;

    /// Reads the whole content of a small file.
    ///
    /// # Arguments
    ///
    /// * `path` - The file to read, relative to the data directory.
    ///
    /// # Returns
    ///
    /// The content, or `None` if the file is absent.
    ///
    /// # Errors
    ///
    /// Fails with the underlying error.
    async fn read_file(&self, path: &str) -> io::Result<Option<Bytes>>;

    /// Removes a file durably. Removing an absent file succeeds.
    ///
    /// # Arguments
    ///
    /// * `path` - The file to remove, relative to the data directory.
    ///
    /// # Errors
    ///
    /// Fails with the underlying error.
    async fn remove_file(&self, path: &str) -> io::Result<()>;

    /// Moves a file to another path, replacing a file already there.
    ///
    /// The move is atomic and durable: once it returns, `to_path` holds the moved file and `from_path` is
    /// gone, also after a crash. A crash before it returns leaves either both files as they were, or the
    /// move done. A reader of `to_path` never sees a mix.
    ///
    /// This is how a file is replaced without a moment at which its old content is gone and its new
    /// content is not yet there. No handle to either file may be open during the move.
    ///
    /// # Arguments
    ///
    /// * `from_path` - The file to move, relative to the data directory.
    /// * `to_path` - Where it goes. A file already there is replaced.
    ///
    /// # Errors
    ///
    /// Fails with [`io::ErrorKind::NotFound`] if `from_path` does not exist, or with the underlying error;
    /// `to_path` is then as it was.
    async fn rename_file(&self, from_path: &str, to_path: &str) -> io::Result<()>;

    /// Lists every file on the disk.
    ///
    /// # Returns
    ///
    /// The path of every file, relative to the data directory, sorted. Files whose writing was
    /// interrupted before they got their name are not listed.
    ///
    /// # Errors
    ///
    /// Fails with the underlying error.
    async fn list_files(&self) -> io::Result<Vec<String>>;

    /// Reports the disk's capacity and free space.
    ///
    /// # Returns
    ///
    /// The total size of the disk and how much of it is unused, as of now.
    ///
    /// # Errors
    ///
    /// Fails with the underlying error.
    async fn space(&self) -> io::Result<DiskSpace>;
}

/// One open file of a [`Disk`].
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> std::io::Result<()> {
/// use bitacora_segment::{Disk, MemoryDisk};
/// use bytes::Bytes;
///
/// let disk = MemoryDisk::new();
/// let mut file = disk.create_file("7/3.seg", Bytes::from_static(b"header"), 0).await?;
///
/// file.write_at(6, Bytes::from_static(b"-batch")).await?;
/// file.sync_data().await?;
///
/// assert_eq!(file.read_at(0, 64).await?, Bytes::from_static(b"header-batch"));
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait DiskFile: Send + Sync {
    /// Reads bytes at an offset.
    ///
    /// # Arguments
    ///
    /// * `offset_bytes` - Where to start reading.
    /// * `length_bytes` - How many bytes to read at most.
    ///
    /// # Returns
    ///
    /// The bytes read. Shorter than `length_bytes` only when the file ends first; empty at or past the
    /// end. What is returned includes writes that were not flushed yet.
    ///
    /// # Errors
    ///
    /// Fails with the underlying error.
    async fn read_at(&self, offset_bytes: u64, length_bytes: usize) -> io::Result<Bytes>;

    /// Writes bytes at an offset, growing the file if needed.
    ///
    /// The data is not durable until [`DiskFile::sync_data`] returns.
    ///
    /// # Arguments
    ///
    /// * `offset_bytes` - Where to write the first byte.
    /// * `data` - The bytes to write.
    ///
    /// # Errors
    ///
    /// Fails with the underlying error.
    async fn write_at(&mut self, offset_bytes: u64, data: Bytes) -> io::Result<()>;

    /// Makes every completed write durable.
    ///
    /// # Errors
    ///
    /// Fails with the underlying error. After a failed flush the file's durable content is unknown and
    /// the copy must be treated as suspect: the kernel may have dropped the dirty pages, so a retry can
    /// falsely succeed.
    async fn sync_data(&mut self) -> io::Result<()>;

    /// Cuts or zero-extends the file to a length and makes the new length durable.
    ///
    /// # Arguments
    ///
    /// * `length_bytes` - The new length of the file.
    ///
    /// # Errors
    ///
    /// Fails with the underlying error.
    async fn set_length(&mut self, length_bytes: u64) -> io::Result<()>;

    /// Returns the file's current length.
    ///
    /// # Returns
    ///
    /// The length in bytes, preallocated space included.
    ///
    /// # Errors
    ///
    /// Fails with the underlying error.
    async fn length_bytes(&self) -> io::Result<u64>;
}

#[cfg(test)]
#[path = "disk_test.rs"]
mod tests;
