//! The injected disk interface (design, section 15).
//!
//! Segment code never opens a file itself. It receives a [`Disk`], which is
//! the local file system in production ([`crate::LocalDisk`]) and an
//! in-memory model with crash semantics under test ([`crate::MemoryDisk`]).

use std::io;

use async_trait::async_trait;
use bytes::Bytes;

/// How much room a disk has.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiskSpace {
    /// The total size of the disk.
    pub capacity_bytes: u64,
    /// How much of it is still unused.
    pub free_bytes: u64,
}

impl DiskSpace {
    /// The share of the disk in use, in percent, rounded down.
    #[must_use]
    pub fn used_percent(self) -> u8 {
        if self.capacity_bytes == 0 {
            return 100;
        }
        let used_bytes = u128::from(self.capacity_bytes.saturating_sub(self.free_bytes));
        let percent = used_bytes * 100 / u128::from(self.capacity_bytes);
        u8::try_from(percent).expect("a share of the whole is at most 100")
    }
}

/// One data directory. Paths are relative to it and use `/` as separator.
#[async_trait]
pub trait Disk: Send + Sync + 'static {
    /// Creates a file that starts with `initial_content`, is zero-filled up
    /// to `preallocated_bytes`, and makes all of it durable.
    ///
    /// Creation is atomic: after a crash the file either does not exist or
    /// starts with `initial_content`. A file that exists under its name can
    /// therefore never be mistaken for one whose creation was interrupted.
    ///
    /// # Errors
    ///
    /// Fails with [`io::ErrorKind::AlreadyExists`] if the file exists, or
    /// with the underlying error.
    async fn create_file(
        &self,
        path: &str,
        initial_content: Bytes,
        preallocated_bytes: u64,
    ) -> io::Result<Box<dyn DiskFile>>;

    /// Opens an existing file for reading and writing.
    ///
    /// # Errors
    ///
    /// Fails with [`io::ErrorKind::NotFound`] if the file does not exist, or
    /// with the underlying error.
    async fn open_file(&self, path: &str) -> io::Result<Box<dyn DiskFile>>;

    /// Replaces the whole content of a small file. After it returns, a crash
    /// leaves either the old content or the new one, never a mix, and the new
    /// content is durable.
    ///
    /// # Errors
    ///
    /// Fails with the underlying error; the old content is then intact.
    async fn write_file_atomically(&self, path: &str, contents: Bytes) -> io::Result<()>;

    /// Reads the whole content of a small file, or `None` if it is absent.
    ///
    /// # Errors
    ///
    /// Fails with the underlying error.
    async fn read_file(&self, path: &str) -> io::Result<Option<Bytes>>;

    /// Removes a file durably. Removing an absent file succeeds.
    ///
    /// # Errors
    ///
    /// Fails with the underlying error.
    async fn remove_file(&self, path: &str) -> io::Result<()>;

    /// Lists every file on the disk, sorted by path.
    ///
    /// # Errors
    ///
    /// Fails with the underlying error.
    async fn list_files(&self) -> io::Result<Vec<String>>;

    /// Reports the disk's capacity and free space.
    ///
    /// # Errors
    ///
    /// Fails with the underlying error.
    async fn space(&self) -> io::Result<DiskSpace>;
}

/// One open file of a [`Disk`].
#[async_trait]
pub trait DiskFile: Send + Sync {
    /// Reads up to `length_bytes` starting at `offset_bytes`. The result is
    /// shorter only when the file ends first.
    ///
    /// # Errors
    ///
    /// Fails with the underlying error.
    async fn read_at(&self, offset_bytes: u64, length_bytes: usize) -> io::Result<Bytes>;

    /// Writes `data` at `offset_bytes`, growing the file if needed. The data
    /// is not durable until [`DiskFile::sync_data`] returns.
    ///
    /// # Errors
    ///
    /// Fails with the underlying error.
    async fn write_at(&mut self, offset_bytes: u64, data: Bytes) -> io::Result<()>;

    /// Makes every completed write durable.
    ///
    /// # Errors
    ///
    /// Fails with the underlying error. After a failed flush the file's
    /// durable content is unknown and the copy must be treated as suspect:
    /// the kernel may have dropped the dirty pages, so a retry can falsely
    /// succeed.
    async fn sync_data(&mut self) -> io::Result<()>;

    /// Cuts or zero-extends the file to `length_bytes` and makes the new
    /// length durable.
    ///
    /// # Errors
    ///
    /// Fails with the underlying error.
    async fn set_length(&mut self, length_bytes: u64) -> io::Result<()>;

    /// Returns the file's current length.
    ///
    /// # Errors
    ///
    /// Fails with the underlying error.
    async fn length_bytes(&self) -> io::Result<u64>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_disk_uses_zero_percent() {
        let space = DiskSpace {
            capacity_bytes: 1_000,
            free_bytes: 1_000,
        };
        assert_eq!(space.used_percent(), 0);
    }

    #[test]
    fn used_share_rounds_down() {
        let space = DiskSpace {
            capacity_bytes: 1_000,
            free_bytes: 149,
        };
        assert_eq!(space.used_percent(), 85);
    }

    #[test]
    fn disk_without_capacity_counts_as_full() {
        let space = DiskSpace {
            capacity_bytes: 0,
            free_bytes: 0,
        };
        assert_eq!(space.used_percent(), 100);
    }

    #[test]
    fn huge_disk_does_not_overflow() {
        let space = DiskSpace {
            capacity_bytes: u64::MAX,
            free_bytes: 0,
        };
        assert_eq!(space.used_percent(), 100);
    }
}
