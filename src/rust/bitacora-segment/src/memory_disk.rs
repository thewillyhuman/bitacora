//! An in-memory [`Disk`] that models what a crash does to unflushed data.
//!
//! # Why this file exists
//!
//! The one promise of a replica is that what it acknowledged survives a power loss. Testing that promise
//! needs a disk on which a test can cut the power at an exact point and then look at what is left, and
//! that can leave behind exactly the damage real disks leave: nothing of the last writes, a part of them,
//! a part with a mangled last byte. A real file system offers none of this. This file is a disk that does:
//! it keeps, for every file, both what the process sees and what the platter holds, and lets a test decide
//! what a crash does with the difference. The unit tests of this crate, the tests of every crate above it
//! and the deterministic simulator all run on it.
//!
//! # What it contains
//!
//! - [`MemoryDisk`]: the disk. Clones share the same files, like two handles to one device.
//! - [`CrashMode`]: what a crash leaves behind, as data.
//! - [`DEFAULT_MEMORY_DISK_CAPACITY_BYTES`]: the capacity a disk reports unless told otherwise.
//!
//! # How it works
//!
//! Every file has two contents: the **visible** one, which reads return and writes change (the page
//! cache), and the **durable** one, which a crash falls back to (the platter). A write changes only the
//! visible content. A flush copies visible to durable. Operations that the interface defines as atomic and
//! durable (creating a file, replacing a small file, renaming, setting the length) change both at once.
//!
//! [`MemoryDisk::crash_with`] replaces both contents of every file by what its [`CrashMode`] says
//! survived. Open handles stay usable and see the surviving content, like a process restarted on the same
//! disk.
//!
//! Three more switches inject faults that are not crashes: [`MemoryDisk::fail_flushes`] makes flushes
//! return an error and persist nothing, [`MemoryDisk::corrupt_byte`] flips bits that were already durable,
//! and the capacity can be set so that a test can fill the disk.
//!
//! Files live in a map sorted by path, so listing and crashing walk them in the same order on every run.
//! A crash that draws random numbers therefore draws them for the same files in the same order, which is
//! what keeps a simulated run replayable from its seed.
//!
//! # Invariants
//!
//! - The durable content of a file changes only through a successful flush, an operation the interface
//!   defines as durable, a crash, or [`MemoryDisk::corrupt_byte`].
//! - While flushes fail, nothing becomes durable through a flush or a rename.
//! - Every critical section finishes its change before anything in it can panic, so a lock that was held
//!   by a panicking test is still safe to take.
//!
//! # See also
//!
//! - `disk.rs`: the interface, and what each operation promises about crashes.
//! - `local_disk.rs`: the implementation on real files.
//! - `bitacora-testkit`: the in-process cluster, whose nodes each live on one of these.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use bitacora_types::DeterministicRandom;
use bytes::Bytes;

use crate::disk::{Disk, DiskFile, DiskSpace};

/// The capacity of a [`MemoryDisk`] unless a test sets another one: 1 TiB, far more than any test writes,
/// so that admission by free space never interferes unless a test asks for it.
pub const DEFAULT_MEMORY_DISK_CAPACITY_BYTES: u64 = 1 << 40;

/// The byte a file is filled with where nothing was ever written, as on a freshly allocated file.
const UNWRITTEN_BYTE: u8 = 0;

/// What a crash leaves on a [`MemoryDisk`].
///
/// A real disk that loses power keeps everything that was flushed. Of what was written but not flushed it
/// may keep nothing, any part, or a part whose last sector is garbage. Each variant is one of those
/// outcomes, applied to every file of the disk.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> std::io::Result<()> {
/// use bitacora_segment::{CrashMode, Disk, MemoryDisk};
/// use bytes::Bytes;
///
/// let disk = MemoryDisk::new();
/// let mut file = disk.create_file("7/3.seg", Bytes::from_static(b"flushed"), 0).await?;
/// file.write_at(7, Bytes::from_static(b"-unflushed")).await?;
///
/// // Power is lost after the first three unflushed bytes reached the platter.
/// disk.crash_with(CrashMode::KeepUnflushedBytes {
///     offset_bytes: 7,
///     length_bytes: 3,
/// });
///
/// assert_eq!(file.read_at(0, 64).await?, Bytes::from_static(b"flushed-un"));
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CrashMode {
    /// A clean power loss: every file is left as it was at its last flush, and everything written since
    /// is gone.
    LoseUnflushed,
    /// Every file keeps only the first bytes of what it had flushed, and nothing written since. The value
    /// is how many bytes survive; a file that had flushed less keeps all of it. This is a device that
    /// loses the end of what it had acknowledged, which a correct disk never does: it is for tests of what
    /// a replica makes of a file cut short.
    KeepFirstBytes(usize),
    /// A torn write at a known place: besides what was flushed, the bytes written in one range of each
    /// file reach the platter, and everything else written since the last flush is lost.
    KeepUnflushedBytes {
        /// Where the surviving range starts in each file.
        offset_bytes: usize,
        /// How long the surviving range is. It is cut at the end of what was written.
        length_bytes: usize,
    },
    /// A torn write at a random place. In every file whose path ends with `path_suffix` and that has
    /// unflushed writes, a random number of the unflushed bytes survive, in file order, and the last
    /// surviving one is damaged: every bit of it flipped. Other files lose their unflushed writes whole.
    ///
    /// The unflushed bytes of a file need not be one run. A segment file, for example, rewrites a mark in
    /// its header with every append. "In file order" means lower offsets survive first.
    TearUnflushedWrites {
        /// Decides, per file, how many unflushed bytes survive. One draw per torn file, in path order, so
        /// the same generator tears the same disk the same way.
        random: DeterministicRandom,
        /// Only files whose path ends with this are torn, for example `.seg`.
        path_suffix: String,
    },
}

impl CrashMode {
    /// Decides what one file holds after the crash.
    ///
    /// Takes `&mut self` because a mode that tears at random advances its generator.
    ///
    /// # Arguments
    ///
    /// * `path` - The file's path on the disk.
    /// * `durable` - The file's content at its last flush.
    /// * `visible` - The file's content when power was lost.
    ///
    /// # Returns
    ///
    /// The content found in the file on restart.
    fn surviving_content(&mut self, path: &str, durable: &[u8], visible: &[u8]) -> Vec<u8> {
        match self {
            Self::LoseUnflushed => durable.to_vec(),
            Self::KeepFirstBytes(kept_bytes) => durable[..durable.len().min(*kept_bytes)].to_vec(),
            Self::KeepUnflushedBytes {
                offset_bytes,
                length_bytes,
            } => keep_unflushed_bytes(durable, visible, *offset_bytes, *length_bytes),
            Self::TearUnflushedWrites { random, path_suffix } => {
                if path.ends_with(path_suffix.as_str()) {
                    tear_unflushed_writes(durable, visible, random)
                } else {
                    durable.to_vec()
                }
            }
        }
    }
}

/// Returns the flushed content of a file with one range of what was written since copied over it.
///
/// The range is cut at the end of the visible content, and the result is extended with unwritten bytes if
/// the range lies past the end of the durable content.
fn keep_unflushed_bytes(durable: &[u8], visible: &[u8], offset_bytes: usize, length_bytes: usize) -> Vec<u8> {
    let mut survived = durable.to_vec();
    let range_end = offset_bytes.saturating_add(length_bytes).min(visible.len());
    if offset_bytes >= range_end {
        return survived;
    }
    if survived.len() < range_end {
        survived.resize(range_end, UNWRITTEN_BYTE);
    }
    survived[offset_bytes..range_end].copy_from_slice(&visible[offset_bytes..range_end]);
    survived
}

/// Returns what a torn write leaves in one file: everything that was flushed, plus a random-length part
/// of what was written after it, whose last byte is damaged.
///
/// A file without unflushed writes is returned as it was flushed, and nothing is drawn for it.
fn tear_unflushed_writes(durable: &[u8], visible: &[u8], random: &mut DeterministicRandom) -> Vec<u8> {
    let unflushed_offsets = unflushed_offsets(durable, visible);
    if unflushed_offsets.is_empty() {
        return durable.to_vec();
    }
    let torn_position = random.index(unflushed_offsets.len());
    let torn_offset = unflushed_offsets[torn_position];

    let mut survived = durable.to_vec();
    if survived.len() <= torn_offset {
        survived.resize(torn_offset + 1, UNWRITTEN_BYTE);
    }
    for offset in &unflushed_offsets[..torn_position] {
        survived[*offset] = written_byte(visible, *offset);
    }
    survived[torn_offset] = !written_byte(visible, torn_offset);
    survived
}

/// Returns the offsets, in file order, at which the visible content of a file differs from its durable
/// content: the bytes written since the last flush. An offset past the end of one of the two counts.
fn unflushed_offsets(durable: &[u8], visible: &[u8]) -> Vec<usize> {
    let longest_bytes = durable.len().max(visible.len());
    let mut offsets = Vec::new();
    for offset in 0..longest_bytes {
        if durable.get(offset) != visible.get(offset) {
            offsets.push(offset);
        }
    }
    offsets
}

/// Returns the byte the process had written at `offset`, or an unwritten byte if the visible content ends
/// before it, as when the file was shortened without a flush.
fn written_byte(visible: &[u8], offset: usize) -> u8 {
    visible.get(offset).copied().unwrap_or(UNWRITTEN_BYTE)
}

/// One file of a [`MemoryDisk`]: its two contents.
#[derive(Clone, Debug, Default)]
struct MemoryFile {
    /// What reads see: every completed write.
    visible: Vec<u8>,
    /// What a crash leaves behind: the content at the last flush.
    durable: Vec<u8>,
}

/// Everything a [`MemoryDisk`] holds, behind its lock.
#[derive(Debug)]
struct MemoryDiskState {
    /// The files, by path. Sorted, so that listings and crashes walk them in the same order on every run.
    files_by_path: BTreeMap<String, MemoryFile>,
    /// The total size the disk reports, in bytes.
    capacity_bytes: u64,
    /// While set, every flush fails and persists nothing.
    are_flushes_failing: bool,
}

/// An in-memory disk. Clones share the same files, like two handles to one device.
///
/// A test keeps one clone to inject faults and gives another to the code under test.
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
/// let mut file = disk.create_file("7/3.seg", Bytes::new(), 0).await?;
/// file.write_at(0, Bytes::from_static(b"acknowledged")).await?;
/// file.sync_data().await?;
/// file.write_at(12, Bytes::from_static(b" and not yet")).await?;
///
/// disk.crash();
///
/// assert_eq!(file.read_at(0, 64).await?, Bytes::from_static(b"acknowledged"));
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct MemoryDisk {
    /// The files and switches, shared by every clone of this handle.
    state: Arc<Mutex<MemoryDiskState>>,
}

impl Default for MemoryDisk {
    /// Returns an empty disk of the default capacity, like [`MemoryDisk::new`].
    fn default() -> Self {
        Self::with_capacity(DEFAULT_MEMORY_DISK_CAPACITY_BYTES)
    }
}

impl MemoryDisk {
    /// Creates an empty disk of the default capacity.
    ///
    /// # Returns
    ///
    /// A disk that holds no file, reports [`DEFAULT_MEMORY_DISK_CAPACITY_BYTES`] of space, and has every
    /// fault switched off.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> std::io::Result<()> {
    /// use bitacora_segment::{Disk, MemoryDisk};
    ///
    /// let disk = MemoryDisk::new();
    /// assert!(disk.list_files().await?.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty disk that reports a given total size.
    ///
    /// The capacity is only reported, never enforced: a write past it succeeds. It exists so that a test
    /// can make a replica see a nearly full disk.
    ///
    /// # Arguments
    ///
    /// * `capacity_bytes` - The total size the disk reports.
    ///
    /// # Returns
    ///
    /// A disk that holds no file and has every fault switched off.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> std::io::Result<()> {
    /// use bitacora_segment::{Disk, MemoryDisk};
    /// use bytes::Bytes;
    ///
    /// let disk = MemoryDisk::with_capacity(1_000);
    /// disk.create_file("7/3.seg", Bytes::new(), 950).await?;
    ///
    /// assert_eq!(disk.space().await?.used_percent(), 95);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_capacity(capacity_bytes: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(MemoryDiskState {
                files_by_path: BTreeMap::new(),
                capacity_bytes,
                are_flushes_failing: false,
            })),
        }
    }

    /// Simulates a clean power loss: every file falls back to its content at its last flush.
    ///
    /// Open [`DiskFile`] handles stay usable and see that content, like a process restarted on the same
    /// disk. This is [`MemoryDisk::crash_with`] with [`CrashMode::LoseUnflushed`].
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
    /// file.write_at(6, Bytes::from_static(b"-never flushed")).await?;
    ///
    /// disk.crash();
    ///
    /// assert_eq!(file.read_at(0, 64).await?, Bytes::from_static(b"header"));
    /// # Ok(())
    /// # }
    /// ```
    pub fn crash(&self) {
        self.crash_with(CrashMode::LoseUnflushed);
    }

    /// Simulates a power loss that leaves on every file what `mode` says.
    ///
    /// Both contents of every file, visible and durable, become the surviving content, so open
    /// [`DiskFile`] handles see it too. Files are handled in path order. The switch set by
    /// [`MemoryDisk::fail_flushes`] is not touched.
    ///
    /// # Arguments
    ///
    /// * `mode` - What survives of each file. It is consumed: a mode that tears at random draws from its
    ///   generator and is not meant to be used for a second crash.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> std::io::Result<()> {
    /// use bitacora_segment::{CrashMode, Disk, MemoryDisk};
    /// use bytes::Bytes;
    ///
    /// let disk = MemoryDisk::new();
    /// disk.write_file_atomically("7/3.state", Bytes::from_static(b"fenced at 6"))
    ///     .await?;
    ///
    /// // A device that lost all but the first six bytes of what it had flushed.
    /// disk.crash_with(CrashMode::KeepFirstBytes(6));
    ///
    /// assert_eq!(disk.read_file("7/3.state").await?, Some(Bytes::from_static(b"fenced")));
    /// # Ok(())
    /// # }
    /// ```
    pub fn crash_with(&self, mode: CrashMode) {
        let mut mode = mode;
        for (path, file) in &mut self.lock().files_by_path {
            let survived = mode.surviving_content(path, &file.durable, &file.visible);
            file.visible.clone_from(&survived);
            file.durable = survived;
        }
    }

    /// Makes every flush fail, or succeed again.
    ///
    /// A failed flush persists nothing, like a disk that returns an I/O error from `fsync`. While the
    /// switch is on, [`DiskFile::sync_data`] and [`Disk::rename_file`] fail; operations that are atomic by
    /// themselves, such as creating a file, still succeed.
    ///
    /// # Arguments
    ///
    /// * `should_fail` - `true` to make flushes fail from now on, `false` to let them succeed again.
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
    /// let mut file = disk.create_file("7/3.seg", Bytes::new(), 0).await?;
    ///
    /// disk.fail_flushes(true);
    ///
    /// assert!(file.sync_data().await.is_err());
    /// # Ok(())
    /// # }
    /// ```
    pub fn fail_flushes(&self, should_fail: bool) {
        self.lock().are_flushes_failing = should_fail;
    }

    /// Silently corrupts one byte of a file, in both its visible and its durable content.
    ///
    /// This is bit rot: the damage is on the platter, and nobody is told. Does nothing if the file is
    /// absent or shorter than the offset.
    ///
    /// # Arguments
    ///
    /// * `path` - The file to damage.
    /// * `offset_bytes` - Which byte of it.
    /// * `flipped_bits` - The bits to flip in that byte: it is XOR-ed with this value.
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
    /// disk.write_file_atomically("7/3.state", Bytes::from_static(b"abc")).await?;
    ///
    /// disk.corrupt_byte("7/3.state", 1, 0x01);
    ///
    /// assert_eq!(disk.read_file("7/3.state").await?, Some(Bytes::from_static(b"acc")));
    /// # Ok(())
    /// # }
    /// ```
    pub fn corrupt_byte(&self, path: &str, offset_bytes: usize, flipped_bits: u8) {
        let mut state = self.lock();
        let Some(file) = state.files_by_path.get_mut(path) else {
            return;
        };
        for content in [&mut file.visible, &mut file.durable] {
            if let Some(damaged_byte) = content.get_mut(offset_bytes) {
                *damaged_byte ^= flipped_bits;
            }
        }
    }

    /// Returns what a clean crash right now would leave in a file.
    ///
    /// # Arguments
    ///
    /// * `path` - The file to look at.
    ///
    /// # Returns
    ///
    /// A copy of the file's content at its last flush, or `None` if the file does not exist.
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
    /// file.write_at(6, Bytes::from_static(b"-not flushed")).await?;
    ///
    /// assert_eq!(disk.durable_content("7/3.seg"), Some(b"header".to_vec()));
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn durable_content(&self, path: &str) -> Option<Vec<u8>> {
        let state = self.lock();
        let file = state.files_by_path.get(path)?;
        Some(file.durable.clone())
    }

    /// Locks the disk's state, whether or not another holder of the lock panicked.
    ///
    /// A panic while holding the lock cannot leave the maps half-updated: every critical section finishes
    /// its change before any call that can panic. Recovering the guard keeps one failed test from
    /// poisoning the rest.
    fn lock(&self) -> MutexGuard<'_, MemoryDiskState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Returns a handle to the file at `path`. The handle looks the file up on every use, so it fails once
    /// the file is removed.
    fn handle_to(&self, path: &str) -> Box<dyn DiskFile> {
        Box::new(MemoryDiskFile {
            disk: self.clone(),
            path: path.to_owned(),
        })
    }
}

/// The error for an operation on a file that does not exist.
fn not_found(path: &str) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, format!("no such file: {path}"))
}

/// The error a flush, or anything that needs one, fails with while [`MemoryDisk::fail_flushes`] is on.
fn injected_flush_failure() -> io::Error {
    io::Error::other("injected flush failure")
}

/// Converts a file offset or length to an index into a file's content.
///
/// Fails with [`io::ErrorKind::InvalidInput`] if the value does not fit in memory on this platform.
fn to_index(offset_bytes: u64) -> io::Result<usize> {
    match usize::try_from(offset_bytes) {
        Ok(index) => Ok(index),
        Err(_too_large) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("offset {offset_bytes} does not fit in memory"),
        )),
    }
}

/// Converts the length of a file's content to the `u64` the interface reports.
fn to_length_bytes(content: &[u8]) -> u64 {
    u64::try_from(content.len()).expect("lengths fit in 64 bits")
}

#[async_trait]
impl Disk for MemoryDisk {
    /// Inserts the file with both contents equal to the initial content, zero-filled to the preallocated
    /// size. It does not depend on a flush, so it succeeds while flushes fail.
    async fn create_file(
        &self,
        path: &str,
        initial_content: Bytes,
        preallocated_bytes: u64,
    ) -> io::Result<Box<dyn DiskFile>> {
        let mut content = initial_content.to_vec();
        let preallocated_length = to_index(preallocated_bytes)?;
        if content.len() < preallocated_length {
            content.resize(preallocated_length, UNWRITTEN_BYTE);
        }
        let mut state = self.lock();
        if state.files_by_path.contains_key(path) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("file exists: {path}"),
            ));
        }
        let created_file = MemoryFile {
            visible: content.clone(),
            durable: content,
        };
        state.files_by_path.insert(path.to_owned(), created_file);
        Ok(self.handle_to(path))
    }

    /// Returns a handle if the file exists.
    async fn open_file(&self, path: &str) -> io::Result<Box<dyn DiskFile>> {
        if !self.lock().files_by_path.contains_key(path) {
            return Err(not_found(path));
        }
        Ok(self.handle_to(path))
    }

    /// Sets both contents of the file at once, so a crash sees either the old file or the new one.
    async fn write_file_atomically(&self, path: &str, contents: Bytes) -> io::Result<()> {
        let replaced_file = MemoryFile {
            visible: contents.to_vec(),
            durable: contents.to_vec(),
        };
        self.lock().files_by_path.insert(path.to_owned(), replaced_file);
        Ok(())
    }

    /// Returns a copy of the visible content.
    async fn read_file(&self, path: &str) -> io::Result<Option<Bytes>> {
        let state = self.lock();
        let Some(file) = state.files_by_path.get(path) else {
            return Ok(None);
        };
        Ok(Some(Bytes::copy_from_slice(&file.visible)))
    }

    /// Drops the file from the map. Handles to it fail from then on.
    async fn remove_file(&self, path: &str) -> io::Result<()> {
        self.lock().files_by_path.remove(path);
        Ok(())
    }

    /// Moves the map entry, with both of its contents. Fails without effect while flushes fail.
    async fn rename_file(&self, from_path: &str, to_path: &str) -> io::Result<()> {
        let mut state = self.lock();
        // Making a move durable is a flush of the directory.
        if state.are_flushes_failing {
            return Err(injected_flush_failure());
        }
        let Some(moved_file) = state.files_by_path.remove(from_path) else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{from_path} does not exist"),
            ));
        };
        state.files_by_path.insert(to_path.to_owned(), moved_file);
        Ok(())
    }

    /// Returns the keys of the map, which are sorted.
    async fn list_files(&self) -> io::Result<Vec<String>> {
        Ok(self.lock().files_by_path.keys().cloned().collect())
    }

    /// Reports the configured capacity, less the visible size of every file.
    async fn space(&self) -> io::Result<DiskSpace> {
        let state = self.lock();
        let mut used_bytes = 0_u64;
        for file in state.files_by_path.values() {
            used_bytes += to_length_bytes(&file.visible);
        }
        Ok(DiskSpace {
            capacity_bytes: state.capacity_bytes,
            free_bytes: state.capacity_bytes.saturating_sub(used_bytes),
        })
    }
}

/// A handle to one file of a [`MemoryDisk`].
///
/// It holds a path, not the file: the file is looked up on every use, so the handle follows a crash and
/// fails once the file is removed.
#[derive(Debug)]
struct MemoryDiskFile {
    /// The disk the file is on.
    disk: MemoryDisk,
    /// The file's path on that disk.
    path: String,
}

impl MemoryDiskFile {
    /// Runs `operation` on the file under the disk's lock, failing with [`io::ErrorKind::NotFound`] if the
    /// file was removed meanwhile.
    fn with_file<Output>(&self, operation: impl FnOnce(&mut MemoryFile) -> io::Result<Output>) -> io::Result<Output> {
        let mut state = self.disk.lock();
        let Some(file) = state.files_by_path.get_mut(&self.path) else {
            return Err(not_found(&self.path));
        };
        operation(file)
    }
}

#[async_trait]
impl DiskFile for MemoryDiskFile {
    /// Copies the requested part of the visible content.
    async fn read_at(&self, offset_bytes: u64, length_bytes: usize) -> io::Result<Bytes> {
        let start = to_index(offset_bytes)?;
        self.with_file(|file| {
            let end = start.saturating_add(length_bytes).min(file.visible.len());
            let available = file.visible.get(start..end).unwrap_or_default();
            Ok(Bytes::copy_from_slice(available))
        })
    }

    /// Changes the visible content only.
    async fn write_at(&mut self, offset_bytes: u64, data: Bytes) -> io::Result<()> {
        let start = to_index(offset_bytes)?;
        self.with_file(|file| {
            let end = start + data.len();
            if file.visible.len() < end {
                file.visible.resize(end, UNWRITTEN_BYTE);
            }
            file.visible[start..end].copy_from_slice(&data);
            Ok(())
        })
    }

    /// Copies the visible content to the durable one, unless flushes are failing.
    async fn sync_data(&mut self) -> io::Result<()> {
        if self.disk.lock().are_flushes_failing {
            return Err(injected_flush_failure());
        }
        self.with_file(|file| {
            file.durable.clone_from(&file.visible);
            Ok(())
        })
    }

    /// Resizes the visible content and makes the whole of it durable, as the interface asks.
    async fn set_length(&mut self, length_bytes: u64) -> io::Result<()> {
        let new_length = to_index(length_bytes)?;
        self.with_file(|file| {
            file.visible.resize(new_length, UNWRITTEN_BYTE);
            file.durable.clone_from(&file.visible);
            Ok(())
        })
    }

    /// Returns the length of the visible content.
    async fn length_bytes(&self) -> io::Result<u64> {
        self.with_file(|file| Ok(to_length_bytes(&file.visible)))
    }
}

#[cfg(test)]
#[path = "memory_disk_test.rs"]
mod tests;
