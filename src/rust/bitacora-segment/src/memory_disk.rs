//! An in-memory [`Disk`] that models what a crash does to unflushed data.
//!
//! Every file has two contents: what reads see (the page cache) and what
//! survives a power loss (the platter). A write changes only the first; a
//! flush copies it to the second; [`MemoryDisk::crash`] throws the first
//! away. Tests and the simulator use this to check that nothing is
//! acknowledged before it is durable.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use bytes::Bytes;

use crate::disk::{Disk, DiskFile, DiskSpace};

/// The capacity of a [`MemoryDisk`] unless a test sets another one.
pub const DEFAULT_MEMORY_DISK_CAPACITY_BYTES: u64 = 1 << 40;

#[derive(Clone, Debug, Default)]
struct MemoryFile {
    /// What reads see: every completed write.
    visible: Vec<u8>,
    /// What a crash leaves behind: the content at the last flush.
    durable: Vec<u8>,
}

#[derive(Debug)]
struct MemoryDiskState {
    files_by_path: BTreeMap<String, MemoryFile>,
    capacity_bytes: u64,
    /// While set, every flush fails and persists nothing.
    are_flushes_failing: bool,
}

/// An in-memory disk. Clones share the same files, like two handles to one
/// device.
#[derive(Clone, Debug)]
pub struct MemoryDisk {
    state: Arc<Mutex<MemoryDiskState>>,
}

impl Default for MemoryDisk {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_MEMORY_DISK_CAPACITY_BYTES)
    }
}

impl MemoryDisk {
    /// Creates an empty disk of the default capacity.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty disk that reports `capacity_bytes` of total space.
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

    /// Simulates a power loss: every file falls back to its content at its
    /// last flush. Open [`DiskFile`] handles stay usable and see that
    /// content, like a process restarted on the same disk.
    pub fn crash(&self) {
        self.crash_keeping(|_path, durable, _visible| durable.to_vec());
    }

    /// Simulates a power loss where `surviving_content` decides, per file,
    /// what is found on restart. It receives the path, the content at the
    /// last flush and the content that was visible when power was lost.
    ///
    /// A real disk may have persisted any part of the unflushed writes, so a
    /// fault injector uses this to leave torn or partial tails behind.
    pub fn crash_keeping(&self, mut surviving_content: impl FnMut(&str, &[u8], &[u8]) -> Vec<u8>) {
        for (path, file) in &mut self.lock().files_by_path {
            let survived = surviving_content(path, &file.durable, &file.visible);
            file.visible.clone_from(&survived);
            file.durable = survived;
        }
    }

    /// Makes every flush fail, or succeed again. A failed flush persists
    /// nothing, like a disk that returns an I/O error from `fsync`.
    pub fn fail_flushes(&self, should_fail: bool) {
        self.lock().are_flushes_failing = should_fail;
    }

    /// Silently corrupts one byte, in both the visible and the durable
    /// content, by XOR-ing it with `flipped_bits`. Does nothing if the file
    /// is absent or shorter than `offset_bytes`.
    pub fn corrupt_byte(&self, path: &str, offset_bytes: usize, flipped_bits: u8) {
        if let Some(file) = self.lock().files_by_path.get_mut(path) {
            for content in [&mut file.visible, &mut file.durable] {
                if let Some(byte) = content.get_mut(offset_bytes) {
                    *byte ^= flipped_bits;
                }
            }
        }
    }

    /// Returns what a crash right now would leave in `path`, or `None` if
    /// the file does not exist.
    #[must_use]
    pub fn durable_content(&self, path: &str) -> Option<Vec<u8>> {
        self.lock().files_by_path.get(path).map(|file| file.durable.clone())
    }

    fn lock(&self) -> MutexGuard<'_, MemoryDiskState> {
        // A panic while holding the lock cannot leave the maps half-updated:
        // every critical section below finishes its change before any call
        // that can panic. Recovering the guard keeps one failed test from
        // poisoning the rest.
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn not_found(path: &str) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, format!("no such file: {path}"))
}

fn to_index(offset_bytes: u64) -> io::Result<usize> {
    usize::try_from(offset_bytes).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("offset {offset_bytes} does not fit in memory"),
        )
    })
}

#[async_trait]
impl Disk for MemoryDisk {
    async fn create_file(
        &self,
        path: &str,
        initial_content: Bytes,
        preallocated_bytes: u64,
    ) -> io::Result<Box<dyn DiskFile>> {
        let mut content = initial_content.to_vec();
        let preallocated_length = to_index(preallocated_bytes)?;
        if content.len() < preallocated_length {
            content.resize(preallocated_length, 0);
        }
        let mut state = self.lock();
        if state.files_by_path.contains_key(path) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("file exists: {path}"),
            ));
        }
        state.files_by_path.insert(
            path.to_owned(),
            MemoryFile {
                visible: content.clone(),
                durable: content,
            },
        );
        Ok(Box::new(MemoryDiskFile {
            disk: self.clone(),
            path: path.to_owned(),
        }))
    }

    async fn open_file(&self, path: &str) -> io::Result<Box<dyn DiskFile>> {
        if !self.lock().files_by_path.contains_key(path) {
            return Err(not_found(path));
        }
        Ok(Box::new(MemoryDiskFile {
            disk: self.clone(),
            path: path.to_owned(),
        }))
    }

    async fn write_file_atomically(&self, path: &str, contents: Bytes) -> io::Result<()> {
        self.lock().files_by_path.insert(
            path.to_owned(),
            MemoryFile {
                visible: contents.to_vec(),
                durable: contents.to_vec(),
            },
        );
        Ok(())
    }

    async fn read_file(&self, path: &str) -> io::Result<Option<Bytes>> {
        Ok(self
            .lock()
            .files_by_path
            .get(path)
            .map(|file| Bytes::copy_from_slice(&file.visible)))
    }

    async fn remove_file(&self, path: &str) -> io::Result<()> {
        self.lock().files_by_path.remove(path);
        Ok(())
    }

    async fn rename_file(&self, from_path: &str, to_path: &str) -> io::Result<()> {
        let mut state = self.lock();
        // Making a move durable is a flush of the directory.
        if state.are_flushes_failing {
            return Err(io::Error::other("injected flush failure"));
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

    async fn list_files(&self) -> io::Result<Vec<String>> {
        Ok(self.lock().files_by_path.keys().cloned().collect())
    }

    async fn space(&self) -> io::Result<DiskSpace> {
        let state = self.lock();
        let used_bytes: u64 = state
            .files_by_path
            .values()
            .map(|file| u64::try_from(file.visible.len()).expect("lengths fit in 64 bits"))
            .sum();
        Ok(DiskSpace {
            capacity_bytes: state.capacity_bytes,
            free_bytes: state.capacity_bytes.saturating_sub(used_bytes),
        })
    }
}

/// A handle to one file of a [`MemoryDisk`].
#[derive(Debug)]
struct MemoryDiskFile {
    disk: MemoryDisk,
    path: String,
}

impl MemoryDiskFile {
    /// Runs `operation` on the file, failing if it was removed meanwhile.
    fn with_file<Output>(&self, operation: impl FnOnce(&mut MemoryFile) -> io::Result<Output>) -> io::Result<Output> {
        let mut state = self.disk.lock();
        let file = state
            .files_by_path
            .get_mut(&self.path)
            .ok_or_else(|| not_found(&self.path))?;
        operation(file)
    }
}

#[async_trait]
impl DiskFile for MemoryDiskFile {
    async fn read_at(&self, offset_bytes: u64, length_bytes: usize) -> io::Result<Bytes> {
        let start = to_index(offset_bytes)?;
        self.with_file(|file| {
            let end = start.saturating_add(length_bytes).min(file.visible.len());
            let available = file.visible.get(start..end).unwrap_or_default();
            Ok(Bytes::copy_from_slice(available))
        })
    }

    async fn write_at(&mut self, offset_bytes: u64, data: Bytes) -> io::Result<()> {
        let start = to_index(offset_bytes)?;
        self.with_file(|file| {
            let end = start + data.len();
            if file.visible.len() < end {
                file.visible.resize(end, 0);
            }
            file.visible[start..end].copy_from_slice(&data);
            Ok(())
        })
    }

    async fn sync_data(&mut self) -> io::Result<()> {
        if self.disk.lock().are_flushes_failing {
            return Err(io::Error::other("injected flush failure"));
        }
        self.with_file(|file| {
            file.durable.clone_from(&file.visible);
            Ok(())
        })
    }

    async fn set_length(&mut self, length_bytes: u64) -> io::Result<()> {
        let new_length = to_index(length_bytes)?;
        self.with_file(|file| {
            file.visible.resize(new_length, 0);
            file.durable.clone_from(&file.visible);
            Ok(())
        })
    }

    async fn length_bytes(&self) -> io::Result<u64> {
        self.with_file(|file| Ok(u64::try_from(file.visible.len()).expect("lengths fit in 64 bits")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PATH: &str = "7/3.seg";

    async fn file_with(disk: &MemoryDisk, flushed: &'static [u8]) -> Box<dyn DiskFile> {
        let mut file = disk.create_file(PATH, Bytes::new(), 0).await.expect("new file");
        file.write_at(0, Bytes::from_static(flushed)).await.expect("write");
        file.sync_data().await.expect("flush");
        file
    }

    #[tokio::test]
    async fn new_file_is_zero_filled_to_its_preallocated_size() {
        let disk = MemoryDisk::new();
        let file = disk.create_file(PATH, Bytes::new(), 4).await.expect("new file");
        assert_eq!(file.read_at(0, 16).await.expect("read"), Bytes::from_static(&[0; 4]));
    }

    #[tokio::test]
    async fn new_file_starts_with_its_initial_content_even_after_a_crash() {
        let disk = MemoryDisk::new();
        let file = disk
            .create_file(PATH, Bytes::from_static(b"header"), 8)
            .await
            .expect("new file");
        disk.crash();
        assert_eq!(
            file.read_at(0, 16).await.expect("read"),
            Bytes::from_static(b"header\0\0")
        );
    }

    #[tokio::test]
    async fn creation_does_not_depend_on_a_flush() {
        let disk = MemoryDisk::new();
        disk.fail_flushes(true);
        let created = disk.create_file(PATH, Bytes::from_static(b"header"), 0).await;
        assert!(created.is_ok());
    }

    #[tokio::test]
    async fn creating_an_existing_file_fails() {
        let disk = MemoryDisk::new();
        disk.create_file(PATH, Bytes::new(), 0).await.expect("new file");
        let second_attempt = disk.create_file(PATH, Bytes::new(), 0).await;
        assert_eq!(
            second_attempt.err().map(|error| error.kind()),
            Some(io::ErrorKind::AlreadyExists)
        );
    }

    #[tokio::test]
    async fn opening_a_missing_file_fails() {
        let disk = MemoryDisk::new();
        let opened = disk.open_file(PATH).await;
        assert_eq!(opened.err().map(|error| error.kind()), Some(io::ErrorKind::NotFound));
    }

    #[tokio::test]
    async fn written_bytes_are_visible_before_a_flush() {
        let disk = MemoryDisk::new();
        let mut file = disk.create_file(PATH, Bytes::new(), 0).await.expect("new file");
        file.write_at(2, Bytes::from_static(b"ab")).await.expect("write");
        assert_eq!(file.read_at(0, 16).await.expect("read"), Bytes::from_static(b"\0\0ab"));
    }

    #[tokio::test]
    async fn crash_drops_bytes_that_were_not_flushed() {
        let disk = MemoryDisk::new();
        let mut file = file_with(&disk, b"flushed").await;
        file.write_at(7, Bytes::from_static(b"-lost")).await.expect("write");
        disk.crash();
        assert_eq!(file.read_at(0, 64).await.expect("read"), Bytes::from_static(b"flushed"));
    }

    #[tokio::test]
    async fn crash_keeps_bytes_that_were_flushed() {
        let disk = MemoryDisk::new();
        let mut file = file_with(&disk, b"first").await;
        file.write_at(5, Bytes::from_static(b"-second")).await.expect("write");
        file.sync_data().await.expect("flush");
        disk.crash();
        assert_eq!(
            file.read_at(0, 64).await.expect("read"),
            Bytes::from_static(b"first-second")
        );
    }

    #[tokio::test]
    async fn crash_can_leave_a_torn_tail_behind() {
        let disk = MemoryDisk::new();
        let mut file = file_with(&disk, b"flushed").await;
        file.write_at(7, Bytes::from_static(b"-unflushed"))
            .await
            .expect("write");
        disk.crash_keeping(|_path, durable, visible| visible[..durable.len() + 3].to_vec());
        assert_eq!(
            file.read_at(0, 64).await.expect("read"),
            Bytes::from_static(b"flushed-un")
        );
    }

    #[tokio::test]
    async fn failing_flush_reports_an_error_and_persists_nothing() {
        let disk = MemoryDisk::new();
        let mut file = file_with(&disk, b"flushed").await;
        file.write_at(7, Bytes::from_static(b"-lost")).await.expect("write");
        disk.fail_flushes(true);
        let flushed = file.sync_data().await;
        disk.crash();
        assert!(flushed.is_err() && file.read_at(0, 64).await.expect("read") == Bytes::from_static(b"flushed"));
    }

    #[tokio::test]
    async fn flushes_work_again_once_the_failure_is_lifted() {
        let disk = MemoryDisk::new();
        let mut file = file_with(&disk, b"flushed").await;
        disk.fail_flushes(true);
        disk.fail_flushes(false);
        assert!(file.sync_data().await.is_ok());
    }

    #[tokio::test]
    async fn atomic_write_survives_a_crash() {
        let disk = MemoryDisk::new();
        disk.write_file_atomically("7/3.state", Bytes::from_static(b"state"))
            .await
            .expect("write");
        disk.crash();
        assert_eq!(
            disk.read_file("7/3.state").await.expect("read"),
            Some(Bytes::from_static(b"state"))
        );
    }

    #[tokio::test]
    async fn renamed_file_is_found_under_its_new_name_only() {
        let disk = MemoryDisk::new();
        disk.write_file_atomically("7/3.seg.repair", Bytes::from_static(b"new copy"))
            .await
            .expect("write");

        disk.rename_file("7/3.seg.repair", "7/3.seg").await.expect("rename");

        assert_eq!(
            disk.read_file("7/3.seg").await.expect("read"),
            Some(Bytes::from_static(b"new copy"))
        );
        assert_eq!(disk.read_file("7/3.seg.repair").await.expect("read"), None);
    }

    #[tokio::test]
    async fn rename_replaces_the_file_that_was_there() {
        let disk = MemoryDisk::new();
        disk.write_file_atomically("7/3.seg", Bytes::from_static(b"old copy"))
            .await
            .expect("write");
        disk.write_file_atomically("7/3.seg.repair", Bytes::from_static(b"new copy"))
            .await
            .expect("write");

        disk.rename_file("7/3.seg.repair", "7/3.seg").await.expect("rename");

        assert_eq!(
            disk.read_file("7/3.seg").await.expect("read"),
            Some(Bytes::from_static(b"new copy"))
        );
    }

    #[tokio::test]
    async fn rename_survives_a_crash() {
        let disk = MemoryDisk::new();
        disk.write_file_atomically("7/3.seg.repair", Bytes::from_static(b"new copy"))
            .await
            .expect("write");
        disk.rename_file("7/3.seg.repair", "7/3.seg").await.expect("rename");

        disk.crash();

        assert_eq!(
            disk.read_file("7/3.seg").await.expect("read"),
            Some(Bytes::from_static(b"new copy"))
        );
    }

    #[tokio::test]
    async fn rename_of_a_missing_file_fails_and_leaves_the_target_alone() {
        let disk = MemoryDisk::new();
        disk.write_file_atomically("7/3.seg", Bytes::from_static(b"old copy"))
            .await
            .expect("write");

        let renamed = disk.rename_file("7/3.seg.repair", "7/3.seg").await;

        assert_eq!(renamed.map_err(|error| error.kind()), Err(io::ErrorKind::NotFound));
        assert_eq!(
            disk.read_file("7/3.seg").await.expect("read"),
            Some(Bytes::from_static(b"old copy"))
        );
    }

    #[tokio::test]
    async fn rename_fails_without_effect_while_flushes_fail() {
        let disk = MemoryDisk::new();
        disk.write_file_atomically("7/3.seg", Bytes::from_static(b"old copy"))
            .await
            .expect("write");
        disk.write_file_atomically("7/3.seg.repair", Bytes::from_static(b"new copy"))
            .await
            .expect("write");
        disk.fail_flushes(true);

        let renamed = disk.rename_file("7/3.seg.repair", "7/3.seg").await;

        assert!(renamed.is_err());
        assert_eq!(
            disk.read_file("7/3.seg").await.expect("read"),
            Some(Bytes::from_static(b"old copy"))
        );
    }

    #[tokio::test]
    async fn reading_a_missing_small_file_returns_none() {
        let disk = MemoryDisk::new();
        assert_eq!(disk.read_file("7/3.state").await.expect("read"), None);
    }

    #[tokio::test]
    async fn removed_file_is_gone_and_its_handles_fail() {
        let disk = MemoryDisk::new();
        let file = file_with(&disk, b"content").await;
        disk.remove_file(PATH).await.expect("remove");
        assert_eq!(
            file.read_at(0, 1).await.err().map(|error| error.kind()),
            Some(io::ErrorKind::NotFound)
        );
    }

    #[tokio::test]
    async fn removing_a_missing_file_succeeds() {
        let disk = MemoryDisk::new();
        assert!(disk.remove_file(PATH).await.is_ok());
    }

    #[tokio::test]
    async fn files_are_listed_sorted_by_path() {
        let disk = MemoryDisk::new();
        for path in ["7/3.seg", "2/9.seg", "7/1.seg"] {
            disk.create_file(path, Bytes::new(), 0).await.expect("new file");
        }
        assert_eq!(
            disk.list_files().await.expect("list"),
            ["2/9.seg", "7/1.seg", "7/3.seg"]
        );
    }

    #[tokio::test]
    async fn free_space_shrinks_by_what_files_occupy() {
        let disk = MemoryDisk::with_capacity(1_000);
        disk.create_file(PATH, Bytes::new(), 400).await.expect("new file");
        assert_eq!(
            disk.space().await.expect("space"),
            DiskSpace {
                capacity_bytes: 1_000,
                free_bytes: 600
            }
        );
    }

    #[tokio::test]
    async fn shrinking_a_file_cuts_its_content_durably() {
        let disk = MemoryDisk::new();
        let mut file = file_with(&disk, b"keep-drop").await;
        file.set_length(4).await.expect("shrink");
        disk.crash();
        assert_eq!(file.read_at(0, 64).await.expect("read"), Bytes::from_static(b"keep"));
    }

    #[tokio::test]
    async fn corruption_reaches_the_durable_content() {
        let disk = MemoryDisk::new();
        let _file = file_with(&disk, b"abc").await;
        disk.corrupt_byte(PATH, 1, 0xFF);
        assert_eq!(disk.durable_content(PATH), Some(vec![b'a', b'b' ^ 0xFF, b'c']));
    }
}
