//! A disk that records how it performs.
//!
//! # Why this file exists
//!
//! An append is acknowledged only after it is flushed to disk, so when append latency degrades the flush
//! latency of a replica is usually the cause (design, section 14, "Observability"). The replica's own
//! code must stay free of a metrics library, so that it runs unchanged in the simulator. It already
//! reaches the disk through an injected interface, [`Disk`]; this file wraps that interface and measures
//! there.
//!
//! # What it contains
//!
//! - [`InstrumentedDisk`]: a [`Disk`] that forwards to another one and records metrics.
//! - `InstrumentedDiskFile`: the same for the files it opens.
//! - `DiskMetrics`: the metrics both record.
//!
//! # How it works
//!
//! Every call is forwarded unchanged and its result is returned unchanged. On the way:
//!
//! - `bitacora_disk_flush_duration_seconds` observes every [`DiskFile::sync_data`], failed ones included.
//! - `bitacora_disk_written_bytes_total` counts the bytes handed to the disk by `write_at`, by the
//!   initial content of `create_file` and by `write_file_atomically`.
//! - `bitacora_disk_operation_failures_total{operation}` counts calls that returned an error. A file
//!   that is absent when it is opened or read is an answer, not a failure of the disk, and is not
//!   counted.
//! - `bitacora_disk_capacity_bytes` and `bitacora_disk_free_bytes` hold what the disk last reported about
//!   its space. The replica asks regularly, for admission and for its registration, so the gauges stay
//!   current without a timer of their own.
//!
//! # Invariants
//!
//! - The decorator never changes, delays or retries a call.
//! - A failed flush is counted and passed on; deciding what it means stays with the replica.
//!
//! # See also
//!
//! - `metrics.rs`: the naming scheme.
//! - `bitacora-segment/src/disk.rs`: the interface that is wrapped.

use std::io;
use std::sync::Arc;

use async_trait::async_trait;
use bitacora_segment::{Disk, DiskFile, DiskSpace};
use bytes::Bytes;
use prometheus_client::metrics::counter::Counter as LibraryCounter;
use prometheus_client::metrics::family::Family;
use tokio::time::Instant;

use crate::metrics::{Counter, DurationHistogram, Gauge, MetricsRegistry};

/// The label that names the disk operation that failed.
const OPERATION_LABEL: &str = "operation";

/// The label set of the failure counter.
type OperationLabels = [(&'static str, &'static str); 1];

/// What an instrumented disk and its files record.
#[derive(Clone, Debug)]
struct DiskMetrics {
    /// How long flushes took.
    flush_duration: DurationHistogram,
    /// How many bytes were handed to the disk.
    written_bytes: Counter,
    /// Calls that returned an error, by operation.
    operation_failures: Family<OperationLabels, LibraryCounter>,
    /// The total size of the disk, as last reported.
    capacity_bytes: Gauge,
    /// The unused part of the disk, as last reported.
    free_bytes: Gauge,
}

impl DiskMetrics {
    /// Counts one failed call of `operation` if `outcome` is an error, and passes `outcome` on.
    fn count_failure<Value>(&self, operation: &'static str, outcome: io::Result<Value>) -> io::Result<Value> {
        if outcome.is_err() {
            self.operation_failures
                .get_or_create(&[(OPERATION_LABEL, operation)])
                .inc();
        }
        outcome
    }

    /// Like [`DiskMetrics::count_failure`], for the calls where an absent file is an expected answer.
    fn count_failure_unless_absent<Value>(
        &self,
        operation: &'static str,
        outcome: io::Result<Value>,
    ) -> io::Result<Value> {
        match &outcome {
            Err(error) if error.kind() == io::ErrorKind::NotFound => outcome,
            _other => self.count_failure(operation, outcome),
        }
    }
}

/// A [`Disk`] that forwards every call to another disk and records metrics about them.
///
/// A daemon wraps its real disk in one before handing it to the replica.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use bitacora_process::{InstrumentedDisk, MetricsRegistry};
/// use bitacora_segment::{Disk, MemoryDisk};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> std::io::Result<()> {
/// let registry = MetricsRegistry::new();
/// let disk = InstrumentedDisk::new(Arc::new(MemoryDisk::new()), &registry);
///
/// disk.write_file_atomically("replica.id", "replica-a".into()).await?;
///
/// assert!(registry.encode().contains("bitacora_disk_written_bytes_total 9"));
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct InstrumentedDisk {
    /// The disk that does the work.
    disk: Arc<dyn Disk>,
    /// What is recorded about it.
    metrics: DiskMetrics,
}

impl InstrumentedDisk {
    /// Wraps `disk` and registers the disk metrics in `registry`.
    ///
    /// # Arguments
    ///
    /// * `disk` - The disk that does the work.
    /// * `registry` - The registry of the process. Wrap only one disk per registry: a second one would
    ///   register the same metric names again.
    #[must_use]
    pub fn new(disk: Arc<dyn Disk>, registry: &MetricsRegistry) -> Self {
        let operation_failures = Family::<OperationLabels, LibraryCounter>::default();
        registry.register_family(
            "disk_operation_failures",
            "Disk operations that returned an error, by operation",
            operation_failures.clone(),
        );
        let metrics = DiskMetrics {
            flush_duration: registry
                .duration_histogram("disk_flush_duration", "How long making written data durable took"),
            written_bytes: registry.bytes_counter("disk_written", "Bytes handed to the disk"),
            operation_failures,
            capacity_bytes: registry.bytes_gauge("disk_capacity", "Total size of the disk, as last reported"),
            free_bytes: registry.bytes_gauge("disk_free", "Unused part of the disk, as last reported"),
        };
        Self { disk, metrics }
    }

    /// Wraps a file of the inner disk so that its writes and flushes are recorded too.
    fn instrument_file(&self, file: Box<dyn DiskFile>) -> Box<dyn DiskFile> {
        Box::new(InstrumentedDiskFile {
            file,
            metrics: self.metrics.clone(),
        })
    }
}

#[async_trait]
impl Disk for InstrumentedDisk {
    async fn create_file(
        &self,
        path: &str,
        initial_content: Bytes,
        preallocated_bytes: u64,
    ) -> io::Result<Box<dyn DiskFile>> {
        let initial_bytes = initial_content.len() as u64;
        let created = self.disk.create_file(path, initial_content, preallocated_bytes).await;
        let file = self.metrics.count_failure("create_file", created)?;
        self.metrics.written_bytes.add(initial_bytes);
        Ok(self.instrument_file(file))
    }

    async fn open_file(&self, path: &str) -> io::Result<Box<dyn DiskFile>> {
        let opened = self.disk.open_file(path).await;
        let file = self.metrics.count_failure_unless_absent("open_file", opened)?;
        Ok(self.instrument_file(file))
    }

    async fn write_file_atomically(&self, path: &str, contents: Bytes) -> io::Result<()> {
        let written_bytes = contents.len() as u64;
        let written = self.disk.write_file_atomically(path, contents).await;
        self.metrics.count_failure("write_file_atomically", written)?;
        self.metrics.written_bytes.add(written_bytes);
        Ok(())
    }

    async fn read_file(&self, path: &str) -> io::Result<Option<Bytes>> {
        let read = self.disk.read_file(path).await;
        self.metrics.count_failure("read_file", read)
    }

    async fn remove_file(&self, path: &str) -> io::Result<()> {
        let removed = self.disk.remove_file(path).await;
        self.metrics.count_failure("remove_file", removed)
    }

    async fn rename_file(&self, from_path: &str, to_path: &str) -> io::Result<()> {
        let renamed = self.disk.rename_file(from_path, to_path).await;
        self.metrics.count_failure("rename_file", renamed)
    }

    async fn list_files(&self) -> io::Result<Vec<String>> {
        let listed = self.disk.list_files().await;
        self.metrics.count_failure("list_files", listed)
    }

    async fn space(&self) -> io::Result<DiskSpace> {
        let measured = self.disk.space().await;
        let space = self.metrics.count_failure("space", measured)?;
        self.metrics.capacity_bytes.set_unsigned(space.capacity_bytes);
        self.metrics.free_bytes.set_unsigned(space.free_bytes);
        Ok(space)
    }
}

/// A [`DiskFile`] that forwards every call to a file of the inner disk and records metrics about them.
struct InstrumentedDiskFile {
    /// The file that does the work.
    file: Box<dyn DiskFile>,
    /// What is recorded about it, shared with the disk it came from.
    metrics: DiskMetrics,
}

#[async_trait]
impl DiskFile for InstrumentedDiskFile {
    async fn read_at(&self, offset_bytes: u64, length_bytes: usize) -> io::Result<Bytes> {
        let read = self.file.read_at(offset_bytes, length_bytes).await;
        self.metrics.count_failure("read_at", read)
    }

    async fn write_at(&mut self, offset_bytes: u64, data: Bytes) -> io::Result<()> {
        let written_bytes = data.len() as u64;
        let written = self.file.write_at(offset_bytes, data).await;
        self.metrics.count_failure("write_at", written)?;
        self.metrics.written_bytes.add(written_bytes);
        Ok(())
    }

    async fn sync_data(&mut self) -> io::Result<()> {
        let started_at = Instant::now();
        let flushed = self.file.sync_data().await;
        self.metrics.flush_duration.observe(started_at.elapsed());
        self.metrics.count_failure("sync_data", flushed)
    }

    async fn set_length(&mut self, length_bytes: u64) -> io::Result<()> {
        let resized = self.file.set_length(length_bytes).await;
        self.metrics.count_failure("set_length", resized)
    }

    async fn length_bytes(&self) -> io::Result<u64> {
        let measured = self.file.length_bytes().await;
        self.metrics.count_failure("length_bytes", measured)
    }
}

#[cfg(test)]
#[path = "instrumented_disk_test.rs"]
mod tests;
