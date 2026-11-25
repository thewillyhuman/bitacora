//! The replica: every segment copy on one disk, behind [`ReplicaApi`].

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use bitacora_segment::{Disk, DiskSpace};
use bitacora_types::replica_api::{
    AppendRequest, AppendResponse, FenceRequest, FenceResponse, ReadLimit, ReadRequest,
    ReadResponse, ReplicaApi, ReplicaError, ReplicaStatus, SealRequest, SealResponse,
    SegmentStatus,
};
use bitacora_types::{ReplicaId, SegmentAddress, SegmentId, StreamId};
use bytes::Bytes;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::actor::{SegmentHandle, WriteFailureHandler};
use crate::copy::{Admission, SegmentCopy};

/// The file that ties a data directory to its replica identity.
const REPLICA_ID_PATH: &str = "replica.id";

/// How a replica uses its disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaSettings {
    /// How much disk a new segment file reserves, and grows by when full.
    pub preallocated_segment_bytes: u64,
    /// From this share of used disk on, the replica accepts no new segments.
    pub refuse_new_segments_at_used_percent: u8,
    /// From this share of used disk on, the replica refuses every append,
    /// which makes writers roll elsewhere.
    pub refuse_appends_at_used_percent: u8,
    /// How long a measurement of the disk's free space is reused.
    pub space_refresh_interval: Duration,
    /// The longest a read may wait for records, whatever the reader asked.
    pub longest_read_wait: Duration,
}

impl Default for ReplicaSettings {
    fn default() -> Self {
        Self {
            preallocated_segment_bytes: 64 * 1024 * 1024,
            refuse_new_segments_at_used_percent: 85,
            refuse_appends_at_used_percent: 95,
            space_refresh_interval: Duration::from_secs(1),
            longest_read_wait: Duration::from_secs(30),
        }
    }
}

/// The segment tasks of a replica. Dropping it stops them all.
struct Registry {
    handles_by_address: BTreeMap<SegmentAddress, SegmentHandle>,
    tasks: JoinSet<()>,
}

/// One storage node: it appends batches to segment files, flushes them and
/// serves them back. It makes no decisions about ordering, membership or
/// leadership (design, section 7).
pub struct Replica {
    replica_id: ReplicaId,
    disk: Arc<dyn Disk>,
    settings: ReplicaSettings,
    registry: Mutex<Registry>,
    is_draining: AtomicBool,
    is_stopped: AtomicBool,
    last_space_measurement: Mutex<Option<(Instant, DiskSpace)>>,
    on_write_failure: WriteFailureHandler,
}

impl Replica {
    /// Opens the replica that lives on `disk` and loads every segment copy
    /// found there.
    ///
    /// The identity belongs to the data directory: a directory used before
    /// keeps the identifier it was first opened with, and
    /// `proposed_replica_id` is stored only on an empty one. A replica whose
    /// disk was wiped therefore returns under a new identity and can never
    /// claim copies it no longer holds.
    ///
    /// `on_write_failure` is called once per copy whose write or flush the
    /// disk refuses; the production handler stops the process.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaError::Storage`] if the disk cannot be listed or the
    /// identity cannot be read or stored.
    pub async fn open(
        disk: Arc<dyn Disk>,
        proposed_replica_id: ReplicaId,
        settings: ReplicaSettings,
        on_write_failure: WriteFailureHandler,
    ) -> Result<Self, ReplicaError> {
        let replica_id = adopt_identity(disk.as_ref(), proposed_replica_id).await?;
        let stored_paths = disk.list_files().await.map_err(storage_error("list"))?;
        let stored_addresses: Vec<SegmentAddress> = {
            let mut addresses: Vec<SegmentAddress> = stored_paths
                .iter()
                .filter_map(|path| parse_segment_path(path))
                .collect();
            addresses.sort_unstable();
            addresses.dedup();
            addresses
        };

        let mut registry = Registry {
            handles_by_address: BTreeMap::new(),
            tasks: JoinSet::new(),
        };
        for address in stored_addresses {
            let copy = SegmentCopy::load(
                address,
                Arc::clone(&disk),
                settings.preallocated_segment_bytes,
            )
            .await;
            let handle = SegmentHandle::spawn(
                address,
                copy,
                &mut registry.tasks,
                Arc::clone(&on_write_failure),
            );
            registry.handles_by_address.insert(address, handle);
        }
        tracing::info!(
            replica_id = %replica_id,
            segment_count = registry.handles_by_address.len(),
            "replica opened"
        );
        Ok(Self {
            replica_id,
            disk,
            settings,
            registry: Mutex::new(registry),
            is_draining: AtomicBool::new(false),
            is_stopped: AtomicBool::new(false),
            last_space_measurement: Mutex::new(None),
            on_write_failure,
        })
    }

    /// The identity of this replica's data directory.
    #[must_use]
    pub const fn replica_id(&self) -> &ReplicaId {
        &self.replica_id
    }

    /// Stops every segment task at once, as when the process is killed.
    ///
    /// Requests in flight get no answer, nothing more reaches the disk, and
    /// every later request fails as [`ReplicaError::Unavailable`]. What was
    /// flushed stays on the disk for the next [`Replica::open`].
    pub fn stop(&self) {
        self.is_stopped.store(true, Ordering::Relaxed);
        let mut registry = self.lock_registry();
        registry.tasks.abort_all();
        registry.handles_by_address.clear();
        tracing::info!(replica_id = %self.replica_id, "replica stopped");
    }

    /// Rereads every sealed copy and marks the ones that fail a checksum as
    /// damaged, so that the controller replaces them (design, section 7,
    /// "Integrity"). Returns how many copies are damaged afterwards.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaError::Unavailable`] if the replica is shutting down.
    pub async fn scrub_sealed_segments(&self) -> Result<usize, ReplicaError> {
        let mut damaged_count = 0;
        for handle in self.all_handles() {
            let is_sealed = handle
                .status()
                .await?
                .is_some_and(|status| status.sealed_end.is_some());
            if is_sealed {
                handle.verify().await?;
            }
            let is_damaged = handle
                .status()
                .await?
                .is_some_and(|status| status.is_damaged);
            damaged_count += usize::from(is_damaged);
        }
        Ok(damaged_count)
    }

    fn lock_registry(&self) -> MutexGuard<'_, Registry> {
        // Registry updates are single map operations, so a panic elsewhere
        // cannot leave the map half-updated.
        self.registry.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn all_handles(&self) -> Vec<SegmentHandle> {
        self.lock_registry()
            .handles_by_address
            .values()
            .cloned()
            .collect()
    }

    fn existing_handle(&self, address: SegmentAddress) -> Option<SegmentHandle> {
        self.lock_registry()
            .handles_by_address
            .get(&address)
            .cloned()
    }

    fn fail_if_stopped(&self) -> Result<(), ReplicaError> {
        if self.is_stopped.load(Ordering::Relaxed) {
            return Err(ReplicaError::Unavailable {
                reason: "the replica is stopped".to_owned(),
            });
        }
        Ok(())
    }

    /// Returns the task of the copy at `address`, starting one for a copy
    /// the replica has nothing stored for yet.
    fn handle_or_spawn(&self, address: SegmentAddress) -> Result<SegmentHandle, ReplicaError> {
        let mut registry = self.lock_registry();
        // Checked under the registry lock, so that no task can be started
        // after `stop` has aborted them all.
        self.fail_if_stopped()?;
        if let Some(handle) = registry.handles_by_address.get(&address) {
            return Ok(handle.clone());
        }
        let copy = SegmentCopy::absent(
            address,
            Arc::clone(&self.disk),
            self.settings.preallocated_segment_bytes,
        );
        let handle = SegmentHandle::spawn(
            address,
            copy,
            &mut registry.tasks,
            Arc::clone(&self.on_write_failure),
        );
        registry.handles_by_address.insert(address, handle.clone());
        Ok(handle)
    }

    /// Measures the disk's free space, reusing a recent measurement.
    async fn disk_space(&self) -> Result<DiskSpace, ReplicaError> {
        let now = Instant::now();
        let recent_measurement = self
            .last_space_measurement
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .filter(|(measured_at, _space)| {
                now.duration_since(*measured_at) < self.settings.space_refresh_interval
            });
        if let Some((_measured_at, space)) = recent_measurement {
            return Ok(space);
        }
        let space = self.disk.space().await.map_err(storage_error("measure"))?;
        *self
            .last_space_measurement
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some((now, space));
        Ok(space)
    }
}

/// Reads the identity stored on `disk`, or stores `proposed_replica_id` if
/// the disk has none.
async fn adopt_identity(
    disk: &dyn Disk,
    proposed_replica_id: ReplicaId,
) -> Result<ReplicaId, ReplicaError> {
    let stored_identity = disk
        .read_file(REPLICA_ID_PATH)
        .await
        .map_err(storage_error("read identity"))?;
    if let Some(stored_identity) = stored_identity {
        let identity_text = String::from_utf8(stored_identity.to_vec()).map_err(|utf8_error| {
            ReplicaError::Storage {
                reason: format!("{REPLICA_ID_PATH} is not text: {utf8_error}"),
            }
        })?;
        return ReplicaId::new(identity_text).map_err(|invalid| ReplicaError::Storage {
            reason: format!("{REPLICA_ID_PATH} is invalid: {invalid}"),
        });
    }
    disk.write_file_atomically(
        REPLICA_ID_PATH,
        Bytes::copy_from_slice(proposed_replica_id.as_str().as_bytes()),
    )
    .await
    .map_err(storage_error("store identity"))?;
    Ok(proposed_replica_id)
}

/// Recognizes `<stream_id>/<segment_id>.seg` and `.state` paths.
fn parse_segment_path(path: &str) -> Option<SegmentAddress> {
    let (stream_directory, file_name) = path.split_once('/')?;
    let segment_number = file_name
        .strip_suffix(".seg")
        .or_else(|| file_name.strip_suffix(".state"))?;
    Some(SegmentAddress::new(
        StreamId::new(stream_directory.parse().ok()?),
        SegmentId::new(segment_number.parse().ok()?),
    ))
}

fn storage_error(operation: &'static str) -> impl FnOnce(std::io::Error) -> ReplicaError {
    move |io_error| ReplicaError::Storage {
        reason: format!("cannot {operation} the data directory: {io_error}"),
    }
}

#[async_trait]
impl ReplicaApi for Replica {
    async fn append(&self, request: AppendRequest) -> Result<AppendResponse, ReplicaError> {
        if self.is_draining.load(Ordering::Relaxed) {
            return Err(ReplicaError::Draining);
        }
        let used_percent = self.disk_space().await?.used_percent();
        if used_percent >= self.settings.refuse_appends_at_used_percent {
            return Err(ReplicaError::NoCapacity { used_percent });
        }
        let admission = Admission {
            may_create_segments: used_percent < self.settings.refuse_new_segments_at_used_percent,
            used_percent,
        };
        self.handle_or_spawn(request.segment.address)?
            .append(request, admission)
            .await
    }

    async fn read(&self, request: ReadRequest) -> Result<ReadResponse, ReplicaError> {
        self.fail_if_stopped()?;
        let Some(handle) = self.existing_handle(request.segment) else {
            return Err(ReplicaError::SegmentNotFound {
                segment: request.segment,
            });
        };
        if let (Some(requested_wait), ReadLimit::DurableSequence) =
            (request.wait_for_records, request.limit)
        {
            let longest_wait = requested_wait.min(self.settings.longest_read_wait);
            handle
                .wait_until_readable(request.from_sequence, longest_wait)
                .await;
        }
        handle.read(request).await
    }

    async fn fence(&self, request: FenceRequest) -> Result<FenceResponse, ReplicaError> {
        self.handle_or_spawn(request.segment)?
            .fence(request.epoch)
            .await
    }

    async fn seal(&self, request: SealRequest) -> Result<SealResponse, ReplicaError> {
        self.handle_or_spawn(request.segment)?
            .seal(request.sealed_end)
            .await
    }

    async fn delete_segment(&self, segment: SegmentAddress) -> Result<(), ReplicaError> {
        self.fail_if_stopped()?;
        let Some(handle) = self.existing_handle(segment) else {
            return Ok(());
        };
        handle.delete().await?;
        self.lock_registry().handles_by_address.remove(&segment);
        tracing::info!(%segment, "segment copy deleted");
        Ok(())
    }

    async fn list_segments(&self) -> Result<Vec<SegmentStatus>, ReplicaError> {
        self.fail_if_stopped()?;
        let mut stored_copies = Vec::new();
        for handle in self.all_handles() {
            if let Some(status) = handle.status().await? {
                stored_copies.push(status);
            }
        }
        Ok(stored_copies)
    }

    async fn status(&self) -> Result<ReplicaStatus, ReplicaError> {
        self.fail_if_stopped()?;
        let space = self.disk.space().await.map_err(storage_error("measure"))?;
        let segment_count = self.list_segments().await?.len();
        Ok(ReplicaStatus {
            replica_id: self.replica_id.clone(),
            capacity_bytes: space.capacity_bytes,
            free_bytes: space.free_bytes,
            is_draining: self.is_draining.load(Ordering::Relaxed),
            segment_count: u64::try_from(segment_count).expect("counts fit in 64 bits"),
        })
    }

    async fn drain(&self) -> Result<(), ReplicaError> {
        self.fail_if_stopped()?;
        self.is_draining.store(true, Ordering::Relaxed);
        tracing::info!(replica_id = %self.replica_id, "replica is draining");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use bitacora_segment::MemoryDisk;
    use bitacora_segment::format::{Batch, decode_frames};
    use bitacora_types::replica_api::SegmentDescriptor;
    use bitacora_types::{Epoch, SequenceNumber};

    use super::*;

    const SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
    const OTHER_SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(8), SegmentId::new(1));
    const EPOCH: Epoch = Epoch::new(5);
    const FIRST_SEQUENCE: u64 = 100;

    fn small_segments() -> ReplicaSettings {
        ReplicaSettings {
            preallocated_segment_bytes: 1024,
            ..ReplicaSettings::default()
        }
    }

    fn ignore_write_failures() -> WriteFailureHandler {
        Arc::new(|_segment, _reason| {})
    }

    async fn open_replica(disk: &MemoryDisk, name: &str) -> Replica {
        Replica::open(
            Arc::new(disk.clone()),
            ReplicaId::new(name).expect("valid identifier"),
            small_segments(),
            ignore_write_failures(),
        )
        .await
        .expect("replica opens")
    }

    /// An append to `segment` of `record_count` records starting at
    /// `first_sequence`, declaring `durable_sequence` durable.
    fn append_to(
        segment: SegmentAddress,
        first_sequence: u64,
        record_count: u64,
        durable_sequence: u64,
    ) -> AppendRequest {
        let batch_frames = if record_count == 0 {
            Bytes::new()
        } else {
            let payloads: Vec<Bytes> = (first_sequence..first_sequence + record_count)
                .map(|sequence| Bytes::from(format!("record-{sequence}")))
                .collect();
            Batch::encode(segment, SequenceNumber::new(first_sequence), &payloads)
                .expect("valid batch")
                .frame()
                .clone()
        };
        AppendRequest {
            segment: SegmentDescriptor {
                address: segment,
                epoch: EPOCH,
                first_sequence: SequenceNumber::new(FIRST_SEQUENCE),
            },
            writer_epoch: EPOCH,
            batch_frames,
            durable_sequence: SequenceNumber::new(durable_sequence),
        }
    }

    fn read_from(from_sequence: u64, wait_for_records: Option<Duration>) -> ReadRequest {
        ReadRequest {
            segment: SEGMENT,
            from_sequence: SequenceNumber::new(from_sequence),
            max_bytes: usize::MAX,
            wait_for_records,
            limit: ReadLimit::DurableSequence,
        }
    }

    fn sequences_in(response: &ReadResponse) -> Vec<u64> {
        decode_frames(SEGMENT, &response.batch_frames)
            .expect("valid frames")
            .iter()
            .flat_map(Batch::records)
            .map(|record| record.sequence.get())
            .collect()
    }

    // ---- identity ----

    #[tokio::test]
    async fn empty_disk_adopts_the_proposed_identity() {
        let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
        assert_eq!(replica.replica_id().as_str(), "replica-a");
    }

    #[tokio::test]
    async fn used_disk_keeps_its_first_identity() {
        let disk = MemoryDisk::new();
        open_replica(&disk, "replica-a").await;
        let reopened = open_replica(&disk, "replica-b").await;
        assert_eq!(reopened.replica_id().as_str(), "replica-a");
    }

    #[tokio::test]
    async fn disk_with_an_unusable_identity_is_refused() {
        let disk = MemoryDisk::new();
        disk.write_file_atomically(REPLICA_ID_PATH, Bytes::from_static(b"not/valid"))
            .await
            .expect("write");
        let opened = Replica::open(
            Arc::new(disk),
            ReplicaId::new("replica-a").expect("valid identifier"),
            small_segments(),
            ignore_write_failures(),
        )
        .await;
        assert!(matches!(opened, Err(ReplicaError::Storage { .. })));
    }

    // ---- data path ----

    #[tokio::test]
    async fn appended_records_are_readable_once_durable() {
        let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
        replica
            .append(append_to(SEGMENT, 100, 5, 104))
            .await
            .expect("append");
        let response = replica.read(read_from(100, None)).await.expect("read");
        assert_eq!(sequences_in(&response), [100, 101, 102, 103, 104]);
    }

    #[tokio::test]
    async fn read_of_an_unknown_segment_finds_nothing() {
        let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
        assert_eq!(
            replica.read(read_from(100, None)).await,
            Err(ReplicaError::SegmentNotFound { segment: SEGMENT })
        );
    }

    #[tokio::test]
    async fn restarted_replica_still_holds_its_segments() {
        let disk = MemoryDisk::new();
        let replica = open_replica(&disk, "replica-a").await;
        replica
            .append(append_to(SEGMENT, 100, 5, 0))
            .await
            .expect("append");
        replica
            .fence(FenceRequest {
                segment: OTHER_SEGMENT,
                epoch: Epoch::new(9),
            })
            .await
            .expect("fence");
        drop(replica);
        disk.crash();

        let restarted = open_replica(&disk, "replica-a").await;
        let listed: Vec<(SegmentAddress, Option<SequenceNumber>, Epoch)> = restarted
            .list_segments()
            .await
            .expect("list")
            .iter()
            .map(|status| (status.address, status.local_end, status.fenced_epoch))
            .collect();
        assert_eq!(
            listed,
            [
                (SEGMENT, Some(SequenceNumber::new(104)), EPOCH),
                (OTHER_SEGMENT, None, Epoch::new(9)),
            ]
        );
    }

    #[tokio::test]
    async fn refused_append_does_not_list_a_segment() {
        let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
        let _refused = replica.append(append_to(SEGMENT, 105, 1, 0)).await;
        assert_eq!(replica.list_segments().await, Ok(Vec::new()));
    }

    // ---- waiting reads ----

    #[tokio::test(start_paused = true)]
    async fn waiting_read_returns_as_soon_as_records_become_durable() {
        let replica = Arc::new(open_replica(&MemoryDisk::new(), "replica-a").await);
        replica
            .append(append_to(SEGMENT, 100, 5, 0))
            .await
            .expect("append");
        let waiting_reader = {
            let replica = Arc::clone(&replica);
            tokio::spawn(async move {
                let started_at = Instant::now();
                let response = replica
                    .read(read_from(100, Some(Duration::from_secs(10))))
                    .await
                    .expect("read");
                (sequences_in(&response), started_at.elapsed())
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        replica
            .append(append_to(SEGMENT, 105, 0, 104))
            .await
            .expect("heartbeat");

        let (sequences, waited) = waiting_reader.await.expect("reader finishes");
        assert_eq!(
            (sequences, waited),
            (vec![100, 101, 102, 103, 104], Duration::from_millis(50))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn waiting_read_gives_up_empty_after_its_wait() {
        let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
        replica
            .append(append_to(SEGMENT, 100, 5, 0))
            .await
            .expect("append");
        let started_at = Instant::now();
        let response = replica
            .read(read_from(100, Some(Duration::from_secs(2))))
            .await
            .expect("read");
        assert_eq!(
            (response.batch_frames.len(), started_at.elapsed()),
            (0, Duration::from_secs(2))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn waiting_read_is_capped_by_the_replica() {
        let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
        replica
            .append(append_to(SEGMENT, 100, 5, 0))
            .await
            .expect("append");
        let started_at = Instant::now();
        replica
            .read(read_from(100, Some(Duration::from_secs(3_600))))
            .await
            .expect("read");
        assert_eq!(started_at.elapsed(), small_segments().longest_read_wait);
    }

    #[tokio::test(start_paused = true)]
    async fn waiting_read_at_the_end_of_a_sealed_segment_returns_at_once() {
        let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
        replica
            .append(append_to(SEGMENT, 100, 5, 0))
            .await
            .expect("append");
        replica
            .seal(SealRequest {
                segment: SEGMENT,
                sealed_end: SequenceNumber::new(104),
            })
            .await
            .expect("seal");
        let started_at = Instant::now();
        let response = replica
            .read(read_from(105, Some(Duration::from_secs(10))))
            .await
            .expect("read");
        assert_eq!(
            (response.sealed_end, started_at.elapsed()),
            (Some(SequenceNumber::new(104)), Duration::ZERO)
        );
    }

    // ---- admission ----

    /// Opens a replica on a disk of `capacity_bytes` that already holds one
    /// segment, with `used_bytes` taken up by an unrelated file.
    async fn replica_on_a_disk_with(capacity_bytes: u64, used_bytes: u64) -> Replica {
        let disk = MemoryDisk::with_capacity(capacity_bytes);
        let replica = open_replica(&disk, "replica-a").await;
        replica
            .append(append_to(SEGMENT, 100, 5, 0))
            .await
            .expect("append");
        disk.create_file("ballast", used_bytes)
            .await
            .expect("ballast");
        tokio::time::advance(small_segments().space_refresh_interval).await;
        replica
    }

    #[tokio::test(start_paused = true)]
    async fn disk_past_the_first_threshold_refuses_new_segments_only() {
        let replica = replica_on_a_disk_with(100_000, 88_000).await;
        let new_segment = replica.append(append_to(OTHER_SEGMENT, 100, 1, 0)).await;
        let existing_segment = replica.append(append_to(SEGMENT, 105, 1, 0)).await;
        assert!(
            matches!(new_segment, Err(ReplicaError::NoCapacity { .. })) && existing_segment.is_ok(),
            "new: {new_segment:?}, existing: {existing_segment:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn disk_past_the_second_threshold_refuses_every_append() {
        let replica = replica_on_a_disk_with(100_000, 96_000).await;
        let existing_segment = replica.append(append_to(SEGMENT, 105, 1, 0)).await;
        assert!(matches!(
            existing_segment,
            Err(ReplicaError::NoCapacity { used_percent }) if used_percent >= 95
        ));
    }

    #[tokio::test]
    async fn draining_replica_refuses_appends_but_serves_reads() {
        let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
        replica
            .append(append_to(SEGMENT, 100, 5, 104))
            .await
            .expect("append");
        replica.drain().await.expect("drain");
        let append = replica.append(append_to(SEGMENT, 105, 1, 0)).await;
        let read = replica.read(read_from(100, None)).await;
        assert!(append == Err(ReplicaError::Draining) && read.is_ok());
    }

    // ---- lifecycle ----

    #[tokio::test]
    async fn deleted_segment_is_gone() {
        let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
        replica
            .append(append_to(SEGMENT, 100, 5, 104))
            .await
            .expect("append");
        replica.delete_segment(SEGMENT).await.expect("delete");
        assert_eq!(
            (
                replica.list_segments().await,
                replica.read(read_from(100, None)).await
            ),
            (
                Ok(Vec::new()),
                Err(ReplicaError::SegmentNotFound { segment: SEGMENT })
            )
        );
    }

    #[tokio::test]
    async fn deleting_an_unknown_segment_succeeds() {
        let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
        assert_eq!(replica.delete_segment(SEGMENT).await, Ok(()));
    }

    #[tokio::test]
    async fn deleted_segment_can_be_written_again() {
        let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
        replica
            .append(append_to(SEGMENT, 100, 5, 0))
            .await
            .expect("append");
        replica.delete_segment(SEGMENT).await.expect("delete");
        let response = replica.append(append_to(SEGMENT, 100, 2, 0)).await;
        assert_eq!(
            response,
            Ok(AppendResponse {
                local_end: SequenceNumber::new(101)
            })
        );
    }

    #[tokio::test]
    async fn status_reports_identity_space_and_segments() {
        let replica = open_replica(&MemoryDisk::with_capacity(1_000_000), "replica-a").await;
        replica
            .append(append_to(SEGMENT, 100, 5, 0))
            .await
            .expect("append");
        let status = replica.status().await.expect("status");
        assert!(
            status.replica_id.as_str() == "replica-a"
                && status.capacity_bytes == 1_000_000
                && status.free_bytes < 1_000_000
                && !status.is_draining
                && status.segment_count == 1,
            "{status:?}"
        );
    }

    // ---- integrity ----

    #[tokio::test]
    async fn scrub_finds_a_corrupted_sealed_copy() {
        let disk = MemoryDisk::new();
        let replica = open_replica(&disk, "replica-a").await;
        replica
            .append(append_to(SEGMENT, 100, 5, 0))
            .await
            .expect("append");
        replica
            .seal(SealRequest {
                segment: SEGMENT,
                sealed_end: SequenceNumber::new(104),
            })
            .await
            .expect("seal");
        disk.corrupt_byte("7/3.seg", 100, 0x20);
        assert_eq!(replica.scrub_sealed_segments().await, Ok(1));
    }

    #[tokio::test]
    async fn scrub_of_intact_copies_finds_nothing() {
        let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
        replica
            .append(append_to(SEGMENT, 100, 5, 0))
            .await
            .expect("append");
        replica
            .seal(SealRequest {
                segment: SEGMENT,
                sealed_end: SequenceNumber::new(104),
            })
            .await
            .expect("seal");
        assert_eq!(replica.scrub_sealed_segments().await, Ok(0));
    }

    #[tokio::test]
    async fn failed_flush_is_reported_once_and_never_acknowledged() {
        let disk = MemoryDisk::new();
        let reported_failures = Arc::new(AtomicUsize::new(0));
        let count_failures: WriteFailureHandler = {
            let reported_failures = Arc::clone(&reported_failures);
            Arc::new(move |_segment, _reason| {
                reported_failures.fetch_add(1, Ordering::Relaxed);
            })
        };
        let replica = Replica::open(
            Arc::new(disk.clone()),
            ReplicaId::new("replica-a").expect("valid identifier"),
            small_segments(),
            count_failures,
        )
        .await
        .expect("replica opens");
        replica
            .append(append_to(SEGMENT, 100, 5, 0))
            .await
            .expect("append");

        disk.fail_flushes(true);
        let failed = replica.append(append_to(SEGMENT, 105, 1, 0)).await;
        disk.fail_flushes(false);
        let after_failure = replica.append(append_to(SEGMENT, 105, 1, 0)).await;

        assert!(
            matches!(failed, Err(ReplicaError::Storage { .. }))
                && matches!(after_failure, Err(ReplicaError::Storage { .. }))
                && reported_failures.load(Ordering::Relaxed) == 1,
            "failed: {failed:?}, after: {after_failure:?}"
        );
    }

    // ---- stopping ----

    #[tokio::test]
    async fn stopped_replica_refuses_every_request() {
        let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
        replica
            .append(append_to(SEGMENT, 100, 5, 104))
            .await
            .expect("append");
        replica.stop();
        let append = replica.append(append_to(SEGMENT, 105, 1, 0)).await;
        let read = replica.read(read_from(100, None)).await;
        let list = replica.list_segments().await;
        assert!(
            matches!(append, Err(ReplicaError::Unavailable { .. }))
                && matches!(read, Err(ReplicaError::Unavailable { .. }))
                && matches!(list, Err(ReplicaError::Unavailable { .. }))
        );
    }

    #[tokio::test]
    async fn stopped_replica_leaves_flushed_records_for_the_next_start() {
        let disk = MemoryDisk::new();
        let replica = open_replica(&disk, "replica-a").await;
        replica
            .append(append_to(SEGMENT, 100, 5, 0))
            .await
            .expect("append");
        replica.stop();
        disk.crash();
        let restarted = open_replica(&disk, "replica-a").await;
        let local_ends: Vec<_> = restarted
            .list_segments()
            .await
            .expect("list")
            .iter()
            .map(|status| status.local_end)
            .collect();
        assert_eq!(local_ends, [Some(SequenceNumber::new(104))]);
    }

    #[tokio::test(start_paused = true)]
    async fn request_waiting_on_a_stopped_replica_fails_instead_of_hanging() {
        let replica = Arc::new(open_replica(&MemoryDisk::new(), "replica-a").await);
        replica
            .append(append_to(SEGMENT, 100, 5, 0))
            .await
            .expect("append");
        let waiting_reader = {
            let replica = Arc::clone(&replica);
            tokio::spawn(async move {
                replica
                    .read(read_from(100, Some(Duration::from_secs(10))))
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        replica.stop();
        let answer = waiting_reader.await.expect("reader finishes");
        assert!(matches!(answer, Err(ReplicaError::Unavailable { .. })));
    }

    // ---- path parsing ----

    #[test]
    fn segment_and_state_paths_name_the_same_segment() {
        assert_eq!(
            (
                parse_segment_path("7/3.seg"),
                parse_segment_path("7/3.state")
            ),
            (Some(SEGMENT), Some(SEGMENT))
        );
    }

    #[test]
    fn unrelated_paths_are_not_segments() {
        for path in ["replica.id", "7/3.tmp", "x/3.seg", "7/y.seg", "7/3/4.seg"] {
            assert_eq!(parse_segment_path(path), None, "path {path:?}");
        }
    }
}
