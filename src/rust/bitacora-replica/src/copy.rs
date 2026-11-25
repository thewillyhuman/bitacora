//! One replica's copy of one segment, and the rules it enforces.
//!
//! [`SegmentCopy`] is where the replica side of the protocol lives: which
//! appends are accepted, what fencing and sealing do, and how far a read may
//! go. It owns its files exclusively and is driven by one task
//! (see `actor`), so every method runs to completion before the next starts.

use std::sync::Arc;

use bitacora_segment::format::{Batch, SegmentHeader, decode_frames};
use bitacora_segment::{Disk, SegmentError, SegmentFile, SegmentState};
use bitacora_types::replica_api::{
    AppendRequest, AppendResponse, FenceResponse, ReadLimit, ReadRequest, ReadResponse,
    ReplicaError, SealResponse, SegmentStatus,
};
use bitacora_types::{Epoch, SegmentAddress, SequenceNumber};
use bytes::Bytes;

/// The path of a segment's data file on the replica's disk.
pub(crate) fn segment_file_path(address: SegmentAddress) -> String {
    format!("{}/{}.seg", address.stream_id, address.segment_id)
}

/// The path of a segment's fencing and sealing state on the replica's disk.
pub(crate) fn segment_state_path(address: SegmentAddress) -> String {
    format!("{}/{}.state", address.stream_id, address.segment_id)
}

/// Whether the replica's disk has room for what an append needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Admission {
    /// Whether a new segment file may be created.
    pub may_create_segments: bool,
    /// How full the disk is, to report when creation is refused.
    pub used_percent: u8,
}

/// One replica's copy of one segment.
pub(crate) struct SegmentCopy {
    address: SegmentAddress,
    disk: Arc<dyn Disk>,
    preallocated_bytes: u64,
    /// The data file. Absent while the replica only holds fencing or sealing
    /// state for a segment it never received records for.
    file: Option<SegmentFile>,
    state: SegmentState,
    /// Whether `state` has ever been written to the sidecar file.
    has_stored_state: bool,
    /// The latest durable sequence heard from the writer. In memory only:
    /// after a restart the copy serves nothing until it hears again.
    known_durable: SequenceNumber,
    /// Set when a checksum or a disk operation failed. A damaged copy
    /// refuses everything and waits to be replaced.
    damage: Option<String>,
    /// Set when the disk refused a write or a flush. Unlike corruption found
    /// later, this means the process can no longer trust its page cache.
    has_failed_write: bool,
}

impl SegmentCopy {
    /// A copy the replica has nothing on disk for yet.
    pub(crate) fn absent(
        address: SegmentAddress,
        disk: Arc<dyn Disk>,
        preallocated_bytes: u64,
    ) -> Self {
        Self {
            address,
            disk,
            preallocated_bytes,
            file: None,
            state: SegmentState::default(),
            has_stored_state: false,
            known_durable: SequenceNumber::ZERO,
            damage: None,
            has_failed_write: false,
        }
    }

    /// Loads a copy from disk, as after a restart. A copy whose files cannot
    /// be read or verified is loaded as damaged, so that it is reported and
    /// replaced instead of silently forgotten.
    pub(crate) async fn load(
        address: SegmentAddress,
        disk: Arc<dyn Disk>,
        preallocated_bytes: u64,
    ) -> Self {
        let mut copy = Self::absent(address, Arc::clone(&disk), preallocated_bytes);

        match disk.read_file(&segment_state_path(address)).await {
            Ok(Some(stored_state)) => match SegmentState::decode(&stored_state) {
                Ok(state) => {
                    copy.state = state;
                    copy.has_stored_state = true;
                }
                Err(state_error) => copy.damage = Some(state_error.to_string()),
            },
            Ok(None) => {}
            Err(io_error) => copy.damage = Some(io_error.to_string()),
        }

        match SegmentFile::open(
            disk.as_ref(),
            &segment_file_path(address),
            address,
            preallocated_bytes,
        )
        .await
        {
            Ok((file, _scan_stop)) => {
                // Without a sidecar the copy was never fenced or sealed, so
                // the lowest epoch it accepts is the one that created it.
                copy.state.fenced_epoch = copy.state.fenced_epoch.max(file.header().epoch);
                copy.file = Some(file);
            }
            Err(SegmentError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(segment_error) => copy.damage = Some(segment_error.to_string()),
        }
        copy
    }

    /// Whether anything about this segment is stored on the replica's disk.
    pub(crate) const fn is_stored(&self) -> bool {
        self.file.is_some() || self.has_stored_state
    }

    /// The last sequence number readers may be served, if the copy has data.
    pub(crate) fn readable_end(&self) -> Option<SequenceNumber> {
        let local_end = self.file.as_ref()?.local_end();
        Some(local_end.min(self.state.sealed_end.unwrap_or(self.known_durable)))
    }

    /// The segment's final sequence number, if the copy knows it is sealed.
    pub(crate) const fn sealed_end(&self) -> Option<SequenceNumber> {
        self.state.sealed_end
    }

    /// Handles an append or a heartbeat (design, section 5, "Persist").
    pub(crate) async fn append(
        &mut self,
        request: &AppendRequest,
        admission: Admission,
    ) -> Result<AppendResponse, ReplicaError> {
        self.fail_if_damaged()?;
        if request.writer_epoch < request.segment.epoch {
            return Err(invalid_request(
                "writer epoch is older than the segment's own epoch",
            ));
        }
        let fenced_epoch = self.state.fenced_epoch.max(self.file_epoch());
        if request.writer_epoch < fenced_epoch {
            return Err(ReplicaError::Fenced {
                request_epoch: request.writer_epoch,
                fenced_epoch,
            });
        }
        if let Some(sealed_end) = self.state.sealed_end {
            return Err(ReplicaError::Sealed { sealed_end });
        }
        if let Some(file) = &self.file {
            let header = file.header();
            if header.epoch != request.segment.epoch
                || header.first_sequence != request.segment.first_sequence
            {
                return Err(invalid_request(
                    "segment descriptor does not match the replica's copy",
                ));
            }
        }
        let batches = decode_frames(self.address, &request.batch_frames)
            .map_err(|frames_error| invalid_request(&frames_error.to_string()))?;

        let local_end = self.local_end_or(request.segment.first_sequence.preceding());
        let new_batches = Self::batches_after(&batches, local_end)?;
        if let Some(first_new_batch) = new_batches.first() {
            if first_new_batch.first_sequence() != local_end.next() {
                return Err(ReplicaError::OutOfOrder {
                    first_sequence: first_new_batch.first_sequence(),
                    local_end,
                });
            }
            self.store(request, new_batches, admission).await?;
        }

        self.known_durable = self.known_durable.max(request.durable_sequence);
        Ok(AppendResponse {
            local_end: self.local_end_or(local_end),
        })
    }

    /// Raises the fenced epoch and reports the end of the copy (design,
    /// section 9, step 3). The new epoch is durable before this returns.
    pub(crate) async fn fence(&mut self, epoch: Epoch) -> Result<FenceResponse, ReplicaError> {
        self.fail_if_damaged()?;
        let fenced_epoch = self.state.fenced_epoch.max(self.file_epoch());
        if epoch < fenced_epoch {
            return Err(ReplicaError::Fenced {
                request_epoch: epoch,
                fenced_epoch,
            });
        }
        if epoch > self.state.fenced_epoch || !self.has_stored_state {
            self.store_state(SegmentState {
                fenced_epoch: epoch,
                sealed_end: self.state.sealed_end,
            })
            .await?;
        }
        Ok(FenceResponse {
            local_end: self.file.as_ref().map(SegmentFile::local_end),
            sealed_end: self.state.sealed_end,
        })
    }

    /// Records the sealed end and discards every record above it.
    pub(crate) async fn seal(
        &mut self,
        sealed_end: SequenceNumber,
    ) -> Result<SealResponse, ReplicaError> {
        self.fail_if_damaged()?;
        match self.state.sealed_end {
            Some(existing_sealed_end) if existing_sealed_end != sealed_end => {
                return Err(ReplicaError::ConflictingSeal {
                    existing_sealed_end,
                    requested_sealed_end: sealed_end,
                });
            }
            Some(_) => {
                return Ok(SealResponse {
                    local_end: self.file.as_ref().map(SegmentFile::local_end),
                });
            }
            None => {}
        }

        // Cut the tail before recording the seal. A crash in between leaves
        // an open copy with a shorter tail, which the next seal completes.
        if let Some(file) = &mut self.file
            && file.local_end() > sealed_end
        {
            match file.truncate_to(sealed_end).await {
                Ok(()) => {}
                Err(SegmentError::NotABatchBoundary { .. }) => {
                    return Err(invalid_request(
                        "sealed end is not a batch boundary of the replica's copy",
                    ));
                }
                Err(segment_error) => return Err(self.damaged_by(&segment_error)),
            }
        }
        self.store_state(SegmentState {
            fenced_epoch: self.state.fenced_epoch,
            sealed_end: Some(sealed_end),
        })
        .await?;
        if let Some(file) = &mut self.file
            && let Err(segment_error) = file.release_unused_space().await
        {
            return Err(self.damaged_by(&segment_error));
        }
        Ok(SealResponse {
            local_end: self.file.as_ref().map(SegmentFile::local_end),
        })
    }

    /// Reads batches for a reader or a recovering writer.
    pub(crate) async fn read(
        &mut self,
        request: &ReadRequest,
    ) -> Result<ReadResponse, ReplicaError> {
        self.fail_if_damaged()?;
        let Some(file) = &self.file else {
            return Err(ReplicaError::SegmentNotFound {
                segment: self.address,
            });
        };
        let readable_end = match (self.state.sealed_end, request.limit) {
            (Some(sealed_end), _) => file.local_end().min(sealed_end),
            (None, ReadLimit::DurableSequence) => file.local_end().min(self.known_durable),
            (None, ReadLimit::LocalEnd { recovery_epoch }) => {
                if recovery_epoch < self.state.fenced_epoch {
                    return Err(ReplicaError::Fenced {
                        request_epoch: recovery_epoch,
                        fenced_epoch: self.state.fenced_epoch,
                    });
                }
                if recovery_epoch > self.state.fenced_epoch {
                    return Err(invalid_request(
                        "the copy must be fenced at the recovery epoch before its tail is read",
                    ));
                }
                file.local_end()
            }
        };
        let batch_frames = match file
            .read_frames(request.from_sequence, readable_end, request.max_bytes)
            .await
        {
            Ok(batch_frames) => batch_frames,
            Err(segment_error) => return Err(self.damaged_by(&segment_error)),
        };
        Ok(ReadResponse {
            batch_frames,
            readable_end,
            sealed_end: self.state.sealed_end,
        })
    }

    /// Rereads the whole copy and marks it damaged if anything fails its
    /// checksum, as the background scrubber does.
    pub(crate) async fn verify(&mut self) {
        let Some(file) = &self.file else {
            return;
        };
        if let Err(segment_error) = file.verify().await {
            self.damaged_by(&segment_error);
        }
    }

    /// Reports what the replica holds for this segment.
    pub(crate) fn status(&self) -> SegmentStatus {
        SegmentStatus {
            address: self.address,
            local_end: self.file.as_ref().map(SegmentFile::local_end),
            fenced_epoch: self.state.fenced_epoch.max(self.file_epoch()),
            sealed_end: self.state.sealed_end,
            size_bytes: self.file.as_ref().map_or(0, SegmentFile::content_bytes),
            is_damaged: self.damage.is_some(),
        }
    }

    /// Removes everything the replica stores for this segment.
    pub(crate) async fn delete(mut self) -> Result<(), ReplicaError> {
        self.file = None;
        for path in [
            segment_file_path(self.address),
            segment_state_path(self.address),
        ] {
            self.disk
                .remove_file(&path)
                .await
                .map_err(|io_error| ReplicaError::Storage {
                    reason: format!("cannot remove {path}: {io_error}"),
                })?;
        }
        Ok(())
    }

    /// The reason the disk refused a write or a flush, if it ever did.
    ///
    /// After a failed flush the kernel may have dropped the unwritten pages,
    /// so a retry could falsely succeed. The process must stop (design,
    /// section 7, "Flush policy").
    pub(crate) fn failed_write(&self) -> Option<&str> {
        if self.has_failed_write {
            self.damage.as_deref()
        } else {
            None
        }
    }

    /// Returns the batches that lie after `local_end`, dropping the ones the
    /// copy already holds.
    ///
    /// A retried request may resend batches that were already stored. Within
    /// one segment there is only one possible record at each sequence number
    /// (design, section 4, rule 1), so they can be acknowledged again without
    /// comparing content.
    fn batches_after(
        batches: &[Batch],
        local_end: SequenceNumber,
    ) -> Result<&[Batch], ReplicaError> {
        let already_stored = batches
            .iter()
            .take_while(|batch| batch.last_sequence() <= local_end)
            .count();
        let new_batches = &batches[already_stored..];
        match new_batches.first() {
            Some(straddling_batch) if straddling_batch.first_sequence() <= local_end => Err(
                invalid_request("batch boundaries do not match the replica's copy"),
            ),
            _ => Ok(new_batches),
        }
    }

    /// Writes and flushes `new_batches`, creating the data file if needed.
    async fn store(
        &mut self,
        request: &AppendRequest,
        new_batches: &[Batch],
        admission: Admission,
    ) -> Result<(), ReplicaError> {
        if self.file.is_none() {
            if !admission.may_create_segments {
                return Err(ReplicaError::NoCapacity {
                    used_percent: admission.used_percent,
                });
            }
            let header = SegmentHeader {
                address: self.address,
                epoch: request.segment.epoch,
                first_sequence: request.segment.first_sequence,
            };
            let created = SegmentFile::create(
                self.disk.as_ref(),
                &segment_file_path(self.address),
                header,
                self.preallocated_bytes,
            )
            .await;
            match created {
                Ok(file) => self.file = Some(file),
                Err(segment_error) => return Err(self.damaged_by(&segment_error)),
            }
        }
        let file = self.file.as_mut().expect("the data file was just ensured");
        let stored = match file.append(new_batches).await {
            Ok(()) => file.flush().await,
            Err(segment_error) => Err(segment_error),
        };
        stored.map_err(|segment_error| {
            self.has_failed_write = true;
            self.damaged_by(&segment_error)
        })
    }

    /// Replaces the sidecar state durably, then adopts it in memory.
    async fn store_state(&mut self, new_state: SegmentState) -> Result<(), ReplicaError> {
        let stored = self
            .disk
            .write_file_atomically(
                &segment_state_path(self.address),
                Bytes::copy_from_slice(&new_state.encode()),
            )
            .await;
        if let Err(io_error) = stored {
            let reason = format!("cannot store segment state: {io_error}");
            self.has_failed_write = true;
            self.damage = Some(reason.clone());
            return Err(ReplicaError::Storage { reason });
        }
        self.state = new_state;
        self.has_stored_state = true;
        Ok(())
    }

    fn local_end_or(&self, end_of_an_empty_copy: SequenceNumber) -> SequenceNumber {
        self.file
            .as_ref()
            .map_or(end_of_an_empty_copy, SegmentFile::local_end)
    }

    /// The epoch that created the data file, which nothing older may write.
    fn file_epoch(&self) -> Epoch {
        self.file
            .as_ref()
            .map_or(Epoch::ZERO, |file| file.header().epoch)
    }

    fn fail_if_damaged(&self) -> Result<(), ReplicaError> {
        match &self.damage {
            Some(reason) => Err(ReplicaError::Storage {
                reason: reason.clone(),
            }),
            None => Ok(()),
        }
    }

    /// Marks the copy damaged and returns the error to answer with.
    fn damaged_by(&mut self, segment_error: &SegmentError) -> ReplicaError {
        let reason = segment_error.to_string();
        tracing::error!(segment = %self.address, %reason, "segment copy is damaged");
        self.damage = Some(reason.clone());
        ReplicaError::Storage { reason }
    }
}

fn invalid_request(reason: &str) -> ReplicaError {
    ReplicaError::InvalidRequest {
        reason: reason.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use bitacora_segment::MemoryDisk;
    use bitacora_types::replica_api::SegmentDescriptor;
    use bitacora_types::{SegmentId, StreamId};
    use bytes::BytesMut;

    use super::*;

    const SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
    const SEGMENT_EPOCH: Epoch = Epoch::new(5);
    const NEXT_EPOCH: Epoch = Epoch::new(6);
    const FIRST_SEQUENCE: u64 = 100;
    const PREALLOCATED_BYTES: u64 = 4 * 1024;
    const ROOMY: Admission = Admission {
        may_create_segments: true,
        used_percent: 10,
    };
    const NEARLY_FULL: Admission = Admission {
        may_create_segments: false,
        used_percent: 90,
    };

    fn descriptor() -> SegmentDescriptor {
        SegmentDescriptor {
            address: SEGMENT,
            epoch: SEGMENT_EPOCH,
            first_sequence: SequenceNumber::new(FIRST_SEQUENCE),
        }
    }

    /// Frames for one batch per `(first_sequence, record_count)` pair.
    fn frames(batches: &[(u64, u64)]) -> Bytes {
        let mut batch_frames = BytesMut::new();
        for (first_sequence, record_count) in batches {
            let payloads: Vec<Bytes> = (*first_sequence..first_sequence + record_count)
                .map(|sequence| Bytes::from(format!("record-{sequence}")))
                .collect();
            let batch = Batch::encode(SEGMENT, SequenceNumber::new(*first_sequence), &payloads)
                .expect("valid batch");
            batch_frames.extend_from_slice(batch.frame());
        }
        batch_frames.freeze()
    }

    fn append_request(batches: &[(u64, u64)], durable_sequence: u64) -> AppendRequest {
        AppendRequest {
            segment: descriptor(),
            writer_epoch: SEGMENT_EPOCH,
            batch_frames: frames(batches),
            durable_sequence: SequenceNumber::new(durable_sequence),
        }
    }

    fn read_request(from_sequence: u64, limit: ReadLimit) -> ReadRequest {
        ReadRequest {
            segment: SEGMENT,
            from_sequence: SequenceNumber::new(from_sequence),
            max_bytes: usize::MAX,
            wait_for_records: None,
            limit,
        }
    }

    fn new_copy(disk: &MemoryDisk) -> SegmentCopy {
        SegmentCopy::absent(SEGMENT, Arc::new(disk.clone()), PREALLOCATED_BYTES)
    }

    /// A copy holding records 100..=109 in two batches, with 104 durable.
    async fn copy_with_records(disk: &MemoryDisk) -> SegmentCopy {
        let mut copy = new_copy(disk);
        copy.append(&append_request(&[(100, 5), (105, 5)], 104), ROOMY)
            .await
            .expect("append");
        copy
    }

    async fn reload(disk: &MemoryDisk) -> SegmentCopy {
        SegmentCopy::load(SEGMENT, Arc::new(disk.clone()), PREALLOCATED_BYTES).await
    }

    async fn readable_sequences(copy: &mut SegmentCopy, limit: ReadLimit) -> Vec<u64> {
        let response = copy
            .read(&read_request(FIRST_SEQUENCE, limit))
            .await
            .expect("read");
        decode_frames(SEGMENT, &response.batch_frames)
            .expect("valid frames")
            .iter()
            .flat_map(Batch::records)
            .map(|record| record.sequence.get())
            .collect()
    }

    // ---- append ----

    #[tokio::test]
    async fn append_acknowledges_the_new_local_end() {
        let disk = MemoryDisk::new();
        let response = new_copy(&disk)
            .append(&append_request(&[(100, 5)], 0), ROOMY)
            .await;
        assert_eq!(
            response,
            Ok(AppendResponse {
                local_end: SequenceNumber::new(104)
            })
        );
    }

    #[tokio::test]
    async fn acknowledged_records_survive_a_crash() {
        let disk = MemoryDisk::new();
        copy_with_records(&disk).await;
        disk.crash();
        assert_eq!(
            reload(&disk).await.status().local_end,
            Some(SequenceNumber::new(109))
        );
    }

    #[tokio::test]
    async fn append_leaving_a_gap_reports_where_to_resend_from() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        let refused = copy.append(&append_request(&[(112, 1)], 0), ROOMY).await;
        assert_eq!(
            refused,
            Err(ReplicaError::OutOfOrder {
                first_sequence: SequenceNumber::new(112),
                local_end: SequenceNumber::new(109),
            })
        );
    }

    #[tokio::test]
    async fn first_append_not_starting_at_the_segment_start_is_refused() {
        let disk = MemoryDisk::new();
        let refused = new_copy(&disk)
            .append(&append_request(&[(105, 5)], 0), ROOMY)
            .await;
        assert_eq!(
            refused,
            Err(ReplicaError::OutOfOrder {
                first_sequence: SequenceNumber::new(105),
                local_end: SequenceNumber::new(99),
            })
        );
    }

    #[tokio::test]
    async fn refused_first_append_creates_no_file() {
        let disk = MemoryDisk::new();
        let mut copy = new_copy(&disk);
        let _ = copy.append(&append_request(&[(105, 5)], 0), ROOMY).await;
        assert!(!copy.is_stored());
    }

    #[tokio::test]
    async fn resent_batches_are_acknowledged_again() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        let response = copy
            .append(&append_request(&[(100, 5), (105, 5)], 0), ROOMY)
            .await;
        assert_eq!(
            response,
            Ok(AppendResponse {
                local_end: SequenceNumber::new(109)
            })
        );
    }

    #[tokio::test]
    async fn resend_overlapping_the_end_stores_only_the_new_batches() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        copy.append(&append_request(&[(105, 5), (110, 2)], 111), ROOMY)
            .await
            .expect("append");
        assert_eq!(
            readable_sequences(&mut copy, ReadLimit::DurableSequence).await,
            (100..=111).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn resend_with_different_batch_boundaries_is_refused() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        let refused = copy.append(&append_request(&[(108, 4)], 0), ROOMY).await;
        assert!(matches!(refused, Err(ReplicaError::InvalidRequest { .. })));
    }

    #[tokio::test]
    async fn frames_failing_their_checksum_are_refused() {
        let disk = MemoryDisk::new();
        let mut damaged_frames = frames(&[(100, 5)]).to_vec();
        damaged_frames[30] ^= 0x01;
        let request = AppendRequest {
            batch_frames: Bytes::from(damaged_frames),
            ..append_request(&[], 0)
        };
        let refused = new_copy(&disk).append(&request, ROOMY).await;
        assert!(matches!(refused, Err(ReplicaError::InvalidRequest { .. })));
    }

    #[tokio::test]
    async fn heartbeat_to_a_replica_without_a_copy_reports_an_empty_copy() {
        let disk = MemoryDisk::new();
        let response = new_copy(&disk).append(&append_request(&[], 0), ROOMY).await;
        assert_eq!(
            response,
            Ok(AppendResponse {
                local_end: SequenceNumber::new(FIRST_SEQUENCE - 1)
            })
        );
    }

    #[tokio::test]
    async fn nearly_full_disk_refuses_to_create_a_segment() {
        let disk = MemoryDisk::new();
        let refused = new_copy(&disk)
            .append(&append_request(&[(100, 5)], 0), NEARLY_FULL)
            .await;
        assert_eq!(refused, Err(ReplicaError::NoCapacity { used_percent: 90 }));
    }

    #[tokio::test]
    async fn nearly_full_disk_still_appends_to_an_existing_segment() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        let response = copy
            .append(&append_request(&[(110, 1)], 0), NEARLY_FULL)
            .await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn append_with_a_descriptor_that_contradicts_the_copy_is_refused() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        let mut request = append_request(&[(110, 1)], 0);
        request.segment.first_sequence = SequenceNumber::new(90);
        let refused = copy.append(&request, ROOMY).await;
        assert!(matches!(refused, Err(ReplicaError::InvalidRequest { .. })));
    }

    #[tokio::test]
    async fn writer_older_than_the_segment_is_refused() {
        let disk = MemoryDisk::new();
        let request = AppendRequest {
            writer_epoch: Epoch::new(4),
            ..append_request(&[(100, 5)], 0)
        };
        let refused = new_copy(&disk).append(&request, ROOMY).await;
        assert!(matches!(refused, Err(ReplicaError::InvalidRequest { .. })));
    }

    // ---- fencing ----

    #[tokio::test]
    async fn fence_reports_the_end_of_the_copy() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        assert_eq!(
            copy.fence(NEXT_EPOCH).await,
            Ok(FenceResponse {
                local_end: Some(SequenceNumber::new(109)),
                sealed_end: None,
            })
        );
    }

    #[tokio::test]
    async fn fenced_copy_refuses_the_old_writer() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        copy.fence(NEXT_EPOCH).await.expect("fence");
        let refused = copy.append(&append_request(&[(110, 1)], 0), ROOMY).await;
        assert_eq!(
            refused,
            Err(ReplicaError::Fenced {
                request_epoch: SEGMENT_EPOCH,
                fenced_epoch: NEXT_EPOCH,
            })
        );
    }

    #[tokio::test]
    async fn fenced_copy_refuses_the_old_writers_heartbeat() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        copy.fence(NEXT_EPOCH).await.expect("fence");
        let refused = copy.append(&append_request(&[], 109), ROOMY).await;
        assert!(matches!(refused, Err(ReplicaError::Fenced { .. })));
    }

    #[tokio::test]
    async fn fence_survives_a_crash() {
        let disk = MemoryDisk::new();
        copy_with_records(&disk)
            .await
            .fence(NEXT_EPOCH)
            .await
            .expect("fence");
        disk.crash();
        let refused = reload(&disk)
            .await
            .append(&append_request(&[(110, 1)], 0), ROOMY)
            .await;
        assert!(matches!(refused, Err(ReplicaError::Fenced { .. })));
    }

    #[tokio::test]
    async fn fenced_copy_accepts_the_recovering_writer() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        copy.fence(NEXT_EPOCH).await.expect("fence");
        let request = AppendRequest {
            writer_epoch: NEXT_EPOCH,
            ..append_request(&[(110, 1)], 0)
        };
        assert!(copy.append(&request, ROOMY).await.is_ok());
    }

    #[tokio::test]
    async fn fence_with_an_older_epoch_is_refused() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        copy.fence(Epoch::new(8)).await.expect("fence");
        assert_eq!(
            copy.fence(Epoch::new(7)).await,
            Err(ReplicaError::Fenced {
                request_epoch: Epoch::new(7),
                fenced_epoch: Epoch::new(8),
            })
        );
    }

    #[tokio::test]
    async fn fence_can_be_repeated() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        copy.fence(NEXT_EPOCH).await.expect("fence");
        assert!(copy.fence(NEXT_EPOCH).await.is_ok());
    }

    #[tokio::test]
    async fn fence_of_a_segment_the_replica_never_saw_reports_no_copy() {
        let disk = MemoryDisk::new();
        assert_eq!(
            new_copy(&disk).fence(NEXT_EPOCH).await,
            Ok(FenceResponse {
                local_end: None,
                sealed_end: None,
            })
        );
    }

    /// A replica that was partitioned while the segment was written must
    /// still refuse the old writer once it has promised the new one.
    #[tokio::test]
    async fn fence_of_an_unseen_segment_still_blocks_the_old_writer_after_a_crash() {
        let disk = MemoryDisk::new();
        new_copy(&disk).fence(NEXT_EPOCH).await.expect("fence");
        disk.crash();
        let refused = reload(&disk)
            .await
            .append(&append_request(&[(100, 5)], 0), ROOMY)
            .await;
        assert!(matches!(refused, Err(ReplicaError::Fenced { .. })));
    }

    // ---- sealing ----

    #[tokio::test]
    async fn seal_discards_records_above_the_sealed_end() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        copy.seal(SequenceNumber::new(104)).await.expect("seal");
        assert_eq!(
            readable_sequences(&mut copy, ReadLimit::DurableSequence).await,
            (100..=104).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn seal_makes_the_whole_segment_readable() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        copy.seal(SequenceNumber::new(109)).await.expect("seal");
        assert_eq!(
            readable_sequences(&mut copy, ReadLimit::DurableSequence).await,
            (100..=109).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn sealed_copy_refuses_appends() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        copy.seal(SequenceNumber::new(109)).await.expect("seal");
        let refused = copy.append(&append_request(&[(110, 1)], 0), ROOMY).await;
        assert_eq!(
            refused,
            Err(ReplicaError::Sealed {
                sealed_end: SequenceNumber::new(109)
            })
        );
    }

    #[tokio::test]
    async fn seal_survives_a_crash() {
        let disk = MemoryDisk::new();
        copy_with_records(&disk)
            .await
            .seal(SequenceNumber::new(104))
            .await
            .expect("seal");
        disk.crash();
        let status = reload(&disk).await.status();
        assert_eq!(
            (status.sealed_end, status.local_end),
            (
                Some(SequenceNumber::new(104)),
                Some(SequenceNumber::new(104))
            )
        );
    }

    #[tokio::test]
    async fn seal_can_be_repeated() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        copy.seal(SequenceNumber::new(104)).await.expect("seal");
        assert_eq!(
            copy.seal(SequenceNumber::new(104)).await,
            Ok(SealResponse {
                local_end: Some(SequenceNumber::new(104))
            })
        );
    }

    #[tokio::test]
    async fn seal_at_a_different_end_is_a_conflict() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        copy.seal(SequenceNumber::new(104)).await.expect("seal");
        assert_eq!(
            copy.seal(SequenceNumber::new(109)).await,
            Err(ReplicaError::ConflictingSeal {
                existing_sealed_end: SequenceNumber::new(104),
                requested_sealed_end: SequenceNumber::new(109),
            })
        );
    }

    #[tokio::test]
    async fn seal_inside_a_batch_is_refused_and_changes_nothing() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        let refused = copy.seal(SequenceNumber::new(106)).await;
        assert!(
            matches!(refused, Err(ReplicaError::InvalidRequest { .. }))
                && copy.status().sealed_end.is_none()
        );
    }

    #[tokio::test]
    async fn seal_beyond_a_short_copy_reports_how_short_it_is() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        assert_eq!(
            copy.seal(SequenceNumber::new(120)).await,
            Ok(SealResponse {
                local_end: Some(SequenceNumber::new(109))
            })
        );
    }

    #[tokio::test]
    async fn seal_of_a_segment_the_replica_never_saw_blocks_later_appends() {
        let disk = MemoryDisk::new();
        let mut copy = new_copy(&disk);
        copy.seal(SequenceNumber::new(104)).await.expect("seal");
        let refused = copy.append(&append_request(&[(100, 5)], 0), ROOMY).await;
        assert!(matches!(refused, Err(ReplicaError::Sealed { .. })));
    }

    #[tokio::test]
    async fn seal_releases_the_preallocated_space() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        copy.seal(SequenceNumber::new(109)).await.expect("seal");
        let file_bytes = disk
            .durable_content(&segment_file_path(SEGMENT))
            .expect("file exists")
            .len();
        assert_eq!(
            u64::try_from(file_bytes).expect("small"),
            copy.status().size_bytes
        );
    }

    // ---- reads ----

    #[tokio::test]
    async fn readers_see_only_what_the_writer_declared_durable() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        assert_eq!(
            readable_sequences(&mut copy, ReadLimit::DurableSequence).await,
            (100..=104).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn heartbeat_advances_what_readers_see() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        copy.append(&append_request(&[], 109), ROOMY)
            .await
            .expect("heartbeat");
        assert_eq!(copy.readable_end(), Some(SequenceNumber::new(109)));
    }

    #[tokio::test]
    async fn durable_sequence_never_moves_backwards() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        copy.append(&append_request(&[], 100), ROOMY)
            .await
            .expect("heartbeat");
        assert_eq!(copy.readable_end(), Some(SequenceNumber::new(104)));
    }

    #[tokio::test]
    async fn durable_sequence_beyond_the_copy_is_capped_at_its_end() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        copy.append(&append_request(&[], 500), ROOMY)
            .await
            .expect("heartbeat");
        assert_eq!(copy.readable_end(), Some(SequenceNumber::new(109)));
    }

    #[tokio::test]
    async fn restarted_copy_serves_nothing_until_it_hears_from_the_writer() {
        let disk = MemoryDisk::new();
        copy_with_records(&disk).await;
        disk.crash();
        let mut reloaded = reload(&disk).await;
        assert_eq!(
            readable_sequences(&mut reloaded, ReadLimit::DurableSequence).await,
            Vec::<u64>::new()
        );
    }

    #[tokio::test]
    async fn recovering_writer_reads_past_the_durable_sequence() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        copy.fence(NEXT_EPOCH).await.expect("fence");
        let recovery_read = ReadLimit::LocalEnd {
            recovery_epoch: NEXT_EPOCH,
        };
        assert_eq!(
            readable_sequences(&mut copy, recovery_read).await,
            (100..=109).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn uncommitted_tail_is_hidden_from_anyone_who_did_not_fence() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        copy.fence(NEXT_EPOCH).await.expect("fence");
        let stale_recovery_read = read_request(
            FIRST_SEQUENCE,
            ReadLimit::LocalEnd {
                recovery_epoch: SEGMENT_EPOCH,
            },
        );
        assert!(matches!(
            copy.read(&stale_recovery_read).await,
            Err(ReplicaError::Fenced { .. })
        ));
    }

    #[tokio::test]
    async fn uncommitted_tail_is_hidden_until_the_copy_is_fenced() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        let premature_recovery_read = read_request(
            FIRST_SEQUENCE,
            ReadLimit::LocalEnd {
                recovery_epoch: NEXT_EPOCH,
            },
        );
        assert!(matches!(
            copy.read(&premature_recovery_read).await,
            Err(ReplicaError::InvalidRequest { .. })
        ));
    }

    #[tokio::test]
    async fn read_reports_the_sealed_end() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        copy.seal(SequenceNumber::new(109)).await.expect("seal");
        let response = copy
            .read(&read_request(110, ReadLimit::DurableSequence))
            .await
            .expect("read");
        assert_eq!(
            (response.batch_frames.len(), response.sealed_end),
            (0, Some(SequenceNumber::new(109)))
        );
    }

    #[tokio::test]
    async fn read_of_a_segment_without_data_finds_nothing() {
        let disk = MemoryDisk::new();
        let mut copy = new_copy(&disk);
        copy.fence(NEXT_EPOCH).await.expect("fence");
        assert_eq!(
            copy.read(&read_request(100, ReadLimit::DurableSequence))
                .await,
            Err(ReplicaError::SegmentNotFound { segment: SEGMENT })
        );
    }

    // ---- damage ----

    #[tokio::test]
    async fn scrub_marks_a_corrupted_copy_as_damaged() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        disk.corrupt_byte(&segment_file_path(SEGMENT), 100, 0x10);
        copy.verify().await;
        assert!(copy.status().is_damaged);
    }

    #[tokio::test]
    async fn scrub_leaves_an_intact_copy_alone() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        copy.verify().await;
        assert!(!copy.status().is_damaged);
    }

    #[tokio::test]
    async fn damaged_copy_refuses_every_request() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        disk.corrupt_byte(&segment_file_path(SEGMENT), 100, 0x10);
        copy.verify().await;
        let append = copy.append(&append_request(&[(110, 1)], 0), ROOMY).await;
        let fence = copy.fence(NEXT_EPOCH).await;
        let seal = copy.seal(SequenceNumber::new(109)).await;
        assert!(
            matches!(append, Err(ReplicaError::Storage { .. }))
                && matches!(fence, Err(ReplicaError::Storage { .. }))
                && matches!(seal, Err(ReplicaError::Storage { .. }))
        );
    }

    #[tokio::test]
    async fn copy_with_an_unreadable_header_loads_as_damaged() {
        let disk = MemoryDisk::new();
        copy_with_records(&disk).await;
        disk.corrupt_byte(&segment_file_path(SEGMENT), 20, 0x01);
        assert!(reload(&disk).await.status().is_damaged);
    }

    #[tokio::test]
    async fn copy_with_an_unreadable_state_loads_as_damaged() {
        let disk = MemoryDisk::new();
        copy_with_records(&disk)
            .await
            .fence(NEXT_EPOCH)
            .await
            .expect("fence");
        disk.corrupt_byte(&segment_state_path(SEGMENT), 15, 0x01);
        assert!(reload(&disk).await.status().is_damaged);
    }

    // ---- deletion ----

    #[tokio::test]
    async fn deleted_copy_leaves_nothing_on_disk() {
        let disk = MemoryDisk::new();
        let mut copy = copy_with_records(&disk).await;
        copy.seal(SequenceNumber::new(109)).await.expect("seal");
        copy.delete().await.expect("delete");
        assert_eq!(disk.list_files().await.expect("list"), Vec::<String>::new());
    }
}
