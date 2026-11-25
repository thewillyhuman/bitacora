//! One replica's copy of a segment: a file of batch frames on a [`Disk`].
//!
//! [`SegmentFile`] is single-owner on purpose. The replica runs every
//! state-changing operation on a segment from one task, so no locking is
//! needed here and a dropped request can never leave a half-applied change.

use std::io;

use bitacora_types::{SegmentAddress, SequenceNumber};
use bytes::{Bytes, BytesMut};
use thiserror::Error;

use crate::disk::{Disk, DiskFile};
use crate::format::{
    Batch, HeaderError, SEGMENT_HEADER_BYTES, ScanStop, SegmentHeader, scan_valid_prefix,
};
use crate::index::{IndexEntry, SparseIndex};

/// How much of a segment file one read brings into memory while scanning.
const SCAN_CHUNK_BYTES: usize = 1024 * 1024;

const HEADER_END_OFFSET: u64 = SEGMENT_HEADER_BYTES as u64;

/// Why an operation on a segment file failed.
#[derive(Debug, Error)]
pub enum SegmentError {
    /// The disk refused an operation.
    #[error("{operation} failed on {path}: {source}")]
    Io {
        /// What was being done.
        operation: &'static str,
        /// The file it was done to.
        path: String,
        /// The disk's error.
        #[source]
        source: io::Error,
    },
    /// The file does not start with a valid segment header.
    #[error("{path} has an invalid header: {reason}")]
    InvalidHeader {
        /// The file that was opened.
        path: String,
        /// What is wrong with its header.
        reason: HeaderError,
    },
    /// The file is a copy of another segment than the one asked for.
    #[error("{path} holds segment {found}, expected {expected}")]
    WrongSegment {
        /// The file that was opened.
        path: String,
        /// The segment the caller asked for.
        expected: SegmentAddress,
        /// The segment named in the file's header.
        found: SegmentAddress,
    },
    /// The batches to append do not start right after the copy's end.
    #[error("batches start at {found} but the copy ends at {local_end}")]
    OutOfOrder {
        /// The last sequence number the copy holds.
        local_end: SequenceNumber,
        /// The first sequence number of the rejected batches.
        found: SequenceNumber,
    },
    /// The copy cannot be cut at the requested position.
    #[error(
        "cannot cut the copy at {requested_end}: it ends at {local_end} and batches are kept whole"
    )]
    NotABatchBoundary {
        /// The position the caller asked to cut at.
        requested_end: SequenceNumber,
        /// The last sequence number the copy holds.
        local_end: SequenceNumber,
    },
    /// Bytes that were acknowledged as stored no longer verify.
    #[error("{path} is corrupt at byte {offset_bytes}: {reason}")]
    Corrupt {
        /// The damaged file.
        path: String,
        /// Where the first invalid frame starts.
        offset_bytes: u64,
        /// What is wrong with it.
        reason: String,
    },
}

/// Whether a batch visitor wants the scan to go on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Visit {
    Continue,
    Stop,
}

/// Where and why a scan over part of the file ended.
#[derive(Clone, Copy, Debug)]
struct RegionEnd {
    /// The offset right after the last valid batch that was visited.
    offset_bytes: u64,
    /// `None` if the visitor stopped the scan.
    stop: Option<ScanStop>,
}

/// One replica's copy of a segment.
pub struct SegmentFile {
    file: Box<dyn DiskFile>,
    path: String,
    header: SegmentHeader,
    /// Where the next batch frame goes.
    end_offset_bytes: u64,
    /// The length the file is currently extended to.
    allocated_bytes: u64,
    /// How much the file grows by when it fills up. Zero lets writes extend
    /// it one batch at a time.
    growth_bytes: u64,
    /// The last sequence number held, or the position before the segment's
    /// first record if the copy is empty.
    local_end: SequenceNumber,
    index: SparseIndex,
}

impl std::fmt::Debug for SegmentFile {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SegmentFile")
            .field("path", &self.path)
            .field("header", &self.header)
            .field("end_offset_bytes", &self.end_offset_bytes)
            .field("local_end", &self.local_end)
            .finish_non_exhaustive()
    }
}

impl SegmentFile {
    /// Creates an empty copy at `path` and makes it durable.
    ///
    /// The file is preallocated to `preallocated_bytes` and grows by the same
    /// amount whenever it fills up, so that a flush only has to persist data
    /// and not a change of the file's size.
    ///
    /// # Errors
    ///
    /// Fails if the file exists or the disk refuses the operation.
    pub async fn create(
        disk: &dyn Disk,
        path: &str,
        header: SegmentHeader,
        preallocated_bytes: u64,
    ) -> Result<Self, SegmentError> {
        let allocated_bytes = preallocated_bytes.max(HEADER_END_OFFSET);
        let mut file = disk
            .create_file(path, allocated_bytes)
            .await
            .map_err(io_error("create", path))?;
        file.write_at(0, Bytes::copy_from_slice(&header.encode()))
            .await
            .map_err(io_error("write header", path))?;
        file.sync_data()
            .await
            .map_err(io_error("flush header", path))?;
        Ok(Self {
            file,
            path: path.to_owned(),
            header,
            end_offset_bytes: HEADER_END_OFFSET,
            allocated_bytes,
            growth_bytes: preallocated_bytes,
            local_end: header.first_sequence.preceding(),
            index: SparseIndex::new(),
        })
    }

    /// Opens the copy at `path` and scans it, as after a restart.
    ///
    /// The scan keeps the longest run of valid, consecutive batches and
    /// reports why it stopped. Whatever follows was never acknowledged; it is
    /// left in place and overwritten by the next append. The file itself is
    /// not modified. `growth_bytes` is how much the file grows by when it
    /// fills up, as in [`SegmentFile::create`].
    ///
    /// # Errors
    ///
    /// Fails if the file is missing, has no valid header, belongs to another
    /// segment than `expected`, or cannot be read.
    pub async fn open(
        disk: &dyn Disk,
        path: &str,
        expected: SegmentAddress,
        growth_bytes: u64,
    ) -> Result<(Self, ScanStop), SegmentError> {
        let file = disk.open_file(path).await.map_err(io_error("open", path))?;
        let header_bytes = file
            .read_at(0, SEGMENT_HEADER_BYTES)
            .await
            .map_err(io_error("read header", path))?;
        let header =
            SegmentHeader::decode(&header_bytes).map_err(|reason| SegmentError::InvalidHeader {
                path: path.to_owned(),
                reason,
            })?;
        if header.address != expected {
            return Err(SegmentError::WrongSegment {
                path: path.to_owned(),
                expected,
                found: header.address,
            });
        }
        let file_bytes = file
            .length_bytes()
            .await
            .map_err(io_error("measure", path))?;

        let mut segment = Self {
            file,
            path: path.to_owned(),
            header,
            end_offset_bytes: HEADER_END_OFFSET,
            allocated_bytes: file_bytes,
            growth_bytes,
            local_end: header.first_sequence.preceding(),
            index: SparseIndex::new(),
        };
        let mut index = SparseIndex::new();
        let mut local_end = segment.local_end;
        let region_end = segment
            .visit_batches(
                HEADER_END_OFFSET,
                header.first_sequence,
                file_bytes,
                |batch, offset_bytes| {
                    index.observe_batch(batch.first_sequence(), offset_bytes);
                    local_end = batch.last_sequence();
                    Visit::Continue
                },
            )
            .await?;
        segment.index = index;
        segment.local_end = local_end;
        segment.end_offset_bytes = region_end.offset_bytes;
        let stop = region_end
            .stop
            .expect("the recovery visitor never stops the scan");
        Ok((segment, stop))
    }

    /// The header the copy was created with.
    #[must_use]
    pub const fn header(&self) -> SegmentHeader {
        self.header
    }

    /// The last sequence number the copy holds, or the position before the
    /// segment's first record if it is empty.
    #[must_use]
    pub const fn local_end(&self) -> SequenceNumber {
        self.local_end
    }

    /// How many bytes of the file hold the header and valid batches.
    #[must_use]
    pub const fn content_bytes(&self) -> u64 {
        self.end_offset_bytes
    }

    /// Writes `batches` after the copy's end. They are not durable until
    /// [`SegmentFile::flush`] returns.
    ///
    /// # Errors
    ///
    /// Fails with [`SegmentError::OutOfOrder`] if the batches do not start
    /// right after the copy's end, without changing anything, or with
    /// [`SegmentError::Io`] if the disk refuses the write.
    pub async fn append(&mut self, batches: &[Batch]) -> Result<(), SegmentError> {
        let mut expected = self.local_end.next();
        for batch in batches {
            if batch.first_sequence() != expected {
                return Err(SegmentError::OutOfOrder {
                    local_end: expected.preceding(),
                    found: batch.first_sequence(),
                });
            }
            expected = batch.last_sequence().next();
        }

        let total_bytes: usize = batches.iter().map(|batch| batch.frame().len()).sum();
        let mut frames = BytesMut::with_capacity(total_bytes);
        for batch in batches {
            frames.extend_from_slice(batch.frame());
        }
        self.grow_to_fit(self.end_offset_bytes + to_u64(total_bytes))
            .await?;
        self.file
            .write_at(self.end_offset_bytes, frames.freeze())
            .await
            .map_err(io_error("append", &self.path))?;

        for batch in batches {
            self.index
                .observe_batch(batch.first_sequence(), self.end_offset_bytes);
            self.end_offset_bytes += to_u64(batch.frame().len());
            self.local_end = batch.last_sequence();
        }
        Ok(())
    }

    /// Makes every appended batch durable.
    ///
    /// # Errors
    ///
    /// Fails with [`SegmentError::Io`] if the flush fails. The copy must then
    /// be treated as suspect and never flushed again: the kernel may have
    /// dropped the unwritten pages, so a retry could falsely succeed.
    pub async fn flush(&mut self) -> Result<(), SegmentError> {
        self.file
            .sync_data()
            .await
            .map_err(io_error("flush", &self.path))
    }

    /// Reads whole batch frames, starting with the batch that holds
    /// `from_sequence` and ending no later than `through_sequence`.
    ///
    /// Frames are verified again as they are read. The result stops after
    /// the first batch that takes it past `max_bytes`, so it always makes
    /// progress, and it is empty if the copy holds nothing in the range.
    ///
    /// # Errors
    ///
    /// Fails with [`SegmentError::Corrupt`] if a frame that was once valid no
    /// longer verifies, or with [`SegmentError::Io`] if the disk fails.
    pub async fn read_frames(
        &self,
        from_sequence: SequenceNumber,
        through_sequence: SequenceNumber,
        max_bytes: usize,
    ) -> Result<Bytes, SegmentError> {
        let last_readable = through_sequence.min(self.local_end);
        if from_sequence > last_readable {
            return Ok(Bytes::new());
        }
        let start = self.scan_start_for(from_sequence);
        let mut frames = BytesMut::new();
        let region_end = self
            .visit_batches(
                start.offset_bytes,
                start.first_sequence,
                self.end_offset_bytes,
                |batch, _offset_bytes| {
                    if batch.last_sequence() < from_sequence {
                        return Visit::Continue;
                    }
                    if batch.last_sequence() > last_readable {
                        return Visit::Stop;
                    }
                    frames.extend_from_slice(batch.frame());
                    if frames.len() >= max_bytes || batch.last_sequence() == last_readable {
                        Visit::Stop
                    } else {
                        Visit::Continue
                    }
                },
            )
            .await?;
        self.require_intact(region_end)?;
        Ok(frames.freeze())
    }

    /// Discards every record above `end` and makes the cut durable.
    ///
    /// # Errors
    ///
    /// Fails with [`SegmentError::NotABatchBoundary`] if `end` lies beyond
    /// the copy's end or inside a batch, without changing anything.
    pub async fn truncate_to(&mut self, end: SequenceNumber) -> Result<(), SegmentError> {
        if end == self.local_end {
            return Ok(());
        }
        let not_a_boundary = SegmentError::NotABatchBoundary {
            requested_end: end,
            local_end: self.local_end,
        };
        if end > self.local_end || end < self.header.first_sequence.preceding() {
            return Err(not_a_boundary);
        }
        let first_discarded = end.next();
        let start = self.scan_start_for(first_discarded);
        let mut cut_offset_bytes = None;
        let region_end = self
            .visit_batches(
                start.offset_bytes,
                start.first_sequence,
                self.end_offset_bytes,
                |batch, offset_bytes| {
                    if batch.first_sequence() == first_discarded {
                        cut_offset_bytes = Some(offset_bytes);
                    }
                    if batch.first_sequence() >= first_discarded {
                        Visit::Stop
                    } else {
                        Visit::Continue
                    }
                },
            )
            .await?;
        self.require_intact(region_end)?;
        let Some(cut_offset_bytes) = cut_offset_bytes else {
            return Err(not_a_boundary);
        };

        self.file
            .set_length(cut_offset_bytes)
            .await
            .map_err(io_error("truncate", &self.path))?;
        self.index.forget_from(cut_offset_bytes);
        self.allocated_bytes = cut_offset_bytes;
        self.end_offset_bytes = cut_offset_bytes;
        self.local_end = end;
        Ok(())
    }

    /// Gives back the preallocated space the copy never used. Called once a
    /// segment is sealed and can no longer grow.
    ///
    /// # Errors
    ///
    /// Fails with [`SegmentError::Io`] if the disk refuses the operation.
    pub async fn release_unused_space(&mut self) -> Result<(), SegmentError> {
        self.file
            .set_length(self.end_offset_bytes)
            .await
            .map_err(io_error("release unused space", &self.path))?;
        self.allocated_bytes = self.end_offset_bytes;
        Ok(())
    }

    /// Rereads the whole copy from disk and verifies every checksum, as the
    /// background scrubber does.
    ///
    /// # Errors
    ///
    /// Fails with [`SegmentError::Corrupt`] if any stored frame no longer
    /// verifies, or with [`SegmentError::Io`] if the disk fails.
    pub async fn verify(&self) -> Result<(), SegmentError> {
        let region_end = self
            .visit_batches(
                HEADER_END_OFFSET,
                self.header.first_sequence,
                self.end_offset_bytes,
                |_batch, _offset_bytes| Visit::Continue,
            )
            .await?;
        self.require_intact(region_end)
    }

    /// Extends the file by whole growth steps until `needed_bytes` fit.
    ///
    /// Growing ahead of the data keeps the size change out of the flush that
    /// acknowledges an append: it is made durable here, once per step.
    async fn grow_to_fit(&mut self, needed_bytes: u64) -> Result<(), SegmentError> {
        if self.growth_bytes == 0 || needed_bytes <= self.allocated_bytes {
            return Ok(());
        }
        let missing_bytes = needed_bytes - self.allocated_bytes;
        let growth_steps = missing_bytes.div_ceil(self.growth_bytes);
        let grown_bytes = self.allocated_bytes + growth_steps * self.growth_bytes;
        self.file
            .set_length(grown_bytes)
            .await
            .map_err(io_error("grow", &self.path))?;
        self.allocated_bytes = grown_bytes;
        Ok(())
    }

    /// Returns the closest indexed place at or before `sequence` to start a
    /// scan from: an index entry, or the first batch of the file.
    fn scan_start_for(&self, sequence: SequenceNumber) -> IndexEntry {
        self.index
            .entry_at_or_before(sequence)
            .unwrap_or(IndexEntry {
                first_sequence: self.header.first_sequence,
                offset_bytes: HEADER_END_OFFSET,
            })
    }

    /// Fails unless a scan of acknowledged content ended at its limit or was
    /// stopped by its visitor.
    fn require_intact(&self, region_end: RegionEnd) -> Result<(), SegmentError> {
        let reason = match region_end.stop {
            None => return Ok(()),
            Some(ScanStop::EndOfData) if region_end.offset_bytes == self.end_offset_bytes => {
                return Ok(());
            }
            Some(ScanStop::EndOfData) => "frames are missing before the end of the copy".to_owned(),
            Some(ScanStop::Incomplete { needed_bytes }) => {
                format!("frame is cut short, it needs {needed_bytes} bytes")
            }
            Some(ScanStop::Invalid(frame_error)) => frame_error.to_string(),
            Some(ScanStop::SequenceGap { expected, found }) => {
                format!("frame starts at sequence {found} where {expected} was expected")
            }
        };
        Err(SegmentError::Corrupt {
            path: self.path.clone(),
            offset_bytes: region_end.offset_bytes,
            reason,
        })
    }

    /// Visits the consecutive valid batches found between `start_offset_bytes`
    /// and `limit_offset_bytes`, reading the file in chunks.
    ///
    /// `visit` receives each batch and its file offset and may stop the scan.
    async fn visit_batches(
        &self,
        start_offset_bytes: u64,
        start_sequence: SequenceNumber,
        limit_offset_bytes: u64,
        mut visit: impl FnMut(&Batch, u64) -> Visit,
    ) -> Result<RegionEnd, SegmentError> {
        let mut offset_bytes = start_offset_bytes;
        let mut next_sequence = start_sequence;
        let mut wanted_bytes = SCAN_CHUNK_BYTES;
        loop {
            let remaining_bytes = limit_offset_bytes.saturating_sub(offset_bytes);
            let chunk_bytes =
                wanted_bytes.min(usize::try_from(remaining_bytes).unwrap_or(usize::MAX));
            let chunk = self
                .file
                .read_at(offset_bytes, chunk_bytes)
                .await
                .map_err(io_error("read", &self.path))?;
            let reached_the_limit = chunk.len() < wanted_bytes;

            let prefix = scan_valid_prefix(self.header.address, next_sequence, &chunk);
            for batch in &prefix.batches {
                let batch_offset_bytes = offset_bytes;
                offset_bytes += to_u64(batch.frame().len());
                next_sequence = batch.last_sequence().next();
                if visit(batch, batch_offset_bytes) == Visit::Stop {
                    return Ok(RegionEnd {
                        offset_bytes,
                        stop: None,
                    });
                }
            }

            wanted_bytes = SCAN_CHUNK_BYTES;
            let consumed_the_chunk = prefix.valid_bytes == chunk.len();
            match prefix.stop {
                ScanStop::EndOfData if consumed_the_chunk && !reached_the_limit => {}
                ScanStop::Incomplete { needed_bytes } if !reached_the_limit => {
                    // The chunk ended inside a frame: read again from the
                    // frame's start, with room for all of it.
                    wanted_bytes = wanted_bytes.max(needed_bytes);
                }
                stop => {
                    return Ok(RegionEnd {
                        offset_bytes,
                        stop: Some(stop),
                    });
                }
            }
        }
    }
}

fn io_error<'path>(
    operation: &'static str,
    path: &'path str,
) -> impl FnOnce(io::Error) -> SegmentError + 'path {
    move |source| SegmentError::Io {
        operation,
        path: path.to_owned(),
        source,
    }
}

fn to_u64(length_bytes: usize) -> u64 {
    u64::try_from(length_bytes).expect("lengths fit in 64 bits")
}

#[cfg(test)]
mod tests {
    use bitacora_types::{DeterministicRandom, Epoch, LogRecord, SegmentId, StreamId};

    use super::*;
    use crate::format::decode_frames;
    use crate::memory_disk::MemoryDisk;

    const SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
    const PATH: &str = "7/3.seg";
    const FIRST_SEQUENCE: u64 = 100;
    const PREALLOCATED_BYTES: u64 = 4 * 1024;

    fn header() -> SegmentHeader {
        SegmentHeader {
            address: SEGMENT,
            epoch: Epoch::new(5),
            first_sequence: SequenceNumber::new(FIRST_SEQUENCE),
        }
    }

    /// One batch of `record_count` records starting at `first_sequence`,
    /// whose payloads name their own sequence number.
    fn batch(first_sequence: u64, record_count: u64) -> Batch {
        let payloads: Vec<Bytes> = (first_sequence..first_sequence + record_count)
            .map(|sequence| Bytes::from(format!("record-{sequence}")))
            .collect();
        Batch::encode(SEGMENT, SequenceNumber::new(first_sequence), &payloads).expect("valid batch")
    }

    async fn new_segment(disk: &MemoryDisk) -> SegmentFile {
        SegmentFile::create(disk, PATH, header(), PREALLOCATED_BYTES)
            .await
            .expect("new segment")
    }

    /// A segment holding batches 100..=102, 103..=104 and 105..=109, flushed.
    async fn segment_with_three_batches(disk: &MemoryDisk) -> SegmentFile {
        let mut segment = new_segment(disk).await;
        segment
            .append(&[batch(100, 3), batch(103, 2), batch(105, 5)])
            .await
            .expect("append");
        segment.flush().await.expect("flush");
        segment
    }

    async fn reopen(disk: &MemoryDisk) -> (SegmentFile, ScanStop) {
        SegmentFile::open(disk, PATH, SEGMENT, PREALLOCATED_BYTES)
            .await
            .expect("reopen")
    }

    fn sequences_in(frames: &Bytes) -> Vec<u64> {
        decode_frames(SEGMENT, frames)
            .expect("valid frames")
            .iter()
            .flat_map(Batch::records)
            .map(|record| record.sequence.get())
            .collect()
    }

    async fn read_all(segment: &SegmentFile) -> Vec<u64> {
        let frames = segment
            .read_frames(
                SequenceNumber::new(FIRST_SEQUENCE),
                SequenceNumber::new(u64::MAX),
                usize::MAX,
            )
            .await
            .expect("read");
        sequences_in(&frames)
    }

    // ---- create and append ----

    #[tokio::test]
    async fn new_segment_ends_before_its_first_sequence() {
        let segment = new_segment(&MemoryDisk::new()).await;
        assert_eq!(segment.local_end(), SequenceNumber::new(FIRST_SEQUENCE - 1));
    }

    #[tokio::test]
    async fn creating_a_segment_twice_fails() {
        let disk = MemoryDisk::new();
        new_segment(&disk).await;
        let second_attempt = SegmentFile::create(&disk, PATH, header(), PREALLOCATED_BYTES).await;
        assert!(matches!(second_attempt, Err(SegmentError::Io { .. })));
    }

    #[tokio::test]
    async fn append_moves_the_end_to_the_last_record() {
        let segment = segment_with_three_batches(&MemoryDisk::new()).await;
        assert_eq!(segment.local_end(), SequenceNumber::new(109));
    }

    #[tokio::test]
    async fn appending_nothing_changes_nothing() {
        let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;
        segment.append(&[]).await.expect("append");
        assert_eq!(segment.local_end(), SequenceNumber::new(109));
    }

    #[tokio::test]
    async fn append_that_skips_a_sequence_is_refused() {
        let mut segment = new_segment(&MemoryDisk::new()).await;
        let refused = segment.append(&[batch(101, 1)]).await;
        assert!(matches!(
            refused,
            Err(SegmentError::OutOfOrder { local_end, found })
                if local_end == SequenceNumber::new(99) && found == SequenceNumber::new(101)
        ));
    }

    #[tokio::test]
    async fn append_that_repeats_a_sequence_is_refused() {
        let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;
        let refused = segment.append(&[batch(105, 5)]).await;
        assert!(matches!(refused, Err(SegmentError::OutOfOrder { .. })));
    }

    #[tokio::test]
    async fn refused_append_leaves_the_copy_unchanged() {
        let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;
        let _ = segment.append(&[batch(110, 1), batch(112, 1)]).await;
        assert_eq!(read_all(&segment).await, (100..=109).collect::<Vec<_>>());
    }

    // ---- read ----

    #[tokio::test]
    async fn read_returns_the_appended_records_in_order() {
        let segment = segment_with_three_batches(&MemoryDisk::new()).await;
        assert_eq!(read_all(&segment).await, (100..=109).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn read_returns_the_payloads_that_were_appended() {
        let segment = segment_with_three_batches(&MemoryDisk::new()).await;
        let frames = segment
            .read_frames(
                SequenceNumber::new(103),
                SequenceNumber::new(104),
                usize::MAX,
            )
            .await
            .expect("read");
        let records: Vec<LogRecord> = decode_frames(SEGMENT, &frames)
            .expect("valid frames")
            .iter()
            .flat_map(Batch::records)
            .collect();
        assert_eq!(records[1].payload, Bytes::from("record-104"));
    }

    #[tokio::test]
    async fn read_from_inside_a_batch_starts_with_that_whole_batch() {
        let segment = segment_with_three_batches(&MemoryDisk::new()).await;
        let frames = segment
            .read_frames(
                SequenceNumber::new(104),
                SequenceNumber::new(109),
                usize::MAX,
            )
            .await
            .expect("read");
        assert_eq!(sequences_in(&frames), (103..=109).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn read_never_returns_a_batch_that_ends_past_the_limit() {
        let segment = segment_with_three_batches(&MemoryDisk::new()).await;
        let frames = segment
            .read_frames(
                SequenceNumber::new(100),
                SequenceNumber::new(107),
                usize::MAX,
            )
            .await
            .expect("read");
        assert_eq!(sequences_in(&frames), (100..=104).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn read_past_the_end_is_empty() {
        let segment = segment_with_three_batches(&MemoryDisk::new()).await;
        let frames = segment
            .read_frames(
                SequenceNumber::new(110),
                SequenceNumber::new(200),
                usize::MAX,
            )
            .await
            .expect("read");
        assert_eq!(frames, Bytes::new());
    }

    #[tokio::test]
    async fn read_above_the_limit_is_empty() {
        let segment = segment_with_three_batches(&MemoryDisk::new()).await;
        let frames = segment
            .read_frames(
                SequenceNumber::new(105),
                SequenceNumber::new(104),
                usize::MAX,
            )
            .await
            .expect("read");
        assert_eq!(frames, Bytes::new());
    }

    #[tokio::test]
    async fn read_of_an_empty_segment_is_empty() {
        let segment = new_segment(&MemoryDisk::new()).await;
        assert_eq!(read_all(&segment).await, Vec::<u64>::new());
    }

    #[tokio::test]
    async fn read_stops_after_the_batch_that_reaches_the_size_limit() {
        let segment = segment_with_three_batches(&MemoryDisk::new()).await;
        let frames = segment
            .read_frames(SequenceNumber::new(100), SequenceNumber::new(109), 1)
            .await
            .expect("read");
        assert_eq!(sequences_in(&frames), [100, 101, 102]);
    }

    // ---- crash recovery ----

    #[tokio::test]
    async fn reopened_segment_holds_everything_that_was_flushed() {
        let disk = MemoryDisk::new();
        segment_with_three_batches(&disk).await;
        disk.crash();
        let (reopened, stop) = reopen(&disk).await;
        assert_eq!(
            (read_all(&reopened).await, stop),
            ((100..=109).collect::<Vec<_>>(), ScanStop::EndOfData)
        );
    }

    #[tokio::test]
    async fn crash_before_a_flush_loses_only_the_unflushed_batches() {
        let disk = MemoryDisk::new();
        let mut segment = segment_with_three_batches(&disk).await;
        segment.append(&[batch(110, 4)]).await.expect("append");
        disk.crash();
        let (reopened, _stop) = reopen(&disk).await;
        assert_eq!(reopened.local_end(), SequenceNumber::new(109));
    }

    /// Crashes `disk` so that only the first `torn_bytes` of everything
    /// written after `flushed_bytes` reach the platter: a torn write.
    fn crash_with_torn_tail(disk: &MemoryDisk, flushed_bytes: u64, torn_bytes: usize) {
        let flushed_bytes = usize::try_from(flushed_bytes).expect("small file");
        disk.crash_keeping(|_path, durable, visible| {
            let mut survived = durable.to_vec();
            survived[flushed_bytes..flushed_bytes + torn_bytes]
                .copy_from_slice(&visible[flushed_bytes..flushed_bytes + torn_bytes]);
            survived
        });
    }

    #[tokio::test]
    async fn torn_tail_is_ignored_and_reported() {
        let disk = MemoryDisk::new();
        let mut segment = segment_with_three_batches(&disk).await;
        let flushed_bytes = segment.content_bytes();
        segment.append(&[batch(110, 4)]).await.expect("append");
        crash_with_torn_tail(&disk, flushed_bytes, 9);
        let (reopened, stop) = reopen(&disk).await;
        assert!(
            reopened.local_end() == SequenceNumber::new(109)
                && matches!(stop, ScanStop::Invalid(_)),
            "local end {}, stop {stop:?}",
            reopened.local_end()
        );
    }

    #[tokio::test]
    async fn segment_accepts_appends_over_a_torn_tail() {
        let disk = MemoryDisk::new();
        let mut segment = segment_with_three_batches(&disk).await;
        let flushed_bytes = segment.content_bytes();
        segment.append(&[batch(110, 4)]).await.expect("append");
        crash_with_torn_tail(&disk, flushed_bytes, 9);
        let (mut reopened, _stop) = reopen(&disk).await;
        reopened.append(&[batch(110, 2)]).await.expect("append");
        reopened.flush().await.expect("flush");
        disk.crash();
        let (after_second_crash, _stop) = reopen(&disk).await;
        assert_eq!(
            read_all(&after_second_crash).await,
            (100..=111).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn opening_a_missing_segment_fails() {
        let opened = SegmentFile::open(&MemoryDisk::new(), PATH, SEGMENT, PREALLOCATED_BYTES).await;
        assert!(matches!(opened, Err(SegmentError::Io { .. })));
    }

    #[tokio::test]
    async fn opening_another_segments_file_is_refused() {
        let disk = MemoryDisk::new();
        new_segment(&disk).await;
        let other_segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(4));
        let opened = SegmentFile::open(&disk, PATH, other_segment, PREALLOCATED_BYTES).await;
        assert!(matches!(opened, Err(SegmentError::WrongSegment { .. })));
    }

    #[tokio::test]
    async fn opening_a_file_with_a_damaged_header_is_refused() {
        let disk = MemoryDisk::new();
        new_segment(&disk).await;
        disk.corrupt_byte(PATH, 20, 0x01);
        let opened = SegmentFile::open(&disk, PATH, SEGMENT, PREALLOCATED_BYTES).await;
        assert!(matches!(opened, Err(SegmentError::InvalidHeader { .. })));
    }

    // ---- truncation ----

    #[tokio::test]
    async fn truncating_at_a_batch_boundary_discards_what_follows() {
        let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;
        segment
            .truncate_to(SequenceNumber::new(104))
            .await
            .expect("truncate");
        assert_eq!(read_all(&segment).await, (100..=104).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn truncation_survives_a_crash() {
        let disk = MemoryDisk::new();
        let mut segment = segment_with_three_batches(&disk).await;
        segment
            .truncate_to(SequenceNumber::new(102))
            .await
            .expect("truncate");
        disk.crash();
        let (reopened, _stop) = reopen(&disk).await;
        assert_eq!(reopened.local_end(), SequenceNumber::new(102));
    }

    #[tokio::test]
    async fn truncating_to_before_the_first_record_empties_the_copy() {
        let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;
        segment
            .truncate_to(SequenceNumber::new(FIRST_SEQUENCE - 1))
            .await
            .expect("truncate");
        assert_eq!(read_all(&segment).await, Vec::<u64>::new());
    }

    #[tokio::test]
    async fn truncating_at_the_current_end_changes_nothing() {
        let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;
        segment
            .truncate_to(SequenceNumber::new(109))
            .await
            .expect("truncate");
        assert_eq!(read_all(&segment).await, (100..=109).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn truncating_inside_a_batch_is_refused() {
        let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;
        let refused = segment.truncate_to(SequenceNumber::new(106)).await;
        assert!(matches!(
            refused,
            Err(SegmentError::NotABatchBoundary { .. })
        ));
    }

    #[tokio::test]
    async fn truncating_past_the_end_is_refused() {
        let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;
        let refused = segment.truncate_to(SequenceNumber::new(110)).await;
        assert!(matches!(
            refused,
            Err(SegmentError::NotABatchBoundary { .. })
        ));
    }

    #[tokio::test]
    async fn truncating_below_the_segment_is_refused() {
        let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;
        let refused = segment.truncate_to(SequenceNumber::new(50)).await;
        assert!(matches!(
            refused,
            Err(SegmentError::NotABatchBoundary { .. })
        ));
    }

    #[tokio::test]
    async fn segment_accepts_appends_after_a_truncation() {
        let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;
        segment
            .truncate_to(SequenceNumber::new(104))
            .await
            .expect("truncate");
        segment.append(&[batch(105, 2)]).await.expect("append");
        assert_eq!(read_all(&segment).await, (100..=106).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn releasing_unused_space_shrinks_the_file_to_its_content() {
        let disk = MemoryDisk::new();
        let mut segment = segment_with_three_batches(&disk).await;
        segment.release_unused_space().await.expect("release");
        let file_bytes = disk.durable_content(PATH).expect("file exists").len();
        assert_eq!(to_u64(file_bytes), segment.content_bytes());
    }

    #[tokio::test]
    async fn full_file_grows_by_whole_preallocation_steps() {
        let disk = MemoryDisk::new();
        let mut segment = new_segment(&disk).await;
        let payload = Bytes::from(vec![0xAB; 3 * 1024]);
        for sequence in FIRST_SEQUENCE..FIRST_SEQUENCE + 2 {
            let large_batch = Batch::encode(
                SEGMENT,
                SequenceNumber::new(sequence),
                std::slice::from_ref(&payload),
            )
            .expect("valid batch");
            segment.append(&[large_batch]).await.expect("append");
        }
        let file_bytes = disk.durable_content(PATH).expect("file exists").len();
        assert_eq!(to_u64(file_bytes), 2 * PREALLOCATED_BYTES);
    }

    #[tokio::test]
    async fn file_without_preallocation_grows_only_with_its_content() {
        let disk = MemoryDisk::new();
        let mut segment = SegmentFile::create(&disk, PATH, header(), 0)
            .await
            .expect("new segment");
        segment.append(&[batch(100, 3)]).await.expect("append");
        segment.flush().await.expect("flush");
        let file_bytes = disk.durable_content(PATH).expect("file exists").len();
        assert_eq!(to_u64(file_bytes), segment.content_bytes());
    }

    // ---- scrubbing ----

    #[tokio::test]
    async fn intact_segment_verifies() {
        let segment = segment_with_three_batches(&MemoryDisk::new()).await;
        assert!(segment.verify().await.is_ok());
    }

    #[tokio::test]
    async fn silent_corruption_is_found_by_verification() {
        let disk = MemoryDisk::new();
        let segment = segment_with_three_batches(&disk).await;
        disk.corrupt_byte(PATH, SEGMENT_HEADER_BYTES + 30, 0x04);
        assert!(matches!(
            segment.verify().await,
            Err(SegmentError::Corrupt { .. })
        ));
    }

    #[tokio::test]
    async fn silent_corruption_is_found_by_a_read() {
        let disk = MemoryDisk::new();
        let segment = segment_with_three_batches(&disk).await;
        disk.corrupt_byte(PATH, SEGMENT_HEADER_BYTES + 30, 0x04);
        let read = segment
            .read_frames(
                SequenceNumber::new(100),
                SequenceNumber::new(109),
                usize::MAX,
            )
            .await;
        assert!(matches!(read, Err(SegmentError::Corrupt { .. })));
    }

    // ---- large files: chunked scans and the index ----

    /// Appends random batches until the file spans several scan chunks and
    /// many index intervals, then checks that reads from arbitrary positions
    /// and a reopen agree with what was appended.
    #[tokio::test]
    async fn large_segment_reads_and_recovers_across_chunk_boundaries() {
        let mut random = DeterministicRandom::from_seed(11);
        let disk = MemoryDisk::new();
        let mut segment = new_segment(&disk).await;
        let mut next_sequence = FIRST_SEQUENCE;
        while segment.content_bytes() < 3 * to_u64(SCAN_CHUNK_BYTES) {
            let payload_bytes = usize::try_from(random.between(1, 96 * 1024)).expect("small");
            let payload = Bytes::from(random.bytes(payload_bytes));
            let large_batch =
                Batch::encode(SEGMENT, SequenceNumber::new(next_sequence), &[payload])
                    .expect("valid batch");
            segment.append(&[large_batch]).await.expect("append");
            next_sequence += 1;
        }
        segment.flush().await.expect("flush");
        let last_sequence = next_sequence - 1;

        for _ in 0..20 {
            let from_sequence = random.between(FIRST_SEQUENCE, last_sequence);
            let frames = segment
                .read_frames(
                    SequenceNumber::new(from_sequence),
                    SequenceNumber::new(last_sequence),
                    256 * 1024,
                )
                .await
                .expect("read");
            assert_eq!(sequences_in(&frames).first(), Some(&from_sequence));
        }
        disk.crash();
        let (reopened, stop) = reopen(&disk).await;
        assert_eq!(
            (reopened.local_end().get(), stop),
            (last_sequence, ScanStop::EndOfData)
        );
    }
}
