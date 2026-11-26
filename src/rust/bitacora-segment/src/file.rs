//! One replica's copy of a segment: a file of batch frames on a [`Disk`].
//!
//! # Why this file exists
//!
//! A replica's whole job is to hold copies of segments and to say truthfully what each copy contains
//! (design, section 7). The byte layout is defined in `format.rs`, and a disk is reached through
//! `disk.rs`. Something has to put the two together and keep, for one copy, the facts the replication
//! protocol asks about: where the copy ends, what of it is durable, and whether the bytes it once
//! acknowledged still verify. This file is that something. It also owns the rules that let a copy survive
//! a crash: what is written in which order, and how a file is read back after a restart.
//!
//! # What it contains
//!
//! - [`SegmentFile`]: the copy, with its operations: create, open, append, flush, read, truncate, release
//!   unused space and verify.
//! - [`SegmentError`]: why an operation failed.
//!
//! # How it works
//!
//! **Creation.** The disk creates the file with its header in one atomic step and reserves the
//! preallocated size for it. The file later grows by the same step whenever it fills up, so that the flush
//! which acknowledges an append persists data only, never a change of the file's size.
//!
//! **Append and flush.** An append checks that the batches continue the copy, writes all their frames with
//! one write, and only then moves the copy's end and its index. Nothing is durable until a flush returns;
//! the replica acknowledges after that.
//!
//! **The flushed mark.** Each flush records how far the file is flushed. The mark in the header that makes
//! this known after a restart is written with the *next* append, so that it rides on that append's flush
//! and costs no flush of its own. It therefore lags by one flush while a stream is busy;
//! [`SegmentFile::flush_mark`] brings it up to date when the stream goes idle.
//!
//! **Opening.** After a restart the file is scanned from the header: the longest run of valid,
//! consecutive batches is the copy's content, and the scan rebuilds the index on the way. If that run ends
//! before the flushed mark, flushed records are damaged and the copy is refused. If it ends after the mark,
//! the rest is a torn tail that was never acknowledged; it is left in place and overwritten by the next
//! append.
//!
//! **Reading.** A read looks up the closest index entry before the wanted record and reads the file in
//! chunks of `SCAN_CHUNK_BYTES` from there, verifying every frame again. One private loop,
//! `visit_batches`, does this for reads, truncation, verification and recovery; each of them passes the
//! visitor that says what to do with a batch.
//!
//! **Truncation.** A copy is cut only at a batch boundary. The mark is lowered and flushed first, then the
//! file is cut: in the other order a crash could leave a file shorter than its mark, which would read as
//! damage.
//!
//! # Invariants
//!
//! - A [`SegmentFile`] has a single owner. Every operation that changes it takes `&mut self`, and the
//!   replica runs them from one task, so no locking is needed here and a dropped request can never leave a
//!   half-applied change.
//! - The copy's end, its end offset and its index always describe the same batches: they change together,
//!   and only after the write they describe succeeded.
//! - The content is whole frames, consecutive in sequence, from the header on. A batch is never split.
//! - The flushed mark never runs ahead of what was flushed. It may lag behind.
//! - After a failed flush the copy must be treated as suspect and never flushed again: the kernel may have
//!   dropped the unwritten pages, so a second flush could falsely succeed.
//! - Appends, flushes and reads run for every request, so they must stay cheap. An append copies each
//!   frame once into one buffer and makes one write. A flush is one call to the disk. A read copies each
//!   returned frame once and verifies it where it was read. `visit_batches` is the loop that reads and
//!   verifies for all of them; it is kept whole in one function, so that nothing hides between reading a
//!   chunk and deciding what its end means.
//!
//! # See also
//!
//! - `format.rs`: the bytes this file stores, and the scan it recovers with.
//! - `index.rs`: the sparse index a read starts from.
//! - `disk.rs`: what the disk promises, and `memory_disk.rs`, which models the crashes tested here.
//! - `state.rs`: the fencing and sealing state kept next to the copy.

use std::io;

use bitacora_types::{SegmentAddress, SequenceNumber};
use bytes::{Bytes, BytesMut};
use thiserror::Error;

use crate::disk::{Disk, DiskFile};
use crate::format::{
    Batch, FlushedMark, HeaderError, SEGMENT_HEADER_BYTES, ScanStop, SegmentHeader, scan_valid_prefix, to_u64,
};
use crate::index::{IndexEntry, SparseIndex};

/// How much of a segment file one read brings into memory while scanning: 1 MiB. Large enough that a scan
/// makes few reads, small enough that many scans at once do not exhaust memory.
const SCAN_CHUNK_BYTES: usize = 1024 * 1024;

/// Where the first batch frame of a segment file starts: right after the header.
const HEADER_END_OFFSET: u64 = SEGMENT_HEADER_BYTES as u64;

/// The mark of a file whose header is all that is known to be flushed.
const NOTHING_FLUSHED: FlushedMark = FlushedMark {
    flushed_bytes: HEADER_END_OFFSET,
    generation: 0,
};

/// Why an operation on a segment file failed.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use bitacora_segment::format::{Batch, SegmentHeader};
/// use bitacora_segment::{MemoryDisk, SegmentError, SegmentFile};
/// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
/// use bytes::Bytes;
///
/// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
/// let header = SegmentHeader {
///     address: segment,
///     epoch: Epoch::new(5),
///     first_sequence: SequenceNumber::new(100),
/// };
/// let disk = MemoryDisk::new();
/// let mut copy = SegmentFile::create(&disk, "7/3.seg", header, 4_096).await?;
/// let skips_a_record = Batch::encode(segment, SequenceNumber::new(101), &[Bytes::from_static(b"late")])?;
///
/// let refused = copy.append(&[skips_a_record]).await;
///
/// assert!(matches!(refused, Err(SegmentError::OutOfOrder { .. })));
/// # Ok(())
/// # }
/// ```
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
    #[error("cannot cut the copy at {requested_end}: it ends at {local_end} and batches are kept whole")]
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
    /// Visit the next batch too.
    Continue,
    /// The visitor has what it came for; end the scan after this batch.
    Stop,
}

/// Where and why a scan over part of the file ended.
#[derive(Clone, Copy, Debug)]
struct RegionEnd {
    /// The offset right after the last valid batch that was visited.
    offset_bytes: u64,
    /// Why the scan found no further valid batch. `None` if the visitor stopped the scan.
    stop: Option<ScanStop>,
}

/// One replica's copy of a segment.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use bitacora_segment::format::{Batch, SegmentHeader, decode_frames};
/// use bitacora_segment::{MemoryDisk, SegmentFile};
/// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
/// use bytes::Bytes;
///
/// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
/// let header = SegmentHeader {
///     address: segment,
///     epoch: Epoch::new(5),
///     first_sequence: SequenceNumber::new(100),
/// };
/// let disk = MemoryDisk::new();
/// let mut copy = SegmentFile::create(&disk, "7/3.seg", header, 4_096).await?;
/// let batch = Batch::encode(segment, SequenceNumber::new(100), &[Bytes::from_static(b"hello")])?;
///
/// copy.append(&[batch.clone()]).await?;
/// copy.flush().await?;
///
/// let frames = copy
///     .read_frames(SequenceNumber::new(100), SequenceNumber::new(100), usize::MAX)
///     .await?;
/// assert_eq!(decode_frames(segment, &frames)?, [batch]);
/// # Ok(())
/// # }
/// ```
pub struct SegmentFile {
    /// The open file on the disk.
    file: Box<dyn DiskFile>,
    /// The file's path on the disk, for error messages.
    path: String,
    /// The identity the file starts with.
    header: SegmentHeader,
    /// Where the next batch frame goes.
    end_offset_bytes: u64,
    /// The length the file is currently extended to.
    allocated_bytes: u64,
    /// How much the file grows by when it fills up. Zero lets writes extend it one batch at a time.
    growth_bytes: u64,
    /// Where the content ended at the last successful flush.
    flushed_end_bytes: u64,
    /// The flushed mark last written to the header. It lags behind `flushed_end_bytes` until the next
    /// append or [`SegmentFile::flush_mark`].
    written_mark: FlushedMark,
    /// The last sequence number held, or the position before the segment's first record if the copy is
    /// empty.
    local_end: SequenceNumber,
    /// Where roughly every 64 KiB a batch starts, so a read need not scan from the header.
    index: SparseIndex,
}

impl std::fmt::Debug for SegmentFile {
    /// Shows the copy's identity and extent, leaving out the file handle and the index.
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
    /// Creates an empty copy and makes it durable.
    ///
    /// The disk creates the file with its header in one atomic step, so a copy that exists always has a
    /// valid header: a crash or a failed flush during creation cannot leave a file that looks damaged.
    /// The file is preallocated, and grows by the same amount whenever it fills up, so that a flush only
    /// has to persist data and not a change of the file's size.
    ///
    /// # Arguments
    ///
    /// * `disk` - The disk to create the file on.
    /// * `path` - The file's path on that disk.
    /// * `header` - The identity of the segment the file is a copy of.
    /// * `preallocated_bytes` - How much space to reserve now, and again each time the file fills up. Zero
    ///   reserves nothing and lets every append extend the file.
    ///
    /// # Returns
    ///
    /// The empty copy. Its end is the position before the segment's first record.
    ///
    /// # Errors
    ///
    /// Fails with [`SegmentError::Io`] if the file exists or the disk refuses the operation.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use bitacora_segment::format::SegmentHeader;
    /// use bitacora_segment::{MemoryDisk, SegmentFile};
    /// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
    ///
    /// let header = SegmentHeader {
    ///     address: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
    ///     epoch: Epoch::new(5),
    ///     first_sequence: SequenceNumber::new(100),
    /// };
    /// let disk = MemoryDisk::new();
    ///
    /// let copy = SegmentFile::create(&disk, "7/3.seg", header, 4_096).await?;
    ///
    /// assert_eq!(copy.local_end(), SequenceNumber::new(99));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create(
        disk: &dyn Disk,
        path: &str,
        header: SegmentHeader,
        preallocated_bytes: u64,
    ) -> Result<Self, SegmentError> {
        let allocated_bytes = preallocated_bytes.max(HEADER_END_OFFSET);
        let file = disk
            .create_file(path, Bytes::copy_from_slice(&header.encode()), allocated_bytes)
            .await
            .map_err(|source| io_failure("create", path, source))?;
        Ok(Self {
            file,
            path: path.to_owned(),
            header,
            end_offset_bytes: HEADER_END_OFFSET,
            allocated_bytes,
            growth_bytes: preallocated_bytes,
            flushed_end_bytes: HEADER_END_OFFSET,
            written_mark: NOTHING_FLUSHED,
            local_end: header.first_sequence.preceding(),
            index: SparseIndex::new(),
        })
    }

    /// Opens an existing copy and scans it, as after a restart.
    ///
    /// The scan keeps the longest run of valid, consecutive batches and reports why it stopped. Whatever
    /// follows was never acknowledged; it is left in place and overwritten by the next append. The file
    /// itself is not modified.
    ///
    /// # Arguments
    ///
    /// * `disk` - The disk the file is on.
    /// * `path` - The file's path on that disk.
    /// * `expected` - The segment the caller expects the file to be a copy of.
    /// * `growth_bytes` - How much the file grows by when it fills up, as `preallocated_bytes` in
    ///   [`SegmentFile::create`].
    ///
    /// # Returns
    ///
    /// The copy, and why the scan found no further batch: [`ScanStop::EndOfData`] for a file that ends
    /// cleanly, anything else for a torn tail.
    ///
    /// # Errors
    ///
    /// * [`SegmentError::Io`] if the file is missing or cannot be read.
    /// * [`SegmentError::InvalidHeader`] if it does not start with a valid header.
    /// * [`SegmentError::WrongSegment`] if it is a copy of another segment than `expected`.
    /// * [`SegmentError::Corrupt`] if the valid batches end before the file's flushed mark: records that
    ///   were flushed, and so possibly acknowledged, are damaged, and the copy can no longer vouch for
    ///   what it held.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use bitacora_segment::format::{Batch, ScanStop, SegmentHeader};
    /// use bitacora_segment::{MemoryDisk, SegmentFile};
    /// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
    /// use bytes::Bytes;
    ///
    /// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
    /// let header = SegmentHeader {
    ///     address: segment,
    ///     epoch: Epoch::new(5),
    ///     first_sequence: SequenceNumber::new(100),
    /// };
    /// let disk = MemoryDisk::new();
    /// let mut copy = SegmentFile::create(&disk, "7/3.seg", header, 4_096).await?;
    /// let batch = Batch::encode(segment, SequenceNumber::new(100), &[Bytes::from_static(b"kept")])?;
    /// copy.append(&[batch]).await?;
    /// copy.flush().await?;
    /// disk.crash();
    ///
    /// let (reopened, scan_stop) = SegmentFile::open(&disk, "7/3.seg", segment, 4_096).await?;
    ///
    /// assert_eq!(reopened.local_end(), SequenceNumber::new(100));
    /// assert_eq!(scan_stop, ScanStop::EndOfData);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn open(
        disk: &dyn Disk,
        path: &str,
        expected: SegmentAddress,
        growth_bytes: u64,
    ) -> Result<(Self, ScanStop), SegmentError> {
        let file = disk
            .open_file(path)
            .await
            .map_err(|source| io_failure("open", path, source))?;
        let header_bytes = file
            .read_at(0, SEGMENT_HEADER_BYTES)
            .await
            .map_err(|source| io_failure("read header", path, source))?;
        let header = decode_expected_header(path, expected, &header_bytes)?;
        let file_bytes = file
            .length_bytes()
            .await
            .map_err(|source| io_failure("measure", path, source))?;

        let mut segment = Self {
            file,
            path: path.to_owned(),
            header,
            end_offset_bytes: HEADER_END_OFFSET,
            allocated_bytes: file_bytes,
            growth_bytes,
            flushed_end_bytes: HEADER_END_OFFSET,
            written_mark: FlushedMark::latest_in(header.address, &header_bytes).unwrap_or(NOTHING_FLUSHED),
            local_end: header.first_sequence.preceding(),
            index: SparseIndex::new(),
        };
        let stop = segment.recover_content(file_bytes).await?;
        Ok((segment, stop))
    }

    /// Returns the header the copy was created with.
    ///
    /// # Returns
    ///
    /// The identity the file starts with.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use bitacora_segment::format::SegmentHeader;
    /// use bitacora_segment::{MemoryDisk, SegmentFile};
    /// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
    ///
    /// let header = SegmentHeader {
    ///     address: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
    ///     epoch: Epoch::new(5),
    ///     first_sequence: SequenceNumber::new(100),
    /// };
    /// let copy = SegmentFile::create(&MemoryDisk::new(), "7/3.seg", header, 4_096).await?;
    ///
    /// assert_eq!(copy.header(), header);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub const fn header(&self) -> SegmentHeader {
        self.header
    }

    /// Returns where the copy ends.
    ///
    /// # Returns
    ///
    /// The last sequence number the copy holds, or the position before the segment's first record if it
    /// is empty. It counts appended batches whether or not they were flushed.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use bitacora_segment::format::{Batch, SegmentHeader};
    /// use bitacora_segment::{MemoryDisk, SegmentFile};
    /// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
    /// use bytes::Bytes;
    ///
    /// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
    /// let header = SegmentHeader {
    ///     address: segment,
    ///     epoch: Epoch::new(5),
    ///     first_sequence: SequenceNumber::new(100),
    /// };
    /// let mut copy = SegmentFile::create(&MemoryDisk::new(), "7/3.seg", header, 4_096).await?;
    /// let payloads = [Bytes::from_static(b"a"), Bytes::from_static(b"b")];
    ///
    /// copy.append(&[Batch::encode(segment, SequenceNumber::new(100), &payloads)?])
    ///     .await?;
    ///
    /// assert_eq!(copy.local_end(), SequenceNumber::new(101));
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub const fn local_end(&self) -> SequenceNumber {
        self.local_end
    }

    /// Returns how many bytes of the file hold the header and valid batches.
    ///
    /// # Returns
    ///
    /// The offset right after the last batch. Preallocated space beyond it is not counted.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use bitacora_segment::format::{SEGMENT_HEADER_BYTES, SegmentHeader};
    /// use bitacora_segment::{MemoryDisk, SegmentFile};
    /// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
    ///
    /// let header = SegmentHeader {
    ///     address: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
    ///     epoch: Epoch::new(5),
    ///     first_sequence: SequenceNumber::new(100),
    /// };
    /// let copy = SegmentFile::create(&MemoryDisk::new(), "7/3.seg", header, 4_096).await?;
    ///
    /// assert_eq!(copy.content_bytes(), u64::try_from(SEGMENT_HEADER_BYTES)?);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub const fn content_bytes(&self) -> u64 {
        self.end_offset_bytes
    }

    /// Writes batches after the copy's end.
    ///
    /// All the frames go to the disk in one write. They are readable at once but not durable until
    /// [`SegmentFile::flush`] returns.
    ///
    /// # Arguments
    ///
    /// * `batches` - The batches to add. The first must start right after the copy's end, and each later
    ///   one right after the one before it. An empty slice changes nothing.
    ///
    /// # Errors
    ///
    /// * [`SegmentError::OutOfOrder`] if the batches do not continue the copy. Nothing is changed.
    /// * [`SegmentError::Io`] if the disk refuses the write. The copy's end does not move.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use bitacora_segment::format::{Batch, SegmentHeader};
    /// use bitacora_segment::{MemoryDisk, SegmentFile};
    /// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
    /// use bytes::Bytes;
    ///
    /// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
    /// let header = SegmentHeader {
    ///     address: segment,
    ///     epoch: Epoch::new(5),
    ///     first_sequence: SequenceNumber::new(100),
    /// };
    /// let mut copy = SegmentFile::create(&MemoryDisk::new(), "7/3.seg", header, 4_096).await?;
    /// let first_batch = Batch::encode(segment, SequenceNumber::new(100), &[Bytes::from_static(b"a")])?;
    /// let second_batch = Batch::encode(segment, SequenceNumber::new(101), &[Bytes::from_static(b"b")])?;
    ///
    /// copy.append(&[first_batch, second_batch]).await?;
    ///
    /// assert_eq!(copy.local_end(), SequenceNumber::new(101));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn append(&mut self, batches: &[Batch]) -> Result<(), SegmentError> {
        self.require_continuation(batches)?;

        let total_bytes: usize = batches.iter().map(|batch| batch.frame().len()).sum();
        let mut frames = BytesMut::with_capacity(total_bytes);
        for batch in batches {
            frames.extend_from_slice(batch.frame());
        }
        self.grow_to_fit(self.end_offset_bytes + to_u64(total_bytes)).await?;
        // The mark for what earlier flushes covered rides on the flush that follows this append, at no
        // cost of its own.
        self.write_mark_if_behind().await?;
        self.file
            .write_at(self.end_offset_bytes, frames.freeze())
            .await
            .map_err(|source| io_failure("append", &self.path, source))?;

        for batch in batches {
            self.index.observe_batch(batch.first_sequence(), self.end_offset_bytes);
            self.end_offset_bytes += to_u64(batch.frame().len());
            self.local_end = batch.last_sequence();
        }
        Ok(())
    }

    /// Makes every appended batch durable.
    ///
    /// # Errors
    ///
    /// Fails with [`SegmentError::Io`] if the flush fails. The copy must then be treated as suspect and
    /// never flushed again: the kernel may have dropped the unwritten pages, so a retry could falsely
    /// succeed.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use bitacora_segment::format::{Batch, SegmentHeader};
    /// use bitacora_segment::{MemoryDisk, SegmentFile};
    /// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
    /// use bytes::Bytes;
    ///
    /// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
    /// let header = SegmentHeader {
    ///     address: segment,
    ///     epoch: Epoch::new(5),
    ///     first_sequence: SequenceNumber::new(100),
    /// };
    /// let disk = MemoryDisk::new();
    /// let mut copy = SegmentFile::create(&disk, "7/3.seg", header, 4_096).await?;
    /// copy.append(&[Batch::encode(segment, SequenceNumber::new(100), &[Bytes::from_static(b"a")])?])
    ///     .await?;
    ///
    /// copy.flush().await?;
    ///
    /// disk.crash();
    /// let (reopened, _scan_stop) = SegmentFile::open(&disk, "7/3.seg", segment, 4_096).await?;
    /// assert_eq!(reopened.local_end(), SequenceNumber::new(100));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn flush(&mut self) -> Result<(), SegmentError> {
        self.file
            .sync_data()
            .await
            .map_err(|source| io_failure("flush", &self.path, source))?;
        self.flushed_end_bytes = self.end_offset_bytes;
        Ok(())
    }

    /// Brings the file's flushed mark up to date and makes it durable, if it lags.
    ///
    /// Called when a stream goes idle, so that the mark covers everything acknowledged and damage to any
    /// of it is detected when the copy is next opened. It does nothing, and touches no disk, when the mark
    /// already covers everything flushed.
    ///
    /// # Errors
    ///
    /// Fails with [`SegmentError::Io`] as [`SegmentFile::flush`] does, with the same consequence.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use bitacora_segment::format::{Batch, SEGMENT_HEADER_BYTES, SegmentHeader};
    /// use bitacora_segment::{MemoryDisk, SegmentError, SegmentFile};
    /// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
    /// use bytes::Bytes;
    ///
    /// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
    /// let header = SegmentHeader {
    ///     address: segment,
    ///     epoch: Epoch::new(5),
    ///     first_sequence: SequenceNumber::new(100),
    /// };
    /// let disk = MemoryDisk::new();
    /// let mut copy = SegmentFile::create(&disk, "7/3.seg", header, 4_096).await?;
    /// copy.append(&[Batch::encode(segment, SequenceNumber::new(100), &[Bytes::from_static(b"a")])?])
    ///     .await?;
    /// copy.flush().await?;
    ///
    /// copy.flush_mark().await?;
    ///
    /// // Damage to the flushed record is now told apart from a torn write.
    /// disk.corrupt_byte("7/3.seg", SEGMENT_HEADER_BYTES + 21, 0x01);
    /// let reopened = SegmentFile::open(&disk, "7/3.seg", segment, 4_096).await;
    /// assert!(matches!(reopened, Err(SegmentError::Corrupt { .. })));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn flush_mark(&mut self) -> Result<(), SegmentError> {
        if self.flushed_end_bytes <= self.written_mark.flushed_bytes {
            return Ok(());
        }
        self.write_mark_if_behind().await?;
        self.file
            .sync_data()
            .await
            .map_err(|source| io_failure("flush mark", &self.path, source))
    }

    /// Reads whole batch frames from a range of the copy.
    ///
    /// The read starts with the batch that holds `from_sequence`, even if that batch starts earlier, and
    /// never returns a batch that ends after `through_sequence`. Frames are verified again as they are
    /// read. The result stops after the first batch that takes it to `max_bytes`, so it always makes
    /// progress.
    ///
    /// # Arguments
    ///
    /// * `from_sequence` - The first record wanted.
    /// * `through_sequence` - The last record that may be returned.
    /// * `max_bytes` - How many bytes of frames to aim for. At least one batch is returned if the range
    ///   holds one, however large it is.
    ///
    /// # Returns
    ///
    /// The frames, back to back, ready for [`decode_frames`](crate::format::decode_frames). Empty if the
    /// copy holds nothing in the range.
    ///
    /// # Errors
    ///
    /// * [`SegmentError::Corrupt`] if a frame that was once valid no longer verifies.
    /// * [`SegmentError::Io`] if the disk fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use bitacora_segment::format::{Batch, SegmentHeader, decode_frames};
    /// use bitacora_segment::{MemoryDisk, SegmentFile};
    /// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
    /// use bytes::Bytes;
    ///
    /// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
    /// let header = SegmentHeader {
    ///     address: segment,
    ///     epoch: Epoch::new(5),
    ///     first_sequence: SequenceNumber::new(100),
    /// };
    /// let mut copy = SegmentFile::create(&MemoryDisk::new(), "7/3.seg", header, 4_096).await?;
    /// let first_batch = Batch::encode(segment, SequenceNumber::new(100), &[Bytes::from_static(b"a")])?;
    /// let second_batch = Batch::encode(segment, SequenceNumber::new(101), &[Bytes::from_static(b"b")])?;
    /// copy.append(&[first_batch, second_batch.clone()]).await?;
    ///
    /// let frames = copy
    ///     .read_frames(SequenceNumber::new(101), SequenceNumber::new(200), usize::MAX)
    ///     .await?;
    ///
    /// assert_eq!(decode_frames(segment, &frames)?, [second_batch]);
    /// # Ok(())
    /// # }
    /// ```
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
        let collect_wanted_frames = |batch: &Batch, _offset_bytes: u64| {
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
        };
        let region_end = self
            .visit_batches(
                start.offset_bytes,
                start.first_sequence,
                self.end_offset_bytes,
                collect_wanted_frames,
            )
            .await?;
        self.require_intact(region_end)?;
        Ok(frames.freeze())
    }

    /// Discards every record above a position and makes the cut durable.
    ///
    /// The copy is cut only where a batch ends, because a frame cannot be split. The flushed mark is
    /// lowered and flushed before the file is cut, so that no crash leaves a file shorter than its mark.
    ///
    /// # Arguments
    ///
    /// * `end` - The last record to keep. The position before the segment's first record empties the
    ///   copy. The copy's current end changes nothing.
    ///
    /// # Errors
    ///
    /// * [`SegmentError::NotABatchBoundary`] if `end` lies beyond the copy's end, below the segment, or
    ///   inside a batch. Nothing is changed.
    /// * [`SegmentError::Corrupt`] if the batches read to find the cut no longer verify.
    /// * [`SegmentError::Io`] if the disk fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use bitacora_segment::format::{Batch, SegmentHeader};
    /// use bitacora_segment::{MemoryDisk, SegmentFile};
    /// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
    /// use bytes::Bytes;
    ///
    /// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
    /// let header = SegmentHeader {
    ///     address: segment,
    ///     epoch: Epoch::new(5),
    ///     first_sequence: SequenceNumber::new(100),
    /// };
    /// let mut copy = SegmentFile::create(&MemoryDisk::new(), "7/3.seg", header, 4_096).await?;
    /// let first_batch = Batch::encode(segment, SequenceNumber::new(100), &[Bytes::from_static(b"a")])?;
    /// let second_batch = Batch::encode(segment, SequenceNumber::new(101), &[Bytes::from_static(b"b")])?;
    /// copy.append(&[first_batch, second_batch]).await?;
    ///
    /// copy.truncate_to(SequenceNumber::new(100)).await?;
    ///
    /// assert_eq!(copy.local_end(), SequenceNumber::new(100));
    /// # Ok(())
    /// # }
    /// ```
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
        let Some(cut_offset_bytes) = self.offset_of_batch_starting_at(end.next()).await? else {
            return Err(not_a_boundary);
        };

        self.lower_mark_to(cut_offset_bytes).await?;
        self.file
            .set_length(cut_offset_bytes)
            .await
            .map_err(|source| io_failure("truncate", &self.path, source))?;
        self.index.forget_from(cut_offset_bytes);
        self.allocated_bytes = cut_offset_bytes;
        self.end_offset_bytes = cut_offset_bytes;
        self.flushed_end_bytes = self.flushed_end_bytes.min(cut_offset_bytes);
        self.local_end = end;
        Ok(())
    }

    /// Gives back the preallocated space the copy never used.
    ///
    /// Called once a segment is sealed and can no longer grow. The file is cut to its content, durably.
    ///
    /// # Errors
    ///
    /// Fails with [`SegmentError::Io`] if the disk refuses the operation.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use bitacora_segment::format::{SEGMENT_HEADER_BYTES, SegmentHeader};
    /// use bitacora_segment::{MemoryDisk, SegmentFile};
    /// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
    ///
    /// let header = SegmentHeader {
    ///     address: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
    ///     epoch: Epoch::new(5),
    ///     first_sequence: SequenceNumber::new(100),
    /// };
    /// let disk = MemoryDisk::new();
    /// let mut copy = SegmentFile::create(&disk, "7/3.seg", header, 4_096).await?;
    ///
    /// copy.release_unused_space().await?;
    ///
    /// let file_content = disk.durable_content("7/3.seg").ok_or("the file exists")?;
    /// assert_eq!(file_content.len(), SEGMENT_HEADER_BYTES);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn release_unused_space(&mut self) -> Result<(), SegmentError> {
        self.file
            .set_length(self.end_offset_bytes)
            .await
            .map_err(|source| io_failure("release unused space", &self.path, source))?;
        self.allocated_bytes = self.end_offset_bytes;
        Ok(())
    }

    /// Rereads the whole copy from disk and verifies every checksum, as the background scrubber does.
    ///
    /// # Errors
    ///
    /// * [`SegmentError::Corrupt`] if any stored frame no longer verifies.
    /// * [`SegmentError::Io`] if the disk fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use bitacora_segment::format::{Batch, SEGMENT_HEADER_BYTES, SegmentHeader};
    /// use bitacora_segment::{MemoryDisk, SegmentError, SegmentFile};
    /// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
    /// use bytes::Bytes;
    ///
    /// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
    /// let header = SegmentHeader {
    ///     address: segment,
    ///     epoch: Epoch::new(5),
    ///     first_sequence: SequenceNumber::new(100),
    /// };
    /// let disk = MemoryDisk::new();
    /// let mut copy = SegmentFile::create(&disk, "7/3.seg", header, 4_096).await?;
    /// copy.append(&[Batch::encode(segment, SequenceNumber::new(100), &[Bytes::from_static(b"a")])?])
    ///     .await?;
    /// copy.verify().await?;
    ///
    /// disk.corrupt_byte("7/3.seg", SEGMENT_HEADER_BYTES + 21, 0x01);
    ///
    /// assert!(matches!(copy.verify().await, Err(SegmentError::Corrupt { .. })));
    /// # Ok(())
    /// # }
    /// ```
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

    /// Scans the whole file for valid batches and adopts what it finds as the copy's content: its end,
    /// its index and how far it is flushed.
    ///
    /// Returns why the scan found no further batch. Fails with [`SegmentError::Corrupt`] if the valid
    /// batches end before the flushed mark, and with [`SegmentError::Io`] if the disk fails; the copy is
    /// then left as it was.
    async fn recover_content(&mut self, file_bytes: u64) -> Result<ScanStop, SegmentError> {
        let mut index = SparseIndex::new();
        let mut local_end = self.local_end;
        let remember_batch = |batch: &Batch, offset_bytes: u64| {
            index.observe_batch(batch.first_sequence(), offset_bytes);
            local_end = batch.last_sequence();
            Visit::Continue
        };
        let region_end = self
            .visit_batches(
                HEADER_END_OFFSET,
                self.header.first_sequence,
                file_bytes,
                remember_batch,
            )
            .await?;
        let stop = region_end.stop.expect("the recovery visitor never stops the scan");
        if region_end.offset_bytes < self.written_mark.flushed_bytes {
            return Err(SegmentError::Corrupt {
                path: self.path.clone(),
                offset_bytes: region_end.offset_bytes,
                reason: format!(
                    "valid records end here, but the file was flushed through byte {}",
                    self.written_mark.flushed_bytes
                ),
            });
        }
        self.index = index;
        self.local_end = local_end;
        self.end_offset_bytes = region_end.offset_bytes;
        // Whatever survived a restart is on the disk.
        self.flushed_end_bytes = region_end.offset_bytes;
        Ok(stop)
    }

    /// Checks that `batches` start right after the copy's end and follow one another without a gap.
    ///
    /// Fails with [`SegmentError::OutOfOrder`] at the first batch that does not.
    fn require_continuation(&self, batches: &[Batch]) -> Result<(), SegmentError> {
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
        Ok(())
    }

    /// Finds the file offset of the batch whose first record is `first_sequence`.
    ///
    /// Returns `None` if no batch starts there: the position is inside a batch. Fails with
    /// [`SegmentError::Corrupt`] if the batches read on the way no longer verify, and with
    /// [`SegmentError::Io`] if the disk fails.
    async fn offset_of_batch_starting_at(&self, first_sequence: SequenceNumber) -> Result<Option<u64>, SegmentError> {
        let start = self.scan_start_for(first_sequence);
        let mut found_offset_bytes = None;
        let look_for_the_batch = |batch: &Batch, offset_bytes: u64| {
            if batch.first_sequence() == first_sequence {
                found_offset_bytes = Some(offset_bytes);
            }
            if batch.first_sequence() >= first_sequence {
                Visit::Stop
            } else {
                Visit::Continue
            }
        };
        let region_end = self
            .visit_batches(
                start.offset_bytes,
                start.first_sequence,
                self.end_offset_bytes,
                look_for_the_batch,
            )
            .await?;
        self.require_intact(region_end)?;
        Ok(found_offset_bytes)
    }

    /// Lowers the flushed mark to `cut_offset_bytes`, durably, if it is above it.
    ///
    /// Done before a file is cut there: in the other order a crash could leave a file shorter than its
    /// mark, which reads as damage.
    async fn lower_mark_to(&mut self, cut_offset_bytes: u64) -> Result<(), SegmentError> {
        if self.written_mark.flushed_bytes <= cut_offset_bytes {
            return Ok(());
        }
        self.write_mark(cut_offset_bytes).await?;
        self.file
            .sync_data()
            .await
            .map_err(|source| io_failure("flush mark", &self.path, source))
    }

    /// Writes a new flushed mark if the last one written lags behind what has been flushed. The write
    /// itself is not flushed.
    async fn write_mark_if_behind(&mut self) -> Result<(), SegmentError> {
        if self.flushed_end_bytes > self.written_mark.flushed_bytes {
            self.write_mark(self.flushed_end_bytes).await?;
        }
        Ok(())
    }

    /// Writes a flushed mark saying the file is flushed through `flushed_bytes`, into the slot the
    /// previous mark does not occupy.
    async fn write_mark(&mut self, flushed_bytes: u64) -> Result<(), SegmentError> {
        let mark = FlushedMark {
            flushed_bytes,
            generation: self.written_mark.generation.wrapping_add(1),
        };
        self.file
            .write_at(
                to_u64(mark.slot_offset()),
                Bytes::copy_from_slice(&mark.encode(self.header.address)),
            )
            .await
            .map_err(|source| io_failure("write flushed mark", &self.path, source))?;
        self.written_mark = mark;
        Ok(())
    }

    /// Extends the file by whole growth steps until `needed_bytes` fit.
    ///
    /// Growing ahead of the data keeps the size change out of the flush that acknowledges an append: it
    /// is made durable here, once per step.
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
            .map_err(|source| io_failure("grow", &self.path, source))?;
        self.allocated_bytes = grown_bytes;
        Ok(())
    }

    /// Returns the closest indexed place at or before `sequence` to start a scan from: an index entry, or
    /// the first batch of the file.
    fn scan_start_for(&self, sequence: SequenceNumber) -> IndexEntry {
        self.index.entry_at_or_before(sequence).unwrap_or(IndexEntry {
            first_sequence: self.header.first_sequence,
            offset_bytes: HEADER_END_OFFSET,
        })
    }

    /// Fails with [`SegmentError::Corrupt`] unless a scan of acknowledged content ended at the copy's end
    /// or was stopped by its visitor.
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

    /// Visits the consecutive valid batches found between `start_offset_bytes` and `limit_offset_bytes`,
    /// reading the file in chunks.
    ///
    /// `start_offset_bytes` must be where a frame starts, and `start_sequence` the sequence number that
    /// frame starts with. `visit` receives each batch and its file offset, in order, and may stop the
    /// scan. A frame larger than a chunk is read again with room for all of it.
    ///
    /// Returns where the scan ended and why. Fails with [`SegmentError::Io`] if a read fails; invalid
    /// bytes do not fail the scan, they end it.
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
            let chunk_bytes = wanted_bytes.min(usize::try_from(remaining_bytes).unwrap_or(usize::MAX));
            let chunk = self
                .file
                .read_at(offset_bytes, chunk_bytes)
                .await
                .map_err(|source| io_failure("read", &self.path, source))?;
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
                // The chunk ended exactly between two frames: read the next one.
                ScanStop::EndOfData if consumed_the_chunk && !reached_the_limit => {}
                ScanStop::Incomplete { needed_bytes } if !reached_the_limit => {
                    // The chunk ended inside a frame: read again from the frame's start, with room for
                    // all of it.
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

/// Reads the header at the start of a file and checks that it names the segment the caller expects.
///
/// Fails with [`SegmentError::InvalidHeader`] if `header_bytes` are not a valid header, and with
/// [`SegmentError::WrongSegment`] if they name another segment than `expected`.
fn decode_expected_header(
    path: &str,
    expected: SegmentAddress,
    header_bytes: &[u8],
) -> Result<SegmentHeader, SegmentError> {
    let header = match SegmentHeader::decode(header_bytes) {
        Ok(header) => header,
        Err(reason) => {
            return Err(SegmentError::InvalidHeader {
                path: path.to_owned(),
                reason,
            });
        }
    };
    if header.address != expected {
        return Err(SegmentError::WrongSegment {
            path: path.to_owned(),
            expected,
            found: header.address,
        });
    }
    Ok(header)
}

/// Wraps a disk error with what was being done and to which file.
fn io_failure(operation: &'static str, path: &str, source: io::Error) -> SegmentError {
    SegmentError::Io {
        operation,
        path: path.to_owned(),
        source,
    }
}

#[cfg(test)]
#[path = "file_test.rs"]
mod tests;
