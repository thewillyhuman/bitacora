//! The byte layout of segment files and batch frames.
//!
//! # Why this file exists
//!
//! A committed record exists as bytes in three places: on the wire between a writer and a replica, in a
//! segment file on each replica's disk, and in an object uploaded to the object store. If each place had
//! an encoding of its own, every hop would encode again, and a checksum computed by the writer would mean
//! nothing at the next one. So there is one layout (design, section 7), and this file is its only
//! definition: the header that opens a segment file, the frame that holds a batch of records, and the
//! scans that find where valid data ends.
//!
//! # What it contains
//!
//! - [`SegmentHeader`]: the identity at the start of a segment file, and [`HeaderError`].
//! - [`FlushedMark`]: the note inside the header of how far the file is known to be flushed.
//! - [`Batch`]: one verified batch frame, with [`BatchRecords`] to walk its records, and the errors
//!   [`EncodeError`] and [`FrameError`].
//! - [`decode_frames`]: the strict decoder for a run of frames in a request or a response, and
//!   [`FramesError`].
//! - [`scan_valid_prefix`]: the forgiving scan that recovers a file, with [`ValidPrefix`] and
//!   [`ScanStop`].
//! - The constants of the layout.
//!
//! # How it works
//!
//! Everything here is a pure function over bytes, so the format is tested exhaustively without a disk.
//! All integers are little-endian.
//!
//! ```text
//! segment file   header | batch frame*
//! header         identity | zero padding to 64 | flushed mark | flushed mark | zero padding to 128
//! identity       magic 8 | format version u32 | stream id u64 | segment id u64 | epoch u64
//!                | first sequence u64 | crc32c u32
//! flushed mark   flushed bytes u64 | generation u32 | crc32c u32
//! batch frame    body length u32 | first sequence u64 | record count u32 | crc32c u32 | body
//! body           record*
//! record         payload length u32 | payload
//! ```
//!
//! A batch checksum covers the stream id, the segment id, the three header fields before it and the body.
//! The two identifiers are not stored in the frame: mixing them in is what detects a frame written to the
//! wrong segment. Covering the first sequence detects a frame at the wrong offset.
//!
//! A body length of zero is not a frame. It is the unwritten, zero-filled space a preallocated file ends
//! with, which is why a batch must hold at least one record.
//!
//! There are two ways to read a run of frames. [`decode_frames`] is strict: every byte must belong to a
//! valid frame, as in a message from the network. [`scan_valid_prefix`] is forgiving: it returns the
//! longest valid start and says why it stopped, because after a crash the end of a file is expected to be
//! torn.
//!
//! # Invariants
//!
//! - The identity never changes after the file is created. The two flushed mark slots are the only bytes
//!   of a segment file that are rewritten.
//! - A [`Batch`] can only come from [`Batch::encode`] or [`Batch::decode`], so holding one proves that its
//!   checksum and structure were verified.
//! - Decoding copies no payload: a decoded batch, and the records read from it, share the memory of the
//!   buffer they were decoded from.
//! - No decoder panics, whatever bytes it is given. `tests/decoder_fuzz_test.rs` feeds them noise.
//! - The layout is format version 1 and is held to golden bytes by `tests/format_golden_test.rs`.
//! - Encoding and decoding run once per batch on every append and every read, so they must stay cheap.
//!   [`Batch::encode`] makes one allocation, the frame. [`Batch::decode`] makes none; [`decode_frames`] and
//!   [`scan_valid_prefix`] allocate only the list of batches they return. The loops that walk a body or a
//!   run of frames are each kept whole in one function, so that nothing hides between a byte and its
//!   check.
//!
//! # See also
//!
//! - `file.rs`: the segment file that stores these bytes and recovers them.
//! - `tiered.rs`: the object that carries the same frames in the object store.
//! - `state.rs`: the small file next to a segment file.
//! - `docs/protocol/disk-format.md`: the same layout, for other languages.

use bitacora_types::{Epoch, LogRecord, MAX_RECORD_PAYLOAD_BYTES, SegmentAddress, SegmentId, SequenceNumber, StreamId};
use bytes::{BufMut, Bytes, BytesMut};
use thiserror::Error;

/// The first bytes of every segment file. They tell a segment file from any other file.
pub const SEGMENT_MAGIC: [u8; 8] = *b"BITASEG\0";

/// The segment format this build writes. Older formats stay readable.
pub const SEGMENT_FORMAT_VERSION: u32 = 1;

/// The size of the segment header in bytes, flushed marks and padding included.
pub const SEGMENT_HEADER_BYTES: usize = 128;

/// The size of one encoded [`FlushedMark`], in bytes.
pub const FLUSHED_MARK_BYTES: usize = 16;

/// The size of a batch frame header, in bytes.
pub const BATCH_HEADER_BYTES: usize = 20;

/// The size of the length prefix in front of every record payload, in bytes.
pub const RECORD_HEADER_BYTES: usize = 4;

/// The largest batch body the format accepts, in bytes: 8 MiB. It leaves room for one record of the
/// maximum size next to a full batch of small ones.
pub const MAX_BATCH_BODY_BYTES: usize = 8 * 1024 * 1024;

/// The size of an encoded `u32`, in bytes.
const U32_BYTES: usize = 4;

/// The size of an encoded `u64`, in bytes.
const U64_BYTES: usize = 8;

/// Where the format version starts in a segment header, right after the magic.
const HEADER_FORMAT_VERSION_OFFSET: usize = 8;

/// Where the stream id starts in a segment header.
const HEADER_STREAM_ID_OFFSET: usize = 12;

/// Where the segment id starts in a segment header.
const HEADER_SEGMENT_ID_OFFSET: usize = 20;

/// Where the writer's epoch starts in a segment header.
const HEADER_EPOCH_OFFSET: usize = 28;

/// Where the first sequence number starts in a segment header.
const HEADER_FIRST_SEQUENCE_OFFSET: usize = 36;

/// Where the checksum of the identity starts in a segment header. It covers every byte before it.
const HEADER_CHECKSUM_OFFSET: usize = 44;

/// Where the first of the flushed mark slots starts in a segment header.
const FLUSHED_MARKS_OFFSET: usize = 64;

/// How many flushed mark slots a segment header has. Two, so that a torn write of one leaves the other.
const FLUSHED_MARK_SLOT_COUNT: u32 = 2;

/// Where the flushed bytes start in an encoded flushed mark: they are the first field.
const MARK_FLUSHED_BYTES_OFFSET: usize = 0;

/// Where the generation starts in an encoded flushed mark, right after the flushed bytes.
const MARK_GENERATION_OFFSET: usize = 8;

/// Where the checksum starts in an encoded flushed mark. It covers every byte before it.
const MARK_CHECKSUM_OFFSET: usize = 12;

/// Where the body length starts in a batch frame: it is the first field.
const FRAME_BODY_LENGTH_OFFSET: usize = 0;

/// Where the first sequence number starts in a batch frame.
const FRAME_FIRST_SEQUENCE_OFFSET: usize = 4;

/// Where the record count starts in a batch frame.
const FRAME_RECORD_COUNT_OFFSET: usize = 12;

/// Where the checksum starts in a batch frame. It is the last field of the frame header.
const FRAME_CHECKSUM_OFFSET: usize = 16;

/// What the checksum field of a frame holds while the frame is being built, before the checksum is known.
const CHECKSUM_PLACEHOLDER: u32 = 0;

/// Why bytes are not a valid segment header.
///
/// # Examples
///
/// ```
/// use bitacora_segment::format::{HeaderError, SegmentHeader};
///
/// let refused = SegmentHeader::decode(b"too short");
///
/// assert_eq!(refused, Err(HeaderError::Incomplete { available_bytes: 9 }));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum HeaderError {
    /// There are fewer bytes than a header needs.
    #[error("segment header needs {SEGMENT_HEADER_BYTES} bytes, found {available_bytes}")]
    Incomplete {
        /// How many bytes were available.
        available_bytes: usize,
    },
    /// The file does not start with [`SEGMENT_MAGIC`].
    #[error("not a segment file: wrong magic")]
    WrongMagic,
    /// The file was written by a newer build.
    #[error("segment format version {found_version} is newer than {SEGMENT_FORMAT_VERSION}")]
    UnsupportedVersion {
        /// The version found in the header.
        found_version: u32,
    },
    /// The header bytes do not match their checksum.
    #[error("segment header checksum mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    ChecksumMismatch {
        /// The checksum stored in the header.
        stored: u32,
        /// The checksum of the bytes actually read.
        computed: u32,
    },
    /// The header claims the segment starts at the empty position.
    #[error("segment header starts at the empty position")]
    StartsAtEmptyPosition,
}

/// The fixed-size identity at the start of every segment file.
///
/// It says which segment the file is a copy of, so that a file found under the wrong name, or left over
/// from another stream, is never mistaken for the copy a replica is looking for.
///
/// # Examples
///
/// ```
/// use bitacora_segment::format::SegmentHeader;
/// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
///
/// let header = SegmentHeader {
///     address: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
///     epoch: Epoch::new(5),
///     first_sequence: SequenceNumber::new(100),
/// };
///
/// let file_start = header.encode();
///
/// assert_eq!(SegmentHeader::decode(&file_start), Ok(header));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentHeader {
    /// Which segment this file is a copy of.
    pub address: SegmentAddress,
    /// The epoch of the only writer that ever authored records in it.
    pub epoch: Epoch,
    /// The sequence number of the segment's first record.
    pub first_sequence: SequenceNumber,
}

impl SegmentHeader {
    /// Serializes the header, checksum and padding included.
    ///
    /// The flushed mark slots are left zero-filled, which is how a file that was never flushed past its
    /// header looks.
    ///
    /// # Returns
    ///
    /// The first [`SEGMENT_HEADER_BYTES`] bytes of the segment file.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_segment::format::{SEGMENT_HEADER_BYTES, SegmentHeader};
    /// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
    ///
    /// let header = SegmentHeader {
    ///     address: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
    ///     epoch: Epoch::new(5),
    ///     first_sequence: SequenceNumber::FIRST,
    /// };
    ///
    /// let file_start = header.encode();
    ///
    /// assert_eq!(file_start.len(), SEGMENT_HEADER_BYTES);
    /// assert!(file_start.starts_with(b"BITASEG\0"));
    /// ```
    #[must_use]
    pub fn encode(&self) -> [u8; SEGMENT_HEADER_BYTES] {
        let mut encoded = [0_u8; SEGMENT_HEADER_BYTES];
        let mut unwritten = &mut encoded[..];
        unwritten.put_slice(&SEGMENT_MAGIC);
        unwritten.put_u32_le(SEGMENT_FORMAT_VERSION);
        unwritten.put_u64_le(self.address.stream_id.get());
        unwritten.put_u64_le(self.address.segment_id.get());
        unwritten.put_u64_le(self.epoch.get());
        unwritten.put_u64_le(self.first_sequence.get());
        let checksum = crc32c::crc32c(&encoded[..HEADER_CHECKSUM_OFFSET]);
        encoded[HEADER_CHECKSUM_OFFSET..HEADER_CHECKSUM_OFFSET + U32_BYTES].copy_from_slice(&checksum.to_le_bytes());
        encoded
    }

    /// Parses and verifies a header from the start of a file.
    ///
    /// The checks run in the order that gives the most useful error: size, magic, checksum, version, then
    /// the first sequence. The fields are looked at only once the checksum has shown that they are what
    /// was written. The flushed marks are not part of the identity; read them with
    /// [`FlushedMark::latest_in`].
    ///
    /// # Arguments
    ///
    /// * `file_start` - The start of the file. Bytes after the header are ignored.
    ///
    /// # Returns
    ///
    /// The identity the file starts with.
    ///
    /// # Errors
    ///
    /// * [`HeaderError::Incomplete`] if there are fewer than [`SEGMENT_HEADER_BYTES`] bytes.
    /// * [`HeaderError::WrongMagic`] if the bytes do not start with [`SEGMENT_MAGIC`].
    /// * [`HeaderError::ChecksumMismatch`] if the identity fails its checksum.
    /// * [`HeaderError::UnsupportedVersion`] if the file was written by a newer format.
    /// * [`HeaderError::StartsAtEmptyPosition`] if the first sequence is zero, which names no record.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_segment::format::{HeaderError, SEGMENT_HEADER_BYTES, SegmentHeader};
    ///
    /// let zero_filled = [0_u8; SEGMENT_HEADER_BYTES];
    ///
    /// assert_eq!(SegmentHeader::decode(&zero_filled), Err(HeaderError::WrongMagic));
    /// ```
    pub fn decode(file_start: &[u8]) -> Result<Self, HeaderError> {
        let Some(encoded) = file_start.get(..SEGMENT_HEADER_BYTES) else {
            return Err(HeaderError::Incomplete {
                available_bytes: file_start.len(),
            });
        };
        if encoded[..SEGMENT_MAGIC.len()] != SEGMENT_MAGIC {
            return Err(HeaderError::WrongMagic);
        }
        let stored = read_u32(encoded, HEADER_CHECKSUM_OFFSET);
        let computed = crc32c::crc32c(&encoded[..HEADER_CHECKSUM_OFFSET]);
        if stored != computed {
            return Err(HeaderError::ChecksumMismatch { stored, computed });
        }
        let found_version = read_u32(encoded, HEADER_FORMAT_VERSION_OFFSET);
        if found_version > SEGMENT_FORMAT_VERSION {
            return Err(HeaderError::UnsupportedVersion { found_version });
        }
        let first_sequence = SequenceNumber::new(read_u64(encoded, HEADER_FIRST_SEQUENCE_OFFSET));
        if first_sequence == SequenceNumber::ZERO {
            return Err(HeaderError::StartsAtEmptyPosition);
        }
        Ok(Self {
            address: SegmentAddress::new(
                StreamId::new(read_u64(encoded, HEADER_STREAM_ID_OFFSET)),
                SegmentId::new(read_u64(encoded, HEADER_SEGMENT_ID_OFFSET)),
            ),
            epoch: Epoch::new(read_u64(encoded, HEADER_EPOCH_OFFSET)),
            first_sequence,
        })
    }
}

/// A copy's durable note of how far it has been flushed: every byte of the file before `flushed_bytes`
/// reached the disk.
///
/// A replica that restarts cannot otherwise tell a torn write, which is normal and loses nothing that was
/// acknowledged, from damage to records it had flushed and acknowledged, which makes its copy unfit to
/// vouch for what is committed. With the mark, invalid bytes before it are damage, and invalid bytes
/// after it are an ordinary torn tail.
///
/// The mark is written without a flush of its own: it rides on the next flush of data, or on a flush made
/// when the stream goes idle. It is therefore allowed to lag behind the truth, never to run ahead of it.
/// Two slots are written alternately, so that a torn write of one leaves the other intact.
///
/// # Examples
///
/// ```
/// use bitacora_segment::format::{FLUSHED_MARK_BYTES, FlushedMark, SegmentHeader};
/// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
///
/// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
/// let header = SegmentHeader {
///     address: segment,
///     epoch: Epoch::new(5),
///     first_sequence: SequenceNumber::FIRST,
/// };
/// let mut header_bytes = header.encode();
/// let mark = FlushedMark {
///     flushed_bytes: 4_096,
///     generation: 1,
/// };
///
/// let slot_offset = mark.slot_offset();
/// header_bytes[slot_offset..slot_offset + FLUSHED_MARK_BYTES].copy_from_slice(&mark.encode(segment));
///
/// assert_eq!(FlushedMark::latest_in(segment, &header_bytes), Some(mark));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlushedMark {
    /// How many bytes from the start of the file are known to be flushed.
    pub flushed_bytes: u64,
    /// Counts the marks written to this file. The valid mark with the highest generation is the current
    /// one.
    pub generation: u32,
}

impl FlushedMark {
    /// Returns where this mark is stored in the file.
    ///
    /// The two slots alternate with the generation, so a mark never overwrites the one before it.
    ///
    /// # Returns
    ///
    /// The offset of the mark's slot, in bytes from the start of the file.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_segment::format::FlushedMark;
    ///
    /// let fourth_mark = FlushedMark {
    ///     flushed_bytes: 4_096,
    ///     generation: 4,
    /// };
    /// let fifth_mark = FlushedMark {
    ///     flushed_bytes: 8_192,
    ///     generation: 5,
    /// };
    ///
    /// assert_eq!(fourth_mark.slot_offset(), 64);
    /// assert_eq!(fifth_mark.slot_offset(), 80);
    /// ```
    #[must_use]
    pub const fn slot_offset(self) -> usize {
        slot_offset_of(self.generation % FLUSHED_MARK_SLOT_COUNT)
    }

    /// Serializes the mark for one segment, checksum included.
    ///
    /// The segment's address is mixed into the checksum, so the bytes are valid only in a file of that
    /// segment.
    ///
    /// # Arguments
    ///
    /// * `segment` - The segment whose file the mark will be written to.
    ///
    /// # Returns
    ///
    /// The [`FLUSHED_MARK_BYTES`] bytes to write at [`FlushedMark::slot_offset`].
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_segment::format::{FLUSHED_MARK_BYTES, FlushedMark};
    /// use bitacora_types::{SegmentAddress, SegmentId, StreamId};
    ///
    /// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
    /// let mark = FlushedMark {
    ///     flushed_bytes: 4_096,
    ///     generation: 1,
    /// };
    ///
    /// assert_eq!(mark.encode(segment).len(), FLUSHED_MARK_BYTES);
    /// ```
    #[must_use]
    pub fn encode(self, segment: SegmentAddress) -> [u8; FLUSHED_MARK_BYTES] {
        let mut encoded = [0_u8; FLUSHED_MARK_BYTES];
        encoded[MARK_FLUSHED_BYTES_OFFSET..MARK_GENERATION_OFFSET].copy_from_slice(&self.flushed_bytes.to_le_bytes());
        encoded[MARK_GENERATION_OFFSET..MARK_CHECKSUM_OFFSET].copy_from_slice(&self.generation.to_le_bytes());
        let checksum = flushed_mark_checksum(segment, &encoded);
        encoded[MARK_CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_le_bytes());
        encoded
    }

    /// Finds the current mark stored in a segment header.
    ///
    /// Both slots are read. A slot counts only if it matches its checksum for this segment, so an
    /// unwritten slot, a torn one and one copied from another file are all skipped. Of the valid ones,
    /// the mark with the highest generation wins.
    ///
    /// # Arguments
    ///
    /// * `segment` - The segment the file is a copy of.
    /// * `header` - The start of the file, at least [`SEGMENT_HEADER_BYTES`] long. A slot that lies past
    ///   the end of a shorter slice is skipped.
    ///
    /// # Returns
    ///
    /// The current mark, or `None` if neither slot holds a valid one, as in a file that was never flushed
    /// past its header.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_segment::format::{FlushedMark, SEGMENT_HEADER_BYTES};
    /// use bitacora_types::{SegmentAddress, SegmentId, StreamId};
    ///
    /// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
    /// let never_marked = [0_u8; SEGMENT_HEADER_BYTES];
    ///
    /// assert_eq!(FlushedMark::latest_in(segment, &never_marked), None);
    /// ```
    #[must_use]
    pub fn latest_in(segment: SegmentAddress, header: &[u8]) -> Option<Self> {
        let mut latest: Option<Self> = None;
        for slot_number in 0..FLUSHED_MARK_SLOT_COUNT {
            let Some(mark) = Self::valid_in_slot(segment, header, slot_number) else {
                continue;
            };
            let is_latest_so_far = match latest {
                Some(earlier_slot_mark) => mark.generation >= earlier_slot_mark.generation,
                None => true,
            };
            if is_latest_so_far {
                latest = Some(mark);
            }
        }
        latest
    }

    /// Reads the mark in one slot of a header. Returns `None` if the slot lies past the end of `header` or
    /// does not match its checksum for `segment`.
    fn valid_in_slot(segment: SegmentAddress, header: &[u8], slot_number: u32) -> Option<Self> {
        let slot_start = slot_offset_of(slot_number);
        let encoded = header.get(slot_start..slot_start + FLUSHED_MARK_BYTES)?;
        let stored = read_u32(encoded, MARK_CHECKSUM_OFFSET);
        if stored != flushed_mark_checksum(segment, encoded) {
            return None;
        }
        Some(Self {
            flushed_bytes: read_u64(encoded, MARK_FLUSHED_BYTES_OFFSET),
            generation: read_u32(encoded, MARK_GENERATION_OFFSET),
        })
    }
}

/// Where the flushed mark slot with the given number starts in a segment header.
const fn slot_offset_of(slot_number: u32) -> usize {
    FLUSHED_MARKS_OFFSET + FLUSHED_MARK_BYTES * slot_number as usize
}

/// Computes the checksum of an encoded flushed mark, skipping the checksum field. The segment's address
/// is mixed in, so that an all-zero slot is never valid and a mark copied from another file is refused.
fn flushed_mark_checksum(segment: SegmentAddress, encoded: &[u8]) -> u32 {
    let mut checksum = crc32c::crc32c(&segment.stream_id.get().to_le_bytes());
    checksum = crc32c::crc32c_append(checksum, &segment.segment_id.get().to_le_bytes());
    crc32c::crc32c_append(checksum, &encoded[..MARK_CHECKSUM_OFFSET])
}

/// Why records cannot be encoded as one batch.
///
/// # Examples
///
/// ```
/// use bitacora_segment::format::{Batch, EncodeError};
/// use bitacora_types::{SegmentAddress, SegmentId, SequenceNumber, StreamId};
///
/// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
///
/// let refused = Batch::encode(segment, SequenceNumber::FIRST, &[]);
///
/// assert_eq!(refused, Err(EncodeError::NoRecords));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum EncodeError {
    /// A batch must hold at least one record: a zero body length marks the end of the data in a segment
    /// file.
    #[error("a batch must hold at least one record")]
    NoRecords,
    /// The batch would start at the empty position.
    #[error("a batch cannot start at the empty position")]
    StartsAtEmptyPosition,
    /// One payload exceeds [`MAX_RECORD_PAYLOAD_BYTES`].
    #[error("record payload is {payload_bytes} bytes, the limit is {MAX_RECORD_PAYLOAD_BYTES}")]
    RecordTooLarge {
        /// The size of the rejected payload.
        payload_bytes: usize,
    },
    /// The records together exceed [`MAX_BATCH_BODY_BYTES`].
    #[error("batch body is {body_bytes} bytes, the limit is {MAX_BATCH_BODY_BYTES}")]
    BatchTooLarge {
        /// The size the body would have.
        body_bytes: usize,
    },
}

/// Why bytes are not a valid batch frame.
///
/// # Examples
///
/// ```
/// use bitacora_segment::format::{Batch, FrameError};
/// use bitacora_types::{SegmentAddress, SegmentId, StreamId};
/// use bytes::Bytes;
///
/// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
/// let unwritten_space = Bytes::from_static(&[0; 64]);
///
/// assert_eq!(Batch::decode(segment, &unwritten_space), Err(FrameError::EndOfData));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum FrameError {
    /// The bytes end before the frame does. In a segment file this is a torn write; on the wire it is a
    /// truncated message.
    #[error("frame needs {needed_bytes} bytes, found {available_bytes}")]
    Incomplete {
        /// How many bytes the frame needs in total, as far as is known.
        needed_bytes: usize,
        /// How many bytes were available.
        available_bytes: usize,
    },
    /// The body length is zero: preallocated space that was never written.
    #[error("no frame: the body length is zero")]
    EndOfData,
    /// The body length exceeds [`MAX_BATCH_BODY_BYTES`].
    #[error("frame body is {body_bytes} bytes, the limit is {MAX_BATCH_BODY_BYTES}")]
    BodyTooLarge {
        /// The body length found in the frame header.
        body_bytes: usize,
    },
    /// The frame does not match its checksum for this segment.
    #[error("frame checksum mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    ChecksumMismatch {
        /// The checksum stored in the frame.
        stored: u32,
        /// The checksum of the bytes actually read.
        computed: u32,
    },
    /// The checksum holds but the content breaks a format rule, which only a faulty writer can cause.
    #[error("frame is malformed: {reason}")]
    Malformed {
        /// Which rule the frame breaks.
        reason: &'static str,
    },
}

/// One batch of records: decoded header fields plus the exact frame bytes.
///
/// A `Batch` can only be obtained from [`Batch::encode`] or [`Batch::decode`], so holding one proves its
/// checksum and structure were verified. Cloning one is cheap: the frame bytes are shared, not copied.
///
/// # Examples
///
/// ```
/// use bitacora_segment::format::Batch;
/// use bitacora_types::{SegmentAddress, SegmentId, SequenceNumber, StreamId};
/// use bytes::Bytes;
///
/// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
/// let payloads = [Bytes::from_static(b"first"), Bytes::from_static(b"second")];
///
/// let batch = Batch::encode(segment, SequenceNumber::new(100), &payloads)?;
///
/// assert_eq!(batch.record_count(), 2);
/// assert_eq!(batch.last_sequence(), SequenceNumber::new(101));
/// assert_eq!(Batch::decode(segment, batch.frame()), Ok(batch));
/// # Ok::<(), bitacora_segment::format::EncodeError>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Batch {
    /// The sequence number of the batch's first record, as stored in the frame header.
    first_sequence: SequenceNumber,
    /// How many records the body holds, as stored in the frame header. Never zero.
    record_count: u32,
    /// The whole frame, header and body, exactly as stored and sent.
    frame: Bytes,
}

impl Batch {
    /// Encodes payloads as one frame of a segment.
    ///
    /// The records are numbered from `first_sequence` in the order given. The frame is built in one
    /// buffer of exactly its final size, and each payload is copied into it once.
    ///
    /// # Arguments
    ///
    /// * `segment` - The segment the batch belongs to. It is mixed into the checksum, not stored.
    /// * `first_sequence` - The sequence number of the first record.
    /// * `payloads` - The records' payloads, in order.
    ///
    /// # Returns
    ///
    /// The batch, with the frame ready to be sent or stored.
    ///
    /// # Errors
    ///
    /// * [`EncodeError::NoRecords`] if `payloads` is empty.
    /// * [`EncodeError::StartsAtEmptyPosition`] if `first_sequence` is zero.
    /// * [`EncodeError::RecordTooLarge`] if a payload exceeds [`MAX_RECORD_PAYLOAD_BYTES`].
    /// * [`EncodeError::BatchTooLarge`] if the records together exceed [`MAX_BATCH_BODY_BYTES`].
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_segment::format::{BATCH_HEADER_BYTES, Batch, RECORD_HEADER_BYTES};
    /// use bitacora_types::{SegmentAddress, SegmentId, SequenceNumber, StreamId};
    /// use bytes::Bytes;
    ///
    /// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
    ///
    /// let batch = Batch::encode(segment, SequenceNumber::FIRST, &[Bytes::from_static(b"hello")])?;
    ///
    /// assert_eq!(batch.frame().len(), BATCH_HEADER_BYTES + RECORD_HEADER_BYTES + 5);
    /// # Ok::<(), bitacora_segment::format::EncodeError>(())
    /// ```
    pub fn encode(
        segment: SegmentAddress,
        first_sequence: SequenceNumber,
        payloads: &[Bytes],
    ) -> Result<Self, EncodeError> {
        if payloads.is_empty() {
            return Err(EncodeError::NoRecords);
        }
        if first_sequence == SequenceNumber::ZERO {
            return Err(EncodeError::StartsAtEmptyPosition);
        }
        let body_bytes = body_bytes_of(payloads)?;
        let record_count = encode_length(payloads.len());

        let mut frame = BytesMut::with_capacity(BATCH_HEADER_BYTES + body_bytes);
        frame.put_u32_le(encode_length(body_bytes));
        frame.put_u64_le(first_sequence.get());
        frame.put_u32_le(record_count);
        frame.put_u32_le(CHECKSUM_PLACEHOLDER);
        for payload in payloads {
            frame.put_u32_le(encode_length(payload.len()));
            frame.put_slice(payload);
        }
        let checksum = frame_checksum(segment, &frame);
        frame[FRAME_CHECKSUM_OFFSET..BATCH_HEADER_BYTES].copy_from_slice(&checksum.to_le_bytes());

        Ok(Self {
            first_sequence,
            record_count,
            frame: frame.freeze(),
        })
    }

    /// Decodes and verifies the frame at the start of a buffer.
    ///
    /// The frame's length is read and checked first, then its checksum, then the structure of its body.
    /// The returned batch shares the buffer's memory; nothing is copied.
    ///
    /// # Arguments
    ///
    /// * `segment` - The segment the frame is expected to belong to.
    /// * `source` - Bytes that start with a frame. They may hold further bytes after it.
    ///
    /// # Returns
    ///
    /// The batch. Its frame is the first [`Batch::frame`]`.len()` bytes of `source`.
    ///
    /// # Errors
    ///
    /// * [`FrameError::EndOfData`] if `source` is empty or starts with a zero body length.
    /// * [`FrameError::Incomplete`] if `source` ends before the frame does.
    /// * [`FrameError::BodyTooLarge`] if the frame claims a body over [`MAX_BATCH_BODY_BYTES`].
    /// * [`FrameError::ChecksumMismatch`] if the frame does not match its checksum for `segment`.
    /// * [`FrameError::Malformed`] if the checksum holds but the content breaks a rule of the format.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_segment::format::{Batch, FrameError};
    /// use bitacora_types::{SegmentAddress, SegmentId, SequenceNumber, StreamId};
    /// use bytes::Bytes;
    ///
    /// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
    /// let other_segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(4));
    /// let batch = Batch::encode(segment, SequenceNumber::FIRST, &[Bytes::from_static(b"hello")])?;
    ///
    /// assert_eq!(Batch::decode(segment, batch.frame()), Ok(batch.clone()));
    /// assert!(matches!(
    ///     Batch::decode(other_segment, batch.frame()),
    ///     Err(FrameError::ChecksumMismatch { .. })
    /// ));
    /// # Ok::<(), bitacora_segment::format::EncodeError>(())
    /// ```
    pub fn decode(segment: SegmentAddress, source: &Bytes) -> Result<Self, FrameError> {
        let frame_bytes = complete_frame_bytes(source)?;
        let frame = source.slice(..frame_bytes);
        let stored = read_u32(&frame, FRAME_CHECKSUM_OFFSET);
        let computed = frame_checksum(segment, &frame);
        if stored != computed {
            return Err(FrameError::ChecksumMismatch { stored, computed });
        }

        let first_sequence = SequenceNumber::new(read_u64(&frame, FRAME_FIRST_SEQUENCE_OFFSET));
        if first_sequence == SequenceNumber::ZERO {
            return Err(FrameError::Malformed {
                reason: "batch starts at the empty position",
            });
        }
        let record_count = read_u32(&frame, FRAME_RECORD_COUNT_OFFSET);
        validate_body(&frame[BATCH_HEADER_BYTES..], record_count)?;
        if first_sequence.get().checked_add(u64::from(record_count)).is_none() {
            return Err(FrameError::Malformed {
                reason: "batch runs past the end of the sequence space",
            });
        }
        Ok(Self {
            first_sequence,
            record_count,
            frame,
        })
    }

    /// Returns the sequence number of the batch's first record.
    ///
    /// # Returns
    ///
    /// The first record's position in the stream. Never [`SequenceNumber::ZERO`].
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_segment::format::Batch;
    /// use bitacora_types::{SegmentAddress, SegmentId, SequenceNumber, StreamId};
    /// use bytes::Bytes;
    ///
    /// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
    /// let batch = Batch::encode(segment, SequenceNumber::new(100), &[Bytes::from_static(b"a")])?;
    ///
    /// assert_eq!(batch.first_sequence(), SequenceNumber::new(100));
    /// # Ok::<(), bitacora_segment::format::EncodeError>(())
    /// ```
    #[must_use]
    pub const fn first_sequence(&self) -> SequenceNumber {
        self.first_sequence
    }

    /// Returns the sequence number of the batch's last record.
    ///
    /// # Returns
    ///
    /// The last record's position in the stream. It equals the first for a batch of one record.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_segment::format::Batch;
    /// use bitacora_types::{SegmentAddress, SegmentId, SequenceNumber, StreamId};
    /// use bytes::Bytes;
    ///
    /// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
    /// let payloads = [Bytes::from_static(b"a"), Bytes::from_static(b"b"), Bytes::from_static(b"c")];
    /// let batch = Batch::encode(segment, SequenceNumber::new(100), &payloads)?;
    ///
    /// assert_eq!(batch.last_sequence(), SequenceNumber::new(102));
    /// # Ok::<(), bitacora_segment::format::EncodeError>(())
    /// ```
    #[must_use]
    pub const fn last_sequence(&self) -> SequenceNumber {
        self.first_sequence.advanced_by(self.record_count as u64 - 1)
    }

    /// Returns how many records the batch holds.
    ///
    /// # Returns
    ///
    /// The number of records. Never zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_segment::format::Batch;
    /// use bitacora_types::{SegmentAddress, SegmentId, SequenceNumber, StreamId};
    /// use bytes::Bytes;
    ///
    /// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
    /// let payloads = [Bytes::from_static(b"a"), Bytes::from_static(b"b")];
    /// let batch = Batch::encode(segment, SequenceNumber::FIRST, &payloads)?;
    ///
    /// assert_eq!(batch.record_count(), 2);
    /// # Ok::<(), bitacora_segment::format::EncodeError>(())
    /// ```
    #[must_use]
    pub const fn record_count(&self) -> u32 {
        self.record_count
    }

    /// Returns the exact bytes of the frame, as stored and sent.
    ///
    /// # Returns
    ///
    /// The frame, header and body. Cloning it shares the memory.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_segment::format::{BATCH_HEADER_BYTES, Batch};
    /// use bitacora_types::{SegmentAddress, SegmentId, SequenceNumber, StreamId};
    /// use bytes::Bytes;
    ///
    /// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
    /// let batch = Batch::encode(segment, SequenceNumber::FIRST, &[Bytes::new()])?;
    ///
    /// assert!(batch.frame().len() > BATCH_HEADER_BYTES);
    /// # Ok::<(), bitacora_segment::format::EncodeError>(())
    /// ```
    #[must_use]
    pub const fn frame(&self) -> &Bytes {
        &self.frame
    }

    /// Iterates over the batch's records in sequence order, without copying their payloads.
    ///
    /// # Returns
    ///
    /// An iterator that yields each record with its sequence number. Every payload shares the frame's
    /// memory.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_segment::format::Batch;
    /// use bitacora_types::{LogRecord, SegmentAddress, SegmentId, SequenceNumber, StreamId};
    /// use bytes::Bytes;
    ///
    /// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
    /// let batch = Batch::encode(segment, SequenceNumber::new(100), &[Bytes::from_static(b"hello")])?;
    ///
    /// let first_record = batch.records().next();
    ///
    /// assert_eq!(
    ///     first_record,
    ///     Some(LogRecord {
    ///         sequence: SequenceNumber::new(100),
    ///         payload: Bytes::from_static(b"hello"),
    ///     })
    /// );
    /// # Ok::<(), bitacora_segment::format::EncodeError>(())
    /// ```
    #[must_use]
    pub fn records(&self) -> BatchRecords {
        BatchRecords {
            next_sequence: self.first_sequence,
            unread_body: self.frame.slice(BATCH_HEADER_BYTES..),
        }
    }
}

/// Iterator over the records of a [`Batch`], in sequence order.
///
/// # Examples
///
/// ```
/// use bitacora_segment::format::Batch;
/// use bitacora_types::{SegmentAddress, SegmentId, SequenceNumber, StreamId};
/// use bytes::Bytes;
///
/// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
/// let payloads = [Bytes::from_static(b"a"), Bytes::from_static(b"b")];
/// let batch = Batch::encode(segment, SequenceNumber::new(100), &payloads)?;
///
/// let mut sequences = Vec::new();
/// for record in batch.records() {
///     sequences.push(record.sequence.get());
/// }
///
/// assert_eq!(sequences, [100, 101]);
/// # Ok::<(), bitacora_segment::format::EncodeError>(())
/// ```
#[derive(Clone, Debug)]
pub struct BatchRecords {
    /// The sequence number of the next record to yield.
    next_sequence: SequenceNumber,
    /// The part of the batch body not yielded yet. It starts at a record's length prefix, or is empty.
    unread_body: Bytes,
}

impl Iterator for BatchRecords {
    type Item = LogRecord;

    /// Splits the next record off the unread body and numbers it.
    fn next(&mut self) -> Option<LogRecord> {
        if self.unread_body.is_empty() {
            return None;
        }
        // `Batch::decode` and `Batch::encode` validated the body, so every length prefix is in bounds.
        let payload_bytes = decode_length(read_u32(&self.unread_body, 0));
        let mut record_bytes = self.unread_body.split_to(RECORD_HEADER_BYTES + payload_bytes);
        let payload = record_bytes.split_off(RECORD_HEADER_BYTES);
        let sequence = self.next_sequence;
        self.next_sequence = sequence.next();
        Some(LogRecord { sequence, payload })
    }
}

/// Why a run of frames is not what the receiver expected.
///
/// # Examples
///
/// ```
/// use bitacora_segment::format::{FrameError, FramesError, decode_frames};
/// use bitacora_types::{SegmentAddress, SegmentId, StreamId};
/// use bytes::Bytes;
///
/// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
/// let cut_frame = Bytes::from_static(&[9, 0, 0]);
///
/// let refused = decode_frames(segment, &cut_frame);
///
/// assert!(matches!(
///     refused,
///     Err(FramesError::InvalidFrame {
///         offset_bytes: 0,
///         reason: FrameError::Incomplete { .. }
///     })
/// ));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum FramesError {
    /// One of the frames is not valid.
    #[error("frame at byte {offset_bytes} is invalid: {reason}")]
    InvalidFrame {
        /// Where the invalid frame starts within the run.
        offset_bytes: usize,
        /// What is wrong with it.
        reason: FrameError,
    },
    /// A frame does not continue where the previous one ended.
    #[error("frame starts at sequence {found} where {expected} was expected")]
    SequenceGap {
        /// The sequence number the next frame had to start at.
        expected: SequenceNumber,
        /// The sequence number it actually starts at.
        found: SequenceNumber,
    },
}

/// Decodes a run of back-to-back frames where every byte must belong to a valid frame, as in an append
/// request or a read response.
///
/// The frames are decoded one after the other, each sharing the memory of `frames`. The first frame may
/// start at any sequence number; every later one must continue where the one before it ended.
///
/// # Arguments
///
/// * `segment` - The segment every frame must belong to.
/// * `frames` - The frames, with nothing before, between or after them.
///
/// # Returns
///
/// The batches in order. Empty if `frames` is empty.
///
/// # Errors
///
/// * [`FramesError::InvalidFrame`] if any frame is invalid, including unwritten space and a frame that is
///   cut short.
/// * [`FramesError::SequenceGap`] if a frame does not start right after the previous one.
///
/// # Examples
///
/// ```
/// use bitacora_segment::format::{Batch, decode_frames};
/// use bitacora_types::{SegmentAddress, SegmentId, SequenceNumber, StreamId};
/// use bytes::Bytes;
///
/// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
/// let first_batch = Batch::encode(segment, SequenceNumber::new(100), &[Bytes::from_static(b"a")])?;
/// let second_batch = Batch::encode(segment, SequenceNumber::new(101), &[Bytes::from_static(b"b")])?;
/// let request_body = Bytes::from([first_batch.frame().as_ref(), second_batch.frame().as_ref()].concat());
///
/// let decoded = decode_frames(segment, &request_body);
///
/// assert_eq!(decoded, Ok(vec![first_batch, second_batch]));
/// # Ok::<(), bitacora_segment::format::EncodeError>(())
/// ```
pub fn decode_frames(segment: SegmentAddress, frames: &Bytes) -> Result<Vec<Batch>, FramesError> {
    let mut batches: Vec<Batch> = Vec::new();
    let mut offset_bytes = 0;
    while offset_bytes < frames.len() {
        let batch = match Batch::decode(segment, &frames.slice(offset_bytes..)) {
            Ok(batch) => batch,
            Err(reason) => return Err(FramesError::InvalidFrame { offset_bytes, reason }),
        };
        if let Some(previous_batch) = batches.last() {
            let expected = previous_batch.last_sequence().next();
            if batch.first_sequence() != expected {
                return Err(FramesError::SequenceGap {
                    expected,
                    found: batch.first_sequence(),
                });
            }
        }
        offset_bytes += batch.frame().len();
        batches.push(batch);
    }
    Ok(batches)
}

/// Why a scan of segment file content stopped.
///
/// # Examples
///
/// ```
/// use bitacora_segment::format::{ScanStop, scan_valid_prefix};
/// use bitacora_types::{SegmentAddress, SegmentId, SequenceNumber, StreamId};
/// use bytes::Bytes;
///
/// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
/// let unwritten_space = Bytes::from_static(&[0; 64]);
///
/// let scanned = scan_valid_prefix(segment, SequenceNumber::FIRST, &unwritten_space);
///
/// assert_eq!(scanned.stop, ScanStop::EndOfData);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanStop {
    /// Every byte was consumed, or the rest is unwritten space.
    EndOfData,
    /// The bytes end in the middle of a frame. A caller reading a file in chunks should read on from here;
    /// at the true end of the file this is a torn write.
    Incomplete {
        /// How many bytes the cut frame needs in total, as far as is known.
        needed_bytes: usize,
    },
    /// The next frame is invalid: a torn or corrupted write.
    Invalid(FrameError),
    /// The next frame is valid but does not continue the sequence, so it is a leftover from an earlier,
    /// discarded tail.
    SequenceGap {
        /// The sequence number the next frame had to start at.
        expected: SequenceNumber,
        /// The sequence number it actually starts at.
        found: SequenceNumber,
    },
}

/// The valid batches at the start of some segment file content.
///
/// # Examples
///
/// ```
/// use bitacora_segment::format::{Batch, scan_valid_prefix};
/// use bitacora_types::{SegmentAddress, SegmentId, SequenceNumber, StreamId};
/// use bytes::Bytes;
///
/// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
/// let batch = Batch::encode(segment, SequenceNumber::FIRST, &[Bytes::from_static(b"a")])?;
///
/// let scanned = scan_valid_prefix(segment, SequenceNumber::FIRST, batch.frame());
///
/// assert_eq!(scanned.valid_bytes, batch.frame().len());
/// assert_eq!(scanned.batches, [batch]);
/// # Ok::<(), bitacora_segment::format::EncodeError>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidPrefix {
    /// The consecutive valid batches, in order.
    pub batches: Vec<Batch>,
    /// How many bytes those batches occupy.
    pub valid_bytes: usize,
    /// Why the scan did not go further.
    pub stop: ScanStop,
}

/// Finds the longest run of valid, consecutive batches at the start of some segment file content.
///
/// This is the crash-recovery scan: whatever follows the returned prefix was never acknowledged, because
/// a replica acknowledges only after the whole frame is flushed. It never fails; invalid bytes only end
/// the prefix. The batches share the memory of `content`.
///
/// # Arguments
///
/// * `segment` - The segment the content belongs to.
/// * `next_sequence` - The sequence number the first batch must start at.
/// * `content` - Bytes that start where a frame is expected: right after the header, or right after a
///   batch already known to be valid.
///
/// # Returns
///
/// The valid batches, how many bytes they occupy, and why the scan stopped there.
///
/// # Examples
///
/// ```
/// use bitacora_segment::format::{Batch, ScanStop, scan_valid_prefix};
/// use bitacora_types::{SegmentAddress, SegmentId, SequenceNumber, StreamId};
/// use bytes::Bytes;
///
/// let segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
/// let whole_batch = Batch::encode(segment, SequenceNumber::new(100), &[Bytes::from_static(b"kept")])?;
/// let torn_batch = Batch::encode(segment, SequenceNumber::new(101), &[Bytes::from_static(b"torn")])?;
/// let torn_frame = &torn_batch.frame()[..torn_batch.frame().len() - 1];
/// let file_content = Bytes::from([whole_batch.frame().as_ref(), torn_frame].concat());
///
/// let scanned = scan_valid_prefix(segment, SequenceNumber::new(100), &file_content);
///
/// assert_eq!(scanned.batches, [whole_batch]);
/// assert!(matches!(scanned.stop, ScanStop::Incomplete { .. }));
/// # Ok::<(), bitacora_segment::format::EncodeError>(())
/// ```
#[must_use]
pub fn scan_valid_prefix(segment: SegmentAddress, next_sequence: SequenceNumber, content: &Bytes) -> ValidPrefix {
    let mut batches = Vec::new();
    let mut valid_bytes = 0;
    let mut expected = next_sequence;
    let stop = loop {
        if valid_bytes == content.len() {
            break ScanStop::EndOfData;
        }
        match Batch::decode(segment, &content.slice(valid_bytes..)) {
            Ok(batch) if batch.first_sequence() != expected => {
                break ScanStop::SequenceGap {
                    expected,
                    found: batch.first_sequence(),
                };
            }
            Ok(batch) => {
                valid_bytes += batch.frame().len();
                expected = batch.last_sequence().next();
                batches.push(batch);
            }
            Err(FrameError::EndOfData) => break ScanStop::EndOfData,
            Err(FrameError::Incomplete { needed_bytes, .. }) => {
                break ScanStop::Incomplete { needed_bytes };
            }
            Err(invalid) => break ScanStop::Invalid(invalid),
        }
    };
    ValidPrefix {
        batches,
        valid_bytes,
        stop,
    }
}

/// Adds up the size of the body that `payloads` encode to, checking both size limits on the way.
///
/// Fails with [`EncodeError::RecordTooLarge`] at the first payload over the limit, or with
/// [`EncodeError::BatchTooLarge`] if the records together exceed [`MAX_BATCH_BODY_BYTES`].
fn body_bytes_of(payloads: &[Bytes]) -> Result<usize, EncodeError> {
    let mut body_bytes = 0;
    for payload in payloads {
        if payload.len() > MAX_RECORD_PAYLOAD_BYTES {
            return Err(EncodeError::RecordTooLarge {
                payload_bytes: payload.len(),
            });
        }
        body_bytes += RECORD_HEADER_BYTES + payload.len();
    }
    if body_bytes > MAX_BATCH_BODY_BYTES {
        return Err(EncodeError::BatchTooLarge { body_bytes });
    }
    Ok(body_bytes)
}

/// Works out how long the frame at the start of `source` is, and checks that all of it is there.
///
/// Fails with [`FrameError::EndOfData`] if there is no frame, [`FrameError::BodyTooLarge`] if the frame
/// claims a body over the limit, and [`FrameError::Incomplete`] if `source` ends before the frame does.
fn complete_frame_bytes(source: &[u8]) -> Result<usize, FrameError> {
    if source.is_empty() {
        return Err(FrameError::EndOfData);
    }
    if source.len() < BATCH_HEADER_BYTES {
        // Not `EndOfData`, even if every byte is zero: a body length such as 65536 starts with zero bytes,
        // and a caller reading a file in chunks may have cut the header right after them.
        return Err(FrameError::Incomplete {
            needed_bytes: BATCH_HEADER_BYTES,
            available_bytes: source.len(),
        });
    }
    let body_bytes = decode_length(read_u32(source, FRAME_BODY_LENGTH_OFFSET));
    if body_bytes == 0 {
        return Err(FrameError::EndOfData);
    }
    if body_bytes > MAX_BATCH_BODY_BYTES {
        return Err(FrameError::BodyTooLarge { body_bytes });
    }
    let frame_bytes = BATCH_HEADER_BYTES + body_bytes;
    if source.len() < frame_bytes {
        return Err(FrameError::Incomplete {
            needed_bytes: frame_bytes,
            available_bytes: source.len(),
        });
    }
    Ok(frame_bytes)
}

/// Computes the checksum of a whole frame, skipping the checksum field. The segment's address is mixed in
/// first, so a frame is valid only in the segment it was written for.
fn frame_checksum(segment: SegmentAddress, frame: &[u8]) -> u32 {
    let mut checksum = crc32c::crc32c(&segment.stream_id.get().to_le_bytes());
    checksum = crc32c::crc32c_append(checksum, &segment.segment_id.get().to_le_bytes());
    checksum = crc32c::crc32c_append(checksum, &frame[..FRAME_CHECKSUM_OFFSET]);
    crc32c::crc32c_append(checksum, &frame[BATCH_HEADER_BYTES..])
}

/// Checks that `body` is exactly `record_count` well-formed records.
///
/// Fails with [`FrameError::Malformed`], naming the broken rule. One pass over the length prefixes; the
/// payloads themselves are not read.
fn validate_body(body: &[u8], record_count: u32) -> Result<(), FrameError> {
    let mut unread_body = body;
    let mut records_found: u32 = 0;
    while !unread_body.is_empty() {
        if unread_body.len() < RECORD_HEADER_BYTES {
            return Err(FrameError::Malformed {
                reason: "body ends inside a record length",
            });
        }
        let payload_bytes = decode_length(read_u32(unread_body, 0));
        if payload_bytes > MAX_RECORD_PAYLOAD_BYTES {
            return Err(FrameError::Malformed {
                reason: "record payload exceeds the size limit",
            });
        }
        let record_bytes = RECORD_HEADER_BYTES + payload_bytes;
        if unread_body.len() < record_bytes {
            return Err(FrameError::Malformed {
                reason: "body ends inside a record payload",
            });
        }
        unread_body = &unread_body[record_bytes..];
        records_found += 1;
    }
    if records_found != record_count {
        return Err(FrameError::Malformed {
            reason: "record count does not match the body",
        });
    }
    Ok(())
}

/// Widens a length in memory to the `u64` that offsets in a file or an object are counted in.
///
/// It is here, next to the layout, because every module that places these bytes somewhere needs it.
///
/// # Arguments
///
/// * `length_bytes` - A length of bytes held in memory.
///
/// # Returns
///
/// The same length, as a `u64`.
///
/// # Panics
///
/// Panics on a target whose `usize` is wider than 64 bits; there is none.
pub(crate) fn to_u64(length_bytes: usize) -> u64 {
    u64::try_from(length_bytes).expect("lengths fit in 64 bits")
}

/// Narrows a length or a count to the `u32` the format stores it in.
///
/// Panics if it does not fit. The callers pass only values already checked against a limit of the
/// format, and every limit is far below 2^32.
fn encode_length(length: usize) -> u32 {
    u32::try_from(length).expect("lengths within the format's limits fit in 32 bits")
}

/// Widens a length read from the format to a `usize`.
///
/// Panics on a target whose `usize` is narrower than 32 bits; bitacora builds for none.
fn decode_length(stored_length: u32) -> usize {
    usize::try_from(stored_length).expect("32 bits fit in usize")
}

/// Reads the little-endian `u32` at `offset` in `source`.
///
/// Panics if `source` is shorter than `offset + 4`; every caller has checked the length it reads from.
fn read_u32(source: &[u8], offset: usize) -> u32 {
    let mut raw = [0_u8; U32_BYTES];
    raw.copy_from_slice(&source[offset..offset + U32_BYTES]);
    u32::from_le_bytes(raw)
}

/// Reads the little-endian `u64` at `offset` in `source`.
///
/// Panics if `source` is shorter than `offset + 8`; every caller has checked the length it reads from.
fn read_u64(source: &[u8], offset: usize) -> u64 {
    let mut raw = [0_u8; U64_BYTES];
    raw.copy_from_slice(&source[offset..offset + U64_BYTES]);
    u64::from_le_bytes(raw)
}

#[cfg(test)]
#[path = "format_test.rs"]
mod tests;
