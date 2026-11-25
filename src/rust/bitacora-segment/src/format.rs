//! The byte layout of segment files and batch frames (design, section 7).
//!
//! Everything here is a pure function over bytes, so the format can be
//! tested exhaustively without a disk. All integers are little-endian.
//!
//! ```text
//! segment file   header | batch frame*
//! header         identity | zero padding to 64 | flushed mark | flushed mark
//!                | zero padding to 128
//! identity       magic 8 | format version u32 | stream id u64 | segment id u64
//!                | epoch u64 | first sequence u64 | crc32c u32
//! flushed mark   flushed bytes u64 | generation u32 | crc32c u32
//! batch frame    body length u32 | first sequence u64 | record count u32
//!                | crc32c u32 | body
//! body           record*
//! record         payload length u32 | payload
//! ```
//!
//! The identity never changes after the file is created. The two flushed
//! marks are the only bytes of a segment file that are rewritten: see
//! [`FlushedMark`].
//!
//! A batch checksum covers the stream id, the segment id, the three header
//! fields before it and the body. The two identifiers are not stored in the
//! frame: mixing them in is what detects a frame written to the wrong
//! segment. Covering the first sequence detects a frame at the wrong offset.
//!
//! The same frame bytes travel on the wire, sit on replica disks and are
//! uploaded to the object store, so a checksum computed once by the writer
//! is verified at every hop.

use bitacora_types::{Epoch, LogRecord, MAX_RECORD_PAYLOAD_BYTES, SegmentAddress, SegmentId, SequenceNumber, StreamId};
use bytes::{BufMut, Bytes, BytesMut};
use thiserror::Error;

/// The first bytes of every segment file.
pub const SEGMENT_MAGIC: [u8; 8] = *b"BITASEG\0";

/// The segment format this build writes. Older formats stay readable.
pub const SEGMENT_FORMAT_VERSION: u32 = 1;

/// The size of the segment header, flushed marks and padding included.
pub const SEGMENT_HEADER_BYTES: usize = 128;

/// The size of one encoded [`FlushedMark`].
pub const FLUSHED_MARK_BYTES: usize = 16;

/// Where the first of the two flushed marks starts in the header.
const FLUSHED_MARKS_OFFSET: usize = 64;

/// The size of a batch frame header.
pub const BATCH_HEADER_BYTES: usize = 20;

/// The size of the length prefix in front of every record payload.
pub const RECORD_HEADER_BYTES: usize = 4;

/// The largest batch body the format accepts. It leaves room for one record
/// of the maximum size next to a full batch of small ones.
pub const MAX_BATCH_BODY_BYTES: usize = 8 * 1024 * 1024;

const HEADER_CHECKSUM_OFFSET: usize = 44;
const U32_BYTES: usize = 4;
const U64_BYTES: usize = 8;

/// Why bytes are not a valid segment header.
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

/// The fixed-size header at the start of every segment file.
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

    /// Parses and verifies a header from the start of `file_start`.
    ///
    /// # Errors
    ///
    /// Returns [`HeaderError`] if the bytes are too short, are not a segment
    /// file, come from a newer format, or fail their checksum.
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
        let found_version = read_u32(encoded, 8);
        if found_version > SEGMENT_FORMAT_VERSION {
            return Err(HeaderError::UnsupportedVersion { found_version });
        }
        let first_sequence = SequenceNumber::new(read_u64(encoded, 36));
        if first_sequence == SequenceNumber::ZERO {
            return Err(HeaderError::StartsAtEmptyPosition);
        }
        Ok(Self {
            address: SegmentAddress::new(
                StreamId::new(read_u64(encoded, 12)),
                SegmentId::new(read_u64(encoded, 20)),
            ),
            epoch: Epoch::new(read_u64(encoded, 28)),
            first_sequence,
        })
    }
}

/// A copy's durable note of how far it has been flushed: every byte of the
/// file before `flushed_bytes` reached the disk.
///
/// A replica that restarts cannot otherwise tell a torn write, which is
/// normal and loses nothing that was acknowledged, from damage to records
/// it had flushed and acknowledged, which makes its copy unfit to vouch for
/// what is committed. With the mark, invalid bytes before it are damage,
/// and invalid bytes after it are an ordinary torn tail.
///
/// The mark is written without a flush of its own: it rides on the next
/// flush of data, or on a flush made when the stream goes idle. It is
/// therefore allowed to lag behind the truth, never to run ahead of it.
/// Two slots are written alternately, so that a torn write of one leaves
/// the other intact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlushedMark {
    /// How many bytes from the start of the file are known to be flushed.
    pub flushed_bytes: u64,
    /// Counts the marks written to this file. The valid mark with the
    /// highest generation is the current one.
    pub generation: u32,
}

impl FlushedMark {
    /// Where this mark is stored in the file: the two slots alternate with
    /// the generation.
    #[must_use]
    pub const fn slot_offset(self) -> usize {
        FLUSHED_MARKS_OFFSET + FLUSHED_MARK_BYTES * (self.generation % 2) as usize
    }

    /// Serializes the mark for `segment`, checksum included.
    #[must_use]
    pub fn encode(self, segment: SegmentAddress) -> [u8; FLUSHED_MARK_BYTES] {
        let mut encoded = [0_u8; FLUSHED_MARK_BYTES];
        encoded[..U64_BYTES].copy_from_slice(&self.flushed_bytes.to_le_bytes());
        encoded[U64_BYTES..U64_BYTES + U32_BYTES].copy_from_slice(&self.generation.to_le_bytes());
        let checksum = flushed_mark_checksum(segment, &encoded);
        encoded[U64_BYTES + U32_BYTES..].copy_from_slice(&checksum.to_le_bytes());
        encoded
    }

    /// Returns the current mark stored in a segment header, or `None` if
    /// neither slot holds a valid one, as in a file that was never flushed
    /// past its header.
    #[must_use]
    pub fn latest_in(segment: SegmentAddress, header: &[u8]) -> Option<Self> {
        (0..2)
            .filter_map(|slot| {
                let slot_start = FLUSHED_MARKS_OFFSET + slot * FLUSHED_MARK_BYTES;
                let encoded = header.get(slot_start..slot_start + FLUSHED_MARK_BYTES)?;
                let stored = read_u32(encoded, U64_BYTES + U32_BYTES);
                (stored == flushed_mark_checksum(segment, encoded)).then(|| Self {
                    flushed_bytes: read_u64(encoded, 0),
                    generation: read_u32(encoded, U64_BYTES),
                })
            })
            .max_by_key(|mark| mark.generation)
    }
}

/// Computes the checksum of an encoded flushed mark, skipping the checksum
/// field. The segment's address is mixed in, so that an all-zero slot is
/// never valid and a mark copied from another file is refused.
fn flushed_mark_checksum(segment: SegmentAddress, encoded: &[u8]) -> u32 {
    let mut checksum = crc32c::crc32c(&segment.stream_id.get().to_le_bytes());
    checksum = crc32c::crc32c_append(checksum, &segment.segment_id.get().to_le_bytes());
    crc32c::crc32c_append(checksum, &encoded[..U64_BYTES + U32_BYTES])
}

/// Why records cannot be encoded as one batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum EncodeError {
    /// A batch must hold at least one record: a zero body length marks the
    /// end of the data in a segment file.
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum FrameError {
    /// The bytes end before the frame does. In a segment file this is a torn
    /// write; on the wire it is a truncated message.
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
    /// The checksum holds but the content breaks a format rule, which only a
    /// faulty writer can cause.
    #[error("frame is malformed: {reason}")]
    Malformed {
        /// Which rule the frame breaks.
        reason: &'static str,
    },
}

/// One batch of records: decoded header fields plus the exact frame bytes.
///
/// A `Batch` can only be obtained from [`Batch::encode`] or [`Batch::decode`],
/// so holding one proves its checksum and structure were verified.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Batch {
    first_sequence: SequenceNumber,
    record_count: u32,
    frame: Bytes,
}

impl Batch {
    /// Encodes `payloads` as one frame of `segment`, numbering the records
    /// from `first_sequence`.
    ///
    /// # Errors
    ///
    /// Returns [`EncodeError`] if there are no payloads, the batch would
    /// start at the empty position, or a size limit is exceeded.
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
        if let Some(oversized) = payloads.iter().find(|payload| payload.len() > MAX_RECORD_PAYLOAD_BYTES) {
            return Err(EncodeError::RecordTooLarge {
                payload_bytes: oversized.len(),
            });
        }
        let body_bytes: usize = payloads.iter().map(|payload| RECORD_HEADER_BYTES + payload.len()).sum();
        if body_bytes > MAX_BATCH_BODY_BYTES {
            return Err(EncodeError::BatchTooLarge { body_bytes });
        }
        let record_count = u32::try_from(payloads.len()).expect("a body within the limit holds fewer than 2^32");

        let mut frame = BytesMut::with_capacity(BATCH_HEADER_BYTES + body_bytes);
        frame.put_u32_le(u32::try_from(body_bytes).expect("the body limit fits in 32 bits"));
        frame.put_u64_le(first_sequence.get());
        frame.put_u32_le(record_count);
        frame.put_u32_le(0); // Placeholder, patched once the body is in place.
        for payload in payloads {
            frame.put_u32_le(u32::try_from(payload.len()).expect("the payload limit fits in 32 bits"));
            frame.put_slice(payload);
        }
        let checksum = frame_checksum(segment, &frame);
        frame[CHECKSUM_OFFSET..BATCH_HEADER_BYTES].copy_from_slice(&checksum.to_le_bytes());

        Ok(Self {
            first_sequence,
            record_count,
            frame: frame.freeze(),
        })
    }

    /// Decodes and verifies the frame at the start of `source`, which may
    /// hold further bytes after it.
    ///
    /// The returned batch shares `source`'s memory; nothing is copied.
    ///
    /// # Errors
    ///
    /// Returns [`FrameError`] if the bytes are not a complete, valid frame of
    /// `segment`.
    pub fn decode(segment: SegmentAddress, source: &Bytes) -> Result<Self, FrameError> {
        if source.is_empty() {
            return Err(FrameError::EndOfData);
        }
        if source.len() < BATCH_HEADER_BYTES {
            // Not `EndOfData`, even if every byte is zero: a body length such
            // as 65536 starts with zero bytes, and a caller reading a file in
            // chunks may have cut the header right after them.
            return Err(FrameError::Incomplete {
                needed_bytes: BATCH_HEADER_BYTES,
                available_bytes: source.len(),
            });
        }
        let body_bytes = usize::try_from(read_u32(source, 0)).expect("32 bits fit in usize");
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
        let frame = source.slice(..frame_bytes);
        let stored = read_u32(&frame, CHECKSUM_OFFSET);
        let computed = frame_checksum(segment, &frame);
        if stored != computed {
            return Err(FrameError::ChecksumMismatch { stored, computed });
        }

        let first_sequence = SequenceNumber::new(read_u64(&frame, FIRST_SEQUENCE_OFFSET));
        if first_sequence == SequenceNumber::ZERO {
            return Err(FrameError::Malformed {
                reason: "batch starts at the empty position",
            });
        }
        let record_count = read_u32(&frame, RECORD_COUNT_OFFSET);
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

    /// The sequence number of the batch's first record.
    #[must_use]
    pub const fn first_sequence(&self) -> SequenceNumber {
        self.first_sequence
    }

    /// The sequence number of the batch's last record.
    #[must_use]
    pub const fn last_sequence(&self) -> SequenceNumber {
        self.first_sequence.advanced_by(self.record_count as u64 - 1)
    }

    /// How many records the batch holds. Never zero.
    #[must_use]
    pub const fn record_count(&self) -> u32 {
        self.record_count
    }

    /// The exact bytes of the frame, as stored and sent.
    #[must_use]
    pub const fn frame(&self) -> &Bytes {
        &self.frame
    }

    /// Iterates over the batch's records in sequence order, without copying
    /// their payloads.
    #[must_use]
    pub fn records(&self) -> BatchRecords {
        BatchRecords {
            next_sequence: self.first_sequence,
            unread_body: self.frame.slice(BATCH_HEADER_BYTES..),
        }
    }
}

/// Iterator over the records of a [`Batch`].
#[derive(Clone, Debug)]
pub struct BatchRecords {
    next_sequence: SequenceNumber,
    unread_body: Bytes,
}

impl Iterator for BatchRecords {
    type Item = LogRecord;

    fn next(&mut self) -> Option<LogRecord> {
        if self.unread_body.is_empty() {
            return None;
        }
        // `Batch::decode` and `Batch::encode` validated the body, so every
        // length prefix is in bounds.
        let payload_bytes = usize::try_from(read_u32(&self.unread_body, 0)).expect("32 bits fit in usize");
        let mut record_bytes = self.unread_body.split_to(RECORD_HEADER_BYTES + payload_bytes);
        let payload = record_bytes.split_off(RECORD_HEADER_BYTES);
        let sequence = self.next_sequence;
        self.next_sequence = sequence.next();
        Some(LogRecord { sequence, payload })
    }
}

/// Why a run of frames is not what the receiver expected.
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

/// Decodes a run of back-to-back frames where every byte must belong to a
/// valid frame, as in an append request or a read response.
///
/// # Errors
///
/// Returns [`FramesError`] if any frame is invalid, or if the frames are not
/// consecutive in sequence.
pub fn decode_frames(segment: SegmentAddress, frames: &Bytes) -> Result<Vec<Batch>, FramesError> {
    let mut batches: Vec<Batch> = Vec::new();
    let mut offset_bytes = 0;
    while offset_bytes < frames.len() {
        let batch = Batch::decode(segment, &frames.slice(offset_bytes..))
            .map_err(|reason| FramesError::InvalidFrame { offset_bytes, reason })?;
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanStop {
    /// Every byte was consumed, or the rest is unwritten space.
    EndOfData,
    /// The bytes end in the middle of a frame. A caller reading a file in
    /// chunks should read on from here; at the true end of the file this is
    /// a torn write.
    Incomplete {
        /// How many bytes the cut frame needs in total, as far as is known.
        needed_bytes: usize,
    },
    /// The next frame is invalid: a torn or corrupted write.
    Invalid(FrameError),
    /// The next frame is valid but does not continue the sequence, so it is
    /// a leftover from an earlier, discarded tail.
    SequenceGap {
        /// The sequence number the next frame had to start at.
        expected: SequenceNumber,
        /// The sequence number it actually starts at.
        found: SequenceNumber,
    },
}

/// The valid batches at the start of some segment file content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidPrefix {
    /// The consecutive valid batches, in order.
    pub batches: Vec<Batch>,
    /// How many bytes those batches occupy.
    pub valid_bytes: usize,
    /// Why the scan did not go further.
    pub stop: ScanStop,
}

/// Finds the longest run of valid, consecutive batches at the start of
/// `content`, expecting the first one to start at `next_sequence`.
///
/// This is the crash-recovery scan: whatever follows the returned prefix was
/// never acknowledged, because a replica acknowledges only after the whole
/// frame is flushed. It never fails; invalid bytes only end the prefix.
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

const FIRST_SEQUENCE_OFFSET: usize = 4;
const RECORD_COUNT_OFFSET: usize = 12;
const CHECKSUM_OFFSET: usize = 16;

/// Computes the checksum of a whole frame, skipping the checksum field.
fn frame_checksum(segment: SegmentAddress, frame: &[u8]) -> u32 {
    let mut checksum = crc32c::crc32c(&segment.stream_id.get().to_le_bytes());
    checksum = crc32c::crc32c_append(checksum, &segment.segment_id.get().to_le_bytes());
    checksum = crc32c::crc32c_append(checksum, &frame[..CHECKSUM_OFFSET]);
    crc32c::crc32c_append(checksum, &frame[BATCH_HEADER_BYTES..])
}

/// Checks that `body` is exactly `record_count` well-formed records.
fn validate_body(body: &[u8], record_count: u32) -> Result<(), FrameError> {
    let mut unread_body = body;
    let mut records_found: u32 = 0;
    while !unread_body.is_empty() {
        if unread_body.len() < RECORD_HEADER_BYTES {
            return Err(FrameError::Malformed {
                reason: "body ends inside a record length",
            });
        }
        let payload_bytes = usize::try_from(read_u32(unread_body, 0)).expect("32 bits fit");
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

fn read_u32(source: &[u8], offset: usize) -> u32 {
    let mut raw = [0_u8; U32_BYTES];
    raw.copy_from_slice(&source[offset..offset + U32_BYTES]);
    u32::from_le_bytes(raw)
}

fn read_u64(source: &[u8], offset: usize) -> u64 {
    let mut raw = [0_u8; U64_BYTES];
    raw.copy_from_slice(&source[offset..offset + U64_BYTES]);
    u64::from_le_bytes(raw)
}

#[cfg(test)]
mod tests {
    use bitacora_types::DeterministicRandom;

    use super::*;

    const SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
    const OTHER_SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(4));

    fn header() -> SegmentHeader {
        SegmentHeader {
            address: SEGMENT,
            epoch: Epoch::new(5),
            first_sequence: SequenceNumber::new(100),
        }
    }

    fn payloads(texts: &[&'static str]) -> Vec<Bytes> {
        texts.iter().map(|text| Bytes::from(*text)).collect()
    }

    fn batch(first_sequence: u64, texts: &[&'static str]) -> Batch {
        Batch::encode(SEGMENT, SequenceNumber::new(first_sequence), &payloads(texts)).expect("valid batch")
    }

    fn concatenated(batches: &[Batch]) -> Bytes {
        let mut frames = BytesMut::new();
        for batch in batches {
            frames.put_slice(batch.frame());
        }
        frames.freeze()
    }

    /// Builds `batch_count` consecutive batches of random shape from `seed`.
    fn random_batches(seed: u64, batch_count: usize) -> Vec<Batch> {
        let mut random = DeterministicRandom::from_seed(seed);
        let mut next_sequence = SequenceNumber::new(random.between(1, 1_000));
        let mut batches = Vec::new();
        for _ in 0..batch_count {
            let record_count = random.between(1, 5);
            let record_payloads: Vec<Bytes> = (0..record_count)
                .map(|_| {
                    let payload_bytes = usize::try_from(random.between(0, 40)).expect("small");
                    Bytes::from(random.bytes(payload_bytes))
                })
                .collect();
            let batch = Batch::encode(SEGMENT, next_sequence, &record_payloads).expect("valid batch");
            next_sequence = batch.last_sequence().next();
            batches.push(batch);
        }
        batches
    }

    // ---- segment header ----

    #[test]
    fn header_survives_an_encode_decode_round_trip() {
        assert_eq!(SegmentHeader::decode(&header().encode()), Ok(header()));
    }

    #[test]
    fn header_shorter_than_its_size_is_incomplete() {
        let encoded = header().encode();
        assert_eq!(
            SegmentHeader::decode(&encoded[..SEGMENT_HEADER_BYTES - 1]),
            Err(HeaderError::Incomplete {
                available_bytes: SEGMENT_HEADER_BYTES - 1
            })
        );
    }

    #[test]
    fn header_with_wrong_magic_is_not_a_segment_file() {
        let mut encoded = header().encode();
        encoded[0] ^= 0xFF;
        assert_eq!(SegmentHeader::decode(&encoded), Err(HeaderError::WrongMagic));
    }

    #[test]
    fn header_with_any_flipped_field_bit_fails_its_checksum() {
        for flipped_byte in SEGMENT_MAGIC.len()..HEADER_CHECKSUM_OFFSET + U32_BYTES {
            let mut encoded = header().encode();
            encoded[flipped_byte] ^= 0x01;
            assert!(
                matches!(
                    SegmentHeader::decode(&encoded),
                    Err(HeaderError::ChecksumMismatch { .. })
                ),
                "flip at byte {flipped_byte} went undetected"
            );
        }
    }

    #[test]
    fn header_from_a_newer_format_is_refused() {
        let mut encoded = header().encode();
        encoded[8..12].copy_from_slice(&(SEGMENT_FORMAT_VERSION + 1).to_le_bytes());
        let checksum = crc32c::crc32c(&encoded[..HEADER_CHECKSUM_OFFSET]);
        encoded[HEADER_CHECKSUM_OFFSET..HEADER_CHECKSUM_OFFSET + U32_BYTES].copy_from_slice(&checksum.to_le_bytes());
        assert_eq!(
            SegmentHeader::decode(&encoded),
            Err(HeaderError::UnsupportedVersion {
                found_version: SEGMENT_FORMAT_VERSION + 1
            })
        );
    }

    #[test]
    fn header_starting_at_the_empty_position_is_refused() {
        let starts_at_zero = SegmentHeader {
            first_sequence: SequenceNumber::ZERO,
            ..header()
        };
        assert_eq!(
            SegmentHeader::decode(&starts_at_zero.encode()),
            Err(HeaderError::StartsAtEmptyPosition)
        );
    }

    // ---- flushed mark ----

    fn header_with_marks(marks: &[FlushedMark]) -> Vec<u8> {
        let mut header_bytes = header().encode().to_vec();
        for mark in marks {
            let slot_offset = mark.slot_offset();
            header_bytes[slot_offset..slot_offset + FLUSHED_MARK_BYTES].copy_from_slice(&mark.encode(SEGMENT));
        }
        header_bytes
    }

    fn mark(flushed_bytes: u64, generation: u32) -> FlushedMark {
        FlushedMark {
            flushed_bytes,
            generation,
        }
    }

    #[test]
    fn fresh_header_holds_no_flushed_mark() {
        assert_eq!(FlushedMark::latest_in(SEGMENT, &header().encode()), None);
    }

    #[test]
    fn stored_mark_is_read_back() {
        let header_bytes = header_with_marks(&[mark(4_096, 1)]);
        assert_eq!(FlushedMark::latest_in(SEGMENT, &header_bytes), Some(mark(4_096, 1)));
    }

    #[test]
    fn mark_with_the_highest_generation_is_the_current_one() {
        let header_bytes = header_with_marks(&[mark(4_096, 4), mark(8_192, 5)]);
        assert_eq!(FlushedMark::latest_in(SEGMENT, &header_bytes), Some(mark(8_192, 5)));
    }

    #[test]
    fn consecutive_marks_use_different_slots() {
        assert_ne!(mark(0, 4).slot_offset(), mark(0, 5).slot_offset());
    }

    #[test]
    fn torn_mark_leaves_the_previous_one_current() {
        let mut header_bytes = header_with_marks(&[mark(4_096, 4), mark(8_192, 5)]);
        header_bytes[mark(0, 5).slot_offset() + 3] ^= 0xFF;
        assert_eq!(FlushedMark::latest_in(SEGMENT, &header_bytes), Some(mark(4_096, 4)));
    }

    #[test]
    fn mark_of_another_segment_is_refused() {
        let header_bytes = header_with_marks(&[mark(4_096, 1)]);
        assert_eq!(FlushedMark::latest_in(OTHER_SEGMENT, &header_bytes), None);
    }

    #[test]
    fn storing_a_mark_leaves_the_header_identity_valid() {
        let header_bytes = header_with_marks(&[mark(4_096, 1), mark(8_192, 2)]);
        assert_eq!(SegmentHeader::decode(&header_bytes), Ok(header()));
    }

    // ---- batch encoding ----

    #[test]
    fn batch_numbers_its_records_from_the_first_sequence() {
        let sequences: Vec<u64> = batch(100, &["a", "b", "c"])
            .records()
            .map(|record| record.sequence.get())
            .collect();
        assert_eq!(sequences, [100, 101, 102]);
    }

    #[test]
    fn batch_returns_the_payloads_it_was_given() {
        let decoded_payloads: Vec<Bytes> = batch(100, &["alpha", "", "gamma"])
            .records()
            .map(|record| record.payload)
            .collect();
        assert_eq!(decoded_payloads, payloads(&["alpha", "", "gamma"]));
    }

    #[test]
    fn batch_last_sequence_is_its_final_record() {
        assert_eq!(batch(100, &["a", "b", "c"]).last_sequence(), SequenceNumber::new(102));
    }

    #[test]
    fn batch_without_records_is_rejected() {
        assert_eq!(
            Batch::encode(SEGMENT, SequenceNumber::FIRST, &[]),
            Err(EncodeError::NoRecords)
        );
    }

    #[test]
    fn batch_starting_at_the_empty_position_is_rejected() {
        assert_eq!(
            Batch::encode(SEGMENT, SequenceNumber::ZERO, &payloads(&["a"])),
            Err(EncodeError::StartsAtEmptyPosition)
        );
    }

    #[test]
    fn batch_accepts_a_record_of_exactly_the_maximum_size() {
        let largest_payload = Bytes::from(vec![0xAB; MAX_RECORD_PAYLOAD_BYTES]);
        let encoded = Batch::encode(SEGMENT, SequenceNumber::FIRST, std::slice::from_ref(&largest_payload))
            .expect("the maximum size is allowed");
        assert_eq!(encoded.records().next().expect("one record").payload, largest_payload);
    }

    #[test]
    fn batch_rejects_a_record_over_the_maximum_size() {
        let oversized_payload = Bytes::from(vec![0xAB; MAX_RECORD_PAYLOAD_BYTES + 1]);
        assert_eq!(
            Batch::encode(SEGMENT, SequenceNumber::FIRST, &[oversized_payload]),
            Err(EncodeError::RecordTooLarge {
                payload_bytes: MAX_RECORD_PAYLOAD_BYTES + 1
            })
        );
    }

    #[test]
    fn batch_rejects_a_body_over_the_maximum_size() {
        let record_payload = Bytes::from(vec![0xAB; MAX_RECORD_PAYLOAD_BYTES]);
        let too_many = vec![record_payload; 3];
        assert!(matches!(
            Batch::encode(SEGMENT, SequenceNumber::FIRST, &too_many),
            Err(EncodeError::BatchTooLarge { .. })
        ));
    }

    // ---- batch decoding ----

    #[test]
    fn decoding_an_encoded_frame_returns_the_same_batch() {
        let encoded = batch(100, &["a", "b"]);
        assert_eq!(Batch::decode(SEGMENT, encoded.frame()), Ok(encoded));
    }

    #[test]
    fn decoding_ignores_bytes_after_the_frame() {
        let first = batch(100, &["a", "b"]);
        let both = concatenated(&[first.clone(), batch(102, &["c"])]);
        assert_eq!(Batch::decode(SEGMENT, &both), Ok(first));
    }

    #[test]
    fn frame_for_another_segment_fails_its_checksum() {
        let encoded = batch(100, &["a"]);
        assert!(matches!(
            Batch::decode(OTHER_SEGMENT, encoded.frame()),
            Err(FrameError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn frame_with_any_flipped_bit_is_never_accepted_as_is() {
        let encoded = batch(100, &["alpha", "beta"]);
        for flipped_byte in 0..encoded.frame().len() {
            for flipped_bit in 0..8 {
                let mut damaged = encoded.frame().to_vec();
                damaged[flipped_byte] ^= 1 << flipped_bit;
                assert!(
                    Batch::decode(SEGMENT, &Bytes::from(damaged)).is_err(),
                    "flip of bit {flipped_bit} at byte {flipped_byte} went undetected"
                );
            }
        }
    }

    #[test]
    fn frame_cut_short_is_incomplete() {
        let encoded = batch(100, &["alpha"]);
        let cut = encoded.frame().slice(..encoded.frame().len() - 1);
        assert_eq!(
            Batch::decode(SEGMENT, &cut),
            Err(FrameError::Incomplete {
                needed_bytes: encoded.frame().len(),
                available_bytes: encoded.frame().len() - 1,
            })
        );
    }

    #[test]
    fn zero_filled_space_is_the_end_of_data() {
        let unwritten_space = Bytes::from(vec![0_u8; 256]);
        assert_eq!(Batch::decode(SEGMENT, &unwritten_space), Err(FrameError::EndOfData));
    }

    /// A body length such as 65536 starts with zero bytes, so a header cut
    /// short must ask for more bytes even when all it shows is zeros.
    #[test]
    fn a_cut_header_of_zero_bytes_is_incomplete_not_the_end() {
        let cut_header = Bytes::from(vec![0_u8; BATCH_HEADER_BYTES - 1]);
        assert_eq!(
            Batch::decode(SEGMENT, &cut_header),
            Err(FrameError::Incomplete {
                needed_bytes: BATCH_HEADER_BYTES,
                available_bytes: BATCH_HEADER_BYTES - 1,
            })
        );
    }

    #[test]
    fn no_bytes_at_all_are_the_end_of_data() {
        assert_eq!(Batch::decode(SEGMENT, &Bytes::new()), Err(FrameError::EndOfData));
    }

    #[test]
    fn frame_claiming_an_oversized_body_is_refused_before_reading_it() {
        let mut oversized = BytesMut::new();
        oversized.put_u32_le(u32::try_from(MAX_BATCH_BODY_BYTES + 1).expect("fits"));
        oversized.put_slice(&[0_u8; BATCH_HEADER_BYTES]);
        assert_eq!(
            Batch::decode(SEGMENT, &oversized.freeze()),
            Err(FrameError::BodyTooLarge {
                body_bytes: MAX_BATCH_BODY_BYTES + 1
            })
        );
    }

    /// Builds a frame with a correct checksum around an arbitrary body, the
    /// way only a faulty writer could.
    fn frame_with_valid_checksum(first_sequence: u64, record_count: u32, body: &[u8]) -> Bytes {
        let mut frame = BytesMut::new();
        frame.put_u32_le(u32::try_from(body.len()).expect("small body"));
        frame.put_u64_le(first_sequence);
        frame.put_u32_le(record_count);
        frame.put_u32_le(0);
        frame.put_slice(body);
        let checksum = frame_checksum(SEGMENT, &frame);
        frame[CHECKSUM_OFFSET..BATCH_HEADER_BYTES].copy_from_slice(&checksum.to_le_bytes());
        frame.freeze()
    }

    #[test]
    fn frame_whose_record_count_disagrees_with_its_body_is_malformed() {
        let one_record_body = [1, 0, 0, 0, b'a'];
        let frame = frame_with_valid_checksum(100, 2, &one_record_body);
        assert_eq!(
            Batch::decode(SEGMENT, &frame),
            Err(FrameError::Malformed {
                reason: "record count does not match the body"
            })
        );
    }

    #[test]
    fn frame_whose_record_runs_past_the_body_is_malformed() {
        let claims_nine_bytes_holds_one = [9, 0, 0, 0, b'a'];
        let frame = frame_with_valid_checksum(100, 1, &claims_nine_bytes_holds_one);
        assert_eq!(
            Batch::decode(SEGMENT, &frame),
            Err(FrameError::Malformed {
                reason: "body ends inside a record payload"
            })
        );
    }

    #[test]
    fn frame_whose_body_ends_inside_a_length_is_malformed() {
        let frame = frame_with_valid_checksum(100, 1, &[1, 0]);
        assert_eq!(
            Batch::decode(SEGMENT, &frame),
            Err(FrameError::Malformed {
                reason: "body ends inside a record length"
            })
        );
    }

    #[test]
    fn frame_starting_at_the_empty_position_is_malformed() {
        let frame = frame_with_valid_checksum(0, 1, &[1, 0, 0, 0, b'a']);
        assert_eq!(
            Batch::decode(SEGMENT, &frame),
            Err(FrameError::Malformed {
                reason: "batch starts at the empty position"
            })
        );
    }

    #[test]
    fn frame_running_past_the_sequence_space_is_malformed() {
        let frame = frame_with_valid_checksum(u64::MAX, 1, &[1, 0, 0, 0, b'a']);
        assert_eq!(
            Batch::decode(SEGMENT, &frame),
            Err(FrameError::Malformed {
                reason: "batch runs past the end of the sequence space"
            })
        );
    }

    // ---- strict decoding of a run of frames ----

    #[test]
    fn consecutive_frames_decode_in_order() {
        let batches = vec![batch(100, &["a", "b"]), batch(102, &["c"])];
        assert_eq!(decode_frames(SEGMENT, &concatenated(&batches)), Ok(batches));
    }

    #[test]
    fn no_frames_decode_to_no_batches() {
        assert_eq!(decode_frames(SEGMENT, &Bytes::new()), Ok(Vec::new()));
    }

    #[test]
    fn frames_with_a_sequence_gap_are_refused() {
        let frames = concatenated(&[batch(100, &["a", "b"]), batch(103, &["c"])]);
        assert_eq!(
            decode_frames(SEGMENT, &frames),
            Err(FramesError::SequenceGap {
                expected: SequenceNumber::new(102),
                found: SequenceNumber::new(103),
            })
        );
    }

    #[test]
    fn frames_followed_by_trailing_garbage_are_refused() {
        let first = batch(100, &["a"]);
        let mut frames = first.frame().to_vec();
        frames.extend_from_slice(&[0xFF; 7]);
        assert!(matches!(
            decode_frames(SEGMENT, &Bytes::from(frames)),
            Err(FramesError::InvalidFrame { offset_bytes, .. }) if offset_bytes == first.frame().len()
        ));
    }

    // ---- recovery scan ----

    #[test]
    fn scan_of_intact_content_finds_every_batch() {
        let batches = random_batches(1, 8);
        let scanned = scan_valid_prefix(SEGMENT, batches[0].first_sequence(), &concatenated(&batches));
        assert_eq!((scanned.batches, scanned.stop), (batches, ScanStop::EndOfData));
    }

    #[test]
    fn scan_stops_at_unwritten_space() {
        let batches = random_batches(2, 3);
        let mut content = concatenated(&batches).to_vec();
        let written_bytes = content.len();
        content.extend_from_slice(&[0_u8; 512]);
        let scanned = scan_valid_prefix(SEGMENT, batches[0].first_sequence(), &Bytes::from(content));
        assert_eq!(
            (scanned.valid_bytes, scanned.stop),
            (written_bytes, ScanStop::EndOfData)
        );
    }

    #[test]
    fn scan_stops_at_a_frame_that_does_not_continue_the_sequence() {
        let content = concatenated(&[batch(100, &["a", "b"]), batch(110, &["stale"])]);
        let scanned = scan_valid_prefix(SEGMENT, SequenceNumber::new(100), &content);
        assert_eq!(
            (scanned.batches.len(), scanned.stop),
            (
                1,
                ScanStop::SequenceGap {
                    expected: SequenceNumber::new(102),
                    found: SequenceNumber::new(110),
                }
            )
        );
    }

    #[test]
    fn scan_refuses_content_that_starts_at_the_wrong_sequence() {
        let content = concatenated(&[batch(100, &["a"])]);
        let scanned = scan_valid_prefix(SEGMENT, SequenceNumber::new(99), &content);
        assert_eq!(scanned.batches.len(), 0);
    }

    /// The property the design names for the recovery scan: any byte prefix
    /// of a valid file recovers to a valid prefix of its batches.
    #[test]
    fn any_byte_prefix_recovers_to_a_prefix_of_the_batches() {
        for seed in 0..40 {
            let batches = random_batches(seed, 6);
            let content = concatenated(&batches);
            for prefix_bytes in 0..=content.len() {
                let scanned = scan_valid_prefix(SEGMENT, batches[0].first_sequence(), &content.slice(..prefix_bytes));
                let whole_batches = batches
                    .iter()
                    .scan(0, |end_offset, batch| {
                        *end_offset += batch.frame().len();
                        Some(*end_offset)
                    })
                    .take_while(|end_offset| *end_offset <= prefix_bytes)
                    .count();
                assert_eq!(
                    scanned.batches,
                    batches[..whole_batches],
                    "seed {seed}, prefix of {prefix_bytes} bytes"
                );
            }
        }
    }

    /// A torn write can leave any mix of old and new bytes in the last
    /// frame. Whatever it leaves, the scan must keep every batch before the
    /// damage and never return a batch that was not written.
    #[test]
    fn damage_never_invalidates_earlier_batches_or_invents_new_ones() {
        for seed in 0..200 {
            let mut random = DeterministicRandom::from_seed(seed);
            let batches = random_batches(seed, 6);
            let mut content = concatenated(&batches).to_vec();
            let damaged_byte = random.index(content.len());
            content[damaged_byte] ^= u8::try_from(random.between(1, 255)).expect("one byte");

            let scanned = scan_valid_prefix(SEGMENT, batches[0].first_sequence(), &Bytes::from(content));

            let batches_before_damage = batches
                .iter()
                .scan(0, |end_offset, batch| {
                    *end_offset += batch.frame().len();
                    Some(*end_offset)
                })
                .take_while(|end_offset| *end_offset <= damaged_byte)
                .count();
            assert_eq!(
                scanned.batches,
                batches[..batches_before_damage],
                "seed {seed}, damage at byte {damaged_byte}"
            );
        }
    }

    /// Fuzz-style check: no input, however hostile, makes the decoder panic
    /// or accept a frame whose checksum it did not verify.
    #[test]
    fn arbitrary_bytes_never_panic_the_scanner() {
        for seed in 0..2_000 {
            let mut random = DeterministicRandom::from_seed(seed);
            let content_bytes = usize::try_from(random.between(0, 200)).expect("small");
            let content = Bytes::from(random.bytes(content_bytes));
            let scanned = scan_valid_prefix(SEGMENT, SequenceNumber::FIRST, &content);
            assert!(scanned.valid_bytes <= content.len(), "seed {seed}");
        }
    }
}
