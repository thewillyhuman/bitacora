//! The byte layout of a sealed segment in the object store.
//!
//! # Why this file exists
//!
//! A sealed segment is uploaded to the object store as one object (design, section 10, "Tiering"). After
//! the replicas drop their copies, that object is the only place its records can be read from, and it is
//! read with range requests: a reader must find the bytes for one sequence number without downloading a
//! gigabyte. This file defines the object so that this is possible, and so that damage to any part of it
//! is detected.
//!
//! # What it contains
//!
//! - [`encode_tiered_segment`]: builds the object from the batch frames of a sealed copy.
//! - [`TieredFooter`]: the fixed-size trailer that identifies the segment and locates the block index.
//! - [`TieredIndex`]: the block index, which maps a sequence number to the bytes to fetch.
//! - [`TieredError`]: why bytes are not a valid tiered object, or cannot become one.
//!
//! # How it works
//!
//! All integers are little-endian.
//!
//! ```text
//! tiered object  batch frame* | block index | footer
//! block index    entry*
//! entry          first sequence u64 | offset u64
//! footer         magic 8 | format version u32 | stream id u64 | segment id u64
//!                | epoch u64 | first sequence u64 | sealed end u64
//!                | frames length u64 | index entry count u32
//!                | index crc32c u32 | footer crc32c u32
//! ```
//!
//! The batch frames are the bytes of the segment file after its header, unchanged, so the checksum the
//! writer computed still guards every batch. An index entry says where a block begins: a batch frame at
//! least [`TIERED_BLOCK_BYTES`] after the previous entry. A block is everything from one entry to the next.
//!
//! A reader that knows the object's size, which the metadata store records, fetches the footer, then the
//! index, then whole blocks. The footer is at the end, not the start, because the index can only be
//! written after the frames have been walked, and an object is written in one piece.
//!
//! # Invariants
//!
//! - Everything here is a pure function over bytes: no I/O, no clock.
//! - An object is built only from frames that are exactly the sealed segment, first record to sealed end.
//!   A short or damaged copy never reaches the object store.
//! - A block range returned by [`TieredIndex::blocks_from`] starts and ends between two frames, so the
//!   fetched bytes always decode on their own.
//!
//! # See also
//!
//! - `format.rs`: the batch frames and the header of segment files.
//! - `index.rs`: the rule that decides where a block starts, shared with the in-memory index of a file.

use std::ops::Range;

use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

use crate::format::{FramesError, SegmentHeader, decode_frames};
use crate::index::{INDEX_INTERVAL_BYTES, IndexEntry, SparseIndex};

/// The first bytes of the footer of every tiered object.
pub const TIERED_MAGIC: [u8; 8] = *b"BITATIER";

/// The tiered object format this code reads and writes.
pub const TIERED_FORMAT_VERSION: u32 = 1;

/// The size of the footer at the end of every tiered object.
pub const TIERED_FOOTER_BYTES: usize = 72;

/// How far apart two blocks of a tiered object start, at least. The same interval as the in-memory index of
/// a segment file, so a read costs about the same wherever the segment lives.
pub const TIERED_BLOCK_BYTES: u64 = INDEX_INTERVAL_BYTES;

/// The size of one block index entry: a sequence number and an offset.
const INDEX_ENTRY_BYTES: usize = 16;

/// Where the footer's own checksum starts: it is the last field and covers everything before it.
const FOOTER_CHECKSUM_OFFSET: usize = TIERED_FOOTER_BYTES - 4;

/// Why bytes are not a valid tiered object, or cannot become one.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum TieredError {
    /// The frames to upload are not a valid run of batches.
    #[error("segment frames are invalid: {0}")]
    InvalidFrames(#[from] FramesError),
    /// The frames to upload are not exactly the sealed segment.
    #[error(
        "frames hold records {found_first:?} to {found_end:?}, but the segment runs from \
         {expected_first} to {expected_end}"
    )]
    WrongExtent {
        /// The segment's first sequence number.
        expected_first: SequenceNumber,
        /// The segment's sealed end.
        expected_end: SequenceNumber,
        /// The first record found in the frames, if any.
        found_first: Option<SequenceNumber>,
        /// The last record found in the frames, if any.
        found_end: Option<SequenceNumber>,
    },
    /// Fewer bytes than a footer were given.
    #[error("a footer is {TIERED_FOOTER_BYTES} bytes but only {available_bytes} were read")]
    IncompleteFooter {
        /// How many bytes were given.
        available_bytes: usize,
    },
    /// The bytes do not end like a tiered object.
    #[error("object does not end with a tiered segment footer")]
    BadMagic,
    /// The object was written by a newer version of the format.
    #[error("tiered format version {found} is not supported")]
    UnsupportedVersion {
        /// The version found in the footer.
        found: u32,
    },
    /// The footer fails its checksum.
    #[error("footer checksum is {computed:#010x} but {stored:#010x} is stored")]
    FooterChecksumMismatch {
        /// The checksum stored in the footer.
        stored: u32,
        /// The checksum of the footer as read.
        computed: u32,
    },
    /// The index fails its checksum or is not the size the footer says.
    #[error("block index is damaged: {reason}")]
    DamagedIndex {
        /// What is wrong with it.
        reason: &'static str,
    },
}

/// What the footer says about a tiered object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TieredFooter {
    /// The identity of the segment, as in the header of its segment files.
    pub segment: SegmentHeader,
    /// The segment's final sequence number. One below the first sequence
    /// number if the segment was sealed empty.
    pub sealed_end: SequenceNumber,
    /// How many bytes of batch frames the object starts with.
    pub frames_bytes: u64,
    /// How many entries the block index has.
    pub index_entry_count: u32,
    /// The checksum of the block index.
    index_checksum: u32,
}

impl TieredFooter {
    /// Parses and verifies a footer from the last bytes of an object.
    ///
    /// # Errors
    ///
    /// Returns [`TieredError`] if the bytes are too short, are not a footer,
    /// come from a newer format, or fail their checksum.
    pub fn decode(object_end: &[u8]) -> Result<Self, TieredError> {
        let footer_start = object_end
            .len()
            .checked_sub(TIERED_FOOTER_BYTES)
            .ok_or(TieredError::IncompleteFooter {
                available_bytes: object_end.len(),
            })?;
        let encoded = &object_end[footer_start..];
        let mut unread = encoded;
        let mut magic = [0_u8; TIERED_MAGIC.len()];
        unread.copy_to_slice(&mut magic);
        if magic != TIERED_MAGIC {
            return Err(TieredError::BadMagic);
        }
        let version = unread.get_u32_le();
        if version != TIERED_FORMAT_VERSION {
            return Err(TieredError::UnsupportedVersion { found: version });
        }
        let stream_id = StreamId::new(unread.get_u64_le());
        let segment_id = SegmentId::new(unread.get_u64_le());
        let epoch = Epoch::new(unread.get_u64_le());
        let first_sequence = SequenceNumber::new(unread.get_u64_le());
        let sealed_end = SequenceNumber::new(unread.get_u64_le());
        let frames_bytes = unread.get_u64_le();
        let index_entry_count = unread.get_u32_le();
        let index_checksum = unread.get_u32_le();
        let stored = unread.get_u32_le();
        let computed = crc32c::crc32c(&encoded[..FOOTER_CHECKSUM_OFFSET]);
        if stored != computed {
            return Err(TieredError::FooterChecksumMismatch { stored, computed });
        }
        Ok(Self {
            segment: SegmentHeader {
                address: SegmentAddress::new(stream_id, segment_id),
                epoch,
                first_sequence,
            },
            sealed_end,
            frames_bytes,
            index_entry_count,
            index_checksum,
        })
    }

    fn encode(&self) -> [u8; TIERED_FOOTER_BYTES] {
        let mut encoded = [0_u8; TIERED_FOOTER_BYTES];
        let mut unwritten = &mut encoded[..];
        unwritten.put_slice(&TIERED_MAGIC);
        unwritten.put_u32_le(TIERED_FORMAT_VERSION);
        unwritten.put_u64_le(self.segment.address.stream_id.get());
        unwritten.put_u64_le(self.segment.address.segment_id.get());
        unwritten.put_u64_le(self.segment.epoch.get());
        unwritten.put_u64_le(self.segment.first_sequence.get());
        unwritten.put_u64_le(self.sealed_end.get());
        unwritten.put_u64_le(self.frames_bytes);
        unwritten.put_u32_le(self.index_entry_count);
        unwritten.put_u32_le(self.index_checksum);
        let checksum = crc32c::crc32c(&encoded[..FOOTER_CHECKSUM_OFFSET]);
        encoded[FOOTER_CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_le_bytes());
        encoded
    }

    /// Where the block index lies in the object.
    #[must_use]
    pub fn index_range(&self) -> Range<u64> {
        self.frames_bytes..self.frames_bytes + self.index_bytes()
    }

    /// The size of the whole object.
    #[must_use]
    pub fn object_bytes(&self) -> u64 {
        self.frames_bytes + self.index_bytes() + TIERED_FOOTER_BYTES as u64
    }

    fn index_bytes(&self) -> u64 {
        u64::from(self.index_entry_count) * INDEX_ENTRY_BYTES as u64
    }
}

/// The block index of a tiered object: where to start reading for a given
/// sequence number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TieredIndex {
    /// Where each block starts, in object order, which is sequence order.
    blocks: Vec<IndexEntry>,
    frames_bytes: u64,
}

impl TieredIndex {
    /// Parses and verifies the block index that `footer` describes.
    ///
    /// # Errors
    ///
    /// Returns [`TieredError::DamagedIndex`] if the bytes are not the index
    /// the footer describes.
    pub fn decode(footer: &TieredFooter, index_bytes: &[u8]) -> Result<Self, TieredError> {
        let damaged = |reason| TieredError::DamagedIndex { reason };
        if index_bytes.len() as u64 != footer.index_bytes() {
            return Err(damaged("its size is not what the footer says"));
        }
        if crc32c::crc32c(index_bytes) != footer.index_checksum {
            return Err(damaged("it fails its checksum"));
        }
        // The size was checked above, so the bytes are a whole number of entries.
        let mut blocks = Vec::with_capacity(index_bytes.len() / INDEX_ENTRY_BYTES);
        let mut unread = index_bytes;
        while unread.has_remaining() {
            blocks.push(IndexEntry {
                first_sequence: SequenceNumber::new(unread.get_u64_le()),
                offset_bytes: unread.get_u64_le(),
            });
        }
        let is_ordered = blocks
            .windows(2)
            .all(|pair| pair[0].first_sequence < pair[1].first_sequence && pair[0].offset_bytes < pair[1].offset_bytes);
        let starts_at_the_first_frame = blocks.first().is_none_or(|first_block| {
            first_block.offset_bytes == 0 && first_block.first_sequence == footer.segment.first_sequence
        });
        let ends_inside_the_frames = blocks
            .last()
            .is_none_or(|last_block| last_block.offset_bytes < footer.frames_bytes);
        let covers_the_frames = blocks.is_empty() == (footer.frames_bytes == 0);
        if !(is_ordered && starts_at_the_first_frame && ends_inside_the_frames && covers_the_frames) {
            return Err(damaged("its entries do not describe the frames"));
        }
        Ok(Self {
            blocks,
            frames_bytes: footer.frames_bytes,
        })
    }

    /// The bytes of the object to fetch in order to read from `sequence`:
    /// the block that holds it and as many of the following blocks as fit
    /// in `max_bytes`. Always at least one whole block, so the range starts
    /// and ends between two frames. Empty only if the segment is empty.
    #[must_use]
    pub fn blocks_from(&self, sequence: SequenceNumber, max_bytes: u64) -> Range<u64> {
        let blocks_at_or_before = self.blocks.partition_point(|block| block.first_sequence <= sequence);
        // A sequence below the segment's first is read from the start.
        let first_block = blocks_at_or_before.saturating_sub(1);
        let Some(start) = self.blocks.get(first_block).map(|block| block.offset_bytes) else {
            return 0..0;
        };
        let later_block_starts = self.blocks[first_block + 1..].iter().map(|block| block.offset_bytes);
        let mut end = None;
        for block_end in later_block_starts.chain([self.frames_bytes]) {
            if end.is_some() && block_end - start > max_bytes {
                break;
            }
            end = Some(block_end);
        }
        start..end.unwrap_or(self.frames_bytes)
    }
}

/// Builds the tiered object of a sealed segment from the batch frames of
/// one of its copies.
///
/// # Errors
///
/// Returns [`TieredError`] if the frames are not valid, or are not exactly
/// the records from the segment's first sequence number to `sealed_end`.
/// An upload therefore never stores a short or damaged copy.
pub fn encode_tiered_segment(
    segment: SegmentHeader,
    sealed_end: SequenceNumber,
    frames: &Bytes,
) -> Result<Bytes, TieredError> {
    let batches = decode_frames(segment.address, frames)?;
    let found_first = batches.first().map(crate::format::Batch::first_sequence);
    let found_end = batches.last().map(crate::format::Batch::last_sequence);
    let is_the_sealed_segment = match (found_first, found_end) {
        (Some(first), Some(end)) => first == segment.first_sequence && end == sealed_end,
        _no_records => sealed_end == segment.first_sequence.preceding(),
    };
    if !is_the_sealed_segment {
        return Err(TieredError::WrongExtent {
            expected_first: segment.first_sequence,
            expected_end: sealed_end,
            found_first,
            found_end,
        });
    }

    let mut index = SparseIndex::new();
    let mut offset_bytes = 0;
    for batch in &batches {
        index.observe_batch(batch.first_sequence(), offset_bytes);
        offset_bytes += batch.frame().len() as u64;
    }
    let mut encoded_index = BytesMut::with_capacity(index.entries().len() * INDEX_ENTRY_BYTES);
    for block in index.entries() {
        encoded_index.put_u64_le(block.first_sequence.get());
        encoded_index.put_u64_le(block.offset_bytes);
    }
    let footer = TieredFooter {
        segment,
        sealed_end,
        frames_bytes: frames.len() as u64,
        index_entry_count: u32::try_from(index.entries().len())
            .expect("a segment has far fewer than 2^32 blocks of 64 KiB"),
        index_checksum: crc32c::crc32c(&encoded_index),
    };

    let mut object = BytesMut::with_capacity(frames.len() + encoded_index.len() + TIERED_FOOTER_BYTES);
    object.put_slice(frames);
    object.put_slice(&encoded_index);
    object.put_slice(&footer.encode());
    Ok(object.freeze())
}

#[cfg(test)]
#[path = "tiered_test.rs"]
mod tests;
