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
//! - Looking up a block allocates nothing: it is a binary search and a walk over the entries.
//!
//! # See also
//!
//! - `format.rs`: the batch frames and the header of segment files.
//! - `index.rs`: the rule that decides where a block starts, shared with the in-memory index of a file.
//! - `docs/protocol/tiered-object.md`: the same layout, for other languages.

use std::ops::Range;

use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

use crate::format::{Batch, FramesError, SegmentHeader, decode_frames, to_u64};
use crate::index::{INDEX_INTERVAL_BYTES, IndexEntry, SparseIndex};

/// The first bytes of the footer of every tiered object.
pub const TIERED_MAGIC: [u8; 8] = *b"BITATIER";

/// The tiered object format this code reads and writes.
pub const TIERED_FORMAT_VERSION: u32 = 1;

/// The size of the footer at the end of every tiered object, in bytes.
pub const TIERED_FOOTER_BYTES: usize = 72;

/// How far apart two blocks of a tiered object start, at least. The same interval as the in-memory index of
/// a segment file, so a read costs about the same wherever the segment lives.
pub const TIERED_BLOCK_BYTES: u64 = INDEX_INTERVAL_BYTES;

/// The size of one block index entry, in bytes: a sequence number and an offset.
const INDEX_ENTRY_BYTES: usize = 16;

/// Where the footer's own checksum starts: it is the last field and covers everything before it.
const FOOTER_CHECKSUM_OFFSET: usize = TIERED_FOOTER_BYTES - 4;

/// Why bytes are not a valid tiered object, or cannot become one.
///
/// # Examples
///
/// ```
/// use bitacora_segment::tiered::{TieredError, TieredFooter};
///
/// let refused = TieredFooter::decode(b"too short");
///
/// assert_eq!(refused, Err(TieredError::IncompleteFooter { available_bytes: 9 }));
/// ```
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
///
/// A footer is obtained from [`TieredFooter::decode`] only, so holding one proves its checksum was
/// verified.
///
/// # Examples
///
/// ```
/// use bitacora_segment::format::{Batch, SegmentHeader};
/// use bitacora_segment::tiered::{TieredFooter, encode_tiered_segment};
/// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
/// use bytes::Bytes;
///
/// let address = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
/// let segment = SegmentHeader {
///     address,
///     epoch: Epoch::new(5),
///     first_sequence: SequenceNumber::new(100),
/// };
/// let batch = Batch::encode(address, SequenceNumber::new(100), &[Bytes::from_static(b"sealed")])?;
/// let object = encode_tiered_segment(segment, SequenceNumber::new(100), batch.frame())?;
///
/// let footer = TieredFooter::decode(&object)?;
///
/// assert_eq!(footer.segment, segment);
/// assert_eq!(footer.sealed_end, SequenceNumber::new(100));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TieredFooter {
    /// The identity of the segment, as in the header of its segment files.
    pub segment: SegmentHeader,
    /// The segment's final sequence number. One below the first sequence number if the segment was sealed
    /// empty.
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
    /// The magic and the version are checked first, so that bytes which are not a footer at all, or are a
    /// footer of a newer format, are named as such instead of failing a checksum.
    ///
    /// # Arguments
    ///
    /// * `object_end` - The end of the object: its last [`TIERED_FOOTER_BYTES`] bytes, or more. Bytes
    ///   before the footer are ignored.
    ///
    /// # Returns
    ///
    /// What the footer says about the object.
    ///
    /// # Errors
    ///
    /// * [`TieredError::IncompleteFooter`] if there are fewer bytes than a footer.
    /// * [`TieredError::BadMagic`] if the bytes do not end with a tiered segment footer.
    /// * [`TieredError::UnsupportedVersion`] if the footer is not of the format this code reads.
    /// * [`TieredError::FooterChecksumMismatch`] if the footer fails its checksum.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_segment::tiered::{TIERED_FOOTER_BYTES, TieredError, TieredFooter};
    ///
    /// let not_an_object = [b'x'; TIERED_FOOTER_BYTES];
    ///
    /// assert_eq!(TieredFooter::decode(&not_an_object), Err(TieredError::BadMagic));
    /// ```
    pub fn decode(object_end: &[u8]) -> Result<Self, TieredError> {
        let Some(footer_start) = object_end.len().checked_sub(TIERED_FOOTER_BYTES) else {
            return Err(TieredError::IncompleteFooter {
                available_bytes: object_end.len(),
            });
        };
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

    /// Serializes the footer, its own checksum included.
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

    /// Returns where the block index lies in the object.
    ///
    /// # Returns
    ///
    /// The byte range to fetch and hand to [`TieredIndex::decode`]. It starts where the frames end and is
    /// empty for a segment that was sealed empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_segment::format::SegmentHeader;
    /// use bitacora_segment::tiered::{TieredFooter, encode_tiered_segment};
    /// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
    /// use bytes::Bytes;
    ///
    /// let segment = SegmentHeader {
    ///     address: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
    ///     epoch: Epoch::new(5),
    ///     first_sequence: SequenceNumber::new(100),
    /// };
    /// let sealed_empty = encode_tiered_segment(segment, SequenceNumber::new(99), &Bytes::new())?;
    ///
    /// let footer = TieredFooter::decode(&sealed_empty)?;
    ///
    /// assert_eq!(footer.index_range(), 0..0);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn index_range(&self) -> Range<u64> {
        self.frames_bytes..self.frames_bytes + self.index_bytes()
    }

    /// Returns the size of the whole object.
    ///
    /// # Returns
    ///
    /// The bytes of frames, block index and footer together. A reader compares it with the size the
    /// metadata store recorded.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_segment::format::SegmentHeader;
    /// use bitacora_segment::tiered::{TIERED_FOOTER_BYTES, TieredFooter, encode_tiered_segment};
    /// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
    /// use bytes::Bytes;
    ///
    /// let segment = SegmentHeader {
    ///     address: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
    ///     epoch: Epoch::new(5),
    ///     first_sequence: SequenceNumber::new(100),
    /// };
    /// let sealed_empty = encode_tiered_segment(segment, SequenceNumber::new(99), &Bytes::new())?;
    ///
    /// let footer = TieredFooter::decode(&sealed_empty)?;
    ///
    /// assert_eq!(footer.object_bytes(), u64::try_from(TIERED_FOOTER_BYTES)?);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn object_bytes(&self) -> u64 {
        self.frames_bytes + self.index_bytes() + to_u64(TIERED_FOOTER_BYTES)
    }

    /// The size of the block index the footer describes, in bytes.
    fn index_bytes(&self) -> u64 {
        u64::from(self.index_entry_count) * to_u64(INDEX_ENTRY_BYTES)
    }
}

/// The block index of a tiered object: where to start reading for a given sequence number.
///
/// # Examples
///
/// ```
/// use bitacora_segment::format::{Batch, SegmentHeader};
/// use bitacora_segment::tiered::{TieredFooter, TieredIndex, encode_tiered_segment};
/// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
/// use bytes::Bytes;
///
/// let address = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
/// let segment = SegmentHeader {
///     address,
///     epoch: Epoch::new(5),
///     first_sequence: SequenceNumber::new(100),
/// };
/// let batch = Batch::encode(address, SequenceNumber::new(100), &[Bytes::from_static(b"sealed")])?;
/// let object = encode_tiered_segment(segment, SequenceNumber::new(100), batch.frame())?;
/// let footer = TieredFooter::decode(&object)?;
/// let index_range = footer.index_range();
/// let index_bytes = &object[usize::try_from(index_range.start)?..usize::try_from(index_range.end)?];
///
/// let index = TieredIndex::decode(&footer, index_bytes)?;
///
/// assert_eq!(index.blocks_from(SequenceNumber::new(100), 1), 0..footer.frames_bytes);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TieredIndex {
    /// Where each block starts, in object order, which is sequence order.
    blocks: Vec<IndexEntry>,
    /// How many bytes of batch frames the object starts with: where the last block ends.
    frames_bytes: u64,
}

impl TieredIndex {
    /// Parses and verifies the block index that a footer describes.
    ///
    /// The size and the checksum are checked against the footer, then the entries are checked against
    /// each other: they must start at the first frame, increase, and stay inside the frames.
    ///
    /// # Arguments
    ///
    /// * `footer` - The footer of the object the index belongs to.
    /// * `index_bytes` - The bytes at [`TieredFooter::index_range`].
    ///
    /// # Returns
    ///
    /// The index, ready for lookups.
    ///
    /// # Errors
    ///
    /// Fails with [`TieredError::DamagedIndex`] if the bytes are not the index the footer describes: they
    /// have another size, fail the checksum, or hold entries that cannot describe the frames.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_segment::format::SegmentHeader;
    /// use bitacora_segment::tiered::{TieredError, TieredFooter, TieredIndex, encode_tiered_segment};
    /// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
    /// use bytes::Bytes;
    ///
    /// let segment = SegmentHeader {
    ///     address: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
    ///     epoch: Epoch::new(5),
    ///     first_sequence: SequenceNumber::new(100),
    /// };
    /// let sealed_empty = encode_tiered_segment(segment, SequenceNumber::new(99), &Bytes::new())?;
    /// let footer = TieredFooter::decode(&sealed_empty)?;
    ///
    /// let refused = TieredIndex::decode(&footer, b"an index of another size");
    ///
    /// assert!(matches!(refused, Err(TieredError::DamagedIndex { .. })));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn decode(footer: &TieredFooter, index_bytes: &[u8]) -> Result<Self, TieredError> {
        if to_u64(index_bytes.len()) != footer.index_bytes() {
            return Err(TieredError::DamagedIndex {
                reason: "its size is not what the footer says",
            });
        }
        if crc32c::crc32c(index_bytes) != footer.index_checksum {
            return Err(TieredError::DamagedIndex {
                reason: "it fails its checksum",
            });
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
        if !blocks_describe_the_frames(&blocks, footer) {
            return Err(TieredError::DamagedIndex {
                reason: "its entries do not describe the frames",
            });
        }
        Ok(Self {
            blocks,
            frames_bytes: footer.frames_bytes,
        })
    }

    /// Returns the bytes of the object to fetch in order to read from a sequence number.
    ///
    /// The range is the block that holds the sequence number and as many of the following blocks as fit
    /// in the limit. It is always at least one whole block, so it starts and ends between two frames.
    ///
    /// # Arguments
    ///
    /// * `sequence` - The first record wanted. A sequence number below the segment's first is read from
    ///   the start; one above its end gets the last block.
    /// * `max_bytes` - How many bytes to aim for. The first block is fetched whole even if it is larger.
    ///
    /// # Returns
    ///
    /// The byte range of whole blocks. Empty only if the segment is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_segment::format::SegmentHeader;
    /// use bitacora_segment::tiered::{TieredFooter, TieredIndex, encode_tiered_segment};
    /// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
    /// use bytes::Bytes;
    ///
    /// let segment = SegmentHeader {
    ///     address: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
    ///     epoch: Epoch::new(5),
    ///     first_sequence: SequenceNumber::new(100),
    /// };
    /// let sealed_empty = encode_tiered_segment(segment, SequenceNumber::new(99), &Bytes::new())?;
    /// let footer = TieredFooter::decode(&sealed_empty)?;
    /// let index = TieredIndex::decode(&footer, &[])?;
    ///
    /// assert_eq!(index.blocks_from(SequenceNumber::new(100), 1_024), 0..0);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn blocks_from(&self, sequence: SequenceNumber, max_bytes: u64) -> Range<u64> {
        let blocks_at_or_before = self.blocks.partition_point(|block| block.first_sequence <= sequence);
        // A sequence below the segment's first is read from the start.
        let first_block = blocks_at_or_before.saturating_sub(1);
        let Some(start_block) = self.blocks.get(first_block) else {
            return 0..0;
        };
        let start = start_block.offset_bytes;
        // The first block is fetched whole, whatever the limit; later ones only while they fit.
        let mut end = self.end_of_block(first_block);
        for later_block in first_block + 1..self.blocks.len() {
            let later_end = self.end_of_block(later_block);
            if later_end - start > max_bytes {
                break;
            }
            end = later_end;
        }
        start..end
    }

    /// Where the block with the given number ends: at the start of the next block, or at the end of the
    /// frames for the last one.
    fn end_of_block(&self, block_number: usize) -> u64 {
        match self.blocks.get(block_number + 1) {
            Some(next_block) => next_block.offset_bytes,
            None => self.frames_bytes,
        }
    }
}

/// Whether the entries of a block index can be the index of the frames a footer describes.
///
/// They can if there are none and the object has no frames, or if the first entry is the segment's first
/// record at offset zero, every later entry is further on in both sequence and offset, and the last one
/// starts inside the frames.
fn blocks_describe_the_frames(blocks: &[IndexEntry], footer: &TieredFooter) -> bool {
    let (Some(first_block), Some(last_block)) = (blocks.first(), blocks.last()) else {
        return footer.frames_bytes == 0;
    };
    let starts_at_the_first_frame =
        first_block.offset_bytes == 0 && first_block.first_sequence == footer.segment.first_sequence;
    let ends_inside_the_frames = last_block.offset_bytes < footer.frames_bytes;
    if !starts_at_the_first_frame || !ends_inside_the_frames {
        return false;
    }
    for neighbours in blocks.windows(2) {
        let is_ordered = neighbours[0].first_sequence < neighbours[1].first_sequence
            && neighbours[0].offset_bytes < neighbours[1].offset_bytes;
        if !is_ordered {
            return false;
        }
    }
    true
}

/// Builds the tiered object of a sealed segment from the batch frames of one of its copies.
///
/// Every frame is verified before anything is built, and the frames must be exactly the segment, so an
/// upload never stores a short or damaged copy. The frames are copied into the object unchanged.
///
/// # Arguments
///
/// * `segment` - The identity of the sealed segment.
/// * `sealed_end` - The segment's final sequence number. One below its first sequence number if it was
///   sealed empty.
/// * `frames` - The batch frames of a copy: the bytes of its segment file after the header.
///
/// # Returns
///
/// The whole object: frames, block index and footer.
///
/// # Errors
///
/// * [`TieredError::InvalidFrames`] if any frame is invalid or the frames are not consecutive.
/// * [`TieredError::WrongExtent`] if the frames are not exactly the records from the segment's first
///   sequence number to `sealed_end`.
///
/// # Panics
///
/// Panics if the frames hold 2^32 blocks or more, which would take a segment of 256 TiB.
///
/// # Examples
///
/// ```
/// use bitacora_segment::format::{Batch, SegmentHeader};
/// use bitacora_segment::tiered::{TieredError, encode_tiered_segment};
/// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
/// use bytes::Bytes;
///
/// let address = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
/// let segment = SegmentHeader {
///     address,
///     epoch: Epoch::new(5),
///     first_sequence: SequenceNumber::new(100),
/// };
/// let batch = Batch::encode(address, SequenceNumber::new(100), &[Bytes::from_static(b"sealed")])?;
///
/// let object = encode_tiered_segment(segment, SequenceNumber::new(100), batch.frame())?;
/// let short_copy = encode_tiered_segment(segment, SequenceNumber::new(105), batch.frame());
///
/// assert!(object.starts_with(batch.frame()));
/// assert!(matches!(short_copy, Err(TieredError::WrongExtent { .. })));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn encode_tiered_segment(
    segment: SegmentHeader,
    sealed_end: SequenceNumber,
    frames: &Bytes,
) -> Result<Bytes, TieredError> {
    let batches = decode_frames(segment.address, frames)?;
    require_the_sealed_extent(segment, sealed_end, &batches)?;

    let encoded_index = encode_block_index(&batches);
    let index_entry_count = encoded_index.len() / INDEX_ENTRY_BYTES;
    let footer = TieredFooter {
        segment,
        sealed_end,
        frames_bytes: to_u64(frames.len()),
        index_entry_count: u32::try_from(index_entry_count)
            .expect("a segment has far fewer than 2^32 blocks of 64 KiB"),
        index_checksum: crc32c::crc32c(&encoded_index),
    };

    let mut object = BytesMut::with_capacity(frames.len() + encoded_index.len() + TIERED_FOOTER_BYTES);
    object.put_slice(frames);
    object.put_slice(&encoded_index);
    object.put_slice(&footer.encode());
    Ok(object.freeze())
}

/// Checks that `batches` are exactly the sealed segment: its first record to its sealed end, or nothing
/// at all for a segment that was sealed empty.
///
/// Fails with [`TieredError::WrongExtent`] otherwise.
fn require_the_sealed_extent(
    segment: SegmentHeader,
    sealed_end: SequenceNumber,
    batches: &[Batch],
) -> Result<(), TieredError> {
    let found_first = batches.first().map(Batch::first_sequence);
    let found_end = batches.last().map(Batch::last_sequence);
    let is_the_sealed_segment = match (found_first, found_end) {
        (Some(first), Some(end)) => first == segment.first_sequence && end == sealed_end,
        _no_records => sealed_end == segment.first_sequence.preceding(),
    };
    if is_the_sealed_segment {
        return Ok(());
    }
    Err(TieredError::WrongExtent {
        expected_first: segment.first_sequence,
        expected_end: sealed_end,
        found_first,
        found_end,
    })
}

/// Builds the block index of `batches` laid end to end, and serializes it.
///
/// The blocks start where the sparse index of a segment file would put its entries, with offsets counted
/// from the first frame.
fn encode_block_index(batches: &[Batch]) -> BytesMut {
    let mut index = SparseIndex::new();
    let mut offset_bytes = 0;
    for batch in batches {
        index.observe_batch(batch.first_sequence(), offset_bytes);
        offset_bytes += to_u64(batch.frame().len());
    }
    let mut encoded_index = BytesMut::with_capacity(index.entries().len() * INDEX_ENTRY_BYTES);
    for block in index.entries() {
        encoded_index.put_u64_le(block.first_sequence.get());
        encoded_index.put_u64_le(block.offset_bytes);
    }
    encoded_index
}

#[cfg(test)]
#[path = "tiered_test.rs"]
mod tests;
