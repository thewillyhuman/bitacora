//! The layer files of the page adapter.
//!
//! # Why this file exists
//!
//! The page adapter keeps the history of every page as immutable files: delta layers, which hold the
//! records that changed pages over a range of sequence numbers, and image layers, which hold whole pages
//! as of one sequence number (design, section 11, "Versioned pages"; the layering follows Neon). A page
//! read fetches one page out of a layer that may hold thousands, so the file must let a reader find and
//! check one page without downloading the rest. This file defines those bytes.
//!
//! # What it contains
//!
//! - [`encode_layer`]: the bytes of a layer, from its pages.
//! - [`decode_header`], [`decode_index`], [`verify_blob`]: the three steps of reading one page.
//! - [`encode_records`] and [`decode_records`]: the contents of one page's entry in a delta layer.
//! - [`LayerKind`], [`LayerHeader`], [`LayerEntry`], [`LayerDamage`].
//!
//! # How it works
//!
//! All integers are little-endian. One layout serves both kinds of layer: a sorted map from page
//! identifier to a blob.
//!
//! ```text
//! layer   header | index | blob*
//! header  magic 8 | format version u32 | kind u8 | first sequence u64 | last sequence u64
//!         | page count u32 | index length u32 | index crc32c u32 | header crc32c u32
//! index   entry*, sorted by page identifier bytes
//! entry   page identifier length u32 | page identifier | blob offset u64 | blob length u32
//!         | blob crc32c u32
//! blob    in an image layer: the page
//!         in a delta layer:  (sequence u64 | payload length u32 | payload)*
//! ```
//!
//! The header has a fixed size, so a reader fetches it first, learns how long the index is, fetches the
//! index, and then fetches exactly the blob it wants. Each of the three parts carries its own checksum,
//! so each fetch can be checked on its own. An image layer covers one sequence number: its first and last
//! sequence are equal. A blob offset counts from the start of the layer.
//!
//! # Invariants
//!
//! - Pure functions over bytes: no I/O.
//! - Decoding never panics, whatever the input.
//! - Page identifiers are opaque bytes here. Their order in the index is byte order, which need not be the
//!   order of the identifiers they encode; nothing depends on it beyond being deterministic.
//!
//! # See also
//!
//! - `page_store.rs`: what decides which layers exist and reads pages through them.

use std::collections::BTreeMap;
use std::ops::Range;

use bitacora_types::{LogRecord, SequenceNumber};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// The first bytes of every layer.
const LAYER_MAGIC: [u8; 8] = *b"BITALAYR";

/// The layer format this code reads and writes.
const LAYER_FORMAT_VERSION: u32 = 1;

/// The size of the header at the start of every layer.
pub(crate) const LAYER_HEADER_BYTES: usize = 45;

/// Where the header's own checksum starts: it is the last field and covers everything before it.
const HEADER_CHECKSUM_OFFSET: usize = LAYER_HEADER_BYTES - 4;

/// The fixed part of a delta record: its sequence number and the length of its payload.
const RECORD_HEADER_BYTES: usize = 12;

/// What the blobs of a layer hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LayerKind {
    /// Whole pages as of one sequence number.
    Image,
    /// The records that changed pages over a range of sequence numbers.
    Delta,
}

impl LayerKind {
    /// The byte the kind is stored as.
    const fn code(self) -> u8 {
        match self {
            Self::Image => 1,
            Self::Delta => 2,
        }
    }

    /// The kind a stored byte stands for, if any.
    const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Image),
            2 => Some(Self::Delta),
            _ => None,
        }
    }
}

/// Why bytes are not the part of a layer they should be.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LayerDamage {
    /// What is wrong, for the error that names the artifact.
    pub(crate) reason: &'static str,
}

/// Shorthand for the error of a damaged layer.
const fn damage(reason: &'static str) -> LayerDamage {
    LayerDamage { reason }
}

/// What the header says about a layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LayerHeader {
    /// What the blobs hold.
    pub(crate) kind: LayerKind,
    /// The first sequence number the layer covers.
    pub(crate) first_sequence: SequenceNumber,
    /// The last sequence number the layer covers. Equal to the first for an image layer.
    pub(crate) last_sequence: SequenceNumber,
    /// How many pages the layer holds.
    pub(crate) page_count: u32,
    /// How many bytes the index takes, right after the header.
    pub(crate) index_bytes: u32,
    /// The checksum of the index.
    index_checksum: u32,
}

impl LayerHeader {
    /// Where the index lies in the layer.
    pub(crate) const fn index_range(&self) -> Range<u64> {
        let index_start = LAYER_HEADER_BYTES as u64;
        index_start..index_start + self.index_bytes as u64
    }
}

/// Where one page's blob lies in a layer, and what it must hash to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LayerEntry {
    /// Where the blob starts, counted from the start of the layer.
    blob_offset: u64,
    /// How long the blob is.
    blob_bytes: u32,
    /// The checksum of the blob.
    blob_checksum: u32,
}

impl LayerEntry {
    /// Where the blob lies in the layer.
    pub(crate) const fn blob_range(&self) -> Range<u64> {
        self.blob_offset..self.blob_offset + self.blob_bytes as u64
    }
}

/// Builds a layer from its pages.
///
/// # Arguments
///
/// * `kind` - What the blobs hold.
/// * `first_sequence`, `last_sequence` - The sequence numbers the layer covers.
/// * `blobs_by_page` - The blob of every page of the layer, keyed by the page's encoded identifier.
///
/// # Returns
///
/// The whole layer, ready to be stored as one artifact.
///
/// # Panics
///
/// Panics if a page identifier or blob is 4 GiB or larger, or the layer holds 2^32 pages or more.
pub(crate) fn encode_layer(
    kind: LayerKind,
    first_sequence: SequenceNumber,
    last_sequence: SequenceNumber,
    blobs_by_page: &BTreeMap<Bytes, Bytes>,
) -> Bytes {
    let mut index_bytes = 0;
    for encoded_page_id in blobs_by_page.keys() {
        index_bytes += 4 + encoded_page_id.len() + 8 + 4 + 4;
    }
    let mut index = BytesMut::with_capacity(index_bytes);
    let mut blob_offset = (LAYER_HEADER_BYTES + index_bytes) as u64;
    for (encoded_page_id, blob) in blobs_by_page {
        index.put_u32_le(length_as_u32(encoded_page_id.len()));
        index.put_slice(encoded_page_id);
        index.put_u64_le(blob_offset);
        index.put_u32_le(length_as_u32(blob.len()));
        index.put_u32_le(crc32c::crc32c(blob));
        blob_offset += blob.len() as u64;
    }

    let mut layer =
        BytesMut::with_capacity(usize::try_from(blob_offset).expect("a layer that was just built fits in memory"));
    layer.put_slice(&LAYER_MAGIC);
    layer.put_u32_le(LAYER_FORMAT_VERSION);
    layer.put_u8(kind.code());
    layer.put_u64_le(first_sequence.get());
    layer.put_u64_le(last_sequence.get());
    layer.put_u32_le(length_as_u32(blobs_by_page.len()));
    layer.put_u32_le(length_as_u32(index.len()));
    layer.put_u32_le(crc32c::crc32c(&index));
    let header_checksum = crc32c::crc32c(&layer);
    layer.put_u32_le(header_checksum);
    layer.put_slice(&index);
    for blob in blobs_by_page.values() {
        layer.put_slice(blob);
    }
    layer.freeze()
}

/// Parses and verifies the header at the start of a layer.
///
/// # Arguments
///
/// * `layer_start` - The first [`LAYER_HEADER_BYTES`] bytes of the layer, or more.
///
/// # Errors
///
/// Returns [`LayerDamage`] if the bytes are too few, are not a layer, come from a newer format, or fail
/// their checksum.
pub(crate) fn decode_header(layer_start: &[u8]) -> Result<LayerHeader, LayerDamage> {
    let Some(header) = layer_start.get(..LAYER_HEADER_BYTES) else {
        return Err(damage("it is shorter than a layer header"));
    };
    let mut stored_checksum = &header[HEADER_CHECKSUM_OFFSET..];
    if stored_checksum.get_u32_le() != crc32c::crc32c(&header[..HEADER_CHECKSUM_OFFSET]) {
        return Err(damage("its header fails its checksum"));
    }
    let mut unread = header;
    let mut magic = [0_u8; LAYER_MAGIC.len()];
    unread.copy_to_slice(&mut magic);
    if magic != LAYER_MAGIC {
        return Err(damage("it does not start like a layer"));
    }
    if unread.get_u32_le() != LAYER_FORMAT_VERSION {
        return Err(damage("it was written by a newer format"));
    }
    let Some(kind) = LayerKind::from_code(unread.get_u8()) else {
        return Err(damage("its kind is unknown"));
    };
    Ok(LayerHeader {
        kind,
        first_sequence: SequenceNumber::new(unread.get_u64_le()),
        last_sequence: SequenceNumber::new(unread.get_u64_le()),
        page_count: unread.get_u32_le(),
        index_bytes: unread.get_u32_le(),
        index_checksum: unread.get_u32_le(),
    })
}

/// Parses and verifies the index of a layer.
///
/// # Arguments
///
/// * `header` - The layer's header.
/// * `index` - The bytes at [`LayerHeader::index_range`].
///
/// # Returns
///
/// Where the blob of each page lies, keyed by the page's encoded identifier.
///
/// # Errors
///
/// Returns [`LayerDamage`] if the bytes are not the index the header describes.
pub(crate) fn decode_index(header: &LayerHeader, index: &Bytes) -> Result<BTreeMap<Bytes, LayerEntry>, LayerDamage> {
    if index.len() != header.index_bytes as usize {
        return Err(damage("its index is not the size its header says"));
    }
    if crc32c::crc32c(index) != header.index_checksum {
        return Err(damage("its index fails its checksum"));
    }
    let cut_short = |_cut_short| damage("its index is cut short");
    let mut entries = BTreeMap::new();
    let mut unread = index.clone();
    for _page in 0..header.page_count {
        let page_id_bytes = unread.try_get_u32_le().map_err(cut_short)? as usize;
        if unread.remaining() < page_id_bytes {
            return Err(damage("its index is cut short"));
        }
        let encoded_page_id = unread.split_to(page_id_bytes);
        let entry = LayerEntry {
            blob_offset: unread.try_get_u64_le().map_err(cut_short)?,
            blob_bytes: unread.try_get_u32_le().map_err(cut_short)?,
            blob_checksum: unread.try_get_u32_le().map_err(cut_short)?,
        };
        entries.insert(encoded_page_id, entry);
    }
    if unread.has_remaining() || entries.len() != header.page_count as usize {
        return Err(damage("its index does not hold the pages its header says"));
    }
    Ok(entries)
}

/// Checks that `blob` is the blob `entry` describes.
///
/// # Errors
///
/// Returns [`LayerDamage`] if the length or the checksum differs.
pub(crate) fn verify_blob(entry: &LayerEntry, blob: &[u8]) -> Result<(), LayerDamage> {
    if blob.len() != entry.blob_bytes as usize {
        return Err(damage("a page is not the size its index says"));
    }
    if crc32c::crc32c(blob) != entry.blob_checksum {
        return Err(damage("a page fails its checksum"));
    }
    Ok(())
}

/// Serializes the records that changed one page, as its blob in a delta layer.
///
/// # Arguments
///
/// * `records` - The records, in sequence order.
pub(crate) fn encode_records(records: &[LogRecord]) -> Bytes {
    let mut blob_bytes = 0;
    for record in records {
        blob_bytes += RECORD_HEADER_BYTES + record.payload.len();
    }
    let mut blob = BytesMut::with_capacity(blob_bytes);
    for record in records {
        blob.put_u64_le(record.sequence.get());
        blob.put_u32_le(length_as_u32(record.payload.len()));
        blob.put_slice(&record.payload);
    }
    blob.freeze()
}

/// Parses the blob of one page of a delta layer, already verified with [`verify_blob`].
///
/// # Returns
///
/// The records that changed the page, in the order they were stored. Their payloads share the blob's
/// memory.
///
/// # Errors
///
/// Returns [`LayerDamage`] if the blob ends in the middle of a record.
pub(crate) fn decode_records(blob: &Bytes) -> Result<Vec<LogRecord>, LayerDamage> {
    let cut_short = |_cut_short| damage("a page's records are cut short");
    let mut records = Vec::new();
    let mut unread = blob.clone();
    while unread.has_remaining() {
        let sequence = SequenceNumber::new(unread.try_get_u64_le().map_err(cut_short)?);
        let payload_bytes = unread.try_get_u32_le().map_err(cut_short)? as usize;
        if unread.remaining() < payload_bytes {
            return Err(damage("a page's records are cut short"));
        }
        records.push(LogRecord {
            sequence,
            payload: unread.split_to(payload_bytes),
        });
    }
    Ok(records)
}

/// A length as the four bytes it is stored in.
fn length_as_u32(length: usize) -> u32 {
    u32::try_from(length).expect("a part of a layer is smaller than 4 GiB")
}

#[cfg(test)]
#[path = "layer_test.rs"]
mod tests;
