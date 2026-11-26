//! The block: the immutable file that holds the samples of a stretch of the log.
//!
//! # Why this file exists
//!
//! The metrics store keeps recent samples in memory and everything older in blocks in the object store
//! (design, section 19, "Prometheus metrics"). A block is written once, when the builder seals what it
//! holds, and from then on it is the only copy of those samples: the log below it is truncated. A query
//! must find the series it wants in a block and fetch only their samples, because a block can be large
//! and the object store is far away. This file defines the block so that both are possible, and so that
//! damage to any part of it is detected.
//!
//! # What it contains
//!
//! - [`encode_block`]: series and their samples as the bytes of a block.
//! - [`BlockSummary`]: how many series and samples a block holds and which span of time they cover.
//!   It is in the block's footer and, as 28 bytes, in the block's catalog entry.
//! - `BlockFooter`, `SeriesEntry`, `BlockIndex`: the parts of a block a reader fetches first.
//! - `load_block_index`, `read_series`: the two steps of a query against one block, with range reads.
//! - `decode_whole_block`: everything in a block, for merging blocks and for restoring a saved head.
//! - [`DamagedBlock`]: why bytes are not a block.
//!
//! # How it works
//!
//! All integers are little-endian. This is format version 1.
//!
//! ```text
//! block         chunk* | series index | postings | footer
//! series index  series entry*
//! series entry  labels | chunk offset u64 | chunk length u32 | chunk crc32c u32
//!               | sample count u32 | min time i64 | max time i64
//! labels        label count u16 | label*
//! label         name length u16 | name | value length u16 | value
//! postings      pair count u32 | posting*
//! posting       name length u16 | name | value length u16 | value | series count u32 | series number u32*
//! footer        magic 8 "BITAMBLK" | format version u32 | series count u32 | sample count u64
//!               | min time i64 | max time i64
//!               | series index offset u64 | series index length u64 | postings length u64
//!               | series index crc32c u32 | postings crc32c u32 | footer crc32c u32
//! ```
//!
//! - There is one chunk per series (see `chunk.rs`), in the order of the series index, back to back from
//!   the start of the block. Series are in ascending order of their labels, and a series' number is its
//!   position in the index, from 0.
//! - A series entry says where the series' chunk is and gives the checksum of the chunk's bytes, so that
//!   one chunk can be fetched and verified alone. It also gives the times of the series' first and last
//!   sample, so that a query for another span of time does not fetch the chunk at all.
//! - The postings list, for every label name and value that occurs, the numbers of the series that have
//!   it, ascending. Pairs are in ascending order of name, then value. A query for `job="api"` reads one
//!   list instead of looking at every series.
//! - The postings start right after the series index. The footer is the last 76 bytes. Its own checksum
//!   covers the footer before it. The minimum and maximum time of a block without samples are both 0.
//!
//! The catalog entry of a block carries the block's summary (`series count u32 | sample count u64 |
//! min time i64 | max time i64`), so a reader can leave a block alone without fetching anything.
//!
//! A reader that knows the block's size, which the catalog records, fetches the footer, then the series
//! index and the postings in one read, then the chunks of the series it selected, joining chunks that lie
//! next to each other into one read. A block of a million series is still read in a handful of requests
//! when the query wants a handful of series.
//!
//! The footer is at the end because the index can only be written once every chunk has been, and a block
//! is stored in one piece.
//!
//! # Invariants
//!
//! - Everything here except the two functions that take an `ArtifactNamespace` is a pure function over
//!   bytes.
//! - Encoding is deterministic: equal series and samples give equal bytes.
//! - Nothing is returned from a block before the checksum that covers it was verified.
//! - The samples of a series in a block are strictly increasing in time.
//!
//! # See also
//!
//! - `chunk.rs`: the samples of one series inside a block.
//! - `builder.rs`: where blocks are written and merged.
//! - `query.rs`: where blocks are read.

use std::collections::BTreeMap;
use std::ops::Range;

use bitacora_materializer::{ArtifactNamespace, MaterializerError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

use crate::chunk::{decode_chunk, decode_chunk_in_range, encode_chunk};
use crate::record::{LabelsReadError, put_labels, take_labels};
use crate::sample::{Label, Labels, Sample, SeriesSamples, TimeRange};
use crate::selector::Selector;

/// The first bytes of the footer of every block.
const BLOCK_MAGIC: [u8; 8] = *b"BITAMBLK";

/// The block format this code reads and writes.
const BLOCK_FORMAT_VERSION: u32 = 1;

/// The size of the footer at the end of every block.
pub(crate) const BLOCK_FOOTER_BYTES: usize = 76;

/// The size of a block's summary in its catalog entry: two counts and two times.
const BLOCK_SUMMARY_BYTES: usize = 4 + 8 + 8 + 8;

/// Where the footer's own checksum starts: it is the last field and covers everything before it.
const FOOTER_CHECKSUM_OFFSET: usize = BLOCK_FOOTER_BYTES - 4;

/// Two selected chunks this close together are fetched in one read, gap included. A request to an
/// object store costs far more than these bytes do.
const JOINED_READ_GAP_BYTES: u64 = 64 * 1024;

/// Why bytes are not a block, or not the part of a block they were read as.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("block is damaged: {reason}")]
pub struct DamagedBlock {
    /// What is wrong with the bytes.
    pub reason: String,
}

impl DamagedBlock {
    /// A damaged block with the reason `reason`.
    fn because(reason: &str) -> Self {
        Self {
            reason: reason.to_owned(),
        }
    }
}

/// What a block holds, in four numbers.
///
/// # Examples
///
/// ```
/// use bitacora_metrics::BlockSummary;
///
/// let summary = BlockSummary { series_count: 2, sample_count: 240, min_time_millis: 0, max_time_millis: 59_500 };
/// assert_eq!(summary.sample_count, 240);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockSummary {
    /// How many series the block holds.
    pub series_count: u32,
    /// How many samples the block holds, over all series.
    pub sample_count: u64,
    /// The time of the earliest sample in the block. 0 if the block holds no sample.
    pub min_time_millis: i64,
    /// The time of the latest sample in the block. 0 if the block holds no sample.
    pub max_time_millis: i64,
}

impl BlockSummary {
    /// Whether the block can hold a sample in `time_range`.
    pub(crate) const fn overlaps(&self, time_range: TimeRange) -> bool {
        self.sample_count > 0 && time_range.overlaps(self.min_time_millis, self.max_time_millis)
    }

    /// The summary as the bytes a catalog entry carries: the four numbers, little-endian, in the order
    /// of the fields.
    pub(crate) fn encode(&self) -> Bytes {
        let mut encoded = BytesMut::with_capacity(BLOCK_SUMMARY_BYTES);
        encoded.put_u32_le(self.series_count);
        encoded.put_u64_le(self.sample_count);
        encoded.put_i64_le(self.min_time_millis);
        encoded.put_i64_le(self.max_time_millis);
        encoded.freeze()
    }

    /// Reads the summary of a block out of the block's catalog entry.
    ///
    /// # Arguments
    ///
    /// * `catalog_summary` - `ArtifactDescriptor::summary` of a catalog entry of a metrics store, as
    ///   [`MetricsStore::blocks`](crate::MetricsStore::blocks) and
    ///   [`BlockReader::blocks`](crate::BlockReader::blocks) return it.
    ///
    /// # Returns
    ///
    /// The summary, or `None` if the bytes are not a summary of this format. The block's footer then
    /// still says the same; it only costs a fetch to learn it.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_metrics::BlockSummary;
    ///
    /// let mut catalog_summary = Vec::new();
    /// catalog_summary.extend_from_slice(&2_u32.to_le_bytes());
    /// catalog_summary.extend_from_slice(&240_u64.to_le_bytes());
    /// catalog_summary.extend_from_slice(&0_i64.to_le_bytes());
    /// catalog_summary.extend_from_slice(&59_500_i64.to_le_bytes());
    ///
    /// let summary = BlockSummary::decode(&catalog_summary).expect("28 bytes are a summary");
    ///
    /// assert_eq!(summary.max_time_millis, 59_500);
    /// assert_eq!(BlockSummary::decode(b"something else"), None);
    /// ```
    #[must_use]
    pub fn decode(catalog_summary: &[u8]) -> Option<Self> {
        if catalog_summary.len() != BLOCK_SUMMARY_BYTES {
            return None;
        }
        let mut unread = catalog_summary;
        Some(Self {
            series_count: unread.get_u32_le(),
            sample_count: unread.get_u64_le(),
            min_time_millis: unread.get_i64_le(),
            max_time_millis: unread.get_i64_le(),
        })
    }
}

/// A block as bytes, with what it holds.
pub(crate) struct EncodedBlock {
    /// The bytes to store as one artifact.
    pub bytes: Bytes,
    /// What the block holds.
    pub summary: BlockSummary,
}

/// Encodes series and their samples as one block.
///
/// # Arguments
///
/// * `samples_by_series` - Every series of the block with its samples, strictly increasing in time. A
///   series without samples is left out of the block.
///
/// # Returns
///
/// The block's bytes and its summary. A block of no series is valid: a footer and an empty index.
pub(crate) fn encode_block(samples_by_series: &BTreeMap<Labels, Vec<Sample>>) -> EncodedBlock {
    let mut block = BytesMut::new();
    let mut series_index = BytesMut::new();
    let mut series_by_label: BTreeMap<&Label, Vec<u32>> = BTreeMap::new();
    let mut summary = BlockSummary {
        series_count: 0,
        sample_count: 0,
        min_time_millis: i64::MAX,
        max_time_millis: i64::MIN,
    };

    for (labels, samples) in samples_by_series {
        let (Some(first_sample), Some(last_sample)) = (samples.first(), samples.last()) else {
            continue;
        };
        let chunk = encode_chunk(samples);
        put_labels(&mut series_index, labels);
        series_index.put_u64_le(block.len() as u64);
        series_index.put_u32_le(u32::try_from(chunk.len()).expect("the chunk of one series is far below 4 GiB"));
        series_index.put_u32_le(crc32c::crc32c(&chunk));
        series_index.put_u32_le(u32::try_from(samples.len()).expect("a series has far fewer than 2^32 samples"));
        series_index.put_i64_le(first_sample.timestamp_millis);
        series_index.put_i64_le(last_sample.timestamp_millis);
        block.put_slice(&chunk);

        for label in labels.labels() {
            series_by_label.entry(label).or_default().push(summary.series_count);
        }
        summary.series_count += 1;
        summary.sample_count += samples.len() as u64;
        summary.min_time_millis = summary.min_time_millis.min(first_sample.timestamp_millis);
        summary.max_time_millis = summary.max_time_millis.max(last_sample.timestamp_millis);
    }
    if summary.sample_count == 0 {
        summary.min_time_millis = 0;
        summary.max_time_millis = 0;
    }

    let postings = encode_postings(&series_by_label);
    let footer = BlockFooter {
        summary,
        series_index_offset: block.len() as u64,
        series_index_bytes: series_index.len() as u64,
        postings_bytes: postings.len() as u64,
        series_index_checksum: crc32c::crc32c(&series_index),
        postings_checksum: crc32c::crc32c(&postings),
    };
    block.put_slice(&series_index);
    block.put_slice(&postings);
    block.put_slice(&footer.encode());
    EncodedBlock {
        bytes: block.freeze(),
        summary,
    }
}

/// Encodes the postings of a block: for each label, the numbers of the series that have it.
fn encode_postings(series_by_label: &BTreeMap<&Label, Vec<u32>>) -> BytesMut {
    let mut postings = BytesMut::new();
    postings.put_u32_le(u32::try_from(series_by_label.len()).expect("far fewer than 2^32 label pairs"));
    for (label, series_numbers) in series_by_label {
        postings.put_u16_le(u16::try_from(label.name.len()).expect("a label text is at most 1024 bytes"));
        postings.put_slice(label.name.as_bytes());
        postings.put_u16_le(u16::try_from(label.value.len()).expect("a label text is at most 1024 bytes"));
        postings.put_slice(label.value.as_bytes());
        postings.put_u32_le(u32::try_from(series_numbers.len()).expect("far fewer than 2^32 series"));
        for series_number in series_numbers {
            postings.put_u32_le(*series_number);
        }
    }
    postings
}

/// What the footer says about a block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BlockFooter {
    /// What the block holds.
    pub summary: BlockSummary,
    /// Where the series index starts: the length of the chunks before it.
    pub series_index_offset: u64,
    /// The length of the series index.
    pub series_index_bytes: u64,
    /// The length of the postings, which follow the series index.
    pub postings_bytes: u64,
    /// The checksum of the series index.
    series_index_checksum: u32,
    /// The checksum of the postings.
    postings_checksum: u32,
}

impl BlockFooter {
    /// Serializes the footer, checksum included.
    fn encode(&self) -> [u8; BLOCK_FOOTER_BYTES] {
        let mut encoded = [0_u8; BLOCK_FOOTER_BYTES];
        let mut unwritten = &mut encoded[..];
        unwritten.put_slice(&BLOCK_MAGIC);
        unwritten.put_u32_le(BLOCK_FORMAT_VERSION);
        unwritten.put_u32_le(self.summary.series_count);
        unwritten.put_u64_le(self.summary.sample_count);
        unwritten.put_i64_le(self.summary.min_time_millis);
        unwritten.put_i64_le(self.summary.max_time_millis);
        unwritten.put_u64_le(self.series_index_offset);
        unwritten.put_u64_le(self.series_index_bytes);
        unwritten.put_u64_le(self.postings_bytes);
        unwritten.put_u32_le(self.series_index_checksum);
        unwritten.put_u32_le(self.postings_checksum);
        let checksum = crc32c::crc32c(&encoded[..FOOTER_CHECKSUM_OFFSET]);
        encoded[FOOTER_CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_le_bytes());
        encoded
    }

    /// Parses and verifies a footer from the last bytes of a block.
    ///
    /// # Errors
    ///
    /// Returns [`DamagedBlock`] if the bytes are too short, do not end like a block, come from another
    /// format version, or fail their checksum.
    pub(crate) fn decode(block_end: &[u8]) -> Result<Self, DamagedBlock> {
        let Some(footer_start) = block_end.len().checked_sub(BLOCK_FOOTER_BYTES) else {
            return Err(DamagedBlock::because("it is too short to hold a footer"));
        };
        let encoded = &block_end[footer_start..];
        let mut unread = encoded;
        let mut magic = [0_u8; BLOCK_MAGIC.len()];
        unread.copy_to_slice(&mut magic);
        if magic != BLOCK_MAGIC {
            return Err(DamagedBlock::because("it does not end with a block footer"));
        }
        let format_version = unread.get_u32_le();
        if format_version != BLOCK_FORMAT_VERSION {
            return Err(DamagedBlock {
                reason: format!("block format version {format_version} is not supported"),
            });
        }
        let footer = Self {
            summary: BlockSummary {
                series_count: unread.get_u32_le(),
                sample_count: unread.get_u64_le(),
                min_time_millis: unread.get_i64_le(),
                max_time_millis: unread.get_i64_le(),
            },
            series_index_offset: unread.get_u64_le(),
            series_index_bytes: unread.get_u64_le(),
            postings_bytes: unread.get_u64_le(),
            series_index_checksum: unread.get_u32_le(),
            postings_checksum: unread.get_u32_le(),
        };
        let stored_checksum = unread.get_u32_le();
        if stored_checksum != crc32c::crc32c(&encoded[..FOOTER_CHECKSUM_OFFSET]) {
            return Err(DamagedBlock::because("its footer fails its checksum"));
        }
        Ok(footer)
    }

    /// Where the series index and the postings lie in the block: one stretch of bytes.
    ///
    /// # Returns
    ///
    /// The range, or `None` if the footer's numbers do not add up to a place inside a block.
    pub(crate) fn index_range(&self) -> Option<Range<u64>> {
        let index_end = self
            .series_index_offset
            .checked_add(self.series_index_bytes)?
            .checked_add(self.postings_bytes)?;
        Some(self.series_index_offset..index_end)
    }

    /// The size of the whole block, by the footer's own numbers.
    pub(crate) fn block_bytes(&self) -> Option<u64> {
        self.index_range()?.end.checked_add(BLOCK_FOOTER_BYTES as u64)
    }
}

/// What the series index says about one series of a block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SeriesEntry {
    /// The series.
    pub labels: Labels,
    /// Where the series' chunk starts in the block.
    pub chunk_offset: u64,
    /// The length of the chunk.
    pub chunk_bytes: u32,
    /// The checksum of the chunk's bytes.
    pub chunk_checksum: u32,
    /// How many samples the chunk holds.
    pub sample_count: u32,
    /// The time of the series' first sample in the block.
    pub min_time_millis: i64,
    /// The time of the series' last sample in the block.
    pub max_time_millis: i64,
}

impl SeriesEntry {
    /// Where the chunk lies in the block.
    fn chunk_range(&self) -> Range<u64> {
        self.chunk_offset..self.chunk_offset + u64::from(self.chunk_bytes)
    }
}

/// The parts of a block a query reads before any chunk: the footer, the series index and the postings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BlockIndex {
    /// The block's footer.
    pub footer: BlockFooter,
    /// Every series of the block, in ascending order of labels. A series' number is its position.
    pub series: Vec<SeriesEntry>,
    /// For each label that occurs, the numbers of the series that have it, ascending.
    postings: BTreeMap<Label, Vec<u32>>,
}

impl BlockIndex {
    /// Parses and verifies the series index and the postings that `footer` describes.
    ///
    /// # Arguments
    ///
    /// * `footer` - The block's footer.
    /// * `index_bytes` - The bytes of [`BlockFooter::index_range`].
    ///
    /// # Errors
    ///
    /// Returns [`DamagedBlock`] if the bytes are not the size the footer states, fail a checksum, or do
    /// not describe chunks that lie inside the block.
    pub(crate) fn decode(footer: BlockFooter, index_bytes: &Bytes) -> Result<Self, DamagedBlock> {
        let stated_bytes = footer.series_index_bytes.checked_add(footer.postings_bytes);
        if stated_bytes != Some(index_bytes.len() as u64) {
            return Err(DamagedBlock::because("its index is not the size its footer states"));
        }
        let series_index_bytes = usize::try_from(footer.series_index_bytes).expect("checked against a length");
        let series_index = index_bytes.slice(..series_index_bytes);
        let postings = index_bytes.slice(series_index_bytes..);
        if crc32c::crc32c(&series_index) != footer.series_index_checksum {
            return Err(DamagedBlock::because("its series index fails its checksum"));
        }
        if crc32c::crc32c(&postings) != footer.postings_checksum {
            return Err(DamagedBlock::because("its postings fail their checksum"));
        }
        let series = decode_series_index(&footer, series_index)?;
        let postings = decode_postings(postings, series.len())?;
        Ok(Self {
            footer,
            series,
            postings,
        })
    }

    /// The numbers of the series that `selector` matches and that have a sample in `time_range`,
    /// ascending.
    ///
    /// Matchers that ask for one value of a label are answered from the postings; the series they leave
    /// are then checked against every matcher.
    pub(crate) fn select(&self, selector: &Selector, time_range: TimeRange) -> Vec<u32> {
        let mut candidates: Option<Vec<u32>> = None;
        for required_label in selector.required_labels() {
            let Some(series_with_label) = self.postings.get(&required_label) else {
                return Vec::new();
            };
            candidates = Some(match candidates {
                Some(narrowed) => intersect_ascending(&narrowed, series_with_label),
                None => series_with_label.clone(),
            });
        }
        let series_count = u32::try_from(self.series.len()).expect("a block has far fewer than 2^32 series");
        let candidates = match candidates {
            Some(candidates) => candidates,
            None => (0..series_count).collect(),
        };
        let mut selected = Vec::new();
        for series_number in candidates {
            let entry = &self.series[series_number as usize];
            if selector.matches(&entry.labels) && time_range.overlaps(entry.min_time_millis, entry.max_time_millis) {
                selected.push(series_number);
            }
        }
        selected
    }
}

/// The numbers that are in both of two ascending lists.
fn intersect_ascending(left: &[u32], right: &[u32]) -> Vec<u32> {
    let mut in_both = Vec::new();
    let mut left_position = 0;
    let mut right_position = 0;
    while left_position < left.len() && right_position < right.len() {
        match left[left_position].cmp(&right[right_position]) {
            std::cmp::Ordering::Less => left_position += 1,
            std::cmp::Ordering::Greater => right_position += 1,
            std::cmp::Ordering::Equal => {
                in_both.push(left[left_position]);
                left_position += 1;
                right_position += 1;
            }
        }
    }
    in_both
}

/// Reads the series index of a block and checks that it describes the block the footer does.
fn decode_series_index(footer: &BlockFooter, series_index: Bytes) -> Result<Vec<SeriesEntry>, DamagedBlock> {
    let cut_short = || DamagedBlock::because("its series index ends inside an entry");
    let mut unread = series_index;
    let mut series: Vec<SeriesEntry> = Vec::new();
    let mut next_chunk_offset = 0_u64;
    for _entry in 0..footer.summary.series_count {
        let labels = match take_labels(&mut unread) {
            Ok(labels) => labels,
            Err(LabelsReadError::Malformed(_reason)) => return Err(cut_short()),
            Err(LabelsReadError::Invalid(invalid_labels)) => {
                return Err(DamagedBlock {
                    reason: format!("a series has invalid labels: {invalid_labels}"),
                });
            }
        };
        // Offset, length, checksum, count and the two times.
        if unread.remaining() < 8 + 4 + 4 + 4 + 8 + 8 {
            return Err(cut_short());
        }
        let entry = SeriesEntry {
            labels,
            chunk_offset: unread.get_u64_le(),
            chunk_bytes: unread.get_u32_le(),
            chunk_checksum: unread.get_u32_le(),
            sample_count: unread.get_u32_le(),
            min_time_millis: unread.get_i64_le(),
            max_time_millis: unread.get_i64_le(),
        };
        if entry.chunk_offset != next_chunk_offset {
            return Err(DamagedBlock::because("its chunks are not back to back"));
        }
        if let Some(previous_entry) = series.last()
            && previous_entry.labels >= entry.labels
        {
            return Err(DamagedBlock::because("its series are not in order"));
        }
        next_chunk_offset = entry.chunk_range().end;
        series.push(entry);
    }
    if unread.has_remaining() {
        return Err(DamagedBlock::because("its series index holds more than its count says"));
    }
    if next_chunk_offset != footer.series_index_offset {
        return Err(DamagedBlock::because(
            "its chunks do not end where its series index starts",
        ));
    }
    Ok(series)
}

/// Reads the postings of a block.
fn decode_postings(postings: Bytes, series_count: usize) -> Result<BTreeMap<Label, Vec<u32>>, DamagedBlock> {
    let cut_short = || DamagedBlock::because("its postings end inside an entry");
    let mut unread = postings;
    let Ok(pair_count) = unread.try_get_u32_le() else {
        return Err(cut_short());
    };
    let mut series_by_label = BTreeMap::new();
    for _pair in 0..pair_count {
        let name = take_posting_text(&mut unread)?;
        let value = take_posting_text(&mut unread)?;
        let Ok(listed_count) = unread.try_get_u32_le() else {
            return Err(cut_short());
        };
        // Checked before allocating: a damaged count must not ask for gigabytes.
        if unread.remaining() / 4 < listed_count as usize {
            return Err(cut_short());
        }
        let mut series_numbers: Vec<u32> = Vec::with_capacity(listed_count as usize);
        for _listed in 0..listed_count {
            let series_number = unread.get_u32_le();
            if series_number as usize >= series_count {
                return Err(DamagedBlock::because("its postings name a series it does not hold"));
            }
            // Selecting series intersects these lists, which only works on ascending ones.
            if let Some(previous_number) = series_numbers.last()
                && *previous_number >= series_number
            {
                return Err(DamagedBlock::because("its postings are not in order"));
            }
            series_numbers.push(series_number);
        }
        series_by_label.insert(Label { name, value }, series_numbers);
    }
    if unread.has_remaining() {
        return Err(DamagedBlock::because("its postings hold more than their count says"));
    }
    Ok(series_by_label)
}

/// Reads one name or value of a posting.
fn take_posting_text(unread: &mut Bytes) -> Result<String, DamagedBlock> {
    let cut_short = || DamagedBlock::because("its postings end inside an entry");
    let Ok(text_bytes) = unread.try_get_u16_le() else {
        return Err(cut_short());
    };
    if unread.remaining() < usize::from(text_bytes) {
        return Err(cut_short());
    }
    match String::from_utf8(unread.split_to(usize::from(text_bytes)).to_vec()) {
        Ok(text) => Ok(text),
        Err(_not_utf8) => Err(DamagedBlock::because("a posting is not UTF-8")),
    }
}

/// Decodes every series of a block, verifying every checksum on the way.
///
/// # Arguments
///
/// * `block` - The whole block.
///
/// # Returns
///
/// Every series of the block with its samples.
///
/// # Errors
///
/// Returns [`DamagedBlock`] if any part of the block fails its checksum or does not decode.
pub(crate) fn decode_whole_block(block: &Bytes) -> Result<BTreeMap<Labels, Vec<Sample>>, DamagedBlock> {
    let footer = BlockFooter::decode(block)?;
    if footer.block_bytes() != Some(block.len() as u64) {
        return Err(DamagedBlock::because("it is not the size its footer states"));
    }
    let index_range = footer.index_range().expect("the block's size was just derived from it");
    let index_bytes = block.slice(byte_range_as_usize(&index_range));
    let index = BlockIndex::decode(footer, &index_bytes)?;
    let mut samples_by_series = BTreeMap::new();
    for entry in &index.series {
        let chunk = block.slice(byte_range_as_usize(&entry.chunk_range()));
        let samples = decode_verified_chunk(entry, &chunk, None)?;
        samples_by_series.insert(entry.labels.clone(), samples);
    }
    Ok(samples_by_series)
}

/// A range of block offsets as a range of positions in memory.
fn byte_range_as_usize(byte_range: &Range<u64>) -> Range<usize> {
    let start = usize::try_from(byte_range.start).expect("a block in memory fits in memory");
    let end = usize::try_from(byte_range.end).expect("a block in memory fits in memory");
    start..end
}

/// Verifies the checksum of one chunk and decodes it.
///
/// # Arguments
///
/// * `entry` - The series entry that describes the chunk.
/// * `chunk` - The chunk's bytes.
/// * `time_range` - If given, only samples in this range are returned.
///
/// # Errors
///
/// Returns [`DamagedBlock`] if the chunk fails its checksum, does not decode, or does not hold the
/// samples its entry describes.
fn decode_verified_chunk(
    entry: &SeriesEntry,
    chunk: &[u8],
    time_range: Option<TimeRange>,
) -> Result<Vec<Sample>, DamagedBlock> {
    if crc32c::crc32c(chunk) != entry.chunk_checksum {
        return Err(DamagedBlock::because("a chunk fails its checksum"));
    }
    let decoded = match time_range {
        Some(time_range) => {
            let mut samples_in_range = Vec::new();
            decode_chunk_in_range(chunk, time_range, &mut samples_in_range).map(|()| samples_in_range)
        }
        None => decode_chunk(chunk),
    };
    let samples = match decoded {
        Ok(samples) => samples,
        Err(damaged_chunk) => {
            return Err(DamagedBlock {
                reason: damaged_chunk.to_string(),
            });
        }
    };
    if time_range.is_none() && samples.len() != entry.sample_count as usize {
        return Err(DamagedBlock::because(
            "a chunk does not hold the samples its entry states",
        ));
    }
    Ok(samples)
}

/// Wraps a [`DamagedBlock`] as the error the framework knows, naming the artifact.
pub(crate) fn damaged_artifact(artifact_key: &str, damaged_block: DamagedBlock) -> MaterializerError {
    MaterializerError::DamagedArtifact {
        artifact_key: artifact_key.to_owned(),
        reason: damaged_block.reason,
    }
}

/// Fetches and verifies the footer, the series index and the postings of a block: two range reads.
///
/// # Arguments
///
/// * `artifacts` - Where the block is stored.
/// * `artifact_key` - The block's key.
/// * `size_bytes` - The block's size, as the catalog records it. It locates the footer.
///
/// # Errors
///
/// Returns [`MaterializerError::ArtifactStore`] if the block cannot be read, and
/// [`MaterializerError::DamagedArtifact`] if what was read is not the index of a block of that size.
pub(crate) async fn load_block_index(
    artifacts: &ArtifactNamespace,
    artifact_key: &str,
    size_bytes: u64,
) -> Result<BlockIndex, MaterializerError> {
    let damaged = |damaged_block| damaged_artifact(artifact_key, damaged_block);
    let Some(footer_start) = size_bytes.checked_sub(BLOCK_FOOTER_BYTES as u64) else {
        return Err(damaged(DamagedBlock::because("it is too short to hold a footer")));
    };
    let footer_bytes = artifacts
        .get_artifact_range(artifact_key, footer_start..size_bytes)
        .await?;
    let footer = BlockFooter::decode(&footer_bytes).map_err(damaged)?;
    if footer.block_bytes() != Some(size_bytes) {
        return Err(damaged(DamagedBlock::because("it is not the size its footer states")));
    }
    let index_range = footer.index_range().expect("the block's size was just derived from it");
    let index_bytes = artifacts.get_artifact_range(artifact_key, index_range).await?;
    BlockIndex::decode(footer, &index_bytes).map_err(damaged)
}

/// Fetches, verifies and decodes the samples of the selected series of a block that lie in a time range.
///
/// Chunks that lie next to each other, or nearly so, are fetched in one range read.
///
/// # Arguments
///
/// * `artifacts` - Where the block is stored.
/// * `artifact_key` - The block's key.
/// * `index` - The block's index, from [`load_block_index`].
/// * `selected_series` - The numbers of the series to read, ascending, from [`BlockIndex::select`].
/// * `time_range` - Only samples in this range are returned.
///
/// # Returns
///
/// The selected series that have a sample in the range, in ascending order of labels, each with its
/// samples in the range.
///
/// # Errors
///
/// Returns [`MaterializerError::ArtifactStore`] if a chunk cannot be read, and
/// [`MaterializerError::DamagedArtifact`] if one fails its checksum or does not decode.
pub(crate) async fn read_series(
    artifacts: &ArtifactNamespace,
    artifact_key: &str,
    index: &BlockIndex,
    selected_series: &[u32],
    time_range: TimeRange,
) -> Result<Vec<SeriesSamples>, MaterializerError> {
    let mut series_in_range = Vec::new();
    let mut run_start = 0;
    while run_start < selected_series.len() {
        let run_end = end_of_joined_read(index, selected_series, run_start);
        let first_entry = &index.series[selected_series[run_start] as usize];
        let last_entry = &index.series[selected_series[run_end - 1] as usize];
        let joined_range = first_entry.chunk_offset..last_entry.chunk_range().end;
        let joined_bytes = artifacts.get_artifact_range(artifact_key, joined_range.clone()).await?;
        if joined_bytes.len() as u64 != joined_range.end - joined_range.start {
            return Err(damaged_artifact(
                artifact_key,
                DamagedBlock::because("a read returned fewer bytes than were asked for"),
            ));
        }
        for series_number in &selected_series[run_start..run_end] {
            let entry = &index.series[*series_number as usize];
            let chunk_start = usize::try_from(entry.chunk_offset - joined_range.start).expect("within one read");
            let chunk = &joined_bytes[chunk_start..chunk_start + entry.chunk_bytes as usize];
            let samples = decode_verified_chunk(entry, chunk, Some(time_range))
                .map_err(|damaged_block| damaged_artifact(artifact_key, damaged_block))?;
            if !samples.is_empty() {
                series_in_range.push(SeriesSamples {
                    labels: entry.labels.clone(),
                    samples,
                });
            }
        }
        run_start = run_end;
    }
    Ok(series_in_range)
}

/// Where the run of selected series that starts at `run_start` ends, if their chunks are fetched in one
/// read: before the first series whose chunk lies further than [`JOINED_READ_GAP_BYTES`] from the last.
fn end_of_joined_read(index: &BlockIndex, selected_series: &[u32], run_start: usize) -> usize {
    let mut run_end = run_start + 1;
    while run_end < selected_series.len() {
        let previous_entry = &index.series[selected_series[run_end - 1] as usize];
        let next_entry = &index.series[selected_series[run_end] as usize];
        if next_entry.chunk_offset - previous_entry.chunk_range().end > JOINED_READ_GAP_BYTES {
            break;
        }
        run_end += 1;
    }
    run_end
}

#[cfg(test)]
#[path = "block_test.rs"]
mod tests;
