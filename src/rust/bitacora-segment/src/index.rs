//! The sparse in-memory index of a segment file.
//!
//! # Why this file exists
//!
//! A segment file is a long run of batch frames of varying size, up to a gigabyte of them. A reader asks
//! for "records from sequence number N", and without help the only way to find N is to walk the frames
//! from the start of the file. This file is the help (design, section 7): a small table that says where,
//! roughly every 64 KiB, a batch starts and which sequence number it starts with. A read looks up the
//! closest earlier entry and walks from there, so it scans at most about 64 KiB instead of the file.
//!
//! # What it contains
//!
//! - [`SparseIndex`]: the table, with the rule for which batches get an entry.
//! - [`IndexEntry`]: one entry, a sequence number and a file offset.
//! - [`INDEX_INTERVAL_BYTES`]: how far apart entries are, at least.
//!
//! # How it works
//!
//! Whoever walks a file (the scan when it is opened, or an append) tells the index about every batch in
//! file order. The index keeps a batch only if it starts at least [`INDEX_INTERVAL_BYTES`] after the last
//! one it kept, so the first batch always has an entry and the table stays small: about 16 thousand
//! entries for a gigabyte.
//!
//! The index is never stored. Opening a segment rebuilds it from the scan that has to happen anyway to
//! verify the file. The same rule, with offsets counted from the first frame, decides where the blocks of
//! a tiered object start; there the entries are written into the object.
//!
//! # Invariants
//!
//! - Entries are in file order, which is also sequence order: both fields increase from one to the next.
//! - Every entry is the start of a batch frame. A reader that starts at an entry's offset finds a frame
//!   there.
//! - Two neighbouring entries are at least [`INDEX_INTERVAL_BYTES`] apart.
//!
//! # See also
//!
//! - `file.rs`: the segment file that keeps one of these and reads through it.
//! - `tiered.rs`: the tiered object, whose block index is built by the same rule.

use bitacora_types::SequenceNumber;

/// How much file the index leaves between two entries, at least: 64 KiB. Smaller would make the table
/// larger without making reads noticeably faster; larger would make every read scan more.
pub const INDEX_INTERVAL_BYTES: u64 = 64 * 1024;

/// Where one batch starts, in the file and in the stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexEntry {
    /// The sequence number of the batch's first record.
    pub first_sequence: SequenceNumber,
    /// The byte offset of the batch frame, counted from wherever the index's owner counts: the start of
    /// the segment file, or the first frame of a tiered object.
    pub offset_bytes: u64,
}

/// A sparse map from sequence numbers to file offsets.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SparseIndex {
    /// Entries in file order, which is also sequence order.
    entries: Vec<IndexEntry>,
}

impl SparseIndex {
    /// Creates an index that knows no batch.
    ///
    /// # Returns
    ///
    /// An empty index. It allocates nothing until the first batch is observed.
    #[must_use]
    pub const fn new() -> Self {
        Self { entries: Vec::new() }
    }

    /// Tells the index about the next batch of the file.
    ///
    /// Batches must be reported in file order. The index keeps only those that start far enough from the
    /// previous entry; the call is cheap for the others.
    ///
    /// # Arguments
    ///
    /// * `first_sequence` - The sequence number of the batch's first record.
    /// * `offset_bytes` - Where the batch frame starts.
    pub fn observe_batch(&mut self, first_sequence: SequenceNumber, offset_bytes: u64) {
        let is_far_enough = match self.entries.last() {
            Some(last_entry) => offset_bytes >= last_entry.offset_bytes + INDEX_INTERVAL_BYTES,
            None => true,
        };
        if is_far_enough {
            self.entries.push(IndexEntry {
                first_sequence,
                offset_bytes,
            });
        }
    }

    /// Finds the closest known place to start looking for a sequence number.
    ///
    /// A binary search over the entries; it allocates nothing.
    ///
    /// # Arguments
    ///
    /// * `sequence` - The sequence number a reader wants.
    ///
    /// # Returns
    ///
    /// The last indexed batch that starts at or before `sequence`. `None` if every indexed batch starts
    /// after it, or the index is empty.
    #[must_use]
    pub fn entry_at_or_before(&self, sequence: SequenceNumber) -> Option<IndexEntry> {
        let entries_at_or_before = self.entries.partition_point(|entry| entry.first_sequence <= sequence);
        let position = entries_at_or_before.checked_sub(1)?;
        self.entries.get(position).copied()
    }

    /// Returns the indexed batches.
    ///
    /// # Returns
    ///
    /// Every entry, in file order.
    #[must_use]
    pub fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }

    /// Forgets every batch at or after an offset, after the file was cut there.
    ///
    /// # Arguments
    ///
    /// * `offset_bytes` - Where the file now ends. Entries that start before it are kept.
    pub fn forget_from(&mut self, offset_bytes: u64) {
        self.entries.retain(|entry| entry.offset_bytes < offset_bytes);
    }
}

#[cfg(test)]
#[path = "index_test.rs"]
mod tests;
