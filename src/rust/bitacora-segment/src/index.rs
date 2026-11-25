//! The sparse in-memory index of a segment file (design, section 7).
//!
//! It remembers where roughly every 64 KiB of the file starts in sequence
//! space, so a read begins near its target without scanning the whole file.
//! It is never stored: opening a segment rebuilds it from the scan.

use bitacora_types::SequenceNumber;

/// How much file the index leaves between two entries.
pub const INDEX_INTERVAL_BYTES: u64 = 64 * 1024;

/// Where one batch starts, in the file and in the stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexEntry {
    /// The sequence number of the batch's first record.
    pub first_sequence: SequenceNumber,
    /// The byte offset of the batch frame in the segment file.
    pub offset_bytes: u64,
}

/// A sparse map from sequence numbers to file offsets.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SparseIndex {
    /// Entries in file order, which is also sequence order.
    entries: Vec<IndexEntry>,
}

impl SparseIndex {
    /// Creates an empty index.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Tells the index about the next batch of the file. Batches must be
    /// reported in file order; the index keeps only those far enough from
    /// the previous entry.
    pub fn observe_batch(&mut self, first_sequence: SequenceNumber, offset_bytes: u64) {
        let is_far_enough = self.entries.last().is_none_or(|last_entry| {
            offset_bytes >= last_entry.offset_bytes + INDEX_INTERVAL_BYTES
        });
        if is_far_enough {
            self.entries.push(IndexEntry {
                first_sequence,
                offset_bytes,
            });
        }
    }

    /// Returns the last indexed batch that starts at or before `sequence`:
    /// the closest known place to start looking for it. Returns `None` if
    /// every indexed batch starts after `sequence`.
    #[must_use]
    pub fn entry_at_or_before(&self, sequence: SequenceNumber) -> Option<IndexEntry> {
        let entries_at_or_before = self
            .entries
            .partition_point(|entry| entry.first_sequence <= sequence);
        entries_at_or_before
            .checked_sub(1)
            .and_then(|position| self.entries.get(position))
            .copied()
    }

    /// Forgets every batch at or after `offset_bytes`, after the file was cut
    /// there.
    pub fn forget_from(&mut self, offset_bytes: u64) {
        self.entries
            .retain(|entry| entry.offset_bytes < offset_bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER_BYTES: u64 = 64;

    fn entry(first_sequence: u64, offset_bytes: u64) -> IndexEntry {
        IndexEntry {
            first_sequence: SequenceNumber::new(first_sequence),
            offset_bytes,
        }
    }

    /// Indexes batches of ten records and `batch_bytes` bytes each.
    fn index_of(batch_count: u64, batch_bytes: u64) -> SparseIndex {
        let mut index = SparseIndex::new();
        for batch_number in 0..batch_count {
            index.observe_batch(
                SequenceNumber::new(1 + batch_number * 10),
                HEADER_BYTES + batch_number * batch_bytes,
            );
        }
        index
    }

    #[test]
    fn empty_index_knows_no_starting_point() {
        assert_eq!(
            SparseIndex::new().entry_at_or_before(SequenceNumber::new(5)),
            None
        );
    }

    #[test]
    fn first_batch_is_always_indexed() {
        assert_eq!(
            index_of(1, 100).entry_at_or_before(SequenceNumber::new(1)),
            Some(entry(1, HEADER_BYTES))
        );
    }

    #[test]
    fn batches_closer_than_the_interval_are_skipped() {
        let small_batches = index_of(100, 100);
        assert_eq!(
            small_batches.entry_at_or_before(SequenceNumber::new(995)),
            Some(entry(1, HEADER_BYTES))
        );
    }

    #[test]
    fn batch_at_exactly_the_interval_is_indexed() {
        let interval_sized_batches = index_of(3, INDEX_INTERVAL_BYTES);
        assert_eq!(
            interval_sized_batches.entry_at_or_before(SequenceNumber::new(11)),
            Some(entry(11, HEADER_BYTES + INDEX_INTERVAL_BYTES))
        );
    }

    #[test]
    fn lookup_inside_a_batch_returns_that_batch() {
        let interval_sized_batches = index_of(3, INDEX_INTERVAL_BYTES);
        assert_eq!(
            interval_sized_batches.entry_at_or_before(SequenceNumber::new(15)),
            Some(entry(11, HEADER_BYTES + INDEX_INTERVAL_BYTES))
        );
    }

    #[test]
    fn lookup_past_the_end_returns_the_last_entry() {
        let interval_sized_batches = index_of(3, INDEX_INTERVAL_BYTES);
        assert_eq!(
            interval_sized_batches.entry_at_or_before(SequenceNumber::new(10_000)),
            Some(entry(21, HEADER_BYTES + 2 * INDEX_INTERVAL_BYTES))
        );
    }

    #[test]
    fn lookup_before_the_first_batch_finds_nothing() {
        let mut index = SparseIndex::new();
        index.observe_batch(SequenceNumber::new(100), HEADER_BYTES);
        assert_eq!(index.entry_at_or_before(SequenceNumber::new(99)), None);
    }

    #[test]
    fn forgetting_from_an_offset_drops_later_entries_only() {
        let mut index = index_of(3, INDEX_INTERVAL_BYTES);
        index.forget_from(HEADER_BYTES + INDEX_INTERVAL_BYTES);
        assert_eq!(
            index.entry_at_or_before(SequenceNumber::new(10_000)),
            Some(entry(1, HEADER_BYTES))
        );
    }

    #[test]
    fn index_accepts_new_batches_after_forgetting() {
        let mut index = index_of(3, INDEX_INTERVAL_BYTES);
        index.forget_from(HEADER_BYTES + INDEX_INTERVAL_BYTES);
        index.observe_batch(SequenceNumber::new(11), HEADER_BYTES + INDEX_INTERVAL_BYTES);
        assert_eq!(
            index.entry_at_or_before(SequenceNumber::new(11)),
            Some(entry(11, HEADER_BYTES + INDEX_INTERVAL_BYTES))
        );
    }
}
