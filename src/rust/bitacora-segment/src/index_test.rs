//! Tests of the sparse index.
//!
//! # What is covered
//!
//! - Which batches get an entry: the first always, later ones only at the interval.
//! - Lookups: inside a batch, past the end, before the first batch, in an empty index.
//! - Forgetting entries after a cut, and indexing again afterwards.
//!
//! # How the fixtures work
//!
//! `index_of(batch_count, batch_bytes)` indexes batches of ten records each (so batch `n` starts at
//! sequence number `1 + 10 n`) that are `batch_bytes` long and follow a header of [`HEADER_BYTES`].

use super::*;

/// The size of the header the fixture's batches follow. Any value would do: the index only compares
/// distances.
const HEADER_BYTES: u64 = 64;

/// How many records each of the fixture's batches holds.
const RECORDS_PER_BATCH: u64 = 10;

/// The entry of a batch that starts with `first_sequence` at `offset_bytes`.
fn entry(first_sequence: u64, offset_bytes: u64) -> IndexEntry {
    IndexEntry {
        first_sequence: SequenceNumber::new(first_sequence),
        offset_bytes,
    }
}

/// An index that was told about `batch_count` batches of [`RECORDS_PER_BATCH`] records and `batch_bytes`
/// bytes each.
fn index_of(batch_count: u64, batch_bytes: u64) -> SparseIndex {
    let mut index = SparseIndex::new();
    for batch_number in 0..batch_count {
        index.observe_batch(
            SequenceNumber::new(1 + batch_number * RECORDS_PER_BATCH),
            HEADER_BYTES + batch_number * batch_bytes,
        );
    }
    index
}

#[test]
fn empty_index_knows_no_starting_point() {
    let found = SparseIndex::new().entry_at_or_before(SequenceNumber::new(5));

    assert_eq!(found, None);
}

#[test]
fn first_batch_is_always_indexed() {
    let found = index_of(1, 100).entry_at_or_before(SequenceNumber::new(1));

    assert_eq!(found, Some(entry(1, HEADER_BYTES)));
}

#[test]
fn batches_closer_than_the_interval_are_skipped() {
    let small_batches = index_of(100, 100);

    let found = small_batches.entry_at_or_before(SequenceNumber::new(995));

    assert_eq!(found, Some(entry(1, HEADER_BYTES)));
}

#[test]
fn index_of_small_batches_has_one_entry() {
    let small_batches = index_of(100, 100);

    assert_eq!(small_batches.entries(), [entry(1, HEADER_BYTES)]);
}

#[test]
fn batch_at_exactly_the_interval_is_indexed() {
    let interval_sized_batches = index_of(3, INDEX_INTERVAL_BYTES);

    let found = interval_sized_batches.entry_at_or_before(SequenceNumber::new(11));

    assert_eq!(found, Some(entry(11, HEADER_BYTES + INDEX_INTERVAL_BYTES)));
}

#[test]
fn batch_one_byte_short_of_the_interval_is_skipped() {
    let slightly_smaller_batches = index_of(2, INDEX_INTERVAL_BYTES - 1);

    assert_eq!(slightly_smaller_batches.entries(), [entry(1, HEADER_BYTES)]);
}

#[test]
fn lookup_inside_a_batch_returns_that_batch() {
    let interval_sized_batches = index_of(3, INDEX_INTERVAL_BYTES);

    let found = interval_sized_batches.entry_at_or_before(SequenceNumber::new(15));

    assert_eq!(found, Some(entry(11, HEADER_BYTES + INDEX_INTERVAL_BYTES)));
}

#[test]
fn lookup_past_the_end_returns_the_last_entry() {
    let interval_sized_batches = index_of(3, INDEX_INTERVAL_BYTES);

    let found = interval_sized_batches.entry_at_or_before(SequenceNumber::new(10_000));

    assert_eq!(found, Some(entry(21, HEADER_BYTES + 2 * INDEX_INTERVAL_BYTES)));
}

#[test]
fn lookup_before_the_first_batch_finds_nothing() {
    let mut index = SparseIndex::new();
    index.observe_batch(SequenceNumber::new(100), HEADER_BYTES);

    let found = index.entry_at_or_before(SequenceNumber::new(99));

    assert_eq!(found, None);
}

#[test]
fn entries_are_listed_in_file_order() {
    let interval_sized_batches = index_of(3, INDEX_INTERVAL_BYTES);

    assert_eq!(
        interval_sized_batches.entries(),
        [
            entry(1, HEADER_BYTES),
            entry(11, HEADER_BYTES + INDEX_INTERVAL_BYTES),
            entry(21, HEADER_BYTES + 2 * INDEX_INTERVAL_BYTES),
        ]
    );
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
