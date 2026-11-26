//! Tests of the tiered segment object format.
//!
//! # What is covered
//!
//! - Encoding: the object starts with the frames unchanged, and a copy that is not exactly the sealed
//!   segment (short, long, misplaced, empty or damaged) is refused.
//! - The footer: what it says about the object, and every way bytes are refused as one.
//! - The block index: every sequence number is found in the blocks fetched for it, and fetch limits are
//!   rounded to whole blocks.
//! - Damage: every flipped bit in the index or the footer is detected.
//!
//! # How the fixtures work
//!
//! `large_frames` is a segment of 60 batches of about 10 KiB each, so that it spans several blocks of
//! 64 KiB; `large_object` is its tiered object. `open` reads an object the way a reader does: the footer
//! first, then the index it points at.

use bitacora_types::DeterministicRandom;

use super::*;

/// The segment every fixture belongs to.
const SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));

/// The sequence number of the segment's first record.
const FIRST_SEQUENCE: u64 = 100;

/// How many batches the large fixture holds.
const LARGE_BATCH_COUNT: u64 = 60;

/// How many records each batch of the large fixture holds.
const LARGE_RECORDS_PER_BATCH: u64 = 5;

/// The size of each record of the large fixture, which makes a batch about 10 KiB.
const LARGE_PAYLOAD_BYTES: usize = 2_000;

/// The sealed end of the large fixture: its last record.
const LARGE_SEALED_END: SequenceNumber =
    SequenceNumber::new(FIRST_SEQUENCE + LARGE_BATCH_COUNT * LARGE_RECORDS_PER_BATCH - 1);

/// A fetch limit that ends in the middle of a block.
const THREE_AND_A_HALF_BLOCKS: u64 = 3 * TIERED_BLOCK_BYTES + TIERED_BLOCK_BYTES / 2;

/// Where the sealed end starts in a footer: after the magic, the version and the four fields of the
/// segment's identity.
const SEALED_END_OFFSET_IN_FOOTER: usize = 44;

/// The header of [`SEGMENT`], written at epoch 5 and starting at [`FIRST_SEQUENCE`].
fn header() -> SegmentHeader {
    SegmentHeader {
        address: SEGMENT,
        epoch: Epoch::new(5),
        first_sequence: SequenceNumber::new(FIRST_SEQUENCE),
    }
}

/// The frames of `batch_count` consecutive batches of the segment, each of `records_per_batch` records of
/// `payload_bytes` bytes.
fn frames_of(batch_count: u64, records_per_batch: u64, payload_bytes: usize) -> Bytes {
    let mut frames = BytesMut::new();
    for batch_number in 0..batch_count {
        let payloads: Vec<Bytes> = (0..records_per_batch)
            .map(|_record_number| Bytes::from(vec![b'x'; payload_bytes]))
            .collect();
        let batch = Batch::encode(
            SEGMENT,
            SequenceNumber::new(FIRST_SEQUENCE + batch_number * records_per_batch),
            &payloads,
        )
        .expect("the fixture's payloads are within every limit");
        frames.put_slice(batch.frame());
    }
    frames.freeze()
}

/// The frames of the large fixture: several blocks, sealed at [`LARGE_SEALED_END`].
fn large_frames() -> Bytes {
    frames_of(LARGE_BATCH_COUNT, LARGE_RECORDS_PER_BATCH, LARGE_PAYLOAD_BYTES)
}

/// The tiered object of the large fixture.
fn large_object() -> Bytes {
    encode_tiered_segment(header(), LARGE_SEALED_END, &large_frames()).expect("the frames are the sealed segment")
}

/// The part of `object` that `byte_range` names, as a reader's range request would return it.
fn bytes_of(object: &Bytes, byte_range: &Range<u64>) -> Bytes {
    let start = usize::try_from(byte_range.start).expect("test objects are small");
    let end = usize::try_from(byte_range.end).expect("test objects are small");
    object.slice(start..end)
}

/// What a reader holds once it has opened a tiered object.
#[derive(Clone, Debug, PartialEq, Eq)]
struct OpenedObject {
    /// The object's footer.
    footer: TieredFooter,
    /// The block index the footer points at.
    index: TieredIndex,
}

/// Reads an object the way a reader does: the footer first, then the index it points at.
fn open(object: &Bytes) -> Result<OpenedObject, TieredError> {
    let footer = TieredFooter::decode(object)?;
    let index_range = footer.index_range();
    if index_range.end > to_u64(object.len()) {
        return Err(TieredError::DamagedIndex {
            reason: "it lies outside the object",
        });
    }
    let index = TieredIndex::decode(&footer, &bytes_of(object, &index_range))?;
    Ok(OpenedObject { footer, index })
}

/// Opens the large fixture's object.
fn open_large_object() -> OpenedObject {
    open(&large_object()).expect("an object that was just encoded is valid")
}

/// The object of the segment sealed empty: no frames, no blocks.
fn empty_object() -> Bytes {
    let sealed_end = SequenceNumber::new(FIRST_SEQUENCE - 1);
    encode_tiered_segment(header(), sealed_end, &Bytes::new()).expect("no frames are a segment sealed empty")
}

/// The sequence numbers of the records in the blocks that `byte_range` names.
fn sequences_in(object: &Bytes, byte_range: &Range<u64>) -> Vec<u64> {
    let batches = decode_frames(SEGMENT, &bytes_of(object, byte_range)).expect("a block range holds whole frames");
    let mut sequences = Vec::new();
    for batch in &batches {
        for record in batch.records() {
            sequences.push(record.sequence.get());
        }
    }
    sequences
}

/// The byte range fetched from the large fixture for a limit of [`THREE_AND_A_HALF_BLOCKS`].
fn range_for_three_and_a_half_blocks() -> Range<u64> {
    open_large_object()
        .index
        .blocks_from(SequenceNumber::new(FIRST_SEQUENCE), THREE_AND_A_HALF_BLOCKS)
}

/// The serialized block index that holds `blocks`, in the order given.
fn encoded_blocks(blocks: &[IndexEntry]) -> BytesMut {
    let mut index_bytes = BytesMut::new();
    for block in blocks {
        index_bytes.put_u64_le(block.first_sequence.get());
        index_bytes.put_u64_le(block.offset_bytes);
    }
    index_bytes
}

/// A footer with a correct checksum over `index_bytes`, for an object of [`SEGMENT`] with 1000 bytes of
/// frames: what only a faulty writer could produce around an impossible index.
fn footer_over(index_bytes: &[u8]) -> TieredFooter {
    TieredFooter {
        segment: header(),
        sealed_end: LARGE_SEALED_END,
        frames_bytes: 1_000,
        index_entry_count: u32::try_from(index_bytes.len() / INDEX_ENTRY_BYTES).expect("a handful of entries"),
        index_checksum: crc32c::crc32c(index_bytes),
    }
}

/// The block that starts with record `first_sequence` at `offset_bytes`.
fn block(first_sequence: u64, offset_bytes: u64) -> IndexEntry {
    IndexEntry {
        first_sequence: SequenceNumber::new(first_sequence),
        offset_bytes,
    }
}

// The footer.

#[test]
fn footer_names_the_segment() {
    assert_eq!(open_large_object().footer.segment, header());
}

#[test]
fn footer_records_the_sealed_end() {
    assert_eq!(open_large_object().footer.sealed_end, LARGE_SEALED_END);
}

#[test]
fn footer_counts_the_bytes_of_frames() {
    assert_eq!(open_large_object().footer.frames_bytes, to_u64(large_frames().len()));
}

#[test]
fn footer_knows_the_size_of_the_whole_object() {
    let object = large_object();

    let opened = open(&object).expect("valid object");

    assert_eq!(opened.footer.object_bytes(), to_u64(object.len()));
}

#[test]
fn bytes_shorter_than_a_footer_are_refused() {
    let decoded = TieredFooter::decode(&[0; TIERED_FOOTER_BYTES - 1]);

    assert_eq!(
        decoded,
        Err(TieredError::IncompleteFooter {
            available_bytes: TIERED_FOOTER_BYTES - 1
        })
    );
}

#[test]
fn object_that_is_not_a_tiered_segment_is_refused() {
    let decoded = TieredFooter::decode(&[b'x'; 200]);

    assert_eq!(decoded, Err(TieredError::BadMagic));
}

#[test]
fn footer_of_a_newer_format_is_refused() {
    let mut object = large_object().to_vec();
    let version_offset = object.len() - TIERED_FOOTER_BYTES + TIERED_MAGIC.len();
    object[version_offset] = 2;

    let decoded = TieredFooter::decode(&object);

    assert_eq!(decoded, Err(TieredError::UnsupportedVersion { found: 2 }));
}

#[test]
fn footer_with_a_flipped_bit_fails_its_checksum() {
    let mut object = large_object().to_vec();
    let sealed_end_offset = object.len() - TIERED_FOOTER_BYTES + SEALED_END_OFFSET_IN_FOOTER;
    object[sealed_end_offset] ^= 0x01;

    let decoded = TieredFooter::decode(&object);

    assert!(matches!(decoded, Err(TieredError::FooterChecksumMismatch { .. })));
}

// Encoding.

#[test]
fn object_starts_with_the_frames_unchanged() {
    let frames = large_frames();

    let object = large_object();

    assert_eq!(object.slice(..frames.len()), frames);
}

/// Guards the fixture: the tests of block ranges mean something only if there are several blocks.
#[test]
fn large_fixture_is_at_least_five_blocks_long() {
    let whole_blocks = to_u64(large_frames().len()) / TIERED_BLOCK_BYTES;

    assert!(whole_blocks >= 5, "{whole_blocks} blocks");
}

#[test]
fn segment_larger_than_a_block_gets_several_blocks() {
    let whole_blocks = to_u64(large_frames().len()) / TIERED_BLOCK_BYTES;

    let footer = open_large_object().footer;

    assert!(
        u64::from(footer.index_entry_count) >= whole_blocks,
        "{} blocks",
        footer.index_entry_count
    );
}

#[test]
fn segment_sealed_empty_is_a_footer_alone() {
    assert_eq!(empty_object().len(), TIERED_FOOTER_BYTES);
}

#[test]
fn object_of_a_segment_sealed_empty_records_its_sealed_end() {
    let opened = open(&empty_object()).expect("valid object");

    assert_eq!(opened.footer.sealed_end, SequenceNumber::new(FIRST_SEQUENCE - 1));
}

#[test]
fn object_of_a_segment_sealed_empty_has_nothing_to_fetch() {
    let opened = open(&empty_object()).expect("valid object");

    let byte_range = opened.index.blocks_from(SequenceNumber::new(FIRST_SEQUENCE), 1);

    assert_eq!(byte_range, 0..0);
}

#[test]
fn copy_that_stops_short_of_the_sealed_end_is_not_uploaded() {
    let short_frames = frames_of(LARGE_BATCH_COUNT - 1, LARGE_RECORDS_PER_BATCH, LARGE_PAYLOAD_BYTES);

    let encoded = encode_tiered_segment(header(), LARGE_SEALED_END, &short_frames);

    assert!(matches!(
        encoded,
        Err(TieredError::WrongExtent {
            found_end: Some(found_end),
            ..
        }) if found_end < LARGE_SEALED_END
    ));
}

#[test]
fn copy_that_runs_past_the_sealed_end_is_not_uploaded() {
    let long_frames = frames_of(LARGE_BATCH_COUNT + 1, LARGE_RECORDS_PER_BATCH, LARGE_PAYLOAD_BYTES);

    let encoded = encode_tiered_segment(header(), LARGE_SEALED_END, &long_frames);

    assert!(matches!(encoded, Err(TieredError::WrongExtent { .. })));
}

#[test]
fn copy_that_does_not_start_at_the_segment_start_is_not_uploaded() {
    let mut earlier_segment = header();
    earlier_segment.first_sequence = SequenceNumber::new(FIRST_SEQUENCE - 5);

    let encoded = encode_tiered_segment(earlier_segment, LARGE_SEALED_END, &large_frames());

    assert!(matches!(encoded, Err(TieredError::WrongExtent { .. })));
}

#[test]
fn empty_copy_of_a_segment_with_records_is_not_uploaded() {
    let encoded = encode_tiered_segment(header(), LARGE_SEALED_END, &Bytes::new());

    assert!(matches!(
        encoded,
        Err(TieredError::WrongExtent { found_first: None, .. })
    ));
}

#[test]
fn copy_with_a_damaged_frame_is_not_uploaded() {
    let mut damaged_frames = large_frames().to_vec();
    damaged_frames[30_000] ^= 0x01;

    let encoded = encode_tiered_segment(header(), LARGE_SEALED_END, &Bytes::from(damaged_frames));

    assert!(matches!(encoded, Err(TieredError::InvalidFrames(_))));
}

// The block index.

#[test]
fn every_sequence_is_found_in_the_blocks_fetched_for_it() {
    let object = large_object();
    let opened = open(&object).expect("valid object");

    for sequence in FIRST_SEQUENCE..=LARGE_SEALED_END.get() {
        let byte_range = opened.index.blocks_from(SequenceNumber::new(sequence), 1);

        assert!(
            sequences_in(&object, &byte_range).contains(&sequence),
            "sequence {sequence} is not in bytes {byte_range:?}"
        );
    }
}

#[test]
fn fetch_limit_smaller_than_a_block_still_fetches_one_whole_block() {
    let opened = open_large_object();

    let byte_range = opened.index.blocks_from(SequenceNumber::new(FIRST_SEQUENCE), 1);

    assert!(
        (TIERED_BLOCK_BYTES..2 * TIERED_BLOCK_BYTES).contains(&(byte_range.end - byte_range.start)),
        "{byte_range:?}"
    );
}

#[test]
fn fetch_limit_of_several_blocks_is_not_exceeded() {
    let byte_range = range_for_three_and_a_half_blocks();

    assert!(
        byte_range.end - byte_range.start <= THREE_AND_A_HALF_BLOCKS,
        "{byte_range:?}"
    );
}

/// A block is less than two intervals long, so a range that stops more than two intervals short of the
/// limit left out a block that would have fit.
#[test]
fn fetch_limit_of_several_blocks_is_used_up_to_the_last_whole_block() {
    let byte_range = range_for_three_and_a_half_blocks();

    assert!(
        byte_range.end - byte_range.start > THREE_AND_A_HALF_BLOCKS - 2 * TIERED_BLOCK_BYTES,
        "{byte_range:?}"
    );
}

#[test]
fn blocks_fetched_for_a_limit_of_several_blocks_decode_on_their_own() {
    let byte_range = range_for_three_and_a_half_blocks();

    let fetched_sequences = sequences_in(&large_object(), &byte_range);

    assert_eq!(fetched_sequences.first(), Some(&FIRST_SEQUENCE));
}

#[test]
fn fetch_limit_larger_than_the_segment_fetches_all_frames() {
    let opened = open_large_object();

    let byte_range = opened.index.blocks_from(SequenceNumber::new(FIRST_SEQUENCE), u64::MAX);

    assert_eq!(byte_range, 0..opened.footer.frames_bytes);
}

#[test]
fn last_block_ends_where_the_frames_end() {
    let opened = open_large_object();

    let byte_range = opened.index.blocks_from(LARGE_SEALED_END, 1);

    assert_eq!(byte_range.end, opened.footer.frames_bytes);
}

#[test]
fn sequence_below_the_segment_is_read_from_its_start() {
    let opened = open_large_object();

    let byte_range = opened.index.blocks_from(SequenceNumber::new(1), 1);

    assert_eq!(byte_range.start, 0);
}

#[test]
fn sequence_above_the_segment_gets_the_last_block() {
    let opened = open_large_object();

    let byte_range = opened.index.blocks_from(SequenceNumber::new(u64::MAX), 1);

    assert_eq!(byte_range, opened.index.blocks_from(LARGE_SEALED_END, 1));
}

// Damage.

#[test]
fn index_of_another_size_than_the_footer_says_is_refused() {
    let object = large_object();
    let footer = TieredFooter::decode(&object).expect("valid footer");
    let index_bytes = bytes_of(&object, &footer.index_range());
    let short_index = &index_bytes[..index_bytes.len() - 1];

    let decoded = TieredIndex::decode(&footer, short_index);

    assert_eq!(
        decoded,
        Err(TieredError::DamagedIndex {
            reason: "its size is not what the footer says"
        })
    );
}

#[test]
fn index_with_a_flipped_bit_fails_its_checksum() {
    let object = large_object();
    let footer = TieredFooter::decode(&object).expect("valid footer");
    let mut index_bytes = bytes_of(&object, &footer.index_range()).to_vec();
    index_bytes[0] ^= 0x01;

    let decoded = TieredIndex::decode(&footer, &index_bytes);

    assert_eq!(
        decoded,
        Err(TieredError::DamagedIndex {
            reason: "it fails its checksum"
        })
    );
}

#[test]
fn every_flipped_bit_in_the_index_or_footer_is_detected() {
    let object = large_object();
    let intact = open(&object).expect("valid object");
    let frames_bytes = large_frames().len();
    let mut random = DeterministicRandom::from_seed(1);

    for damaged_offset in frames_bytes..object.len() {
        let mut damaged = object.to_vec();
        damaged[damaged_offset] ^= 1 << random.index(8);

        let opened = open(&Bytes::from(damaged));

        assert_ne!(
            opened,
            Ok(intact.clone()),
            "damage at byte {damaged_offset} went unnoticed"
        );
    }
}

/// A footer that claims frames but an index without blocks: internally consistent checksums, impossible
/// content.
#[test]
fn index_without_blocks_for_an_object_with_frames_is_refused() {
    let footer = footer_over(&[]);

    let decoded = TieredIndex::decode(&footer, &[]);

    assert_eq!(
        decoded,
        Err(TieredError::DamagedIndex {
            reason: "its entries do not describe the frames"
        })
    );
}

/// An index whose checksum holds but whose first block is not the start of the frames: a reader that
/// trusted it would start reading in the middle of a frame.
#[test]
fn index_that_does_not_start_at_the_first_frame_is_refused() {
    let index_bytes = encoded_blocks(&[block(FIRST_SEQUENCE, 8)]);
    let footer = footer_over(&index_bytes);

    let decoded = TieredIndex::decode(&footer, &index_bytes);

    assert_eq!(
        decoded,
        Err(TieredError::DamagedIndex {
            reason: "its entries do not describe the frames"
        })
    );
}

/// An index whose checksum holds but whose last block starts where the frames have already ended.
#[test]
fn index_whose_last_block_starts_past_the_frames_is_refused() {
    let index_bytes = encoded_blocks(&[block(FIRST_SEQUENCE, 0), block(FIRST_SEQUENCE + 10, 1_000)]);
    let footer = footer_over(&index_bytes);

    let decoded = TieredIndex::decode(&footer, &index_bytes);

    assert_eq!(
        decoded,
        Err(TieredError::DamagedIndex {
            reason: "its entries do not describe the frames"
        })
    );
}

/// An index whose checksum holds but whose blocks go backwards.
#[test]
fn index_whose_blocks_are_out_of_order_is_refused() {
    let index_bytes = encoded_blocks(&[
        block(FIRST_SEQUENCE, 0),
        block(FIRST_SEQUENCE + 20, 500),
        block(FIRST_SEQUENCE + 10, 700),
    ]);
    let footer = footer_over(&index_bytes);

    let decoded = TieredIndex::decode(&footer, &index_bytes);

    assert_eq!(
        decoded,
        Err(TieredError::DamagedIndex {
            reason: "its entries do not describe the frames"
        })
    );
}
