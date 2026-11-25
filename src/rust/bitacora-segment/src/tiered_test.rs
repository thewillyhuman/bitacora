//! Tests of the tiered segment object format.
//!
//! # What is covered
//!
//! - Encoding: the object starts with the frames unchanged, and a copy that is not exactly the sealed
//!   segment (short, long, misplaced, empty or damaged) is refused.
//! - The block index: every sequence number is found in the blocks fetched for it, and fetch limits are
//!   rounded to whole blocks.
//! - Damage: every flipped bit in the index or the footer is detected.
//!
//! # How the fixtures work
//!
//! `large_frames` is a segment of 60 batches of about 10 KiB each, so that it spans several blocks of
//! 64 KiB. `open` reads an object the way a reader does: the footer first, then the index it points at.

use bitacora_types::DeterministicRandom;

use super::*;
use crate::format::Batch;

const SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
const FIRST_SEQUENCE: u64 = 100;

fn header() -> SegmentHeader {
    SegmentHeader {
        address: SEGMENT,
        epoch: Epoch::new(5),
        first_sequence: SequenceNumber::new(FIRST_SEQUENCE),
    }
}

/// Consecutive batches of the segment, each of `records_per_batch`
/// records of `payload_bytes` bytes, and the frames they make.
fn frames_of(batch_count: u64, records_per_batch: u64, payload_bytes: usize) -> Bytes {
    let mut frames = BytesMut::new();
    for batch_number in 0..batch_count {
        let payloads: Vec<Bytes> = (0..records_per_batch)
            .map(|_| Bytes::from(vec![b'x'; payload_bytes]))
            .collect();
        let batch = Batch::encode(
            SEGMENT,
            SequenceNumber::new(FIRST_SEQUENCE + batch_number * records_per_batch),
            &payloads,
        )
        .expect("valid batch");
        frames.put_slice(batch.frame());
    }
    frames.freeze()
}

/// A segment of 60 batches of 5 records and about 10 KiB each: several
/// blocks, sealed at sequence 399.
fn large_frames() -> Bytes {
    frames_of(60, 5, 2_000)
}

const LARGE_SEALED_END: SequenceNumber = SequenceNumber::new(FIRST_SEQUENCE + 60 * 5 - 1);

fn large_object() -> Bytes {
    encode_tiered_segment(header(), LARGE_SEALED_END, &large_frames()).expect("valid segment")
}

/// The part of `object` that `byte_range` names, as a reader's range request would return it.
fn bytes_of(object: &Bytes, byte_range: &Range<u64>) -> Bytes {
    let start = usize::try_from(byte_range.start).expect("test objects are small");
    let end = usize::try_from(byte_range.end).expect("test objects are small");
    object.slice(start..end)
}

/// Reads an object the way a reader does: the footer first, then the index it points at.
fn open(object: &Bytes) -> Result<(TieredFooter, TieredIndex), TieredError> {
    let footer = TieredFooter::decode(object)?;
    let index_range = footer.index_range();
    if index_range.end > object.len() as u64 {
        return Err(TieredError::DamagedIndex {
            reason: "it lies outside the object",
        });
    }
    let index = TieredIndex::decode(&footer, &bytes_of(object, &index_range))?;
    Ok((footer, index))
}

/// The sequence numbers of the records in the blocks that `byte_range` names.
fn sequences_in(object: &Bytes, byte_range: &Range<u64>) -> Vec<u64> {
    let batches = decode_frames(SEGMENT, &bytes_of(object, byte_range))
        .expect("a block range holds whole frames");
    let mut sequences = Vec::new();
    for batch in &batches {
        for record in batch.records() {
            sequences.push(record.sequence.get());
        }
    }
    sequences
}

#[test]
fn footer_describes_the_segment() {
    let object = large_object();
    let (footer, _index) = open(&object).expect("valid object");
    assert_eq!(
        (
            footer.segment,
            footer.sealed_end,
            footer.frames_bytes,
            footer.object_bytes()
        ),
        (
            header(),
            LARGE_SEALED_END,
            large_frames().len() as u64,
            object.len() as u64
        )
    );
}

#[test]
fn object_starts_with_the_frames_unchanged() {
    let frames = large_frames();
    assert_eq!(large_object().slice(..frames.len()), frames);
}

#[test]
fn segment_larger_than_a_block_gets_several_blocks() {
    let (footer, _index) = open(&large_object()).expect("valid object");
    let expected_blocks = large_frames().len() as u64 / TIERED_BLOCK_BYTES;
    assert!(
        u64::from(footer.index_entry_count) >= expected_blocks && expected_blocks >= 5,
        "{} blocks",
        footer.index_entry_count
    );
}

#[test]
fn every_sequence_is_found_in_the_blocks_fetched_for_it() {
    let object = large_object();
    let (_footer, index) = open(&object).expect("valid object");
    for sequence in FIRST_SEQUENCE..=LARGE_SEALED_END.get() {
        let byte_range = index.blocks_from(SequenceNumber::new(sequence), 1);
        assert!(
            sequences_in(&object, &byte_range).contains(&sequence),
            "sequence {sequence} is not in bytes {byte_range:?}"
        );
    }
}

#[test]
fn fetch_limit_smaller_than_a_block_still_fetches_one_whole_block() {
    let object = large_object();
    let (_footer, index) = open(&object).expect("valid object");
    let byte_range = index.blocks_from(SequenceNumber::new(FIRST_SEQUENCE), 1);
    assert!(
        (TIERED_BLOCK_BYTES..2 * TIERED_BLOCK_BYTES).contains(&(byte_range.end - byte_range.start)),
        "{byte_range:?}"
    );
}

#[test]
fn fetch_limit_of_several_blocks_fetches_whole_blocks_within_it() {
    let object = large_object();
    let (_footer, index) = open(&object).expect("valid object");
    let limit_bytes = 3 * TIERED_BLOCK_BYTES + TIERED_BLOCK_BYTES / 2;
    let byte_range = index.blocks_from(SequenceNumber::new(FIRST_SEQUENCE), limit_bytes);
    let fetched_bytes = byte_range.end - byte_range.start;
    assert!(
        fetched_bytes <= limit_bytes
            && fetched_bytes > limit_bytes - 2 * TIERED_BLOCK_BYTES
            && !sequences_in(&object, &byte_range).is_empty(),
        "{byte_range:?}"
    );
}

#[test]
fn fetch_limit_larger_than_the_segment_fetches_all_frames() {
    let object = large_object();
    let (footer, index) = open(&object).expect("valid object");
    assert_eq!(
        index.blocks_from(SequenceNumber::new(FIRST_SEQUENCE), u64::MAX),
        0..footer.frames_bytes
    );
}

#[test]
fn last_block_ends_where_the_frames_end() {
    let object = large_object();
    let (footer, index) = open(&object).expect("valid object");
    let byte_range = index.blocks_from(LARGE_SEALED_END, 1);
    assert_eq!(byte_range.end, footer.frames_bytes);
}

#[test]
fn sequence_below_the_segment_is_read_from_its_start() {
    let (_footer, index) = open(&large_object()).expect("valid object");
    assert_eq!(index.blocks_from(SequenceNumber::new(1), 1).start, 0);
}

#[test]
fn segment_sealed_empty_is_a_footer_alone() {
    let sealed_end = SequenceNumber::new(FIRST_SEQUENCE - 1);
    let object = encode_tiered_segment(header(), sealed_end, &Bytes::new()).expect("valid segment");
    let (footer, index) = open(&object).expect("valid object");
    assert_eq!(
        (
            object.len(),
            footer.sealed_end,
            index.blocks_from(SequenceNumber::new(FIRST_SEQUENCE), 1)
        ),
        (TIERED_FOOTER_BYTES, sealed_end, 0..0)
    );
}

#[test]
fn copy_that_stops_short_of_the_sealed_end_is_not_uploaded() {
    let short_frames = frames_of(59, 5, 2_000);
    assert!(matches!(
        encode_tiered_segment(header(), LARGE_SEALED_END, &short_frames),
        Err(TieredError::WrongExtent {
            found_end: Some(found_end),
            ..
        }) if found_end < LARGE_SEALED_END
    ));
}

#[test]
fn copy_that_runs_past_the_sealed_end_is_not_uploaded() {
    let long_frames = frames_of(61, 5, 2_000);
    assert!(matches!(
        encode_tiered_segment(header(), LARGE_SEALED_END, &long_frames),
        Err(TieredError::WrongExtent { .. })
    ));
}

#[test]
fn copy_that_does_not_start_at_the_segment_start_is_not_uploaded() {
    let mut later_segment = header();
    later_segment.first_sequence = SequenceNumber::new(FIRST_SEQUENCE - 5);
    assert!(matches!(
        encode_tiered_segment(later_segment, LARGE_SEALED_END, &large_frames()),
        Err(TieredError::WrongExtent { .. })
    ));
}

#[test]
fn empty_copy_of_a_segment_with_records_is_not_uploaded() {
    assert!(matches!(
        encode_tiered_segment(header(), LARGE_SEALED_END, &Bytes::new()),
        Err(TieredError::WrongExtent {
            found_first: None,
            ..
        })
    ));
}

#[test]
fn copy_with_a_damaged_frame_is_not_uploaded() {
    let mut damaged_frames = large_frames().to_vec();
    damaged_frames[30_000] ^= 0x01;
    assert!(matches!(
        encode_tiered_segment(header(), LARGE_SEALED_END, &Bytes::from(damaged_frames)),
        Err(TieredError::InvalidFrames(_))
    ));
}

#[test]
fn bytes_shorter_than_a_footer_are_refused() {
    assert_eq!(
        TieredFooter::decode(&[0; TIERED_FOOTER_BYTES - 1]),
        Err(TieredError::IncompleteFooter {
            available_bytes: TIERED_FOOTER_BYTES - 1
        })
    );
}

#[test]
fn object_that_is_not_a_tiered_segment_is_refused() {
    assert_eq!(
        TieredFooter::decode(&[b'x'; 200]),
        Err(TieredError::BadMagic)
    );
}

#[test]
fn footer_of_a_newer_format_is_refused() {
    let mut object = large_object().to_vec();
    let version_offset = object.len() - TIERED_FOOTER_BYTES + TIERED_MAGIC.len();
    object[version_offset] = 2;
    assert_eq!(
        TieredFooter::decode(&object),
        Err(TieredError::UnsupportedVersion { found: 2 })
    );
}

#[test]
fn index_of_another_size_than_the_footer_says_is_refused() {
    let object = large_object();
    let footer = TieredFooter::decode(&object).expect("valid footer");
    let index_bytes = bytes_of(&object, &footer.index_range());
    let short_index = &index_bytes[..index_bytes.len() - 1];
    assert!(matches!(
        TieredIndex::decode(&footer, short_index),
        Err(TieredError::DamagedIndex { .. })
    ));
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
        assert_ne!(
            open(&Bytes::from(damaged)),
            Ok(intact.clone()),
            "damage at byte {damaged_offset} went unnoticed"
        );
    }
}

#[test]
fn index_whose_entries_do_not_describe_the_frames_is_refused() {
    // A footer that claims frames but an index without blocks: internally
    // consistent checksums, impossible content.
    let footer = TieredFooter {
        segment: header(),
        sealed_end: LARGE_SEALED_END,
        frames_bytes: 1_000,
        index_entry_count: 0,
        index_checksum: crc32c::crc32c(&[]),
    };
    assert_eq!(
        TieredIndex::decode(&footer, &[]),
        Err(TieredError::DamagedIndex {
            reason: "its entries do not describe the frames"
        })
    );
}
