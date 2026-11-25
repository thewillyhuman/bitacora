//! Fuzz-style tests of every decoder of the segment crate.
//!
//! # What is covered
//!
//! Every function that turns bytes from a disk, a socket or an object store into a value is fed bytes it
//! was not written for: pure noise, and valid encodings that were cut, extended or had bits flipped. The
//! property is the one a fuzzer checks: the decoder returns, with a value or an error, and never panics,
//! overflows or tries to allocate what a corrupted length field asks for. Whether a decoder *rejects*
//! damaged input is covered next to each decoder, by its own tests.
//!
//! # How the fixtures work
//!
//! Inputs come from the workspace's seeded generator, so a failure names a seed and replays exactly. Each
//! test runs a fixed number of rounds that keeps the whole file well under a second.

use bitacora_segment::SegmentState;
use bitacora_segment::format::{
    Batch, FlushedMark, SEGMENT_HEADER_BYTES, SegmentHeader, decode_frames, scan_valid_prefix,
};
use bitacora_segment::tiered::{TieredFooter, TieredIndex, encode_tiered_segment};
use bitacora_types::{DeterministicRandom, Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
use bytes::{BufMut, Bytes, BytesMut};

/// The segment every valid encoding in this file belongs to.
const SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
/// The sequence number of the segment's first record.
const FIRST_SEQUENCE: SequenceNumber = SequenceNumber::new(100);
/// How many inputs each test tries.
const ROUNDS: u64 = 2_000;
/// The longest noise input, a little more than the largest fixed-size structure.
const LONGEST_NOISE_BYTES: u64 = 300;

/// The header of [`SEGMENT`].
fn segment_header() -> SegmentHeader {
    SegmentHeader {
        address: SEGMENT,
        epoch: Epoch::new(5),
        first_sequence: FIRST_SEQUENCE,
    }
}

/// A few consecutive batches of the segment, of random shape, and their last sequence number.
fn valid_frames(random: &mut DeterministicRandom) -> (Bytes, SequenceNumber) {
    let mut frames = BytesMut::new();
    let mut next_sequence = FIRST_SEQUENCE;
    let batch_count = random.between(1, 4);
    for _batch in 0..batch_count {
        let record_count = random.between(1, 5);
        let mut payloads = Vec::new();
        for _record in 0..record_count {
            let payload_bytes = usize::try_from(random.below(40)).expect("small");
            payloads.push(Bytes::from(random.bytes(payload_bytes)));
        }
        let batch = Batch::encode(SEGMENT, next_sequence, &payloads).expect("valid batch");
        frames.put_slice(batch.frame());
        next_sequence = batch.last_sequence().next();
    }
    (frames.freeze(), next_sequence.preceding())
}

/// Returns `valid` damaged in one random way: cut short, extended with noise, or with some bits flipped.
fn damaged(valid: &[u8], random: &mut DeterministicRandom) -> Vec<u8> {
    let mut bytes = valid.to_vec();
    match random.below(3) {
        0 => bytes.truncate(random.index(bytes.len() + 1)),
        1 => {
            let extra_bytes = usize::try_from(random.between(1, 64)).expect("small");
            bytes.extend(random.bytes(extra_bytes));
        }
        _flip_bits => {
            let flip_count = random.between(1, 8);
            for _flip in 0..flip_count {
                if !bytes.is_empty() {
                    let position = random.index(bytes.len());
                    bytes[position] ^= 1 << random.below(8);
                }
            }
        }
    }
    bytes
}

/// Noise of a random length.
fn noise(random: &mut DeterministicRandom) -> Vec<u8> {
    let length_bytes = usize::try_from(random.below(LONGEST_NOISE_BYTES)).expect("small");
    random.bytes(length_bytes)
}

/// Runs every decoder of the crate over `bytes`. Returning at all is the assertion.
fn decode_with_everything(bytes: &[u8]) {
    let shared_bytes = Bytes::copy_from_slice(bytes);
    let _header = SegmentHeader::decode(bytes);
    let _mark = FlushedMark::latest_in(SEGMENT, bytes);
    let _state = SegmentState::decode(bytes);
    let _batch = Batch::decode(SEGMENT, &shared_bytes);
    let _frames = decode_frames(SEGMENT, &shared_bytes);
    let _prefix = scan_valid_prefix(SEGMENT, FIRST_SEQUENCE, &shared_bytes);
    if let Ok(footer) = TieredFooter::decode(bytes) {
        let _index = TieredIndex::decode(&footer, bytes);
    }
}

#[test]
fn no_decoder_panics_on_noise() {
    let mut random = DeterministicRandom::from_seed(1);

    for _round in 0..ROUNDS {
        decode_with_everything(&noise(&mut random));
    }
}

#[test]
fn no_decoder_panics_on_a_damaged_segment_header() {
    let mut random = DeterministicRandom::from_seed(2);
    let valid_header = segment_header().encode();

    for _round in 0..ROUNDS {
        decode_with_everything(&damaged(&valid_header, &mut random));
    }
}

#[test]
fn no_decoder_panics_on_damaged_batch_frames() {
    let mut random = DeterministicRandom::from_seed(3);

    for _round in 0..ROUNDS {
        let (frames, _last_sequence) = valid_frames(&mut random);
        decode_with_everything(&damaged(&frames, &mut random));
    }
}

#[test]
fn no_decoder_panics_on_a_damaged_segment_file() {
    let mut random = DeterministicRandom::from_seed(4);

    for _round in 0..ROUNDS {
        let (frames, _last_sequence) = valid_frames(&mut random);
        let mut file_content = segment_header().encode().to_vec();
        file_content.extend_from_slice(&frames);
        let damaged_file = damaged(&file_content, &mut random);

        decode_with_everything(&damaged_file);
        if damaged_file.len() > SEGMENT_HEADER_BYTES {
            decode_with_everything(&damaged_file[SEGMENT_HEADER_BYTES..]);
        }
    }
}

#[test]
fn no_decoder_panics_on_a_damaged_tiered_object() {
    let mut random = DeterministicRandom::from_seed(5);

    for _round in 0..ROUNDS {
        let (frames, last_sequence) = valid_frames(&mut random);
        let object = encode_tiered_segment(segment_header(), last_sequence, &frames).expect("valid segment");
        let damaged_object = damaged(&object, &mut random);

        decode_with_everything(&damaged_object);
        if let Ok(footer) = TieredFooter::decode(&damaged_object) {
            // A reader fetches the index range the footer names; a damaged footer may name anything.
            let index_range = footer.index_range();
            let object_bytes = damaged_object.len() as u64;
            if index_range.start <= index_range.end && index_range.end <= object_bytes {
                let start = usize::try_from(index_range.start).expect("checked against the length");
                let end = usize::try_from(index_range.end).expect("checked against the length");
                if let Ok(index) = TieredIndex::decode(&footer, &damaged_object[start..end]) {
                    let _blocks = index.blocks_from(FIRST_SEQUENCE, random.next_u64());
                }
            }
        }
    }
}

/// A corrupted length field must not make a decoder reserve the memory it names.
#[test]
fn frame_that_claims_a_huge_body_is_refused_without_allocating_it() {
    let mut frame_with_huge_length = vec![0xFF, 0xFF, 0xFF, 0xFF];
    frame_with_huge_length.extend_from_slice(&[0_u8; 64]);

    let decoded = Batch::decode(SEGMENT, &Bytes::from(frame_with_huge_length));

    assert!(decoded.is_err(), "{decoded:?}");
}
