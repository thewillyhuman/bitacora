//! The byte formats of the segment crate, held to bytes written by format version 1.
//!
//! # What is covered
//!
//! For the segment header, the flushed mark, a batch frame, the segment state file and the tiered object:
//! today's code produces exactly the bytes below, and reads those bytes back to the values they were made
//! from. The other tests of the crate encode and decode with the same code, so they would all still pass
//! if a change altered a layout. These would not: they are what catches an accidental format change, and
//! what a deliberate one has to update together with the format version and `docs/protocol/`.
//!
//! # How the fixtures work
//!
//! The golden bytes were printed once by version 1 of each format and are written here in hexadecimal,
//! sixteen bytes per line. Everything describes one small segment: stream 7, segment 3, written in epoch
//! 5, holding records 100 to 102 in a single batch.

use bitacora_segment::SegmentState;
use bitacora_segment::format::{
    Batch, FlushedMark, SEGMENT_HEADER_BYTES, SegmentHeader, decode_frames,
};
use bitacora_segment::tiered::{TieredFooter, TieredIndex, encode_tiered_segment};
use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
use bytes::Bytes;

/// The segment every golden encoding belongs to.
const SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
/// The segment's first sequence number.
const FIRST_SEQUENCE: SequenceNumber = SequenceNumber::new(100);
/// The segment's last sequence number.
const SEALED_END: SequenceNumber = SequenceNumber::new(102);

/// The first 48 bytes of the segment file: the identity. The rest of the 128-byte header is zeros.
const GOLDEN_IDENTITY: &str = "
    42495441534547000100000007000000
    00000000030000000000000005000000
    000000006400000000000000595d7fcc";

/// A flushed mark saying that 4096 bytes are flushed, generation 9.
const GOLDEN_FLUSHED_MARK: &str = "0010000000000000090000000681c0b8";

/// One batch frame: records 100 to 102 with the payloads `alpha`, nothing, and `gamma!`.
const GOLDEN_FRAME: &str = "
    17000000640000000000000003000000
    668c9d1f05000000616c706861000000
    000600000067616d6d6121";

/// The state file of a copy fenced at epoch 6 and sealed at record 102.
const GOLDEN_SEALED_STATE: &str = "
    42495441535441000100000006000000
    00000000016600000000000000fa89f2
    ab";

/// The state file of a copy fenced at epoch 6 and not sealed.
const GOLDEN_OPEN_STATE: &str = "
    42495441535441000100000006000000
    000000000000000000000000003a89a9
    fb";

/// The tiered object of the segment: the frame, one index entry, the footer.
const GOLDEN_TIERED_OBJECT: &str = "
    17000000640000000000000003000000
    668c9d1f05000000616c706861000000
    000600000067616d6d61216400000000
    00000000000000000000004249544154
    49455201000000070000000000000003
    00000000000000050000000000000064
    0000000000000066000000000000002b
    000000000000000100000083c5a0f034
    ccd63b";

/// Turns hexadecimal text, with any white space in it, into bytes.
fn bytes_of(hexadecimal: &str) -> Vec<u8> {
    let mut digits = Vec::new();
    for character in hexadecimal.chars() {
        if !character.is_whitespace() {
            digits.push(character.to_digit(16).expect("a hexadecimal digit"));
        }
    }
    let mut bytes = Vec::new();
    for pair in digits.chunks(2) {
        bytes.push(u8::try_from(pair[0] * 16 + pair[1]).expect("two digits make a byte"));
    }
    bytes
}

/// The header of the golden segment.
fn segment_header() -> SegmentHeader {
    SegmentHeader {
        address: SEGMENT,
        epoch: Epoch::new(5),
        first_sequence: FIRST_SEQUENCE,
    }
}

/// The payloads of the golden batch.
fn payloads() -> Vec<Bytes> {
    vec![
        Bytes::from_static(b"alpha"),
        Bytes::new(),
        Bytes::from_static(b"gamma!"),
    ]
}

/// The golden flushed mark as a value.
fn flushed_mark() -> FlushedMark {
    FlushedMark {
        flushed_bytes: 4096,
        generation: 9,
    }
}

/// The golden frame as shared bytes.
fn golden_frame() -> Bytes {
    Bytes::from(bytes_of(GOLDEN_FRAME))
}

#[test]
fn segment_header_is_written_as_in_version_1() {
    let mut golden_header = bytes_of(GOLDEN_IDENTITY);
    golden_header.resize(SEGMENT_HEADER_BYTES, 0);

    assert_eq!(segment_header().encode().to_vec(), golden_header);
}

#[test]
fn segment_header_of_version_1_is_read() {
    let mut golden_header = bytes_of(GOLDEN_IDENTITY);
    golden_header.resize(SEGMENT_HEADER_BYTES, 0);

    assert_eq!(SegmentHeader::decode(&golden_header), Ok(segment_header()));
}

#[test]
fn flushed_mark_is_written_as_in_version_1() {
    assert_eq!(
        flushed_mark().encode(SEGMENT).to_vec(),
        bytes_of(GOLDEN_FLUSHED_MARK)
    );
}

#[test]
fn flushed_mark_with_an_odd_generation_goes_to_the_second_slot() {
    assert_eq!(flushed_mark().slot_offset(), 80);
}

#[test]
fn flushed_mark_of_version_1_is_read_from_its_slot() {
    let mut golden_header = bytes_of(GOLDEN_IDENTITY);
    golden_header.resize(SEGMENT_HEADER_BYTES, 0);
    golden_header[80..96].copy_from_slice(&bytes_of(GOLDEN_FLUSHED_MARK));

    assert_eq!(
        FlushedMark::latest_in(SEGMENT, &golden_header),
        Some(flushed_mark())
    );
}

#[test]
fn batch_frame_is_written_as_in_version_1() {
    let batch = Batch::encode(SEGMENT, FIRST_SEQUENCE, &payloads()).expect("valid batch");

    assert_eq!(batch.frame().to_vec(), bytes_of(GOLDEN_FRAME));
}

#[test]
fn batch_frame_of_version_1_is_read() {
    let batch = Batch::decode(SEGMENT, &golden_frame()).expect("the golden frame is valid");

    let mut read_payloads = Vec::new();
    for record in batch.records() {
        read_payloads.push(record.payload);
    }
    assert_eq!(read_payloads, payloads());
}

#[test]
fn batch_frame_of_version_1_numbers_its_records_from_its_first_sequence() {
    let batches = decode_frames(SEGMENT, &golden_frame()).expect("the golden frame is valid");

    assert_eq!(batches[0].first_sequence(), FIRST_SEQUENCE);
    assert_eq!(batches[0].last_sequence(), SEALED_END);
}

#[test]
fn sealed_state_is_written_as_in_version_1() {
    let state = SegmentState {
        fenced_epoch: Epoch::new(6),
        sealed_end: Some(SEALED_END),
    };

    assert_eq!(state.encode().to_vec(), bytes_of(GOLDEN_SEALED_STATE));
}

#[test]
fn sealed_state_of_version_1_is_read() {
    let state = SegmentState::decode(&bytes_of(GOLDEN_SEALED_STATE));

    assert_eq!(
        state,
        Ok(SegmentState {
            fenced_epoch: Epoch::new(6),
            sealed_end: Some(SEALED_END),
        })
    );
}

#[test]
fn open_state_is_written_as_in_version_1() {
    let state = SegmentState {
        fenced_epoch: Epoch::new(6),
        sealed_end: None,
    };

    assert_eq!(state.encode().to_vec(), bytes_of(GOLDEN_OPEN_STATE));
}

#[test]
fn open_state_of_version_1_is_read() {
    let state = SegmentState::decode(&bytes_of(GOLDEN_OPEN_STATE));

    assert_eq!(
        state,
        Ok(SegmentState {
            fenced_epoch: Epoch::new(6),
            sealed_end: None,
        })
    );
}

#[test]
fn tiered_object_is_written_as_in_version_1() {
    let object = encode_tiered_segment(segment_header(), SEALED_END, &golden_frame())
        .expect("the golden frame is the whole segment");

    assert_eq!(object.to_vec(), bytes_of(GOLDEN_TIERED_OBJECT));
}

#[test]
fn footer_of_a_version_1_tiered_object_is_read() {
    let golden_object = bytes_of(GOLDEN_TIERED_OBJECT);

    let footer = TieredFooter::decode(&golden_object).expect("the golden footer is valid");

    assert_eq!(footer.segment, segment_header());
    assert_eq!(footer.sealed_end, SEALED_END);
    assert_eq!(footer.frames_bytes, 43);
    assert_eq!(footer.object_bytes(), golden_object.len() as u64);
}

#[test]
fn index_of_a_version_1_tiered_object_is_read() {
    let golden_object = bytes_of(GOLDEN_TIERED_OBJECT);
    let footer = TieredFooter::decode(&golden_object).expect("the golden footer is valid");
    let index_bytes = &golden_object[43..59];

    let index = TieredIndex::decode(&footer, index_bytes).expect("the golden index is valid");

    assert_eq!(index.blocks_from(FIRST_SEQUENCE, 1), 0..43);
}
