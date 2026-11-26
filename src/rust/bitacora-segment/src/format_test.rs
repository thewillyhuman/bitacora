//! Tests of the segment file layout.
//!
//! # What is covered
//!
//! - The segment header: a round trip, and every way it is refused.
//! - The flushed marks: which one is current, and what a torn or foreign one does.
//! - Encoding a batch: numbering, payloads and the size limits.
//! - Decoding a frame: a round trip, every single flipped bit, cut and unwritten input, and frames only a
//!   faulty writer could produce.
//! - Strict decoding of a run of frames.
//! - The recovery scan, including the two properties the design names: any byte prefix of a valid file
//!   recovers to a prefix of its batches, and damage never loses an earlier batch or invents a new one.
//!
//! The exact bytes of the layout are held by `tests/format_golden_test.rs`.
//!
//! # How the fixtures work
//!
//! Everything belongs to [`SEGMENT`]; [`OTHER_SEGMENT`] is the segment a frame or a mark must not be
//! accepted for. `batch` encodes text payloads, `concatenated` lays batches end to end as a file or a
//! message does, and `random_batches` builds consecutive batches of random shape from a seed, so that a
//! failing property test can name the seed.

use bitacora_types::DeterministicRandom;

use super::*;

/// The segment every fixture belongs to.
const SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));

/// A segment of the same stream that the fixtures do not belong to.
const OTHER_SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(4));

/// How many seeds the test of byte prefixes tries.
const BYTE_PREFIX_SEEDS: u64 = 40;

/// How many seeds the test of single damaged bytes tries.
const DAMAGE_SEEDS: u64 = 200;

/// How many seeds the test of arbitrary bytes tries.
const NOISE_SEEDS: u64 = 2_000;

/// The header of [`SEGMENT`], written at epoch 5 and starting at record 100.
fn header() -> SegmentHeader {
    SegmentHeader {
        address: SEGMENT,
        epoch: Epoch::new(5),
        first_sequence: SequenceNumber::new(100),
    }
}

/// The payloads that hold `texts`, one record each.
fn payloads(texts: &[&'static str]) -> Vec<Bytes> {
    texts.iter().map(|text| Bytes::from(*text)).collect()
}

/// A batch of [`SEGMENT`] that holds `texts` and starts at `first_sequence`.
fn batch(first_sequence: u64, texts: &[&'static str]) -> Batch {
    Batch::encode(SEGMENT, SequenceNumber::new(first_sequence), &payloads(texts))
        .expect("the fixture's payloads are within every limit")
}

/// The frames of `batches` laid end to end, as in a segment file or a message.
fn concatenated(batches: &[Batch]) -> Bytes {
    let mut frames = BytesMut::new();
    for batch in batches {
        frames.put_slice(batch.frame());
    }
    frames.freeze()
}

/// Builds `batch_count` consecutive batches of random shape from `seed`: one to five records of up to 40
/// bytes each, starting at a sequence number between 1 and 1000.
fn random_batches(seed: u64, batch_count: usize) -> Vec<Batch> {
    let mut random = DeterministicRandom::from_seed(seed);
    let mut next_sequence = SequenceNumber::new(random.between(1, 1_000));
    let mut batches = Vec::new();
    for _batch_number in 0..batch_count {
        let record_count = random.between(1, 5);
        let mut record_payloads = Vec::new();
        for _record_number in 0..record_count {
            let payload_bytes = usize::try_from(random.between(0, 40)).expect("at most 40");
            record_payloads.push(Bytes::from(random.bytes(payload_bytes)));
        }
        let batch = Batch::encode(SEGMENT, next_sequence, &record_payloads).expect("small payloads are valid");
        next_sequence = batch.last_sequence().next();
        batches.push(batch);
    }
    batches
}

/// How many of `batches`, laid end to end, lie entirely within the first `byte_count` bytes.
fn whole_batches_within(batches: &[Batch], byte_count: usize) -> usize {
    let mut whole_batches = 0;
    let mut end_offset = 0;
    for batch in batches {
        end_offset += batch.frame().len();
        if end_offset > byte_count {
            break;
        }
        whole_batches += 1;
    }
    whole_batches
}

/// The flushed mark with the given fields.
fn mark(flushed_bytes: u64, generation: u32) -> FlushedMark {
    FlushedMark {
        flushed_bytes,
        generation,
    }
}

/// The header of [`SEGMENT`] with each of `marks` written to its slot, in the order given.
fn header_with_marks(marks: &[FlushedMark]) -> Vec<u8> {
    let mut header_bytes = header().encode().to_vec();
    for mark in marks {
        let slot_offset = mark.slot_offset();
        header_bytes[slot_offset..slot_offset + FLUSHED_MARK_BYTES].copy_from_slice(&mark.encode(SEGMENT));
    }
    header_bytes
}

/// Builds a frame with a correct checksum around an arbitrary body and header fields, the way only a
/// faulty writer could.
fn frame_with_valid_checksum(first_sequence: u64, record_count: u32, body: &[u8]) -> Bytes {
    let mut frame = BytesMut::new();
    frame.put_u32_le(u32::try_from(body.len()).expect("the fixture's bodies are small"));
    frame.put_u64_le(first_sequence);
    frame.put_u32_le(record_count);
    frame.put_u32_le(CHECKSUM_PLACEHOLDER);
    frame.put_slice(body);
    let checksum = frame_checksum(SEGMENT, &frame);
    frame[FRAME_CHECKSUM_OFFSET..BATCH_HEADER_BYTES].copy_from_slice(&checksum.to_le_bytes());
    frame.freeze()
}

// The segment header.

#[test]
fn header_survives_an_encode_decode_round_trip() {
    let decoded = SegmentHeader::decode(&header().encode());

    assert_eq!(decoded, Ok(header()));
}

#[test]
fn header_decoding_ignores_the_bytes_after_it() {
    let mut file_start = header().encode().to_vec();
    file_start.extend_from_slice(b"batch frames follow");

    let decoded = SegmentHeader::decode(&file_start);

    assert_eq!(decoded, Ok(header()));
}

#[test]
fn header_shorter_than_its_size_is_incomplete() {
    let encoded = header().encode();

    let decoded = SegmentHeader::decode(&encoded[..SEGMENT_HEADER_BYTES - 1]);

    assert_eq!(
        decoded,
        Err(HeaderError::Incomplete {
            available_bytes: SEGMENT_HEADER_BYTES - 1
        })
    );
}

#[test]
fn header_with_wrong_magic_is_not_a_segment_file() {
    let mut encoded = header().encode();
    encoded[0] ^= 0xFF;

    let decoded = SegmentHeader::decode(&encoded);

    assert_eq!(decoded, Err(HeaderError::WrongMagic));
}

#[test]
fn header_with_any_flipped_field_bit_fails_its_checksum() {
    for flipped_byte in SEGMENT_MAGIC.len()..HEADER_CHECKSUM_OFFSET + U32_BYTES {
        let mut encoded = header().encode();
        encoded[flipped_byte] ^= 0x01;

        let decoded = SegmentHeader::decode(&encoded);

        assert!(
            matches!(decoded, Err(HeaderError::ChecksumMismatch { .. })),
            "flip at byte {flipped_byte} went undetected"
        );
    }
}

#[test]
fn header_from_a_newer_format_is_refused() {
    let mut encoded = header().encode();
    encoded[HEADER_FORMAT_VERSION_OFFSET..HEADER_STREAM_ID_OFFSET]
        .copy_from_slice(&(SEGMENT_FORMAT_VERSION + 1).to_le_bytes());
    let checksum = crc32c::crc32c(&encoded[..HEADER_CHECKSUM_OFFSET]);
    encoded[HEADER_CHECKSUM_OFFSET..HEADER_CHECKSUM_OFFSET + U32_BYTES].copy_from_slice(&checksum.to_le_bytes());

    let decoded = SegmentHeader::decode(&encoded);

    assert_eq!(
        decoded,
        Err(HeaderError::UnsupportedVersion {
            found_version: SEGMENT_FORMAT_VERSION + 1
        })
    );
}

#[test]
fn header_starting_at_the_empty_position_is_refused() {
    let starts_at_zero = SegmentHeader {
        first_sequence: SequenceNumber::ZERO,
        ..header()
    };

    let decoded = SegmentHeader::decode(&starts_at_zero.encode());

    assert_eq!(decoded, Err(HeaderError::StartsAtEmptyPosition));
}

// The flushed marks.

#[test]
fn fresh_header_holds_no_flushed_mark() {
    let latest = FlushedMark::latest_in(SEGMENT, &header().encode());

    assert_eq!(latest, None);
}

#[test]
fn stored_mark_is_read_back() {
    let header_bytes = header_with_marks(&[mark(4_096, 1)]);

    let latest = FlushedMark::latest_in(SEGMENT, &header_bytes);

    assert_eq!(latest, Some(mark(4_096, 1)));
}

#[test]
fn mark_with_the_highest_generation_is_the_current_one() {
    let header_bytes = header_with_marks(&[mark(4_096, 4), mark(8_192, 5)]);

    let latest = FlushedMark::latest_in(SEGMENT, &header_bytes);

    assert_eq!(latest, Some(mark(8_192, 5)));
}

/// The first slot holds the even generations, so the current mark is there as often as in the second.
#[test]
fn mark_in_the_first_slot_is_current_when_its_generation_is_higher() {
    let header_bytes = header_with_marks(&[mark(4_096, 5), mark(8_192, 6)]);

    let latest = FlushedMark::latest_in(SEGMENT, &header_bytes);

    assert_eq!(latest, Some(mark(8_192, 6)));
}

#[test]
fn consecutive_marks_use_different_slots() {
    assert_ne!(mark(0, 4).slot_offset(), mark(0, 5).slot_offset());
}

#[test]
fn marks_two_generations_apart_use_the_same_slot() {
    assert_eq!(mark(0, 4).slot_offset(), mark(0, 6).slot_offset());
}

#[test]
fn both_mark_slots_lie_inside_the_header() {
    for generation in [0, 1] {
        let slot_end = mark(0, generation).slot_offset() + FLUSHED_MARK_BYTES;

        assert!(slot_end <= SEGMENT_HEADER_BYTES, "generation {generation}");
    }
}

#[test]
fn torn_mark_leaves_the_previous_one_current() {
    let mut header_bytes = header_with_marks(&[mark(4_096, 4), mark(8_192, 5)]);
    header_bytes[mark(0, 5).slot_offset() + 3] ^= 0xFF;

    let latest = FlushedMark::latest_in(SEGMENT, &header_bytes);

    assert_eq!(latest, Some(mark(4_096, 4)));
}

#[test]
fn mark_of_another_segment_is_refused() {
    let header_bytes = header_with_marks(&[mark(4_096, 1)]);

    let latest = FlushedMark::latest_in(OTHER_SEGMENT, &header_bytes);

    assert_eq!(latest, None);
}

#[test]
fn header_cut_before_its_marks_holds_none() {
    let header_bytes = header_with_marks(&[mark(4_096, 1)]);

    let latest = FlushedMark::latest_in(SEGMENT, &header_bytes[..FLUSHED_MARKS_OFFSET]);

    assert_eq!(latest, None);
}

#[test]
fn storing_a_mark_leaves_the_header_identity_valid() {
    let header_bytes = header_with_marks(&[mark(4_096, 1), mark(8_192, 2)]);

    let decoded = SegmentHeader::decode(&header_bytes);

    assert_eq!(decoded, Ok(header()));
}

// Encoding a batch.

#[test]
fn batch_numbers_its_records_from_the_first_sequence() {
    let encoded = batch(100, &["a", "b", "c"]);

    let sequences: Vec<u64> = encoded.records().map(|record| record.sequence.get()).collect();

    assert_eq!(sequences, [100, 101, 102]);
}

#[test]
fn batch_returns_the_payloads_it_was_given() {
    let encoded = batch(100, &["alpha", "", "gamma"]);

    let decoded_payloads: Vec<Bytes> = encoded.records().map(|record| record.payload).collect();

    assert_eq!(decoded_payloads, payloads(&["alpha", "", "gamma"]));
}

#[test]
fn batch_reports_its_first_sequence() {
    assert_eq!(batch(100, &["a", "b", "c"]).first_sequence(), SequenceNumber::new(100));
}

#[test]
fn batch_last_sequence_is_its_final_record() {
    assert_eq!(batch(100, &["a", "b", "c"]).last_sequence(), SequenceNumber::new(102));
}

#[test]
fn batch_of_one_record_ends_where_it_starts() {
    assert_eq!(batch(100, &["a"]).last_sequence(), SequenceNumber::new(100));
}

#[test]
fn batch_reports_how_many_records_it_holds() {
    assert_eq!(batch(100, &["a", "b", "c"]).record_count(), 3);
}

#[test]
fn batch_frame_is_its_header_and_every_record() {
    let encoded = batch(100, &["alpha", ""]);

    assert_eq!(
        encoded.frame().len(),
        BATCH_HEADER_BYTES + 2 * RECORD_HEADER_BYTES + "alpha".len()
    );
}

#[test]
fn batch_without_records_is_rejected() {
    let encoded = Batch::encode(SEGMENT, SequenceNumber::FIRST, &[]);

    assert_eq!(encoded, Err(EncodeError::NoRecords));
}

#[test]
fn batch_starting_at_the_empty_position_is_rejected() {
    let encoded = Batch::encode(SEGMENT, SequenceNumber::ZERO, &payloads(&["a"]));

    assert_eq!(encoded, Err(EncodeError::StartsAtEmptyPosition));
}

#[test]
fn batch_accepts_a_record_of_exactly_the_maximum_size() {
    let largest_payload = Bytes::from(vec![0xAB; MAX_RECORD_PAYLOAD_BYTES]);

    let encoded = Batch::encode(SEGMENT, SequenceNumber::FIRST, std::slice::from_ref(&largest_payload))
        .expect("the maximum size is allowed");

    assert_eq!(encoded.records().next().expect("one record").payload, largest_payload);
}

#[test]
fn batch_rejects_a_record_over_the_maximum_size() {
    let oversized_payload = Bytes::from(vec![0xAB; MAX_RECORD_PAYLOAD_BYTES + 1]);

    let encoded = Batch::encode(SEGMENT, SequenceNumber::FIRST, &[oversized_payload]);

    assert_eq!(
        encoded,
        Err(EncodeError::RecordTooLarge {
            payload_bytes: MAX_RECORD_PAYLOAD_BYTES + 1
        })
    );
}

#[test]
fn batch_rejects_a_body_over_the_maximum_size() {
    let record_payload = Bytes::from(vec![0xAB; MAX_RECORD_PAYLOAD_BYTES]);
    let too_many = vec![record_payload; 3];

    let encoded = Batch::encode(SEGMENT, SequenceNumber::FIRST, &too_many);

    assert_eq!(
        encoded,
        Err(EncodeError::BatchTooLarge {
            body_bytes: 3 * (RECORD_HEADER_BYTES + MAX_RECORD_PAYLOAD_BYTES)
        })
    );
}

/// An oversized record is the more specific complaint, wherever it is in the batch.
#[test]
fn oversized_record_is_reported_before_an_oversized_body() {
    let record_payload = Bytes::from(vec![0xAB; MAX_RECORD_PAYLOAD_BYTES]);
    let oversized_payload = Bytes::from(vec![0xAB; MAX_RECORD_PAYLOAD_BYTES + 1]);
    let too_many_and_one_too_large = [record_payload.clone(), record_payload, oversized_payload];

    let encoded = Batch::encode(SEGMENT, SequenceNumber::FIRST, &too_many_and_one_too_large);

    assert_eq!(
        encoded,
        Err(EncodeError::RecordTooLarge {
            payload_bytes: MAX_RECORD_PAYLOAD_BYTES + 1
        })
    );
}

// Decoding a frame.

#[test]
fn decoding_an_encoded_frame_returns_the_same_batch() {
    let encoded = batch(100, &["a", "b"]);

    let decoded = Batch::decode(SEGMENT, encoded.frame());

    assert_eq!(decoded, Ok(encoded));
}

#[test]
fn decoding_ignores_bytes_after_the_frame() {
    let first = batch(100, &["a", "b"]);
    let both = concatenated(&[first.clone(), batch(102, &["c"])]);

    let decoded = Batch::decode(SEGMENT, &both);

    assert_eq!(decoded, Ok(first));
}

#[test]
fn frame_for_another_segment_fails_its_checksum() {
    let encoded = batch(100, &["a"]);

    let decoded = Batch::decode(OTHER_SEGMENT, encoded.frame());

    assert!(matches!(decoded, Err(FrameError::ChecksumMismatch { .. })));
}

#[test]
fn frame_with_any_flipped_bit_is_never_accepted_as_is() {
    let encoded = batch(100, &["alpha", "beta"]);

    for flipped_byte in 0..encoded.frame().len() {
        for flipped_bit in 0..u8::BITS {
            let mut damaged = encoded.frame().to_vec();
            damaged[flipped_byte] ^= 1 << flipped_bit;

            let decoded = Batch::decode(SEGMENT, &Bytes::from(damaged));

            assert!(
                decoded.is_err(),
                "flip of bit {flipped_bit} at byte {flipped_byte} went undetected"
            );
        }
    }
}

#[test]
fn frame_cut_short_is_incomplete() {
    let encoded = batch(100, &["alpha"]);
    let cut = encoded.frame().slice(..encoded.frame().len() - 1);

    let decoded = Batch::decode(SEGMENT, &cut);

    assert_eq!(
        decoded,
        Err(FrameError::Incomplete {
            needed_bytes: encoded.frame().len(),
            available_bytes: encoded.frame().len() - 1,
        })
    );
}

#[test]
fn zero_filled_space_is_the_end_of_data() {
    let unwritten_space = Bytes::from(vec![0_u8; 256]);

    let decoded = Batch::decode(SEGMENT, &unwritten_space);

    assert_eq!(decoded, Err(FrameError::EndOfData));
}

/// A body length such as 65536 starts with zero bytes, so a header cut short must ask for more bytes even
/// when all it shows is zeros.
#[test]
fn cut_header_of_zero_bytes_is_incomplete_not_the_end() {
    let cut_header = Bytes::from(vec![0_u8; BATCH_HEADER_BYTES - 1]);

    let decoded = Batch::decode(SEGMENT, &cut_header);

    assert_eq!(
        decoded,
        Err(FrameError::Incomplete {
            needed_bytes: BATCH_HEADER_BYTES,
            available_bytes: BATCH_HEADER_BYTES - 1,
        })
    );
}

#[test]
fn no_bytes_at_all_are_the_end_of_data() {
    let decoded = Batch::decode(SEGMENT, &Bytes::new());

    assert_eq!(decoded, Err(FrameError::EndOfData));
}

#[test]
fn frame_claiming_an_oversized_body_is_refused_before_reading_it() {
    let mut oversized = BytesMut::new();
    oversized.put_u32_le(u32::try_from(MAX_BATCH_BODY_BYTES + 1).expect("the limit fits in 32 bits"));
    oversized.put_slice(&[0_u8; BATCH_HEADER_BYTES]);

    let decoded = Batch::decode(SEGMENT, &oversized.freeze());

    assert_eq!(
        decoded,
        Err(FrameError::BodyTooLarge {
            body_bytes: MAX_BATCH_BODY_BYTES + 1
        })
    );
}

#[test]
fn frame_whose_record_count_disagrees_with_its_body_is_malformed() {
    let one_record_body = [1, 0, 0, 0, b'a'];
    let frame = frame_with_valid_checksum(100, 2, &one_record_body);

    let decoded = Batch::decode(SEGMENT, &frame);

    assert_eq!(
        decoded,
        Err(FrameError::Malformed {
            reason: "record count does not match the body"
        })
    );
}

#[test]
fn frame_whose_record_runs_past_the_body_is_malformed() {
    let claims_nine_bytes_holds_one = [9, 0, 0, 0, b'a'];
    let frame = frame_with_valid_checksum(100, 1, &claims_nine_bytes_holds_one);

    let decoded = Batch::decode(SEGMENT, &frame);

    assert_eq!(
        decoded,
        Err(FrameError::Malformed {
            reason: "body ends inside a record payload"
        })
    );
}

#[test]
fn frame_whose_body_ends_inside_a_length_is_malformed() {
    let frame = frame_with_valid_checksum(100, 1, &[1, 0]);

    let decoded = Batch::decode(SEGMENT, &frame);

    assert_eq!(
        decoded,
        Err(FrameError::Malformed {
            reason: "body ends inside a record length"
        })
    );
}

#[test]
fn frame_whose_record_claims_more_than_the_record_limit_is_malformed() {
    let oversized_length = u32::try_from(MAX_RECORD_PAYLOAD_BYTES + 1).expect("the limit fits in 32 bits");
    let frame = frame_with_valid_checksum(100, 1, &oversized_length.to_le_bytes());

    let decoded = Batch::decode(SEGMENT, &frame);

    assert_eq!(
        decoded,
        Err(FrameError::Malformed {
            reason: "record payload exceeds the size limit"
        })
    );
}

#[test]
fn frame_starting_at_the_empty_position_is_malformed() {
    let frame = frame_with_valid_checksum(0, 1, &[1, 0, 0, 0, b'a']);

    let decoded = Batch::decode(SEGMENT, &frame);

    assert_eq!(
        decoded,
        Err(FrameError::Malformed {
            reason: "batch starts at the empty position"
        })
    );
}

#[test]
fn frame_running_past_the_sequence_space_is_malformed() {
    let frame = frame_with_valid_checksum(u64::MAX, 1, &[1, 0, 0, 0, b'a']);

    let decoded = Batch::decode(SEGMENT, &frame);

    assert_eq!(
        decoded,
        Err(FrameError::Malformed {
            reason: "batch runs past the end of the sequence space"
        })
    );
}

// Strict decoding of a run of frames.

#[test]
fn consecutive_frames_decode_in_order() {
    let batches = vec![batch(100, &["a", "b"]), batch(102, &["c"])];

    let decoded = decode_frames(SEGMENT, &concatenated(&batches));

    assert_eq!(decoded, Ok(batches));
}

#[test]
fn no_frames_decode_to_no_batches() {
    let decoded = decode_frames(SEGMENT, &Bytes::new());

    assert_eq!(decoded, Ok(Vec::new()));
}

#[test]
fn frames_with_a_sequence_gap_are_refused() {
    let frames = concatenated(&[batch(100, &["a", "b"]), batch(103, &["c"])]);

    let decoded = decode_frames(SEGMENT, &frames);

    assert_eq!(
        decoded,
        Err(FramesError::SequenceGap {
            expected: SequenceNumber::new(102),
            found: SequenceNumber::new(103),
        })
    );
}

#[test]
fn frames_followed_by_trailing_garbage_are_refused() {
    let first = batch(100, &["a"]);
    let mut frames = first.frame().to_vec();
    frames.extend_from_slice(&[0xFF; 7]);

    let decoded = decode_frames(SEGMENT, &Bytes::from(frames));

    assert_eq!(
        decoded,
        Err(FramesError::InvalidFrame {
            offset_bytes: first.frame().len(),
            reason: FrameError::Incomplete {
                needed_bytes: BATCH_HEADER_BYTES,
                available_bytes: 7,
            },
        })
    );
}

/// A message is not a file: unwritten space after the last frame is an error there, not the end.
#[test]
fn frames_followed_by_zero_filled_space_are_refused() {
    let first = batch(100, &["a"]);
    let mut frames = first.frame().to_vec();
    frames.extend_from_slice(&[0; 64]);

    let decoded = decode_frames(SEGMENT, &Bytes::from(frames));

    assert_eq!(
        decoded,
        Err(FramesError::InvalidFrame {
            offset_bytes: first.frame().len(),
            reason: FrameError::EndOfData,
        })
    );
}

// The recovery scan.

#[test]
fn scan_of_intact_content_finds_every_batch() {
    let batches = random_batches(1, 8);

    let scanned = scan_valid_prefix(SEGMENT, batches[0].first_sequence(), &concatenated(&batches));

    assert_eq!(scanned.batches, batches);
}

#[test]
fn scan_of_intact_content_stops_at_the_end_of_data() {
    let batches = random_batches(1, 8);

    let scanned = scan_valid_prefix(SEGMENT, batches[0].first_sequence(), &concatenated(&batches));

    assert_eq!(scanned.stop, ScanStop::EndOfData);
}

#[test]
fn scan_of_intact_content_counts_every_byte_as_valid() {
    let batches = random_batches(1, 8);
    let content = concatenated(&batches);

    let scanned = scan_valid_prefix(SEGMENT, batches[0].first_sequence(), &content);

    assert_eq!(scanned.valid_bytes, content.len());
}

#[test]
fn scan_stops_at_unwritten_space() {
    let batches = random_batches(2, 3);
    let mut content = concatenated(&batches).to_vec();
    content.extend_from_slice(&[0_u8; 512]);

    let scanned = scan_valid_prefix(SEGMENT, batches[0].first_sequence(), &Bytes::from(content));

    assert_eq!(scanned.stop, ScanStop::EndOfData);
}

#[test]
fn scan_does_not_count_unwritten_space_as_valid() {
    let batches = random_batches(2, 3);
    let mut content = concatenated(&batches).to_vec();
    let written_bytes = content.len();
    content.extend_from_slice(&[0_u8; 512]);

    let scanned = scan_valid_prefix(SEGMENT, batches[0].first_sequence(), &Bytes::from(content));

    assert_eq!(scanned.valid_bytes, written_bytes);
}

#[test]
fn scan_stops_at_a_frame_that_does_not_continue_the_sequence() {
    let content = concatenated(&[batch(100, &["a", "b"]), batch(110, &["stale"])]);

    let scanned = scan_valid_prefix(SEGMENT, SequenceNumber::new(100), &content);

    assert_eq!(
        scanned.stop,
        ScanStop::SequenceGap {
            expected: SequenceNumber::new(102),
            found: SequenceNumber::new(110),
        }
    );
}

#[test]
fn scan_keeps_the_batches_before_a_sequence_gap() {
    let first = batch(100, &["a", "b"]);
    let content = concatenated(&[first.clone(), batch(110, &["stale"])]);

    let scanned = scan_valid_prefix(SEGMENT, SequenceNumber::new(100), &content);

    assert_eq!(scanned.batches, [first]);
}

#[test]
fn scan_refuses_content_that_starts_at_the_wrong_sequence() {
    let content = concatenated(&[batch(100, &["a"])]);

    let scanned = scan_valid_prefix(SEGMENT, SequenceNumber::new(99), &content);

    assert_eq!(scanned.batches.len(), 0);
}

#[test]
fn scan_of_a_cut_frame_says_how_many_bytes_the_frame_needs() {
    let first = batch(100, &["a"]);
    let cut = batch(101, &["cut short"]);
    let mut content = first.frame().to_vec();
    content.extend_from_slice(&cut.frame()[..cut.frame().len() - 1]);

    let scanned = scan_valid_prefix(SEGMENT, SequenceNumber::new(100), &Bytes::from(content));

    assert_eq!(
        scanned.stop,
        ScanStop::Incomplete {
            needed_bytes: cut.frame().len()
        }
    );
}

#[test]
fn scan_stops_at_a_frame_that_fails_its_checksum() {
    let mut content = concatenated(&[batch(100, &["a"]), batch(101, &["damaged"])]).to_vec();
    let last_byte = content.len() - 1;
    content[last_byte] ^= 0x01;

    let scanned = scan_valid_prefix(SEGMENT, SequenceNumber::new(100), &Bytes::from(content));

    assert!(matches!(
        scanned.stop,
        ScanStop::Invalid(FrameError::ChecksumMismatch { .. })
    ));
}

/// The property the design names for the recovery scan: any byte prefix of a valid file recovers to a
/// valid prefix of its batches.
#[test]
fn any_byte_prefix_recovers_to_a_prefix_of_the_batches() {
    for seed in 0..BYTE_PREFIX_SEEDS {
        let batches = random_batches(seed, 6);
        let content = concatenated(&batches);

        for prefix_bytes in 0..=content.len() {
            let scanned = scan_valid_prefix(SEGMENT, batches[0].first_sequence(), &content.slice(..prefix_bytes));

            let whole_batches = whole_batches_within(&batches, prefix_bytes);
            assert_eq!(
                scanned.batches,
                batches[..whole_batches],
                "seed {seed}, prefix of {prefix_bytes} bytes"
            );
        }
    }
}

/// A torn write can leave any mix of old and new bytes in the last frame. Whatever it leaves, the scan
/// must keep every batch before the damage and never return a batch that was not written.
#[test]
fn damage_never_invalidates_earlier_batches_or_invents_new_ones() {
    for seed in 0..DAMAGE_SEEDS {
        let mut random = DeterministicRandom::from_seed(seed);
        let batches = random_batches(seed, 6);
        let mut content = concatenated(&batches).to_vec();
        let damaged_byte = random.index(content.len());
        content[damaged_byte] ^= u8::try_from(random.between(1, 255)).expect("at most 255");

        let scanned = scan_valid_prefix(SEGMENT, batches[0].first_sequence(), &Bytes::from(content));

        let batches_before_damage = whole_batches_within(&batches, damaged_byte);
        assert_eq!(
            scanned.batches,
            batches[..batches_before_damage],
            "seed {seed}, damage at byte {damaged_byte}"
        );
    }
}

/// Fuzz-style check: no input, however hostile, makes the decoder panic or count bytes it was not given.
#[test]
fn arbitrary_bytes_never_panic_the_scanner() {
    for seed in 0..NOISE_SEEDS {
        let mut random = DeterministicRandom::from_seed(seed);
        let content_bytes = usize::try_from(random.between(0, 200)).expect("at most 200");
        let content = Bytes::from(random.bytes(content_bytes));

        let scanned = scan_valid_prefix(SEGMENT, SequenceNumber::FIRST, &content);

        assert!(scanned.valid_bytes <= content.len(), "seed {seed}");
    }
}
