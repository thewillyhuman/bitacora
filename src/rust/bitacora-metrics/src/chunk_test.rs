//! Tests of the chunk encoding.
//!
//! # What is covered
//!
//! - Bits written are the bits read, across byte boundaries and at every width.
//! - Any samples come back bit for bit: none, one, steady, irregular, out of order, at the limits of
//!   timestamps and with every odd kind of value. Thousands of random chunks confirm it.
//! - Each timestamp code is used for exactly its range, and regular series are small.
//! - Bytes that are not a chunk are refused without a panic.
//!
//! # How the fixtures work
//!
//! `sample` builds one sample. `random_samples` draws samples from the workspace's seeded generator, in
//! one of several shapes (steady, jittered, wild), so a failure names a seed and replays exactly.

use bitacora_types::DeterministicRandom;

use super::*;

/// A sample at `timestamp_millis` with the value `value`.
fn sample(timestamp_millis: i64, value: f64) -> Sample {
    Sample {
        timestamp_millis,
        value,
    }
}

/// `sample_count` samples every `interval_millis` from `start_millis`, all with the value `value`.
fn steady_samples(start_millis: i64, interval_millis: i64, sample_count: i64, value: f64) -> Vec<Sample> {
    let mut samples = Vec::new();
    for position in 0..sample_count {
        samples.push(sample(start_millis + position * interval_millis, value));
    }
    samples
}

/// Samples of a random shape: a steady counter, a jittered gauge, or timestamps and values with nothing
/// in common.
fn random_samples(random: &mut DeterministicRandom) -> Vec<Sample> {
    let sample_count = random.below(60);
    let shape = random.below(3);
    let mut samples = Vec::new();
    #[allow(clippy::cast_possible_wrap)]
    let mut timestamp_millis = random.next_u64() as i64 / 4;
    let mut value = 100.0_f64;
    for _sample in 0..sample_count {
        match shape {
            0 => {
                timestamp_millis += 15_000;
                value += 1.0;
            }
            1 => {
                timestamp_millis += 15_000 + i64::try_from(random.below(2_000)).expect("small") - 1_000;
                #[allow(clippy::cast_precision_loss)]
                let change = random.below(1_000) as f64 / 7.0;
                value += change;
            }
            _wild => {
                #[allow(clippy::cast_possible_wrap)]
                let wild_timestamp = random.next_u64() as i64;
                timestamp_millis = wild_timestamp;
                value = f64::from_bits(random.next_u64());
            }
        }
        samples.push(sample(timestamp_millis, value));
    }
    samples
}

#[test]
fn bits_written_across_byte_boundaries_are_read_back() {
    let mut writer = BitWriter::after(Vec::new());
    writer.write_bits(0b101, 3);
    writer.write_bits(0x1234_5678_9abc_def0, 64);
    writer.write_bits(0b1, 1);
    writer.write_bits(0x3ff, 10);
    let bytes = writer.into_bytes();

    let mut reader = BitReader {
        bytes: &bytes,
        position_bits: 0,
    };

    assert_eq!(reader.read_bits(3), Some(0b101));
    assert_eq!(reader.read_bits(64), Some(0x1234_5678_9abc_def0));
    assert_eq!(reader.read_bits(1), Some(0b1));
    assert_eq!(reader.read_bits(10), Some(0x3ff));
}

#[test]
fn every_width_of_bits_is_read_back() {
    for bit_count in 1..=64_u8 {
        let value = if bit_count == 64 {
            u64::MAX
        } else {
            (1_u64 << bit_count) - 1
        };
        let mut writer = BitWriter::after(Vec::new());
        // An odd offset, so that no write starts at a byte boundary.
        writer.write_bits(0b1, 1);
        writer.write_bits(value, bit_count);
        let bytes = writer.into_bytes();

        let mut reader = BitReader {
            bytes: &bytes,
            position_bits: 1,
        };

        assert_eq!(reader.read_bits(bit_count), Some(value), "{bit_count} bits");
    }
}

#[test]
fn only_the_low_bits_asked_for_are_written() {
    let mut writer = BitWriter::after(Vec::new());
    writer.write_bits(0xffff, 4);
    writer.write_bits(0, 4);

    assert_eq!(writer.into_bytes(), [0b1111_0000]);
}

#[test]
fn reading_past_the_end_returns_nothing() {
    let mut reader = BitReader {
        bytes: &[0xff],
        position_bits: 0,
    };

    assert_eq!(reader.read_bits(9), None);
}

#[test]
fn reading_exactly_to_the_end_succeeds() {
    let mut reader = BitReader {
        bytes: &[0xff],
        position_bits: 0,
    };

    assert_eq!(reader.read_bits(8), Some(0xff));
}

#[test]
fn chunk_without_samples_is_its_count_alone() {
    assert_eq!(encode_chunk(&[]), [0, 0, 0, 0]);
}

#[test]
fn chunk_without_samples_decodes_to_nothing() {
    assert_eq!(decode_chunk(&encode_chunk(&[])), Ok(Vec::new()));
}

#[test]
fn chunk_of_one_sample_comes_back() {
    let samples = [sample(1_700_000_000_000, 42.5)];

    assert_eq!(decode_chunk(&encode_chunk(&samples)), Ok(samples.to_vec()));
}

#[test]
fn steady_samples_come_back() {
    let samples = steady_samples(1_700_000_000_000, 15_000, 500, 7.0);

    assert_eq!(decode_chunk(&encode_chunk(&samples)), Ok(samples));
}

#[test]
fn steady_series_with_a_constant_value_costs_two_bits_a_sample() {
    let samples = steady_samples(1_700_000_000_000, 15_000, 1_001, 7.0);

    let chunk = encode_chunk(&samples);

    // The count, the first sample in full, the second sample's first delta, then two bits for each of
    // the 999 that follow.
    let expected_bits = 4 * 8 + 128 + (3 + 17 + 1) + 999 * 2;
    assert_eq!(chunk.len(), usize::div_ceil(expected_bits, 8));
}

#[test]
fn samples_out_of_time_order_come_back_in_the_order_given() {
    let samples = [sample(3_000, 3.0), sample(1_000, 1.0), sample(2_000, 2.0)];

    assert_eq!(decode_chunk(&encode_chunk(&samples)), Ok(samples.to_vec()));
}

#[test]
fn timestamps_at_the_limits_come_back() {
    let samples = [
        sample(i64::MIN, 1.0),
        sample(i64::MAX, 2.0),
        sample(0, 3.0),
        sample(i64::MIN, 4.0),
        sample(-1, 5.0),
    ];

    assert_eq!(decode_chunk(&encode_chunk(&samples)), Ok(samples.to_vec()));
}

#[test]
fn odd_values_come_back_bit_for_bit() {
    let odd_values = [
        0.0,
        -0.0,
        1.0,
        -1.0,
        f64::NAN,
        f64::from_bits(0x7ff8_0000_0000_0001),
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::MIN_POSITIVE,
        f64::MAX,
        f64::MIN,
        f64::EPSILON,
        f64::from_bits(1),
        f64::from_bits(u64::MAX),
    ];
    let mut samples = Vec::new();
    for (position, value) in odd_values.iter().enumerate() {
        samples.push(sample(i64::try_from(position).expect("small") * 1_000, *value));
    }

    assert_eq!(decode_chunk(&encode_chunk(&samples)), Ok(samples));
}

#[test]
fn value_that_differs_in_its_lowest_bit_only_comes_back() {
    let samples = [
        sample(1_000, f64::from_bits(0x4000_0000_0000_0000)),
        sample(2_000, f64::from_bits(0x4000_0000_0000_0001)),
        sample(3_000, f64::from_bits(0x4000_0000_0000_0000)),
    ];

    assert_eq!(decode_chunk(&encode_chunk(&samples)), Ok(samples.to_vec()));
}

#[test]
fn value_that_differs_in_all_64_bits_comes_back() {
    let samples = [
        sample(1_000, f64::from_bits(0)),
        sample(2_000, f64::from_bits(u64::MAX)),
    ];

    assert_eq!(decode_chunk(&encode_chunk(&samples)), Ok(samples.to_vec()));
}

#[test]
fn each_short_timestamp_code_reaches_exactly_its_range() {
    for bit_count in SHORT_DOD_BITS {
        let reach = 1_i64 << (bit_count - 1);

        assert!(fits_in_bits(reach, bit_count), "{bit_count} bits, upper end");
        assert!(
            !fits_in_bits(reach + 1, bit_count),
            "{bit_count} bits, past the upper end"
        );
        assert!(fits_in_bits(-(reach - 1), bit_count), "{bit_count} bits, lower end");
        assert!(!fits_in_bits(-reach, bit_count), "{bit_count} bits, past the lower end");
    }
}

#[test]
fn changes_of_interval_at_the_edge_of_every_code_come_back() {
    let mut edges = vec![0_i64, 1, -1];
    for bit_count in SHORT_DOD_BITS {
        let reach = 1_i64 << (bit_count - 1);
        edges.extend([reach, reach + 1, -(reach - 1), -reach]);
    }
    for delta_of_delta in edges {
        // The second sample's delta is its delta of delta; the third repeats the interval.
        let samples = [
            sample(1_000_000_000, 1.0),
            sample(1_000_000_000 + delta_of_delta, 1.0),
            sample(1_000_000_000 + 2 * delta_of_delta, 1.0),
        ];

        assert_eq!(
            decode_chunk(&encode_chunk(&samples)),
            Ok(samples.to_vec()),
            "a change of interval of {delta_of_delta}"
        );
    }
}

#[test]
fn random_chunks_come_back() {
    let mut random = DeterministicRandom::from_seed(7);

    for round in 0..3_000 {
        let samples = random_samples(&mut random);

        assert_eq!(decode_chunk(&encode_chunk(&samples)), Ok(samples), "round {round}");
    }
}

#[test]
fn encoding_is_deterministic() {
    let samples = steady_samples(0, 1_000, 50, 3.25);

    assert_eq!(encode_chunk(&samples), encode_chunk(&samples));
}

#[test]
fn decoder_hands_out_samples_one_at_a_time_and_then_nothing() {
    let chunk = encode_chunk(&[sample(1_000, 1.0), sample(2_000, 2.0)]);
    let mut decoder = ChunkDecoder::new(&chunk).expect("valid chunk");

    let first = decoder.next_sample();
    let second = decoder.next_sample();
    let after_the_last = decoder.next_sample();

    assert_eq!(first, Ok(Some(sample(1_000, 1.0))));
    assert_eq!(second, Ok(Some(sample(2_000, 2.0))));
    assert_eq!(after_the_last, Ok(None));
}

#[test]
fn only_samples_in_the_time_range_are_decoded_into_the_output() {
    let chunk = encode_chunk(&steady_samples(1_000, 1_000, 10, 1.0));
    let range = TimeRange::new(3_000, 5_000).expect("valid range");
    let mut output = Vec::new();

    decode_chunk_in_range(&chunk, range, &mut output).expect("valid chunk");

    assert_eq!(output, steady_samples(3_000, 1_000, 3, 1.0));
}

#[test]
fn range_decoding_appends_to_what_the_output_holds() {
    let chunk = encode_chunk(&[sample(2_000, 2.0)]);
    let mut output = vec![sample(1_000, 1.0)];

    decode_chunk_in_range(&chunk, TimeRange::ALL, &mut output).expect("valid chunk");

    assert_eq!(output, [sample(1_000, 1.0), sample(2_000, 2.0)]);
}

#[test]
fn bytes_too_short_for_a_count_are_refused() {
    assert_eq!(
        decode_chunk(&[1, 0, 0]),
        Err(DamagedChunk {
            reason: "it is too short to hold its sample count"
        })
    );
}

#[test]
fn count_larger_than_the_bytes_can_hold_is_refused() {
    let lying_count = [0xff, 0xff, 0xff, 0xff, 1, 2, 3];

    assert_eq!(
        decode_chunk(&lying_count),
        Err(DamagedChunk {
            reason: "it states more samples than its bytes can hold"
        })
    );
}

#[test]
fn chunk_cut_short_is_refused() {
    let whole = encode_chunk(&random_samples_of_a_wild_shape());

    for kept_bytes in SAMPLE_COUNT_BYTES..whole.len() {
        let decoded = decode_chunk(&whole[..kept_bytes]);
        assert!(decoded.is_err(), "a chunk cut to {kept_bytes} bytes was accepted");
    }
}

/// Twenty samples whose timestamps and values have nothing in common, so that every sample takes whole
/// bytes and cutting the chunk anywhere removes part of one.
fn random_samples_of_a_wild_shape() -> Vec<Sample> {
    let mut random = DeterministicRandom::from_seed(11);
    let mut samples = Vec::new();
    for _sample in 0..20 {
        #[allow(clippy::cast_possible_wrap)]
        let timestamp_millis = random.next_u64() as i64;
        samples.push(sample(timestamp_millis, f64::from_bits(random.next_u64())));
    }
    samples
}

#[test]
fn value_that_reuses_a_window_never_stated_is_refused() {
    // One count of two, a first sample of zeros, then a zero bit for the timestamp and the bits `10`:
    // "same window as before", before any window was stated.
    let mut writer = BitWriter::after(2_u32.to_le_bytes().to_vec());
    writer.write_bits(0, 64);
    writer.write_bits(0, 64);
    writer.write_bits(0b0, 1);
    writer.write_bits(0b10, 2);
    writer.write_bits(0, 13);

    assert_eq!(
        decode_chunk(&writer.into_bytes()),
        Err(DamagedChunk {
            reason: "a value refers to a window that was never stated"
        })
    );
}

#[test]
fn value_stated_wider_than_64_bits_is_refused() {
    // A new window with 31 leading zeros and a length of 63 bits: 94 bits in all.
    let mut writer = BitWriter::after(2_u32.to_le_bytes().to_vec());
    writer.write_bits(0, 64);
    writer.write_bits(0, 64);
    writer.write_bits(0b0, 1);
    writer.write_bits(0b11, 2);
    writer.write_bits(31, 5);
    writer.write_bits(63, 6);
    writer.write_bits(0, 64);

    assert_eq!(
        decode_chunk(&writer.into_bytes()),
        Err(DamagedChunk {
            reason: "a value is stated to be wider than 64 bits"
        })
    );
}

#[test]
fn decoder_never_panics_on_noise() {
    let mut random = DeterministicRandom::from_seed(3);

    for _round in 0..5_000 {
        let noise_bytes = usize::try_from(random.below(120)).expect("small");
        let mut noise = random.bytes(noise_bytes);
        // A small count, so that the decoder gets past its first check and into the bit stream.
        if noise.len() >= SAMPLE_COUNT_BYTES {
            let small_count = u32::try_from(random.below(40)).expect("small");
            noise[..SAMPLE_COUNT_BYTES].copy_from_slice(&small_count.to_le_bytes());
        }
        let _decoded_or_refused = decode_chunk(&noise);
    }
}

#[test]
fn decoder_never_panics_on_a_chunk_with_flipped_bits() {
    let mut random = DeterministicRandom::from_seed(5);

    for _round in 0..3_000 {
        let mut damaged = encode_chunk(&random_samples(&mut random));
        let position = random.index(damaged.len());
        damaged[position] ^= 1 << random.below(8);
        let _decoded_or_refused = decode_chunk(&damaged);
    }
}
