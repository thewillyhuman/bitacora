//! Tests of the record format of the metrics store.
//!
//! # What is covered
//!
//! - A batch comes back from its payload exactly, including odd values and unordered samples.
//! - The bytes of a small record, held to what format version 1 writes.
//! - Batches that cannot be appended, and payloads that are not records, are refused with the reason.
//! - Damaged payloads never make the decoder panic or allocate what a broken count asks for.
//!
//! # How the fixtures work
//!
//! `series` builds one series from label pairs and `(timestamp, value)` pairs. `GOLDEN_RECORD` is the
//! payload format version 1 writes for one small batch, in hexadecimal.

use bitacora_types::DeterministicRandom;

use super::*;

/// One series with the given labels and samples.
fn series(label_pairs: &[(&str, &str)], samples: &[(i64, f64)]) -> SeriesSamples {
    let mut built_samples = Vec::new();
    for (timestamp_millis, value) in samples {
        built_samples.push(Sample {
            timestamp_millis: *timestamp_millis,
            value: *value,
        });
    }
    SeriesSamples {
        labels: Labels::from_pairs(label_pairs).expect("valid labels"),
        samples: built_samples,
    }
}

/// A batch of two series, three samples in all.
fn small_batch() -> Vec<SeriesSamples> {
    vec![
        series(&[("__name__", "up"), ("job", "api")], &[(1_000, 1.0), (2_000, 0.0)]),
        series(&[("__name__", "up"), ("job", "web")], &[(1_500, 1.0)]),
    ]
}

/// The payload of `series(&[("job", "a")], &[(1, 2.0)])` as format version 1 writes it: version, one
/// series, one label `job`=`a`, one sample at 1 with the bits of 2.0.
const GOLDEN_RECORD: &str = "01 01000000 0100 0300 6a6f62 0100 61 01000000 0100000000000000 0000000000000040";

/// Turns hexadecimal text, with any white space in it, into bytes.
fn bytes_of(hexadecimal: &str) -> Bytes {
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
    Bytes::from(bytes)
}

#[test]
fn batch_survives_the_round_trip() {
    let payload = encode_record(&small_batch()).expect("valid batch");

    assert_eq!(decode_record(&payload), Ok(small_batch()));
}

#[test]
fn record_is_written_as_in_format_version_1() {
    let payload = encode_record(&[series(&[("job", "a")], &[(1, 2.0)])]).expect("valid batch");

    assert_eq!(payload, bytes_of(GOLDEN_RECORD));
}

#[test]
fn record_of_format_version_1_is_read() {
    let batch = decode_record(&bytes_of(GOLDEN_RECORD));

    assert_eq!(batch, Ok(vec![series(&[("job", "a")], &[(1, 2.0)])]));
}

#[test]
fn samples_keep_the_order_they_were_given_in() {
    let unordered = vec![series(&[("job", "a")], &[(30, 3.0), (10, 1.0), (20, 2.0)])];

    let payload = encode_record(&unordered).expect("valid batch");

    assert_eq!(decode_record(&payload), Ok(unordered));
}

#[test]
fn odd_values_and_timestamps_come_back_bit_for_bit() {
    let odd = vec![series(
        &[("job", "a")],
        &[
            (i64::MIN, f64::NAN),
            (-1, f64::NEG_INFINITY),
            (0, -0.0),
            (1, f64::MIN_POSITIVE),
            (i64::MAX, f64::INFINITY),
        ],
    )];

    let payload = encode_record(&odd).expect("valid batch");

    assert_eq!(decode_record(&payload), Ok(odd));
}

#[test]
fn series_given_twice_stays_two_entries_of_the_record() {
    let repeated = vec![
        series(&[("job", "a")], &[(1, 1.0)]),
        series(&[("job", "a")], &[(2, 2.0)]),
    ];

    let payload = encode_record(&repeated).expect("valid batch");

    assert_eq!(decode_record(&payload), Ok(repeated));
}

#[test]
fn series_without_samples_is_kept_next_to_one_with_samples() {
    let batch = vec![series(&[("job", "a")], &[]), series(&[("job", "b")], &[(1, 1.0)])];

    let payload = encode_record(&batch).expect("valid batch");

    assert_eq!(decode_record(&payload), Ok(batch));
}

#[test]
fn encoding_is_deterministic() {
    let first = encode_record(&small_batch());
    let second = encode_record(&small_batch());

    assert_eq!(first, second);
}

#[test]
fn batch_without_series_is_refused() {
    assert_eq!(encode_record(&[]), Err(InvalidBatch::NoSamples));
}

#[test]
fn batch_whose_series_hold_no_sample_is_refused() {
    assert_eq!(
        encode_record(&[series(&[("job", "a")], &[])]),
        Err(InvalidBatch::NoSamples)
    );
}

#[test]
fn batch_larger_than_a_record_is_refused() {
    let sample_count = MAX_RECORD_PAYLOAD_BYTES / SAMPLE_BYTES + 1;
    let mut samples = Vec::with_capacity(sample_count);
    for timestamp_millis in 0..sample_count {
        samples.push(Sample {
            timestamp_millis: i64::try_from(timestamp_millis).expect("small"),
            value: 1.0,
        });
    }
    let huge = vec![SeriesSamples {
        labels: Labels::from_pairs(&[("job", "a")]).expect("valid labels"),
        samples,
    }];

    assert!(matches!(encode_record(&huge), Err(InvalidBatch::TooLarge { .. })));
}

#[test]
fn empty_payload_is_not_a_record() {
    assert_eq!(
        decode_record(&Bytes::new()),
        Err(MetricsRecordError::Malformed { reason: "it is empty" })
    );
}

#[test]
fn payload_of_another_format_version_is_refused() {
    let mut from_the_future = bytes_of(GOLDEN_RECORD).to_vec();
    from_the_future[0] = 2;

    assert_eq!(
        decode_record(&Bytes::from(from_the_future)),
        Err(MetricsRecordError::UnsupportedVersion { found: 2 })
    );
}

#[test]
fn payload_cut_short_anywhere_is_refused() {
    let whole = bytes_of(GOLDEN_RECORD);

    for kept_bytes in 1..whole.len() {
        let cut_short = whole.slice(..kept_bytes);
        assert!(
            matches!(decode_record(&cut_short), Err(MetricsRecordError::Malformed { .. })),
            "a record cut to {kept_bytes} bytes was accepted"
        );
    }
}

#[test]
fn payload_with_bytes_after_the_record_is_refused() {
    let mut with_a_tail = bytes_of(GOLDEN_RECORD).to_vec();
    with_a_tail.push(0);

    assert_eq!(
        decode_record(&Bytes::from(with_a_tail)),
        Err(MetricsRecordError::Malformed {
            reason: "it holds more than its counts say"
        })
    );
}

#[test]
fn sample_count_larger_than_the_payload_is_refused_without_allocating_it() {
    // Version, one series, one label job=a, then a sample count of four billion and nothing after it.
    let lying_count = bytes_of("01 01000000 0100 0300 6a6f62 0100 61 ffffffff");

    assert_eq!(
        decode_record(&lying_count),
        Err(MetricsRecordError::Malformed {
            reason: "it ends inside the samples"
        })
    );
}

#[test]
fn series_with_labels_out_of_order_is_refused() {
    // Two labels, `job` before `a`: not sorted.
    let unsorted = bytes_of("01 01000000 0200 0300 6a6f62 0100 61 0100 61 0100 62 00000000");

    assert!(matches!(
        decode_record(&unsorted),
        Err(MetricsRecordError::InvalidLabels(InvalidLabels::NotSorted { .. }))
    ));
}

#[test]
fn series_without_labels_is_refused() {
    let no_labels = bytes_of("01 01000000 0000 00000000");

    assert_eq!(
        decode_record(&no_labels),
        Err(MetricsRecordError::InvalidLabels(InvalidLabels::NoLabels))
    );
}

#[test]
fn label_that_is_not_utf8_is_refused() {
    // The name of the label is the single byte 0xff.
    let not_utf8 = bytes_of("01 01000000 0100 0100 ff 0100 61 00000000");

    assert_eq!(
        decode_record(&not_utf8),
        Err(MetricsRecordError::Malformed {
            reason: "a label is not UTF-8"
        })
    );
}

#[test]
fn decoder_never_panics_on_noise() {
    let mut random = DeterministicRandom::from_seed(1);

    for _round in 0..2_000 {
        let noise_bytes = usize::try_from(random.below(200)).expect("small");
        let mut noise = random.bytes(noise_bytes);
        // Half of the inputs get the right version, so that the rest of the decoder is reached.
        if let Some(first_byte) = noise.first_mut()
            && random.chance(1, 2)
        {
            *first_byte = RECORD_FORMAT_VERSION;
        }
        let _decoded_or_refused = decode_record(&Bytes::from(noise));
    }
}

#[test]
fn decoder_never_panics_on_a_damaged_record() {
    let mut random = DeterministicRandom::from_seed(2);
    let valid = encode_record(&small_batch()).expect("valid batch");

    for _round in 0..2_000 {
        let mut damaged = valid.to_vec();
        let position = random.index(damaged.len());
        damaged[position] ^= 1 << random.below(8);
        let _decoded_or_refused = decode_record(&Bytes::from(damaged));
    }
}
