//! Tests of the latency histogram.
//!
//! # What is covered
//!
//! - Small values are exact; large values are within the promised 1/32 above the true value and never
//!   below it.
//! - Quantiles at their edges: an empty histogram, one measurement, the median of an even count, shares
//!   of zero and of more than everything.
//! - The largest value is exact, also beyond what the buckets resolve.
//! - Merging equals recording everything into one histogram.
//! - The bucket layout: every bucket's bounds are consistent with where values are counted.
//!
//! # How the fixtures work
//!
//! `histogram_of` records a list of latencies given in microseconds. The accuracy test draws values from
//! a seeded generator, so a failure names values that reproduce it.

use bitacora_types::DeterministicRandom;

use super::*;

/// A histogram that recorded the given latencies, in microseconds.
fn histogram_of(latencies_micros: &[u64]) -> LatencyHistogram {
    let mut histogram = LatencyHistogram::new();
    for micros in latencies_micros {
        histogram.record(Duration::from_micros(*micros));
    }
    histogram
}

#[test]
fn empty_histogram_reports_zero() {
    let histogram = LatencyHistogram::new();

    assert_eq!(histogram.count(), 0);
    assert_eq!(histogram.quantile(990), Duration::ZERO);
    assert_eq!(histogram.largest(), Duration::ZERO);
}

#[test]
fn single_measurement_is_every_quantile() {
    let histogram = histogram_of(&[17]);

    assert_eq!(histogram.quantile(1), Duration::from_micros(17));
    assert_eq!(histogram.quantile(500), Duration::from_micros(17));
    assert_eq!(histogram.quantile(1_000), Duration::from_micros(17));
}

#[test]
fn values_below_thirty_two_microseconds_are_exact() {
    let histogram = histogram_of(&[1, 2, 3, 4, 31]);

    assert_eq!(histogram.quantile(1_000), Duration::from_micros(31));
    assert_eq!(histogram.quantile(600), Duration::from_micros(3));
}

#[test]
fn median_of_an_even_count_is_the_lower_middle_value() {
    let histogram = histogram_of(&[10, 20, 30, 40]);

    assert_eq!(histogram.quantile(500), Duration::from_micros(20));
}

#[test]
fn ninety_ninth_percentile_ignores_the_slowest_hundredth() {
    let mut latencies = vec![10; 99];
    latencies.push(1_000_000);
    let histogram = histogram_of(&latencies);

    assert_eq!(histogram.quantile(990), Duration::from_micros(10));
}

#[test]
fn share_of_zero_is_the_smallest_measurement() {
    let histogram = histogram_of(&[5, 9, 30]);

    assert_eq!(histogram.quantile(0), Duration::from_micros(5));
}

#[test]
fn share_above_everything_is_read_as_everything() {
    let histogram = histogram_of(&[5, 9, 30]);

    assert_eq!(histogram.quantile(5_000), Duration::from_micros(30));
}

#[test]
fn largest_value_is_exact_although_its_bucket_is_wide() {
    let histogram = histogram_of(&[1_000_003]);

    assert_eq!(histogram.largest(), Duration::from_micros(1_000_003));
    assert_eq!(histogram.quantile(1_000), Duration::from_micros(1_000_003));
}

#[test]
fn value_beyond_the_counted_range_is_kept_as_the_largest() {
    let two_days = Duration::from_hours(48);
    let mut histogram = LatencyHistogram::new();
    histogram.record(two_days);

    assert!(two_days > Duration::from_micros(LARGEST_COUNTED_MICROS));
    assert_eq!(histogram.largest(), two_days);
    assert_eq!(histogram.count(), 1);
}

#[test]
fn fractions_of_a_microsecond_are_dropped() {
    let mut histogram = LatencyHistogram::new();
    histogram.record(Duration::from_nanos(2_999));

    assert_eq!(histogram.largest(), Duration::from_micros(2));
}

/// The promise of the file header, checked on many values: a reported quantile is never below the true
/// value and at most 1/32 above it.
#[test]
fn quantile_of_a_single_value_is_within_a_thirty_second_above_it() {
    let mut random = DeterministicRandom::from_seed(7);
    for _sample in 0..10_000 {
        let micros = random.between(1, 60_000_000_000);
        let mut histogram = histogram_of(&[micros]);
        // A second, larger value, so that the cap at the largest value does not hide the bucket's width.
        histogram.record(Duration::from_micros(LARGEST_COUNTED_MICROS + 1));

        let reported = u64::try_from(histogram.quantile(500).as_micros()).expect("fits");

        assert!(reported >= micros, "{micros} was reported as {reported}");
        assert!(reported - micros <= micros / 32, "{micros} was reported as {reported}");
    }
}

#[test]
fn merged_histogram_equals_one_that_recorded_everything() {
    let mut merged = histogram_of(&[1, 50, 7_000]);
    merged.merge(&histogram_of(&[2, 900_000]));

    assert_eq!(merged, histogram_of(&[1, 50, 7_000, 2, 900_000]));
}

#[test]
fn merging_an_empty_histogram_changes_nothing() {
    let mut histogram = histogram_of(&[1, 50, 7_000]);
    histogram.merge(&LatencyHistogram::new());

    assert_eq!(histogram, histogram_of(&[1, 50, 7_000]));
}

#[test]
fn every_bucket_counts_the_values_up_to_its_upper_bound() {
    for bucket in 0..BUCKETS {
        let upper_bound = upper_bound_of(bucket);
        assert_eq!(bucket_of(upper_bound), bucket, "bucket {bucket}");
    }
}

#[test]
fn value_right_above_a_bucket_is_counted_in_the_next_one() {
    for bucket in 0..BUCKETS - 1 {
        let first_value_of_the_next = upper_bound_of(bucket) + 1;
        assert_eq!(bucket_of(first_value_of_the_next), bucket + 1, "bucket {bucket}");
    }
}

#[test]
fn last_bucket_ends_at_the_largest_counted_value() {
    assert_eq!(upper_bound_of(BUCKETS - 1), LARGEST_COUNTED_MICROS);
}
