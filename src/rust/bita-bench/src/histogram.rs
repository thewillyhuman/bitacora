//! A latency histogram with a fixed number of buckets.
//!
//! # Why this file exists
//!
//! A benchmark measures every append, millions of them, and reports percentiles. Keeping every
//! measurement and sorting at the end costs memory in proportion to the run and allocates while the run is
//! being measured, which disturbs what is measured. This file counts measurements in buckets instead:
//! recording one is an index computation and an increment, and the memory used does not depend on how
//! long the benchmark runs.
//!
//! # What it contains
//!
//! - [`LatencyHistogram`]: records latencies, merges with another histogram, and answers quantiles.
//!
//! # How it works
//!
//! Latencies are counted in whole microseconds. Values below 32 microseconds have a bucket each. Above
//! that, every power of two is split into 32 buckets of equal width, so a bucket is never wider than
//! 1/32 of the values it holds: a reported quantile is at most 3.2 % above the true one. Values of
//! 2^36 microseconds, about 19 hours, or more share the last bucket; the largest value seen is also kept
//! exactly.
//!
//! A quantile is answered by walking the buckets until enough measurements are covered, and reporting the
//! upper bound of that bucket, capped at the largest value seen. Reporting the upper bound errs on the
//! side of saying an append was slower than it was, never faster.
//!
//! Each worker of a workload records into a histogram of its own, and the histograms are merged at the
//! end, so recording needs no lock.
//!
//! # Invariants
//!
//! - Recording never allocates.
//! - A quantile is never below the true value of that quantile and never more than 1/32 above it, for
//!   values below 2^36 microseconds.
//! - Merging two histograms gives the histogram of both sets of measurements.
//!
//! # See also
//!
//! - `workload.rs`: where latencies are measured.
//! - `report.rs`: which quantiles are reported.

use std::time::Duration;

/// How many buckets each power of two is split into, as a power of two.
const SUB_BUCKET_BITS: u32 = 5;
/// How many buckets each power of two is split into.
const SUB_BUCKETS: u64 = 1 << SUB_BUCKET_BITS;
/// The highest power of two that still has buckets of its own: values of 2^36 microseconds, about 19
/// hours, and more share the last bucket.
const HIGHEST_EXPONENT: u32 = 36;
/// How many groups of buckets there are: one for the values below [`SUB_BUCKETS`], which are exact, and
/// one for each power of two from 2^5 to 2^35.
const GROUPS: usize = (HIGHEST_EXPONENT - SUB_BUCKET_BITS + 1) as usize;
/// How many buckets a histogram has: every group has as many as a power of two is split into.
const BUCKETS: usize = GROUPS << SUB_BUCKET_BITS;
/// The largest number of microseconds that is counted in a bucket of its own range.
const LARGEST_COUNTED_MICROS: u64 = (1 << HIGHEST_EXPONENT) - 1;

/// Counts of latencies, by bucket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LatencyHistogram {
    /// How many latencies fell into each bucket.
    counts: Vec<u64>,
    /// How many latencies were recorded.
    total: u64,
    /// The largest latency recorded, in microseconds, exact.
    largest_micros: u64,
}

impl LatencyHistogram {
    /// Creates an empty histogram. This is the only allocation a histogram makes.
    pub(crate) fn new() -> Self {
        Self {
            counts: vec![0; BUCKETS],
            total: 0,
            largest_micros: 0,
        }
    }

    /// Records one latency. Does not allocate.
    ///
    /// # Arguments
    ///
    /// * `latency` - The latency to count. Fractions of a microsecond are dropped.
    pub(crate) fn record(&mut self, latency: Duration) {
        let micros = u64::try_from(latency.as_micros()).unwrap_or(u64::MAX);
        self.counts[bucket_of(micros)] += 1;
        self.total += 1;
        self.largest_micros = self.largest_micros.max(micros);
    }

    /// Adds every latency of `other` to this histogram.
    pub(crate) fn merge(&mut self, other: &Self) {
        for (count, other_count) in self.counts.iter_mut().zip(&other.counts) {
            *count += other_count;
        }
        self.total += other.total;
        self.largest_micros = self.largest_micros.max(other.largest_micros);
    }

    /// How many latencies were recorded.
    pub(crate) const fn count(&self) -> u64 {
        self.total
    }

    /// The largest latency recorded, exact to the microsecond. Zero if nothing was recorded.
    pub(crate) const fn largest(&self) -> Duration {
        Duration::from_micros(self.largest_micros)
    }

    /// The latency that the given share of the measurements did not exceed.
    ///
    /// # Arguments
    ///
    /// * `per_mille` - The share in thousandths: 500 for the median, 990 for the 99th percentile, 999 for
    ///   the 99.9th. Values above 1000 are read as 1000.
    ///
    /// # Returns
    ///
    /// The upper bound of the bucket that holds the measurement at that rank, capped at the largest
    /// latency recorded. Zero if nothing was recorded.
    pub(crate) fn quantile(&self, per_mille: u32) -> Duration {
        if self.total == 0 {
            return Duration::ZERO;
        }
        let per_mille = u128::from(per_mille.min(1_000));
        // The rank of the measurement asked for, counting from one and rounding up.
        let rank = (u128::from(self.total) * per_mille).div_ceil(1_000).max(1);
        let mut covered: u128 = 0;
        for (bucket, count) in self.counts.iter().enumerate() {
            covered += u128::from(*count);
            if covered >= rank {
                let upper_bound_micros = upper_bound_of(bucket).min(self.largest_micros);
                return Duration::from_micros(upper_bound_micros);
            }
        }
        self.largest()
    }
}

/// The bucket that counts a latency of `micros` microseconds.
fn bucket_of(micros: u64) -> usize {
    let micros = micros.min(LARGEST_COUNTED_MICROS);
    if micros < SUB_BUCKETS {
        // The first group has one bucket per value.
        return usize::try_from(micros).expect("below 32");
    }
    let exponent = u64::BITS - 1 - micros.leading_zeros();
    let shift = exponent - SUB_BUCKET_BITS;
    let group = u64::from(shift) + 1;
    let position_in_group = (micros >> shift) - SUB_BUCKETS;
    usize::try_from(group * SUB_BUCKETS + position_in_group).expect("below the number of buckets")
}

/// The largest number of microseconds that `bucket` counts.
fn upper_bound_of(bucket: usize) -> u64 {
    let bucket = bucket as u64;
    let group = bucket / SUB_BUCKETS;
    let position_in_group = bucket % SUB_BUCKETS;
    if group == 0 {
        return position_in_group;
    }
    let shift = group - 1;
    let lower_bound = (SUB_BUCKETS + position_in_group) << shift;
    lower_bound + (1 << shift) - 1
}

#[cfg(test)]
#[path = "histogram_test.rs"]
mod tests;
