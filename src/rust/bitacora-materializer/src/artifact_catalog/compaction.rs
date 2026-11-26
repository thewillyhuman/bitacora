//! When the small artifacts of a catalog are merged into larger ones.
//!
//! # Why this file exists
//!
//! A catalog that only ever grows by one artifact per checkpoint ends up with thousands of small files, and every
//! query pays for each of them. Merging fixes that, but merging everything at every checkpoint rewrites the same
//! bytes over and over. The rule that decides which artifacts are merged, and when, is what keeps both costs
//! bounded. It is a pure rule over the sizes in the catalog, and this file holds it by itself.
//!
//! # What it contains
//!
//! - [`CompactionPolicy`]: the two numbers a builder tunes the rule with.
//! - [`oldest_run_to_merge`]: the rule.
//!
//! # How it works
//!
//! This is size-tiered compaction. Every artifact is in a tier by its size: tier 0 below
//! [`CompactionPolicy::smallest_tier_bytes`], and one tier higher for every factor of
//! [`CompactionPolicy::merge_fan_in`] above that. A run is a stretch of neighbours in the catalog that are all in the
//! same tier. Once a run is `merge_fan_in` long, its artifacts are merged into one, which is larger and so lands in
//! a higher tier, where it waits for enough peers to be merged again.
//!
//! Only neighbours are merged, because the catalog is in log order and a merged artifact must cover one range
//! without a gap. The oldest run goes first, so that old data settles into large artifacts.
//!
//! # Invariants
//!
//! - Pure: the answer depends on the entries and the policy alone.
//! - A run is never shorter than two artifacts, whatever the policy says.
//! - An artifact is rewritten once per tier it passes through, so at most a logarithm of the catalog's size in times.
//!
//! # See also
//!
//! - `artifact_catalog.rs`: the adapter that asks at every checkpoint and merges at most one run.

use std::ops::Range;

use super::CatalogEntry;

/// How many neighbours are merged into one artifact, unless the builder says otherwise.
const DEFAULT_MERGE_FAN_IN: usize = 4;

/// Where the lowest size tier ends, unless the builder says otherwise: 1 MiB.
const DEFAULT_SMALLEST_TIER_BYTES: u64 = 1024 * 1024;

/// The shortest run that is merged, and the smallest factor between two tiers, whatever a policy asks for: merging
/// fewer than two artifacts would rewrite one for nothing.
const MIN_MERGE_FAN_IN: usize = 2;

/// When small artifacts are merged into larger ones.
///
/// # Examples
///
/// ```
/// use bitacora_materializer::CompactionPolicy;
///
/// let policy = CompactionPolicy {
///     merge_fan_in: 8,
///     ..CompactionPolicy::default()
/// };
/// assert_eq!(policy.smallest_tier_bytes, 1024 * 1024);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionPolicy {
    /// How many neighbouring artifacts of one size tier are merged into one. At least two.
    pub merge_fan_in: usize,
    /// Artifacts smaller than this are in the lowest tier. Each tier above holds artifacts up to
    /// `merge_fan_in` times larger than the one below.
    pub smallest_tier_bytes: u64,
}

impl Default for CompactionPolicy {
    /// Four neighbours are merged; the lowest tier ends at 1 MiB.
    fn default() -> Self {
        Self {
            merge_fan_in: DEFAULT_MERGE_FAN_IN,
            smallest_tier_bytes: DEFAULT_SMALLEST_TIER_BYTES,
        }
    }
}

impl CompactionPolicy {
    /// The size tier of an artifact: 0 below [`CompactionPolicy::smallest_tier_bytes`], and one more for
    /// every factor of [`CompactionPolicy::merge_fan_in`] above it.
    fn tier_of(&self, size_bytes: u64) -> u32 {
        let fan_in = u64::try_from(self.merge_fan_in.max(MIN_MERGE_FAN_IN)).unwrap_or(u64::MAX);
        let mut tier = 0;
        let mut tier_end = self.smallest_tier_bytes.max(1);
        while size_bytes >= tier_end {
            tier += 1;
            tier_end = tier_end.saturating_mul(fan_in);
            if tier_end == u64::MAX {
                break;
            }
        }
        tier
    }
}

/// Finds the oldest run of neighbours that is long enough to be merged.
///
/// # Arguments
///
/// * `entries` - The catalog, in log order.
/// * `policy` - The builder's compaction policy.
///
/// # Returns
///
/// The positions in `entries` of the oldest `merge_fan_in` neighbours that are all in the same size tier, or `None`
/// if no run is that long yet.
pub(super) fn oldest_run_to_merge(entries: &[CatalogEntry], policy: CompactionPolicy) -> Option<Range<usize>> {
    let merge_fan_in = policy.merge_fan_in.max(MIN_MERGE_FAN_IN);
    let mut run_start = 0;
    for position in 1..=entries.len() {
        let run_goes_on = position < entries.len()
            && policy.tier_of(entries[position].artifact.size_bytes)
                == policy.tier_of(entries[run_start].artifact.size_bytes);
        if position - run_start == merge_fan_in {
            return Some(run_start..position);
        }
        if !run_goes_on {
            run_start = position;
        }
    }
    None
}

#[cfg(test)]
#[path = "compaction_test.rs"]
mod tests;
