//! Tests of the rule that picks the artifacts a checkpoint merges.
//!
//! # What is covered
//!
//! - Tiers: where the lowest one ends and how much wider each next one is.
//! - Runs: that only neighbours of one tier form a run, that a run must be as long as the fan-in, that the oldest
//!   run is picked, and that a fan-in below two counts as two.
//! - The default policy.
//!
//! # How the fixtures work
//!
//! [`POLICY`] merges three neighbours and ends its lowest tier at 100 bytes, so sizes are easy to read: below 100 is
//! tier 0, below 300 tier 1, below 900 tier 2. `catalog_of` builds a gapless catalog of one-record artifacts from
//! their sizes alone; the rule looks at nothing else.
//!
//! How merges change a catalog over many checkpoints is tested through the adapter, in `artifact_catalog_test.rs`.

use bitacora_types::{SequenceNumber, SequenceRange};
use bytes::Bytes;

use super::*;
use crate::artifact_catalog::ArtifactDescriptor;

/// The policy of these tests: three neighbours are merged, and the lowest tier ends at 100 bytes.
const POLICY: CompactionPolicy = CompactionPolicy {
    merge_fan_in: 3,
    smallest_tier_bytes: 100,
};

/// A gapless catalog with one artifact per size in `sizes_bytes`, each covering one record.
fn catalog_of(sizes_bytes: &[u64]) -> Vec<CatalogEntry> {
    let mut entries = Vec::new();
    let mut sequence = SequenceNumber::FIRST;
    for size_bytes in sizes_bytes {
        entries.push(CatalogEntry {
            covered_range: SequenceRange::new(sequence, sequence).expect("a range of one record"),
            artifact: ArtifactDescriptor {
                artifact_key: format!("artifact-of-record-{sequence}"),
                size_bytes: *size_bytes,
                summary: Bytes::new(),
            },
        });
        sequence = sequence.next();
    }
    entries
}

#[test]
fn artifacts_below_the_first_boundary_are_in_the_lowest_tier() {
    assert_eq!(POLICY.tier_of(0), 0);
    assert_eq!(POLICY.tier_of(99), 0);
}

#[test]
fn each_tier_is_as_many_times_wider_as_the_fan_in() {
    assert_eq!(POLICY.tier_of(100), 1);
    assert_eq!(POLICY.tier_of(299), 1);
    assert_eq!(POLICY.tier_of(300), 2);
}

#[test]
fn largest_sizes_share_the_highest_tier() {
    assert_eq!(POLICY.tier_of(u64::MAX), POLICY.tier_of(u64::MAX - 1));
}

#[test]
fn empty_catalog_has_nothing_to_merge() {
    let run = oldest_run_to_merge(&[], POLICY);

    assert_eq!(run, None);
}

#[test]
fn fewer_neighbours_of_one_tier_than_the_fan_in_are_left_alone() {
    let entries = catalog_of(&[10, 20]);

    let run = oldest_run_to_merge(&entries, POLICY);

    assert_eq!(run, None);
}

#[test]
fn as_many_neighbours_of_one_tier_as_the_fan_in_form_a_run() {
    let entries = catalog_of(&[10, 20, 30]);

    let run = oldest_run_to_merge(&entries, POLICY);

    assert_eq!(run, Some(0..3));
}

#[test]
fn run_is_no_longer_than_the_fan_in() {
    let entries = catalog_of(&[10, 20, 30, 40, 50]);

    let run = oldest_run_to_merge(&entries, POLICY);

    assert_eq!(run, Some(0..3));
}

#[test]
fn artifact_of_another_tier_breaks_a_run() {
    let entries = catalog_of(&[10, 20, 150, 30]);

    let run = oldest_run_to_merge(&entries, POLICY);

    assert_eq!(run, None);
}

#[test]
fn run_is_found_behind_larger_artifacts() {
    let entries = catalog_of(&[500, 10, 20, 30]);

    let run = oldest_run_to_merge(&entries, POLICY);

    assert_eq!(run, Some(1..4));
}

#[test]
fn oldest_of_two_runs_is_picked() {
    let entries = catalog_of(&[150, 160, 170, 10, 20, 30]);

    let run = oldest_run_to_merge(&entries, POLICY);

    assert_eq!(run, Some(0..3));
}

#[test]
fn fan_in_below_two_counts_as_two() {
    let merge_everything_alone = CompactionPolicy {
        merge_fan_in: 0,
        smallest_tier_bytes: 100,
    };
    let entries = catalog_of(&[10, 20, 30]);

    let run = oldest_run_to_merge(&entries, merge_everything_alone);

    assert_eq!(run, Some(0..2));
}

#[test]
fn default_policy_merges_four_neighbours() {
    assert_eq!(CompactionPolicy::default().merge_fan_in, 4);
}

#[test]
fn default_policy_ends_its_lowest_tier_at_a_mebibyte() {
    assert_eq!(CompactionPolicy::default().smallest_tier_bytes, 1024 * 1024);
}
