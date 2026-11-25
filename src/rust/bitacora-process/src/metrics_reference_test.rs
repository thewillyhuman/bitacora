//! Tests of the check that keeps the metrics reference true, and the check itself for this crate.
//!
//! # What is covered
//!
//! - `undocumented_metrics` finds a metric the reference does not mention, of each type, and accepts the
//!   ones it mentions.
//! - Every metric the decorators of this crate register is listed in `docs/operations/metrics.md`.
//!
//! # How the fixtures work
//!
//! The reference is read from the repository. Bazel runs tests from the root of the repository's files
//! and Cargo from the crate's directory, so `read_reference` tries both locations.

// The test reads the reference document from the repository.
#![allow(clippy::disallowed_methods)]

use std::sync::Arc;

use bitacora_artifact::MemoryArtifactStore;
use bitacora_meta::MemoryKeyValueStore;
use bitacora_segment::MemoryDisk;
use bitacora_types::replica_api::UnreachableReplica;

use super::*;
use crate::{
    InstrumentedArtifactStore, InstrumentedDisk, InstrumentedKeyValueStore, InstrumentedReplica,
    MetricsRegistry,
};

/// Where the reference lies, seen from where Bazel and Cargo run the tests.
const REFERENCE_LOCATIONS: [&str; 2] = [
    "docs/operations/metrics.md",
    "../../../docs/operations/metrics.md",
];

/// Reads `docs/operations/metrics.md`.
fn read_reference() -> String {
    for location in REFERENCE_LOCATIONS {
        if let Ok(reference) = std::fs::read_to_string(location) {
            return reference;
        }
    }
    panic!("the metrics reference is at none of {REFERENCE_LOCATIONS:?}");
}

/// A registry with one metric of each label-free type.
fn registry_with_one_metric_of_each_type() -> MetricsRegistry {
    let registry = MetricsRegistry::new();
    let _counter = registry.counter("widget_turns", "Turns of the widget");
    let _gauge = registry.gauge("widget_queue_length", "Requests waiting for the widget");
    let _histogram = registry.duration_histogram("widget_turn_duration", "How long a turn takes");
    registry
}

#[test]
fn metric_the_reference_mentions_is_documented() {
    let registry = registry_with_one_metric_of_each_type();
    let reference = "`bitacora_widget_turns_total` `bitacora_widget_queue_length` \
                     `bitacora_widget_turn_duration_seconds`";

    let undocumented = undocumented_metrics(&registry.encode(), reference);

    assert_eq!(undocumented, Vec::<String>::new());
}

#[test]
fn counter_is_looked_up_with_its_total_suffix() {
    let registry = registry_with_one_metric_of_each_type();
    let reference_without_the_suffix = "`bitacora_widget_turns` `bitacora_widget_queue_length` \
                                        `bitacora_widget_turn_duration_seconds`";

    let undocumented = undocumented_metrics(&registry.encode(), reference_without_the_suffix);

    assert_eq!(undocumented, ["bitacora_widget_turns_total"]);
}

#[test]
fn gauge_the_reference_does_not_mention_is_found() {
    let registry = registry_with_one_metric_of_each_type();
    let reference = "`bitacora_widget_turns_total` `bitacora_widget_turn_duration_seconds`";

    let undocumented = undocumented_metrics(&registry.encode(), reference);

    assert_eq!(undocumented, ["bitacora_widget_queue_length"]);
}

#[test]
fn histogram_the_reference_does_not_mention_is_found() {
    let registry = registry_with_one_metric_of_each_type();
    let reference = "`bitacora_widget_turns_total` `bitacora_widget_queue_length`";

    let undocumented = undocumented_metrics(&registry.encode(), reference);

    assert_eq!(undocumented, ["bitacora_widget_turn_duration_seconds"]);
}

#[test]
fn name_that_is_only_part_of_a_longer_documented_name_is_not_documented() {
    let registry = registry_with_one_metric_of_each_type();
    let reference = "`bitacora_widget_turns_total` `bitacora_widget_queue_length_bytes` \
                     `bitacora_widget_turn_duration_seconds`";

    let undocumented = undocumented_metrics(&registry.encode(), reference);

    assert_eq!(undocumented, ["bitacora_widget_queue_length"]);
}

#[test]
fn empty_registry_has_nothing_undocumented() {
    let registry = MetricsRegistry::new();

    let undocumented = undocumented_metrics(&registry.encode(), "");

    assert_eq!(undocumented, Vec::<String>::new());
}

/// The reference is an interface for dashboards and alerts: a metric must not ship without its entry.
#[test]
fn every_metric_of_the_decorators_is_in_the_reference() {
    let registry = MetricsRegistry::new();
    let _disk = InstrumentedDisk::new(Arc::new(MemoryDisk::new()), &registry);
    let _replica =
        InstrumentedReplica::new(Arc::new(UnreachableReplica::new("never called")), &registry);
    let _metadata_store =
        InstrumentedKeyValueStore::new(Arc::new(MemoryKeyValueStore::new()), &registry);
    let _object_store =
        InstrumentedArtifactStore::new(Arc::new(MemoryArtifactStore::new()), &registry);

    let undocumented = undocumented_metrics(&registry.encode(), &read_reference());

    assert_eq!(undocumented, Vec::<String>::new());
}
