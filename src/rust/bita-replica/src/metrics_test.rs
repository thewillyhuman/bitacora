//! Tests of the replica daemon's own metrics.
//!
//! # What is covered
//!
//! - Every metric the daemon registers for itself is listed in `docs/operations/metrics.md`.
//! - The metrics are exposed under the names the reference gives.
//!
//! # How the fixtures work
//!
//! The reference is read from the repository. Bazel runs tests from the root of the repository's files
//! and Cargo from the crate's directory, so `read_reference` tries both locations.

// The test reads the reference document from the repository.
#![allow(clippy::disallowed_methods)]

use bitacora_process::undocumented_metrics;

use super::*;

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

/// The metrics reference is an interface for dashboards and alerts: a metric must not ship without its
/// entry.
#[test]
fn every_metric_of_the_daemon_is_in_the_reference() {
    let registry = MetricsRegistry::new();
    let _metrics = ReplicaDaemonMetrics::register(&registry);

    let undocumented = undocumented_metrics(&registry.encode(), &read_reference());

    assert_eq!(undocumented, Vec::<String>::new());
}

#[test]
fn held_copies_are_exposed_as_a_gauge_of_the_replica() {
    let registry = MetricsRegistry::new();
    let metrics = ReplicaDaemonMetrics::register(&registry);

    metrics.segment_copies.set_unsigned(12);

    assert!(
        registry
            .encode()
            .contains("bitacora_replica_segment_copies 12\n"),
        "{}",
        registry.encode()
    );
}

#[test]
fn failed_registrations_are_exposed_as_a_counter_of_the_replica() {
    let registry = MetricsRegistry::new();
    let metrics = ReplicaDaemonMetrics::register(&registry);

    metrics.registration_failures.add(2);

    assert!(
        registry
            .encode()
            .contains("bitacora_replica_registration_failures_total 2\n"),
        "{}",
        registry.encode()
    );
}
