//! Tests of the controller daemon's metrics.
//!
//! # What is covered
//!
//! - What a round that led, a round that stood by and a round that failed each record.
//! - Every field of a pass report reaches its own counter.
//! - Every metric the daemon registers is listed in `docs/operations/metrics.md`.
//!
//! # How the fixtures work
//!
//! Rounds are made up: a `PassReport` with a different number in every field, so that a counter fed from
//! the wrong field shows. Metrics are read from the text exposition, as Prometheus reads them.

// The test reads the reference document from the repository.
#![allow(clippy::disallowed_methods)]

use bitacora_meta::StoreUnavailable;
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

/// A registry and the controller's metrics registered in it.
fn registered_metrics() -> (MetricsRegistry, ControllerMetrics) {
    let registry = MetricsRegistry::new();
    let metrics = ControllerMetrics::register(&registry);
    (registry, metrics)
}

/// The value of the exposition line that starts with `series`, if there is such a line.
fn exposition_value(registry: &MetricsRegistry, series: &str) -> Option<String> {
    for line in registry.encode().lines() {
        if let Some(value) = line.strip_prefix(series) {
            return Some(value.trim().to_owned());
        }
    }
    None
}

/// A pass report with a different number in every field.
fn report_with_distinct_values() -> PassReport {
    PassReport {
        standbys_placed: 1,
        copies_confirmed: 2,
        copies_marked_bad: 3,
        copies_repaired: 4,
        segments_tiered: 5,
        segments_released: 6,
        segments_deleted: 7,
        copies_removed: 8,
        segments_missing_copies: 9,
        streams_with_errors: 10,
    }
}

/// A round that led and produced [`report_with_distinct_values`].
fn led_round() -> Round {
    Round::Led(report_with_distinct_values())
}

/// A round that failed because the metadata store was away.
fn failed_round() -> Result<Round, MetaError> {
    Err(MetaError::Unavailable(StoreUnavailable {
        reason: "the test says so".to_owned(),
    }))
}

/// How long the made-up rounds took.
const ROUND_TOOK: Duration = Duration::from_millis(1_500);

#[test]
fn every_metric_of_the_daemon_is_in_the_reference() {
    let (registry, _metrics) = registered_metrics();

    let undocumented = undocumented_metrics(&registry.encode(), &read_reference());

    assert_eq!(undocumented, Vec::<String>::new());
}

#[test]
fn round_that_led_reports_leading() {
    let (registry, metrics) = registered_metrics();

    metrics.record_round(&Ok(led_round()), ROUND_TOOK);

    assert_eq!(
        exposition_value(&registry, "bitacora_controller_leading "),
        Some("1".to_owned())
    );
}

#[test]
fn round_that_stood_by_reports_not_leading() {
    let (registry, metrics) = registered_metrics();
    metrics.record_round(&Ok(led_round()), ROUND_TOOK);

    metrics.record_round(&Ok(Round::StoodBy), ROUND_TOOK);

    assert_eq!(
        exposition_value(&registry, "bitacora_controller_leading "),
        Some("0".to_owned())
    );
}

#[test]
fn round_that_failed_reports_not_leading() {
    let (registry, metrics) = registered_metrics();
    metrics.record_round(&Ok(led_round()), ROUND_TOOK);

    metrics.record_round(&failed_round(), ROUND_TOOK);

    assert_eq!(
        exposition_value(&registry, "bitacora_controller_leading "),
        Some("0".to_owned())
    );
}

#[test]
fn every_round_is_counted_whatever_its_outcome() {
    let (registry, metrics) = registered_metrics();

    metrics.record_round(&Ok(led_round()), ROUND_TOOK);
    metrics.record_round(&Ok(Round::StoodBy), ROUND_TOOK);
    metrics.record_round(&failed_round(), ROUND_TOOK);

    assert_eq!(
        exposition_value(&registry, "bitacora_controller_rounds_total "),
        Some("3".to_owned())
    );
}

#[test]
fn only_a_failed_round_is_counted_as_a_failure() {
    let (registry, metrics) = registered_metrics();

    metrics.record_round(&Ok(led_round()), ROUND_TOOK);
    metrics.record_round(&Ok(Round::StoodBy), ROUND_TOOK);
    metrics.record_round(&failed_round(), ROUND_TOOK);

    assert_eq!(
        exposition_value(&registry, "bitacora_controller_round_failures_total "),
        Some("1".to_owned())
    );
}

#[test]
fn duration_of_a_round_that_led_is_observed_as_the_pass_duration() {
    let (registry, metrics) = registered_metrics();

    metrics.record_round(&Ok(led_round()), ROUND_TOOK);

    assert_eq!(
        exposition_value(&registry, "bitacora_controller_pass_duration_seconds_sum "),
        Some("1.5".to_owned())
    );
}

#[test]
fn round_that_ran_no_pass_observes_no_pass_duration() {
    let (registry, metrics) = registered_metrics();

    metrics.record_round(&Ok(Round::StoodBy), ROUND_TOOK);
    metrics.record_round(&failed_round(), ROUND_TOOK);

    assert_eq!(
        exposition_value(
            &registry,
            "bitacora_controller_pass_duration_seconds_count "
        ),
        Some("0".to_owned())
    );
}

#[test]
fn segments_missing_copies_come_from_the_report() {
    let (registry, metrics) = registered_metrics();

    metrics.record_round(&Ok(led_round()), ROUND_TOOK);

    assert_eq!(
        exposition_value(&registry, "bitacora_controller_segments_missing_copies "),
        Some("9".to_owned())
    );
}

/// Only the leader knows; a standby must not go on showing what it saw when it led.
#[test]
fn standby_reports_no_segments_missing_copies() {
    let (registry, metrics) = registered_metrics();
    metrics.record_round(&Ok(led_round()), ROUND_TOOK);

    metrics.record_round(&Ok(Round::StoodBy), ROUND_TOOK);

    assert_eq!(
        exposition_value(&registry, "bitacora_controller_segments_missing_copies "),
        Some("0".to_owned())
    );
}

/// A metadata store that is briefly away must not make durability look fine.
#[test]
fn failed_round_keeps_the_last_known_segments_missing_copies() {
    let (registry, metrics) = registered_metrics();
    metrics.record_round(&Ok(led_round()), ROUND_TOOK);

    metrics.record_round(&failed_round(), ROUND_TOOK);

    assert_eq!(
        exposition_value(&registry, "bitacora_controller_segments_missing_copies "),
        Some("9".to_owned())
    );
}

#[test]
fn every_counter_of_the_report_reaches_its_own_metric() {
    let (registry, metrics) = registered_metrics();
    let counters_and_values = [
        ("standbys_placed", "1"),
        ("copies_confirmed", "2"),
        ("copies_marked_bad", "3"),
        ("copies_repaired", "4"),
        ("segments_tiered", "5"),
        ("segments_released", "6"),
        ("segments_deleted", "7"),
        ("copies_removed", "8"),
        ("stream_errors", "10"),
    ];

    metrics.record_round(&Ok(led_round()), ROUND_TOOK);

    for (counter, expected_value) in counters_and_values {
        assert_eq!(
            exposition_value(&registry, &format!("bitacora_controller_{counter}_total ")),
            Some(expected_value.to_owned()),
            "counter {counter}"
        );
    }
}

#[test]
fn counters_of_the_report_add_up_over_rounds() {
    let (registry, metrics) = registered_metrics();

    metrics.record_round(&Ok(led_round()), ROUND_TOOK);
    metrics.record_round(&Ok(led_round()), ROUND_TOOK);

    assert_eq!(
        exposition_value(&registry, "bitacora_controller_copies_repaired_total "),
        Some("8".to_owned())
    );
}
