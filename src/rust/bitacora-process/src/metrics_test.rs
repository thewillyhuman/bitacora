//! Tests of the metrics registry and of the request metrics every decorator records.
//!
//! # What is covered
//!
//! - The naming scheme: the prefix, the unit and the `_total` suffix appear in the exposition.
//! - Each label-free metric type records what it is given.
//! - `RequestMetrics::observe` returns the request's result unchanged and records the request, its
//!   duration and, for a failure, its kind.
//!
//! # How the fixtures work
//!
//! Tests read metrics the way Prometheus does: from the text exposition. `exposition_line` finds the
//! line of one time series. Time is paused, so a request that sleeps takes exactly as long as it says.

use super::*;

/// A failure with a kind, as the wrapped interfaces return. Not `Copy`, like the real ones, which carry
/// text.
#[derive(Debug, PartialEq, Eq)]
enum TestFailure {
    /// The request was refused.
    Refused,
}

/// Names the kind of a [`TestFailure`].
fn kind_of_test_failure(failure: &TestFailure) -> &'static str {
    match failure {
        TestFailure::Refused => "refused",
    }
}

/// The value at the end of the exposition line that starts with `series`, if there is such a line.
fn exposition_value(registry: &MetricsRegistry, series: &str) -> Option<String> {
    for line in registry.encode().lines() {
        if let Some(value) = line.strip_prefix(series) {
            return Some(value.trim().to_owned());
        }
    }
    None
}

/// A registry with the request metrics of a subsystem called `widget`.
fn registry_with_request_metrics() -> (MetricsRegistry, RequestMetrics) {
    let registry = MetricsRegistry::new();
    let metrics = RequestMetrics::register(&registry, "widget", "Requests to the widget");
    (registry, metrics)
}

#[test]
fn counter_is_exposed_with_the_prefix_and_the_total_suffix() {
    let registry = MetricsRegistry::new();
    let counter = registry.counter("widget_turns", "Turns of the widget");

    counter.add(3);

    assert_eq!(
        exposition_value(&registry, "bitacora_widget_turns_total "),
        Some("3".to_owned())
    );
}

#[test]
fn counter_adds_up() {
    let registry = MetricsRegistry::new();
    let counter = registry.counter("widget_turns", "Turns of the widget");

    counter.add(3);
    counter.add(4);

    assert_eq!(counter.value(), 7);
}

#[test]
fn gauge_holds_the_last_value_it_was_set_to() {
    let registry = MetricsRegistry::new();
    let gauge = registry.gauge("widget_queue_length", "Requests waiting for the widget");

    gauge.set(9);
    gauge.set(4);

    assert_eq!(
        exposition_value(&registry, "bitacora_widget_queue_length "),
        Some("4".to_owned())
    );
}

#[test]
fn gauge_flag_is_one_when_set() {
    let registry = MetricsRegistry::new();
    let gauge = registry.gauge("widget_leading", "Whether the widget leads");

    gauge.set_flag(true);

    assert_eq!(gauge.value(), 1);
}

#[test]
fn gauge_flag_is_zero_when_cleared() {
    let registry = MetricsRegistry::new();
    let gauge = registry.gauge("widget_leading", "Whether the widget leads");
    gauge.set_flag(true);

    gauge.set_flag(false);

    assert_eq!(gauge.value(), 0);
}

#[test]
fn gauge_stores_a_value_too_large_for_it_as_its_largest() {
    let registry = MetricsRegistry::new();
    let gauge = registry.gauge("widget_size", "The size of the widget");

    gauge.set_unsigned(u64::MAX);

    assert_eq!(gauge.value(), i64::MAX);
}

#[test]
fn bytes_gauge_is_exposed_with_its_unit() {
    let registry = MetricsRegistry::new();
    let gauge = registry.bytes_gauge("widget_free", "Unused space of the widget");

    gauge.set_unsigned(4_096);

    assert_eq!(
        exposition_value(&registry, "bitacora_widget_free_bytes "),
        Some("4096".to_owned())
    );
}

#[test]
fn bytes_counter_is_exposed_with_its_unit_before_the_total_suffix() {
    let registry = MetricsRegistry::new();
    let counter = registry.bytes_counter("widget_written", "Bytes written to the widget");

    counter.add(512);

    assert_eq!(
        exposition_value(&registry, "bitacora_widget_written_bytes_total "),
        Some("512".to_owned())
    );
}

#[test]
fn duration_histogram_is_exposed_in_seconds() {
    let registry = MetricsRegistry::new();
    let histogram = registry.duration_histogram("widget_turn_duration", "How long a turn takes");

    histogram.observe(Duration::from_millis(1_500));

    assert_eq!(
        exposition_value(&registry, "bitacora_widget_turn_duration_seconds_sum "),
        Some("1.5".to_owned())
    );
}

#[test]
fn duration_histogram_counts_its_observations() {
    let registry = MetricsRegistry::new();
    let histogram = registry.duration_histogram("widget_turn_duration", "How long a turn takes");

    histogram.observe(Duration::from_millis(2));
    histogram.observe(Duration::from_millis(3));

    assert_eq!(
        exposition_value(&registry, "bitacora_widget_turn_duration_seconds_count "),
        Some("2".to_owned())
    );
}

#[test]
fn duration_buckets_reach_from_a_fast_flush_to_a_slow_upload() {
    let registry = MetricsRegistry::new();
    let histogram = registry.duration_histogram("widget_turn_duration", "How long a turn takes");

    histogram.observe(Duration::from_secs(100));

    assert_eq!(
        exposition_value(
            &registry,
            "bitacora_widget_turn_duration_seconds_bucket{le=\"104.8576\"} "
        ),
        Some("1".to_owned())
    );
}

#[test]
fn exposition_ends_with_the_end_of_file_marker() {
    let registry = MetricsRegistry::new();

    let exposition = registry.encode();

    assert!(exposition.ends_with("# EOF\n"), "{exposition}");
}

#[tokio::test(start_paused = true)]
async fn successful_request_is_returned_unchanged() {
    let (_registry, metrics) = registry_with_request_metrics();

    let outcome = metrics
        .observe(
            "turn",
            async { Ok::<u32, TestFailure>(7) },
            kind_of_test_failure,
        )
        .await;

    assert_eq!(outcome, Ok(7));
}

#[tokio::test(start_paused = true)]
async fn failed_request_is_returned_unchanged() {
    let (_registry, metrics) = registry_with_request_metrics();

    let outcome = metrics
        .observe(
            "turn",
            async { Err::<u32, TestFailure>(TestFailure::Refused) },
            kind_of_test_failure,
        )
        .await;

    assert_eq!(outcome, Err(TestFailure::Refused));
}

#[tokio::test(start_paused = true)]
async fn request_is_counted_under_its_operation() {
    let (registry, metrics) = registry_with_request_metrics();

    let _outcome = metrics
        .observe(
            "turn",
            async { Ok::<u32, TestFailure>(7) },
            kind_of_test_failure,
        )
        .await;

    assert_eq!(
        exposition_value(
            &registry,
            "bitacora_widget_requests_total{operation=\"turn\"} "
        ),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn failed_request_is_counted_as_a_request_too() {
    let (registry, metrics) = registry_with_request_metrics();

    let _outcome = metrics
        .observe(
            "turn",
            async { Err::<u32, TestFailure>(TestFailure::Refused) },
            kind_of_test_failure,
        )
        .await;

    assert_eq!(
        exposition_value(
            &registry,
            "bitacora_widget_requests_total{operation=\"turn\"} "
        ),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn failed_request_is_counted_under_its_operation_and_kind() {
    let (registry, metrics) = registry_with_request_metrics();

    let _outcome = metrics
        .observe(
            "turn",
            async { Err::<u32, TestFailure>(TestFailure::Refused) },
            kind_of_test_failure,
        )
        .await;

    assert_eq!(
        exposition_value(
            &registry,
            "bitacora_widget_request_failures_total{operation=\"turn\",error=\"refused\"} "
        ),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn successful_request_is_not_counted_as_a_failure() {
    let (registry, metrics) = registry_with_request_metrics();

    let _outcome = metrics
        .observe(
            "turn",
            async { Ok::<u32, TestFailure>(7) },
            kind_of_test_failure,
        )
        .await;

    assert!(
        !registry
            .encode()
            .contains("bitacora_widget_request_failures_total{"),
        "{}",
        registry.encode()
    );
}

#[tokio::test(start_paused = true)]
async fn request_duration_is_the_time_the_request_took() {
    let (registry, metrics) = registry_with_request_metrics();
    let slow_request = async {
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok::<u32, TestFailure>(7)
    };

    let _outcome = metrics
        .observe("turn", slow_request, kind_of_test_failure)
        .await;

    assert_eq!(
        exposition_value(
            &registry,
            "bitacora_widget_request_duration_seconds_sum{operation=\"turn\"} "
        ),
        Some("2.0".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn requests_of_two_operations_are_counted_apart() {
    let (registry, metrics) = registry_with_request_metrics();

    let _first_outcome = metrics
        .observe(
            "turn",
            async { Ok::<u32, TestFailure>(7) },
            kind_of_test_failure,
        )
        .await;
    let _second_outcome = metrics
        .observe(
            "stop",
            async { Ok::<u32, TestFailure>(7) },
            kind_of_test_failure,
        )
        .await;

    assert_eq!(
        exposition_value(
            &registry,
            "bitacora_widget_requests_total{operation=\"stop\"} "
        ),
        Some("1".to_owned())
    );
}
