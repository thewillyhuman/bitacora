//! Tests of the benchmark's reports.
//!
//! # What is covered
//!
//! - The arithmetic: rates from counts and elapsed time, also for a run too short to divide by, and
//!   percentiles from the histogram.
//! - The text of both reports, written out in full, with and without a pace and a failure.
//! - The field names of the JSON, which scripts depend on.
//!
//! # How the fixtures work
//!
//! Workloads and measurements are built by hand. `measurement_of` records a list of latencies given in
//! microseconds.

use bitacora::TakeoverPolicy;

use super::*;
use crate::workload::WorkloadLimit;

/// An append workload of single 1 KiB records, sixteen in flight, on one stream.
fn workload() -> AppendWorkload {
    AppendWorkload {
        stream: "orders".to_owned(),
        stream_count: 1,
        record_bytes: 1024,
        records_per_append: 1,
        appends_in_flight: 16,
        limit: WorkloadLimit::Duration(Duration::from_secs(2)),
        target_records_per_second: None,
        takeover: TakeoverPolicy::Force,
    }
}

/// The measurement of appends with the given latencies, in microseconds, over two seconds.
fn measurement_of(latencies_micros: &[u64]) -> AppendMeasurement {
    let mut latencies = LatencyHistogram::new();
    for micros in latencies_micros {
        latencies.record(Duration::from_micros(*micros));
    }
    let appends = latencies.count();
    AppendMeasurement {
        appends,
        records: appends,
        payload_bytes: appends * 1024,
        elapsed: Duration::from_secs(2),
        latencies,
        failure: None,
    }
}

#[test]
fn rates_are_counts_divided_by_the_elapsed_time() {
    let measured = measurement_of(&vec![10; 4_096]);

    let report = AppendReport::of(&workload(), &measured);

    assert_eq!(report.elapsed_milliseconds, 2_000);
    assert_eq!(report.records_per_second, 2_048);
    assert_eq!(report.bytes_per_second, 2 * 1024 * 1024);
}

#[test]
fn run_shorter_than_a_millisecond_reports_a_rate_of_zero() {
    let mut measured = measurement_of(&[10]);
    measured.elapsed = Duration::from_micros(900);

    let report = AppendReport::of(&workload(), &measured);

    assert_eq!(report.records_per_second, 0);
}

#[test]
fn percentiles_come_from_the_histogram() {
    let mut latencies_micros = vec![10; 990];
    latencies_micros.extend([20; 9]);
    latencies_micros.push(31);
    let measured = measurement_of(&latencies_micros);

    let report = AppendReport::of(&workload(), &measured);

    assert_eq!(
        report.latency_microseconds,
        LatencyReport {
            p50: 10,
            p90: 10,
            p99: 10,
            p999: 20,
            max: 31,
        }
    );
}

#[test]
fn report_states_what_was_asked_for() {
    let mut asked = workload();
    asked.stream_count = 4;
    asked.records_per_append = 8;
    asked.target_records_per_second = Some(50_000);

    let report = AppendReport::of(&asked, &measurement_of(&[10]));

    assert_eq!(report.streams, 4);
    assert_eq!(report.records_per_append, 8);
    assert_eq!(report.appends_in_flight, 16);
    assert_eq!(report.record_bytes, 1024);
    assert_eq!(report.target_records_per_second, Some(50_000));
}

#[test]
fn append_report_is_rendered_one_value_per_line() {
    let report = AppendReport::of(&workload(), &measurement_of(&vec![1_210; 4_096]));

    assert_eq!(
        report.render_text(),
        "append      stream orders, 1 stream(s), 16 appends in flight each, 1 record(s) of 1024 B per \
         append, as fast as the cluster commits\n\
         elapsed     2.000 s\n\
         appends     4096\n\
         records     4096 (2048 records/s)\n\
         throughput  2.0 MiB/s\n\
         latency     p50 1.210 ms  p90 1.210 ms  p99 1.210 ms  p99.9 1.210 ms  max 1.210 ms\n"
    );
}

#[test]
fn paced_workload_says_its_rate() {
    let mut asked = workload();
    asked.target_records_per_second = Some(500);

    let rendered = AppendReport::of(&asked, &measurement_of(&[10])).render_text();

    assert!(
        rendered.contains("paced to 500 records/s per stream\n"),
        "{rendered}"
    );
}

#[test]
fn workload_that_stopped_early_says_why_on_its_last_line() {
    let mut measured = measurement_of(&[10]);
    measured.failure = Some("an append failed: fenced".to_owned());

    let rendered = AppendReport::of(&workload(), &measured).render_text();

    assert!(
        rendered.ends_with("stopped     an append failed: fenced\n"),
        "{rendered}"
    );
}

#[test]
fn append_report_as_json_has_stable_field_names_with_units() {
    let report = AppendReport::of(&workload(), &measurement_of(&vec![1_210; 4_096]));

    let json = serde_json::to_value(&report).expect("a report serializes");

    assert_eq!(
        json,
        serde_json::json!({
            "workload": "append",
            "stream": "orders",
            "streams": 1,
            "record_bytes": 1024,
            "records_per_append": 1,
            "appends_in_flight": 16,
            "target_records_per_second": null,
            "elapsed_milliseconds": 2000,
            "appends": 4096,
            "records": 4096,
            "payload_bytes": 4_194_304,
            "records_per_second": 2048,
            "bytes_per_second": 2_097_152,
            "latency_microseconds": {"p50": 1210, "p90": 1210, "p99": 1210, "p999": 1210, "max": 1210},
            "failure": null
        })
    );
}

/// A scan of the stream `orders`, and what it measured: 3000 records of 1 KiB in a second and a half.
fn read_report() -> ReadReport {
    let asked = ReadWorkload {
        stream: "orders".to_owned(),
        from: None,
        limit: WorkloadLimit::Records(3_000),
    };
    let measured = ReadMeasurement {
        records: 3_000,
        payload_bytes: 3_000 * 1024,
        elapsed: Duration::from_millis(1_500),
        failure: None,
    };
    ReadReport::of(&asked, &measured)
}

#[test]
fn read_report_is_rendered_one_value_per_line() {
    assert_eq!(
        read_report().render_text(),
        "read        stream orders\n\
         elapsed     1.500 s\n\
         records     3000 (2000 records/s)\n\
         throughput  1.9 MiB/s\n"
    );
}

#[test]
fn read_report_as_json_has_stable_field_names_with_units() {
    let json = serde_json::to_value(read_report()).expect("a report serializes");

    assert_eq!(
        json,
        serde_json::json!({
            "workload": "read",
            "stream": "orders",
            "elapsed_milliseconds": 1500,
            "records": 3000,
            "payload_bytes": 3_072_000,
            "records_per_second": 2000,
            "bytes_per_second": 2_048_000,
            "failure": null
        })
    );
}

#[test]
fn seconds_are_rendered_with_three_decimals() {
    assert_eq!(format_seconds(30_004), "30.004 s");
}

#[test]
fn milliseconds_are_rendered_with_three_decimals() {
    assert_eq!(format_milliseconds(1_210), "1.210 ms");
}

#[test]
fn throughput_is_rendered_in_mebibytes_rounded_down() {
    assert_eq!(
        format_mebibytes_per_second(1024 * 1024 * 3 / 2),
        "1.5 MiB/s"
    );
}

#[test]
fn throughput_below_a_tenth_of_a_mebibyte_is_rendered_as_zero() {
    assert_eq!(format_mebibytes_per_second(1_000), "0.0 MiB/s");
}
