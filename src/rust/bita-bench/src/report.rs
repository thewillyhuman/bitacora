//! What the benchmark prints.
//!
//! # Why this file exists
//!
//! A measurement is only useful if it can be read by a person at a terminal and compared by a script
//! between two builds, which is how the design tracks performance regressions (section 15). This file
//! turns a workload and its measurement into both forms, and does the arithmetic that goes with it:
//! rates from counts and elapsed time, and percentiles from the histogram.
//!
//! # What it contains
//!
//! - [`AppendReport`]: the result of an append workload.
//! - [`ReadReport`]: the result of a read workload.
//! - [`LatencyReport`]: the percentiles of append-to-commit latency.
//! - [`OutputFormat`]: text or JSON.
//!
//! # How it works
//!
//! A report is a plain structure of integers and text. JSON output is the structure as `serde` writes
//! it; its field names are the tool's interface and carry their units. Text output is one value per line.
//!
//! All arithmetic is in integers: milliseconds, microseconds, bytes. Rates are rounded down. A workload
//! that ran for less than a millisecond reports a rate of zero instead of dividing by zero.
//!
//! # Invariants
//!
//! - A report states only what was measured and what was asked for. It judges nothing: no target, no
//!   pass or fail.
//! - Building and rendering a report are pure.
//!
//! # See also
//!
//! - `workload.rs`: where the measurements come from.
//! - `histogram.rs`: how exact the percentiles are.

use std::fmt::Write as _;
use std::time::Duration;

use clap::ValueEnum;
use serde::Serialize;

use crate::histogram::LatencyHistogram;
use crate::workload::{AppendMeasurement, AppendWorkload, ReadMeasurement, ReadWorkload};

/// How a report is printed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum OutputFormat {
    /// One value per line, for a person.
    Text,
    /// JSON with stable field names, for scripts.
    Json,
}

/// How many bytes one mebibyte has.
const BYTES_PER_MEBIBYTE: u64 = 1024 * 1024;

/// The percentiles of a latency distribution, in microseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct LatencyReport {
    /// Half of the appends committed within this time.
    pub(crate) p50: u64,
    /// Nine in ten appends committed within this time.
    pub(crate) p90: u64,
    /// 99 in 100 appends committed within this time.
    pub(crate) p99: u64,
    /// 999 in 1000 appends committed within this time.
    pub(crate) p999: u64,
    /// The slowest append, exact.
    pub(crate) max: u64,
}

impl LatencyReport {
    /// Reads the reported percentiles off a histogram.
    fn of(latencies: &LatencyHistogram) -> Self {
        Self {
            p50: micros_of(latencies.quantile(500)),
            p90: micros_of(latencies.quantile(900)),
            p99: micros_of(latencies.quantile(990)),
            p999: micros_of(latencies.quantile(999)),
            max: micros_of(latencies.largest()),
        }
    }

    /// The percentiles on one line, in milliseconds with three decimals.
    fn render_text(&self) -> String {
        format!(
            "p50 {}  p90 {}  p99 {}  p99.9 {}  max {}",
            format_milliseconds(self.p50),
            format_milliseconds(self.p90),
            format_milliseconds(self.p99),
            format_milliseconds(self.p999),
            format_milliseconds(self.max)
        )
    }
}

/// A duration in whole microseconds.
fn micros_of(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// A duration in whole milliseconds.
fn millis_of(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// How many of `count` happened per second over `elapsed_milliseconds`, rounded down. Zero if no whole
/// millisecond passed.
fn per_second(count: u64, elapsed_milliseconds: u64) -> u64 {
    if elapsed_milliseconds == 0 {
        return 0;
    }
    let per_second = u128::from(count) * 1_000 / u128::from(elapsed_milliseconds);
    u64::try_from(per_second).unwrap_or(u64::MAX)
}

/// The result of an append workload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct AppendReport {
    /// Always `append`.
    pub(crate) workload: String,
    /// The stream, or the common part of the stream names if there were several.
    pub(crate) stream: String,
    /// How many streams were written at once.
    pub(crate) streams: u64,
    /// The payload size of every record.
    pub(crate) record_bytes: u64,
    /// How many records one append carried.
    pub(crate) records_per_append: u64,
    /// How many appends per stream were in flight.
    pub(crate) appends_in_flight: u64,
    /// The rate each stream was paced to, in records per second, if it was paced.
    pub(crate) target_records_per_second: Option<u64>,
    /// How long the workload ran.
    pub(crate) elapsed_milliseconds: u64,
    /// How many appends committed.
    pub(crate) appends: u64,
    /// How many records committed.
    pub(crate) records: u64,
    /// How many payload bytes committed.
    pub(crate) payload_bytes: u64,
    /// Records committed per second, over all streams.
    pub(crate) records_per_second: u64,
    /// Payload bytes committed per second, over all streams.
    pub(crate) bytes_per_second: u64,
    /// The append-to-commit latency of the appends that committed.
    pub(crate) latency_microseconds: LatencyReport,
    /// Why the workload stopped early, if it did.
    pub(crate) failure: Option<String>,
}

impl AppendReport {
    /// Builds the report of an append workload from what was asked and what was measured.
    pub(crate) fn of(workload: &AppendWorkload, measured: &AppendMeasurement) -> Self {
        let elapsed_milliseconds = millis_of(measured.elapsed);
        Self {
            workload: "append".to_owned(),
            stream: workload.stream.clone(),
            streams: workload.stream_count,
            record_bytes: workload.record_bytes as u64,
            records_per_append: workload.records_per_append as u64,
            appends_in_flight: workload.appends_in_flight as u64,
            target_records_per_second: workload.target_records_per_second,
            elapsed_milliseconds,
            appends: measured.appends,
            records: measured.records,
            payload_bytes: measured.payload_bytes,
            records_per_second: per_second(measured.records, elapsed_milliseconds),
            bytes_per_second: per_second(measured.payload_bytes, elapsed_milliseconds),
            latency_microseconds: LatencyReport::of(&measured.latencies),
            failure: measured.failure.clone(),
        }
    }

    /// Renders the report as text, one value per line.
    pub(crate) fn render_text(&self) -> String {
        let records = format!("{} ({} records/s)", self.records, self.records_per_second);
        let throughput = format_mebibytes_per_second(self.bytes_per_second);
        let mut rendered = String::new();
        let _ = writeln!(rendered, "append      {}", self.workload_text());
        let _ = writeln!(rendered, "elapsed     {}", format_seconds(self.elapsed_milliseconds));
        let _ = writeln!(rendered, "appends     {}", self.appends);
        let _ = writeln!(rendered, "records     {records}");
        let _ = writeln!(rendered, "throughput  {throughput}");
        let _ = writeln!(rendered, "latency     {}", self.latency_microseconds.render_text());
        if let Some(failure) = &self.failure {
            let _ = writeln!(rendered, "stopped     {failure}");
        }
        rendered
    }

    /// What was asked for, as one phrase: the streams, the appends in flight, the size of an append and
    /// the pace.
    fn workload_text(&self) -> String {
        let pace = match self.target_records_per_second {
            Some(rate) => format!("paced to {rate} records/s per stream"),
            None => "as fast as the cluster commits".to_owned(),
        };
        format!(
            "stream {}, {} stream(s), {} appends in flight each, {} record(s) of {} B per append, {pace}",
            self.stream, self.streams, self.appends_in_flight, self.records_per_append, self.record_bytes
        )
    }
}

/// The result of a read workload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ReadReport {
    /// Always `read`.
    pub(crate) workload: String,
    /// The stream that was read.
    pub(crate) stream: String,
    /// How long the scan ran.
    pub(crate) elapsed_milliseconds: u64,
    /// How many records were read.
    pub(crate) records: u64,
    /// How many payload bytes were read.
    pub(crate) payload_bytes: u64,
    /// Records read per second.
    pub(crate) records_per_second: u64,
    /// Payload bytes read per second.
    pub(crate) bytes_per_second: u64,
    /// Why the scan stopped early, if it did.
    pub(crate) failure: Option<String>,
}

impl ReadReport {
    /// Builds the report of a read workload from what was asked and what was measured.
    pub(crate) fn of(workload: &ReadWorkload, measured: &ReadMeasurement) -> Self {
        let elapsed_milliseconds = millis_of(measured.elapsed);
        Self {
            workload: "read".to_owned(),
            stream: workload.stream.clone(),
            elapsed_milliseconds,
            records: measured.records,
            payload_bytes: measured.payload_bytes,
            records_per_second: per_second(measured.records, elapsed_milliseconds),
            bytes_per_second: per_second(measured.payload_bytes, elapsed_milliseconds),
            failure: measured.failure.clone(),
        }
    }

    /// Renders the report as text, one value per line.
    pub(crate) fn render_text(&self) -> String {
        let mut rendered = String::new();
        let _ = writeln!(rendered, "read        stream {}", self.stream);
        let _ = writeln!(rendered, "elapsed     {}", format_seconds(self.elapsed_milliseconds));
        let _ = writeln!(
            rendered,
            "records     {} ({} records/s)",
            self.records, self.records_per_second
        );
        let _ = writeln!(
            rendered,
            "throughput  {}",
            format_mebibytes_per_second(self.bytes_per_second)
        );
        if let Some(failure) = &self.failure {
            let _ = writeln!(rendered, "stopped     {failure}");
        }
        rendered
    }
}

/// Renders milliseconds as seconds with three decimals: `30.004 s`.
fn format_seconds(milliseconds: u64) -> String {
    format!("{}.{:03} s", milliseconds / 1_000, milliseconds % 1_000)
}

/// Renders microseconds as milliseconds with three decimals: `1.210 ms`.
fn format_milliseconds(microseconds: u64) -> String {
    format!("{}.{:03} ms", microseconds / 1_000, microseconds % 1_000)
}

/// Renders bytes per second as mebibytes per second with one decimal, rounded down: `40.1 MiB/s`.
fn format_mebibytes_per_second(bytes_per_second: u64) -> String {
    let whole = bytes_per_second / BYTES_PER_MEBIBYTE;
    let tenths = (bytes_per_second % BYTES_PER_MEBIBYTE) * 10 / BYTES_PER_MEBIBYTE;
    format!("{whole}.{tenths} MiB/s")
}

#[cfg(test)]
#[path = "report_test.rs"]
mod tests;
