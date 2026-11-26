//! The metrics the controller daemon records about its rounds.
//!
//! # Why this file exists
//!
//! The controller is what keeps a cluster healthy over time: it places segments, replaces lost copies,
//! tiers and deletes. An operator has to know that some instance is doing that, how long a pass takes,
//! and above all whether segments are short of copies, which is durability at risk (design, section 14,
//! "Observability"). The controller library has no metrics; it reports what each round did as plain
//! data. This file turns those reports into the metrics of the process.
//!
//! # What it contains
//!
//! - [`ControllerMetrics`]: the metrics, registered in the process's registry.
//! - [`ControllerMetrics::record_round`]: records the outcome of one round.
//!
//! # How it works
//!
//! | Metric | After a round that led | That stood by | That failed |
//! | --- | --- | --- | --- |
//! | `bitacora_controller_rounds_total` | +1 | +1 | +1 |
//! | `bitacora_controller_round_failures_total` | | | +1 |
//! | `bitacora_controller_leading` | 1 | 0 | 0 |
//! | `bitacora_controller_pass_duration_seconds` | observed | | |
//! | `bitacora_controller_segments_missing_copies` | from the report | 0 | unchanged |
//! | the counters of the pass report | + the report's values | | |
//!
//! The counters of the pass report are `standbys_placed`, `copies_confirmed`, `copies_marked_bad`,
//! `copies_repaired`, `segments_tiered`, `segments_released`, `segments_deleted`, `copies_removed`,
//! `metadata_snapshots_written` and `stream_errors`, each as `bitacora_controller_<name>_total`.
//!
//! Only the leader knows how many segments miss copies, so a standby reports zero and a dashboard takes
//! the maximum over the instances. After a failed round the last known value is kept: a metadata store
//! that is briefly away must not make durability look fine.
//!
//! An instance whose round failed does not know whether it still leads, and no pass ran, so it reports
//! that it is not leading. "No instance is leading" is therefore the alert for a cluster that nobody
//! looks after, whatever the cause.
//!
//! # Invariants
//!
//! - Every metric here is listed in `docs/operations/metrics.md`; a test fails otherwise.
//! - Recording never fails and never blocks.
//!
//! # See also
//!
//! - `daemon.rs`: the loop that calls [`ControllerMetrics::record_round`].
//! - `bitacora-controller/src/controller/report.rs`: `Round` and `PassReport`.

use std::time::Duration;

use bitacora_controller::{PassReport, Round};
use bitacora_meta::MetaError;
use bitacora_process::{Counter, DurationHistogram, Gauge, MetricsRegistry};

/// What the controller daemon records about its rounds. Clones share the same metrics.
#[derive(Clone, Debug)]
pub(crate) struct ControllerMetrics {
    /// Rounds that finished, whatever their outcome.
    rounds: Counter,
    /// Rounds that failed because the metadata store could not be used.
    round_failures: Counter,
    /// 1 if the last round led, 0 otherwise.
    leading: Gauge,
    /// How long rounds that led took: one pass over every stream.
    pass_duration: DurationHistogram,
    /// Sealed segments with fewer good copies than their stream asks for, as of the last pass.
    segments_missing_copies: Gauge,
    /// Standby segments placed.
    standbys_placed: Counter,
    /// Copies of sealed segments confirmed complete.
    copies_confirmed: Counter,
    /// Copies of sealed segments found short, damaged or lost.
    copies_marked_bad: Counter,
    /// Bad copies rebuilt from a good one.
    copies_repaired: Counter,
    /// Sealed segments uploaded to the object store.
    segments_tiered: Counter,
    /// Tiered segments left to the object store alone.
    segments_released: Counter,
    /// Segments below the low-water mark deleted.
    segments_deleted: Counter,
    /// Leftover segment copies removed from replicas.
    copies_removed: Counter,
    /// Snapshots of the metadata store written to the object store.
    metadata_snapshots_written: Counter,
    /// Streams a pass could not fully handle.
    stream_errors: Counter,
}

impl ControllerMetrics {
    /// Registers the controller's metrics in `registry`.
    ///
    /// # Arguments
    ///
    /// * `registry` - The registry of the process. Register once per registry.
    pub(crate) fn register(registry: &MetricsRegistry) -> Self {
        Self {
            rounds: registry.counter(
                "controller_rounds",
                "Rounds of the controller's loop that finished, whatever their outcome",
            ),
            round_failures: registry.counter(
                "controller_round_failures",
                "Rounds that failed because the metadata store could not be used",
            ),
            leading: registry.gauge(
                "controller_leading",
                "Whether the last round of this instance led (1) or not (0)",
            ),
            pass_duration: registry
                .duration_histogram("controller_pass_duration", "How long one pass over every stream took"),
            segments_missing_copies: registry.gauge(
                "controller_segments_missing_copies",
                "Sealed segments with fewer good copies than their stream asks for, as of the last pass",
            ),
            standbys_placed: registry.counter("controller_standbys_placed", "Standby segments placed"),
            copies_confirmed: registry.counter(
                "controller_copies_confirmed",
                "Copies of sealed segments confirmed complete",
            ),
            copies_marked_bad: registry.counter(
                "controller_copies_marked_bad",
                "Copies of sealed segments found short, damaged or lost",
            ),
            copies_repaired: registry.counter(
                "controller_copies_repaired",
                "Bad copies of sealed segments rebuilt from a good one",
            ),
            segments_tiered: registry.counter(
                "controller_segments_tiered",
                "Sealed segments uploaded to the object store",
            ),
            segments_released: registry.counter(
                "controller_segments_released",
                "Tiered segments left to the object store alone",
            ),
            segments_deleted: registry.counter(
                "controller_segments_deleted",
                "Segments below the low-water mark deleted",
            ),
            copies_removed: registry.counter(
                "controller_copies_removed",
                "Leftover segment copies removed from replicas",
            ),
            metadata_snapshots_written: registry.counter(
                "controller_metadata_snapshots_written",
                "Snapshots of the metadata store written to the object store",
            ),
            stream_errors: registry.counter(
                "controller_stream_errors",
                "Streams a pass could not fully handle; they are retried on the next pass",
            ),
        }
    }

    /// Records the outcome of one round of the controller's loop.
    ///
    /// # Arguments
    ///
    /// * `outcome` - What `Controller::run_round` returned.
    /// * `took` - How long the round took.
    pub(crate) fn record_round(&self, outcome: &Result<Round, MetaError>, took: Duration) {
        self.rounds.add(1);
        match outcome {
            Ok(Round::Led(report)) => {
                self.leading.set_flag(true);
                self.pass_duration.observe(took);
                self.record_pass(report);
            }
            Ok(Round::StoodBy) => {
                self.leading.set_flag(false);
                self.segments_missing_copies.set(0);
            }
            Err(_meta_error) => {
                self.round_failures.add(1);
                self.leading.set_flag(false);
            }
        }
    }

    /// Adds what one pass did to the counters and stores what it found in the gauge.
    fn record_pass(&self, report: &PassReport) {
        self.segments_missing_copies
            .set_unsigned(report.segments_missing_copies as u64);
        self.standbys_placed.add(report.standbys_placed as u64);
        self.copies_confirmed.add(report.copies_confirmed as u64);
        self.copies_marked_bad.add(report.copies_marked_bad as u64);
        self.copies_repaired.add(report.copies_repaired as u64);
        self.segments_tiered.add(report.segments_tiered as u64);
        self.segments_released.add(report.segments_released as u64);
        self.segments_deleted.add(report.segments_deleted as u64);
        self.copies_removed.add(report.copies_removed as u64);
        self.metadata_snapshots_written
            .add(report.metadata_snapshots_written as u64);
        self.stream_errors.add(report.streams_with_errors as u64);
    }
}

#[cfg(test)]
#[path = "metrics_test.rs"]
mod tests;
