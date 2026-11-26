//! The metrics of a soak.
//!
//! # Why this file exists
//!
//! A soak that runs for a week next to a cluster should be watched the way the cluster is: by a Prometheus that
//! scrapes it, with a rule that fires when something is wrong. The log says everything, but nobody reads a log for a
//! week. This file is the handful of counters that let a dashboard show that the soak is alive and a rule say that
//! it is not clean.
//!
//! # What it contains
//!
//! - [`SoakMetrics`]: the counters, and the registry they are exposed from.
//!
//! # How it works
//!
//! The counters are raised once per round, from the round's report, so they move in steps of a round. They are
//! plain counters without labels, one per thing counted: the registry of `bitacora-process` offers those to other
//! crates, and a kind of fault or an outcome of an append is a short closed list.
//!
//! A run that ends because of a violation ends the process, and with it the metrics. A rule on
//! `bitacora_soak_rounds_violated_total` therefore has one scrape interval to see it; the rule to rely on is the one
//! on the Job that ran the tool having failed.
//!
//! # Invariants
//!
//! - Every metric here is listed in `docs/soak.md`; a test fails otherwise.
//! - The counters only ever grow, and only when a round has ended.
//!
//! # See also
//!
//! - `report.rs`: the report the counters are raised from.
//! - `bitacora-process/src/metrics.rs`: the registry and its naming scheme.
//! - `docs/soak.md`: the metrics, for whoever writes the rule.

use std::collections::BTreeMap;
use std::sync::Arc;

use bitacora_process::{Counter, MetricsRegistry};

use crate::faults::FaultKind;
use crate::report::{RoundReport, RoundVerdict};

/// The counters of a soak.
pub(crate) struct SoakMetrics {
    /// Where the counters are exposed from.
    registry: Arc<MetricsRegistry>,
    /// Rounds that were clean.
    rounds_clean: Counter,
    /// Rounds that broke an invariant.
    rounds_violated: Counter,
    /// Rounds after which the cluster did not recover.
    rounds_not_audited: Counter,
    /// Appends reported as committed.
    appends_committed: Counter,
    /// Appends that ended without their writer learning how.
    appends_unknown: Counter,
    /// Appends that were refused.
    appends_refused: Counter,
    /// Records handed to readers.
    records_read: Counter,
    /// Times a writer took a stream over.
    takeovers: Counter,
    /// Faults injected, by the name of their kind in a report.
    faults_injected: BTreeMap<&'static str, Counter>,
}

impl SoakMetrics {
    /// Registers the counters of a soak in a registry of their own.
    pub(crate) fn new() -> Self {
        let registry = Arc::new(MetricsRegistry::new());
        let mut faults_injected = BTreeMap::new();
        for kind in FaultKind::ALL {
            let name = format!("soak_faults_{}", kind.report_name());
            let help = format!("Faults of the kind {} that were injected", kind.report_name());
            faults_injected.insert(kind.report_name(), registry.counter(&name, &help));
        }
        Self {
            rounds_clean: registry.counter("soak_rounds_clean", "Rounds that were clean"),
            rounds_violated: registry.counter("soak_rounds_violated", "Rounds that broke an invariant"),
            rounds_not_audited: registry.counter(
                "soak_rounds_not_audited",
                "Rounds after which the cluster did not recover in time",
            ),
            appends_committed: registry.counter("soak_appends_committed", "Appends reported as committed"),
            appends_unknown: registry.counter(
                "soak_appends_unknown",
                "Appends that ended without their writer learning the outcome",
            ),
            appends_refused: registry.counter("soak_appends_refused", "Appends that were refused"),
            records_read: registry.counter("soak_records_read", "Records handed to readers"),
            takeovers: registry.counter("soak_takeovers", "Times a writer took a stream over"),
            faults_injected,
            registry,
        }
    }

    /// The registry the counters are exposed from, for the endpoint that serves them.
    pub(crate) fn registry(&self) -> Arc<MetricsRegistry> {
        Arc::clone(&self.registry)
    }

    /// Raises the counters by what the round that `report` describes did.
    pub(crate) fn count_round(&self, report: &RoundReport) {
        match report.verdict {
            RoundVerdict::Clean => self.rounds_clean.add(1),
            RoundVerdict::Violated => self.rounds_violated.add(1),
            RoundVerdict::NotAudited => self.rounds_not_audited.add(1),
            RoundVerdict::Interrupted => {}
        }
        self.appends_committed.add(report.appends_committed);
        self.appends_unknown.add(report.appends_unknown);
        self.appends_refused.add(report.appends_refused);
        self.records_read.add(report.records_read);
        self.takeovers.add(report.takeovers);
        for (kind, injected) in &report.faults {
            if let Some(counter) = self.faults_injected.get(kind) {
                counter.add(*injected);
            }
        }
    }
}

#[cfg(test)]
#[path = "metrics_test.rs"]
mod tests;
