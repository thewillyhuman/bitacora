//! What a soak says about itself: one line per round and one for the run.
//!
//! # Why this file exists
//!
//! A soak runs for days with nobody watching. Whoever comes back to it, a person or a script, has only its log to
//! tell whether the week was clean, and if not, which round broke and how. So the log has to be made for that: one
//! line of JSON per round, with stable names, that says what was done to the cluster, what the clients saw and how
//! the round was judged, and one line at the end for the whole run. This file defines those lines.
//!
//! # What it contains
//!
//! - [`RoundVerdict`] and [`RunVerdict`]: how a round and a run ended.
//! - [`RoundReport`]: the line of a round.
//! - [`Summary`]: the line of a run, added up from its rounds.
//! - [`HistoryCounts`]: what a history holds, counted.
//! - [`json_line`]: a report as one line of text.
//!
//! # How it works
//!
//! The counts of a round come from the histories of its streams, the same events the checker judges: how many
//! appends were reported as committed, how many ended without their writer knowing how, how many were refused, how
//! many records readers were handed, and the longest time between an append's call and its commit. Faults and
//! takeovers are counted by those who make them. A summary adds the rounds up and keeps the verdict of the run.
//!
//! Every line carries a `kind`, `round` or `summary`, so that a script can pick the lines it wants out of a log that
//! may hold other text.
//!
//! # Invariants
//!
//! - The field names are an interface: scripts read them. A name only changes together with `docs/soak.md`.
//! - An append that never returned is counted as of unknown outcome, like one that returned saying so.
//! - A round's counts are those of its own histories; nothing is carried over from the round before.
//!
//! # See also
//!
//! - `round.rs`: builds the report of a round.
//! - `soak.rs`: builds the summary and prints both.
//! - `docs/soak.md`: the fields, for whoever reads the log.

use std::collections::BTreeMap;
use std::time::Duration;

use bitacora_sim::{AppendOutcome, Event, TimedEvent};
use serde::Serialize;

/// How a round ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RoundVerdict {
    /// Every stream was read back whole and the checker found nothing.
    Clean,
    /// The checker found a broken invariant.
    Violated,
    /// The cluster did not recover in time, so not every stream could be read back whole. Nothing is known about
    /// whether an invariant was broken.
    NotAudited,
    /// The tool was told to stop before the round was over.
    Interrupted,
}

/// How a run ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RunVerdict {
    /// The run lasted as long as it was asked to and every round was clean.
    Clean,
    /// A round broke an invariant. The run ended there.
    Violated,
    /// The cluster did not recover after a round's faults were removed. The run ended there.
    NotRecovered,
    /// The tool was told to stop. Every round that finished was clean.
    Interrupted,
    /// The run could not begin: the cluster could not be reached or had no replica.
    CouldNotStart,
}

/// What the histories of a round hold.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct HistoryCounts {
    /// Appends reported as committed.
    pub(crate) appends_committed: u64,
    /// Appends whose writer did not learn the outcome, and appends that never returned.
    pub(crate) appends_unknown: u64,
    /// Appends that were refused before they got a sequence number.
    pub(crate) appends_refused: u64,
    /// Records handed to readers, the audit's read included.
    pub(crate) records_read: u64,
    /// The longest time between the call of an append and its report as committed.
    pub(crate) worst_append_latency: Duration,
    /// How many events were judged.
    pub(crate) events: u64,
}

impl HistoryCounts {
    /// Counts what `events`, the history of one stream, holds.
    pub(crate) fn of(events: &[TimedEvent]) -> Self {
        let mut counts = Self {
            events: events.len() as u64,
            ..Self::default()
        };
        let mut invoked_at: BTreeMap<u64, Duration> = BTreeMap::new();
        for timed in events {
            match &timed.event {
                Event::AppendInvoked { operation, .. } => {
                    invoked_at.insert(*operation, timed.at);
                }
                Event::AppendFinished { operation, outcome } => {
                    let called_at = invoked_at.remove(operation).unwrap_or(timed.at);
                    counts.count_finished_append(outcome, timed.at.saturating_sub(called_at));
                }
                Event::RecordRead { .. } => counts.records_read += 1,
                _other_event => {}
            }
        }
        // What was called and never returned belongs to a writer that was stopped mid-call.
        counts.appends_unknown += invoked_at.len() as u64;
        counts
    }

    /// Counts one append that returned with `outcome`, `latency` after it was called.
    fn count_finished_append(&mut self, outcome: &AppendOutcome, latency: Duration) {
        match outcome {
            AppendOutcome::Committed { .. } => {
                self.appends_committed += 1;
                self.worst_append_latency = self.worst_append_latency.max(latency);
            }
            AppendOutcome::Unknown => self.appends_unknown += 1,
            AppendOutcome::NotAppended => self.appends_refused += 1,
        }
    }

    /// Adds the counts of another stream to these.
    pub(crate) fn add(&mut self, other: &Self) {
        self.appends_committed += other.appends_committed;
        self.appends_unknown += other.appends_unknown;
        self.appends_refused += other.appends_refused;
        self.records_read += other.records_read;
        self.worst_append_latency = self.worst_append_latency.max(other.worst_append_latency);
        self.events += other.events;
    }
}

/// The line of one round.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct RoundReport {
    /// Always `round`.
    pub(crate) kind: &'static str,
    /// The name of the run, which starts the name of every stream it made.
    pub(crate) run: String,
    /// The number of the round within the run, from 1.
    pub(crate) round: u64,
    /// When the round began, in milliseconds since the Unix epoch, by the cluster's clock.
    pub(crate) started_at_unix_millis: u64,
    /// When the round ended.
    pub(crate) ended_at_unix_millis: u64,
    /// How many streams the round wrote.
    pub(crate) streams: u64,
    /// Appends reported as committed.
    pub(crate) appends_committed: u64,
    /// Appends that ended without their writer learning how.
    pub(crate) appends_unknown: u64,
    /// Appends that were refused.
    pub(crate) appends_refused: u64,
    /// Records handed to readers, the audit's read included.
    pub(crate) records_read: u64,
    /// How many faults of each kind were injected.
    pub(crate) faults: BTreeMap<&'static str, u64>,
    /// How many times a fault was due and none was injected: the cluster was not healthy yet, it offered no target,
    /// its management refused, or a cut was not in effect.
    pub(crate) faults_not_injected: u64,
    /// The kinds of fault that were asked for and are no longer tried, each with the reason. A kind listed here
    /// was not survived by the cluster: it did not happen.
    pub(crate) faults_disabled: BTreeMap<&'static str, String>,
    /// How many times a writer took a stream over from the one before it.
    pub(crate) takeovers: u64,
    /// The longest time between the call of an append and its report as committed, in milliseconds.
    pub(crate) worst_append_latency_milliseconds: u64,
    /// How many events were judged.
    pub(crate) events: u64,
    /// How the round ended.
    pub(crate) verdict: RoundVerdict,
    /// What the checker found, one text per violation, each starting with the stream.
    pub(crate) violations: Vec<String>,
    /// Why the round was not audited, if it was not.
    pub(crate) reason: Option<String>,
    /// The files the histories were written to, if the round was not clean.
    pub(crate) dumps: Vec<String>,
    /// Whether the round claimed, on purpose, a commit that never happened: a drill, whose violation says that the
    /// alarm works and nothing about the cluster.
    pub(crate) drill: bool,
}

/// The line of a run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct Summary {
    /// Always `summary`.
    pub(crate) kind: &'static str,
    /// The name of the run.
    pub(crate) run: String,
    /// How the run ended.
    pub(crate) verdict: RunVerdict,
    /// How many rounds were begun.
    pub(crate) rounds: u64,
    /// How many of them were clean.
    pub(crate) clean_rounds: u64,
    /// Appends reported as committed, over all rounds.
    pub(crate) appends_committed: u64,
    /// Appends that ended without their writer learning how.
    pub(crate) appends_unknown: u64,
    /// Appends that were refused.
    pub(crate) appends_refused: u64,
    /// Records handed to readers.
    pub(crate) records_read: u64,
    /// How many faults of each kind were injected.
    pub(crate) faults: BTreeMap<&'static str, u64>,
    /// The kinds of fault that were asked for and given up during the run, each with the reason.
    pub(crate) faults_disabled: BTreeMap<&'static str, String>,
    /// How many times a writer took a stream over.
    pub(crate) takeovers: u64,
    /// The longest time between the call of an append and its report as committed, in milliseconds.
    pub(crate) worst_append_latency_milliseconds: u64,
    /// Why the run could not start or did not recover, if that is how it ended.
    pub(crate) reason: Option<String>,
}

impl Summary {
    /// The summary of the run called `run` before its first round. Its verdict is clean until something says
    /// otherwise.
    pub(crate) fn of_run(run: &str) -> Self {
        Self {
            kind: "summary",
            run: run.to_owned(),
            verdict: RunVerdict::Clean,
            rounds: 0,
            clean_rounds: 0,
            appends_committed: 0,
            appends_unknown: 0,
            appends_refused: 0,
            records_read: 0,
            faults: BTreeMap::new(),
            faults_disabled: BTreeMap::new(),
            takeovers: 0,
            worst_append_latency_milliseconds: 0,
            reason: None,
        }
    }

    /// Adds the round that `report` describes to the summary. The verdict of the run is not touched.
    pub(crate) fn add_round(&mut self, report: &RoundReport) {
        self.rounds += 1;
        if report.verdict == RoundVerdict::Clean {
            self.clean_rounds += 1;
        }
        self.appends_committed += report.appends_committed;
        self.appends_unknown += report.appends_unknown;
        self.appends_refused += report.appends_refused;
        self.records_read += report.records_read;
        for (kind, injected) in &report.faults {
            *self.faults.entry(kind).or_default() += injected;
        }
        for (kind, reason) in &report.faults_disabled {
            self.faults_disabled.insert(kind, reason.clone());
        }
        self.takeovers += report.takeovers;
        self.worst_append_latency_milliseconds = self
            .worst_append_latency_milliseconds
            .max(report.worst_append_latency_milliseconds);
    }
}

/// Renders a report as one line of JSON, ended by a newline.
pub(crate) fn json_line<Report: Serialize>(report: &Report) -> String {
    let mut line = serde_json::to_string(report).expect("reports hold only text, numbers and maps with text keys");
    line.push('\n');
    line
}

#[cfg(test)]
#[path = "report_test.rs"]
mod tests;
