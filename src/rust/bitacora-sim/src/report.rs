//! What a simulated run produced: its history, the verdict on it, and a fingerprint.
//!
//! # Why this file exists
//!
//! A run ends with a list of some thousands of events, and three questions about it. Did it break an invariant? Did the
//! healed cluster recover? And, when the code changes, is the run still the same run? The first two decide whether a
//! seed passes. The third is what lets a rewrite be shown to change no behaviour, and what lets a person tell a real
//! fix from an accidental change of the schedule. This file is where the history turns into those answers, and into
//! something a person can read when the answer is no.
//!
//! # What it contains
//!
//! - [`SimulationReport`]: the events, the violations, why the cluster was stuck if it was, and three counts.
//! - [`SimulationReport::fingerprint`]: one number for the whole history.
//! - [`SimulationReport::failure_summary`]: the text a failing test prints.
//! - [`judge`]: runs the checkers over a finished history and builds the report.
//!
//! # How it works
//!
//! [`judge`] runs the log checker and the linearizability check on every run, and the audit check only on a run whose
//! audit finished: a cluster that never served the whole stream back has no audit to judge, and is reported as stuck
//! instead.
//!
//! The fingerprint is the FNV-1a hash of the `Debug` rendering of every event, in order. The rendering includes the
//! simulated time of each event, so two runs have the same fingerprint only if the same things happened at the same
//! simulated instants.
//!
//! # Invariants
//!
//! - A report is a function of the history and of how the audit ended. Judging the same history twice gives the same
//!   report.
//! - The violations are listed in a fixed order: those of the log checker, then those of the linearizability check,
//!   then those of the audit.
//! - The fingerprint depends on the names of the event types and of their fields (`history.rs`). It is the same on
//!   every machine, and it changes whenever the history does.
//! - A run is clean only if it has no violation and its cluster recovered.
//!
//! # See also
//!
//! - `history.rs`: the events.
//! - `checker.rs` and `linearizability.rs`: the checks.
//! - `simulation.rs`: produces the history and calls [`judge`].

use std::fmt::Write as _;

use crate::checker::{Violation, check, check_audit};
use crate::history::{AppendOutcome, Event, TimedEvent};
use crate::linearizability::check_linearizability;

/// The starting value of the FNV-1a hash, for 64 bits.
const FNV_OFFSET_BASIS: u64 = 0xCBF2_9CE4_8422_2325;

/// The multiplier of the FNV-1a hash, for 64 bits.
const FNV_PRIME: u64 = 0x0000_0100_0000_01B3;

/// How many violations a failure summary lists. The first few name the bug; the rest follow from it.
const VIOLATIONS_SHOWN: usize = 10;

/// How many of the last events of a run a failure summary lists.
const LAST_EVENTS_SHOWN: usize = 40;

/// What a run produced.
///
/// Get one from [`run`](crate::run). A test asserts [`SimulationReport::is_clean`] and prints
/// [`SimulationReport::failure_summary`] when it is not.
///
/// # Examples
///
/// ```
/// use bitacora_sim::SimulationReport;
///
/// let report = SimulationReport {
///     seed: 7,
///     events: Vec::new(),
///     violations: Vec::new(),
///     stuck: Some("the healed cluster did not settle within 300s".to_owned()),
///     committed_appends: 0,
///     faults_injected: 0,
///     tiered_segment_reads: 0,
/// };
///
/// assert!(!report.is_clean());
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimulationReport {
    /// The seed that reproduces the run.
    pub seed: u64,
    /// Everything clients did and saw, in order.
    pub events: Vec<TimedEvent>,
    /// Every invariant the history breaks.
    pub violations: Vec<Violation>,
    /// Why the healed cluster did not recover, if it did not. Safety is required always; liveness only once every fault
    /// is gone.
    pub stuck: Option<String>,
    /// How many appends were reported as committed.
    pub committed_appends: usize,
    /// How many faults were injected.
    pub faults_injected: usize,
    /// How many range reads of tiered segments the object store served: whether, and how much, the run read segments
    /// that had left the replicas. Reads of the key-value store's own artifacts do not count.
    pub tiered_segment_reads: u64,
}

impl SimulationReport {
    /// Says whether the run broke no invariant and the cluster recovered.
    ///
    /// # Returns
    ///
    /// `true` if there is no violation and the healed cluster settled in time.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_sim::SimulationReport;
    ///
    /// let report = SimulationReport {
    ///     seed: 7,
    ///     events: Vec::new(),
    ///     violations: Vec::new(),
    ///     stuck: None,
    ///     committed_appends: 0,
    ///     faults_injected: 0,
    ///     tiered_segment_reads: 0,
    /// };
    ///
    /// assert!(report.is_clean());
    /// ```
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.violations.is_empty() && self.stuck.is_none()
    }

    /// Computes a number that identifies the run's whole history.
    ///
    /// Two runs of the same seed on the same code have the same fingerprint, on any machine; comparing fingerprints is
    /// the quickest way to see whether a change altered behaviour. It is the FNV-1a hash of the `Debug` rendering of
    /// every event, so it costs one formatting of the whole history.
    ///
    /// # Returns
    ///
    /// The hash. A history without events hashes to the offset basis of FNV-1a.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_sim::{History, SimulationReport};
    ///
    /// let history = History::new();
    /// history.fault("node 2 crashes");
    /// let report = SimulationReport {
    ///     seed: 7,
    ///     events: history.events(),
    ///     violations: Vec::new(),
    ///     stuck: None,
    ///     committed_appends: 0,
    ///     faults_injected: 1,
    ///     tiered_segment_reads: 0,
    /// };
    ///
    /// assert_eq!(report.fingerprint(), report.clone().fingerprint());
    /// # }
    /// ```
    #[must_use]
    pub fn fingerprint(&self) -> u64 {
        let mut hash = FNV_OFFSET_BASIS;
        for timed in &self.events {
            for byte in format!("{timed:?}").bytes() {
                hash = (hash ^ u64::from(byte)).wrapping_mul(FNV_PRIME);
            }
        }
        hash
    }

    /// Describes what went wrong, with the seed that reproduces it.
    ///
    /// # Returns
    ///
    /// Several lines: the seed and the counts, why the cluster was stuck if it was, the first ten violations, and the
    /// last forty events of the history with their simulated times.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_sim::SimulationReport;
    ///
    /// let report = SimulationReport {
    ///     seed: 7,
    ///     events: Vec::new(),
    ///     violations: Vec::new(),
    ///     stuck: Some("the healed cluster did not settle within 300s".to_owned()),
    ///     committed_appends: 12,
    ///     faults_injected: 3,
    ///     tiered_segment_reads: 0,
    /// };
    ///
    /// let summary = report.failure_summary();
    ///
    /// assert!(summary.starts_with("simulation seed 7 failed after 12 committed appends and 3 faults"));
    /// ```
    #[must_use]
    pub fn failure_summary(&self) -> String {
        let mut summary = format!(
            "simulation seed {} failed after {} committed appends and {} faults\n",
            self.seed, self.committed_appends, self.faults_injected
        );
        if let Some(reason) = &self.stuck {
            let _ = writeln!(summary, "stuck: {reason}");
        }
        for violation in self.violations.iter().take(VIOLATIONS_SHOWN) {
            let _ = writeln!(summary, "violation: {violation:?}");
        }
        let _ = writeln!(summary, "last events:");
        let shown_from = self.events.len().saturating_sub(LAST_EVENTS_SHOWN);
        for timed in &self.events[shown_from..] {
            let _ = writeln!(summary, "  {:>10.3?} {:?}", timed.at, timed.event);
        }
        summary
    }
}

/// Checks the history of a finished run and summarizes it.
///
/// Runs the log checker and the linearizability check over the events, and the audit check if the audit finished.
///
/// # Arguments
///
/// * `seed` - The seed of the run, for the report.
/// * `events` - The whole history of the run.
/// * `settled` - How the audit ended: the reading session of its read of the whole stream, or why the cluster did not
///   get that far.
///
/// # Returns
///
/// The report of the run. Its `tiered_segment_reads` is zero: that count is not part of the history, and the caller
/// fills it in.
pub(crate) fn judge(seed: u64, events: &[TimedEvent], settled: Result<u64, String>) -> SimulationReport {
    let mut violations = check(events);
    violations.extend(check_linearizability(events));
    let stuck = match settled {
        Ok(audit_session) => {
            violations.extend(check_audit(events, audit_session));
            None
        }
        Err(reason) => Some(reason),
    };
    SimulationReport {
        seed,
        events: events.to_vec(),
        violations,
        stuck,
        committed_appends: count_events(events, is_committed_append),
        faults_injected: count_events(events, is_fault),
        tiered_segment_reads: 0,
    }
}

/// Counts the events of a history that `is_counted` accepts.
fn count_events(events: &[TimedEvent], is_counted: fn(&Event) -> bool) -> usize {
    let mut count = 0;
    for timed in events {
        if is_counted(&timed.event) {
            count += 1;
        }
    }
    count
}

/// Says whether `event` is an append that returned as committed.
fn is_committed_append(event: &Event) -> bool {
    matches!(
        event,
        Event::AppendFinished {
            outcome: AppendOutcome::Committed { .. },
            ..
        }
    )
}

/// Says whether `event` is something the simulator did to the cluster.
fn is_fault(event: &Event) -> bool {
    matches!(event, Event::Fault { .. })
}

#[cfg(test)]
#[path = "report_test.rs"]
mod tests;
