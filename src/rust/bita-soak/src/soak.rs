//! A whole run: round after round until the time is up or a round is not clean.
//!
//! # Why this file exists
//!
//! The exit gate of the hardening milestone is a week of runs on a real cluster with no invariant violation (design,
//! section 18). One round shows that the cluster survived ten minutes of faults; the gate needs thousands of them,
//! one after the other, unattended, and a clear end: the time is up and every round was clean, or a round was not
//! and the run stops there with the evidence. This file is that loop.
//!
//! # What it contains
//!
//! - [`SoakSettings`]: the name of the run, how long it lasts, its seed, and how its rounds are played.
//! - [`run_soak`]: runs the rounds, prints a line for each and one for the run, and returns the summary.
//!
//! # How it works
//!
//! Before the first round the run removes any cut an earlier run left behind and notes which replicas are
//! registered and alive: those are the replicas every round must find alive again before it is judged. A cluster
//! without a live replica cannot be soaked, and the run does not start.
//!
//! Then rounds follow each other for as long as the run's time lasts. A clean round is followed by the next. A
//! round that broke an invariant, or after which the cluster did not recover, ends the run: its streams and the
//! files of its histories are what there is to look at, and more rounds would only write over the state that
//! explains it. A run that is told to stop ends after the round it interrupted.
//!
//! # Invariants
//!
//! - A run is clean only if its time is up and every round was clean.
//! - Every round's line is printed before the next round begins, so the log is complete up to the moment the tool
//!   dies, whatever kills it.
//! - The run keeps the summary's numbers and nothing else from a round.
//!
//! # See also
//!
//! - `round.rs`: one round.
//! - `report.rs`: the lines.
//! - `metrics.rs`: the same numbers as metrics.

use std::collections::BTreeSet;
use std::io::Write;
use std::time::Duration;

use bitacora_types::ReplicaId;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::cluster::SoakedCluster;
use crate::health::alive_replicas;
use crate::metrics::SoakMetrics;
use crate::nemesis::{NemesisMemory, remove_cuts_within};
use crate::report::{RoundReport, RoundVerdict, RunVerdict, Summary, json_line};
use crate::round::{RoundContext, RoundSettings, run_round};

/// How long a run keeps trying to remove the cuts an earlier run left, before it gives up starting.
const START_TIME_LIMIT: Duration = Duration::from_secs(60);

/// How a run is made.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SoakSettings {
    /// The name of the run. It starts the name of every stream the run makes, so two runs on one cluster need two
    /// names.
    pub(crate) run: String,
    /// How long the run lasts. A round that has begun is finished.
    pub(crate) duration: Duration,
    /// The seed of the choices of the nemesis.
    pub(crate) seed: u64,
    /// How every round is played.
    pub(crate) round: RoundSettings,
    /// Whether the first round claims a commit that never happened, to prove that a violation ends the run.
    pub(crate) plants_lost_record: bool,
}

/// Runs rounds against `cluster` until the run's time is up, a round is not clean, or the tool is told to stop.
///
/// # Arguments
///
/// * `cluster` - The cluster under test.
/// * `settings` - How the run is made.
/// * `metrics` - Counts what the rounds did, for whoever scrapes the tool.
/// * `output` - Where the lines of the rounds and of the run are written.
/// * `stop` - Becomes `true`, or is dropped, when the tool must end.
///
/// # Returns
///
/// The summary of the run, which was also written to `output` as its last line.
pub(crate) async fn run_soak(
    cluster: &SoakedCluster,
    settings: &SoakSettings,
    metrics: &SoakMetrics,
    output: &mut (dyn Write + Send),
    stop: &mut watch::Receiver<bool>,
) -> Summary {
    let mut summary = Summary::of_run(&settings.run);
    match replicas_to_expect(cluster).await {
        Ok(expected_replicas) => {
            run_rounds(
                cluster,
                settings,
                &expected_replicas,
                metrics,
                &mut summary,
                output,
                stop,
            )
            .await;
        }
        Err(reason) => {
            summary.verdict = RunVerdict::CouldNotStart;
            summary.reason = Some(reason);
        }
    }
    print(output, &json_line(&summary));
    summary
}

/// Prepares the cluster for a run and says which replicas the run expects to stay.
///
/// # Errors
///
/// Returns why the run cannot start: a cut of an earlier run cannot be removed, the registry cannot be read, or no
/// replica is alive.
async fn replicas_to_expect(cluster: &SoakedCluster) -> Result<BTreeSet<ReplicaId>, String> {
    remove_cuts_within(cluster, START_TIME_LIMIT).await?;
    let expected_replicas = alive_replicas(cluster).await?;
    if expected_replicas.is_empty() {
        return Err("no replica is registered and alive".to_owned());
    }
    Ok(expected_replicas)
}

/// Runs round after round and keeps `summary` up to date. Ends when the time is up, when a round is not clean, or
/// when the tool is told to stop.
async fn run_rounds(
    cluster: &SoakedCluster,
    settings: &SoakSettings,
    expected_replicas: &BTreeSet<ReplicaId>,
    metrics: &SoakMetrics,
    summary: &mut Summary,
    output: &mut (dyn Write + Send),
    stop: &mut watch::Receiver<bool>,
) {
    let mut memory = NemesisMemory::from_seed(settings.seed);
    let run_ends_at = Instant::now() + settings.duration;
    while Instant::now() < run_ends_at {
        let number = summary.rounds + 1;
        let context = RoundContext {
            run: &settings.run,
            number,
            expected_replicas,
            plants_lost_record: settings.plants_lost_record && number == 1,
        };
        let report = run_round(cluster, &settings.round, context, &mut memory, stop).await;
        print(output, &json_line(&report));
        metrics.count_round(&report);
        summary.add_round(&report);
        if let Some(verdict) = verdict_that_ends_the_run(&report) {
            summary.verdict = verdict;
            summary.reason.clone_from(&report.reason);
            return;
        }
    }
}

/// Says how the run ends because of the round `report` describes, or `None` if the next round may begin.
const fn verdict_that_ends_the_run(report: &RoundReport) -> Option<RunVerdict> {
    match report.verdict {
        RoundVerdict::Clean => None,
        RoundVerdict::Violated => Some(RunVerdict::Violated),
        RoundVerdict::NotAudited => Some(RunVerdict::NotRecovered),
        RoundVerdict::Interrupted => Some(RunVerdict::Interrupted),
    }
}

/// Writes one line to the output and makes sure it leaves the process. A log that cannot be written is not worth
/// stopping a soak for: the metrics and the exit code still say how it went.
fn print(output: &mut (dyn Write + Send), line: &str) {
    let _written = output.write_all(line.as_bytes());
    let _flushed = output.flush();
}

#[cfg(test)]
#[path = "soak_test.rs"]
mod tests;
