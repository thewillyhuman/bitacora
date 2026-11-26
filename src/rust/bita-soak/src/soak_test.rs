//! Tests of a whole run, played against the in-process cluster.
//!
//! # What is covered
//!
//! - A run plays rounds until its time is up and is clean if they all were; it prints a line per round and the
//!   summary last.
//! - Nothing of a round's history reaches the next round: the second round holds as many events as the first, not
//!   those of both.
//! - A planted lost record ends the run after its first round as violated, with the exit code for that and the
//!   histories in files.
//! - A cut that an earlier run left behind is removed before the first round; a run that cannot remove it, or that
//!   finds no live replica, does not start.
//! - A round that cannot be audited ends the run as not recovered; a run that is told to stop ends as interrupted.
//! - A kind of fault that was given up stays given up for the rest of the run and is named in every line.
//!
//! # How the fixtures work
//!
//! `Venue` is the in-process cluster of `test_support.rs` with a controller leading, a temporary directory for
//! dumps, the metrics, and a buffer in place of standard output. `Venue::run` runs a soak of quick rounds (a
//! virtual minute each) for a hundred virtual seconds, which is two rounds.

use std::sync::Arc;

use serde_json::Value;
use tempfile::TempDir;
use tokio::time::sleep;

use super::*;
use crate::faults::{Fault, FaultKind};
use crate::test_support::{InProcessCluster, quick_round};

/// What a run needs, around the in-process cluster.
struct Venue {
    /// The cluster.
    cluster: InProcessCluster,
    /// Where dumps go.
    dump_directory: TempDir,
    /// The counters of the run.
    metrics: SoakMetrics,
    /// What the run printed.
    output: Vec<u8>,
    /// Tells the run to stop.
    told_to_stop: watch::Sender<bool>,
    /// What the run listens to.
    stop: watch::Receiver<bool>,
}

impl Venue {
    /// The cluster, with a controller leading.
    async fn start() -> Self {
        let cluster = InProcessCluster::start().await;
        sleep(Duration::from_secs(2)).await;
        let (told_to_stop, stop) = watch::channel(false);
        Self {
            cluster,
            dump_directory: tempfile::tempdir().expect("a temporary directory"),
            metrics: SoakMetrics::new(),
            output: Vec::new(),
            told_to_stop,
            stop,
        }
    }

    /// The settings of a run of a hundred seconds whose rounds inject faults of `kinds`.
    fn settings(&self, kinds: Vec<FaultKind>) -> SoakSettings {
        SoakSettings {
            run: "soak-test".to_owned(),
            duration: Duration::from_secs(100),
            seed: 7,
            round: quick_round(kinds, self.dump_directory.path()),
            plants_lost_record: false,
        }
    }

    /// Runs a soak with `settings`.
    async fn run_with(&mut self, settings: &SoakSettings) -> Summary {
        run_soak(
            &self.cluster.soaked,
            settings,
            &self.metrics,
            &mut self.output,
            &mut self.stop,
        )
        .await
    }

    /// Runs a soak without faults.
    async fn run(&mut self) -> Summary {
        let settings = self.settings(Vec::new());
        self.run_with(&settings).await
    }

    /// The lines the run printed, each parsed as JSON.
    fn printed(&self) -> Vec<Value> {
        let text = String::from_utf8(self.output.clone()).expect("the output is text");
        let mut lines = Vec::new();
        for line in text.lines() {
            lines.push(serde_json::from_str(line).expect("every line is JSON"));
        }
        lines
    }
}

#[tokio::test(start_paused = true)]
async fn run_plays_rounds_until_its_time_is_up() {
    let mut venue = Venue::start().await;

    let summary = venue.run().await;

    assert_eq!(summary.rounds, 2, "{summary:?}");
}

#[tokio::test(start_paused = true)]
async fn run_whose_rounds_were_all_clean_is_clean() {
    let mut venue = Venue::start().await;

    let summary = venue.run().await;

    assert_eq!(
        (summary.verdict, summary.clean_rounds),
        (RunVerdict::Clean, 2),
        "{summary:?}"
    );
    assert_eq!(crate::exit_code_of(summary.verdict), 0);
}

#[tokio::test(start_paused = true)]
async fn run_prints_a_line_per_round_and_the_summary_last() {
    let mut venue = Venue::start().await;

    venue.run().await;

    let mut kinds = Vec::new();
    for line in venue.printed() {
        kinds.push(format!("{} {}", line["kind"], line["round"]));
    }
    assert_eq!(kinds, ["\"round\" 1", "\"round\" 2", "\"summary\" null"]);
}

#[tokio::test(start_paused = true)]
async fn summary_that_is_printed_is_the_one_that_is_returned() {
    let mut venue = Venue::start().await;

    let summary = venue.run().await;

    let text = String::from_utf8(venue.output.clone()).expect("the output is text");
    assert!(text.ends_with(&json_line(&summary)), "{text}");
}

/// What keeps a week of rounds within the memory of one: a round's histories end with the round.
#[tokio::test(start_paused = true)]
async fn history_of_a_round_does_not_reach_the_next_round() {
    let mut venue = Venue::start().await;

    venue.run().await;

    let printed = venue.printed();
    let events_of_first_round = printed[0]["events"].as_u64().expect("a count");
    let events_of_second_round = printed[1]["events"].as_u64().expect("a count");
    assert!(
        events_of_second_round.abs_diff(events_of_first_round) < events_of_first_round / 10,
        "{events_of_first_round} events in the first round, {events_of_second_round} in the second"
    );
}

#[tokio::test(start_paused = true)]
async fn run_with_faults_of_every_kind_is_clean() {
    let mut venue = Venue::start().await;
    let settings = venue.settings(FaultKind::ALL.to_vec());

    let summary = venue.run_with(&settings).await;

    assert_eq!(summary.verdict, RunVerdict::Clean, "{:?}", venue.printed());
}

/// The drill, end to end: a commit is claimed that never happened, and the run must end there, as violated, with
/// the exit code a Job fails on and the histories on disk.
#[tokio::test(start_paused = true)]
async fn planted_lost_record_ends_the_run_as_violated_after_its_first_round() {
    let mut venue = Venue::start().await;
    let mut settings = venue.settings(Vec::new());
    settings.plants_lost_record = true;

    let summary = venue.run_with(&settings).await;

    assert_eq!(
        (summary.verdict, summary.rounds),
        (RunVerdict::Violated, 1),
        "{summary:?}"
    );
    assert_eq!(crate::exit_code_of(summary.verdict), 3);
    assert!(venue.dump_directory.path().join("soak-test-1-0.history").exists());
}

#[tokio::test(start_paused = true)]
async fn dump_of_a_violated_run_holds_the_commit_that_was_lost() {
    let mut venue = Venue::start().await;
    let mut settings = venue.settings(Vec::new());
    settings.plants_lost_record = true;

    venue.run_with(&settings).await;

    let dump_path = venue.dump_directory.path().join("soak-test-1-0.history");
    let dump = std::fs::read_to_string(dump_path).expect("the dump can be read");
    assert!(dump.contains("AppendInvoked"), "{dump}");
    assert!(dump.contains("soak-test-1-0/drill"), "{dump}");
}

#[tokio::test(start_paused = true)]
async fn cut_left_by_an_earlier_run_is_removed_before_the_first_round() {
    let mut venue = Venue::start().await;
    let left_behind = Fault::CutZone {
        zone: "zone-2".to_owned(),
    };
    venue
        .cluster
        .soaked
        .faults
        .inject(&left_behind)
        .await
        .expect("the cut is made");
    let faults = Arc::clone(&venue.cluster.faults);
    let cuts_before_the_run = faults.cuts_in_place();
    let cuts_as_the_first_round_begins = tokio::spawn(async move {
        sleep(Duration::from_secs(1)).await;
        faults.cuts_in_place()
    });

    venue.run().await;

    assert_eq!(cuts_before_the_run, 2);
    assert_eq!(cuts_as_the_first_round_begins.await.expect("the task ends"), 0);
}

#[tokio::test(start_paused = true)]
async fn run_that_cannot_remove_the_cuts_of_an_earlier_run_does_not_start() {
    let mut venue = Venue::start().await;
    venue.cluster.faults.refuse_to_remove_cuts(true);

    let summary = venue.run().await;

    assert_eq!(
        (summary.verdict, summary.rounds),
        (RunVerdict::CouldNotStart, 0),
        "{summary:?}"
    );
    assert_eq!(crate::exit_code_of(summary.verdict), 1);
    let reason = summary.reason.expect("a run that did not start says why");
    assert!(reason.starts_with("a network cut could not be removed"), "{reason}");
}

#[tokio::test(start_paused = true)]
async fn run_does_not_start_on_a_cluster_without_a_live_replica() {
    let mut venue = Venue::start().await;
    for node_number in 0..6 {
        venue.cluster.faults.crash_for(node_number, Duration::from_secs(36_000));
    }
    sleep(Duration::from_secs(20)).await;

    let summary = venue.run().await;

    assert_eq!(summary.verdict, RunVerdict::CouldNotStart, "{summary:?}");
    assert_eq!(summary.reason.as_deref(), Some("no replica is registered and alive"));
}

/// The cluster's management stops answering in the middle of a round, with a cut in place that nobody can remove.
#[tokio::test(start_paused = true)]
async fn round_that_cannot_be_audited_ends_the_run_as_not_recovered() {
    let mut venue = Venue::start().await;
    let settings = venue.settings(vec![FaultKind::CutReplica]);
    let faults = Arc::clone(&venue.cluster.faults);
    tokio::spawn(async move {
        sleep(Duration::from_secs(5)).await;
        faults.refuse_to_remove_cuts(true);
    });

    let summary = venue.run_with(&settings).await;

    assert_eq!(
        (summary.verdict, summary.rounds),
        (RunVerdict::NotRecovered, 1),
        "{summary:?}"
    );
    assert_eq!(crate::exit_code_of(summary.verdict), 4);
    let reason = summary.reason.expect("a run that did not recover says why");
    assert!(reason.starts_with("a network cut could not be removed"), "{reason}");
}

#[tokio::test(start_paused = true)]
async fn run_that_is_told_to_stop_ends_as_interrupted() {
    let mut venue = Venue::start().await;
    let told_to_stop = venue.told_to_stop.clone();
    tokio::spawn(async move {
        sleep(Duration::from_secs(25)).await;
        told_to_stop.send(true).expect("the run listens");
    });

    let summary = venue.run().await;

    assert_eq!(
        (summary.verdict, summary.rounds),
        (RunVerdict::Interrupted, 1),
        "{summary:?}"
    );
    assert_eq!(crate::exit_code_of(summary.verdict), 5);
}

#[tokio::test(start_paused = true)]
async fn metrics_count_the_rounds_of_the_run() {
    let mut venue = Venue::start().await;

    venue.run().await;

    let exposition = venue.metrics.registry().encode();
    assert!(
        exposition.contains("bitacora_soak_rounds_clean_total 2\n"),
        "{exposition}"
    );
}

/// On a cluster whose network does not enforce cuts, a week of clean rounds must not read as a week of survived
/// partitions: every line says which kinds were given up, and the cut is tried once, not once per round.
#[tokio::test(start_paused = true)]
async fn kind_of_fault_that_was_given_up_stays_given_up_and_is_in_every_line() {
    let mut venue = Venue::start().await;
    venue.cluster.faults.let_cuts_do_nothing(true);
    let settings = venue.settings(vec![FaultKind::CutReplica]);

    let summary = venue.run_with(&settings).await;

    let mut lines_that_say_so = 0;
    for line in venue.printed() {
        if line["faults_disabled"]["cut_replica"].is_string() {
            lines_that_say_so += 1;
        }
    }
    assert_eq!(lines_that_say_so, 3, "{:?}", venue.printed());
    assert_eq!(venue.cluster.faults.injected().len(), 1);
    assert_eq!(
        (summary.verdict, summary.faults["cut_replica"]),
        (RunVerdict::Clean, 0),
        "{summary:?}"
    );
}
