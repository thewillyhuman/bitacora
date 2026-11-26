//! Tests of the lines a soak prints and of the counts in them.
//!
//! # What is covered
//!
//! - What a history holds, counted: commits, appends of unknown outcome (those that never returned included),
//!   refused appends, records read, and the worst latency of a commit.
//! - Adding the counts of several streams.
//! - A summary adds its rounds up, counts the clean ones and keeps the worst latency.
//! - A report is one line of JSON with the field names `docs/soak.md` lists.
//! - The kinds of fault that were given up are printed with the reason, and stay in the summary.
//!
//! # How the fixtures work
//!
//! The histories are written by hand on a paused clock, so the time between a call and its return is what the test
//! advanced the clock by. `clean_round` is the report of a round with a few appends, one fault and no violation.

use std::time::Duration;

use bitacora_sim::History;
use bitacora_types::{Epoch, SequenceNumber};
use bytes::Bytes;

use super::*;

/// Records an append of `payload` that returns as `outcome` after `latency`.
async fn append(history: &History, payload: &'static str, latency: Duration, outcome: AppendOutcome) {
    let operation = history.append_invoked(Bytes::from_static(payload.as_bytes()));
    tokio::time::advance(latency).await;
    history.append_finished(operation, outcome);
}

/// The outcome of an append committed at `sequence`.
fn committed_at(sequence: u64) -> AppendOutcome {
    AppendOutcome::Committed {
        sequence: SequenceNumber::new(sequence),
        epoch: Epoch::new(1),
    }
}

/// The report of the first round of the run `soak-7`: three commits, one unknown append, a killed replica, clean.
fn clean_round() -> RoundReport {
    RoundReport {
        kind: "round",
        run: "soak-7".to_owned(),
        round: 1,
        started_at_unix_millis: 1_000,
        ended_at_unix_millis: 61_000,
        streams: 2,
        appends_committed: 3,
        appends_unknown: 1,
        appends_refused: 0,
        records_read: 6,
        faults: BTreeMap::from([("kill_replica", 1)]),
        faults_not_injected: 2,
        faults_disabled: BTreeMap::new(),
        takeovers: 1,
        worst_append_latency_milliseconds: 40,
        events: 15,
        verdict: RoundVerdict::Clean,
        violations: Vec::new(),
        reason: None,
        dumps: Vec::new(),
        drill: false,
    }
}

#[tokio::test(start_paused = true)]
async fn appends_are_counted_by_how_they_ended() {
    let history = History::new();
    append(&history, "a", Duration::from_millis(5), committed_at(1)).await;
    append(&history, "b", Duration::from_millis(5), committed_at(2)).await;
    append(&history, "c", Duration::from_millis(5), AppendOutcome::Unknown).await;
    append(&history, "d", Duration::from_millis(5), AppendOutcome::NotAppended).await;

    let counts = HistoryCounts::of(&history.events());

    assert_eq!(counts.appends_committed, 2);
    assert_eq!(counts.appends_unknown, 1);
    assert_eq!(counts.appends_refused, 1);
}

/// A writer that was stopped in the middle of an append leaves a call without a return. Nobody knows how it ended.
#[tokio::test(start_paused = true)]
async fn append_that_never_returned_counts_as_unknown() {
    let history = History::new();
    let _never_finished = history.append_invoked(Bytes::from_static(b"a"));

    let counts = HistoryCounts::of(&history.events());

    assert_eq!(counts.appends_unknown, 1);
}

#[tokio::test(start_paused = true)]
async fn worst_latency_is_the_longest_wait_for_a_commit() {
    let history = History::new();
    append(&history, "a", Duration::from_millis(5), committed_at(1)).await;
    append(&history, "b", Duration::from_millis(160), committed_at(2)).await;
    append(&history, "c", Duration::from_millis(9), committed_at(3)).await;

    let counts = HistoryCounts::of(&history.events());

    assert_eq!(counts.worst_append_latency, Duration::from_millis(160));
}

/// An append that took long and was not committed says nothing about how long a commit takes.
#[tokio::test(start_paused = true)]
async fn appends_that_did_not_commit_do_not_count_towards_the_latency() {
    let history = History::new();
    append(&history, "a", Duration::from_millis(5), committed_at(1)).await;
    append(&history, "b", Duration::from_secs(30), AppendOutcome::Unknown).await;

    let counts = HistoryCounts::of(&history.events());

    assert_eq!(counts.worst_append_latency, Duration::from_millis(5));
}

#[tokio::test(start_paused = true)]
async fn records_read_and_events_are_counted() {
    let history = History::new();
    append(&history, "a", Duration::from_millis(5), committed_at(1)).await;
    let session = history.reader_started(SequenceNumber::FIRST);
    history.record_read(session, SequenceNumber::FIRST, Bytes::from_static(b"a"));
    history.fault("replica bitacora-replica-1 is killed");

    let counts = HistoryCounts::of(&history.events());

    assert_eq!(counts.records_read, 1);
    assert_eq!(counts.events, 5);
}

#[test]
fn counts_of_two_streams_add_up_and_keep_the_worse_latency() {
    let mut counts = HistoryCounts {
        appends_committed: 3,
        appends_unknown: 1,
        appends_refused: 0,
        records_read: 6,
        worst_append_latency: Duration::from_millis(40),
        events: 15,
    };
    let other_stream = HistoryCounts {
        appends_committed: 2,
        appends_unknown: 0,
        appends_refused: 1,
        records_read: 4,
        worst_append_latency: Duration::from_millis(90),
        events: 10,
    };

    counts.add(&other_stream);

    assert_eq!(
        counts,
        HistoryCounts {
            appends_committed: 5,
            appends_unknown: 1,
            appends_refused: 1,
            records_read: 10,
            worst_append_latency: Duration::from_millis(90),
            events: 25,
        }
    );
}

#[test]
fn summary_of_a_new_run_is_clean_and_empty() {
    let summary = Summary::of_run("soak-7");

    assert_eq!(summary.verdict, RunVerdict::Clean);
    assert_eq!(summary.rounds, 0);
}

#[test]
fn summary_adds_its_rounds_up() {
    let mut summary = Summary::of_run("soak-7");

    summary.add_round(&clean_round());
    summary.add_round(&clean_round());

    assert_eq!(summary.rounds, 2);
    assert_eq!(summary.clean_rounds, 2);
    assert_eq!(summary.appends_committed, 6);
    assert_eq!(summary.records_read, 12);
    assert_eq!(summary.faults, BTreeMap::from([("kill_replica", 2)]));
    assert_eq!(summary.takeovers, 2);
}

#[test]
fn round_that_is_not_clean_is_counted_as_a_round_only() {
    let mut summary = Summary::of_run("soak-7");
    let mut violated_round = clean_round();
    violated_round.verdict = RoundVerdict::Violated;

    summary.add_round(&violated_round);

    assert_eq!(summary.rounds, 1);
    assert_eq!(summary.clean_rounds, 0);
}

#[test]
fn summary_keeps_the_worst_latency_of_its_rounds() {
    let mut summary = Summary::of_run("soak-7");
    let mut slow_round = clean_round();
    slow_round.worst_append_latency_milliseconds = 163;

    summary.add_round(&slow_round);
    summary.add_round(&clean_round());

    assert_eq!(summary.worst_append_latency_milliseconds, 163);
}

/// A week of clean rounds must not read as a week of survived partitions: what was given up stays in the summary.
#[test]
fn summary_keeps_the_kinds_of_fault_its_rounds_gave_up() {
    let mut summary = Summary::of_run("soak-7");
    let mut round_that_gave_cuts_up = clean_round();
    round_that_gave_cuts_up
        .faults_disabled
        .insert("cut_zone", "network cuts are not enforced on this cluster".to_owned());

    summary.add_round(&round_that_gave_cuts_up);
    summary.add_round(&clean_round());

    assert_eq!(
        summary.faults_disabled,
        BTreeMap::from([("cut_zone", "network cuts are not enforced on this cluster".to_owned())])
    );
}

#[test]
fn kinds_that_were_given_up_are_printed_with_the_reason() {
    let mut round = clean_round();
    round.faults_disabled.insert("cut_zone", "not enforced".to_owned());

    let line = json_line(&round);

    assert!(
        line.contains("\"faults_disabled\":{\"cut_zone\":\"not enforced\"}"),
        "{line}"
    );
}

/// Adding a round never decides how the run ended: that is for the loop that runs the rounds.
#[test]
fn adding_a_round_leaves_the_verdict_of_the_run_alone() {
    let mut summary = Summary::of_run("soak-7");
    let mut violated_round = clean_round();
    violated_round.verdict = RoundVerdict::Violated;

    summary.add_round(&violated_round);

    assert_eq!(summary.verdict, RunVerdict::Clean);
}

#[test]
fn round_is_printed_as_one_line_with_the_documented_fields() {
    let line = json_line(&clean_round());

    assert_eq!(
        line,
        "{\"kind\":\"round\",\"run\":\"soak-7\",\"round\":1,\"started_at_unix_millis\":1000,\
         \"ended_at_unix_millis\":61000,\"streams\":2,\"appends_committed\":3,\"appends_unknown\":1,\
         \"appends_refused\":0,\"records_read\":6,\"faults\":{\"kill_replica\":1},\"faults_not_injected\":2,\
         \"faults_disabled\":{},\"takeovers\":1,\"worst_append_latency_milliseconds\":40,\"events\":15,\"verdict\":\"clean\",\
         \"violations\":[],\"reason\":null,\"dumps\":[],\"drill\":false}\n"
    );
}

#[test]
fn summary_is_printed_as_one_line_with_the_documented_fields() {
    let mut summary = Summary::of_run("soak-7");
    summary.add_round(&clean_round());
    summary.verdict = RunVerdict::NotRecovered;
    summary.reason = Some("replica bitacora-replica-1 is no longer registered".to_owned());

    let line = json_line(&summary);

    assert_eq!(
        line,
        "{\"kind\":\"summary\",\"run\":\"soak-7\",\"verdict\":\"not_recovered\",\"rounds\":1,\"clean_rounds\":1,\
         \"appends_committed\":3,\"appends_unknown\":1,\"appends_refused\":0,\"records_read\":6,\
         \"faults\":{\"kill_replica\":1},\"faults_disabled\":{},\"takeovers\":1,\
         \"worst_append_latency_milliseconds\":40,\
         \"reason\":\"replica bitacora-replica-1 is no longer registered\"}\n"
    );
}

#[test]
fn every_verdict_of_a_round_has_a_name_a_script_can_match() {
    let mut names = Vec::new();
    for verdict in [
        RoundVerdict::Clean,
        RoundVerdict::Violated,
        RoundVerdict::NotAudited,
        RoundVerdict::Interrupted,
    ] {
        names.push(json_line(&verdict));
    }

    assert_eq!(
        names,
        [
            "\"clean\"\n",
            "\"violated\"\n",
            "\"not_audited\"\n",
            "\"interrupted\"\n"
        ]
    );
}
