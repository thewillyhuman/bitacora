//! Tests of the metrics of a soak.
//!
//! # What is covered
//!
//! - Every metric the tool registers is listed in `docs/soak.md`.
//! - A round raises the counters by what its report says: its verdict, its appends, its reads, its takeovers and
//!   its faults by kind.
//! - An interrupted round is no verdict on the cluster and raises none of the three verdict counters.
//!
//! # How the fixtures work
//!
//! The reference is read from the repository. Bazel runs tests from the root of the repository's files and Cargo
//! from the crate's directory, so `read_reference` tries both locations. `round_report` is the report of a round
//! with a few appends and two killed replicas.

// The test reads the reference document from the repository.
#![allow(clippy::disallowed_methods)]

use bitacora_process::undocumented_metrics;

use super::*;

/// Where the reference lies, seen from where Bazel and Cargo run the tests.
const REFERENCE_LOCATIONS: [&str; 2] = ["docs/soak.md", "../../../docs/soak.md"];

/// Reads `docs/soak.md`.
fn read_reference() -> String {
    for location in REFERENCE_LOCATIONS {
        if let Ok(reference) = std::fs::read_to_string(location) {
            return reference;
        }
    }
    panic!("the reference of the soak tool is at none of {REFERENCE_LOCATIONS:?}");
}

/// The report of a round that ended as `verdict` after seven commits, with two killed replicas.
fn round_report(verdict: RoundVerdict) -> RoundReport {
    RoundReport {
        kind: "round",
        run: "soak-7".to_owned(),
        round: 1,
        started_at_unix_millis: 1_000,
        ended_at_unix_millis: 61_000,
        streams: 2,
        appends_committed: 7,
        appends_unknown: 2,
        appends_refused: 1,
        records_read: 14,
        faults: BTreeMap::from([("kill_replica", 2), ("cut_zone", 0)]),
        faults_not_injected: 0,
        faults_disabled: BTreeMap::new(),
        takeovers: 3,
        worst_append_latency_milliseconds: 40,
        events: 30,
        verdict,
        violations: Vec::new(),
        reason: None,
        dumps: Vec::new(),
        drill: false,
    }
}

/// The exposition of the metrics after one round that ended as `verdict`.
fn exposition_after_a_round(verdict: RoundVerdict) -> String {
    let metrics = SoakMetrics::new();
    metrics.count_round(&round_report(verdict));
    metrics.registry().encode()
}

/// The reference is what a rule or a dashboard is written from: a metric must not ship without its entry.
#[test]
fn every_metric_of_the_tool_is_in_the_reference() {
    let metrics = SoakMetrics::new();

    let undocumented = undocumented_metrics(&metrics.registry().encode(), &read_reference());

    assert_eq!(undocumented, Vec::<String>::new());
}

#[test]
fn clean_round_is_counted_as_clean() {
    let exposition = exposition_after_a_round(RoundVerdict::Clean);

    assert!(
        exposition.contains("bitacora_soak_rounds_clean_total 1\n"),
        "{exposition}"
    );
}

#[test]
fn violated_round_is_counted_as_violated() {
    let exposition = exposition_after_a_round(RoundVerdict::Violated);

    assert!(
        exposition.contains("bitacora_soak_rounds_violated_total 1\n"),
        "{exposition}"
    );
    assert!(
        exposition.contains("bitacora_soak_rounds_clean_total 0\n"),
        "{exposition}"
    );
}

#[test]
fn round_that_was_not_audited_is_counted_as_such() {
    let exposition = exposition_after_a_round(RoundVerdict::NotAudited);

    assert!(
        exposition.contains("bitacora_soak_rounds_not_audited_total 1\n"),
        "{exposition}"
    );
    assert!(
        exposition.contains("bitacora_soak_rounds_clean_total 0\n"),
        "{exposition}"
    );
}

#[test]
fn interrupted_round_raises_no_verdict_counter() {
    let exposition = exposition_after_a_round(RoundVerdict::Interrupted);

    assert!(
        exposition.contains("bitacora_soak_rounds_clean_total 0\n"),
        "{exposition}"
    );
    assert!(
        exposition.contains("bitacora_soak_rounds_violated_total 0\n"),
        "{exposition}"
    );
    assert!(
        exposition.contains("bitacora_soak_rounds_not_audited_total 0\n"),
        "{exposition}"
    );
}

#[test]
fn appends_are_counted_by_how_they_ended() {
    let exposition = exposition_after_a_round(RoundVerdict::Clean);

    assert!(
        exposition.contains("bitacora_soak_appends_committed_total 7\n"),
        "{exposition}"
    );
    assert!(
        exposition.contains("bitacora_soak_appends_unknown_total 2\n"),
        "{exposition}"
    );
    assert!(
        exposition.contains("bitacora_soak_appends_refused_total 1\n"),
        "{exposition}"
    );
}

#[test]
fn reads_and_takeovers_are_counted() {
    let exposition = exposition_after_a_round(RoundVerdict::Clean);

    assert!(
        exposition.contains("bitacora_soak_records_read_total 14\n"),
        "{exposition}"
    );
    assert!(exposition.contains("bitacora_soak_takeovers_total 3\n"), "{exposition}");
}

#[test]
fn faults_are_counted_by_kind() {
    let exposition = exposition_after_a_round(RoundVerdict::Clean);

    assert!(
        exposition.contains("bitacora_soak_faults_kill_replica_total 2\n"),
        "{exposition}"
    );
    assert!(
        exposition.contains("bitacora_soak_faults_cut_zone_total 0\n"),
        "{exposition}"
    );
}

#[test]
fn counters_add_up_over_rounds() {
    let metrics = SoakMetrics::new();

    metrics.count_round(&round_report(RoundVerdict::Clean));
    metrics.count_round(&round_report(RoundVerdict::Clean));

    let exposition = metrics.registry().encode();
    assert!(
        exposition.contains("bitacora_soak_appends_committed_total 14\n"),
        "{exposition}"
    );
}
