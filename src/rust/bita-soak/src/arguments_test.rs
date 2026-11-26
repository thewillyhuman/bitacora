//! Tests of the command line of `bita-soak`.
//!
//! # What is covered
//!
//! - The defaults: an hour of rounds of ten minutes, four streams at twenty appends a second.
//! - What the parser refuses: no etcd endpoint, a rate of zero, no stream, a run or a round of no time.
//! - How the options become the settings of a run: durations from seconds, the name and the seed from the time the
//!   run starts unless the command line gives them, the drill.
//! - The faults: every kind by default, the kinds named, none with `--no-faults`; how often, and how long a cut
//!   lasts; whether a kind of cut that was not in effect is tried again.
//! - Where the faults land: the tool's own namespace unless one is named, the labels of the chart's replicas and
//!   of the test etcd unless others are given, and no selector that selects everything.
//!
//! # How the fixtures work
//!
//! `parse` parses a command line that names one etcd endpoint and whatever options the test adds. `settings_of`
//! turns it into settings for a run that starts at [`STARTED_AT_UNIX_MILLIS`] with a nemesis that does nothing.

use super::*;
use crate::cut_check::CutCheckSettings;

/// When the runs of these tests start.
const STARTED_AT_UNIX_MILLIS: u64 = 1_790_000_000_123;

/// Parses `bita-soak --etcd-endpoint http://etcd:2379` followed by `options`.
fn parse(options: &[&str]) -> Result<Arguments, clap::Error> {
    let mut command_line = vec!["bita-soak", "--etcd-endpoint", "http://etcd:2379"];
    command_line.extend_from_slice(options);
    Arguments::try_parse_from(command_line)
}

/// A nemesis that does nothing.
fn no_nemesis() -> NemesisSettings {
    NemesisSettings {
        kinds: Vec::new(),
        fault_interval: Duration::from_secs(60),
        cut_duration: Duration::from_secs(30),
        cut_check: CutCheckSettings {
            quiet_period: Duration::from_secs(15),
            time_limit: Duration::from_secs(45),
        },
        retries_cuts_not_in_effect: false,
    }
}

/// The nemesis that `options` ask for.
fn nemesis_of(options: &[&str]) -> NemesisSettings {
    parse(options).expect("a valid command line").nemesis_settings()
}

/// The labels `pairs`, as a selector holds them.
fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    for (name, value) in pairs {
        labels.insert((*name).to_owned(), (*value).to_owned());
    }
    labels
}

/// The settings of the run that `options` ask for.
fn settings_of(options: &[&str]) -> SoakSettings {
    let arguments = parse(options).expect("a valid command line");
    arguments.soak_settings(STARTED_AT_UNIX_MILLIS, no_nemesis())
}

#[test]
fn run_lasts_an_hour_in_rounds_of_ten_minutes_by_default() {
    let settings = settings_of(&[]);

    assert_eq!(settings.duration, Duration::from_secs(3_600));
    assert_eq!(settings.round.duration, Duration::from_secs(600));
}

#[test]
fn round_writes_four_streams_at_twenty_appends_a_second_by_default() {
    let settings = settings_of(&[]);

    assert_eq!(settings.round.stream_count, 4);
    assert_eq!(
        settings.round.workload,
        WorkloadSettings {
            appends_per_second: 20,
            record_bytes: 64,
        }
    );
}

#[test]
fn cluster_is_given_ten_minutes_to_heal_by_default() {
    let settings = settings_of(&[]);

    assert_eq!(settings.round.heal_time_limit, Duration::from_secs(600));
}

#[test]
fn dumps_go_to_the_temporary_directory_by_default() {
    let settings = settings_of(&[]);

    assert_eq!(settings.round.dump_directory, PathBuf::from("/tmp"));
}

#[test]
fn metrics_are_not_served_and_the_tool_does_not_linger_by_default() {
    let arguments = parse(&[]).expect("a valid command line");

    assert_eq!(arguments.metrics_listen_address, None);
    assert_eq!(arguments.linger_seconds, 0);
}

#[test]
fn etcd_endpoint_can_be_repeated() {
    let arguments = parse(&["--etcd-endpoint", "http://etcd-1:2379"]).expect("a valid command line");

    assert_eq!(arguments.etcd_endpoints, ["http://etcd:2379", "http://etcd-1:2379"]);
}

#[test]
fn command_line_without_an_etcd_endpoint_is_refused() {
    let parsed = Arguments::try_parse_from(["bita-soak"]);

    assert!(parsed.is_err());
}

#[test]
fn rate_of_zero_is_refused() {
    assert!(parse(&["--rate", "0"]).is_err());
}

/// A round without streams would have nothing to audit.
#[test]
fn round_without_streams_is_refused() {
    assert!(parse(&["--streams", "0"]).is_err());
}

/// A run of no time would play no round and still end as clean.
#[test]
fn run_of_no_time_is_refused() {
    assert!(parse(&["--seconds", "0"]).is_err());
}

#[test]
fn round_of_no_time_is_refused() {
    assert!(parse(&["--round-seconds", "0"]).is_err());
}

#[test]
fn durations_are_given_in_seconds() {
    let settings = settings_of(&["--seconds", "604800", "--round-seconds", "900", "--heal-seconds", "120"]);

    assert_eq!(settings.duration, Duration::from_hours(168));
    assert_eq!(settings.round.duration, Duration::from_mins(15));
    assert_eq!(settings.round.heal_time_limit, Duration::from_secs(120));
}

#[test]
fn workload_follows_the_options() {
    let settings = settings_of(&["--streams", "2", "--rate", "5", "--record-bytes", "256"]);

    assert_eq!(settings.round.stream_count, 2);
    assert_eq!(
        settings.round.workload,
        WorkloadSettings {
            appends_per_second: 5,
            record_bytes: 256,
        }
    );
}

/// Two runs on one cluster must not write the same streams, so a run that was given no name takes the second it
/// starts in.
#[test]
fn run_is_named_after_the_time_it_starts_unless_it_is_given_a_name() {
    assert_eq!(settings_of(&[]).run, "soak-1790000000");
    assert_eq!(settings_of(&["--run", "nightly-12"]).run, "nightly-12");
}

#[test]
fn seed_is_the_time_the_run_starts_unless_it_is_given() {
    assert_eq!(settings_of(&[]).seed, STARTED_AT_UNIX_MILLIS);
    assert_eq!(settings_of(&["--seed", "7"]).seed, 7);
}

#[test]
fn drill_is_off_unless_asked_for() {
    assert!(!settings_of(&[]).plants_lost_record);
    assert!(settings_of(&["--drill-lost-record"]).plants_lost_record);
}

#[test]
fn nemesis_of_the_rounds_is_the_one_handed_in() {
    let settings = settings_of(&[]);

    assert_eq!(settings.round.nemesis, no_nemesis());
}

#[test]
fn etcd_key_prefix_is_the_cluster_default_unless_given() {
    let by_default = parse(&[]).expect("a valid command line");
    let given = parse(&["--etcd-key-prefix", "/staging"]).expect("a valid command line");

    assert_eq!(by_default.etcd_key_prefix, DEFAULT_ETCD_KEY_PREFIX);
    assert_eq!(given.etcd_key_prefix, "/staging");
}

#[test]
fn every_kind_of_fault_is_injected_by_default() {
    let nemesis = nemesis_of(&[]);

    assert_eq!(nemesis.kinds, FaultKind::ALL);
}

#[test]
fn only_the_kinds_that_are_named_are_injected() {
    let nemesis = nemesis_of(&["--faults", "kill-replica,cut-zone"]);

    assert_eq!(nemesis.kinds, [FaultKind::KillReplica, FaultKind::CutZone]);
}

#[test]
fn no_kind_is_injected_when_no_faults_are_asked_for() {
    let nemesis = nemesis_of(&["--no-faults"]);

    assert_eq!(nemesis.kinds, []);
}

/// Asking for faults and for none is a mistake, not a choice the tool should make.
#[test]
fn faults_and_no_faults_together_are_refused() {
    assert!(parse(&["--no-faults", "--faults", "kill-replica"]).is_err());
}

#[test]
fn kind_of_fault_that_does_not_exist_is_refused() {
    assert!(parse(&["--faults", "kill-replica,lose-disk"]).is_err());
}

#[test]
fn fault_is_due_every_minute_and_a_cut_lasts_thirty_seconds_by_default() {
    let nemesis = nemesis_of(&[]);

    assert_eq!(nemesis.fault_interval, Duration::from_secs(60));
    assert_eq!(nemesis.cut_duration, Duration::from_secs(30));
}

#[test]
fn pace_of_the_faults_follows_the_options() {
    let nemesis = nemesis_of(&["--fault-interval-seconds", "20", "--cut-seconds", "45"]);

    assert_eq!(nemesis.fault_interval, Duration::from_secs(20));
    assert_eq!(nemesis.cut_duration, Duration::from_secs(45));
}

#[test]
fn fault_interval_of_no_time_is_refused() {
    assert!(parse(&["--fault-interval-seconds", "0"]).is_err());
}

#[test]
fn kind_of_cut_that_was_not_in_effect_is_given_up_unless_asked_otherwise() {
    assert!(!nemesis_of(&[]).retries_cuts_not_in_effect);
    assert!(nemesis_of(&["--retry-cuts-not-in-effect"]).retries_cuts_not_in_effect);
}

/// The chart's replicas register every five seconds: three missed renewals are fifteen seconds, nine are
/// forty-five.
#[test]
fn cut_counts_after_three_missed_renewals_and_is_given_nine_by_default() {
    let nemesis = nemesis_of(&[]);

    assert_eq!(
        nemesis.cut_check,
        CutCheckSettings {
            quiet_period: Duration::from_secs(15),
            time_limit: Duration::from_secs(45),
        }
    );
}

/// With replicas that register every twenty seconds, fifteen seconds of silence would prove nothing.
#[test]
fn check_of_a_cut_follows_the_registration_interval_of_the_replicas() {
    let nemesis = nemesis_of(&["--registration-interval-seconds", "20"]);

    assert_eq!(
        nemesis.cut_check,
        CutCheckSettings {
            quiet_period: Duration::from_mins(1),
            time_limit: Duration::from_mins(3),
        }
    );
}

#[test]
fn registration_interval_of_no_time_is_refused() {
    assert!(parse(&["--registration-interval-seconds", "0"]).is_err());
}

#[test]
fn faults_land_in_the_namespace_of_the_tool_unless_one_is_named() {
    let by_default = parse(&[]).expect("a valid command line");
    let named = parse(&["--namespace", "staging"]).expect("a valid command line");

    assert_eq!(by_default.kubernetes_targets("storage").namespace, "storage");
    assert_eq!(named.kubernetes_targets("storage").namespace, "staging");
}

/// The labels of the chart's replicas, of `development/kubernetes/etcd.yaml`, and the zone label of Kubernetes.
#[test]
fn pods_are_found_by_the_labels_of_the_chart_by_default() {
    let targets = parse(&[]).expect("a valid command line").kubernetes_targets("storage");

    assert_eq!(
        targets.replica_labels,
        labels(&[
            ("app.kubernetes.io/name", "bitacora"),
            ("app.kubernetes.io/component", "replica")
        ])
    );
    assert_eq!(targets.etcd_labels, labels(&[("app.kubernetes.io/name", "etcd")]));
    assert_eq!(targets.zone_label, "topology.kubernetes.io/zone");
}

#[test]
fn labels_of_the_pods_follow_the_options() {
    let arguments = parse(&[
        "--replica-selector",
        "app=log,role=replica",
        "--etcd-selector",
        "app=etcd",
        "--zone-label",
        "example.org/zone",
    ])
    .expect("a valid command line");

    let targets = arguments.kubernetes_targets("storage");

    assert_eq!(targets.replica_labels, labels(&[("app", "log"), ("role", "replica")]));
    assert_eq!(targets.etcd_labels, labels(&[("app", "etcd")]));
    assert_eq!(targets.zone_label, "example.org/zone");
}

/// A selector without labels would select every pod of the namespace.
#[test]
fn selector_that_is_not_made_of_labels_is_refused() {
    assert!(parse(&["--replica-selector", ""]).is_err());
    assert!(parse(&["--etcd-selector", "etcd"]).is_err());
}
