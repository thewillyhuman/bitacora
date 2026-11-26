//! The command line of `bita-soak`.
//!
//! # Why this file exists
//!
//! A soak runs in a pod, started by a manifest, and the manifest is where its settings are written down. So every
//! setting is an option of the command line, with a default that suits a first try: a person who reads the Job
//! knows everything about the run. This file defines the options and turns them into the settings the run works
//! with.
//!
//! # What it contains
//!
//! - [`Arguments`]: everything written after `bita-soak`.
//! - [`Arguments::soak_settings`]: the settings of the run those arguments ask for.
//! - [`Arguments::nemesis_settings`]: which faults the run injects, and how often.
//! - [`Arguments::kubernetes_targets`]: where the pods are that the faults hit.
//!
//! # How it works
//!
//! The parser is derived by `clap`. Durations are given in whole seconds, as in `bita-bench`. The name of the run
//! and the seed of the nemesis have no fixed default: without them, both are taken from the time the run starts, so
//! that two runs on one cluster do not write the same streams.
//!
//! # Invariants
//!
//! - Parsing reads nothing but the command line: no environment variable, no file.
//! - Every default is modest. A soak is about days of correct behaviour, not about load, and a cluster on network
//!   volumes needs several milliseconds to make a write durable.
//!
//! # See also
//!
//! - `main.rs`: parses the arguments and runs what they ask for.
//! - `docs/soak.md`: the options, for whoever writes the manifest.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

use crate::cut_check::CutCheckSettings;
use crate::faults::FaultKind;
use crate::kubernetes_faults::{KubernetesTargets, parse_labels};
use crate::nemesis::NemesisSettings;
use crate::round::RoundSettings;
use crate::soak::SoakSettings;
use crate::workload::WorkloadSettings;

/// How many renewals of its registration a replica must miss in a row for a network cut to count. One could be
/// late; three in a row are a replica that cannot reach etcd.
const MISSED_RENEWALS_OF_A_CUT: u32 = 3;

/// How many registration intervals a network cut is given to silence the replicas behind it. A network plugin
/// needs a moment to apply a policy; one that has not after this long is not going to.
const RENEWALS_A_CUT_IS_GIVEN: u32 = 9;

/// Everything written after `bita-soak` on a command line.
#[derive(Clone, Debug, PartialEq, Eq, Parser)]
#[command(
    name = "bita-soak",
    version,
    about = "Drive a bitacora cluster for days and judge what clients saw with the simulator's checker."
)]
pub(crate) struct Arguments {
    /// A TOML file with the cluster configuration: etcd endpoints, key prefix and object store. It is the only way
    /// to name the object store, which the readers and the audit need on a cluster that tiers its segments.
    #[arg(long, value_name = "FILE")]
    pub(crate) config: Option<PathBuf>,

    /// The client URL of an etcd member that holds the cluster's metadata. Repeat it for every member. Required
    /// without --config; with it, the option replaces the members the file names.
    #[arg(long = "etcd-endpoint", value_name = "URL", required_unless_present = "config")]
    pub(crate) etcd_endpoints: Vec<String>,

    /// The text in front of every key of this cluster in etcd. Without it, the prefix is the file's, or
    /// `/bitacora/` without a file.
    #[arg(long, value_name = "PREFIX")]
    pub(crate) etcd_key_prefix: Option<String>,

    /// The name of the run, which starts the name of every stream it makes. Without it, the run is named after the
    /// time it starts.
    #[arg(long, value_name = "NAME")]
    pub(crate) run: Option<String>,

    /// How long the run lasts, in seconds, at least one. A round that has begun is finished.
    #[arg(long, default_value_t = 3_600, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) seconds: u64,

    /// How long the workload of one round runs, in seconds, at least one.
    #[arg(long, default_value_t = 600, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) round_seconds: u64,

    /// How many streams a round writes, each with a writer and a reader of its own. At least one.
    #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) streams: u64,

    /// How many appends a writer makes per second, at most. At least one.
    #[arg(
        long,
        default_value_t = 20,
        value_name = "APPENDS_PER_SECOND",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub(crate) rate: u64,

    /// The payload size of every record, in bytes.
    #[arg(long, default_value_t = 64, value_name = "BYTES")]
    pub(crate) record_bytes: usize,

    /// How long the cluster is given to recover once a round's faults are gone, in seconds.
    #[arg(long, default_value_t = 600)]
    pub(crate) heal_seconds: u64,

    /// The directory the histories of a round that is not clean are written to. It must exist.
    #[arg(long, value_name = "DIRECTORY", default_value = "/tmp")]
    pub(crate) dump_directory: PathBuf,

    /// The address to serve /metrics on, such as 0.0.0.0:9102. Without it, no metrics are served.
    #[arg(long, value_name = "ADDRESS")]
    pub(crate) metrics_listen_address: Option<String>,

    /// How long the tool stays up after a run that was not clean, in seconds, so that its files can be copied and
    /// its metrics scraped before the pod is gone.
    #[arg(long, default_value_t = 0)]
    pub(crate) linger_seconds: u64,

    /// The seed of the nemesis's choices. Without it, the seed is taken from the time the run starts.
    #[arg(long)]
    pub(crate) seed: Option<u64>,

    /// The kinds of fault to inject, separated by commas. A kind that is named twice is twice as likely.
    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        value_name = "KIND,...",
        default_value = "kill-replica,kill-controller,kill-etcd-member,cut-replica,cut-zone"
    )]
    pub(crate) faults: Vec<FaultKind>,

    /// Inject no faults: rounds are plain workloads, judged. The tool then needs no access to the Kubernetes API.
    #[arg(long, conflicts_with = "faults")]
    pub(crate) no_faults: bool,

    /// How long the nemesis waits between two faults, in seconds, at least one. No fault is injected while the
    /// cluster has not recovered from the one before.
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) fault_interval_seconds: u64,

    /// How long a network cut stays in place once it was seen to be in effect, in seconds.
    #[arg(long, default_value_t = 30)]
    pub(crate) cut_seconds: u64,

    /// How often a replica renews its registration in etcd, in seconds, at least one: the chart's
    /// `replica.registrationIntervalSeconds`. A network cut counts once the replicas behind it have missed three
    /// renewals in a row, and is given nine intervals to get there.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) registration_interval_seconds: u64,

    /// Keep trying a kind of network cut that was seen not to be in effect. Without this, such a kind is given up
    /// for the rest of the run. Either way a cut counts only if it was seen to be in effect.
    #[arg(long)]
    pub(crate) retry_cuts_not_in_effect: bool,

    /// The namespace the cluster's pods run in. Without it, the namespace of the tool's own pod.
    #[arg(long, value_name = "NAMESPACE")]
    pub(crate) namespace: Option<String>,

    /// The labels every replica pod carries, as name=value pairs separated by commas.
    #[arg(
        long,
        value_name = "LABELS",
        value_parser = parse_labels,
        default_value = "app.kubernetes.io/name=bitacora,app.kubernetes.io/component=replica"
    )]
    pub(crate) replica_selector: BTreeMap<String, String>,

    /// The labels every pod of the etcd cluster carries, as name=value pairs separated by commas.
    #[arg(
        long,
        value_name = "LABELS",
        value_parser = parse_labels,
        default_value = "app.kubernetes.io/name=etcd"
    )]
    pub(crate) etcd_selector: BTreeMap<String, String>,

    /// The name of the pod label that holds a pod's zone.
    #[arg(long, value_name = "LABEL", default_value = "topology.kubernetes.io/zone")]
    pub(crate) zone_label: String,

    /// A drill: claim, in the first round, a commit that never happened. The run must end with a violation, which
    /// proves that the alarm is wired and says nothing about the cluster.
    #[arg(long)]
    pub(crate) drill_lost_record: bool,
}

impl Arguments {
    /// Works out the settings of the run these arguments ask for.
    ///
    /// # Arguments
    ///
    /// * `started_at_unix_millis` - When the run starts, which names the run and seeds the nemesis unless the
    ///   command line does.
    /// * `nemesis` - What the nemesis of every round does.
    ///
    /// # Returns
    ///
    /// The settings, with every duration converted from seconds.
    pub(crate) fn soak_settings(&self, started_at_unix_millis: u64, nemesis: NemesisSettings) -> SoakSettings {
        let run = match &self.run {
            Some(run) => run.clone(),
            None => format!("soak-{}", started_at_unix_millis / 1_000),
        };
        SoakSettings {
            run,
            duration: Duration::from_secs(self.seconds),
            seed: self.seed.unwrap_or(started_at_unix_millis),
            round: RoundSettings {
                duration: Duration::from_secs(self.round_seconds),
                stream_count: self.streams,
                workload: WorkloadSettings {
                    appends_per_second: self.rate,
                    record_bytes: self.record_bytes,
                },
                nemesis,
                heal_time_limit: Duration::from_secs(self.heal_seconds),
                dump_directory: self.dump_directory.clone(),
            },
            plants_lost_record: self.drill_lost_record,
        }
    }

    /// Works out what the nemesis of every round does.
    ///
    /// # Returns
    ///
    /// The settings. With `--no-faults` they name no kind of fault. A network cut is checked against the
    /// registration interval of the replicas: [`MISSED_RENEWALS_OF_A_CUT`] intervals of silence make a cut, within
    /// [`RENEWALS_A_CUT_IS_GIVEN`] intervals.
    pub(crate) fn nemesis_settings(&self) -> NemesisSettings {
        let kinds = if self.no_faults {
            Vec::new()
        } else {
            self.faults.clone()
        };
        let registration_interval = Duration::from_secs(self.registration_interval_seconds);
        NemesisSettings {
            kinds,
            fault_interval: Duration::from_secs(self.fault_interval_seconds),
            cut_duration: Duration::from_secs(self.cut_seconds),
            cut_check: CutCheckSettings {
                quiet_period: registration_interval * MISSED_RENEWALS_OF_A_CUT,
                time_limit: registration_interval * RENEWALS_A_CUT_IS_GIVEN,
            },
            retries_cuts_not_in_effect: self.retry_cuts_not_in_effect,
        }
    }

    /// Says where the cluster's pods are and how they are labelled.
    ///
    /// # Arguments
    ///
    /// * `own_namespace` - The namespace of the tool's own pod, which is used unless the command line names one.
    pub(crate) fn kubernetes_targets(&self, own_namespace: &str) -> KubernetesTargets {
        KubernetesTargets {
            namespace: self.namespace.clone().unwrap_or_else(|| own_namespace.to_owned()),
            replica_labels: self.replica_selector.clone(),
            etcd_labels: self.etcd_selector.clone(),
            zone_label: self.zone_label.clone(),
        }
    }
}

#[cfg(test)]
#[path = "arguments_test.rs"]
mod tests;
