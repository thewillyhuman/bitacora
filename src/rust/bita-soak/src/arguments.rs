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
//! - Every default is modest. A soak is about days of correct behaviour; the cluster it was first written for
//!   needs five to ten milliseconds to make a write durable.
//!
//! # See also
//!
//! - `main.rs`: parses the arguments and runs what they ask for.
//! - `docs/soak.md`: the options, for whoever writes the manifest.

use std::path::PathBuf;
use std::time::Duration;

use bitacora::DEFAULT_ETCD_KEY_PREFIX;
use clap::Parser;

use crate::nemesis::NemesisSettings;
use crate::round::RoundSettings;
use crate::soak::SoakSettings;
use crate::workload::WorkloadSettings;

/// Everything written after `bita-soak` on a command line.
#[derive(Clone, Debug, PartialEq, Eq, Parser)]
#[command(
    name = "bita-soak",
    version,
    about = "Drive a bitacora cluster for days and judge what clients saw with the simulator's checker."
)]
pub(crate) struct Arguments {
    /// The client URL of an etcd member that holds the cluster's metadata. Repeat it for every member.
    #[arg(long = "etcd-endpoint", value_name = "URL", required = true)]
    pub(crate) etcd_endpoints: Vec<String>,

    /// The text in front of every key of this cluster in etcd.
    #[arg(long, value_name = "PREFIX", default_value = DEFAULT_ETCD_KEY_PREFIX)]
    pub(crate) etcd_key_prefix: String,

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
}

#[cfg(test)]
#[path = "arguments_test.rs"]
mod tests;
