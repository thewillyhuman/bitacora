//! The command line of `bita-bench`.
//!
//! # Why this file exists
//!
//! A benchmark is only repeatable if the whole workload is on the command line: the same line must mean
//! the same load next month. This file is that command line, in one place, with the defaults that a line
//! leaves unsaid.
//!
//! # What it contains
//!
//! - [`Arguments`]: where the cluster is, how to print, and the workload.
//! - [`WorkloadCommand`]: the two workloads, `append` and `read`, with their options.
//! - [`workload_limit`]: when a workload stops, from its two optional limits.
//!
//! # How it works
//!
//! `clap` derives the parser and the help from the types; the doc comment of each item is its help text.
//! A workload stops after a number of seconds or a number of records. Giving both is a usage error;
//! giving neither means [`DEFAULT_SECONDS`].
//!
//! The cluster is named by a configuration file or by etcd endpoints, as for `bita`, but without
//! environment variables: a benchmark's command line should say everything.
//!
//! # Invariants
//!
//! - Parsing has no side effect.
//!
//! # See also
//!
//! - `workload.rs`: what the options mean.
//! - `cluster.rs`: turns the cluster options into a configuration.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};

use crate::report::OutputFormat;
use crate::workload::WorkloadLimit;

/// How long a workload runs if the command line gives no limit.
pub(crate) const DEFAULT_SECONDS: u64 = 10;

/// The most bytes of records a writer sends a replica in one request if the command line does not say: what an
/// application with default settings sends. A test holds it to the client's own default.
pub(crate) const DEFAULT_REQUEST_BYTES: usize = 256 * 1024;

/// Everything written after `bita-bench` on a command line.
#[derive(Clone, Debug, PartialEq, Eq, Parser)]
#[command(
    name = "bita-bench",
    version,
    about = "Drive a bitacora cluster with a workload and report throughput and latency.",
    propagate_version = true
)]
pub(crate) struct Arguments {
    /// A TOML file with the cluster configuration: etcd endpoints, key prefix and object store.
    #[arg(long, global = true, value_name = "FILE")]
    pub(crate) config: Option<PathBuf>,

    /// The client URL of an etcd member that holds the cluster's metadata. Repeat it for every member.
    #[arg(long = "etcd-endpoint", global = true, value_name = "URL")]
    pub(crate) etcd_endpoints: Vec<String>,

    /// The text in front of every key of this cluster in etcd.
    #[arg(long, global = true, value_name = "PREFIX")]
    pub(crate) etcd_key_prefix: Option<String>,

    /// How the result is printed: one value per line, or JSON with stable field names.
    #[arg(long, global = true, value_enum, default_value = "text")]
    pub(crate) output: OutputFormat,

    /// The workload to run.
    #[command(subcommand)]
    pub(crate) workload: WorkloadCommand,
}

/// The workloads the tool can run.
#[derive(Clone, Debug, PartialEq, Eq, Subcommand)]
pub(crate) enum WorkloadCommand {
    /// Append records and measure how long each append takes to commit.
    Append {
        /// The stream to append to. The tool becomes its writer and fences any writer that held it.
        stream: String,
        /// The payload size of every record, in bytes.
        #[arg(long, default_value_t = 1024, value_name = "BYTES")]
        record_bytes: usize,
        /// How many records one append carries.
        #[arg(long, default_value_t = 1, value_name = "RECORDS")]
        records_per_append: usize,
        /// How many appends are in flight at any moment, per stream.
        #[arg(long, default_value_t = 16, value_name = "APPENDS")]
        appends_in_flight: usize,
        /// The most bytes of records the writer sends a replica in one request. A replica makes a
        /// request durable with one flush, so this bounds what one flush carries.
        #[arg(long, default_value_t = DEFAULT_REQUEST_BYTES, value_name = "BYTES")]
        request_bytes: usize,
        /// Stop after this many seconds. Without --seconds and --records, the workload runs for 10
        /// seconds.
        #[arg(long, conflicts_with = "records")]
        seconds: Option<u64>,
        /// Stop after this many records per stream, rounded up to whole appends.
        #[arg(long)]
        records: Option<u64>,
        /// Pace each stream to this many records per second, instead of appending as fast as the
        /// cluster commits. Latency is then measured from the moment each append was due.
        #[arg(long, value_name = "RECORDS_PER_SECOND")]
        rate: Option<u64>,
        /// Write this many streams at once. With more than one, they are called STREAM-0, STREAM-1 and
        /// so on.
        #[arg(long, default_value_t = 1)]
        streams: u64,
        /// Create the streams that do not exist, with three replicas and a write quorum of two.
        #[arg(long)]
        create: bool,
        /// Take each stream from a live writer at once, instead of waiting for its lease to expire.
        #[arg(long)]
        force: bool,
    },
    /// Scan a stream from a sequence number on and measure records and bytes per second.
    Read {
        /// The stream to read.
        stream: String,
        /// The first sequence number to read. Without it, the scan starts at the oldest record still
        /// kept.
        #[arg(long, value_name = "SEQUENCE")]
        from: Option<u64>,
        /// Stop after this many seconds, or at the end of the stream. Without --seconds and --records,
        /// the scan runs for 10 seconds.
        #[arg(long, conflicts_with = "records")]
        seconds: Option<u64>,
        /// Stop after this many records, or at the end of the stream.
        #[arg(long)]
        records: Option<u64>,
    },
}

/// Works out when a workload stops from its two optional limits.
///
/// # Arguments
///
/// * `seconds` - The value of `--seconds`, if given.
/// * `records` - The value of `--records`, if given. The parser refuses a command line with both.
///
/// # Returns
///
/// The record limit if one was given, otherwise the duration, which defaults to [`DEFAULT_SECONDS`].
pub(crate) fn workload_limit(seconds: Option<u64>, records: Option<u64>) -> WorkloadLimit {
    match (records, seconds) {
        (Some(records), _) => WorkloadLimit::Records(records),
        (None, Some(seconds)) => WorkloadLimit::Duration(Duration::from_secs(seconds)),
        (None, None) => WorkloadLimit::Duration(Duration::from_secs(DEFAULT_SECONDS)),
    }
}

#[cfg(test)]
#[path = "arguments_test.rs"]
mod tests;
