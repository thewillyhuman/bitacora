//! `bita-bench`: drives a bitacora cluster with a workload and reports throughput and latency.
//!
//! # Why this file exists
//!
//! The design sets performance targets (section 16) and asks for a benchmark tool to check them and to
//! catch regressions (sections 15 and 16). The targets are about what an application sees: how long an
//! append takes to commit and how much a stream takes per second. So the tool goes through the same
//! client library and the same network path as an application. This file is its entry point.
//!
//! # What it contains
//!
//! - `main`: parses the command line, finds the cluster, runs the workload and prints the report.
//! - The modules of the tool: `arguments` (the command line), `cluster` (finding the cluster), `workload`
//!   (driving and measuring), `histogram` and `pacing` (the measuring instruments), `report` (the
//!   output).
//!
//! # How it works
//!
//! 1. The command line is parsed and the cluster configuration worked out. A mistake in either ends the
//!    tool with exit code 2.
//! 2. A client is connected, as an application would connect one.
//! 3. The workload runs. For `append`, the streams are created first if `--create` was given.
//! 4. The report is printed, as text or as JSON.
//!
//! Exit codes: 0 if the workload ran to its limit, 1 if it could not run or stopped early, 2 if the
//! command line or the configuration is wrong.
//!
//! The tool reports what it measured and nothing else. It knows no targets and judges no result:
//! comparing a run with a target or with an earlier run is for whoever runs it.
//!
//! # Invariants
//!
//! - The report goes to standard output and nothing else does; errors go to standard error.
//! - A workload that stopped early still prints what it measured, and the tool then exits with 1.
//!
//! # See also
//!
//! - `workload.rs`: what is measured and how.
//! - `docs/cli.md`: the tool's options with examples.

use std::io::Write;
use std::process::ExitCode;

use bitacora::{BitacoraClient, ClientSettings, ClusterConfig, TakeoverPolicy, WriterSettings};
use clap::Parser;

use crate::arguments::{Arguments, WorkloadCommand, workload_limit};
use crate::cluster::resolve_cluster_config;
use crate::report::{AppendReport, OutputFormat, ReadReport};
use crate::workload::{AppendWorkload, ReadWorkload, create_missing_streams, run_append_workloads, run_read_workload};

mod arguments;
mod cluster;
mod histogram;
mod pacing;
mod report;
mod workload;

/// The exit code of a workload that could not run or stopped early.
const EXIT_FAILURE: u8 = 1;
/// The exit code of a command line or a configuration that is wrong.
const EXIT_USAGE: u8 = 2;
/// The name the tool's writers record with their lease.
const WRITER_IDENTITY: &str = "bita-bench";

/// Runs the tool and returns its exit code.
fn main() -> ExitCode {
    let arguments = match Arguments::try_parse() {
        Ok(arguments) => arguments,
        Err(usage_error) => {
            // Help and version requests arrive here too; they go to standard output and succeed.
            let is_mistake = usage_error.use_stderr();
            let _ = usage_error.print();
            return if is_mistake {
                ExitCode::from(EXIT_USAGE)
            } else {
                ExitCode::SUCCESS
            };
        }
    };
    let config = match resolve_cluster_config(
        arguments.config.as_deref(),
        &arguments.etcd_endpoints,
        arguments.etcd_key_prefix.as_deref(),
    ) {
        Ok(config) => config,
        Err(config_error) => {
            eprintln!("bita-bench: {config_error}");
            return ExitCode::from(EXIT_USAGE);
        }
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(io_error) => {
            eprintln!("bita-bench: cannot start: {io_error}");
            return ExitCode::from(EXIT_FAILURE);
        }
    };
    match runtime.block_on(run(&arguments, &config)) {
        Ok(WorkloadOutcome::RanToItsLimit) => ExitCode::SUCCESS,
        Ok(WorkloadOutcome::StoppedEarly) => ExitCode::from(EXIT_FAILURE),
        Err(reason) => {
            eprintln!("bita-bench: {reason}");
            ExitCode::from(EXIT_FAILURE)
        }
    }
}

/// How a workload that could be started ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WorkloadOutcome {
    /// The workload ran until its limit, or to the end of the stream it read.
    RanToItsLimit,
    /// The workload stopped before its limit because an append or a read failed. Its report says why.
    StoppedEarly,
}

/// Connects to the cluster, runs the workload of the command line and prints its report.
///
/// # Errors
///
/// Returns the text to show if the cluster configuration cannot be used, the workload could not start,
/// or the report could not be printed.
async fn run(arguments: &Arguments, config: &ClusterConfig) -> Result<WorkloadOutcome, String> {
    let settings = ClientSettings {
        writer: WriterSettings {
            writer_identity: WRITER_IDENTITY.to_owned(),
            ..WriterSettings::default()
        },
        ..ClientSettings::default()
    };
    let client = match bitacora::connect(config, settings).await {
        Ok(client) => client,
        Err(connect_error) => return Err(connect_error.to_string()),
    };
    let (rendered, has_stopped_early) = match &arguments.workload {
        WorkloadCommand::Append {
            stream,
            record_bytes,
            records_per_append,
            appends_in_flight,
            seconds,
            records,
            rate,
            streams,
            create,
            force,
        } => {
            let workload = AppendWorkload {
                stream: stream.clone(),
                stream_count: *streams,
                record_bytes: *record_bytes,
                records_per_append: *records_per_append,
                appends_in_flight: *appends_in_flight,
                limit: workload_limit(*seconds, *records),
                target_records_per_second: *rate,
                takeover: if *force {
                    TakeoverPolicy::Force
                } else {
                    TakeoverPolicy::WaitForLeaseExpiry
                },
            };
            run_append(&client, &workload, *create, arguments.output).await?
        }
        WorkloadCommand::Read {
            stream,
            from,
            seconds,
            records,
        } => {
            let workload = ReadWorkload {
                stream: stream.clone(),
                from: *from,
                limit: workload_limit(*seconds, *records),
            };
            let measured = match run_read_workload(&client, &workload).await {
                Ok(measured) => measured,
                Err(workload_error) => return Err(workload_error.to_string()),
            };
            let report = ReadReport::of(&workload, &measured);
            let rendered = match arguments.output {
                OutputFormat::Text => report.render_text(),
                OutputFormat::Json => to_json(&report),
            };
            (rendered, report.failure.is_some())
        }
    };
    if let Err(io_error) = std::io::stdout().write_all(rendered.as_bytes()) {
        return Err(format!("cannot write the report: {io_error}"));
    }
    if has_stopped_early {
        Ok(WorkloadOutcome::StoppedEarly)
    } else {
        Ok(WorkloadOutcome::RanToItsLimit)
    }
}

/// Runs an append workload and renders its report.
///
/// # Returns
///
/// The rendered report, and whether the workload stopped early.
///
/// # Errors
///
/// Returns the text to show if a stream could not be created or the workload could not start.
async fn run_append(
    client: &BitacoraClient,
    workload: &AppendWorkload,
    creates_missing_streams: bool,
    format: OutputFormat,
) -> Result<(String, bool), String> {
    if creates_missing_streams && let Err(workload_error) = create_missing_streams(client, workload).await {
        return Err(workload_error.to_string());
    }
    let measured = match run_append_workloads(client, workload).await {
        Ok(measured) => measured,
        Err(workload_error) => return Err(workload_error.to_string()),
    };
    let report = AppendReport::of(workload, &measured);
    let rendered = match format {
        OutputFormat::Text => report.render_text(),
        OutputFormat::Json => to_json(&report),
    };
    Ok((rendered, report.failure.is_some()))
}

/// Renders a report as indented JSON, ended by a newline.
fn to_json<Report: serde::Serialize>(report: &Report) -> String {
    let mut rendered =
        serde_json::to_string_pretty(report).expect("reports hold only text, numbers and maps with text keys");
    rendered.push('\n');
    rendered
}
