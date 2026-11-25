//! `bita`: the command-line tool for operating a bitacora cluster.
//!
//! # Why this file exists
//!
//! A cluster has to be runnable by one person (design, section 14). That person needs to create and
//! inspect streams, drain a replica before a restart, remove a failed one, and see what the controller
//! is doing, without writing a program against the client library. `bita` is that tool, and this file is
//! its entry point.
//!
//! # What it contains
//!
//! - `main`: parses the command line, finds the cluster, runs the command and prints its result.
//! - The modules of the tool: `arguments` (the command line), `cluster` (finding and reaching the
//!   cluster), `commands` (the work), `output` and `duration` (notations).
//!
//! # How it works
//!
//! 1. The command line is parsed. A mistake there ends the tool with exit code 2 and a usage message.
//! 2. The cluster is worked out from options, environment and an optional configuration file. Not finding
//!    one is also a usage error.
//! 3. The interfaces to the cluster are opened. This sends nothing.
//! 4. The command runs and returns a value, which is printed as text or as JSON. `stream tail` is the
//!    exception: it prints records as they commit.
//!
//! Exit codes: 0 if the command succeeded, 1 if it failed, 2 if the command line or the configuration is
//! wrong.
//!
//! Everything but this file works through injected interfaces, so the commands are tested against an
//! in-process cluster. This file is the only one that touches the process: its arguments, its
//! environment, standard input and standard output.
//!
//! # Invariants
//!
//! - Results go to standard output and nothing else does; errors go to standard error. A script can pipe
//!   the output of `--output json` into a JSON parser.
//!
//! # See also
//!
//! - `docs/cli.md`: every command with an example.
//! - `commands.rs`: where a parsed command is run.

use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::process::ExitCode;

use bytes::Bytes;
use clap::Parser;

use crate::arguments::{Arguments, Command, StreamCommand};
use crate::cluster::{Cluster, resolve_cluster_config};

mod arguments;
mod cluster;
mod commands;
mod duration;
mod output;

/// The exit code of a command that failed.
const EXIT_FAILURE: u8 = 1;
/// The exit code of a command line or a configuration that is wrong.
const EXIT_USAGE: u8 = 2;

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

    let config = match resolve_cluster_config(&arguments.cluster, &process_environment()) {
        Ok(config) => config,
        Err(config_error) => {
            eprintln!("bita: {config_error}");
            return ExitCode::from(EXIT_USAGE);
        }
    };

    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(io_error) => {
            eprintln!("bita: cannot start: {io_error}");
            return ExitCode::from(EXIT_FAILURE);
        }
    };
    runtime.block_on(async {
        let cluster = match Cluster::connect(&config, arguments.request_timeout).await {
            Ok(cluster) => cluster,
            Err(connect_error) => {
                eprintln!("bita: {connect_error}");
                return ExitCode::from(EXIT_USAGE);
            }
        };
        match run_and_print(&arguments, &cluster).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(reason) => {
                eprintln!("bita: {reason}");
                ExitCode::from(EXIT_FAILURE)
            }
        }
    })
}

/// The environment variables of the process whose names and values are text.
fn process_environment() -> BTreeMap<String, String> {
    let mut environment = BTreeMap::new();
    for (name, value) in std::env::vars_os() {
        if let (Ok(name), Ok(value)) = (name.into_string(), value.into_string()) {
            environment.insert(name, value);
        }
    }
    environment
}

/// Runs the parsed command and prints what it returns.
///
/// # Errors
///
/// Returns the text to show the operator if the command failed or its result could not be printed.
async fn run_and_print(arguments: &Arguments, cluster: &Cluster) -> Result<(), String> {
    // Not locked: the handle is held across waits for the cluster, and a lock could not be.
    let mut standard_output = std::io::stdout();
    if let Command::Stream(StreamCommand::Tail { name, from, count }) = &arguments.command {
        let followed = commands::stream::tail(
            cluster,
            name,
            *from,
            *count,
            arguments.output,
            &mut standard_output,
        )
        .await;
        return match followed {
            Ok(_written_records) => Ok(()),
            Err(command_error) => Err(command_error.to_string()),
        };
    }

    let standard_input_records = match &arguments.command {
        Command::Stream(StreamCommand::Append { records, .. }) if records.is_empty() => {
            read_standard_input_records()?
        }
        _every_other_command => Vec::new(),
    };
    let output = match commands::run(&arguments.command, cluster, standard_input_records).await {
        Ok(output) => output,
        Err(command_error) => return Err(command_error.to_string()),
    };
    let rendered = output.render(arguments.output);
    match standard_output.write_all(rendered.as_bytes()) {
        Ok(()) => Ok(()),
        Err(io_error) => Err(format!("cannot write the output: {io_error}")),
    }
}

/// Reads standard input to its end and returns every line as one record, without its line ending.
///
/// # Errors
///
/// Returns the text to show the operator if standard input cannot be read.
fn read_standard_input_records() -> Result<Vec<Bytes>, String> {
    let mut records = Vec::new();
    for line in std::io::stdin().lock().lines() {
        match line {
            Ok(line) => records.push(Bytes::from(line)),
            Err(io_error) => return Err(format!("cannot read standard input: {io_error}")),
        }
    }
    Ok(records)
}
