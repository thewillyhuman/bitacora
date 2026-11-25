//! `bita-controller`: the process that looks after a bitacora cluster.
//!
//! # Why this file exists
//!
//! The controller is deployed as two or three processes of which one is active (design, section 14,
//! "What you deploy"). This file is that process's entry point. It is deliberately thin: it turns the
//! command line and the configuration file into settings, hands them to the daemon, and turns the
//! daemon's result into an exit code a supervisor can act on.
//!
//! # What it contains
//!
//! - `main`: the entry point.
//! - `start`: loads and checks the configuration, then runs the daemon on a Tokio runtime.
//! - `check_config`: loads and checks the configuration, says whether it is valid, and stops there.
//! - `load_settings`: what both of them do first.
//! - The modules of the daemon: `config`, `daemon`, `metrics`.
//!
//! # How it works
//!
//! ```text
//! bita-controller --config /etc/bitacora/controller.toml
//! bita-controller --config /etc/bitacora/controller.toml --check-config
//! ```
//!
//! With `--check-config` the process reads and validates the configuration exactly as a start does,
//! says whether it is valid, and exits with 0 or 2 without opening or binding anything.
//!
//! The process exits with:
//!
//! | Code | Meaning |
//! | --- | --- |
//! | 0 | It was asked to stop (SIGTERM or SIGINT) and stopped cleanly, or printed its help or version, or checked a valid configuration |
//! | 1 | It could not start: for example its metrics port is taken |
//! | 2 | Its command line or its configuration file is wrong |
//!
//! A supervisor should restart it after code 1 and not after code 2, which needs a person.
//!
//! Until the log output is installed, which needs the configuration, problems are reported on standard
//! error in plain text. From then on everything goes to the log on standard output.
//!
//! A controller holds no state of its own, so there is nothing to lose when it is killed: the next
//! instance that leads reads the metadata store and carries on.
//!
//! # Invariants
//!
//! - Nothing is opened or bound before the whole configuration was checked.
//! - The shutdown listener is installed before the first round, so a signal is never lost.
//!
//! # See also
//!
//! - `daemon.rs`: what the process does while it runs.
//! - `example.toml`: a complete configuration file.
//! - `README.md`: how to operate the daemon.

#![warn(clippy::missing_docs_in_private_items)]

mod config;
mod daemon;
mod metrics;

use std::path::Path;
use std::process::ExitCode;

use bitacora_process::{CommandLine, ShutdownListener, init_logging, load_config_file, parse_command_line, usage};

use crate::config::{ControllerConfigFile, ControllerDaemonSettings};
use crate::daemon::DaemonError;

/// The name the daemon is started under, for messages.
const DAEMON_NAME: &str = "bita-controller";

/// The exit code for a wrong command line or configuration file: restarting cannot help.
const EXIT_CODE_BAD_CONFIGURATION: u8 = 2;

/// The entry point: understands the command line and runs the daemon.
fn main() -> ExitCode {
    match parse_command_line(std::env::args().skip(1)) {
        Ok(CommandLine::Run { config_path }) => start(&config_path),
        Ok(CommandLine::CheckConfig { config_path }) => check_config(&config_path),
        Ok(CommandLine::PrintHelp) => {
            print!("{}", usage(DAEMON_NAME));
            ExitCode::SUCCESS
        }
        Ok(CommandLine::PrintVersion) => {
            println!("{DAEMON_NAME} {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Err(usage_error) => {
            eprintln!("{DAEMON_NAME}: {usage_error}\n\n{}", usage(DAEMON_NAME));
            ExitCode::from(EXIT_CODE_BAD_CONFIGURATION)
        }
    }
}

/// Reads the configuration file at `config_path` and validates it, as the daemon does at start.
///
/// # Returns
///
/// The settings the daemon runs with.
///
/// # Errors
///
/// Returns the message to show the operator: which file, which setting and what is wrong with it.
fn load_settings(config_path: &Path) -> Result<ControllerDaemonSettings, String> {
    let config_file: ControllerConfigFile = match load_config_file(config_path) {
        Ok(config_file) => config_file,
        Err(config_file_error) => return Err(config_file_error.to_string()),
    };
    match config_file.validate() {
        Ok(settings) => Ok(settings),
        Err(invalid_config) => Err(format!(
            "configuration file {}: {invalid_config}",
            config_path.display()
        )),
    }
}

/// Checks the configuration at `config_path` and says whether the daemon would start with it. Opens
/// nothing and starts nothing.
///
/// # Returns
///
/// Exit code 0 if the configuration is valid, 2 if it is not.
fn check_config(config_path: &Path) -> ExitCode {
    match load_settings(config_path) {
        Ok(_settings) => {
            println!("{DAEMON_NAME}: configuration file {} is valid", config_path.display());
            ExitCode::SUCCESS
        }
        Err(problem) => {
            eprintln!("{DAEMON_NAME}: {problem}");
            ExitCode::from(EXIT_CODE_BAD_CONFIGURATION)
        }
    }
}

/// Loads and checks the configuration at `config_path`, then runs the daemon until it stops.
///
/// # Returns
///
/// The exit code of the process, as the file header lists them.
fn start(config_path: &Path) -> ExitCode {
    let settings = match load_settings(config_path) {
        Ok(settings) => settings,
        Err(problem) => {
            eprintln!("{DAEMON_NAME}: {problem}");
            return ExitCode::from(EXIT_CODE_BAD_CONFIGURATION);
        }
    };
    if let Err(logging_error) = init_logging(settings.log) {
        eprintln!("{DAEMON_NAME}: {logging_error}");
        return ExitCode::FAILURE;
    }
    let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(io_error) => {
            tracing::error!(%io_error, "cannot start the runtime");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(serve(settings)) {
        Ok(()) => {
            tracing::info!("controller stopped");
            ExitCode::SUCCESS
        }
        Err(daemon_error) => {
            tracing::error!(%daemon_error, "controller could not start");
            ExitCode::FAILURE
        }
    }
}

/// Opens everything the daemon needs and runs it until it is told to stop.
///
/// # Errors
///
/// Returns [`DaemonError`] if the daemon cannot start.
async fn serve(settings: ControllerDaemonSettings) -> Result<(), DaemonError> {
    let shutdown_listener = match ShutdownListener::install() {
        Ok(shutdown_listener) => shutdown_listener,
        Err(io_error) => {
            return Err(DaemonError::CannotStart {
                action: "listen for the shutdown signal".to_owned(),
                reason: io_error.to_string(),
            });
        }
    };
    let observability_listener = daemon::bind_listener(&settings).await?;
    let environment = daemon::open_environment(&settings).await?;
    daemon::run(settings, environment, observability_listener, shutdown_listener.wait()).await;
    Ok(())
}

#[cfg(test)]
#[path = "main_test.rs"]
mod tests;
