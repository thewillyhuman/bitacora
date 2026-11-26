//! `bita-soak`: drives a bitacora cluster for days and judges what its clients saw.
//!
//! # Why this file exists
//!
//! The deterministic simulator shows that the protocol holds in a simulated world. The exit gate of the hardening
//! milestone asks for the same on real machines: a week of runs on a real cluster with no invariant violation
//! (design, section 18). No person can watch a cluster for a week, and no one-off test proves a week. So there is a
//! tool that does what the simulator does, for as long as it is told to: it writes and reads, breaks the cluster,
//! lets it heal, reads everything back, and hands the history of what clients saw to the simulator's own checker.
//! This file is its entry point.
//!
//! # What it contains
//!
//! - `main`: parses the command line, connects, runs the soak and turns its verdict into an exit code, with one
//!   function per step: `print_usage_error`, `run`, `fault_injector`, `kubernetes_faults`, `connect`,
//!   `serve_metrics`, `listen_for_shutdown`, `linger_unless_clean`, `exit_code_of`.
//! - The modules of the tool: `arguments` (the command line), `cluster` (what a run holds of the cluster), `soak`
//!   (the run), `round` (one round), `workload` (writers and readers), `nemesis`, `faults`, `targets` and
//!   `cut_check` (what goes wrong, and whether it really did), `kubernetes_faults` and `kubernetes_api` (how it
//!   is made to go wrong on Kubernetes), `health` (when the cluster is whole again), `audit` and `judge` (the
//!   verdict), `report` and `metrics` (what the tool says about itself).
//!
//! # How it works
//!
//! 1. The command line is parsed. A mistake ends the tool with exit code 2.
//! 2. Unless the command line asks for no faults, the Kubernetes API is found the way every pod finds it: its
//!    address in the environment, a service account in a mounted directory.
//! 3. A client and the metadata store are connected, as an application would connect them.
//! 4. The soak runs: round after round, a line of JSON for each on standard output, and one for the run at the end.
//! 5. The verdict of the run becomes the exit code.
//!
//! Exit codes: 0 if the run lasted as long as it was asked to and every round was clean; 1 if it could not start;
//! 2 if the command line is wrong; 3 if a round broke an invariant; 4 if the cluster did not recover after a round;
//! 5 if the tool was told to stop.
//!
//! # Invariants
//!
//! - Standard output holds the lines of the rounds and of the run and nothing else; everything else goes to
//!   standard error, each line starting with `bita-soak:`.
//! - The tool exits with 0 only for a run that was clean to its end.
//! - SIGTERM and SIGINT end the run in order: the cuts are removed and the round's line is printed before the
//!   process exits.
//!
//! # See also
//!
//! - `soak.rs`: the run.
//! - `docs/soak.md`: the options, the output and the exit codes.
//! - `README.md` in this crate: the design, and what a soak does and does not prove.

#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#![warn(rustdoc::broken_intra_doc_links)]

use std::net::SocketAddr;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use bitacora::{ClientSettings, ClusterConfig, WriterSettings, resolve_cluster_config};
use bitacora_process::{Readiness, ShutdownListener, serve_observability};
use bitacora_types::{Clock, SystemClock};
use clap::Parser;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::arguments::Arguments;
use crate::cluster::SoakedCluster;
use crate::faults::{FaultInjector, NoFaults};
use crate::kubernetes_api::ApiClient;
use crate::kubernetes_faults::{KubernetesFaults, SERVICE_ACCOUNT_DIRECTORY, ServiceAccount};
use crate::metrics::SoakMetrics;
use crate::nemesis::pause;
use crate::report::RunVerdict;
use crate::soak::run_soak;

mod arguments;
mod audit;
mod cluster;
mod cut_check;
mod faults;
mod health;
mod judge;
mod kubernetes_api;
mod kubernetes_faults;
#[cfg(test)]
mod kubernetes_stand_in;
mod metrics;
mod nemesis;
mod report;
mod round;
mod soak;
mod targets;
#[cfg(test)]
mod test_support;
mod workload;

/// The exit code of a run that could not start.
const EXIT_COULD_NOT_START: u8 = 1;
/// The exit code of a command line that is wrong.
const EXIT_USAGE: u8 = 2;
/// The exit code of a run in which a round broke an invariant.
const EXIT_VIOLATED: u8 = 3;
/// The exit code of a run after which the cluster did not recover.
const EXIT_NOT_RECOVERED: u8 = 4;
/// The exit code of a run that was told to stop.
const EXIT_INTERRUPTED: u8 = 5;
/// The environment variable in which every pod finds the host of the API server.
const API_SERVER_HOST_VARIABLE: &str = "KUBERNETES_SERVICE_HOST";
/// The environment variable in which every pod finds the port of the API server.
const API_SERVER_PORT_VARIABLE: &str = "KUBERNETES_SERVICE_PORT";
/// The name the tool's writers record with their lease.
const WRITER_IDENTITY: &str = "bita-soak";

/// Runs the tool and returns its exit code.
fn main() -> ExitCode {
    let arguments = match Arguments::try_parse() {
        Ok(arguments) => arguments,
        Err(usage_error) => return print_usage_error(&usage_error),
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(io_error) => {
            eprintln!("bita-soak: cannot start: {io_error}");
            return ExitCode::from(EXIT_COULD_NOT_START);
        }
    };
    match runtime.block_on(run(&arguments)) {
        Ok(verdict) => ExitCode::from(exit_code_of(verdict)),
        Err(reason) => {
            eprintln!("bita-soak: {reason}");
            ExitCode::from(EXIT_COULD_NOT_START)
        }
    }
}

/// Prints what the command line parser has to say and returns the exit code that goes with it.
///
/// A mistake is printed to standard error and is a usage error. A request for help or for the version arrives the
/// same way; it is printed to standard output and succeeds.
fn print_usage_error(usage_error: &clap::Error) -> ExitCode {
    let is_mistake = usage_error.use_stderr();
    let _ = usage_error.print();
    if is_mistake {
        ExitCode::from(EXIT_USAGE)
    } else {
        ExitCode::SUCCESS
    }
}

/// Connects to the cluster, runs the soak of the command line and returns how it ended.
///
/// # Errors
///
/// Returns the text to show if the cluster configuration cannot be used, the signals cannot be listened for, or
/// the metrics cannot be served.
async fn run(arguments: &Arguments) -> Result<RunVerdict, String> {
    let faults = fault_injector(arguments)?;
    let cluster = connect(arguments, faults).await?;
    let nemesis = arguments.nemesis_settings();
    let settings = arguments.soak_settings(SystemClock.now().unix_millis(), nemesis);
    let metrics = SoakMetrics::new();
    let mut stop = listen_for_shutdown()?;
    if let Some(address) = serve_metrics(arguments.metrics_listen_address.as_deref(), &metrics, stop.clone()).await? {
        eprintln!("bita-soak: serving /metrics on {address}");
    }
    eprintln!("bita-soak: run {} with seed {}", settings.run, settings.seed);
    if settings.plants_lost_record {
        eprintln!("bita-soak: this is a drill: the first round claims a commit that never happened and must fail");
    }
    let summary = run_soak(&cluster, &settings, &metrics, &mut std::io::stdout(), &mut stop).await;
    linger_unless_clean(
        summary.verdict,
        Duration::from_secs(arguments.linger_seconds),
        &mut stop,
    )
    .await;
    Ok(summary.verdict)
}

/// Says how the faults of the run reach the cluster: through the Kubernetes API, as the tool's own pod finds it,
/// or not at all if the command line asks for no faults.
///
/// # Errors
///
/// Returns the text to show if faults are asked for and the tool does not run in a pod with a service account.
fn fault_injector(arguments: &Arguments) -> Result<Arc<dyn FaultInjector>, String> {
    if arguments.no_faults {
        return Ok(Arc::new(NoFaults));
    }
    let host = std::env::var(API_SERVER_HOST_VARIABLE).ok();
    let port = std::env::var(API_SERVER_PORT_VARIABLE).ok();
    let service_account_directory = Path::new(SERVICE_ACCOUNT_DIRECTORY);
    match kubernetes_faults(arguments, service_account_directory, host.as_deref(), port.as_deref()) {
        Ok(faults) => Ok(Arc::new(faults)),
        Err(reason) => Err(format!(
            "faults are injected through the Kubernetes API, which cannot be used: {reason}; with --no-faults the \
             tool runs without it"
        )),
    }
}

/// Makes the faults of a cluster on Kubernetes from what a pod is given: the address of the API server in its
/// environment and a service account in a directory.
///
/// # Errors
///
/// Returns the text to show if either is missing or cannot be used.
fn kubernetes_faults(
    arguments: &Arguments,
    service_account_directory: &Path,
    host: Option<&str>,
    port: Option<&str>,
) -> Result<KubernetesFaults, String> {
    let (Some(host), Some(port)) = (host, port) else {
        return Err(format!(
            "{API_SERVER_HOST_VARIABLE} and {API_SERVER_PORT_VARIABLE} are not set, so this is not a pod"
        ));
    };
    let service_account = ServiceAccount::read_from(service_account_directory)?;
    let api = match ApiClient::new(service_account.api_settings(host, port)) {
        Ok(api) => api,
        Err(api_error) => return Err(api_error.to_string()),
    };
    let targets = arguments.kubernetes_targets(&service_account.namespace);
    Ok(KubernetesFaults::new(api, targets))
}

/// The configuration of the cluster the command line names: its configuration file, if it names one, with the
/// etcd options in place of the file's values.
///
/// # Errors
///
/// Returns the text to show if the file cannot be read or is not a cluster configuration, or if no etcd member
/// is known in the end.
fn cluster_config(arguments: &Arguments) -> Result<ClusterConfig, String> {
    let resolved = resolve_cluster_config(
        arguments.config.as_deref(),
        &arguments.etcd_endpoints,
        arguments.etcd_key_prefix.as_deref(),
    );
    match resolved {
        Ok(config) => Ok(config),
        Err(config_error) => Err(config_error.to_string()),
    }
}

/// Connects a client and the metadata store of the cluster the command line names.
///
/// # Errors
///
/// Returns the text to show if the cluster configuration cannot be used.
async fn connect(arguments: &Arguments, faults: Arc<dyn FaultInjector>) -> Result<SoakedCluster, String> {
    let config = cluster_config(arguments)?;
    let client = match bitacora::connect(&config, client_settings()).await {
        Ok(client) => client,
        Err(connect_error) => return Err(connect_error.to_string()),
    };
    let metadata = match bitacora::connect_metadata(&config).await {
        Ok(metadata) => metadata,
        Err(connect_error) => return Err(connect_error.to_string()),
    };
    Ok(SoakedCluster {
        client,
        metadata,
        clock: Arc::new(SystemClock),
        faults,
    })
}

/// The settings of the tool's client: an application's defaults, with the tool's name on its leases.
fn client_settings() -> ClientSettings {
    ClientSettings {
        writer: WriterSettings {
            writer_identity: WRITER_IDENTITY.to_owned(),
            ..WriterSettings::default()
        },
        ..ClientSettings::default()
    }
}

/// Starts listening for SIGTERM and SIGINT.
///
/// # Returns
///
/// A value that becomes `true` when either arrives.
///
/// # Errors
///
/// Returns the text to show if the operating system refuses the signal handlers.
fn listen_for_shutdown() -> Result<watch::Receiver<bool>, String> {
    let listener = match ShutdownListener::install() {
        Ok(listener) => listener,
        Err(io_error) => return Err(format!("cannot listen for signals: {io_error}")),
    };
    let (told_to_stop, stop) = watch::channel(false);
    tokio::spawn(async move {
        let signal = listener.wait().await;
        eprintln!("bita-soak: stopping after {signal:?}");
        let _run_was_listening = told_to_stop.send(true);
        // Keeps the sender alive: a dropped sender would read as "stop" to everything that listens.
        told_to_stop.closed().await;
    });
    Ok(stop)
}

/// Serves the metrics of the soak on `listen_address` until `stop` says to end. Does nothing without an address.
///
/// # Returns
///
/// The address the metrics are served on, or `None` if no address was given.
///
/// # Errors
///
/// Returns the text to show if the address cannot be listened on.
async fn serve_metrics(
    listen_address: Option<&str>,
    metrics: &SoakMetrics,
    mut stop: watch::Receiver<bool>,
) -> Result<Option<SocketAddr>, String> {
    let Some(listen_address) = listen_address else {
        return Ok(None);
    };
    let bound = match TcpListener::bind(listen_address).await {
        Ok(listener) => listener.local_addr().map(|address| (listener, address)),
        Err(io_error) => Err(io_error),
    };
    let (listener, address) = match bound {
        Ok(bound) => bound,
        Err(io_error) => return Err(format!("cannot serve metrics on {listen_address}: {io_error}")),
    };
    let readiness = Readiness::new();
    readiness.set_ready(true);
    let registry = metrics.registry();
    tokio::spawn(async move {
        let until_told_to_stop = async move {
            let _told_to_stop = stop.wait_for(|is_stopped| *is_stopped).await;
        };
        if let Err(io_error) = serve_observability(listener, registry, readiness, until_told_to_stop).await {
            eprintln!("bita-soak: the metrics are no longer served: {io_error}");
        }
    });
    Ok(Some(address))
}

/// Keeps the process up for `duration` after a run that was not clean, so that the files of its histories can be
/// copied out of the pod and its metrics scraped. Ends early when the tool is told to stop.
async fn linger_unless_clean(verdict: RunVerdict, duration: Duration, stop: &mut watch::Receiver<bool>) {
    let leaves_evidence = matches!(verdict, RunVerdict::Violated | RunVerdict::NotRecovered);
    if !leaves_evidence || duration.is_zero() {
        return;
    }
    eprintln!("bita-soak: staying up for {duration:?}; the histories of the last round are in its dump files");
    let _whole_duration_passed = pause(duration, Instant::now() + duration, stop).await;
}

/// The exit code that goes with how a run ended.
const fn exit_code_of(verdict: RunVerdict) -> u8 {
    match verdict {
        RunVerdict::Clean => 0,
        RunVerdict::CouldNotStart => EXIT_COULD_NOT_START,
        RunVerdict::Violated => EXIT_VIOLATED,
        RunVerdict::NotRecovered => EXIT_NOT_RECOVERED,
        RunVerdict::Interrupted => EXIT_INTERRUPTED,
    }
}

#[cfg(test)]
#[path = "main_test.rs"]
mod tests;
