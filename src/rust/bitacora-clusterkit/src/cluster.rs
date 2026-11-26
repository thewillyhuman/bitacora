//! A bitacora cluster of real processes on one machine.
//!
//! # Why this file exists
//!
//! The simulator and the in-process cluster test the protocol exhaustively, but neither runs the things
//! an operator deploys: the daemons with their configuration files, their sockets, their signals and
//! their exit codes, talking to a real etcd over a real network stack. The design's last testing layer
//! does (section 15, "Layer 4: real clusters"). This file builds such a cluster for a test: small,
//! on loopback, in a temporary directory, and gone when the test ends.
//!
//! # What it contains
//!
//! - [`RealCluster`]: etcd, some replica daemons and one controller daemon, all running.
//! - [`ExitedDaemon`]: how one daemon ended when the cluster was stopped.
//! - Private steps of starting a cluster: `ReservedPorts`, `start_etcd`, `start_replica`,
//!   `start_controller`, and one function per configuration file.
//!
//! # How it works
//!
//! [`RealCluster::start`] picks free loopback ports for everything, starts a single-member etcd, waits
//! until it is healthy, writes one configuration file per daemon into the cluster's temporary
//! directory, starts the daemons, and waits until each reports ready on `/readyz`. The configurations
//! are the ones an operator would write, with intervals shortened so that a test does not wait:
//! replicas renew their registration every second, the controller makes a pass every 200 milliseconds
//! and presumes a replica dead after three seconds of silence.
//!
//! A test then talks to the cluster as an application does, through
//! `bitacora::connect(cluster.cluster_config(), ..)`.
//!
//! [`RealCluster::kill_replica`] kills one replica with SIGKILL, as a crash would;
//! [`RealCluster::restart_replica`] starts it again on the same data directory and the same ports.
//! [`RealCluster::stop`] sends SIGTERM to every daemon and reports how each exited.
//!
//! Each daemon's output goes to a log file in the temporary directory. When a test panics, the end of
//! every log is printed.
//!
//! # Invariants
//!
//! - Every process of the cluster is killed when the [`RealCluster`] is dropped, and only then is the
//!   temporary directory removed.
//! - A [`RealCluster`] that exists was fully ready when [`RealCluster::start`] returned.
//! - Ports are handed out in a fixed order: etcd's client port and peer port, then for each replica its
//!   gRPC port and its observability port, then the controller's.
//!
//! # See also
//!
//! - `process.rs`: the child processes and the ports.
//! - `binaries.rs`: where the programs come from.
//! - `tests/real_cluster_test.rs`: the test that uses all of this.

// This crate is a test fixture; writing configuration files and starting processes is its purpose.
#![allow(clippy::disallowed_methods)]

use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::time::Duration;

use bitacora::{ClusterConfig, DEFAULT_ETCD_KEY_PREFIX, ObjectStoreConfig};
use bitacora_process::http_get;
use tempfile::TempDir;
use tokio::time::{Instant, sleep};

use crate::binaries::Binaries;
use crate::process::{ChildProcess, free_local_ports};

/// How long a process may take to become ready before the test gives up. Starting takes well under a
/// second on an idle machine; the allowance is for a loaded continuous integration runner.
const STARTUP_ALLOWANCE: Duration = Duration::from_secs(60);

/// How long to wait between two looks at whether a process is ready.
const READINESS_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// How long one readiness probe may take. Short, so that a process that is not listening yet is asked
/// again soon.
const PROBE_TIMEOUT: Duration = Duration::from_millis(500);

/// How long a daemon may take to exit after SIGTERM before [`RealCluster::stop`] gives up on it. Longer
/// than the daemons' own grace period for requests in flight.
const EXIT_ALLOWANCE: Duration = Duration::from_secs(30);

/// The name of the single member of the test's etcd. etcd wants the same name in `--name` and in
/// `--initial-cluster`.
const ETCD_MEMBER_NAME: &str = "bitacora-clusterkit";

/// The disk-usage threshold every daemon of the cluster is given: only a disk that is completely full
/// refuses data. The machine that runs a test may have a nearly full disk, and the defaults (85 and 95
/// percent) would then make replicas refuse every segment. How full the developer's disk is must not
/// decide whether a test passes.
const WHOLE_DISK_PERCENT: u8 = 100;

/// How many ports etcd needs: one for clients, one for peers.
const ETCD_PORT_COUNT: usize = 2;

/// How many ports a replica daemon needs: one for gRPC, one for observability.
const REPLICA_PORT_COUNT: usize = 2;

/// One daemon of the cluster: its process and what is needed to probe and restart it.
#[derive(Debug)]
struct Daemon {
    /// The running process. Replaced when the daemon is restarted.
    process: ChildProcess,
    /// The program the daemon runs.
    binary: PathBuf,
    /// Its configuration file.
    config_path: PathBuf,
    /// Where its output goes.
    log_path: PathBuf,
    /// Where it serves `/metrics`, `/healthz` and `/readyz`, as `host:port`.
    observability_address: String,
}

impl Daemon {
    /// Starts the daemon `name` from `binary` with the configuration at `config_path`.
    fn start(name: &str, binary: &Path, config_path: &Path, observability_address: String) -> Self {
        let log_path = config_path.with_extension("log");
        let process = spawn_daemon(name, binary, config_path, &log_path);
        Self {
            process,
            binary: binary.to_owned(),
            config_path: config_path.to_owned(),
            log_path,
            observability_address,
        }
    }

    /// Starts the daemon's program again after its process was killed, on the same configuration.
    fn restart(&mut self) {
        let name = self.process.name().to_owned();
        // The old process must be gone first, or its ports are still taken.
        self.process.kill();
        self.process = spawn_daemon(&name, &self.binary, &self.config_path, &self.log_path);
    }

    /// Waits until the daemon answers `/readyz` with success.
    ///
    /// # Panics
    ///
    /// Panics if the daemon exits or [`STARTUP_ALLOWANCE`] runs out first.
    async fn wait_until_ready(&mut self) {
        let deadline = Instant::now() + STARTUP_ALLOWANCE;
        loop {
            let answer = http_get(&self.observability_address, "/readyz", PROBE_TIMEOUT).await;
            if answer.is_ok_and(|answer| answer.status == 200) {
                return;
            }
            assert!(
                !self.process.has_exited(),
                "{} exited while starting:\n{}",
                self.process.name(),
                self.process.log()
            );
            assert!(
                Instant::now() < deadline,
                "{} did not become ready within {STARTUP_ALLOWANCE:?}:\n{}",
                self.process.name(),
                self.process.log()
            );
            sleep(READINESS_POLL_INTERVAL).await;
        }
    }
}

/// Starts a daemon's program with `--config`.
fn spawn_daemon(name: &str, binary: &Path, config_path: &Path, log_path: &Path) -> ChildProcess {
    let mut command = Command::new(binary);
    command.arg("--config").arg(config_path);
    ChildProcess::spawn(name, &mut command, log_path)
}

/// How one daemon ended when the cluster was stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExitedDaemon {
    /// The daemon: `replica-0`, `controller`.
    pub name: String,
    /// The status it exited with after SIGTERM, or `None` if it was still running after the allowance
    /// and had to be killed.
    pub exit_status: Option<ExitStatus>,
}

/// A running cluster of real processes: one etcd, some replica daemons and one controller daemon.
///
/// Dropping it kills every process and removes its files.
#[derive(Debug)]
pub struct RealCluster {
    /// The replica daemons, in order: `replica-0`, `replica-1`, ...
    replicas: Vec<Daemon>,
    /// The controller daemon.
    controller: Daemon,
    /// The etcd server. Declared after the daemons so that it is dropped, and so killed, after them.
    _etcd: ChildProcess,
    /// Where the cluster is, as an application is told.
    cluster_config: ClusterConfig,
    /// Holds every configuration file, data directory and log. Declared last, so that it is removed
    /// only after every process is gone.
    _directory: TempDir,
}

impl RealCluster {
    /// Starts etcd, `replica_count` replica daemons and one controller daemon, and waits until all of
    /// them are ready.
    ///
    /// # Arguments
    ///
    /// * `binaries` - The programs to run.
    /// * `replica_count` - How many replica daemons to start. A stream with the default replication
    ///   needs three.
    ///
    /// # Returns
    ///
    /// The running cluster: every replica is registered, and the controller has completed a round.
    ///
    /// # Panics
    ///
    /// Panics if a process cannot be started, exits while starting, or is not ready in time. The
    /// message includes the process's log.
    pub async fn start(binaries: &Binaries, replica_count: usize) -> Self {
        let directory = new_cluster_directory();
        let root = directory.path();
        let mut ports = ReservedPorts::reserve(ETCD_PORT_COUNT + replica_count * REPLICA_PORT_COUNT + 1);

        let etcd_client_endpoint = format!("http://127.0.0.1:{}", ports.take());
        let etcd = start_etcd(&binaries.etcd, root, &etcd_client_endpoint, ports.take()).await;
        let cluster_config = ClusterConfig {
            etcd_endpoints: vec![etcd_client_endpoint],
            etcd_key_prefix: DEFAULT_ETCD_KEY_PREFIX.to_owned(),
            object_store: ObjectStoreConfig::LocalDirectory {
                path: root.join("objects"),
            },
        };
        let mut replicas = Vec::with_capacity(replica_count);
        for replica_number in 0..replica_count {
            let replica = start_replica(replica_number, &binaries.replica, root, &mut ports, &cluster_config);
            replicas.push(replica);
        }
        let mut controller = start_controller(&binaries.controller, root, &mut ports, &cluster_config);

        for replica in &mut replicas {
            replica.wait_until_ready().await;
        }
        controller.wait_until_ready().await;
        Self {
            replicas,
            controller,
            _etcd: etcd,
            cluster_config,
            _directory: directory,
        }
    }

    /// Where the cluster is: what an application passes to `bitacora::connect`.
    #[must_use]
    pub const fn cluster_config(&self) -> &ClusterConfig {
        &self.cluster_config
    }

    /// How many replica daemons the cluster has, running or not.
    #[must_use]
    pub fn replica_count(&self) -> usize {
        self.replicas.len()
    }

    /// Where replica `replica_number` serves `/metrics`, `/healthz` and `/readyz`, as `host:port`.
    ///
    /// # Panics
    ///
    /// Panics if the cluster has no such replica.
    #[must_use]
    pub fn replica_observability_address(&self, replica_number: usize) -> &str {
        &self.replicas[replica_number].observability_address
    }

    /// Where the controller serves `/metrics`, `/healthz` and `/readyz`, as `host:port`.
    #[must_use]
    pub fn controller_observability_address(&self) -> &str {
        &self.controller.observability_address
    }

    /// Kills replica `replica_number` at once with SIGKILL, as a crash or a lost machine would. Its
    /// data directory stays as the kill left it.
    ///
    /// # Panics
    ///
    /// Panics if the cluster has no such replica.
    pub fn kill_replica(&mut self, replica_number: usize) {
        self.replicas[replica_number].process.kill();
    }

    /// Starts replica `replica_number` again on its data directory and its ports, and waits until it is
    /// ready, which means it has registered again.
    ///
    /// # Panics
    ///
    /// Panics if the cluster has no such replica, or if it does not become ready in time.
    pub async fn restart_replica(&mut self, replica_number: usize) {
        let replica = &mut self.replicas[replica_number];
        replica.restart();
        replica.wait_until_ready().await;
    }

    /// Stops every daemon with SIGTERM, as a supervisor does, and waits for each to exit.
    ///
    /// # Returns
    ///
    /// How each daemon ended, the controller first and then the replicas in order. A daemon that was
    /// killed before is reported with the status of that kill.
    pub async fn stop(mut self) -> Vec<ExitedDaemon> {
        self.controller.process.terminate();
        for replica in &mut self.replicas {
            replica.process.terminate();
        }
        let mut exited_daemons = Vec::with_capacity(self.replicas.len() + 1);
        exited_daemons.push(wait_for_exit(&mut self.controller).await);
        for replica in &mut self.replicas {
            exited_daemons.push(wait_for_exit(replica).await);
        }
        exited_daemons
    }
}

/// The loopback ports reserved for one cluster, handed out one at a time.
struct ReservedPorts {
    /// The ports nobody was given yet.
    free_ports: Vec<u16>,
}

impl ReservedPorts {
    /// Reserves `port_count` loopback ports that are free right now.
    fn reserve(port_count: usize) -> Self {
        Self {
            free_ports: free_local_ports(port_count),
        }
    }

    /// Hands out one of the reserved ports. No port is handed out twice.
    ///
    /// # Panics
    ///
    /// Panics if more ports are taken than were reserved.
    fn take(&mut self) -> u16 {
        self.free_ports.pop().expect("enough ports were reserved")
    }
}

/// Writes the configuration of replica `replica_number` below `root` and starts its daemon, without
/// waiting for it to become ready. It takes two ports: gRPC first, then observability.
fn start_replica(
    replica_number: usize,
    binary: &Path,
    root: &Path,
    ports: &mut ReservedPorts,
    cluster_config: &ClusterConfig,
) -> Daemon {
    let name = format!("replica-{replica_number}");
    let grpc_address = format!("127.0.0.1:{}", ports.take());
    let observability_address = format!("127.0.0.1:{}", ports.take());
    let config_path = root.join(format!("{name}.toml"));
    let config_text = replica_config_text(
        &name,
        &root.join(&name),
        &grpc_address,
        &observability_address,
        cluster_config,
    );
    write_file(&config_path, &config_text);
    Daemon::start(&name, binary, &config_path, observability_address)
}

/// Writes the controller's configuration below `root` and starts its daemon, without waiting for it to
/// become ready. It takes one port, for observability.
fn start_controller(binary: &Path, root: &Path, ports: &mut ReservedPorts, cluster_config: &ClusterConfig) -> Daemon {
    let observability_address = format!("127.0.0.1:{}", ports.take());
    let config_path = root.join("controller.toml");
    let config_text = controller_config_text(&observability_address, cluster_config);
    write_file(&config_path, &config_text);
    Daemon::start("controller", binary, &config_path, observability_address)
}

/// Waits for a daemon that was asked to stop.
async fn wait_for_exit(daemon: &mut Daemon) -> ExitedDaemon {
    ExitedDaemon {
        name: daemon.process.name().to_owned(),
        exit_status: daemon.process.wait_for_exit(EXIT_ALLOWANCE).await,
    }
}

/// Creates the directory that holds everything of one cluster, inside the test's own temporary
/// directory when Bazel provides one.
fn new_cluster_directory() -> TempDir {
    let parent = match std::env::var_os("TEST_TMPDIR") {
        Some(test_directory) => PathBuf::from(test_directory),
        None => std::env::temp_dir(),
    };
    tempfile::Builder::new()
        .prefix("bitacora-cluster-")
        .tempdir_in(parent)
        .expect("a temporary directory can be created")
}

/// Writes a configuration file.
fn write_file(path: &Path, text: &str) {
    if let Err(io_error) = std::fs::write(path, text) {
        panic!("cannot write {}: {io_error}", path.display());
    }
}

/// Starts a single-member etcd with its data below `root` and waits until it is healthy.
///
/// # Panics
///
/// Panics if etcd exits while starting or is not healthy within [`STARTUP_ALLOWANCE`].
async fn start_etcd(binary: &Path, root: &Path, client_endpoint: &str, peer_port: u16) -> ChildProcess {
    let mut command = etcd_command(binary, root, client_endpoint, peer_port);
    let mut etcd = ChildProcess::spawn("etcd", &mut command, &root.join("etcd.log"));

    // etcd answers `/health` on its client port with 200 once it has elected itself leader.
    let client_address = client_endpoint.trim_start_matches("http://");
    let deadline = Instant::now() + STARTUP_ALLOWANCE;
    loop {
        let answer = http_get(client_address, "/health", PROBE_TIMEOUT).await;
        if answer.is_ok_and(|answer| answer.status == 200) {
            return etcd;
        }
        assert!(!etcd.has_exited(), "etcd exited while starting:\n{}", etcd.log());
        assert!(
            Instant::now() < deadline,
            "etcd was not healthy within {STARTUP_ALLOWANCE:?}:\n{}",
            etcd.log()
        );
        sleep(READINESS_POLL_INTERVAL).await;
    }
}

/// The command line of a single-member etcd that keeps its data below `root`, serves clients on
/// `client_endpoint` and its one peer, itself, on `peer_port`.
fn etcd_command(binary: &Path, root: &Path, client_endpoint: &str, peer_port: u16) -> Command {
    let peer_endpoint = format!("http://127.0.0.1:{peer_port}");
    let mut command = Command::new(binary);
    command
        .arg("--name")
        .arg(ETCD_MEMBER_NAME)
        .arg("--data-dir")
        .arg(root.join("etcd"))
        .arg("--listen-client-urls")
        .arg(client_endpoint)
        .arg("--advertise-client-urls")
        .arg(client_endpoint)
        .arg("--listen-peer-urls")
        .arg(&peer_endpoint)
        .arg("--initial-advertise-peer-urls")
        .arg(&peer_endpoint)
        .arg("--initial-cluster")
        .arg(format!("{ETCD_MEMBER_NAME}={peer_endpoint}"))
        // The data does not outlive the test, so waiting for the disk on every write would only make
        // the test slower.
        .arg("--unsafe-no-fsync")
        .arg("--log-level")
        .arg("error");
    command
}

/// Writes a path as a TOML string: in double quotes, with backslashes and quotes escaped.
fn toml_path(path: &Path) -> String {
    let path_text = path.display().to_string();
    let mut quoted = String::with_capacity(path_text.len() + 2);
    quoted.push('"');
    for character in path_text.chars() {
        if character == '"' || character == '\\' {
            quoted.push('\\');
        }
        quoted.push(character);
    }
    quoted.push('"');
    quoted
}

/// The `[cluster]` table every daemon of the cluster gets.
fn cluster_table_text(cluster_config: &ClusterConfig) -> String {
    let ObjectStoreConfig::LocalDirectory {
        path: object_store_path,
    } = &cluster_config.object_store
    else {
        panic!("a cluster of this kit always tiers to a local directory");
    };
    format!(
        "[cluster]\n\
         etcd_endpoints = [{etcd_endpoint:?}]\n\
         etcd_key_prefix = {key_prefix:?}\n\
         \n\
         [cluster.object_store]\n\
         kind = \"local_directory\"\n\
         path = {object_store_path}\n",
        etcd_endpoint = cluster_config.etcd_endpoints[0],
        key_prefix = cluster_config.etcd_key_prefix,
        object_store_path = toml_path(object_store_path),
    )
}

/// The configuration file of one replica daemon: what an operator would write, with a registration
/// every second and small segment files.
fn replica_config_text(
    name: &str,
    data_directory: &Path,
    grpc_address: &str,
    observability_address: &str,
    cluster_config: &ClusterConfig,
) -> String {
    format!(
        "replica_id = {name:?}\n\
         data_directory = {data_directory}\n\
         listen_address = {grpc_address:?}\n\
         advertised_address = {grpc_address:?}\n\
         failure_domains = [{host:?}]\n\
         metrics_listen_address = {observability_address:?}\n\
         registration_interval_seconds = 1\n\
         \n\
         [storage]\n\
         preallocated_segment_bytes = 1048576\n\
         refuse_new_segments_at_used_percent = {WHOLE_DISK_PERCENT}\n\
         refuse_appends_at_used_percent = {WHOLE_DISK_PERCENT}\n\
         \n\
         {cluster_table}",
        data_directory = toml_path(data_directory),
        host = format!("host-of-{name}"),
        cluster_table = cluster_table_text(cluster_config),
    )
}

/// The configuration file of the controller daemon: what an operator would write, with a pass five
/// times a second and replicas presumed dead after three seconds of silence.
fn controller_config_text(observability_address: &str, cluster_config: &ClusterConfig) -> String {
    format!(
        "identity = \"controller\"\n\
         metrics_listen_address = {observability_address:?}\n\
         pass_interval_milliseconds = 200\n\
         replica_lost_after_seconds = 60\n\
         \n\
         [placement]\n\
         liveness_window_seconds = 3\n\
         refuse_at_used_percent = {WHOLE_DISK_PERCENT}\n\
         \n\
         {cluster_table}",
        cluster_table = cluster_table_text(cluster_config),
    )
}
