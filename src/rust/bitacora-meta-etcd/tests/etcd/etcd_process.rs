//! A real etcd for tests to talk to.
//!
//! # Why this file exists
//!
//! The tests of the etcd backend need an etcd that is started fresh, is
//! reachable only from the test, and is gone when the test ends, including
//! when the test fails. Doing that by hand in every test would bury what
//! each test is about.
//!
//! # What it contains
//!
//! - [`EtcdProcess`]: one running etcd, stopped and wiped when dropped.
//! - [`shared_etcd`]: the etcd that all tests running at the moment share.
//! - [`etcd_binary`]: where the etcd program is, or `None` to skip.
//! - [`loopback_listener`], [`port_of`] and [`free_local_port`]: how tests
//!   get ports that nobody else is using.
//!
//! # How it works
//!
//! [`EtcdProcess::start`] asks the operating system for two free ports,
//! starts a single-member etcd on them with its data in a temporary
//! directory, and reads a key in a loop until etcd answers. The ports are
//! found by binding port zero and closing the listeners again, so another
//! program could in theory take one before etcd binds it; etcd then exits
//! and the test fails with a clear message instead of misbehaving.
//!
//! Starting etcd takes around a second, so tests share one: [`shared_etcd`]
//! hands out a counted reference and starts a new process only when none is
//! alive. The process is stopped when the last test holding a reference
//! finishes, which is what makes cleanup work without a hook that runs at
//! the end of the test binary. Tests stay independent because each one
//! works under its own key prefix
//! ([`EtcdProcess::store_in_fresh_namespace`]).
//!
//! # Invariants
//!
//! - An [`EtcdProcess`] that exists has answered at least one request.
//! - Dropping it kills the process before its data directory is removed,
//!   also while a test is panicking.
//! - Under Bazel a missing etcd program is a failure, never a skip: a test
//!   that silently does nothing must not look green in continuous
//!   integration.

// Production code must reach the network through the injected interfaces,
// which is why clippy forbids `TcpListener`. This module only uses it to ask
// the operating system for port numbers, and there is no interface for that.
#![allow(clippy::disallowed_types)]

use std::io::Write;
use std::net::{Ipv4Addr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Once, Weak};
use std::time::Duration;

use bitacora_meta::KeyValueStore;
use bitacora_meta_etcd::{EtcdKeyValueStore, EtcdSettings};
use tempfile::TempDir;
use tokio::sync::Mutex;
use tokio::time::Instant;

/// The environment variable that names the etcd program to start.
const ETCD_BINARY_VARIABLE: &str = "BITA_ETCD_BINARY";

/// The name of the single member of the test cluster. etcd wants the same
/// name in `--name` and in `--initial-cluster`.
const MEMBER_NAME: &str = "bitacora-test";

/// How long etcd may take to start and elect itself leader before the test
/// gives up. It takes about a second on an idle machine; the allowance is
/// for a loaded continuous integration runner.
const STARTUP_ALLOWANCE: Duration = Duration::from_secs(30);

/// How long to wait between two attempts to reach an etcd that is starting.
const STARTUP_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// The request timeout of the store that probes a starting etcd. Short, so
/// that an attempt made before etcd has a leader is given up quickly and
/// the next one finds it ready.
const STARTUP_PROBE_TIMEOUT: Duration = Duration::from_millis(250);

/// The request timeout of the stores handed to tests. Much longer than any
/// request takes, so a slow machine does not fail a test that is not about
/// timeouts.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// The etcd shared by the tests that are running, if one is alive. The
/// lock is held while a new etcd starts, so tests that begin together wait
/// for one process instead of starting one each.
static SHARED_ETCD: Mutex<Weak<EtcdProcess>> = Mutex::const_new(Weak::new());

/// The number that makes the next fresh key prefix different from all the
/// ones handed out before.
static NEXT_NAMESPACE_NUMBER: AtomicU64 = AtomicU64::new(0);

/// One running single-member etcd that belongs to the tests.
///
/// Get the shared one from [`shared_etcd`], or start a private one with
/// [`EtcdProcess::start`] when the test is going to kill it. Dropping the
/// value kills etcd and removes its data.
pub(crate) struct EtcdProcess {
    /// The URL stores connect to, `http://127.0.0.1:<port>`.
    client_endpoint: String,

    /// The etcd process, killed when this value is dropped.
    process: Child,

    /// The directory holding etcd's data. Kept only so that it is removed
    /// when this value is dropped, after the process is gone.
    _data_directory: TempDir,
}

impl EtcdProcess {
    /// Starts etcd on free local ports and waits until it answers.
    ///
    /// # Arguments
    ///
    /// * `binary` - Path of the etcd program, as [`etcd_binary`] returns it.
    ///
    /// # Returns
    ///
    /// The running etcd, empty and ready for requests.
    ///
    /// # Panics
    ///
    /// Panics like [`EtcdProcess::start_on_port`].
    pub(crate) async fn start(binary: &Path) -> Self {
        Self::start_on_port(binary, free_local_port()).await
    }

    /// Starts etcd with its client endpoint on a port the test has chosen,
    /// and waits until it answers. For tests that build a store before the
    /// etcd it points to exists.
    ///
    /// # Arguments
    ///
    /// * `binary` - Path of the etcd program, as [`etcd_binary`] returns it.
    /// * `client_port` - The loopback port stores will connect to. It must
    ///   be free, as a port from [`free_local_port`] is.
    ///
    /// # Returns
    ///
    /// The running etcd, empty and ready for requests.
    ///
    /// # Panics
    ///
    /// Panics when the program cannot be started, when it exits while
    /// starting (a port that was taken in the meantime shows up this way),
    /// or when it does not answer within [`STARTUP_ALLOWANCE`]. In the last
    /// two cases the process is killed first.
    pub(crate) async fn start_on_port(binary: &Path, client_port: u16) -> Self {
        let data_directory = new_data_directory();
        let peer_port = free_local_port_other_than(client_port);
        let client_endpoint = format!("http://127.0.0.1:{client_port}");
        let peer_endpoint = format!("http://127.0.0.1:{peer_port}");

        let mut command = Command::new(binary);
        command
            .arg("--name")
            .arg(MEMBER_NAME)
            .arg("--data-dir")
            .arg(data_directory.path())
            .arg("--listen-client-urls")
            .arg(&client_endpoint)
            .arg("--advertise-client-urls")
            .arg(&client_endpoint)
            .arg("--listen-peer-urls")
            .arg(&peer_endpoint)
            .arg("--initial-advertise-peer-urls")
            .arg(&peer_endpoint)
            .arg("--initial-cluster")
            .arg(format!("{MEMBER_NAME}={peer_endpoint}"))
            // The data does not outlive the test, so waiting for the disk
            // on every write would only make the tests slower.
            .arg("--unsafe-no-fsync")
            .arg("--log-level")
            .arg("error")
            .stdin(Stdio::null());
        let process = match command.spawn() {
            Ok(process) => process,
            Err(error) => panic!("cannot start etcd from {}: {error}", binary.display()),
        };

        let mut etcd = Self {
            client_endpoint,
            process,
            _data_directory: data_directory,
        };
        etcd.wait_until_ready().await;
        etcd
    }

    /// Reads a key in a loop until etcd answers.
    ///
    /// # Panics
    ///
    /// Panics when etcd exits or [`STARTUP_ALLOWANCE`] runs out first.
    async fn wait_until_ready(&mut self) {
        let probe = self.store_with("/readiness/", STARTUP_PROBE_TIMEOUT).await;
        let deadline = Instant::now() + STARTUP_ALLOWANCE;
        loop {
            if probe.get("probe").await.is_ok() {
                return;
            }
            let exit_status = self.process.try_wait().expect("etcd's state can be read");
            if let Some(exit_status) = exit_status {
                panic!("etcd stopped while starting: {exit_status}");
            }
            assert!(
                Instant::now() < deadline,
                "etcd did not answer within {STARTUP_ALLOWANCE:?}"
            );
            tokio::time::sleep(STARTUP_POLL_INTERVAL).await;
        }
    }

    /// Connects a store to this etcd under the given key prefix.
    ///
    /// Two stores from this function with the same prefix see the same
    /// data; with different prefixes they do not see each other.
    ///
    /// # Arguments
    ///
    /// * `key_prefix` - The prefix of every key of the store, for example
    ///   `/bitacora/`.
    pub(crate) async fn store_in(&self, key_prefix: &str) -> EtcdKeyValueStore {
        self.store_with(key_prefix, REQUEST_TIMEOUT).await
    }

    /// Connects a store to this etcd under a key prefix that no other store
    /// has, so the test that asks for it starts from an empty store
    /// although etcd is shared.
    pub(crate) async fn store_in_fresh_namespace(&self) -> EtcdKeyValueStore {
        let namespace_number = NEXT_NAMESPACE_NUMBER.fetch_add(1, Ordering::Relaxed);
        self.store_in(&format!("/test-{namespace_number}/")).await
    }

    /// Connects a store to this etcd with the given key prefix and request
    /// timeout.
    async fn store_with(&self, key_prefix: &str, request_timeout: Duration) -> EtcdKeyValueStore {
        let mut settings = EtcdSettings::new(vec![self.client_endpoint.clone()]);
        key_prefix.clone_into(&mut settings.key_prefix);
        settings.request_timeout = request_timeout;
        EtcdKeyValueStore::connect(settings)
            .await
            .expect("the settings of a test store are valid")
    }
}

impl Drop for EtcdProcess {
    /// Kills etcd and waits until it is gone, so that its ports are free
    /// and its data directory can be removed.
    fn drop(&mut self) {
        // Both calls can only fail when etcd has already exited, and an
        // exited etcd is what this function is here to achieve.
        let _already_exited = self.process.kill();
        let _exit_status = self.process.wait();
    }
}

/// Returns the etcd shared by the tests of this binary, starting it when
/// none is running.
///
/// Hold on to the returned reference for the whole test: etcd is stopped
/// when the last reference is dropped.
///
/// # Returns
///
/// `None` when no etcd program is available and the test should pass
/// without running (see [`etcd_binary`]).
///
/// # Panics
///
/// Panics when etcd cannot be started (see [`EtcdProcess::start`]).
pub(crate) async fn shared_etcd() -> Option<Arc<EtcdProcess>> {
    let binary = etcd_binary()?;
    let mut shared = SHARED_ETCD.lock().await;
    if let Some(running) = shared.upgrade() {
        return Some(running);
    }
    let started = Arc::new(EtcdProcess::start(&binary).await);
    *shared = Arc::downgrade(&started);
    Some(started)
}

/// Returns the path of the etcd program named by `BITA_ETCD_BINARY`.
///
/// # Returns
///
/// `None` when the variable is not set, which is how a developer running
/// `cargo test` without etcd skips these tests. The first call that
/// returns `None` says so on standard error.
///
/// # Panics
///
/// Panics when the variable is not set under Bazel, which always provides
/// the program: there, a missing one means the build files are broken.
pub(crate) fn etcd_binary() -> Option<PathBuf> {
    if let Some(binary) = std::env::var_os(ETCD_BINARY_VARIABLE) {
        return Some(PathBuf::from(binary));
    }
    assert!(
        std::env::var_os("BAZEL_TEST").is_none(),
        "{ETCD_BINARY_VARIABLE} is not set: the test target must pass the etcd program"
    );
    announce_skipped_tests();
    None
}

/// Says once, on standard error, that the tests needing etcd are skipped.
fn announce_skipped_tests() {
    static ANNOUNCE: Once = Once::new();
    ANNOUNCE.call_once(|| {
        let announcement = format!(
            "\nSKIPPED: the tests that need etcd did not run. \
             Set {ETCD_BINARY_VARIABLE} to the path of an etcd program to run them, \
             or run them with Bazel, which brings its own.\n\n"
        );
        // Written to the stream itself, not with `eprintln!`: the test
        // harness hides what a passing test prints, and a skipped test
        // passes.
        std::io::stderr()
            .write_all(announcement.as_bytes())
            .expect("standard error can be written");
    });
}

/// Opens a listener on a loopback port chosen by the operating system,
/// which picks one that nothing else is using.
///
/// Nothing ever accepts on the listener. A test keeps it open to have a
/// server that takes connections and never answers, or closes it to have a
/// port that refuses them.
pub(crate) fn loopback_listener() -> TcpListener {
    TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("a loopback port can be bound")
}

/// Returns the port `listener` is bound to.
pub(crate) fn port_of(listener: &TcpListener) -> u16 {
    let address = listener.local_addr().expect("a bound listener has an address");
    address.port()
}

/// Returns a loopback port that nothing is listening on right now: one the
/// operating system just handed out and that was closed again.
pub(crate) fn free_local_port() -> u16 {
    port_of(&loopback_listener())
}

/// Returns a loopback port that nothing is listening on right now and that
/// is not `taken_port`, for the second of two ports that must differ.
fn free_local_port_other_than(taken_port: u16) -> u16 {
    loop {
        let port = free_local_port();
        if port != taken_port {
            return port;
        }
    }
}

/// Creates the empty directory etcd keeps its data in, inside the test's
/// own temporary directory when Bazel provides one.
fn new_data_directory() -> TempDir {
    let parent = match std::env::var_os("TEST_TMPDIR") {
        Some(test_directory) => PathBuf::from(test_directory),
        None => std::env::temp_dir(),
    };
    tempfile::Builder::new()
        .prefix("bitacora-etcd-")
        .tempdir_in(parent)
        .expect("a temporary directory can be created")
}
