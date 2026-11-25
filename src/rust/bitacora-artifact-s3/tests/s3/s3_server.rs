//! A real S3 server for tests to talk to.
//!
//! # Why this file exists
//!
//! The S3 store is a thin adapter, so what can be wrong with it is an assumption about how an S3 service
//! answers: what a range past the end of an object returns, what a missing bucket looks like, how a key
//! with a space travels. A fake would repeat those assumptions. The tests therefore need a real server
//! that is started fresh, is reachable only from the test, and is gone when the test ends, including when
//! the test fails.
//!
//! # What it contains
//!
//! - [`S3Server`]: one running server with one bucket, stopped and wiped when dropped.
//! - [`shared_s3_server`]: the server that all tests running at the moment share.
//! - [`s3_server_binary`]: where the server program is, or `None` to skip.
//! - [`loopback_listener`], [`port_of`] and [`free_local_port`]: how tests get ports that nobody else is
//!   using.
//! - [`BUCKET`], [`ACCESS_KEY`] and [`SECRET_KEY`]: what the server is started with.
//!
//! # How it works
//!
//! The server is the Versity S3 Gateway with its POSIX backend: it serves a directory over the S3
//! protocol, and every directory directly below it is a bucket. [`S3Server::start`] creates that directory
//! with one bucket in it inside the test's temporary directory, asks the operating system for a free port,
//! starts the gateway on it and lists the bucket in a loop until the gateway answers. The gateway keeps
//! object metadata in a second directory (`--sidecar`) and not in extended attributes, so the tests do not
//! depend on what the file system of the temporary directory supports.
//!
//! The port is found by binding port zero and closing the listener again, so another program could in
//! theory take it before the gateway binds it; the gateway then exits and the test fails with a clear
//! message instead of misbehaving.
//!
//! Starting the gateway takes a moment, so tests share one: [`shared_s3_server`] hands out a counted
//! reference and starts a new process only when none is alive. The process is stopped when the last test
//! holding a reference finishes, which is what makes cleanup work without a hook that runs at the end of
//! the test binary. Tests stay independent because each one works under its own key prefix
//! ([`S3Server::store_in_fresh_namespace`]).
//!
//! # Invariants
//!
//! - An [`S3Server`] that exists has answered at least one request.
//! - Dropping it kills the process before its directories are removed, also while a test is panicking.
//! - Under Bazel a missing server program is a failure, never a skip: a test that silently does nothing
//!   must not look green in continuous integration.

// Production code must reach the network through the injected interfaces, which is why clippy forbids
// `TcpListener`, and the disk through `Disk`, which is why it forbids `create_dir_all`. This module only
// asks the operating system for port numbers and prepares the directory of a child process, and there is
// no interface for either.
#![allow(clippy::disallowed_types, clippy::disallowed_methods)]

use std::io::Write;
use std::net::{Ipv4Addr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Once, Weak};
use std::time::Duration;

use bitacora_artifact::ArtifactStore;
use bitacora_artifact_s3::{S3ArtifactStore, S3Credentials, S3Settings};
use tempfile::TempDir;
use tokio::sync::Mutex;
use tokio::time::Instant;

/// The environment variable that names the S3 server program to start.
const S3_SERVER_BINARY_VARIABLE: &str = "BITA_S3_SERVER_BINARY";

/// The one bucket the server is started with.
pub(crate) const BUCKET: &str = "bitacora-test";

/// The access key of the server's root account.
pub(crate) const ACCESS_KEY: &str = "bitacora-test-access-key";

/// The secret key of the server's root account.
pub(crate) const SECRET_KEY: &str = "bitacora-test-secret-key";

/// The region the server signs for unless told otherwise.
const REGION: &str = "us-east-1";

/// How long the server may take to start before the test gives up. It takes a fraction of a second on an
/// idle machine; the allowance is for a loaded continuous integration runner.
const STARTUP_ALLOWANCE: Duration = Duration::from_secs(30);

/// How long to wait between two attempts to reach a server that is starting.
const STARTUP_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// The request timeout of the store that probes a starting server. Short, so that an attempt made before
/// the server listens is given up quickly and the next one finds it ready.
const STARTUP_PROBE_TIMEOUT: Duration = Duration::from_millis(250);

/// The request timeout of the stores handed to tests. Much longer than any request takes, so a slow
/// machine does not fail a test that is not about timeouts.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

/// The server shared by the tests that are running, if one is alive. The lock is held while a new server
/// starts, so tests that begin together wait for one process instead of starting one each.
static SHARED_S3_SERVER: Mutex<Weak<S3Server>> = Mutex::const_new(Weak::new());

/// The number that makes the next fresh key prefix different from all the ones handed out before.
static NEXT_NAMESPACE_NUMBER: AtomicU64 = AtomicU64::new(0);

/// One running S3 server that belongs to the tests, with the bucket [`BUCKET`] in it.
///
/// Get the shared one from [`shared_s3_server`], or start a private one with [`S3Server::start`] when the
/// test is going to kill it. Dropping the value kills the server and removes its data.
pub(crate) struct S3Server {
    /// The URL stores connect to, `http://127.0.0.1:<port>`.
    endpoint: String,

    /// The server process, killed when this value is dropped.
    process: Child,

    /// The directory holding the server's buckets and metadata. Kept only so that it is removed when this
    /// value is dropped, after the process is gone.
    _directory: TempDir,
}

impl S3Server {
    /// Starts the server on a free local port and waits until it answers.
    ///
    /// # Arguments
    ///
    /// * `binary` - Path of the server program, as [`s3_server_binary`] returns it.
    ///
    /// # Returns
    ///
    /// The running server, with the empty bucket [`BUCKET`] and ready for requests.
    ///
    /// # Panics
    ///
    /// Panics like [`S3Server::start_on_port`].
    pub(crate) async fn start(binary: &Path) -> Self {
        Self::start_on_port(binary, free_local_port()).await
    }

    /// Starts the server on a port the test has chosen and waits until it answers. For tests that open a
    /// store before the server it points to exists.
    ///
    /// # Arguments
    ///
    /// * `binary` - Path of the server program, as [`s3_server_binary`] returns it.
    /// * `port` - The loopback port stores will connect to. It must be free, as a port from
    ///   [`free_local_port`] is.
    ///
    /// # Returns
    ///
    /// The running server, with the empty bucket [`BUCKET`] and ready for requests.
    ///
    /// # Panics
    ///
    /// Panics when the program cannot be started, when it exits while starting (a port that was taken in
    /// the meantime shows up this way), or when it does not answer within [`STARTUP_ALLOWANCE`]. In the
    /// last two cases the process is killed first.
    pub(crate) async fn start_on_port(binary: &Path, port: u16) -> Self {
        let directory = new_server_directory();
        let buckets_directory = directory.path().join("buckets");
        let metadata_directory = directory.path().join("metadata");
        std::fs::create_dir_all(buckets_directory.join(BUCKET)).expect("the bucket can be created");
        std::fs::create_dir_all(&metadata_directory)
            .expect("the metadata directory can be created");
        let listen_address = format!("127.0.0.1:{port}");

        let mut command = Command::new(binary);
        command
            // In the environment and not on the command line, where every user of the machine could
            // read them. They are not secrets here, but the habit is the right one.
            .env("ROOT_ACCESS_KEY", ACCESS_KEY)
            .env("ROOT_SECRET_KEY", SECRET_KEY)
            .arg("--port")
            .arg(&listen_address)
            .arg("--region")
            .arg(REGION)
            // No line per request on standard output.
            .arg("--quiet")
            .arg("posix")
            .arg("--sidecar")
            .arg(&metadata_directory)
            .arg(&buckets_directory)
            .stdin(Stdio::null());
        let process = match command.spawn() {
            Ok(process) => process,
            Err(error) => panic!(
                "cannot start the S3 server from {}: {error}",
                binary.display()
            ),
        };

        let mut server = Self {
            endpoint: format!("http://{listen_address}"),
            process,
            _directory: directory,
        };
        server.wait_until_ready().await;
        server
    }

    /// Lists the bucket in a loop until the server answers.
    ///
    /// # Panics
    ///
    /// Panics when the server exits or [`STARTUP_ALLOWANCE`] runs out first.
    async fn wait_until_ready(&mut self) {
        let mut probe_settings = self.settings();
        probe_settings.request_timeout = STARTUP_PROBE_TIMEOUT;
        let probe = open_store(&probe_settings);
        let deadline = Instant::now() + STARTUP_ALLOWANCE;
        loop {
            if probe.list_prefix("readiness/").await.is_ok() {
                return;
            }
            let exit_status = self
                .process
                .try_wait()
                .expect("the server's state can be read");
            if let Some(exit_status) = exit_status {
                panic!("the S3 server stopped while starting: {exit_status}");
            }
            assert!(
                Instant::now() < deadline,
                "the S3 server did not answer within {STARTUP_ALLOWANCE:?}"
            );
            tokio::time::sleep(STARTUP_POLL_INTERVAL).await;
        }
    }

    /// Returns settings that reach this server's bucket, without a key prefix, for a test to change in
    /// one place.
    pub(crate) fn settings(&self) -> S3Settings {
        let mut settings = S3Settings::new(BUCKET, REGION);
        settings.endpoint = Some(self.endpoint.clone());
        settings.allow_http = true;
        settings.request_timeout = REQUEST_TIMEOUT;
        settings
    }

    /// Opens a store on this server under the given key prefix.
    ///
    /// Two stores from this function with the same prefix see the same objects; with different prefixes
    /// they do not see each other.
    pub(crate) fn store_in(&self, key_prefix: &str) -> S3ArtifactStore {
        let mut settings = self.settings();
        key_prefix.clone_into(&mut settings.key_prefix);
        open_store(&settings)
    }

    /// Opens a store on this server under a key prefix that no other store has, so the test that asks for
    /// it starts from an empty store although the server is shared.
    pub(crate) fn store_in_fresh_namespace(&self) -> S3ArtifactStore {
        self.store_in(&fresh_namespace())
    }
}

impl Drop for S3Server {
    /// Kills the server and waits until it is gone, so that its port is free and its directory can be
    /// removed.
    fn drop(&mut self) {
        // Both calls can only fail when the server has already exited, and an exited server is what this
        // function is here to achieve.
        let _already_exited = self.process.kill();
        let _exit_status = self.process.wait();
    }
}

/// Opens a store with the given settings and the key pair of the test server's root account.
///
/// # Panics
///
/// Panics when the settings are invalid: the tests of invalid settings do not need a server.
pub(crate) fn open_store(settings: &S3Settings) -> S3ArtifactStore {
    let credentials = S3Credentials::new(ACCESS_KEY, SECRET_KEY);
    S3ArtifactStore::open_with_credentials(settings, &credentials)
        .expect("the settings of a test store are valid")
}

/// Returns a key prefix that no call has returned before.
pub(crate) fn fresh_namespace() -> String {
    let namespace_number = NEXT_NAMESPACE_NUMBER.fetch_add(1, Ordering::Relaxed);
    format!("test-{namespace_number}")
}

/// Returns the server shared by the tests of this binary, starting it when none is running.
///
/// Hold on to the returned reference for the whole test: the server is stopped when the last reference is
/// dropped.
///
/// # Returns
///
/// `None` when no server program is available and the test should pass without running (see
/// [`s3_server_binary`]).
///
/// # Panics
///
/// Panics when the server cannot be started (see [`S3Server::start`]).
pub(crate) async fn shared_s3_server() -> Option<Arc<S3Server>> {
    let binary = s3_server_binary()?;
    let mut shared = SHARED_S3_SERVER.lock().await;
    if let Some(running) = shared.upgrade() {
        return Some(running);
    }
    let started = Arc::new(S3Server::start(&binary).await);
    *shared = Arc::downgrade(&started);
    Some(started)
}

/// Returns the path of the S3 server program named by `BITA_S3_SERVER_BINARY`.
///
/// # Returns
///
/// `None` when the variable is not set, which is how a developer running `cargo test` without the server
/// skips these tests. The first call that returns `None` says so on standard error.
///
/// # Panics
///
/// Panics when the variable is not set under Bazel, which always provides the program: there, a missing
/// one means the build files are broken.
pub(crate) fn s3_server_binary() -> Option<PathBuf> {
    if let Some(binary) = std::env::var_os(S3_SERVER_BINARY_VARIABLE) {
        return Some(PathBuf::from(binary));
    }
    assert!(
        std::env::var_os("BAZEL_TEST").is_none(),
        "{S3_SERVER_BINARY_VARIABLE} is not set: the test target must pass the S3 server program"
    );
    announce_skipped_tests();
    None
}

/// Says once, on standard error, that the tests needing an S3 server are skipped.
fn announce_skipped_tests() {
    static ANNOUNCE: Once = Once::new();
    ANNOUNCE.call_once(|| {
        let announcement = format!(
            "\nSKIPPED: the tests that need an S3 server did not run. \
             Set {S3_SERVER_BINARY_VARIABLE} to the path of a versitygw program to run them, \
             or run them with Bazel, which brings its own.\n\n"
        );
        // Written to the stream itself, not with `eprintln!`: the test harness hides what a passing test
        // prints, and a skipped test passes.
        std::io::stderr()
            .write_all(announcement.as_bytes())
            .expect("standard error can be written");
    });
}

/// Opens a listener on a loopback port chosen by the operating system, which picks one that nothing else
/// is using.
///
/// Nothing ever accepts on the listener. A test keeps it open to have a server that takes connections and
/// never answers, or closes it to have a port that refuses them.
pub(crate) fn loopback_listener() -> TcpListener {
    TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("a loopback port can be bound")
}

/// Returns the port `listener` is bound to.
pub(crate) fn port_of(listener: &TcpListener) -> u16 {
    let address = listener
        .local_addr()
        .expect("a bound listener has an address");
    address.port()
}

/// Returns a loopback port that nothing is listening on right now: one the operating system just handed
/// out and that was closed again.
pub(crate) fn free_local_port() -> u16 {
    port_of(&loopback_listener())
}

/// Creates the empty directory the server keeps its buckets and metadata in, inside the test's own
/// temporary directory when Bazel provides one.
fn new_server_directory() -> TempDir {
    let parent = match std::env::var_os("TEST_TMPDIR") {
        Some(test_directory) => PathBuf::from(test_directory),
        None => std::env::temp_dir(),
    };
    tempfile::Builder::new()
        .prefix("bitacora-s3-")
        .tempdir_in(parent)
        .expect("a temporary directory can be created")
}
