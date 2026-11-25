//! Where the programs of a real cluster are.
//!
//! # Why this file exists
//!
//! A test that runs a cluster of real processes needs three programs: etcd, the replica daemon and the
//! controller daemon. Bazel builds or fetches all three and tells the test where they are. Under Cargo
//! nothing does, and a developer who runs `cargo test` without them should see the test skipped with a
//! clear message, not fail in a confusing way. This file is where the programs are looked up and where
//! that difference is decided.
//!
//! # What it contains
//!
//! - [`Binaries`]: the paths of the three programs.
//! - [`Binaries::from_environment`]: finds them, or says that the test should be skipped.
//! - The names of the environment variables that carry the paths.
//!
//! # How it works
//!
//! Each program's path comes from an environment variable. The Bazel test target sets all three from
//! its `data` dependencies. Under Cargo a developer sets them by hand, for example to the binaries in
//! `target/debug` and an etcd from the system.
//!
//! If a variable is missing, the test is skipped: [`Binaries::from_environment`] returns `None` and
//! says so once on standard error. Under Bazel a missing variable is a broken build file, never a
//! reason to skip, so it panics instead: a test that silently does nothing must not look green in
//! continuous integration.
//!
//! # Invariants
//!
//! - Every path in a [`Binaries`] is absolute, so it still names the program after a child process
//!   changed its working directory.
//!
//! # See also
//!
//! - `cluster.rs`: starts the programs.

// This crate is a test fixture; reading the environment and resolving paths is its purpose.
#![allow(clippy::disallowed_methods)]

use std::io::Write;
use std::path::PathBuf;
use std::sync::Once;

/// The environment variable that names the etcd program.
pub const ETCD_BINARY_VARIABLE: &str = "BITA_ETCD_BINARY";

/// The environment variable that names the `bita-replica` program.
pub const REPLICA_BINARY_VARIABLE: &str = "BITA_REPLICA_BINARY";

/// The environment variable that names the `bita-controller` program.
pub const CONTROLLER_BINARY_VARIABLE: &str = "BITA_CONTROLLER_BINARY";

/// The variable Bazel sets in every test it runs.
const BAZEL_TEST_VARIABLE: &str = "BAZEL_TEST";

/// The programs a real cluster is made of.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Binaries {
    /// The etcd server.
    pub etcd: PathBuf,
    /// The replica daemon, `bita-replica`.
    pub replica: PathBuf,
    /// The controller daemon, `bita-controller`.
    pub controller: PathBuf,
}

impl Binaries {
    /// Finds the three programs through their environment variables.
    ///
    /// # Returns
    ///
    /// The programs, or `None` when a variable is not set and the test should pass without running.
    /// The first call that returns `None` says so on standard error.
    ///
    /// # Panics
    ///
    /// Panics when a variable is not set under Bazel, which always provides the programs: there, a
    /// missing one means the build files are broken.
    #[must_use]
    pub fn from_environment() -> Option<Self> {
        let etcd = program_from(ETCD_BINARY_VARIABLE);
        let replica = program_from(REPLICA_BINARY_VARIABLE);
        let controller = program_from(CONTROLLER_BINARY_VARIABLE);
        if let (Some(etcd), Some(replica), Some(controller)) = (etcd, replica, controller) {
            return Some(Self {
                etcd,
                replica,
                controller,
            });
        }
        assert!(
            std::env::var_os(BAZEL_TEST_VARIABLE).is_none(),
            "{ETCD_BINARY_VARIABLE}, {REPLICA_BINARY_VARIABLE} and {CONTROLLER_BINARY_VARIABLE} must \
             all be set: the test target must pass the programs"
        );
        announce_skipped_tests();
        None
    }
}

/// Reads the path of a program from `variable` and makes it absolute.
fn program_from(variable: &str) -> Option<PathBuf> {
    let path = PathBuf::from(std::env::var_os(variable)?);
    if path.is_absolute() {
        return Some(path);
    }
    let working_directory = std::env::current_dir().expect("the working directory can be read");
    Some(working_directory.join(path))
}

/// Says once, on standard error, that the tests needing real processes are skipped.
fn announce_skipped_tests() {
    static ANNOUNCE: Once = Once::new();
    ANNOUNCE.call_once(|| {
        let announcement = format!(
            "\nSKIPPED: the tests that run a cluster of real processes did not run. Set \
             {ETCD_BINARY_VARIABLE}, {REPLICA_BINARY_VARIABLE} and {CONTROLLER_BINARY_VARIABLE} to \
             the paths of the programs to run them, or run them with Bazel, which brings its own.\n\n"
        );
        // Written to the stream itself, not with `eprintln!`: the test harness hides what a passing
        // test prints, and a skipped test passes.
        std::io::stderr()
            .write_all(announcement.as_bytes())
            .expect("standard error can be written");
    });
}
