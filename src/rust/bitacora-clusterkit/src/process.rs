//! A child process that a test owns.
//!
//! # Why this file exists
//!
//! A test that starts real daemons must never leave one behind: a leftover process holds its ports and
//! its data directory, and the next test run fails for reasons that have nothing to do with it. Tests
//! also fail, and a test that panics halfway must clean up just the same. This file wraps a child
//! process so that it dies with the value that owns it, and gives tests the three things they do to a
//! daemon: ask it to stop, kill it, and wait for it.
//!
//! # What it contains
//!
//! - [`ChildProcess`]: a running child, killed when dropped.
//! - [`free_local_ports`]: loopback ports that nothing listens on right now.
//!
//! # How it works
//!
//! The child's standard output and standard error go to one log file, so that a failing test can show
//! what the daemon said. [`ChildProcess::terminate`] sends SIGTERM by running the `kill` program, which
//! keeps this crate free of unsafe code and of a dependency for one system call.
//! [`ChildProcess::kill`] sends SIGKILL through the standard library.
//!
//! [`free_local_ports`] binds port zero as often as asked, reads the ports the operating system chose,
//! and closes the sockets again. Another program could take a port before the daemon binds it; the
//! daemon then fails to start and the test fails with that message, instead of misbehaving.
//!
//! # Invariants
//!
//! - Dropping a [`ChildProcess`] kills the child and waits until it is gone, also during a panic.
//! - A child that has exited is never signalled again: its process identifier may belong to another
//!   process by then.
//!
//! # See also
//!
//! - `daemons.rs`: starts the bitacora daemons as child processes.
//! - `etcd.rs`: starts etcd as one.

// This crate is a test fixture: starting processes, writing their logs and asking the operating system
// for ports is its purpose, and there is no injected interface for any of it.
#![allow(clippy::disallowed_types, clippy::disallowed_methods)]

use std::fs::File;
use std::net::{Ipv4Addr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::Duration;

use tokio::time::{Instant, sleep};

/// How long to wait between two looks at whether a child has exited.
const EXIT_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// How many lines of a child's log are shown when the test that owns it panics: enough to hold the
/// error a daemon logs when it stops, and few enough to be read in the output of a failed test.
const LOG_LINES_SHOWN_ON_PANIC: usize = 15;

/// A child process that belongs to a test. Dropping it kills the child.
#[derive(Debug)]
pub struct ChildProcess {
    /// What the process is, for messages: `replica-0`, `etcd`.
    name: String,
    /// The child.
    child: Child,
    /// The file the child's standard output and standard error go to.
    log_path: PathBuf,
    /// The status the child exited with, once it was observed. A child that exited must not be
    /// signalled or waited for again.
    exit_status: Option<ExitStatus>,
}

impl ChildProcess {
    /// Starts `command` as a child process, with its output going to a log file.
    ///
    /// # Arguments
    ///
    /// * `name` - What the process is, for messages.
    /// * `command` - The program and its arguments. Its standard streams are replaced.
    /// * `log_path` - The file that receives the child's standard output and standard error. It is
    ///   created, or emptied if it exists.
    ///
    /// # Panics
    ///
    /// Panics if the log file cannot be created or the program cannot be started. In a test fixture
    /// that is a broken test setup, and the message says which program.
    pub fn spawn(name: &str, command: &mut Command, log_path: &Path) -> Self {
        let log_file = match File::create(log_path) {
            Ok(log_file) => log_file,
            Err(io_error) => panic!("cannot create {}: {io_error}", log_path.display()),
        };
        let log_file_for_errors = log_file.try_clone().expect("an open file can be duplicated");
        command
            .stdin(Stdio::null())
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_file_for_errors));
        let child = match command.spawn() {
            Ok(child) => child,
            Err(io_error) => panic!("cannot start {name} ({command:?}): {io_error}"),
        };
        Self {
            name: name.to_owned(),
            child,
            log_path: log_path.to_owned(),
            exit_status: None,
        }
    }

    /// What the process is, as given to [`ChildProcess::spawn`].
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The operating system's identifier of the child.
    #[must_use]
    pub fn id(&self) -> u32 {
        self.child.id()
    }

    /// Asks the child to stop by sending it SIGTERM, as a supervisor does. Does nothing if the child
    /// has exited.
    ///
    /// # Panics
    ///
    /// Panics if the `kill` program cannot be run.
    pub fn terminate(&mut self) {
        if self.has_exited() {
            return;
        }
        let sent = Command::new("kill")
            .arg("-TERM")
            .arg(self.child.id().to_string())
            .status()
            .expect("the kill program can be run");
        // The child may have exited between the check and the signal; then `kill` fails, harmlessly.
        let _child_was_gone = !sent.success();
    }

    /// Kills the child at once with SIGKILL, as a crash or a lost machine would, and waits until it is
    /// gone. Does nothing if the child has exited.
    pub fn kill(&mut self) {
        if self.has_exited() {
            return;
        }
        // Both calls only fail when the child is already gone, which is what this is for.
        let _already_gone = self.child.kill();
        if let Ok(exit_status) = self.child.wait() {
            self.exit_status = Some(exit_status);
        }
    }

    /// Waits until the child has exited, but no longer than `limit`.
    ///
    /// # Arguments
    ///
    /// * `limit` - The longest to wait.
    ///
    /// # Returns
    ///
    /// The status the child exited with, or `None` if it is still running after `limit`.
    pub async fn wait_for_exit(&mut self, limit: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + limit;
        loop {
            if self.has_exited() {
                return self.exit_status;
            }
            if Instant::now() >= deadline {
                return None;
            }
            sleep(EXIT_POLL_INTERVAL).await;
        }
    }

    /// Whether the child has exited. Remembers its exit status the first time it sees it.
    ///
    /// # Panics
    ///
    /// If the operating system cannot say whether the child is running. A test cannot go on without
    /// knowing, so it fails there with the name of the process.
    pub fn has_exited(&mut self) -> bool {
        if self.exit_status.is_some() {
            return true;
        }
        match self.child.try_wait() {
            Ok(Some(exit_status)) => {
                self.exit_status = Some(exit_status);
                true
            }
            Ok(None) => false,
            Err(io_error) => panic!("cannot tell whether {} is running: {io_error}", self.name),
        }
    }

    /// Everything the child has written to its standard output and standard error so far.
    ///
    /// # Returns
    ///
    /// The log as text, or a note that it could not be read.
    #[must_use]
    pub fn log(&self) -> String {
        match std::fs::read(&self.log_path) {
            Ok(log_bytes) => String::from_utf8_lossy(&log_bytes).into_owned(),
            Err(io_error) => format!("(cannot read {}: {io_error})", self.log_path.display()),
        }
    }
}

impl Drop for ChildProcess {
    /// Kills the child and waits until it is gone, so that its ports are free and its files can be
    /// removed. During a panic it first shows the end of the child's log, which is usually the reason.
    fn drop(&mut self) {
        if std::thread::panicking() {
            let log = self.log();
            eprintln!("--- last output of {} ---", self.name);
            for line in last_lines_of(&log, LOG_LINES_SHOWN_ON_PANIC) {
                eprintln!("{line}");
            }
        }
        self.kill();
    }
}

/// The last `line_count` lines of `text`, in their order, or all of them if there are fewer.
fn last_lines_of(text: &str, line_count: usize) -> Vec<&str> {
    let lines: Vec<&str> = text.lines().collect();
    let first_shown = lines.len().saturating_sub(line_count);
    lines[first_shown..].to_vec()
}

/// Returns `count` different loopback ports that nothing is listening on right now.
///
/// The ports are bound all at once before any is closed again, which is what makes them different from
/// each other: asking the operating system for one port at a time can return the same port twice.
///
/// # Arguments
///
/// * `count` - How many ports are needed.
///
/// # Panics
///
/// Panics if a loopback port cannot be bound.
#[must_use]
pub fn free_local_ports(count: usize) -> Vec<u16> {
    let mut listeners = Vec::with_capacity(count);
    for _port_number in 0..count {
        listeners.push(TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("a loopback port can be bound"));
    }
    let mut ports = Vec::with_capacity(count);
    for listener in &listeners {
        ports.push(listener.local_addr().expect("a bound listener has an address").port());
    }
    ports
}

#[cfg(test)]
#[path = "process_test.rs"]
mod tests;
