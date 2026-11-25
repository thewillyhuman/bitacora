//! Learning that the process was asked to stop.
//!
//! # Why this file exists
//!
//! A supervisor stops a daemon by sending it SIGTERM, and a person at a terminal by pressing Ctrl-C,
//! which sends SIGINT. A daemon that is simply killed by either drops requests in flight and leaves its
//! clients to find out by timeout. Both daemons must instead notice the signal, stop taking new work,
//! and exit cleanly. This file is how they notice.
//!
//! # What it contains
//!
//! - [`ShutdownListener`]: installed at start, then waited on.
//! - [`ShutdownSignal`]: which of the two signals arrived.
//!
//! # How it works
//!
//! Listening has two steps on purpose. [`ShutdownListener::install`] replaces the default reaction to
//! both signals, which is to kill the process, for the rest of the process's life. A daemon calls it
//! first thing, before it opens its data directory or its sockets, so that a signal that arrives during
//! a slow start is remembered and not fatal. [`ShutdownListener::wait`] then completes when the first of
//! the two signals has arrived, including one that arrived before it was called.
//!
//! A second signal while the daemon is shutting down has no effect; a supervisor that loses patience
//! sends SIGKILL, which no process can catch.
//!
//! # Invariants
//!
//! - After `install` returns, SIGTERM and SIGINT no longer kill the process.
//! - A signal that arrives after `install` is never lost.
//!
//! # See also
//!
//! - `http_endpoint.rs`: the readiness flag a daemon clears when the signal arrives.

use std::io;

use tokio::signal::unix::{Signal, SignalKind, signal};

/// Which signal asked the process to stop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShutdownSignal {
    /// SIGTERM: a supervisor, a container runtime or `kill`.
    Terminate,
    /// SIGINT: Ctrl-C at a terminal.
    Interrupt,
}

/// The process's ear for SIGTERM and SIGINT.
///
/// # Examples
///
/// ```no_run
/// use bitacora_process::ShutdownListener;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> std::io::Result<()> {
/// // First thing at start, inside the runtime:
/// let shutdown_listener = ShutdownListener::install()?;
///
/// // ... open the data directory, start serving ...
///
/// let signal = shutdown_listener.wait().await;
/// println!("stopping after {signal:?}");
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct ShutdownListener {
    /// Yields once per SIGTERM received since it was installed.
    terminate_signals: Signal,
    /// Yields once per SIGINT received since it was installed.
    interrupt_signals: Signal,
}

impl ShutdownListener {
    /// Starts listening for SIGTERM and SIGINT. From now on neither kills the process.
    ///
    /// Must be called from inside a Tokio runtime.
    ///
    /// # Errors
    ///
    /// Fails if the operating system refuses to install a signal handler.
    pub fn install() -> io::Result<Self> {
        Ok(Self {
            terminate_signals: signal(SignalKind::terminate())?,
            interrupt_signals: signal(SignalKind::interrupt())?,
        })
    }

    /// Waits until the process is asked to stop.
    ///
    /// # Returns
    ///
    /// The first signal that arrived since [`ShutdownListener::install`]. Returns at once if one
    /// arrived before this call.
    pub async fn wait(mut self) -> ShutdownSignal {
        tokio::select! {
            _received = self.terminate_signals.recv() => ShutdownSignal::Terminate,
            _received = self.interrupt_signals.recv() => ShutdownSignal::Interrupt,
        }
    }
}

#[cfg(test)]
#[path = "shutdown_test.rs"]
mod tests;
