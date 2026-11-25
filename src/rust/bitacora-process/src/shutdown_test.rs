//! Tests of the shutdown listener.
//!
//! # What is covered
//!
//! - SIGTERM and SIGINT sent to the process complete the wait, each as itself, and do not kill it.
//! - A signal that arrives before anybody waits is remembered.
//! - Without a signal the wait does not complete.
//!
//! # How the fixtures work
//!
//! The tests send real signals to the test process with the `kill` program. That is only safe once a
//! listener is installed, so every test installs one first; from the first install on, the signals no
//! longer kill the process, which is what lets the tests of this file run next to all the others.
//!
//! A signal reaches every listener of the process. The tests therefore take a lock, so that a signal
//! sent by one of them is never taken for the signal of another.

use std::process::Command;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::timeout;

use super::*;

/// Held by the test that is currently sending signals.
static SIGNALS_IN_USE: Mutex<()> = Mutex::const_new(());

/// Long enough for a signal to travel from `kill` to the listener on a loaded machine.
const GENEROUS_WAIT: Duration = Duration::from_secs(20);

/// Sends `signal_name`, for example `TERM`, to this process.
fn send_to_this_process(signal_name: &str) {
    let sent = Command::new("kill")
        .arg(format!("-{signal_name}"))
        .arg(std::process::id().to_string())
        .status()
        .expect("the kill program can be run");
    assert!(sent.success(), "kill exited with {sent}");
}

#[tokio::test]
async fn terminate_signal_completes_the_wait() {
    let _signals = SIGNALS_IN_USE.lock().await;
    let listener = ShutdownListener::install().expect("handlers can be installed");

    send_to_this_process("TERM");
    let received = timeout(GENEROUS_WAIT, listener.wait()).await;

    assert_eq!(received.ok(), Some(ShutdownSignal::Terminate));
}

#[tokio::test]
async fn interrupt_signal_completes_the_wait() {
    let _signals = SIGNALS_IN_USE.lock().await;
    let listener = ShutdownListener::install().expect("handlers can be installed");

    send_to_this_process("INT");
    let received = timeout(GENEROUS_WAIT, listener.wait()).await;

    assert_eq!(received.ok(), Some(ShutdownSignal::Interrupt));
}

/// A daemon installs the listener first and waits much later, after a start that may be slow.
#[tokio::test]
async fn signal_that_arrives_before_the_wait_is_remembered() {
    let _signals = SIGNALS_IN_USE.lock().await;
    let listener = ShutdownListener::install().expect("handlers can be installed");
    send_to_this_process("TERM");
    // Let the signal arrive while nobody waits.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let received = timeout(GENEROUS_WAIT, listener.wait()).await;

    assert_eq!(received.ok(), Some(ShutdownSignal::Terminate));
}

#[tokio::test]
async fn wait_does_not_complete_without_a_signal() {
    let _signals = SIGNALS_IN_USE.lock().await;
    let listener = ShutdownListener::install().expect("handlers can be installed");

    let received = timeout(Duration::from_millis(200), listener.wait()).await;

    assert!(received.is_err(), "{received:?}");
}
