//! Tests of the entry point of `bita-soak`.
//!
//! # What is covered
//!
//! - The exit code of every way a run can end: only a clean run exits with 0, and no two ways share a code.
//! - The tool stays up after a run that left evidence, for as long as it was told to, and not after any other run.
//! - The metrics endpoint: without an address nothing is served; with one, the counters of the run can be scraped;
//!   an address that cannot be listened on is an error with the address in it.
//! - The client's writers record the tool's name with their leases.
//!
//! # How the fixtures work
//!
//! The lingering tests run on a paused clock and measure the virtual time that passed. The metrics tests listen on
//! a port the operating system picks, which they find by listening first and asking for the address.

// The tests talk to the tool's own metrics endpoint over a socket.
#![allow(clippy::disallowed_types)]

use std::io::{Read as _, Write as _};

use super::*;

/// A stop that nobody fires, and the sender that keeps it that way.
fn no_stop() -> (watch::Sender<bool>, watch::Receiver<bool>) {
    watch::channel(false)
}

/// How long `linger_unless_clean` kept the process up after a run that ended as `verdict`, told to linger for a
/// minute.
async fn lingered_after(verdict: RunVerdict) -> Duration {
    let (_told_to_stop, mut stop) = no_stop();
    let started_at = Instant::now();
    linger_unless_clean(verdict, Duration::from_secs(60), &mut stop).await;
    started_at.elapsed()
}

/// Fetches `/metrics` from `address` and returns the response, headers included.
fn scrape(address: std::net::SocketAddr) -> String {
    let mut connection = std::net::TcpStream::connect(address).expect("the endpoint accepts connections");
    connection
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: bita-soak\r\nConnection: close\r\n\r\n")
        .expect("the request is sent");
    let mut response = String::new();
    connection.read_to_string(&mut response).expect("the response is read");
    response
}

#[test]
fn only_a_clean_run_exits_with_zero() {
    let mut codes = Vec::new();
    for verdict in [
        RunVerdict::Clean,
        RunVerdict::CouldNotStart,
        RunVerdict::Violated,
        RunVerdict::NotRecovered,
        RunVerdict::Interrupted,
    ] {
        codes.push(exit_code_of(verdict));
    }

    assert_eq!(codes, [0, 1, 3, 4, 5]);
}

/// Exit code 2 is what the command line parser uses for a mistake; no verdict may look like one.
#[test]
fn no_verdict_shares_the_exit_code_of_a_usage_error() {
    assert_eq!(EXIT_USAGE, 2);
}

#[tokio::test(start_paused = true)]
async fn tool_stays_up_after_a_violated_run() {
    assert_eq!(lingered_after(RunVerdict::Violated).await, Duration::from_secs(60));
}

#[tokio::test(start_paused = true)]
async fn tool_stays_up_after_a_run_that_did_not_recover() {
    assert_eq!(lingered_after(RunVerdict::NotRecovered).await, Duration::from_secs(60));
}

#[tokio::test(start_paused = true)]
async fn tool_does_not_stay_up_after_a_clean_run() {
    assert_eq!(lingered_after(RunVerdict::Clean).await, Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn tool_does_not_stay_up_after_it_was_told_to_stop() {
    assert_eq!(lingered_after(RunVerdict::Interrupted).await, Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn lingering_ends_when_the_tool_is_told_to_stop() {
    let (told_to_stop, mut stop) = no_stop();
    let started_at = Instant::now();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        told_to_stop.send(true).expect("the tool listens");
    });

    linger_unless_clean(RunVerdict::Violated, Duration::from_secs(60), &mut stop).await;

    assert_eq!(started_at.elapsed(), Duration::from_secs(5));
}

#[tokio::test]
async fn nothing_is_served_without_an_address() {
    let (_told_to_stop, stop) = no_stop();

    let served = serve_metrics(None, &SoakMetrics::new(), stop).await;

    assert_eq!(served, Ok(None));
}

#[tokio::test(flavor = "multi_thread")]
async fn counters_of_the_run_can_be_scraped() {
    let (_told_to_stop, stop) = no_stop();
    let metrics = SoakMetrics::new();

    let served = serve_metrics(Some("127.0.0.1:0"), &metrics, stop).await;

    let address = served
        .expect("the address can be listened on")
        .expect("an address was given");
    let response = tokio::task::spawn_blocking(move || scrape(address))
        .await
        .expect("the scrape ends");
    assert!(response.contains("bitacora_soak_rounds_clean_total 0\n"), "{response}");
}

#[tokio::test]
async fn address_that_cannot_be_listened_on_is_an_error_that_names_it() {
    let (_told_to_stop, stop) = no_stop();

    let served = serve_metrics(Some("not-an-address"), &SoakMetrics::new(), stop).await;

    let reason = served.expect_err("nothing can listen there");
    assert!(
        reason.starts_with("cannot serve metrics on not-an-address: "),
        "{reason}"
    );
}

#[test]
fn writers_of_the_tool_record_its_name_with_their_leases() {
    assert_eq!(client_settings().writer.writer_identity, "bita-soak");
}

/// This version of the tool has no way to break a cluster: its rounds are plain workloads, judged.
#[test]
fn nemesis_of_this_version_injects_nothing() {
    assert_eq!(nemesis_settings().kinds, Vec::new());
}
