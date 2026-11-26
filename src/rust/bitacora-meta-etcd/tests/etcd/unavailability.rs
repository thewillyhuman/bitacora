//! What a caller sees when etcd cannot be used.
//!
//! # Why this file exists
//!
//! The metadata store is on the path of every writer takeover and every segment roll, and etcd will be down or
//! unreachable at some point. The layers above are written for one rule: a request either answers or fails with
//! `StoreUnavailable` in bounded time. A request that waits forever would freeze a replica or a controller instead of
//! letting it retry, shed load or report. These tests hold the store to that rule in the three ways etcd can be
//! missing.
//!
//! # What it contains
//!
//! - Unreachable: nothing listens on the endpoint's port.
//! - Silent: something accepts the connection and never answers. Only the request timeout can end such a request.
//! - Gone: etcd answered, and was then killed.
//! - And back: a store that failed during an outage works when it ends.
//!
//! # How the fixtures work
//!
//! The first two cases need no etcd and always run. The last two start an etcd of their own, because the test decides
//! when it lives, and are skipped when no etcd program is available (see `etcd_process.rs`).
//!
//! Each test puts its own limit, [`LONGEST_ACCEPTABLE_WAIT`], around the request. It is far above the request timeout
//! of the store under test, so reaching it means the store's own bound did not work, and the test fails instead of
//! hanging until the test runner gives up.

use std::time::Duration;

use bitacora_meta::KeyValueStore;
use bitacora_meta_etcd::{EtcdKeyValueStore, EtcdSettings};

use crate::etcd_process::{EtcdProcess, etcd_binary, free_local_port, loopback_listener, port_of};
use crate::writes::{put, put_mutation};

/// The connect timeout of the stores these tests build.
const CONNECT_TIMEOUT: Duration = Duration::from_millis(200);

/// The request timeout of the stores these tests build. Short, because the test of a silent server waits for exactly
/// this long.
const REQUEST_TIMEOUT: Duration = Duration::from_millis(500);

/// How long a request of these tests may take before the test calls it hanging. Twenty times the request timeout, so
/// that a slow machine does not trip it.
const LONGEST_ACCEPTABLE_WAIT: Duration = Duration::from_secs(10);

/// Returns a store for the etcd that would be listening on `port` of the loopback address, with the short timeouts of
/// these tests.
async fn store_for_port(port: u16) -> EtcdKeyValueStore {
    let mut settings = EtcdSettings::new(vec![format!("http://127.0.0.1:{port}")]);
    settings.connect_timeout = CONNECT_TIMEOUT;
    settings.request_timeout = REQUEST_TIMEOUT;
    EtcdKeyValueStore::connect(settings)
        .await
        .expect("the settings are valid")
}

/// Awaits `request` and returns whether it failed.
///
/// # Panics
///
/// Panics when the request is still waiting after [`LONGEST_ACCEPTABLE_WAIT`]: the store did not bound it.
async fn fails_in_bounded_time<Response, Failure>(request: impl Future<Output = Result<Response, Failure>>) -> bool {
    let answer = tokio::time::timeout(LONGEST_ACCEPTABLE_WAIT, request).await;
    answer.expect("the store bounds every request").is_err()
}

// ---- unreachable: nothing listens ----

#[tokio::test]
async fn read_from_an_unreachable_etcd_fails() {
    let store = store_for_port(free_local_port()).await;

    let failed = fails_in_bounded_time(store.get("streams/orders")).await;

    assert!(failed);
}

#[tokio::test]
async fn listing_from_an_unreachable_etcd_fails() {
    let store = store_for_port(free_local_port()).await;

    let failed = fails_in_bounded_time(store.list("streams/")).await;

    assert!(failed);
}

#[tokio::test]
async fn write_to_an_unreachable_etcd_fails() {
    let store = store_for_port(free_local_port()).await;

    let failed = fails_in_bounded_time(put(&store, "streams/orders", b"v1")).await;

    assert!(failed);
}

// ---- silent: connects, never answers ----

/// The listener takes the connection, since the operating system completes the handshake for it, and nothing ever reads
/// from it. No error will ever come back, so this request ends only if the store's timeout ends it.
#[tokio::test]
async fn read_from_a_silent_server_fails_at_the_request_timeout() {
    let silent_server = loopback_listener();
    let store = store_for_port(port_of(&silent_server)).await;

    let failed = fails_in_bounded_time(store.get("streams/orders")).await;

    assert!(failed);
}

#[tokio::test]
async fn write_to_a_silent_server_fails_at_the_request_timeout() {
    let silent_server = loopback_listener();
    let store = store_for_port(port_of(&silent_server)).await;
    let transaction = store.transact(Vec::new(), vec![put_mutation("streams/orders", b"v1")]);

    let failed = fails_in_bounded_time(transaction).await;

    assert!(failed);
}

// ---- gone: answered, then killed ----

#[tokio::test]
async fn read_fails_once_etcd_is_killed() {
    let Some(binary) = etcd_binary() else {
        return;
    };
    let etcd = EtcdProcess::start(&binary).await;
    let store = etcd.store_in("/bitacora/").await;
    put(&store, "streams/orders", b"v1").await.expect("etcd is up");

    drop(etcd);

    let failed = fails_in_bounded_time(store.get("streams/orders")).await;
    assert!(failed);
}

#[tokio::test]
async fn write_fails_once_etcd_is_killed() {
    let Some(binary) = etcd_binary() else {
        return;
    };
    let etcd = EtcdProcess::start(&binary).await;
    let store = etcd.store_in("/bitacora/").await;
    put(&store, "streams/orders", b"v1").await.expect("etcd is up");

    drop(etcd);

    let failed = fails_in_bounded_time(put(&store, "streams/orders", b"v2")).await;
    assert!(failed);
}

// ---- and back ----

/// An outage ends. A store that was built while etcd was unreachable, and failed, must work once etcd is there, without
/// being built again: a process does not restart because its metadata store came back.
#[tokio::test]
async fn store_works_once_an_unreachable_etcd_comes_up() {
    let Some(binary) = etcd_binary() else {
        return;
    };
    let port = free_local_port();
    let store = store_for_port(port).await;
    let failed_during_the_outage = fails_in_bounded_time(store.get("streams/orders")).await;
    assert!(failed_during_the_outage);

    let _etcd = EtcdProcess::start_on_port(&binary, port).await;

    assert_eq!(store.get("streams/orders").await, Ok(None));
}
