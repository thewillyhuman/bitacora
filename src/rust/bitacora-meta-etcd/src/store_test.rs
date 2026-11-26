//! Tests of the etcd store that need no etcd.
//!
//! # What is covered
//!
//! - Connecting: it contacts nobody, and it refuses settings and endpoints that cannot work.
//! - Keys: the store's prefix goes in front of every key.
//! - Revisions: which revisions of the interface etcd can have given to a key.
//! - The bound on every request: an answer is passed on, an error of the client and a call that never answers both
//!   come back as `StoreUnavailable`.
//!
//! What a request does on a real etcd is tested in `tests/etcd/`.
//!
//! # How the fixtures work
//!
//! `store_with` prepares a store for an endpoint on which nobody listens. That is enough here: connecting sends
//! nothing, and no test of this file sends a request. The tests of the request bound hand the store futures of their
//! own instead of calls to etcd.

use std::future;

use super::*;

/// An endpoint on which nobody listens: the discard port of the loopback address.
const UNUSED_ENDPOINT: &str = "http://127.0.0.1:9";

/// A request timeout short enough to wait out in a test, for a call that never answers.
const SHORT_REQUEST_TIMEOUT: Duration = Duration::from_millis(20);

/// Returns default settings for one endpoint.
fn settings_for(endpoint: &str) -> EtcdSettings {
    EtcdSettings::new(vec![endpoint.to_owned()])
}

/// Returns a store with the given key prefix and request timeout, prepared for [`UNUSED_ENDPOINT`].
async fn store_with(key_prefix: &str, request_timeout: Duration) -> EtcdKeyValueStore {
    let mut settings = settings_for(UNUSED_ENDPOINT);
    key_prefix.clone_into(&mut settings.key_prefix);
    settings.request_timeout = request_timeout;
    EtcdKeyValueStore::connect(settings)
        .await
        .expect("the settings are valid")
}

/// Connecting sends nothing, so a store can be prepared while etcd is down.
#[tokio::test]
async fn connecting_does_not_need_etcd_to_be_up() {
    let store = EtcdKeyValueStore::connect(settings_for(UNUSED_ENDPOINT)).await;

    assert!(store.is_ok());
}

#[tokio::test]
async fn connecting_without_endpoints_is_refused() {
    let store = EtcdKeyValueStore::connect(EtcdSettings::new(Vec::new())).await;

    assert_eq!(store.err(), Some(InvalidEtcdSettings::because("there is no endpoint")));
}

#[tokio::test]
async fn connecting_to_a_malformed_endpoint_is_refused() {
    let store = EtcdKeyValueStore::connect(settings_for("http://not a url")).await;

    assert!(store.is_err());
}

/// TLS is not compiled in yet, and saying so at start beats failing every request later.
#[tokio::test]
async fn connecting_to_an_https_endpoint_is_refused() {
    let store = EtcdKeyValueStore::connect(settings_for("https://127.0.0.1:2379")).await;

    assert!(store.is_err());
}

#[tokio::test]
async fn key_is_sent_with_the_store_prefix_in_front() {
    let store = store_with("/bitacora/", SHORT_REQUEST_TIMEOUT).await;

    let etcd_key = store.to_etcd_key("streams/orders");

    assert_eq!(etcd_key, "/bitacora/streams/orders");
}

#[tokio::test]
async fn key_is_sent_as_it_is_when_the_prefix_is_empty() {
    let store = store_with("", SHORT_REQUEST_TIMEOUT).await;

    let etcd_key = store.to_etcd_key("streams/orders");

    assert_eq!(etcd_key, "streams/orders");
}

#[test]
fn revision_of_an_existing_key_is_the_same_number_in_etcd() {
    let etcd_revision = to_etcd_revision(Revision::new(7));

    assert_eq!(etcd_revision, Some(7));
}

#[test]
fn largest_revision_etcd_counts_is_kept() {
    let etcd_revision = to_etcd_revision(Revision::new(i64::MAX.unsigned_abs()));

    assert_eq!(etcd_revision, Some(i64::MAX));
}

/// etcd reports revision zero for a key that does not exist, so no key that exists can be at it.
#[test]
fn revision_zero_is_one_etcd_never_gives_to_a_key() {
    let etcd_revision = to_etcd_revision(Revision::new(0));

    assert_eq!(etcd_revision, None);
}

#[test]
fn revision_beyond_what_etcd_counts_is_one_etcd_never_gives_to_a_key() {
    let etcd_revision = to_etcd_revision(Revision::new(u64::MAX));

    assert_eq!(etcd_revision, None);
}

#[tokio::test]
async fn call_that_answers_in_time_returns_its_answer() {
    let store = store_with("/bitacora/", SHORT_REQUEST_TIMEOUT).await;
    let answered_call = future::ready(Ok::<&str, etcd_client::Error>("the answer"));

    let outcome = store.await_within_the_request_timeout("read", answered_call).await;

    assert_eq!(outcome, Ok("the answer"));
}

#[tokio::test]
async fn call_that_fails_is_reported_as_unavailable_with_the_operation() {
    let store = store_with("/bitacora/", SHORT_REQUEST_TIMEOUT).await;
    let client_error = etcd_client::Error::InvalidArgs("refused by the client".to_owned());
    let failed_call = future::ready(Err::<(), etcd_client::Error>(client_error));

    let outcome = store.await_within_the_request_timeout("read", failed_call).await;

    let failure = outcome.expect_err("the call failed");
    assert!(failure.reason.starts_with("etcd read failed: "), "{failure}");
}

/// A server that takes the request and never answers is the outage that only a timeout ends.
#[tokio::test]
async fn call_that_never_answers_fails_at_the_request_timeout() {
    let store = store_with("/bitacora/", SHORT_REQUEST_TIMEOUT).await;
    let silent_call = future::pending::<Result<(), etcd_client::Error>>();

    let outcome = store.await_within_the_request_timeout("read", silent_call).await;

    let failure = outcome.expect_err("the call never answered");
    assert_eq!(failure.reason, "etcd did not answer the read within 20ms");
}
