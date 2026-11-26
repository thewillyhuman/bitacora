//! What the store reports when the service cannot do what was asked.
//!
//! # What is covered
//!
//! - A bucket that does not exist is an outage with a reason that names the bucket problem, on every
//!   operation, and never a missing object: a reader must not conclude that a tiered segment is gone when
//!   the configuration is wrong.
//! - Wrong credentials are an outage.
//! - A server that is down, one that stops in the middle of a test, and one that accepts connections and
//!   never answers all fail within bounded time.
//!
//! # How the fixtures work
//!
//! Tests of a wrong bucket or wrong credentials use the shared server with settings changed in one place.
//! Tests that need a server to be absent use a port nothing listens on, a listener nothing accepts on, or a
//! private server that the test kills.

use std::time::Duration;

use bitacora_artifact::{ArtifactError, ArtifactStore};
use bitacora_artifact_s3::{S3ArtifactStore, S3Credentials, S3Settings};
use bitacora_testports::take_shared_port;
use bytes::Bytes;
use tokio::time::Instant;

use crate::s3_server::{
    ACCESS_KEY, BUCKET, S3Server, SECRET_KEY, fresh_namespace, loopback_listener, open_store, port_of,
    s3_server_binary, shared_s3_server,
};

/// A request timeout short enough that a test of a silent server ends quickly, and long enough that a
/// loaded machine cannot make it expire early.
const SHORT_REQUEST_TIMEOUT: Duration = Duration::from_millis(500);

/// How much longer than its timeout a failing operation may take before a test calls it unbounded.
const SCHEDULING_ALLOWANCE: Duration = Duration::from_secs(10);

/// Opens a store on `server` for a bucket that was never created.
fn store_for_a_missing_bucket(server: &S3Server) -> S3ArtifactStore {
    let mut settings = server.settings();
    "no-such-bucket".clone_into(&mut settings.bucket);
    open_store(&settings)
}

/// Opens a store for the test bucket of a service at `endpoint`, with the given request timeout.
fn store_at(endpoint: &str, request_timeout: Duration) -> S3ArtifactStore {
    let mut settings = S3Settings::new(BUCKET, "us-east-1");
    settings.endpoint = Some(endpoint.to_owned());
    settings.allow_http = true;
    settings.request_timeout = request_timeout;
    S3ArtifactStore::open_with_credentials(&settings, &S3Credentials::new(ACCESS_KEY, SECRET_KEY))
        .expect("the settings are valid")
}

/// The reason of an `Unavailable` error.
///
/// # Panics
///
/// Panics when `outcome` is anything else: the test expected an outage.
fn unavailable_reason<Response: std::fmt::Debug>(outcome: Result<Response, ArtifactError>) -> String {
    match outcome {
        Err(ArtifactError::Unavailable { reason }) => reason,
        other => panic!("expected an unavailable error, got {other:?}"),
    }
}

#[tokio::test]
async fn put_into_a_missing_bucket_is_unavailable() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = store_for_a_missing_bucket(&server);

    let outcome = store.put_object("a", Bytes::from_static(b"x")).await;

    let reason = unavailable_reason(outcome);
    assert!(reason.contains("NoSuchBucket"), "{reason}");
}

/// The dangerous one: a missing bucket answers `404`, like a missing object.
#[tokio::test]
async fn get_from_a_missing_bucket_is_unavailable_and_not_a_missing_object() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = store_for_a_missing_bucket(&server);

    let outcome = store.get_object("a").await;

    let reason = unavailable_reason(outcome);
    assert!(reason.contains("NoSuchBucket"), "{reason}");
}

#[tokio::test]
async fn range_read_from_a_missing_bucket_is_unavailable_and_not_a_missing_object() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = store_for_a_missing_bucket(&server);

    let outcome = store.get_range("a", 0..10).await;

    let reason = unavailable_reason(outcome);
    assert!(reason.contains("NoSuchBucket"), "{reason}");
}

#[tokio::test]
async fn delete_in_a_missing_bucket_is_unavailable_and_not_a_success() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = store_for_a_missing_bucket(&server);

    let outcome = store.delete_object("a").await;

    let reason = unavailable_reason(outcome);
    assert!(reason.contains("NoSuchBucket"), "{reason}");
}

#[tokio::test]
async fn listing_a_missing_bucket_is_unavailable_and_not_empty() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = store_for_a_missing_bucket(&server);

    let outcome = store.list_prefix("").await;

    let reason = unavailable_reason(outcome);
    assert!(reason.contains("NoSuchBucket"), "{reason}");
}

#[tokio::test]
async fn wrong_secret_key_is_unavailable() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let wrong_secret = S3Credentials::new(ACCESS_KEY, "not-the-secret-key");
    let store =
        S3ArtifactStore::open_with_credentials(&server.settings(), &wrong_secret).expect("the settings are valid");

    let outcome = store.list_prefix("").await;

    unavailable_reason(outcome);
}

#[tokio::test]
async fn unknown_access_key_is_unavailable() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let unknown_account = S3Credentials::new("nobody-knows-this-key", SECRET_KEY);
    let store =
        S3ArtifactStore::open_with_credentials(&server.settings(), &unknown_account).expect("the settings are valid");

    let outcome = store.put_object("a", Bytes::from_static(b"x")).await;

    unavailable_reason(outcome);
}

#[tokio::test]
async fn wrong_credentials_never_store_anything() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let mut settings = server.settings();
    settings.key_prefix = fresh_namespace();
    let wrong_secret = S3Credentials::new(ACCESS_KEY, "not-the-secret-key");
    let intruder = S3ArtifactStore::open_with_credentials(&settings, &wrong_secret).expect("the settings are valid");
    let _refused = intruder.put_object("a", Bytes::from_static(b"should not land")).await;

    let listed = open_store(&settings).list_prefix("").await;

    assert_eq!(listed, Ok(Vec::new()));
}

/// Needs no server at all, so it also runs where the server program is missing.
#[tokio::test]
async fn service_that_nothing_listens_at_is_unavailable() {
    let endpoint = format!("http://127.0.0.1:{}", take_shared_port());
    let store = store_at(&endpoint, Duration::from_secs(20));
    let asked_at = Instant::now();

    let outcome = store.get_object("a").await;

    unavailable_reason(outcome);
    assert!(
        asked_at.elapsed() < SCHEDULING_ALLOWANCE,
        "a refused connection took {:?} to report",
        asked_at.elapsed()
    );
}

/// Needs no server program either: a listener that nothing accepts on takes the connection and stays
/// silent.
#[tokio::test]
async fn service_that_never_answers_fails_at_the_request_timeout() {
    let silent_listener = loopback_listener();
    let endpoint = format!("http://127.0.0.1:{}", port_of(&silent_listener));
    let store = store_at(&endpoint, SHORT_REQUEST_TIMEOUT);
    let asked_at = Instant::now();

    let outcome = store.put_object("a", Bytes::from_static(b"x")).await;

    unavailable_reason(outcome);
    assert!(
        asked_at.elapsed() >= SHORT_REQUEST_TIMEOUT,
        "gave up after only {:?}",
        asked_at.elapsed()
    );
    assert!(
        asked_at.elapsed() < SHORT_REQUEST_TIMEOUT + SCHEDULING_ALLOWANCE,
        "a silent service took {:?} to report",
        asked_at.elapsed()
    );
}

#[tokio::test]
async fn store_fails_once_its_server_stops() {
    let Some(binary) = s3_server_binary() else {
        return;
    };
    let server = S3Server::start(&binary).await;
    let store = server.store_in("");
    store
        .put_object("a", Bytes::from_static(b"x"))
        .await
        .expect("the server is up");

    drop(server);
    let outcome = store.get_object("a").await;

    unavailable_reason(outcome);
}

/// A store is opened once at start and kept: it must not need reopening after the service was away.
#[tokio::test]
async fn store_opened_before_its_server_was_up_works_once_it_is() {
    let Some(binary) = s3_server_binary() else {
        return;
    };
    let port = take_shared_port();
    let store = store_at(&format!("http://127.0.0.1:{port}"), Duration::from_secs(20));
    let while_down = store.put_object("a", Bytes::from_static(b"x")).await;
    let _server = S3Server::start_on_port(&binary, port).await;

    let once_up = store.put_object("a", Bytes::from_static(b"x")).await;

    unavailable_reason(while_down);
    assert_eq!(once_up, Ok(()));
}
