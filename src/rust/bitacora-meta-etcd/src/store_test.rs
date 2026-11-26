//! Tests of the etcd store that need no etcd.
//!
//! # What is covered
//!
//! - Connecting: it contacts nobody, and it refuses settings and endpoints that cannot work.
//! - Keys: the store's prefix goes in front of every key.
//! - Revisions: which revisions of the interface etcd can have given to a key.
//! - The bound on every request: an answer is passed on, an error of the client and a call that never answers both
//!   come back as `StoreUnavailable`.
//! - Trying a read again: a read that fails is made once more, after a pause, and no more often than that.
//!
//! What a request does on a real etcd is tested in `tests/etcd/`.
//!
//! # How the fixtures work
//!
//! `store_with` prepares a store for an endpoint on which nobody listens. That is enough here: connecting sends
//! nothing, and no test of this file sends a request. The tests of the request bound hand the store futures of their
//! own instead of calls to etcd. The tests of trying again hand `read_again_if_unavailable` a `ScriptedRead`, which
//! answers from a list and counts how often it was asked; they run on a paused clock, so the pause costs no time.

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

/// A read that answers from a list, one entry per attempt, and counts the attempts.
struct ScriptedRead {
    /// What the attempts still to come answer, the next one last.
    answers: Vec<Result<&'static str, StoreUnavailable>>,
    /// How many attempts were made.
    attempts: u32,
}

impl ScriptedRead {
    /// A read whose attempts answer `answers`, in that order.
    fn answering(mut answers: Vec<Result<&'static str, StoreUnavailable>>) -> Self {
        answers.reverse();
        Self { answers, attempts: 0 }
    }

    /// Makes one attempt.
    fn attempt(&mut self) -> future::Ready<Result<&'static str, StoreUnavailable>> {
        self.attempts += 1;
        future::ready(self.answers.pop().expect("the script has an answer for every attempt"))
    }
}

/// The failure of an attempt, with a reason that tells the attempts apart.
fn failed(reason: &str) -> Result<&'static str, StoreUnavailable> {
    Err(unavailable(reason.to_owned()))
}

#[tokio::test(start_paused = true)]
async fn read_that_succeeds_is_made_once() {
    let mut read = ScriptedRead::answering(vec![Ok("the answer")]);

    let outcome = read_again_if_unavailable(|| read.attempt()).await;

    assert_eq!(outcome, Ok("the answer"));
    assert_eq!(read.attempts, 1);
}

/// What a program that has just started runs into: its first request finds no connection yet.
#[tokio::test(start_paused = true)]
async fn read_that_fails_once_is_made_again_and_returns_the_second_answer() {
    let mut read = ScriptedRead::answering(vec![failed("no connection yet"), Ok("the answer")]);

    let outcome = read_again_if_unavailable(|| read.attempt()).await;

    assert_eq!(outcome, Ok("the answer"));
}

#[tokio::test(start_paused = true)]
async fn read_that_keeps_failing_is_given_up_after_the_attempts_allowed() {
    let mut read = ScriptedRead::answering(vec![failed("first"), failed("second"), Ok("never asked for")]);

    let _outcome = read_again_if_unavailable(|| read.attempt()).await;

    assert_eq!(read.attempts, READ_ATTEMPTS);
}

#[tokio::test(start_paused = true)]
async fn read_that_keeps_failing_reports_its_last_failure() {
    let mut read = ScriptedRead::answering(vec![failed("first"), failed("second")]);

    let outcome = read_again_if_unavailable(|| read.attempt()).await;

    assert_eq!(outcome, failed("second"));
}

/// A store that refuses at once must not be asked again in a tight loop.
#[tokio::test(start_paused = true)]
async fn failed_read_waits_before_it_is_made_again() {
    let mut read = ScriptedRead::answering(vec![failed("refused at once"), Ok("the answer")]);
    let started_at = tokio::time::Instant::now();

    let _outcome = read_again_if_unavailable(|| read.attempt()).await;

    assert_eq!(started_at.elapsed(), READ_RETRY_PAUSE);
}
