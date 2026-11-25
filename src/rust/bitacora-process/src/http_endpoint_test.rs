//! Tests of a daemon's HTTP endpoint.
//!
//! # What is covered
//!
//! - `/metrics` answers with the registered metrics in the `OpenMetrics` format.
//! - `/healthz` answers as long as the endpoint runs, ready or not.
//! - `/readyz` follows the readiness flag in both directions.
//! - An unknown path is not found, and the endpoint stops when asked to.
//!
//! # How the fixtures work
//!
//! `RunningEndpoint` serves the real endpoint on a loopback port chosen by the operating system and asks
//! it questions with the crate's own minimal HTTP client, over real sockets.

use std::time::Duration;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::*;
use crate::probe::{HttpAnswer, http_get};

/// Much longer than a request on the loopback interface takes.
const GENEROUS_WAIT: Duration = Duration::from_secs(20);

/// An endpoint that is being served, with what a test needs to use and stop it.
struct RunningEndpoint {
    /// Where the endpoint listens, as `host:port`.
    address: String,
    /// The registry the endpoint exposes.
    metrics: Arc<MetricsRegistry>,
    /// The readiness flag the endpoint reports.
    readiness: Readiness,
    /// Dropping it asks the endpoint to stop.
    stop_sender: oneshot::Sender<()>,
    /// The task that serves the endpoint.
    serving: JoinHandle<io::Result<()>>,
}

impl RunningEndpoint {
    /// Starts an endpoint with an empty registry that is not ready.
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port can be bound");
        let address = listener
            .local_addr()
            .expect("a bound listener has an address")
            .to_string();
        let metrics = Arc::new(MetricsRegistry::new());
        let readiness = Readiness::new();
        let (stop_sender, stop_receiver) = oneshot::channel::<()>();
        let shutdown = async move {
            // Stopping is asked for by sending or by dropping the sender; both complete this future.
            let _stop_was_asked = stop_receiver.await;
        };
        let serving = tokio::spawn(serve_observability(
            listener,
            Arc::clone(&metrics),
            readiness.clone(),
            shutdown,
        ));
        Self {
            address,
            metrics,
            readiness,
            stop_sender,
            serving,
        }
    }

    /// Asks the endpoint for `path`.
    async fn get(&self, path: &str) -> HttpAnswer {
        http_get(&self.address, path, GENEROUS_WAIT)
            .await
            .expect("the endpoint answers")
    }
}

#[tokio::test]
async fn metrics_path_exposes_the_registered_metrics() {
    let endpoint = RunningEndpoint::start().await;
    let rounds = endpoint
        .metrics
        .counter("controller_rounds", "Rounds of the controller's loop");
    rounds.add(3);

    let answer = endpoint.get("/metrics").await;

    assert!(
        answer.body.contains("bitacora_controller_rounds_total 3"),
        "{}",
        answer.body
    );
}

#[tokio::test]
async fn metrics_path_answers_with_success() {
    let endpoint = RunningEndpoint::start().await;

    let answer = endpoint.get("/metrics").await;

    assert_eq!(answer.status, 200);
}

#[tokio::test]
async fn metrics_are_complete_openmetrics_text() {
    let endpoint = RunningEndpoint::start().await;

    let answer = endpoint.get("/metrics").await;

    assert!(answer.body.ends_with("# EOF\n"), "{}", answer.body);
}

#[tokio::test]
async fn liveness_path_answers_while_the_daemon_is_not_ready() {
    let endpoint = RunningEndpoint::start().await;

    let answer = endpoint.get("/healthz").await;

    assert_eq!(answer.status, 200);
}

#[tokio::test]
async fn readiness_path_refuses_until_the_daemon_is_ready() {
    let endpoint = RunningEndpoint::start().await;

    let answer = endpoint.get("/readyz").await;

    assert_eq!(answer.status, 503);
}

#[tokio::test]
async fn readiness_path_answers_once_the_daemon_is_ready() {
    let endpoint = RunningEndpoint::start().await;
    endpoint.readiness.set_ready(true);

    let answer = endpoint.get("/readyz").await;

    assert_eq!(answer.status, 200);
}

#[tokio::test]
async fn readiness_path_refuses_again_while_the_daemon_shuts_down() {
    let endpoint = RunningEndpoint::start().await;
    endpoint.readiness.set_ready(true);
    endpoint.readiness.set_ready(false);

    let answer = endpoint.get("/readyz").await;

    assert_eq!(answer.status, 503);
}

#[tokio::test]
async fn readiness_answer_says_which_it_is() {
    let endpoint = RunningEndpoint::start().await;

    let answer = endpoint.get("/readyz").await;

    assert_eq!(answer.body, "not ready\n");
}

#[tokio::test]
async fn unknown_path_is_not_found() {
    let endpoint = RunningEndpoint::start().await;

    let answer = endpoint.get("/no-such-path").await;

    assert_eq!(answer.status, 404);
}

#[tokio::test]
async fn endpoint_stops_cleanly_when_asked() {
    let endpoint = RunningEndpoint::start().await;

    drop(endpoint.stop_sender);
    let served = tokio::time::timeout(GENEROUS_WAIT, endpoint.serving).await;

    let outcome = served
        .expect("the endpoint stops when asked")
        .expect("the serving task does not panic");
    assert!(outcome.is_ok(), "{outcome:?}");
}

#[test]
fn clones_of_a_readiness_flag_share_it() {
    let readiness = Readiness::new();
    let clone_held_by_the_endpoint = readiness.clone();

    readiness.set_ready(true);

    assert!(clone_held_by_the_endpoint.is_ready());
}
