//! The HTTP endpoint of a daemon: metrics, liveness and readiness.
//!
//! # Why this file exists
//!
//! Prometheus scrapes metrics over HTTP, and Kubernetes and load balancers ask over HTTP whether a
//! process is alive and whether it should receive work. Every bitacora daemon answers the same three
//! questions at the same three paths, so that one scrape configuration and one probe definition fit all
//! of them. This file is that endpoint.
//!
//! # What it contains
//!
//! - [`serve_observability`]: serves the three paths on a listening socket until told to stop.
//! - [`Readiness`]: the flag behind `/readyz`, which the daemon sets and clears.
//!
//! # How it works
//!
//! | Path | Answer |
//! | --- | --- |
//! | `/metrics` | 200 with the metrics of the process in the `OpenMetrics` text format |
//! | `/healthz` | 200 as long as the process runs and its runtime schedules tasks |
//! | `/readyz` | 200 while the daemon is ready, 503 otherwise |
//!
//! Liveness and readiness are different questions. A process that is alive but not ready must not be
//! restarted, only left out: a replica that is still loading its segments, or one that is shutting down.
//! What "ready" means is each daemon's decision; it tells this endpoint through its [`Readiness`].
//!
//! The endpoint listens on its own port, apart from the daemon's gRPC port, so that scraping and probing
//! need no knowledge of the protocol and can be firewalled separately.
//!
//! # Invariants
//!
//! - The endpoint only reads: no request changes the daemon.
//! - A new [`Readiness`] is not ready. A daemon is never reported ready by default.
//!
//! # See also
//!
//! - `metrics.rs`: what `/metrics` exposes.
//! - `probe.rs`: a minimal client for these paths.

use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use tokio::net::TcpListener;

use crate::metrics::MetricsRegistry;

/// The media type of the `OpenMetrics` text format, which is what the registry writes.
const OPENMETRICS_CONTENT_TYPE: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

/// Whether a daemon is ready for work. Clones share the same flag.
///
/// # Examples
///
/// ```
/// use bitacora_process::Readiness;
///
/// let readiness = Readiness::new();
/// assert!(!readiness.is_ready());
///
/// readiness.set_ready(true);
/// assert!(readiness.is_ready());
/// ```
#[derive(Clone, Debug, Default)]
pub struct Readiness {
    /// The flag. Shared between the daemon, which writes it, and the endpoint, which reads it.
    is_ready: Arc<AtomicBool>,
}

impl Readiness {
    /// Creates a flag that says "not ready".
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Says whether the daemon is ready from now on.
    ///
    /// # Arguments
    ///
    /// * `is_ready` - `true` once the daemon can do its work, `false` when it starts to shut down.
    pub fn set_ready(&self, is_ready: bool) {
        self.is_ready.store(is_ready, Ordering::Relaxed);
    }

    /// Whether the daemon is ready right now.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.is_ready.load(Ordering::Relaxed)
    }
}

/// What the request handlers need.
#[derive(Clone)]
struct EndpointState {
    /// The metrics `/metrics` exposes.
    metrics: Arc<MetricsRegistry>,
    /// The flag `/readyz` reports.
    readiness: Readiness,
}

/// Serves `/metrics`, `/healthz` and `/readyz` on `listener` until `shutdown` completes.
///
/// # Arguments
///
/// * `listener` - A bound socket to accept connections on. The caller binds it, so the caller chooses
///   the address and learns the port.
/// * `metrics` - The registry of the process.
/// * `readiness` - The flag the daemon sets once it is ready.
/// * `shutdown` - Completes when the endpoint should stop. Requests in flight are answered first.
///
/// # Errors
///
/// Fails if the listening socket breaks while serving.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// use bitacora_process::{MetricsRegistry, Readiness, http_get, serve_observability};
/// use tokio::net::TcpListener;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let listener = TcpListener::bind("127.0.0.1:0").await?;
/// let address = listener.local_addr()?.to_string();
/// let readiness = Readiness::new();
/// let (stop_sender, stop_receiver) = tokio::sync::oneshot::channel::<()>();
/// let endpoint = tokio::spawn(serve_observability(
///     listener,
///     Arc::new(MetricsRegistry::new()),
///     readiness.clone(),
///     async move {
///         let _stop_was_asked = stop_receiver.await;
///     },
/// ));
///
/// readiness.set_ready(true);
/// let answer = http_get(&address, "/readyz", Duration::from_secs(5)).await?;
/// assert_eq!(answer.status, 200);
///
/// drop(stop_sender);
/// endpoint.await??;
/// # Ok(())
/// # }
/// ```
pub async fn serve_observability(
    listener: TcpListener,
    metrics: Arc<MetricsRegistry>,
    readiness: Readiness,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> io::Result<()> {
    let state = EndpointState { metrics, readiness };
    let router = Router::new()
        .route("/metrics", get(answer_metrics))
        .route("/healthz", get(answer_liveness))
        .route("/readyz", get(answer_readiness))
        .with_state(state);
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
}

/// Answers `/metrics` with the exposition of every registered metric.
async fn answer_metrics(State(state): State<EndpointState>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, OPENMETRICS_CONTENT_TYPE)],
        state.metrics.encode(),
    )
}

/// Answers `/healthz`: being able to answer at all is the proof of life.
async fn answer_liveness() -> impl IntoResponse {
    (StatusCode::OK, "alive\n")
}

/// Answers `/readyz` from the daemon's readiness flag.
async fn answer_readiness(State(state): State<EndpointState>) -> impl IntoResponse {
    if state.readiness.is_ready() {
        (StatusCode::OK, "ready\n")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n")
    }
}

#[cfg(test)]
#[path = "http_endpoint_test.rs"]
mod tests;
