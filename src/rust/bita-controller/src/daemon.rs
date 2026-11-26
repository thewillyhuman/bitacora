//! The controller daemon: a controller wired to a cluster and kept running.
//!
//! # Why this file exists
//!
//! `bitacora-controller` is the controller as a library: one call runs one round, which takes or renews
//! the leadership and, if this instance leads, makes one pass over every stream. It has no loop that an
//! operator can observe, connects to nothing by itself and reacts to no signal, which is what lets it
//! run in the simulator. Somebody has to give it the cluster's metadata store, its replicas and its
//! object store, run rounds for as long as the process lives, and say what they did. That is this file.
//!
//! # What it contains
//!
//! - [`Environment`]: the real things a controller is wired to, or their in-memory stand-ins in tests.
//! - [`open_environment`], [`bind_listener`]: build them from the settings.
//! - [`run`]: runs the daemon until it is told to stop.
//! - [`run_rounds`]: the loop itself, without the wiring.
//! - [`DaemonError`]: why the daemon could not start.
//!
//! # How it works
//!
//! [`run`] starts the observability endpoint, wraps the metadata store and the object store in the
//! decorators that record metrics, builds the controller on them, and hands it to [`run_rounds`].
//!
//! [`run_rounds`] repeats: run one round, record what it did, rest for the pass interval. The shutdown
//! signal is noticed while resting and while a round runs. A round that is running when the signal
//! arrives gets a grace period to finish; after that it is abandoned. Abandoning a round is safe: every
//! write of a pass is conditional on what the pass saw, so half a pass is just a shorter pass.
//!
//! The daemon is **ready** once a round has completed, as leader or as standby: it then knows that it
//! can reach the metadata store and where it stands. A round that fails does not make it ready, and
//! does not take readiness away again either: a standby instance that briefly loses etcd is still the
//! instance that should take over.
//!
//! The leadership is not given up on shutdown. It lapses by itself after the leadership duration, and
//! another instance takes over then.
//!
//! # Invariants
//!
//! - Rounds never overlap: the next one starts only after the previous one ended or was abandoned.
//! - After [`run`] returns, nothing of the daemon is still running.
//! - Shutdown takes at most the grace period, however long a pass would take.
//!
//! # See also
//!
//! - `main.rs`: turns the command line into settings and the result of [`run`] into an exit code.
//! - `metrics.rs`: what is recorded about each round.
//! - `bitacora-controller/src/controller/round.rs` and `controller.rs`: what a round and a pass do.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bitacora_artifact::ArtifactStore;
use bitacora_controller::{Controller, Round};
use bitacora_meta::{KeyValueStore, MetaError, MetadataStore};
use bitacora_process::{
    InstrumentedArtifactStore, InstrumentedKeyValueStore, MetricsRegistry, Readiness, ShutdownSignal,
    serve_observability,
};
use bitacora_types::replica_api::ReplicaConnector;
use bitacora_types::{Clock, SystemClock};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::{Instant, sleep, timeout};

use crate::config::ControllerDaemonSettings;
use crate::metrics::ControllerMetrics;

/// How long a round that is running when the shutdown signal arrives may take to finish, and how long
/// the observability endpoint may take to answer its last requests.
pub(crate) const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// Why the daemon could not start.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub(crate) enum DaemonError {
    /// Something the daemon needs could not be opened: a socket, the object store.
    #[error("cannot {action}: {reason}")]
    CannotStart {
        /// What the daemon was trying to do, for example `open the object store`.
        action: String,
        /// What went wrong.
        reason: String,
    },
}

impl DaemonError {
    /// Builds the error for a start-up step that failed.
    fn cannot_start(action: impl Into<String>, reason: &impl ToString) -> Self {
        Self::CannotStart {
            action: action.into(),
            reason: reason.to_string(),
        }
    }
}

/// What a controller daemon is wired to. Production builds it with [`open_environment`]; tests build it
/// from in-memory stand-ins.
pub(crate) struct Environment {
    /// The store the cluster's metadata is kept in.
    pub key_value_store: Arc<dyn KeyValueStore>,
    /// Where sealed segments are tiered to, if the cluster tiers.
    pub object_store: Option<Arc<dyn ArtifactStore>>,
    /// How the controller reaches replicas.
    pub connector: Arc<dyn ReplicaConnector>,
    /// The clock that leadership, liveness and tiering delays are judged by.
    pub clock: Arc<dyn Clock>,
}

/// Opens what the daemon is wired to in production: the cluster's metadata store in etcd, its object
/// store, gRPC to the replicas, and the system clock.
///
/// # Arguments
///
/// * `settings` - The daemon's validated settings.
///
/// # Errors
///
/// Returns [`DaemonError::CannotStart`] if the etcd settings can never work or the object store cannot
/// be opened. An etcd that is merely down is not an error: rounds fail until it is back.
pub(crate) async fn open_environment(settings: &ControllerDaemonSettings) -> Result<Environment, DaemonError> {
    let key_value_store = match bitacora::connect_key_value_store(&settings.cluster).await {
        Ok(key_value_store) => key_value_store,
        Err(connect_error) => {
            return Err(DaemonError::cannot_start("use the metadata store", &connect_error));
        }
    };
    let object_store = match bitacora::open_object_store(&settings.cluster.object_store).await {
        Ok(object_store) => object_store,
        Err(connect_error) => {
            return Err(DaemonError::cannot_start("open the object store", &connect_error));
        }
    };
    Ok(Environment {
        key_value_store,
        object_store,
        connector: bitacora::replica_connector(),
        clock: Arc::new(SystemClock),
    })
}

/// Binds the socket the observability endpoint is served on.
///
/// # Arguments
///
/// * `settings` - The daemon's validated settings, which name the address.
///
/// # Errors
///
/// Returns [`DaemonError::CannotStart`] if the address cannot be bound, for example because another
/// process listens on it.
pub(crate) async fn bind_listener(settings: &ControllerDaemonSettings) -> Result<TcpListener, DaemonError> {
    match TcpListener::bind(settings.metrics_listen_address).await {
        Ok(listener) => Ok(listener),
        Err(io_error) => Err(DaemonError::cannot_start(
            format!("listen for metrics on {}", settings.metrics_listen_address),
            &io_error,
        )),
    }
}

/// Runs the controller daemon until it is told to stop. The file header describes how.
///
/// # Arguments
///
/// * `settings` - The daemon's validated settings. The listen address in them is not used here: the
///   socket comes in `observability_listener`.
/// * `environment` - What the controller is wired to.
/// * `observability_listener` - The bound socket to serve `/metrics`, `/healthz` and `/readyz` on.
/// * `shutdown` - Completes when the daemon is asked to stop.
///
/// # Returns
///
/// Once the daemon was asked to stop and everything it started has stopped.
pub(crate) async fn run(
    settings: ControllerDaemonSettings,
    environment: Environment,
    observability_listener: TcpListener,
    shutdown: impl Future<Output = ShutdownSignal> + Send,
) {
    let metrics_registry = Arc::new(MetricsRegistry::new());
    let readiness = Readiness::new();
    let (stop_endpoint, endpoint_stop_asked) = oneshot::channel::<()>();
    let mut endpoint = tokio::spawn(serve_observability(
        observability_listener,
        Arc::clone(&metrics_registry),
        readiness.clone(),
        async move {
            // Completes when the sender is used or dropped; both mean "stop".
            let _sent_or_dropped = endpoint_stop_asked.await;
        },
    ));

    let metadata = MetadataStore::new(Arc::new(InstrumentedKeyValueStore::new(
        environment.key_value_store,
        &metrics_registry,
    )));
    let identity = settings.controller.identity.clone();
    let pass_interval = settings.controller.pass_interval;
    let mut controller = Controller::new(metadata, environment.connector, environment.clock, settings.controller);
    if let Some(object_store) = environment.object_store {
        controller = controller.with_artifact_store(Arc::new(InstrumentedArtifactStore::new(
            object_store,
            &metrics_registry,
        )));
    }
    let metrics = ControllerMetrics::register(&metrics_registry);
    tracing::info!(%identity, "controller is running");

    let signal = run_rounds(&controller, &metrics, &readiness, pass_interval, shutdown).await;
    tracing::info!(?signal, %identity, "controller is shutting down");

    readiness.set_ready(false);
    drop(stop_endpoint);
    if timeout(SHUTDOWN_GRACE, &mut endpoint).await.is_err() {
        endpoint.abort();
    }
}

/// Runs rounds of `controller`, resting for `pass_interval` between two, until `shutdown` completes.
///
/// # Arguments
///
/// * `controller` - The controller whose rounds are run.
/// * `metrics` - Where the outcome of every round is recorded.
/// * `readiness` - Set to ready after the first round that completes without an error.
/// * `pass_interval` - How long to rest between two rounds.
/// * `shutdown` - Completes when the loop should stop. A round that is running then gets
///   [`SHUTDOWN_GRACE`] to finish and is abandoned after that.
///
/// # Returns
///
/// The signal that stopped the loop.
pub(crate) async fn run_rounds(
    controller: &Controller,
    metrics: &ControllerMetrics,
    readiness: &Readiness,
    pass_interval: Duration,
    shutdown: impl Future<Output = ShutdownSignal> + Send,
) -> ShutdownSignal {
    tokio::pin!(shutdown);
    loop {
        let started_at = Instant::now();
        let round = controller.run_round();
        tokio::pin!(round);
        let outcome = tokio::select! {
            outcome = &mut round => outcome,
            signal = &mut shutdown => {
                match timeout(SHUTDOWN_GRACE, &mut round).await {
                    Ok(outcome) => record_round(metrics, readiness, &outcome, started_at),
                    Err(_grace_is_over) => {
                        tracing::warn!("the running round did not finish in time and is abandoned");
                    }
                }
                return signal;
            }
        };
        record_round(metrics, readiness, &outcome, started_at);

        tokio::select! {
            signal = &mut shutdown => return signal,
            () = sleep(pass_interval) => {}
        }
    }
}

/// Records what one round did: in the metrics, in the log, and in the readiness flag.
fn record_round(
    metrics: &ControllerMetrics,
    readiness: &Readiness,
    outcome: &Result<Round, MetaError>,
    started_at: Instant,
) {
    metrics.record_round(outcome, started_at.elapsed());
    match outcome {
        Ok(Round::Led(report)) => {
            tracing::debug!(?report, "ran a pass");
            readiness.set_ready(true);
        }
        Ok(Round::StoodBy) => {
            tracing::debug!("another controller leads");
            readiness.set_ready(true);
        }
        Err(meta_error) => {
            tracing::warn!(%meta_error, "controller round failed");
        }
    }
}

#[cfg(test)]
#[path = "daemon_test.rs"]
mod tests;
