//! The replica daemon: a replica wired to a disk, a metadata store, the network and an operator.
//!
//! # Why this file exists
//!
//! `bitacora-replica` is one storage node of the log as a library: it takes requests through an
//! interface and stores segments on an injected disk. It listens on no socket, registers nowhere and
//! reacts to no signal, which is what lets it run in the simulator. Somebody has to give it a real disk
//! and a real network and keep it running as a process that an operator can deploy, observe and stop.
//! That is this file.
//!
//! # What it contains
//!
//! - [`Environment`]: the real things a replica is wired to, or their in-memory stand-ins in tests.
//! - [`Listeners`]: the two sockets the daemon serves on.
//! - [`open_environment`], [`bind_listeners`]: build both from the settings.
//! - [`run`]: runs the daemon until it is told to stop or has to.
//! - [`DaemonError`]: why it could not start or had to stop.
//! - `StartedDaemon`: everything [`run`] started, with one method per phase: `start`,
//!   `wait_for_a_reason_to_stop` and `shut_down`.
//!
//! # How it works
//!
//! [`run`] goes through these steps. The first five are `StartedDaemon::start`, the sixth is
//! `StartedDaemon::wait_for_a_reason_to_stop` and the last is `StartedDaemon::shut_down`:
//!
//! 1. Start the observability endpoint, so that `/healthz` answers while the rest starts.
//! 2. Wrap the disk, the metadata store and the object store in the decorators that record metrics, and
//!    refuse an empty data directory under an identity the cluster knows (`identity.rs`).
//! 3. Open the replica on the disk, which loads every segment copy found there.
//! 4. Serve the replica over gRPC, through the decorator that records requests.
//! 5. Start the registration loop and the scrub schedule.
//! 6. Wait for the first of: the shutdown signal, a write the disk refused, or the server stopping.
//! 7. Shut down in order: stop the loops, report not ready, stop the server, stop the replica, stop the
//!    endpoint.
//!
//! The daemon is **ready** once its first registration succeeded: from then on writers and the
//! controller can find it. It reports not ready again as soon as it starts to shut down.
//!
//! A **refused write or flush** stops the daemon with an error (design, section 7: the replica exits).
//! After a failed flush the durable content of the file is unknown, so the copy cannot be trusted and
//! the process must not go on acknowledging appends; a supervisor restarts it, the restart rescans the
//! disk, and the controller repairs what is short.
//!
//! A **clean shutdown** leaves the registration in place. See `registration.rs` for why.
//!
//! Splitting [`run`] from [`open_environment`] and [`bind_listeners`] is what makes the daemon testable
//! as a whole without etcd and without fixed ports: a test passes in-memory stores and sockets bound to
//! ports the operating system chose.
//!
//! # Invariants
//!
//! - The gRPC server answers under the identity of the data directory, which is also the registered
//!   one.
//! - After [`run`] returns, nothing of the daemon is still running: no task, no open socket.
//! - The server is given a bounded time to finish requests in flight; shutdown never hangs on a client.
//!
//! # See also
//!
//! - `main.rs`: turns the command line into settings and the result of [`run`] into an exit code.
//! - `registration.rs`, `scrub.rs`: the two loops.
//! - `bitacora-process`: the endpoint, the decorators and the shutdown signal.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bitacora_artifact::ArtifactStore;
use bitacora_meta::{KeyValueStore, MetadataStore};
use bitacora_process::{
    InstrumentedArtifactStore, InstrumentedDisk, InstrumentedKeyValueStore, InstrumentedReplica, MetricsRegistry,
    Readiness, ShutdownSignal, serve_observability,
};
use bitacora_proto::{ServeError, serve_replica};
use bitacora_replica::{Replica, WriteFailure};
use bitacora_segment::{Disk, LocalDisk};
use bitacora_types::replica_api::{ReplicaApi, ReplicaConnector};
use bitacora_types::{Clock, ReplicaId, SegmentAddress, SystemClock};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio::time::timeout;

use crate::config::ReplicaDaemonSettings;
use crate::identity::refuse_a_known_identity_on_an_empty_directory;
use crate::metrics::ReplicaDaemonMetrics;
use crate::registration::Registration;
use crate::scrub::run_scrub_schedule;

/// How long the servers may take to finish requests in flight once the daemon shuts down. A follower
/// that waits for new records holds its request open for up to the replica's longest read wait, and
/// shutdown must not wait for that.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// Why the daemon could not start or had to stop.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub(crate) enum DaemonError {
    /// Something the daemon needs could not be opened: the data directory, a socket, the object store.
    #[error("cannot {action}: {reason}")]
    CannotStart {
        /// What the daemon was trying to do, for example `open the data directory /var/lib/bitacora`.
        action: String,
        /// What went wrong.
        reason: String,
    },
    /// The disk refused a write or a flush. The copy can no longer be trusted, so the replica stops.
    #[error("the disk refused a write to segment {segment}: {reason}")]
    StorageFailed {
        /// The segment whose write failed.
        segment: SegmentAddress,
        /// What the disk reported.
        reason: String,
    },
    /// The data directory is empty and the cluster already knows the identity the configuration
    /// proposes for it. Starting again cannot help: the configuration must propose a new identity.
    #[error(
        "the data directory is empty, but the cluster already knows a replica {replica_id}: {known_from}. \
         A directory that lost its contents holds none of the copies that replica held and must not answer \
         in its name. Set a new replica_id for this directory; the copies of {replica_id} are rebuilt \
         elsewhere once it is given up or removed with `bita replica remove {replica_id}`"
    )]
    IdentityAlreadyKnown {
        /// The identity the configuration proposes.
        replica_id: ReplicaId,
        /// How the cluster knows it: where it is registered, or which stream names it.
        known_from: String,
    },
    /// The gRPC server stopped by itself, which it only does when its socket breaks.
    #[error("the gRPC server stopped unexpectedly: {reason}")]
    ServerStopped {
        /// What the server reported.
        reason: String,
    },
}

impl DaemonError {
    /// Builds the error for a start-up step that failed.
    ///
    /// # Arguments
    ///
    /// * `action` - What the daemon was trying to do, worded to follow "cannot".
    /// * `reason` - What went wrong.
    ///
    /// # Returns
    ///
    /// [`DaemonError::CannotStart`] with both as text.
    pub(crate) fn cannot_start(action: impl Into<String>, reason: &impl ToString) -> Self {
        Self::CannotStart {
            action: action.into(),
            reason: reason.to_string(),
        }
    }
}

/// What a replica daemon is wired to. Production builds it with [`open_environment`]; tests build it
/// from in-memory stand-ins.
pub(crate) struct Environment {
    /// The data directory.
    pub disk: Arc<dyn Disk>,
    /// The store the cluster's metadata is kept in.
    pub key_value_store: Arc<dyn KeyValueStore>,
    /// Where sealed segments are uploaded to, if the cluster tiers.
    pub object_store: Option<Arc<dyn ArtifactStore>>,
    /// How the replica reaches its peers when it repairs a copy.
    pub connector: Arc<dyn ReplicaConnector>,
    /// Where the time of a registration comes from.
    pub clock: Arc<dyn Clock>,
}

/// The sockets a replica daemon serves on, already bound.
pub(crate) struct Listeners {
    /// Where writers, readers, the controller and other replicas connect.
    pub grpc: TcpListener,
    /// Where `/metrics`, `/healthz` and `/readyz` are served.
    pub observability: TcpListener,
}

/// Why the daemon stopped waiting and began to shut down.
#[derive(Debug)]
enum StopReason {
    /// It was asked to.
    Signal(ShutdownSignal),
    /// The disk refused a write, as the replica reports it.
    WriteFailure(WriteFailure),
    /// The gRPC server ended by itself, with this description of how.
    ServerStopped(String),
}

impl StopReason {
    /// What [`run`] returns for this reason: success if the daemon was asked to stop, and otherwise the
    /// error that names what made it stop.
    fn into_result(self) -> Result<(), DaemonError> {
        match self {
            Self::Signal(_signal) => Ok(()),
            Self::WriteFailure(write_failure) => Err(DaemonError::StorageFailed {
                segment: write_failure.segment,
                reason: write_failure.reason,
            }),
            Self::ServerStopped(reason) => Err(DaemonError::ServerStopped { reason }),
        }
    }
}

/// Opens what the daemon is wired to in production: the data directory on the local file system, the
/// cluster's metadata store in etcd, its object store, gRPC to other replicas, and the system clock.
///
/// # Arguments
///
/// * `settings` - The daemon's validated settings.
///
/// # Errors
///
/// Returns [`DaemonError::CannotStart`] if the data directory cannot be created or opened, the etcd
/// settings can never work, or the object store cannot be opened. An etcd that is merely down is not
/// an error: the daemon starts and registers when etcd is back.
pub(crate) async fn open_environment(settings: &ReplicaDaemonSettings) -> Result<Environment, DaemonError> {
    let data_directory = settings.data_directory.display();
    let disk = match LocalDisk::open(settings.data_directory.clone()).await {
        Ok(disk) => disk,
        Err(io_error) => {
            return Err(DaemonError::cannot_start(
                format!("open the data directory {data_directory}"),
                &io_error,
            ));
        }
    };
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
        disk: Arc::new(disk),
        key_value_store,
        object_store,
        connector: bitacora::replica_connector(),
        clock: Arc::new(SystemClock),
    })
}

/// Binds the two sockets the daemon serves on.
///
/// # Arguments
///
/// * `settings` - The daemon's validated settings, which name both addresses.
///
/// # Errors
///
/// Returns [`DaemonError::CannotStart`] if an address cannot be bound, for example because another
/// process listens on it.
pub(crate) async fn bind_listeners(settings: &ReplicaDaemonSettings) -> Result<Listeners, DaemonError> {
    let grpc = match TcpListener::bind(settings.listen_address).await {
        Ok(listener) => listener,
        Err(io_error) => {
            return Err(DaemonError::cannot_start(
                format!("listen for gRPC on {}", settings.listen_address),
                &io_error,
            ));
        }
    };
    let observability = match TcpListener::bind(settings.metrics_listen_address).await {
        Ok(listener) => listener,
        Err(io_error) => {
            return Err(DaemonError::cannot_start(
                format!("listen for metrics on {}", settings.metrics_listen_address),
                &io_error,
            ));
        }
    };
    Ok(Listeners { grpc, observability })
}

/// A server task and the way to ask it to stop.
struct StoppableTask<Outcome> {
    /// Dropping it asks the task to stop.
    stop: oneshot::Sender<()>,
    /// The task.
    task: JoinHandle<Outcome>,
}

/// A future that completes when `stop_asked`'s sender is used or dropped; both mean "stop".
async fn stop_was_asked(stop_asked: oneshot::Receiver<()>) {
    let _sent_or_dropped = stop_asked.await;
}

/// Runs the replica daemon until it is told to stop or has to. The file header lists the steps.
///
/// # Arguments
///
/// * `settings` - The daemon's validated settings. The two listen addresses in them are not used here:
///   the sockets come in `listeners`.
/// * `environment` - What the replica is wired to.
/// * `listeners` - The bound sockets to serve on.
/// * `shutdown` - Completes when the daemon is asked to stop.
///
/// # Returns
///
/// `Ok(())` after a shutdown that was asked for. Everything the daemon started has stopped by then.
///
/// # Errors
///
/// * [`DaemonError::CannotStart`] if the replica cannot be opened on the disk, or if the data directory
///   is empty and the metadata store cannot be asked whether the proposed identity is new.
/// * [`DaemonError::IdentityAlreadyKnown`] if the data directory is empty and the cluster already knows
///   the proposed identity.
/// * [`DaemonError::StorageFailed`] if the disk refused a write or a flush while serving.
/// * [`DaemonError::ServerStopped`] if the gRPC server ended by itself.
pub(crate) async fn run(
    settings: ReplicaDaemonSettings,
    environment: Environment,
    listeners: Listeners,
    shutdown: impl Future<Output = ShutdownSignal> + Send,
) -> Result<(), DaemonError> {
    let mut daemon = StartedDaemon::start(&settings, environment, listeners).await?;
    tracing::info!(
        replica_id = %daemon.replica.replica_id(),
        advertised_address = %settings.advertised_address,
        "replica is serving"
    );
    let stop_reason = daemon.wait_for_a_reason_to_stop(shutdown).await;
    tracing::info!(?stop_reason, "replica is shutting down");
    daemon.shut_down(&stop_reason).await;
    stop_reason.into_result()
}

/// Everything a running daemon consists of: what [`run`] started and has to stop again.
struct StartedDaemon {
    /// The replica, opened on its data directory.
    replica: Arc<Replica>,
    /// The gRPC server that serves the replica.
    server: StoppableTask<Result<(), ServeError>>,
    /// The observability endpoint.
    endpoint: StoppableTask<std::io::Result<()>>,
    /// The registration loop and the scrub schedule.
    background_loops: JoinSet<()>,
    /// What `/readyz` reports. The registration loop sets it; shutting down clears it.
    readiness: Readiness,
}

impl StartedDaemon {
    /// Starts the daemon: steps 1 to 5 of the file header, in that order.
    ///
    /// # Errors
    ///
    /// * [`DaemonError::IdentityAlreadyKnown`] or [`DaemonError::CannotStart`] if the identity check
    ///   refuses the data directory. The observability endpoint is stopped before this returns.
    /// * [`DaemonError::CannotStart`] if the replica cannot be opened on the disk. The endpoint is then
    ///   asked to stop but not waited for.
    async fn start(
        settings: &ReplicaDaemonSettings,
        environment: Environment,
        listeners: Listeners,
    ) -> Result<Self, DaemonError> {
        let metrics_registry = Arc::new(MetricsRegistry::new());
        let readiness = Readiness::new();
        let endpoint = start_endpoint(listeners.observability, &metrics_registry, &readiness);
        let key_value_store = InstrumentedKeyValueStore::new(environment.key_value_store, &metrics_registry);
        let metadata = MetadataStore::new(Arc::new(key_value_store));
        // Before anything is written to the directory or answered on the network.
        let disk = environment.disk.as_ref();
        let checked =
            refuse_a_known_identity_on_an_empty_directory(disk, &settings.proposed_replica_id, &metadata).await;
        if let Err(daemon_error) = checked {
            stop_within_the_grace_period(endpoint, "observability endpoint").await;
            return Err(daemon_error);
        }
        let replica_parts = ReplicaParts {
            disk: environment.disk,
            object_store: environment.object_store,
            connector: environment.connector,
        };
        let replica = open_replica(settings, replica_parts, &metrics_registry).await?;
        let server = start_server(&replica, listeners.grpc, &metrics_registry);
        let registration = registration_of(settings, &replica, metadata, environment.clock, &metrics_registry);
        let background_loops = start_background_loops(registration, &replica, settings, &readiness);
        Ok(Self {
            replica,
            server,
            endpoint,
            background_loops,
            readiness,
        })
    }

    /// Waits for the first reason to stop: step 6 of the file header.
    ///
    /// # Returns
    ///
    /// The shutdown signal, the write the disk refused, or how the gRPC server ended by itself.
    async fn wait_for_a_reason_to_stop(&mut self, shutdown: impl Future<Output = ShutdownSignal> + Send) -> StopReason {
        tokio::pin!(shutdown);
        tokio::select! {
            signal = &mut shutdown => StopReason::Signal(signal),
            write_failure = self.replica.write_failed() => StopReason::WriteFailure(write_failure),
            server_outcome = &mut self.server.task => StopReason::ServerStopped(describe_server_end(server_outcome)),
        }
    }

    /// Stops everything that was started: step 7 of the file header.
    ///
    /// The loops go first, so that nothing marks the daemon ready again or registers a replica that is
    /// about to stop answering. A gRPC server that ended by itself is not asked to stop again.
    async fn shut_down(mut self, stop_reason: &StopReason) {
        self.background_loops.shutdown().await;
        self.readiness.set_ready(false);
        if !matches!(stop_reason, StopReason::ServerStopped(_)) {
            stop_within_the_grace_period(self.server, "gRPC server").await;
        }
        self.replica.stop();
        stop_within_the_grace_period(self.endpoint, "observability endpoint").await;
    }
}

/// Says how a gRPC server that was not asked to stop came to an end.
fn describe_server_end(server_outcome: Result<Result<(), ServeError>, JoinError>) -> String {
    match server_outcome {
        Ok(Ok(())) => "it returned without being asked to".to_owned(),
        Ok(Err(serve_error)) => serve_error.to_string(),
        Err(join_error) => join_error.to_string(),
    }
}

/// Builds what the registration loop works with, and registers the daemon's own metrics.
fn registration_of(
    settings: &ReplicaDaemonSettings,
    replica: &Arc<Replica>,
    metadata: MetadataStore,
    clock: Arc<dyn Clock>,
    metrics_registry: &MetricsRegistry,
) -> Registration {
    Registration {
        metadata,
        replica: Arc::clone(replica) as Arc<dyn ReplicaApi>,
        clock,
        advertised_address: settings.advertised_address.clone(),
        failure_domains: settings.failure_domains.clone(),
        metrics: ReplicaDaemonMetrics::register(metrics_registry),
    }
}

/// Starts the two loops that run next to the server: the registration, which also makes the daemon
/// ready, and the scrub schedule. They share the daemon's metrics.
fn start_background_loops(
    registration: Registration,
    replica: &Arc<Replica>,
    settings: &ReplicaDaemonSettings,
    readiness: &Readiness,
) -> JoinSet<()> {
    let mut background_loops = JoinSet::new();
    let scrub_metrics = registration.metrics.clone();
    let readiness_of_the_registration = readiness.clone();
    let registration_interval = settings.registration_interval;
    background_loops.spawn(async move {
        registration
            .run(registration_interval, &readiness_of_the_registration)
            .await;
    });
    background_loops.spawn(run_scrub_schedule(
        Arc::clone(replica),
        settings.scrub_interval,
        scrub_metrics,
    ));
    background_loops
}

/// Starts the observability endpoint, so that `/healthz` answers while the rest of the daemon starts.
fn start_endpoint(
    listener: TcpListener,
    metrics_registry: &Arc<MetricsRegistry>,
    readiness: &Readiness,
) -> StoppableTask<std::io::Result<()>> {
    let (stop, stop_asked) = oneshot::channel::<()>();
    let task = tokio::spawn(serve_observability(
        listener,
        Arc::clone(metrics_registry),
        readiness.clone(),
        stop_was_asked(stop_asked),
    ));
    StoppableTask { stop, task }
}

/// The parts of the [`Environment`] that the replica itself is built on.
struct ReplicaParts {
    /// The data directory.
    disk: Arc<dyn Disk>,
    /// Where sealed segments are uploaded to, if the cluster tiers.
    object_store: Option<Arc<dyn ArtifactStore>>,
    /// How the replica reaches its peers when it repairs a copy.
    connector: Arc<dyn ReplicaConnector>,
}

/// Opens the replica on its instrumented disk and gives it its instrumented object store.
///
/// # Arguments
///
/// * `settings` - The daemon's validated settings.
/// * `parts` - What the replica is built on.
/// * `metrics_registry` - The registry of the process, for the decorators.
///
/// # Errors
///
/// Returns [`DaemonError::CannotStart`] if the data directory cannot be listed or its identity cannot be
/// read or stored.
async fn open_replica(
    settings: &ReplicaDaemonSettings,
    parts: ReplicaParts,
    metrics_registry: &MetricsRegistry,
) -> Result<Arc<Replica>, DaemonError> {
    let disk: Arc<dyn Disk> = Arc::new(InstrumentedDisk::new(parts.disk, metrics_registry));
    let opened = Replica::open(
        disk,
        settings.proposed_replica_id.clone(),
        settings.replica,
        parts.connector,
    )
    .await;
    let mut replica = match opened {
        Ok(replica) => replica,
        Err(replica_error) => {
            return Err(DaemonError::cannot_start(
                "open the replica on its data directory",
                &replica_error,
            ));
        }
    };
    if let Some(object_store) = parts.object_store {
        replica = replica.with_artifact_store(Arc::new(InstrumentedArtifactStore::new(object_store, metrics_registry)));
    }
    if replica.replica_id() != &settings.proposed_replica_id {
        tracing::info!(
            replica_id = %replica.replica_id(),
            proposed_replica_id = %settings.proposed_replica_id,
            "the data directory keeps the identity it was first opened with"
        );
    }
    Ok(Arc::new(replica))
}

/// Serves `replica` over gRPC on `listener`, through the decorator that records requests, under the
/// identity of its data directory.
fn start_server(
    replica: &Arc<Replica>,
    listener: TcpListener,
    metrics_registry: &MetricsRegistry,
) -> StoppableTask<Result<(), ServeError>> {
    let served_replica: Arc<dyn ReplicaApi> = Arc::new(InstrumentedReplica::new(
        Arc::clone(replica) as Arc<dyn ReplicaApi>,
        metrics_registry,
    ));
    let (stop, stop_asked) = oneshot::channel::<()>();
    let task = tokio::spawn(serve_replica(
        served_replica,
        replica.replica_id().clone(),
        listener,
        stop_was_asked(stop_asked),
    ));
    StoppableTask { stop, task }
}

/// Asks a server task to stop and waits for it, but no longer than [`SHUTDOWN_GRACE`]: after that the
/// task is cut off, together with the requests it was still serving.
async fn stop_within_the_grace_period<Outcome>(server: StoppableTask<Outcome>, name: &str) {
    let StoppableTask { stop, mut task } = server;
    drop(stop);
    if timeout(SHUTDOWN_GRACE, &mut task).await.is_err() {
        tracing::warn!(
            server = name,
            "requests were still in flight after the grace period; closing them"
        );
        task.abort();
    }
}

#[cfg(test)]
#[path = "daemon_test.rs"]
mod tests;
