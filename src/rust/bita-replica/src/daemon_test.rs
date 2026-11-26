//! Tests of the replica daemon as a whole, inside the test process.
//!
//! # What is covered
//!
//! - Starting: the daemon serves the replica over gRPC, registers it, and becomes ready after that.
//! - Observing: `/healthz`, `/readyz` and `/metrics` say what the daemon does.
//! - The metadata store being away: the daemon serves, is not ready, and registers when it returns.
//! - Stopping: a shutdown signal ends it cleanly and leaves its registration; a refused flush ends it
//!   with an error.
//! - The pieces around the replica: the data directory's identity, the drain state, the object store.
//!
//! # How the fixtures work
//!
//! `RunningDaemon` runs the real `daemon::run` with in-memory stand-ins for the disk, the metadata store
//! and the object store, and with real sockets on loopback ports that the operating system chose. The
//! tests reach it the way its users do: over gRPC with the real connector and over HTTP. Time is real,
//! because sockets are; the registration interval is 50 milliseconds so that nothing waits long, and
//! every wait is bounded by `GENEROUS_WAIT`.
//!
//! What only a real process shows (exit codes, signals, a real disk, etcd) is covered by the test of a
//! cluster of real processes.

use bitacora_artifact::MemoryArtifactStore;
use bitacora_meta::{MemoryKeyValueStore, ReplicaRecord};
use bitacora_process::{LogSettings, http_get};
use bitacora_proto::{GrpcConnectorSettings, GrpcReplicaConnector};
use bitacora_segment::MemoryDisk;
use bitacora_types::ReplicaId;
use bitacora_types::SequenceNumber;
use bitacora_types::replica_api::{ReplicaError, UploadRequest};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};

use super::*;
use crate::test_support::{SEALED_END, append_of, descriptor, open_replica, small_segments, write_sealed_segment};

/// Far longer than anything here takes; a test that waits this long has found a hang.
const GENEROUS_WAIT: Duration = Duration::from_secs(30);

/// How long a test waits between two looks at something that is about to change.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// How often the daemons under test renew their registration.
const REGISTRATION_INTERVAL: Duration = Duration::from_millis(50);

/// The address the daemons under test register under. Nothing connects to it.
const ADVERTISED_ADDRESS: &str = "replica-a.example.org:7000";

/// What a daemon under test is started on.
struct Stores {
    /// The data directory.
    disk: MemoryDisk,
    /// The backend of the metadata store.
    key_value_store: MemoryKeyValueStore,
    /// The object store, shared with the test.
    object_store: MemoryArtifactStore,
}

impl Stores {
    /// Empty stores, all working.
    fn new() -> Self {
        Self {
            disk: MemoryDisk::new(),
            key_value_store: MemoryKeyValueStore::new(),
            object_store: MemoryArtifactStore::new(),
        }
    }

    /// Working stores whose data directory was used before and has the identity `replica-a`, as on any
    /// start but the first. Such a directory starts without asking the metadata store who it is.
    async fn with_a_used_data_directory() -> Self {
        let stores = Self::new();
        open_replica(&stores.disk, "replica-a").await.stop();
        stores
    }
}

/// A daemon that runs inside the test, with what a test needs to use and stop it.
struct RunningDaemon {
    /// What the daemon was started on.
    stores: Stores,
    /// The replica, reached over gRPC as a client reaches it.
    remote: Arc<dyn ReplicaApi>,
    /// Where the observability endpoint listens, as `host:port`.
    observability_address: String,
    /// Sends the shutdown signal.
    shutdown_sender: oneshot::Sender<ShutdownSignal>,
    /// The task that runs the daemon, and what `run` returns.
    running: JoinHandle<Result<(), DaemonError>>,
}

impl RunningDaemon {
    /// Starts a daemon that proposes the identity `replica-a` on `stores`.
    async fn start(stores: Stores) -> Self {
        let grpc = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port can be bound");
        let observability = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port can be bound");
        let grpc_address = grpc.local_addr().expect("a bound socket has an address");
        let observability_address = observability.local_addr().expect("a bound socket has an address");
        let settings = ReplicaDaemonSettings {
            proposed_replica_id: replica_id("replica-a"),
            data_directory: "/unused/by/these/tests".into(),
            listen_address: grpc_address,
            advertised_address: ADVERTISED_ADDRESS.to_owned(),
            failure_domains: vec!["zone-a".to_owned(), "host-a".to_owned()],
            metrics_listen_address: observability_address,
            registration_interval: REGISTRATION_INTERVAL,
            scrub_interval: Duration::from_hours(1),
            cluster: bitacora::ClusterConfig {
                etcd_endpoints: Vec::new(),
                etcd_key_prefix: bitacora::DEFAULT_ETCD_KEY_PREFIX.to_owned(),
                object_store: bitacora::ObjectStoreConfig::None,
            },
            log: LogSettings::default(),
            replica: small_segments(),
        };
        let environment = Environment {
            disk: Arc::new(stores.disk.clone()),
            key_value_store: Arc::new(stores.key_value_store.clone()),
            object_store: Some(Arc::new(stores.object_store.clone())),
            connector: bitacora::replica_connector(),
            clock: Arc::new(SystemClock),
        };
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<ShutdownSignal>();
        let shutdown = async move {
            match shutdown_receiver.await {
                Ok(signal) => signal,
                // The test dropped the sender without sending: treat it like a terminate.
                Err(_sender_dropped) => ShutdownSignal::Terminate,
            }
        };
        let running = tokio::spawn(run(settings, environment, Listeners { grpc, observability }, shutdown));
        // The identity of the data directory wins over the proposed one, so ask the disk.
        let remote = GrpcReplicaConnector::new(GrpcConnectorSettings::default())
            .connect(&identity_on(&stores.disk).await, &grpc_address.to_string());
        Self {
            stores,
            remote,
            observability_address: observability_address.to_string(),
            shutdown_sender,
            running,
        }
    }

    /// The metadata store the daemon registers in.
    fn metadata(&self) -> MetadataStore {
        MetadataStore::new(Arc::new(self.stores.key_value_store.clone()))
    }

    /// The status of the observability path `path`.
    async fn status_of(&self, path: &str) -> u16 {
        http_get(&self.observability_address, path, GENEROUS_WAIT)
            .await
            .expect("the endpoint answers")
            .status
    }

    /// The daemon's metrics, as Prometheus would scrape them.
    async fn scraped_metrics(&self) -> String {
        http_get(&self.observability_address, "/metrics", GENEROUS_WAIT)
            .await
            .expect("the endpoint answers")
            .body
    }

    /// Waits until `/readyz` answers with `expected_status`.
    async fn wait_for_readiness_status(&self, expected_status: u16) {
        let deadline = Instant::now() + GENEROUS_WAIT;
        while self.status_of("/readyz").await != expected_status {
            assert!(
                Instant::now() < deadline,
                "/readyz did not answer {expected_status} in time"
            );
            sleep(POLL_INTERVAL).await;
        }
    }

    /// Waits until the registry holds a record that `is_expected` accepts, and returns it.
    async fn wait_for_registration(&self, is_expected: fn(&ReplicaRecord) -> bool) -> ReplicaRecord {
        let metadata = self.metadata();
        let deadline = Instant::now() + GENEROUS_WAIT;
        loop {
            let records = metadata.list_replicas().await.unwrap_or_default();
            for record in records {
                if is_expected(&record) {
                    return record;
                }
            }
            assert!(
                Instant::now() < deadline,
                "the expected registration did not appear in time"
            );
            sleep(POLL_INTERVAL).await;
        }
    }

    /// Sends the terminate signal and waits for the daemon to end.
    ///
    /// # Returns
    ///
    /// What `run` returned.
    async fn terminate(self) -> Result<(), DaemonError> {
        // The daemon may have ended by itself already; then nobody listens, which is fine.
        let _daemon_already_ended = self.shutdown_sender.send(ShutdownSignal::Terminate);
        timeout(GENEROUS_WAIT, self.running)
            .await
            .expect("the daemon stops in time")
            .expect("the daemon does not panic")
    }
}

/// A valid replica identifier.
fn replica_id(name: &str) -> ReplicaId {
    ReplicaId::new(name).expect("a valid identifier")
}

/// The identity the data directory on `disk` has or will get: the one stored on it, or `replica-a`,
/// which is what the daemons under test propose.
async fn identity_on(disk: &MemoryDisk) -> ReplicaId {
    use bitacora_segment::Disk as _;
    let stored_identity = disk
        .read_file("replica.id")
        .await
        .expect("an in-memory disk can be read");
    match stored_identity {
        Some(identity) => replica_id(std::str::from_utf8(&identity).expect("an identifier is text")),
        None => replica_id("replica-a"),
    }
}

/// Starts a daemon on `stores` that is expected to end by itself, and returns what `run` returned.
async fn run_until_it_stops(stores: Stores) -> Result<(), DaemonError> {
    let daemon = RunningDaemon::start(stores).await;
    timeout(GENEROUS_WAIT, daemon.running)
        .await
        .expect("the daemon ends by itself")
        .expect("the daemon does not panic")
}

/// Accepts any registration.
fn any_registration(_record: &ReplicaRecord) -> bool {
    true
}

/// Accepts a registration that says the replica is draining.
fn draining_registration(record: &ReplicaRecord) -> bool {
    record.is_draining
}

#[tokio::test]
async fn daemon_serves_the_replica_over_grpc() {
    let daemon = RunningDaemon::start(Stores::new()).await;

    let status = daemon.remote.status().await.expect("the replica answers");

    assert_eq!(status.replica_id, replica_id("replica-a"));
    daemon.terminate().await.expect("a clean stop");
}

#[tokio::test]
async fn daemon_registers_under_its_advertised_address() {
    let daemon = RunningDaemon::start(Stores::new()).await;

    let record = daemon.wait_for_registration(any_registration).await;

    assert_eq!(record.address, ADVERTISED_ADDRESS);
    daemon.terminate().await.expect("a clean stop");
}

#[tokio::test]
async fn daemon_registers_its_failure_domains() {
    let daemon = RunningDaemon::start(Stores::new()).await;

    let record = daemon.wait_for_registration(any_registration).await;

    assert_eq!(record.failure_domains, ["zone-a", "host-a"]);
    daemon.terminate().await.expect("a clean stop");
}

#[tokio::test]
async fn daemon_becomes_ready_once_it_is_registered() {
    let daemon = RunningDaemon::start(Stores::new()).await;

    daemon.wait_for_readiness_status(200).await;

    let registered = daemon.metadata().list_replicas().await.expect("lists");
    assert_eq!(registered.len(), 1);
    daemon.terminate().await.expect("a clean stop");
}

#[tokio::test]
async fn daemon_is_alive_but_not_ready_while_the_metadata_store_is_down() {
    let stores = Stores::with_a_used_data_directory().await;
    stores.key_value_store.set_unavailable(true);
    let daemon = RunningDaemon::start(stores).await;
    // Give the registration loop time for several attempts.
    sleep(REGISTRATION_INTERVAL * 4).await;

    let liveness_status = daemon.status_of("/healthz").await;
    let readiness_status = daemon.status_of("/readyz").await;

    assert_eq!(liveness_status, 200);
    assert_eq!(readiness_status, 503);
    daemon.terminate().await.expect("a clean stop");
}

/// Writers that already know the replica keep using it during a metadata outage.
#[tokio::test]
async fn daemon_serves_requests_while_the_metadata_store_is_down() {
    let stores = Stores::with_a_used_data_directory().await;
    stores.key_value_store.set_unavailable(true);
    let daemon = RunningDaemon::start(stores).await;

    let appended = daemon.remote.append(append_of(1, 3)).await;

    assert!(appended.is_ok(), "{appended:?}");
    daemon.terminate().await.expect("a clean stop");
}

#[tokio::test]
async fn daemon_becomes_ready_when_the_metadata_store_returns() {
    let stores = Stores::with_a_used_data_directory().await;
    stores.key_value_store.set_unavailable(true);
    let daemon = RunningDaemon::start(stores).await;
    daemon.wait_for_readiness_status(503).await;

    daemon.stores.key_value_store.set_unavailable(false);

    daemon.wait_for_readiness_status(200).await;
    daemon.terminate().await.expect("a clean stop");
}

#[tokio::test]
async fn failed_registrations_are_visible_in_the_metrics() {
    let stores = Stores::with_a_used_data_directory().await;
    stores.key_value_store.set_unavailable(true);
    let daemon = RunningDaemon::start(stores).await;
    sleep(REGISTRATION_INTERVAL * 4).await;

    let metrics = daemon.scraped_metrics().await;

    assert!(
        !metrics.contains("bitacora_replica_registration_failures_total 0\n"),
        "{metrics}"
    );
    daemon.terminate().await.expect("a clean stop");
}

#[tokio::test]
async fn requests_the_daemon_served_are_visible_in_the_metrics() {
    let daemon = RunningDaemon::start(Stores::new()).await;
    daemon.remote.status().await.expect("the replica answers");

    let metrics = daemon.scraped_metrics().await;

    assert!(
        metrics.contains("bitacora_replica_requests_total{operation=\"status\"} 1\n"),
        "{metrics}"
    );
    daemon.terminate().await.expect("a clean stop");
}

#[tokio::test]
async fn flushes_of_the_daemon_are_visible_in_the_metrics() {
    let daemon = RunningDaemon::start(Stores::new()).await;
    daemon
        .remote
        .append(append_of(1, 3))
        .await
        .expect("the append is acknowledged");

    let metrics = daemon.scraped_metrics().await;

    assert!(
        !metrics.contains("bitacora_disk_flush_duration_seconds_count 0\n"),
        "{metrics}"
    );
    daemon.terminate().await.expect("a clean stop");
}

#[tokio::test]
async fn shutdown_signal_stops_the_daemon_cleanly() {
    let daemon = RunningDaemon::start(Stores::new()).await;
    daemon.wait_for_readiness_status(200).await;

    let stopped = daemon.terminate().await;

    assert_eq!(stopped, Ok(()));
}

/// A restart for an upgrade must not look like a lost replica: that would start re-replication.
#[tokio::test]
async fn registration_survives_a_clean_shutdown() {
    let daemon = RunningDaemon::start(Stores::new()).await;
    daemon.wait_for_readiness_status(200).await;
    let metadata = daemon.metadata();

    daemon.terminate().await.expect("a clean stop");

    let registered = metadata.list_replicas().await.expect("lists");
    assert_eq!(registered.len(), 1);
}

#[tokio::test]
async fn stopped_daemon_no_longer_answers_requests() {
    let daemon = RunningDaemon::start(Stores::new()).await;
    daemon.wait_for_readiness_status(200).await;
    let remote = Arc::clone(&daemon.remote);

    daemon.terminate().await.expect("a clean stop");

    let status = remote.status().await;
    assert!(matches!(status, Err(ReplicaError::Unavailable { .. })), "{status:?}");
}

/// After a failed flush the durable content of the file is unknown, so the replica must stop.
#[tokio::test]
async fn refused_flush_stops_the_daemon_with_an_error() {
    let daemon = RunningDaemon::start(Stores::new()).await;
    daemon.stores.disk.fail_flushes(true);

    let _refused_append = daemon.remote.append(append_of(1, 3)).await;
    let stopped = timeout(GENEROUS_WAIT, daemon.running)
        .await
        .expect("the daemon stops by itself")
        .expect("the daemon does not panic");

    assert!(matches!(stopped, Err(DaemonError::StorageFailed { .. })), "{stopped:?}");
}

#[tokio::test]
async fn append_whose_flush_was_refused_is_not_acknowledged() {
    let daemon = RunningDaemon::start(Stores::new()).await;
    daemon.stores.disk.fail_flushes(true);

    let refused_append = daemon.remote.append(append_of(1, 3)).await;

    assert!(refused_append.is_err(), "{refused_append:?}");
}

#[tokio::test]
async fn drain_reaches_the_registry() {
    let daemon = RunningDaemon::start(Stores::new()).await;
    daemon.wait_for_readiness_status(200).await;

    daemon.remote.drain().await.expect("the replica drains");

    let record = daemon.wait_for_registration(draining_registration).await;
    assert!(record.is_draining);
    daemon.terminate().await.expect("a clean stop");
}

/// A disk that was replaced comes back empty, with a configuration that still proposes the old name.
#[tokio::test]
async fn daemon_refuses_an_empty_data_directory_under_a_registered_identity() {
    let first_stores = Stores::new();
    let key_value_store = first_stores.key_value_store.clone();
    let first_daemon = RunningDaemon::start(first_stores).await;
    first_daemon.wait_for_registration(any_registration).await;
    first_daemon.terminate().await.expect("a clean stop");
    let stores_with_a_new_disk = Stores {
        disk: MemoryDisk::new(),
        key_value_store,
        object_store: MemoryArtifactStore::new(),
    };

    let outcome = run_until_it_stops(stores_with_a_new_disk).await;

    assert!(
        matches!(outcome, Err(DaemonError::IdentityAlreadyKnown { .. })),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn refused_daemon_writes_nothing_to_the_empty_data_directory() {
    use bitacora_segment::Disk as _;

    let first_stores = Stores::new();
    let key_value_store = first_stores.key_value_store.clone();
    let first_daemon = RunningDaemon::start(first_stores).await;
    first_daemon.wait_for_registration(any_registration).await;
    first_daemon.terminate().await.expect("a clean stop");
    let new_disk = MemoryDisk::new();
    let stores_with_a_new_disk = Stores {
        disk: new_disk.clone(),
        key_value_store,
        object_store: MemoryArtifactStore::new(),
    };

    let _refused = run_until_it_stops(stores_with_a_new_disk).await;

    assert_eq!(new_disk.list_files().await.expect("lists"), Vec::<String>::new());
}

/// Nobody can say whether the identity is new, so the daemon does not start; its supervisor tries again.
#[tokio::test]
async fn daemon_on_an_empty_data_directory_does_not_start_while_the_metadata_store_is_down() {
    let stores = Stores::new();
    stores.key_value_store.set_unavailable(true);

    let outcome = run_until_it_stops(stores).await;

    assert!(matches!(outcome, Err(DaemonError::CannotStart { .. })), "{outcome:?}");
}

/// The identity belongs to the data directory, whatever the configuration proposes.
#[tokio::test]
async fn daemon_registers_the_identity_its_data_directory_already_has() {
    let stores = Stores::new();
    let earlier_replica = open_replica(&stores.disk, "replica-from-before").await;
    earlier_replica.stop();
    drop(earlier_replica);

    let daemon = RunningDaemon::start(stores).await;
    let record = daemon.wait_for_registration(any_registration).await;

    assert_eq!(record.replica_id, replica_id("replica-from-before"));
    daemon.terminate().await.expect("a clean stop");
}

#[tokio::test]
async fn daemon_uploads_a_sealed_segment_to_its_object_store() {
    let daemon = RunningDaemon::start(Stores::new()).await;
    write_sealed_segment(daemon.remote.as_ref()).await;
    let object_key = "streams/7/segments/00000000000000000003";

    let uploaded = daemon
        .remote
        .upload_segment(UploadRequest {
            segment: descriptor(),
            sealed_end: SequenceNumber::new(SEALED_END),
            object_key: object_key.to_owned(),
        })
        .await;

    assert!(uploaded.is_ok(), "{uploaded:?}");
    let stored_object = daemon.stores.object_store.get_object(object_key).await;
    assert!(stored_object.is_ok(), "{stored_object:?}");
    daemon.terminate().await.expect("a clean stop");
}

#[tokio::test]
async fn uploads_of_the_daemon_are_visible_in_the_metrics() {
    let daemon = RunningDaemon::start(Stores::new()).await;
    write_sealed_segment(daemon.remote.as_ref()).await;
    daemon
        .remote
        .upload_segment(UploadRequest {
            segment: descriptor(),
            sealed_end: SequenceNumber::new(SEALED_END),
            object_key: "streams/7/segments/00000000000000000003".to_owned(),
        })
        .await
        .expect("the upload succeeds");

    let metrics = daemon.scraped_metrics().await;

    assert!(
        metrics.contains("bitacora_object_store_requests_total{operation=\"put_object\"} 1\n"),
        "{metrics}"
    );
    daemon.terminate().await.expect("a clean stop");
}

#[tokio::test]
async fn daemon_loads_the_segments_its_data_directory_already_holds() {
    let stores = Stores::new();
    let earlier_replica = open_replica(&stores.disk, "replica-a").await;
    write_sealed_segment(earlier_replica.as_ref()).await;
    earlier_replica.stop();
    drop(earlier_replica);

    let daemon = RunningDaemon::start(stores).await;
    let held_copies = daemon.remote.list_segments().await.expect("lists");

    assert_eq!(held_copies.len(), 1);
    daemon.terminate().await.expect("a clean stop");
}
