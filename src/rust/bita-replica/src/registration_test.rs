//! Tests of the replica's registration and its renewal.
//!
//! # What is covered
//!
//! - What one renewal writes: the identity of the data directory, the advertised address, the failure
//!   domains, the disk's space, the drain state and the time of the clock.
//! - What one renewal records in the daemon's metrics, when it succeeds and when it fails.
//! - The loop: it renews at once and after every interval, keeps going after a failure, and marks the
//!   daemon ready after its first success only.
//!
//! # How the fixtures work
//!
//! A real replica on an in-memory disk is registered in an in-memory metadata store. The heartbeat's
//! time comes from a clock the test moves by hand; the loop's waiting uses Tokio's paused time, so a
//! test that lets ten seconds pass takes no time.

use bitacora_meta::MemoryKeyValueStore;
use bitacora_process::MetricsRegistry;
use bitacora_segment::{Disk, MemoryDisk};
use bitacora_types::{ManualClock, ReplicaId, Timestamp};

use super::*;
use crate::test_support::{open_replica, write_sealed_segment};

/// The moment the manual clock starts at.
const START: Timestamp = Timestamp::from_unix_millis(1_000_000);

/// How long the loop under test waits between renewals.
const INTERVAL: Duration = Duration::from_secs(5);

/// A registration of a real replica, with handles on everything it touches.
struct Fixture {
    /// The registration under test.
    registration: Arc<Registration>,
    /// The backend of the metadata store, to simulate outages.
    key_value_store: MemoryKeyValueStore,
    /// The metadata store, to read what was registered.
    metadata: MetadataStore,
    /// The disk of the replica, to compare the registered space with.
    disk: MemoryDisk,
    /// The clock the heartbeat is read from.
    clock: Arc<ManualClock>,
    /// The replica that is registered.
    replica: Arc<bitacora_replica::Replica>,
    /// The registry the daemon's metrics are in.
    registry: MetricsRegistry,
}

impl Fixture {
    /// Registers a replica called `replica-a` at `replica-a.example.org:7000` in zone `zone-a`.
    async fn new() -> Self {
        let key_value_store = MemoryKeyValueStore::new();
        let metadata = MetadataStore::new(Arc::new(key_value_store.clone()));
        let disk = MemoryDisk::new();
        let replica = open_replica(&disk, "replica-a").await;
        let clock = Arc::new(ManualClock::starting_at(START));
        let registry = MetricsRegistry::new();
        let registration = Arc::new(Registration {
            metadata: metadata.clone(),
            replica: Arc::clone(&replica) as Arc<dyn ReplicaApi>,
            clock: Arc::clone(&clock) as Arc<dyn Clock>,
            advertised_address: "replica-a.example.org:7000".to_owned(),
            failure_domains: vec!["zone-a".to_owned(), "host-a".to_owned()],
            metrics: ReplicaDaemonMetrics::register(&registry),
        });
        Self {
            registration,
            key_value_store,
            metadata,
            disk,
            clock,
            replica,
            registry,
        }
    }

    /// The only record of the registry, as the controller would read it.
    async fn registered(&self) -> ReplicaRecord {
        let mut records = self
            .metadata
            .list_replicas()
            .await
            .expect("the metadata store is up");
        assert_eq!(records.len(), 1, "{records:?}");
        records.remove(0)
    }

    /// Starts the renewal loop in the background.
    ///
    /// # Returns
    ///
    /// The readiness flag the loop sets, and the task, which runs until it is aborted.
    fn start_loop(&self) -> (Readiness, tokio::task::JoinHandle<()>) {
        let readiness = Readiness::new();
        let registration = Arc::clone(&self.registration);
        let readiness_of_the_loop = readiness.clone();
        let running =
            tokio::spawn(async move { registration.run(INTERVAL, &readiness_of_the_loop).await });
        (readiness, running)
    }

    /// The value of the exposition line that starts with `series`, if there is such a line.
    fn metric(&self, series: &str) -> Option<String> {
        for line in self.registry.encode().lines() {
            if let Some(value) = line.strip_prefix(series) {
                return Some(value.trim().to_owned());
            }
        }
        None
    }
}

/// Lets the loop run for `duration` of paused time.
async fn let_pass(duration: Duration) {
    tokio::time::sleep(duration).await;
}

#[tokio::test(start_paused = true)]
async fn renewal_registers_the_identity_of_the_data_directory() {
    let fixture = Fixture::new().await;

    fixture.registration.renew().await.expect("renewed");

    assert_eq!(
        fixture.registered().await.replica_id,
        ReplicaId::new("replica-a").expect("a valid identifier")
    );
}

#[tokio::test(start_paused = true)]
async fn renewal_registers_the_advertised_address() {
    let fixture = Fixture::new().await;

    fixture.registration.renew().await.expect("renewed");

    assert_eq!(
        fixture.registered().await.address,
        "replica-a.example.org:7000"
    );
}

#[tokio::test(start_paused = true)]
async fn renewal_registers_the_failure_domains_in_order() {
    let fixture = Fixture::new().await;

    fixture.registration.renew().await.expect("renewed");

    assert_eq!(
        fixture.registered().await.failure_domains,
        ["zone-a", "host-a"]
    );
}

#[tokio::test(start_paused = true)]
async fn renewal_registers_the_capacity_of_the_disk() {
    let fixture = Fixture::new().await;
    let space = fixture.disk.space().await.expect("measures");

    fixture.registration.renew().await.expect("renewed");

    assert_eq!(
        fixture.registered().await.capacity_bytes,
        space.capacity_bytes
    );
}

#[tokio::test(start_paused = true)]
async fn renewal_registers_the_free_space_left_after_writes() {
    let fixture = Fixture::new().await;
    write_sealed_segment(fixture.replica.as_ref()).await;
    let space = fixture.disk.space().await.expect("measures");

    fixture.registration.renew().await.expect("renewed");

    assert_eq!(fixture.registered().await.free_bytes, space.free_bytes);
}

#[tokio::test(start_paused = true)]
async fn renewal_stamps_the_registration_with_the_time_of_the_clock() {
    let fixture = Fixture::new().await;
    fixture.clock.advance(Duration::from_secs(90));

    fixture.registration.renew().await.expect("renewed");

    assert_eq!(
        fixture.registered().await.heartbeat_at,
        START.plus(Duration::from_secs(90))
    );
}

#[tokio::test(start_paused = true)]
async fn replica_that_is_not_draining_is_registered_as_such() {
    let fixture = Fixture::new().await;

    fixture.registration.renew().await.expect("renewed");

    assert!(!fixture.registered().await.is_draining);
}

#[tokio::test(start_paused = true)]
async fn drain_reaches_the_registry_with_the_next_renewal() {
    let fixture = Fixture::new().await;
    fixture.registration.renew().await.expect("renewed");
    fixture.replica.drain().await.expect("the replica drains");

    fixture.registration.renew().await.expect("renewed");

    assert!(fixture.registered().await.is_draining);
}

#[tokio::test(start_paused = true)]
async fn renewal_returns_the_record_it_wrote() {
    let fixture = Fixture::new().await;

    let returned = fixture.registration.renew().await.expect("renewed");

    assert_eq!(returned, fixture.registered().await);
}

#[tokio::test(start_paused = true)]
async fn renewal_fails_while_the_metadata_store_is_down() {
    let fixture = Fixture::new().await;
    fixture.key_value_store.set_unavailable(true);

    let renewed = fixture.registration.renew().await;

    assert!(
        matches!(renewed, Err(RegistrationError::Metadata(_))),
        "{renewed:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn renewal_fails_once_the_replica_has_stopped() {
    let fixture = Fixture::new().await;
    fixture.replica.stop();

    let renewed = fixture.registration.renew().await;

    assert!(
        matches!(renewed, Err(RegistrationError::Replica(_))),
        "{renewed:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn successful_renewal_is_counted() {
    let fixture = Fixture::new().await;

    fixture.registration.renew().await.expect("renewed");

    assert_eq!(
        fixture.metric("bitacora_replica_registrations_total "),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn failed_renewal_is_counted_as_a_failure() {
    let fixture = Fixture::new().await;
    fixture.key_value_store.set_unavailable(true);

    let _renewed = fixture.registration.renew().await;

    assert_eq!(
        fixture.metric("bitacora_replica_registration_failures_total "),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn failed_renewal_is_not_counted_as_a_registration() {
    let fixture = Fixture::new().await;
    fixture.key_value_store.set_unavailable(true);

    let _renewed = fixture.registration.renew().await;

    assert_eq!(
        fixture.metric("bitacora_replica_registrations_total "),
        Some("0".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn renewal_updates_the_gauge_of_held_copies() {
    let fixture = Fixture::new().await;
    write_sealed_segment(fixture.replica.as_ref()).await;

    fixture.registration.renew().await.expect("renewed");

    assert_eq!(
        fixture.metric("bitacora_replica_segment_copies "),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn renewal_updates_the_gauges_even_while_the_metadata_store_is_down() {
    let fixture = Fixture::new().await;
    write_sealed_segment(fixture.replica.as_ref()).await;
    fixture.key_value_store.set_unavailable(true);

    let _renewed = fixture.registration.renew().await;

    assert_eq!(
        fixture.metric("bitacora_replica_segment_copies "),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn renewal_updates_the_gauge_of_the_drain_state() {
    let fixture = Fixture::new().await;
    fixture.replica.drain().await.expect("the replica drains");

    fixture.registration.renew().await.expect("renewed");

    assert_eq!(
        fixture.metric("bitacora_replica_draining "),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn loop_registers_the_replica_at_once() {
    let fixture = Fixture::new().await;

    let (_readiness, running) = fixture.start_loop();
    let_pass(Duration::from_millis(1)).await;
    running.abort();

    assert_eq!(fixture.registered().await.heartbeat_at, START);
}

#[tokio::test(start_paused = true)]
async fn loop_renews_after_every_interval() {
    let fixture = Fixture::new().await;

    let (_readiness, running) = fixture.start_loop();
    let_pass(INTERVAL * 3 + Duration::from_millis(1)).await;
    running.abort();

    assert_eq!(
        fixture.metric("bitacora_replica_registrations_total "),
        Some("4".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn loop_marks_the_daemon_ready_after_its_first_renewal() {
    let fixture = Fixture::new().await;

    let (readiness, running) = fixture.start_loop();
    let_pass(Duration::from_millis(1)).await;
    running.abort();

    assert!(readiness.is_ready());
}

#[tokio::test(start_paused = true)]
async fn loop_does_not_mark_the_daemon_ready_while_the_metadata_store_is_down() {
    let fixture = Fixture::new().await;
    fixture.key_value_store.set_unavailable(true);

    let (readiness, running) = fixture.start_loop();
    let_pass(INTERVAL * 3).await;
    running.abort();

    assert!(!readiness.is_ready());
}

#[tokio::test(start_paused = true)]
async fn loop_keeps_trying_and_registers_once_the_metadata_store_is_back() {
    let fixture = Fixture::new().await;
    fixture.key_value_store.set_unavailable(true);
    let (readiness, running) = fixture.start_loop();
    let_pass(INTERVAL * 2).await;

    fixture.key_value_store.set_unavailable(false);
    let_pass(INTERVAL * 2).await;
    running.abort();

    assert!(readiness.is_ready());
}

/// A replica that loses the metadata store later still serves what it has, so it stays ready.
#[tokio::test(start_paused = true)]
async fn loop_leaves_the_daemon_ready_when_a_later_renewal_fails() {
    let fixture = Fixture::new().await;
    let (readiness, running) = fixture.start_loop();
    let_pass(Duration::from_millis(1)).await;

    fixture.key_value_store.set_unavailable(true);
    let_pass(INTERVAL * 2).await;
    running.abort();

    assert!(readiness.is_ready());
}
