//! Tests of the rule that says whether a cluster is whole again.
//!
//! # What is covered
//!
//! - The rule as a pure function: a cluster with every expected replica alive and a controller leading is healthy;
//!   a missing replica, a silent one, no leader and an expired leadership are each a reason.
//! - The rule against the in-process cluster: healthy as started, not healthy while a replica is down or the
//!   metadata store does not answer.
//! - The wait: it returns as soon as the cluster is healthy and gives the reason when the time runs out.
//!
//! # How the fixtures work
//!
//! The pure tests build registrations and a leadership by hand around [`NOW`]. The others start the in-process
//! cluster of `test_support.rs` and break it through the test cluster's own handles.

use bitacora_meta::RECORD_SCHEMA_VERSION;

use super::*;
use crate::test_support::InProcessCluster;

/// The time of the check in the pure tests.
const NOW: Timestamp = Timestamp::from_unix_millis(1_000_000);

/// The replica identifier `name`.
fn replica_id(name: &str) -> ReplicaId {
    ReplicaId::new(name).expect("a valid identifier")
}

/// The registration of the replica `name`, last renewed `silent_for` before [`NOW`].
fn registration(name: &str, silent_for: Duration) -> ReplicaRecord {
    let silent_millis = u64::try_from(silent_for.as_millis()).expect("a short time");
    ReplicaRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        replica_id: replica_id(name),
        address: format!("{name}:7000"),
        failure_domains: vec![format!("host-{name}")],
        capacity_bytes: 1_000,
        free_bytes: 900,
        is_draining: false,
        heartbeat_at: Timestamp::from_unix_millis(NOW.unix_millis() - silent_millis),
    }
}

/// A leadership of `controller-0` that lasts five more seconds.
fn leadership_in_force() -> LeaderRecord {
    LeaderRecord {
        holder: "controller-0".to_owned(),
        expires_at: NOW.plus(Duration::from_secs(5)),
        leading_since: None,
    }
}

/// The two replicas every pure test expects.
fn expected() -> BTreeSet<ReplicaId> {
    BTreeSet::from([replica_id("replica-0"), replica_id("replica-1")])
}

/// Two registrations renewed a second ago.
fn both_alive() -> Vec<ReplicaRecord> {
    vec![
        registration("replica-0", Duration::from_secs(1)),
        registration("replica-1", Duration::from_secs(1)),
    ]
}

#[test]
fn cluster_with_every_replica_alive_and_a_leader_is_healthy() {
    let reason = reason_it_is_unhealthy(&expected(), &both_alive(), Some(&leadership_in_force()), NOW);

    assert_eq!(reason, None);
}

#[test]
fn replica_that_is_no_longer_registered_is_a_reason() {
    let registered = vec![registration("replica-0", Duration::from_secs(1))];

    let reason = reason_it_is_unhealthy(&expected(), &registered, Some(&leadership_in_force()), NOW);

    assert_eq!(reason.as_deref(), Some("replica replica-1 is no longer registered"));
}

#[test]
fn replica_silent_for_longer_than_the_liveness_window_is_a_reason() {
    let registered = vec![
        registration("replica-0", Duration::from_secs(1)),
        registration("replica-1", Duration::from_secs(16)),
    ];

    let reason = reason_it_is_unhealthy(&expected(), &registered, Some(&leadership_in_force()), NOW);

    assert_eq!(reason.as_deref(), Some("replica replica-1 has not registered for 16s"));
}

#[test]
fn replica_silent_for_exactly_the_liveness_window_is_still_alive() {
    let registered = vec![
        registration("replica-0", Duration::from_secs(1)),
        registration("replica-1", LIVENESS_WINDOW),
    ];

    let reason = reason_it_is_unhealthy(&expected(), &registered, Some(&leadership_in_force()), NOW);

    assert_eq!(reason, None);
}

#[test]
fn cluster_without_a_leader_is_not_healthy() {
    let reason = reason_it_is_unhealthy(&expected(), &both_alive(), None, NOW);

    assert_eq!(reason.as_deref(), Some("no controller leads"));
}

#[test]
fn leadership_that_has_expired_is_a_reason() {
    let mut leader = leadership_in_force();
    leader.expires_at = NOW;

    let reason = reason_it_is_unhealthy(&expected(), &both_alive(), Some(&leader), NOW);

    assert_eq!(
        reason.as_deref(),
        Some("the leadership of controller controller-0 has expired")
    );
}

/// A replica that joined after the run began is not asked for: the run only promises to find again what it started
/// with.
#[test]
fn replica_nobody_expects_does_not_matter() {
    let mut registered = both_alive();
    registered.push(registration("replica-9", Duration::from_secs(600)));

    let reason = reason_it_is_unhealthy(&expected(), &registered, Some(&leadership_in_force()), NOW);

    assert_eq!(reason, None);
}

/// Lets the controller of an in-process cluster take the leadership.
async fn let_a_controller_lead() {
    tokio::time::sleep(Duration::from_secs(2)).await;
}

#[tokio::test(start_paused = true)]
async fn cluster_as_started_has_every_replica_alive() {
    let cluster = InProcessCluster::start().await;

    let alive = alive_replicas(&cluster.soaked).await.expect("the registry answers");

    assert_eq!(alive.len(), 6);
}

#[tokio::test(start_paused = true)]
async fn cluster_as_started_is_healthy_once_a_controller_leads() {
    let cluster = InProcessCluster::start().await;
    let expected_replicas = alive_replicas(&cluster.soaked).await.expect("the registry answers");
    let_a_controller_lead().await;

    let reason = read_unhealthy_reason(&cluster.soaked, &expected_replicas).await;

    assert_eq!(reason, None);
}

#[tokio::test(start_paused = true)]
async fn metadata_store_that_does_not_answer_is_a_reason() {
    let cluster = InProcessCluster::start().await;
    let expected_replicas = alive_replicas(&cluster.soaked).await.expect("the registry answers");
    cluster.faults.set_metadata_store_unavailable(true);

    let reason = read_unhealthy_reason(&cluster.soaked, &expected_replicas).await;

    let reason = reason.expect("a store that does not answer is not healthy");
    assert!(
        reason.starts_with("the registry of replicas cannot be read"),
        "{reason}"
    );
}

#[tokio::test(start_paused = true)]
async fn wait_returns_once_a_killed_replica_is_back() {
    let cluster = InProcessCluster::start().await;
    let expected_replicas = alive_replicas(&cluster.soaked).await.expect("the registry answers");
    let_a_controller_lead().await;
    cluster.faults.crash_for(0, Duration::from_secs(40));

    let waited = wait_until_healthy(&cluster.soaked, &expected_replicas, Duration::from_secs(120)).await;

    assert_eq!(waited, Ok(()));
}

#[tokio::test(start_paused = true)]
async fn wait_gives_the_reason_when_the_time_runs_out() {
    let cluster = InProcessCluster::start().await;
    let expected_replicas = alive_replicas(&cluster.soaked).await.expect("the registry answers");
    let_a_controller_lead().await;
    cluster.faults.crash_for(0, Duration::from_secs(3_600));
    tokio::time::sleep(Duration::from_secs(20)).await;

    let waited = wait_until_healthy(&cluster.soaked, &expected_replicas, Duration::from_secs(30)).await;

    let reason = waited.expect_err("the replica stays down");
    assert!(
        reason.starts_with("replica replica-0 has not registered for"),
        "{reason}"
    );
}
