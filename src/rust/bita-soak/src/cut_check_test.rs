//! Tests of the check that tells a real network cut from one that only exists on paper.
//!
//! # What is covered
//!
//! - The watch, as plain logic: a first reading starts the quiet period; a registration that changes restarts it;
//!   one that stays the same does not; a replica that is not registered counts as silent; the most recent renewal
//!   among several replicas is the one reported.
//! - The check against the in-process cluster: a cut that cuts is in effect after the quiet period; a cut that
//!   does nothing is not in effect, with the replica that kept registering named; a zone of which one replica
//!   still registers is not in effect.
//! - What is no evidence: a registry that cannot be read, a tool that is told to stop, a cut with no replica
//!   behind it.
//!
//! # How the fixtures work
//!
//! The pure tests build registrations by hand and move a paused clock. The others start the in-process cluster of
//! `test_support.rs`, whose nodes renew their registration every two seconds unless they are cut off, and check
//! with [`QUICK_CUT_CHECK`]: six seconds of silence, within twenty.

use bitacora_meta::RECORD_SCHEMA_VERSION;
use tokio::time::advance;

use super::*;
use crate::faults::Fault;
use crate::test_support::{InProcessCluster, QUICK_CUT_CHECK};

/// The quiet period of the pure tests.
const QUIET: Duration = Duration::from_secs(6);

/// The replica identifier `name`.
fn replica_id(name: &str) -> ReplicaId {
    ReplicaId::new(name).expect("a valid identifier")
}

/// The identifiers of `names`, as the set of replicas behind a cut.
fn behind_the_cut(names: &[&str]) -> BTreeSet<ReplicaId> {
    let mut cut_off = BTreeSet::new();
    for name in names {
        cut_off.insert(replica_id(name));
    }
    cut_off
}

/// The registration of the replica `name`, last renewed at `heartbeat_unix_millis`.
fn registration(name: &str, heartbeat_unix_millis: u64) -> ReplicaRecord {
    ReplicaRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        replica_id: replica_id(name),
        address: format!("{name}:7000"),
        failure_domains: vec![format!("host-{name}")],
        capacity_bytes: 1_000,
        free_bytes: 900,
        is_draining: false,
        heartbeat_at: Timestamp::from_unix_millis(heartbeat_unix_millis),
    }
}

/// A stop that nobody fires, and the sender that keeps it that way.
fn no_stop() -> (watch::Sender<bool>, watch::Receiver<bool>) {
    watch::channel(false)
}

/// Checks, on `cluster`, a cut that hides the replicas `names`, with the quick settings and no stop.
async fn check(cluster: &InProcessCluster, names: &[&str]) -> CutEffect {
    let (_told_to_stop, mut stop) = no_stop();
    check_cut(&cluster.soaked, &behind_the_cut(names), QUICK_CUT_CHECK, &mut stop).await
}

#[tokio::test(start_paused = true)]
async fn watch_that_has_seen_nothing_is_not_quiet_yet() {
    let watch = RenewalWatch::starting_now(&behind_the_cut(&["replica-0"]));

    let latest = watch.latest_renewal_within(QUIET);

    assert_eq!(latest, Some((&replica_id("replica-0"), Duration::ZERO)));
}

/// The registration that is read first may have been written a moment ago: the silence is counted from the
/// reading, not from before it.
#[tokio::test(start_paused = true)]
async fn first_reading_starts_the_quiet_period() {
    let mut watch = RenewalWatch::starting_now(&behind_the_cut(&["replica-0"]));
    advance(Duration::from_secs(5)).await;

    watch.observe(&[registration("replica-0", 1_000)], Instant::now());
    advance(Duration::from_secs(5)).await;

    assert_eq!(
        watch.latest_renewal_within(QUIET),
        Some((&replica_id("replica-0"), Duration::from_secs(5)))
    );
}

#[tokio::test(start_paused = true)]
async fn registration_that_stays_the_same_for_the_quiet_period_is_silence() {
    let mut watch = RenewalWatch::starting_now(&behind_the_cut(&["replica-0"]));
    watch.observe(&[registration("replica-0", 1_000)], Instant::now());
    advance(Duration::from_secs(3)).await;
    watch.observe(&[registration("replica-0", 1_000)], Instant::now());
    advance(Duration::from_secs(3)).await;

    watch.observe(&[registration("replica-0", 1_000)], Instant::now());

    assert_eq!(watch.latest_renewal_within(QUIET), None);
}

#[tokio::test(start_paused = true)]
async fn registration_that_changes_restarts_the_quiet_period() {
    let mut watch = RenewalWatch::starting_now(&behind_the_cut(&["replica-0"]));
    watch.observe(&[registration("replica-0", 1_000)], Instant::now());
    advance(Duration::from_secs(5)).await;
    watch.observe(&[registration("replica-0", 3_000)], Instant::now());
    advance(Duration::from_secs(2)).await;

    let latest = watch.latest_renewal_within(QUIET);

    assert_eq!(latest, Some((&replica_id("replica-0"), Duration::from_secs(2))));
}

/// A replica that is not in the registry is not renewing anything.
#[tokio::test(start_paused = true)]
async fn replica_that_is_not_registered_counts_as_silent() {
    let mut watch = RenewalWatch::starting_now(&behind_the_cut(&["replica-0"]));
    advance(QUIET).await;

    watch.observe(&[registration("replica-1", 1_000)], Instant::now());

    assert_eq!(watch.latest_renewal_within(QUIET), None);
}

#[tokio::test(start_paused = true)]
async fn replicas_outside_the_cut_are_not_watched() {
    let mut watch = RenewalWatch::starting_now(&behind_the_cut(&["replica-0"]));
    watch.observe(
        &[registration("replica-0", 1_000), registration("replica-1", 1_000)],
        Instant::now(),
    );
    advance(QUIET).await;

    watch.observe(
        &[registration("replica-0", 1_000), registration("replica-1", 9_000)],
        Instant::now(),
    );

    assert_eq!(watch.latest_renewal_within(QUIET), None);
}

#[tokio::test(start_paused = true)]
async fn most_recent_renewal_among_several_replicas_is_the_one_reported() {
    let mut watch = RenewalWatch::starting_now(&behind_the_cut(&["replica-0", "replica-3"]));
    watch.observe(
        &[registration("replica-0", 1_000), registration("replica-3", 1_000)],
        Instant::now(),
    );
    advance(Duration::from_secs(4)).await;
    watch.observe(
        &[registration("replica-0", 1_000), registration("replica-3", 5_000)],
        Instant::now(),
    );
    advance(Duration::from_secs(1)).await;

    let latest = watch.latest_renewal_within(QUIET);

    assert_eq!(latest, Some((&replica_id("replica-3"), Duration::from_secs(1))));
}

#[tokio::test(start_paused = true)]
async fn cut_that_cuts_is_in_effect() {
    let cluster = InProcessCluster::start().await;
    let cut = Fault::CutReplica {
        pod: "replica-4".to_owned(),
    };
    cluster.soaked.faults.inject(&cut).await.expect("the cut is made");

    let effect = check(&cluster, &["replica-4"]).await;

    assert_eq!(effect, CutEffect::InEffect);
}

#[tokio::test(start_paused = true)]
async fn cut_is_in_effect_once_the_quiet_period_has_passed() {
    let cluster = InProcessCluster::start().await;
    let cut = Fault::CutReplica {
        pod: "replica-4".to_owned(),
    };
    cluster.soaked.faults.inject(&cut).await.expect("the cut is made");
    let started_at = Instant::now();

    check(&cluster, &["replica-4"]).await;

    assert_eq!(started_at.elapsed(), QUICK_CUT_CHECK.quiet_period);
}

/// The cluster's management accepted the cut and nothing changed: the tool must see that for itself.
#[tokio::test(start_paused = true)]
async fn cut_that_does_nothing_is_not_in_effect() {
    let cluster = InProcessCluster::start().await;
    cluster.faults.let_cuts_do_nothing(true);
    let cut = Fault::CutReplica {
        pod: "replica-4".to_owned(),
    };
    cluster.soaked.faults.inject(&cut).await.expect("the cut is accepted");

    let effect = check(&cluster, &["replica-4"]).await;

    let CutEffect::NotInEffect(evidence) = effect else {
        panic!("expected the cut not to be in effect, got {effect:?}");
    };
    assert!(
        evidence.starts_with("replica replica-4 renewed its registration"),
        "{evidence}"
    );
}

#[tokio::test(start_paused = true)]
async fn check_of_a_cut_that_does_nothing_ends_at_its_time_limit() {
    let cluster = InProcessCluster::start().await;
    cluster.faults.let_cuts_do_nothing(true);
    let started_at = Instant::now();

    check(&cluster, &["replica-4"]).await;

    assert_eq!(started_at.elapsed(), QUICK_CUT_CHECK.time_limit);
}

/// A zone is cut off only if every one of its replicas is.
#[tokio::test(start_paused = true)]
async fn zone_of_which_one_replica_still_registers_is_not_in_effect() {
    let cluster = InProcessCluster::start().await;
    let cut = Fault::CutReplica {
        pod: "replica-1".to_owned(),
    };
    cluster.soaked.faults.inject(&cut).await.expect("the cut is made");

    let effect = check(&cluster, &["replica-1", "replica-4"]).await;

    let CutEffect::NotInEffect(evidence) = effect else {
        panic!("expected the cut not to be in effect, got {effect:?}");
    };
    assert!(
        evidence.starts_with("replica replica-4 renewed its registration"),
        "{evidence}"
    );
}

/// Not being able to look is not the same as having seen the cut fail.
#[tokio::test(start_paused = true)]
async fn registry_that_cannot_be_read_is_no_evidence() {
    let cluster = InProcessCluster::start().await;
    cluster.faults.set_metadata_store_unavailable(true);

    let effect = check(&cluster, &["replica-4"]).await;

    assert_eq!(
        effect,
        CutEffect::Unknown("the registry of replicas could not be read".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn tool_that_is_told_to_stop_ends_the_check_without_evidence() {
    let cluster = InProcessCluster::start().await;
    cluster.faults.let_cuts_do_nothing(true);
    let (told_to_stop, mut stop) = no_stop();
    let started_at = Instant::now();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(3)).await;
        told_to_stop.send(true).expect("the check listens");
    });

    let effect = check_cut(
        &cluster.soaked,
        &behind_the_cut(&["replica-4"]),
        QUICK_CUT_CHECK,
        &mut stop,
    )
    .await;

    assert_eq!(
        effect,
        CutEffect::Unknown("the tool was told to stop before the cut could be checked".to_owned())
    );
    assert_eq!(started_at.elapsed(), Duration::from_secs(3));
}

#[tokio::test(start_paused = true)]
async fn cut_with_no_replica_behind_it_cannot_be_checked() {
    let cluster = InProcessCluster::start().await;

    let effect = check(&cluster, &[]).await;

    assert_eq!(
        effect,
        CutEffect::Unknown("no registered replica is behind the cut".to_owned())
    );
}
