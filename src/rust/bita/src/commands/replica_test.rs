//! Tests of the `bita replica` commands, against an in-process cluster.
//!
//! # What is covered
//!
//! - `list`: liveness follows the registration's age, not the replica's reachability.
//! - `status`: the replica's own answer, and each way of not getting one.
//! - `drain`: the order reaches the replica, and the report names what still points at it, once or while
//!   waiting.
//! - `remove`: refused for a live replica unless forced, allowed for a dead one.
//!
//! # How the fixtures work
//!
//! See `testing.rs`. Replicas of the test cluster are called `replica-0`, `replica-1`, ... and sit on
//! hosts `host-0`, `host-1`, ... A replica "dies" by crashing its node and letting more than the liveness
//! window pass, during which only running nodes renew their registration.

use std::sync::Arc;

use bitacora::TakeoverPolicy;
use bitacora_testkit::LinkFault;
use bitacora_types::replica_api::ReplicaError;

use super::*;
use crate::commands::testing::{STREAM, TestCluster};

/// Longer than the liveness window, so that a replica that stopped renewing is taken for dead.
const LONGER_THAN_THE_LIVENESS_WINDOW: Duration = Duration::from_mins(1);

/// The identifier of the node with this number.
fn replica(node_number: usize) -> ReplicaId {
    ReplicaId::new(format!("replica-{node_number}")).expect("a valid identifier")
}

/// The summary of one replica in a listing.
async fn listed_replica(started: &TestCluster, node_number: usize) -> ReplicaSummary {
    let listed = list(&started.cluster).await.expect("lists");
    listed.replicas[node_number].clone()
}

#[tokio::test(start_paused = true)]
async fn every_registered_replica_is_listed() {
    let started = TestCluster::start(3).await;

    let listed = list(&started.cluster).await.expect("lists");

    assert_eq!(listed.replicas.len(), 3);
}

#[tokio::test(start_paused = true)]
async fn listed_replica_shows_its_address_and_failure_domains() {
    let started = TestCluster::start(3).await;

    let first_replica = listed_replica(&started, 0).await;

    assert_eq!(first_replica.replica_id, "replica-0");
    assert_eq!(first_replica.address, "replica-0:7000");
    assert_eq!(first_replica.failure_domains, ["host-0"]);
}

#[tokio::test(start_paused = true)]
async fn replica_that_just_registered_is_alive() {
    let started = TestCluster::start(3).await;

    let first_replica = listed_replica(&started, 0).await;

    assert!(first_replica.is_alive);
}

#[tokio::test(start_paused = true)]
async fn replica_that_stopped_renewing_is_not_alive() {
    let started = TestCluster::start(3).await;
    started.nodes.nodes()[2].crash();
    started.let_pass(LONGER_THAN_THE_LIVENESS_WINDOW).await;

    let crashed_replica = listed_replica(&started, 2).await;

    assert!(!crashed_replica.is_alive);
    assert_eq!(crashed_replica.heartbeat_age_seconds, 60);
}

#[tokio::test(start_paused = true)]
async fn listing_does_not_need_the_replicas_to_answer() {
    let started = TestCluster::start(3).await;
    for node in started.nodes.nodes() {
        node.set_reachable(false);
    }

    let listed = list(&started.cluster).await.expect("lists");

    assert_eq!(listed.replicas.len(), 3);
}

#[tokio::test(start_paused = true)]
async fn status_is_what_the_replica_answers() {
    let started = TestCluster::start(3).await;

    let reported = status(&started.cluster, &replica(1))
        .await
        .expect("the replica answers");

    assert_eq!(reported.replica_id, "replica-1");
    assert_eq!(reported.address, "replica-1:7000");
    assert!(!reported.is_draining);
    assert_eq!(reported.segment_count, 0);
}

#[tokio::test(start_paused = true)]
async fn status_of_a_replica_that_is_not_registered_fails() {
    let started = TestCluster::start(3).await;

    let failed = status(&started.cluster, &replica(9)).await;

    assert_eq!(
        failed,
        Err(CommandError::UnknownReplica {
            replica_id: replica(9)
        })
    );
}

#[tokio::test(start_paused = true)]
async fn status_of_an_unreachable_replica_reports_it_unavailable() {
    let started = TestCluster::start(3).await;
    started.nodes.nodes()[1].set_reachable(false);

    let failed = status(&started.cluster, &replica(1)).await;

    assert!(matches!(
        failed,
        Err(CommandError::Replica {
            reason: ReplicaError::Unavailable { .. },
            ..
        })
    ));
}

#[tokio::test(start_paused = true)]
async fn status_gives_up_after_the_request_timeout() {
    let started = TestCluster::start(3).await;
    started.nodes.nodes()[1].link().set_fault(LinkFault {
        delay: Duration::from_mins(1),
        ..LinkFault::default()
    });
    let asked_at = Instant::now();

    let failed = status(&started.cluster, &replica(1)).await;

    assert_eq!(
        failed,
        Err(CommandError::ReplicaTimedOut {
            replica_id: replica(1)
        })
    );
    assert_eq!(asked_at.elapsed(), started.cluster.request_timeout);
}

#[tokio::test(start_paused = true)]
async fn drained_replica_reports_itself_as_draining() {
    let started = TestCluster::start(3).await;

    drain(&started.cluster, &replica(0), None)
        .await
        .expect("the replica takes the order");

    let reported = status(&started.cluster, &replica(0))
        .await
        .expect("the replica answers");
    assert!(reported.is_draining);
}

#[tokio::test(start_paused = true)]
async fn replica_that_nothing_names_is_drained_at_once() {
    let started = TestCluster::start(3).await;

    let report = drain(&started.cluster, &replica(0), None)
        .await
        .expect("the replica takes the order");

    assert!(report.is_drained);
}

#[tokio::test(start_paused = true)]
async fn drain_names_the_open_segment_that_still_uses_the_replica() {
    let started = TestCluster::with_stream(3).await;
    let _writer = started
        .cluster
        .client("compute-1")
        .open_writer(STREAM, TakeoverPolicy::Force)
        .await
        .expect("the writer opens");

    let report = drain(&started.cluster, &replica(0), None)
        .await
        .expect("the replica takes the order");

    assert!(!report.is_drained);
    assert_eq!(
        report.open_segments,
        [OpenSegmentReference {
            stream: STREAM.to_owned(),
            segment_id: 1,
        }]
    );
}

#[tokio::test(start_paused = true)]
async fn drain_names_the_streams_whose_standby_set_uses_the_replica() {
    let started = TestCluster::with_stream(3).await;

    let report = drain(&started.cluster, &replica(0), None)
        .await
        .expect("the replica takes the order");

    assert!(!report.is_drained);
    assert_eq!(report.standby_streams, [STREAM]);
}

#[tokio::test(start_paused = true)]
async fn drain_that_waits_returns_as_soon_as_nothing_names_the_replica() {
    let started = Arc::new(TestCluster::with_stream(3).await);
    let draining = {
        let started = Arc::clone(&started);
        tokio::spawn(async move {
            drain(&started.cluster, &replica(0), Some(Duration::from_mins(2))).await
        })
    };
    tokio::time::sleep(Duration::from_secs(10)).await;
    let waiting_since = Instant::now();

    // The controller replaces the standby set, as it does for a replica that registered as draining.
    started
        .cluster
        .metadata
        .update_stream(STREAM, |stream| {
            stream.standby_replicas = None;
            Ok(())
        })
        .await
        .expect("the standby set is replaced");

    let report = draining
        .await
        .expect("the command does not panic")
        .expect("the replica takes the order");
    assert!(report.is_drained);
    assert!(waiting_since.elapsed() <= DRAIN_POLL_INTERVAL);
}

#[tokio::test(start_paused = true)]
async fn drain_that_waits_gives_up_after_its_wait() {
    let started = TestCluster::with_stream(3).await;
    let asked_at = Instant::now();

    let report = drain(&started.cluster, &replica(0), Some(Duration::from_secs(30)))
        .await
        .expect("the replica takes the order");

    assert!(!report.is_drained);
    assert_eq!(asked_at.elapsed(), Duration::from_secs(30));
}

#[tokio::test(start_paused = true)]
async fn drain_of_an_unreachable_replica_fails() {
    let started = TestCluster::start(3).await;
    started.nodes.nodes()[0].set_reachable(false);

    let failed = drain(&started.cluster, &replica(0), None).await;

    assert!(matches!(failed, Err(CommandError::Replica { .. })));
}

#[tokio::test(start_paused = true)]
async fn live_replica_is_not_removed() {
    let started = TestCluster::start(3).await;

    let refused = remove(&started.cluster, &replica(0), false).await;

    assert!(matches!(refused, Err(CommandError::Refused { .. })));
}

#[tokio::test(start_paused = true)]
async fn refused_removal_keeps_the_replica_registered() {
    let started = TestCluster::start(3).await;

    let _refused = remove(&started.cluster, &replica(0), false).await;

    let listed = list(&started.cluster).await.expect("lists");
    assert_eq!(listed.replicas.len(), 3);
}

#[tokio::test(start_paused = true)]
async fn live_replica_is_removed_when_forced() {
    let started = TestCluster::start(3).await;

    remove(&started.cluster, &replica(0), true)
        .await
        .expect("the replica is removed");

    let listed = list(&started.cluster).await.expect("lists");
    assert_eq!(listed.replicas.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn dead_replica_is_removed_without_force() {
    let started = TestCluster::start(3).await;
    started.nodes.nodes()[2].crash();
    started.let_pass(LONGER_THAN_THE_LIVENESS_WINDOW).await;

    remove(&started.cluster, &replica(2), false)
        .await
        .expect("the replica is removed");

    let listed = list(&started.cluster).await.expect("lists");
    assert_eq!(listed.replicas.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn removing_a_replica_that_is_not_registered_fails() {
    let started = TestCluster::start(3).await;

    let failed = remove(&started.cluster, &replica(9), true).await;

    assert_eq!(
        failed,
        Err(CommandError::UnknownReplica {
            replica_id: replica(9)
        })
    );
}
