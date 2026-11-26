//! Tests of leadership and of the loop around the pass.
//!
//! # What is covered
//!
//! - A round of the instance that leads runs a pass; a round of another instance stands by; a round fails while the
//!   metadata store is down.
//! - The loop: a second controller takes over when the first stops, and a controller keeps going through an outage
//!   of the metadata store.
//!
//! # How the fixtures work
//!
//! Controllers are built over the testkit cluster with the default settings and different identities. The loop
//! tests spawn `Controller::run` as a task and let virtual time pass.

use std::sync::Arc;
use std::time::Duration;

use bitacora_testkit::Cluster;

use super::super::ControllerSettings;
use super::super::test_support::{controller_named, create_stream, stream};
use super::*;

/// The identity of the instance that currently holds the leadership, if any.
async fn leader(cluster: &Cluster) -> Option<String> {
    let leadership = cluster
        .metadata()
        .get_leader()
        .await
        .expect("the metadata store is available");
    leadership.map(|record| record.holder)
}

#[tokio::test(start_paused = true)]
async fn round_of_the_leading_controller_runs_a_pass() {
    let cluster = Cluster::start(3).await;
    create_stream(&cluster).await;

    let round = controller_named(&cluster, "controller-1").run_round().await;

    assert!(
        matches!(&round, Ok(Round::Led(report)) if report.standbys_placed == 1),
        "{round:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn round_of_a_controller_that_does_not_lead_stands_by() {
    let cluster = Cluster::start(3).await;
    create_stream(&cluster).await;
    let _leading_round = controller_named(&cluster, "controller-1").run_round().await;

    let round = controller_named(&cluster, "controller-2").run_round().await;

    assert_eq!(round, Ok(Round::StoodBy));
}

#[tokio::test(start_paused = true)]
async fn round_fails_while_the_metadata_store_is_down() {
    let cluster = Cluster::start(3).await;
    cluster.key_value_store().set_unavailable(true);

    let round = controller_named(&cluster, "controller-1").run_round().await;

    assert!(round.is_err(), "{round:?}");
}

/// Starts the loop of a controller with the given identity as a task.
fn spawn_controller(cluster: &Arc<Cluster>, identity: &'static str) -> tokio::task::JoinHandle<()> {
    let cluster = Arc::clone(cluster);
    tokio::spawn(async move { controller_named(&cluster, identity).run().await })
}

#[tokio::test(start_paused = true)]
async fn first_controller_keeps_the_leadership_while_it_runs() {
    let cluster = Arc::new(Cluster::start(3).await);
    let _first = spawn_controller(&cluster, "controller-1");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let _second = spawn_controller(&cluster, "controller-2");

    tokio::time::sleep(Duration::from_secs(3)).await;

    assert_eq!(leader(&cluster).await, Some("controller-1".to_owned()));
}

#[tokio::test(start_paused = true)]
async fn second_controller_takes_over_when_the_first_one_stops() {
    let cluster = Arc::new(Cluster::start(3).await);
    let first = spawn_controller(&cluster, "controller-1");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let _second = spawn_controller(&cluster, "controller-2");
    tokio::time::sleep(Duration::from_secs(3)).await;

    first.abort();
    tokio::time::sleep(ControllerSettings::default().leadership_duration * 2).await;

    assert_eq!(leader(&cluster).await, Some("controller-2".to_owned()));
}

#[tokio::test(start_paused = true)]
async fn controller_keeps_running_through_a_metadata_outage() {
    let cluster = Arc::new(Cluster::start(3).await);
    let running_controller = spawn_controller(&cluster, "controller-1");
    cluster.key_value_store().set_unavailable(true);
    tokio::time::sleep(Duration::from_secs(5)).await;
    cluster.key_value_store().set_unavailable(false);

    create_stream(&cluster).await;
    tokio::time::sleep(Duration::from_secs(5)).await;

    running_controller.abort();
    assert!(stream(&cluster).await.standby_replicas.is_some());
}
