//! Tests of the `bita controller` commands, against an in-process cluster.
//!
//! # What is covered
//!
//! The three states a cluster can be in: no controller ever led, one leads now, the last leadership
//! lapsed.
//!
//! # How the fixtures work
//!
//! See `testing.rs`. A controller "leads" by running one round, which takes the leadership for the
//! default duration; letting more than that pass makes it lapse.

use std::time::Duration;

use bitacora_controller::ControllerSettings;

use super::*;
use crate::commands::testing::TestCluster;

/// Has a controller called `bitacora-controller` take the leadership.
async fn lead(started: &TestCluster) {
    started
        .controller(ControllerSettings::default())
        .run_round()
        .await
        .expect("the round runs");
}

#[tokio::test(start_paused = true)]
async fn cluster_no_controller_ever_led_has_no_leader() {
    let started = TestCluster::start(3).await;

    let reported = status(&started.cluster).await.expect("reads");

    assert_eq!(reported.leader, None);
}

#[tokio::test(start_paused = true)]
async fn controller_that_ran_a_round_leads() {
    let started = TestCluster::start(3).await;
    lead(&started).await;

    let reported = status(&started.cluster).await.expect("reads");

    let leader = reported.leader.expect("somebody leads");
    assert_eq!(leader.holder, "bitacora-controller");
    assert!(leader.is_current);
}

#[tokio::test(start_paused = true)]
async fn leader_shows_for_how_long_it_has_led() {
    let started = TestCluster::start(3).await;
    lead(&started).await;
    tokio::time::advance(Duration::from_secs(4)).await;

    let reported = status(&started.cluster).await.expect("reads");

    let leader = reported.leader.expect("somebody leads");
    assert_eq!(leader.leading_for_seconds, Some(4));
}

#[tokio::test(start_paused = true)]
async fn leadership_that_was_not_renewed_is_shown_as_lapsed() {
    let started = TestCluster::start(3).await;
    lead(&started).await;
    tokio::time::advance(ControllerSettings::default().leadership_duration * 2).await;

    let reported = status(&started.cluster).await.expect("reads");

    let leader = reported.leader.expect("the record stays");
    assert!(!leader.is_current);
    assert_eq!(leader.leading_for_seconds, None);
}
