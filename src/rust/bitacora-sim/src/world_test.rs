//! Tests of what the tasks of a run share.
//!
//! # What is covered
//!
//! - A new world has the stream the run writes to, a history that already knows the store's fillers, and an unlucky
//!   node that is one of its nodes and is drawn from the seed.
//! - The audit's step can be noted and read back, and every step has the wording a stuck run reports.
//! - A client built by the world reaches the stream.
//!
//! # How the fixtures work
//!
//! `start_world` of the test support starts a world of three nodes from a seed, under a paused runtime. Nothing else
//! runs in it: no client, no controller, no nemesis.

use super::*;
use crate::history::Event;
use crate::test_support::{WORLD_NODE_COUNT, start_world};

#[tokio::test(start_paused = true)]
async fn new_world_has_the_stream_the_run_writes_to() {
    let world = start_world(1).await;

    let stream = world.cluster.metadata().get_stream(STREAM).await;

    assert!(stream.is_ok(), "{stream:?}");
}

#[tokio::test(start_paused = true)]
async fn new_world_has_as_many_nodes_as_asked_for() {
    let world = start_world(1).await;

    assert_eq!(world.cluster.nodes().len(), WORLD_NODE_COUNT);
}

#[tokio::test(start_paused = true)]
async fn history_of_a_new_world_holds_only_the_declared_fillers() {
    let world = start_world(1).await;

    let mut other_events = Vec::new();
    for timed in world.history.events() {
        if !matches!(timed.event, Event::FillerDeclared { .. }) {
            other_events.push(timed.event);
        }
    }

    assert_eq!(other_events, Vec::new());
}

#[tokio::test(start_paused = true)]
async fn unlucky_node_is_one_of_the_nodes() {
    let world = start_world(1).await;

    assert!(world.unlucky_node < WORLD_NODE_COUNT);
}

/// The unlucky node is the first number a run draws: the same seed must pick the same node.
#[tokio::test(start_paused = true)]
async fn unlucky_node_is_the_first_draw_of_the_seed() {
    let world = start_world(42).await;

    let first_draw = DeterministicRandom::from_seed(42).index(WORLD_NODE_COUNT);

    assert_eq!(world.unlucky_node, first_draw);
}

#[tokio::test(start_paused = true)]
async fn audit_of_a_new_world_has_not_begun() {
    let world = start_world(1).await;

    assert_eq!(world.audit_step(), AuditStep::Starting);
}

#[tokio::test(start_paused = true)]
async fn noted_audit_step_is_reported() {
    let world = start_world(1).await;

    world.now_waiting_for(AuditStep::ReadingEveryKey);

    assert_eq!(world.audit_step(), AuditStep::ReadingEveryKey);
}

/// The wording goes into the report of a stuck run, where people and scripts read it.
#[test]
fn every_audit_step_has_its_wording() {
    let wordings = [
        AuditStep::Starting.description(),
        AuditStep::CommittingFinalRecord.description(),
        AuditStep::ReadingEveryKey.description(),
        AuditStep::ReadingWholeStream.description(),
        AuditStep::WaitingForFullReplication.description(),
    ];

    assert_eq!(
        wordings,
        [
            "starting",
            "opening a store and committing a final record",
            "reading every key through the store",
            "reading the whole stream back",
            "waiting for every sealed segment to have all its copies good",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn client_of_the_world_reaches_the_stream() {
    let world = start_world(1).await;

    let low_water_mark = world.client("tester").open_reader(STREAM).low_water_mark().await;

    assert!(low_water_mark.is_ok(), "{low_water_mark:?}");
}
