//! Tests of what a run says when its healed cluster does not recover.
//!
//! # What is covered
//!
//! - The description names the step the audit was at and how long it was given.
//! - It shows the stream record, and leaves it out when the metadata store does not answer.
//! - It shows what every node holds: nothing, a copy, or that the node is down.
//!
//! # How the fixtures work
//!
//! Each test starts a small world of the test support, puts it in the state a stuck run would be in, and reads the
//! description. Nothing is stuck for real: the description is a snapshot and can be taken at any time.

use super::*;
use crate::test_support::{start_world, store_one_record_on};
use crate::world::AuditStep;

/// How long the audits of these tests were given.
const WAITED: Duration = Duration::from_mins(5);

#[tokio::test(start_paused = true)]
async fn description_names_the_step_the_audit_was_at() {
    let world = start_world(1).await;
    world.now_waiting_for(AuditStep::ReadingEveryKey);

    let description = describe_stuck_cluster(&world, WAITED).await;

    assert!(
        description
            .starts_with("the healed cluster did not settle within 300s; it was reading every key through the store"),
        "{description}"
    );
}

#[tokio::test(start_paused = true)]
async fn description_shows_the_stream_record() {
    let world = start_world(1).await;

    let description = describe_stuck_cluster(&world, WAITED).await;

    assert!(
        description.contains("; epoch 0, low-water mark 1, standby None, segments:"),
        "{description}"
    );
}

#[tokio::test(start_paused = true)]
async fn description_leaves_the_stream_out_when_the_metadata_store_is_down() {
    let world = start_world(1).await;
    world.cluster.key_value_store().set_unavailable(true);

    let description = describe_stuck_cluster(&world, WAITED).await;

    assert!(!description.contains("epoch"), "{description}");
}

#[tokio::test(start_paused = true)]
async fn description_shows_a_node_that_holds_nothing() {
    let world = start_world(1).await;

    let description = describe_stuck_cluster(&world, WAITED).await;

    assert!(description.contains("; replica-0 holds Ok(\"\")"), "{description}");
}

#[tokio::test(start_paused = true)]
async fn description_shows_a_node_that_is_down() {
    let world = start_world(1).await;
    world.cluster.nodes()[1].crash();

    let description = describe_stuck_cluster(&world, WAITED).await;

    assert!(description.contains("; replica-1 holds Ok(\"down\")"), "{description}");
}

#[tokio::test(start_paused = true)]
async fn description_shows_the_copies_a_node_holds() {
    let world = start_world(1).await;
    store_one_record_on(&world, 2).await;

    let description = describe_stuck_cluster(&world, WAITED).await;

    assert!(
        description.contains("; replica-2 holds Ok(\"1:end=Some(1),fenced=1,sealed=None\")"),
        "{description}"
    );
}

#[tokio::test(start_paused = true)]
async fn description_changes_nothing_in_the_history() {
    let world = start_world(1).await;
    let events_before = world.history.events();

    let _ = describe_stuck_cluster(&world, WAITED).await;

    assert_eq!(world.history.events(), events_before);
}
