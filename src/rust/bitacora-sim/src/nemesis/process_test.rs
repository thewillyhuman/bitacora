//! Tests of the faults that kill a replica process.
//!
//! # What is covered
//!
//! - A crash takes the planned node down and brings it back after the planned time.
//! - A crash is recorded with its node and duration, and a torn crash is recorded as torn.
//! - A node that is already down is left alone.
//! - Which crash touches the nemesis's random source.
//!
//! # How the fixtures work
//!
//! Each test starts a small world of the test support and calls `crash_node` with a plan written by hand, as the
//! nemesis loop would. Time is paused, so a restart planned for three seconds happens exactly when the test has slept
//! that long. `drawn_after` reports the next number of a random source, to tell whether a fault drew from it.

use std::time::Duration;

use super::*;
use crate::test_support::{fault_descriptions, start_world};

/// The node the plans of these tests hit.
const NODE: usize = 1;

/// How long the faults of these tests last.
const LASTS_FOR: Duration = Duration::from_secs(3);

/// A moment just before a fault of these tests is undone.
const JUST_BEFORE_THE_END: Duration = Duration::from_millis(2_999);

/// A moment just after a fault of these tests is undone.
const JUST_AFTER_THE_END: Duration = Duration::from_millis(3_001);

/// The seed of the random source handed to the fault.
const SEED: u64 = 5;

/// A plan for a fault of `kind` on [`NODE`] that lasts [`LASTS_FOR`].
fn plan_of(kind: FaultKind) -> FaultPlan {
    FaultPlan {
        kind,
        node_number: NODE,
        lasts_for: LASTS_FOR,
    }
}

/// The next number `random` draws.
fn drawn_after(mut random: DeterministicRandom) -> u64 {
    random.next_u64()
}

#[tokio::test(start_paused = true)]
async fn crash_takes_the_planned_node_down() {
    let world = start_world(1).await;
    let mut random = DeterministicRandom::from_seed(SEED);
    let mut repairs = JoinSet::new();

    crash_node(&world, plan_of(FaultKind::NodeCrash), &mut random, &mut repairs);

    assert!(!world.cluster.nodes()[NODE].is_running());
}

#[tokio::test(start_paused = true)]
async fn crash_leaves_the_other_nodes_running() {
    let world = start_world(1).await;
    let mut random = DeterministicRandom::from_seed(SEED);
    let mut repairs = JoinSet::new();

    crash_node(&world, plan_of(FaultKind::NodeCrash), &mut random, &mut repairs);

    assert!(world.cluster.nodes()[0].is_running());
    assert!(world.cluster.nodes()[2].is_running());
}

#[tokio::test(start_paused = true)]
async fn crashed_node_is_restarted_after_the_planned_time() {
    let world = start_world(1).await;
    let mut random = DeterministicRandom::from_seed(SEED);
    let mut repairs = JoinSet::new();
    crash_node(&world, plan_of(FaultKind::NodeCrash), &mut random, &mut repairs);

    sleep(JUST_AFTER_THE_END).await;

    assert!(world.cluster.nodes()[NODE].is_running());
}

#[tokio::test(start_paused = true)]
async fn crashed_node_stays_down_until_the_planned_time() {
    let world = start_world(1).await;
    let mut random = DeterministicRandom::from_seed(SEED);
    let mut repairs = JoinSet::new();
    crash_node(&world, plan_of(FaultKind::NodeCrash), &mut random, &mut repairs);

    sleep(JUST_BEFORE_THE_END).await;

    assert!(!world.cluster.nodes()[NODE].is_running());
}

/// The restart belongs to the nemesis: when the nemesis is stopped, healing takes over.
#[tokio::test(start_paused = true)]
async fn restart_is_cancelled_with_the_set_that_owns_it() {
    let world = start_world(1).await;
    let mut random = DeterministicRandom::from_seed(SEED);
    let mut repairs = JoinSet::new();
    crash_node(&world, plan_of(FaultKind::NodeCrash), &mut random, &mut repairs);

    drop(repairs);
    sleep(LASTS_FOR * 2).await;

    assert!(!world.cluster.nodes()[NODE].is_running());
}

#[tokio::test(start_paused = true)]
async fn crash_is_recorded_with_its_node_and_duration() {
    let world = start_world(1).await;
    let mut random = DeterministicRandom::from_seed(SEED);
    let mut repairs = JoinSet::new();

    crash_node(&world, plan_of(FaultKind::NodeCrash), &mut random, &mut repairs);

    assert_eq!(fault_descriptions(&world.history), ["node 1 crashes for 3s"]);
}

#[tokio::test(start_paused = true)]
async fn torn_crash_is_recorded_as_torn() {
    let world = start_world(1).await;
    let mut random = DeterministicRandom::from_seed(SEED);
    let mut repairs = JoinSet::new();

    crash_node(&world, plan_of(FaultKind::TornCrash), &mut random, &mut repairs);

    assert_eq!(
        fault_descriptions(&world.history),
        ["node 1 crashes with torn writes for 3s"]
    );
}

#[tokio::test(start_paused = true)]
async fn torn_crash_takes_the_planned_node_down() {
    let world = start_world(1).await;
    let mut random = DeterministicRandom::from_seed(SEED);
    let mut repairs = JoinSet::new();

    crash_node(&world, plan_of(FaultKind::TornCrash), &mut random, &mut repairs);

    assert!(!world.cluster.nodes()[NODE].is_running());
}

#[tokio::test(start_paused = true)]
async fn node_that_is_already_down_is_left_alone() {
    let world = start_world(1).await;
    let mut random = DeterministicRandom::from_seed(SEED);
    let mut repairs = JoinSet::new();
    world.cluster.nodes()[NODE].crash();

    crash_node(&world, plan_of(FaultKind::TornCrash), &mut random, &mut repairs);

    assert_eq!(fault_descriptions(&world.history), Vec::<String>::new());
    assert_eq!(repairs.len(), 0);
    assert_eq!(drawn_after(random), drawn_after(DeterministicRandom::from_seed(SEED)));
}

#[tokio::test(start_paused = true)]
async fn plain_crash_draws_no_random_number() {
    let world = start_world(1).await;
    let mut random = DeterministicRandom::from_seed(SEED);
    let mut repairs = JoinSet::new();

    crash_node(&world, plan_of(FaultKind::NodeCrash), &mut random, &mut repairs);

    assert_eq!(drawn_after(random), drawn_after(DeterministicRandom::from_seed(SEED)));
}

#[tokio::test(start_paused = true)]
async fn torn_crash_forks_the_random_source_once() {
    let world = start_world(1).await;
    let mut random = DeterministicRandom::from_seed(SEED);
    let mut repairs = JoinSet::new();

    crash_node(&world, plan_of(FaultKind::TornCrash), &mut random, &mut repairs);

    let mut forked_once = DeterministicRandom::from_seed(SEED);
    drop(forked_once.fork());
    assert_eq!(drawn_after(random), drawn_after(forked_once));
}
