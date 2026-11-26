//! Tests of the faults of the network path to a node.
//!
//! # What is covered
//!
//! - Each of the three kinds puts its fault on the link of the planned node, and on no other.
//! - The link is healthy again after the planned time.
//! - The fault is recorded with its node, its content and its duration.
//! - Only a slow link draws a random number, and its delay stays within its bounds.
//!
//! # How the fixtures work
//!
//! Each test starts a small world of the test support and calls `disturb_link` with a plan written by hand, as the
//! nemesis loop would. The fault is read back from the link itself. Time is paused, so a removal planned for three
//! seconds happens exactly when the test has slept that long.

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

/// Injects a link fault of `kind` into a new world and reports what the link of [`NODE`] has afterwards.
async fn fault_injected_for(kind: FaultKind) -> LinkFault {
    let world = start_world(1).await;
    let mut random = DeterministicRandom::from_seed(SEED);
    let mut repairs = JoinSet::new();
    disturb_link(&world, plan_of(kind), &mut random, &mut repairs);
    world.cluster.nodes()[NODE].link().fault()
}

#[tokio::test(start_paused = true)]
async fn partition_cuts_the_link() {
    let fault = fault_injected_for(FaultKind::Partition).await;

    assert_eq!(
        fault,
        LinkFault {
            is_cut: true,
            ..LinkFault::default()
        }
    );
}

#[tokio::test(start_paused = true)]
async fn lost_responses_make_the_link_lose_answers() {
    let fault = fault_injected_for(FaultKind::LostResponses).await;

    assert_eq!(
        fault,
        LinkFault {
            loses_responses: true,
            ..LinkFault::default()
        }
    );
}

#[tokio::test(start_paused = true)]
async fn slow_link_only_delays() {
    let fault = fault_injected_for(FaultKind::SlowLink).await;

    assert_eq!(
        fault,
        LinkFault {
            delay: fault.delay,
            ..LinkFault::default()
        }
    );
}

#[tokio::test(start_paused = true)]
async fn delay_of_a_slow_link_stays_within_its_bounds() {
    let fault = fault_injected_for(FaultKind::SlowLink).await;

    let shortest = Duration::from_millis(SHORTEST_LINK_DELAY_MILLIS);
    let longest = Duration::from_millis(LONGEST_LINK_DELAY_MILLIS);
    assert!(
        shortest <= fault.delay && fault.delay <= longest,
        "the delay is {:?}",
        fault.delay
    );
}

#[tokio::test(start_paused = true)]
async fn fault_hits_the_planned_link_only() {
    let world = start_world(1).await;
    let mut random = DeterministicRandom::from_seed(SEED);
    let mut repairs = JoinSet::new();

    disturb_link(&world, plan_of(FaultKind::Partition), &mut random, &mut repairs);

    assert_eq!(world.cluster.nodes()[0].link().fault(), LinkFault::default());
    assert_eq!(world.cluster.nodes()[2].link().fault(), LinkFault::default());
}

#[tokio::test(start_paused = true)]
async fn link_is_healthy_again_after_the_planned_time() {
    let world = start_world(1).await;
    let mut random = DeterministicRandom::from_seed(SEED);
    let mut repairs = JoinSet::new();
    disturb_link(&world, plan_of(FaultKind::Partition), &mut random, &mut repairs);

    sleep(JUST_AFTER_THE_END).await;

    assert_eq!(world.cluster.nodes()[NODE].link().fault(), LinkFault::default());
}

#[tokio::test(start_paused = true)]
async fn link_stays_disturbed_until_the_planned_time() {
    let world = start_world(1).await;
    let mut random = DeterministicRandom::from_seed(SEED);
    let mut repairs = JoinSet::new();
    disturb_link(&world, plan_of(FaultKind::Partition), &mut random, &mut repairs);

    sleep(JUST_BEFORE_THE_END).await;

    assert!(world.cluster.nodes()[NODE].link().fault().is_cut);
}

/// The entry is part of the fingerprint of a run, down to how the fault is rendered.
#[tokio::test(start_paused = true)]
async fn link_fault_is_recorded_with_its_node_content_and_duration() {
    let world = start_world(1).await;
    let mut random = DeterministicRandom::from_seed(SEED);
    let mut repairs = JoinSet::new();

    disturb_link(&world, plan_of(FaultKind::Partition), &mut random, &mut repairs);

    assert_eq!(
        fault_descriptions(&world.history),
        ["node 1 link gets LinkFault { is_cut: true, loses_responses: false, delay: 0ns } for 3s"]
    );
}

#[test]
fn partition_draws_no_random_number() {
    let mut random = DeterministicRandom::from_seed(SEED);

    let _ = link_fault_of(FaultKind::Partition, &mut random);

    assert_eq!(random.next_u64(), DeterministicRandom::from_seed(SEED).next_u64());
}

#[test]
fn lost_responses_draw_no_random_number() {
    let mut random = DeterministicRandom::from_seed(SEED);

    let _ = link_fault_of(FaultKind::LostResponses, &mut random);

    assert_eq!(random.next_u64(), DeterministicRandom::from_seed(SEED).next_u64());
}

#[test]
fn slow_link_draws_its_delay_from_the_random_source() {
    let mut random = DeterministicRandom::from_seed(SEED);

    let fault = link_fault_of(FaultKind::SlowLink, &mut random);

    let mut expected = DeterministicRandom::from_seed(SEED);
    let expected_delay_millis = expected.between(SHORTEST_LINK_DELAY_MILLIS, LONGEST_LINK_DELAY_MILLIS);
    assert_eq!(fault.delay, Duration::from_millis(expected_delay_millis));
}
