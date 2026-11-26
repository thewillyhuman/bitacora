//! Tests of the nemesis loop.
//!
//! # What is covered
//!
//! - A nemesis with no kind enabled does nothing.
//! - A nemesis with one kind enabled injects faults of that kind only, and keeps injecting.
//! - The same seed gives the same faults at the same times.
//! - A node loses its disk at most once per run.
//! - The wording of a fault on a node.
//!
//! # How the fixtures work
//!
//! `faults_of_a_run` starts a small world of the test support, runs the nemesis on it for a minute of simulated time
//! with nothing else going on, and returns what the nemesis recorded. No client runs, so the faults hit an idle
//! cluster; what a fault does to a busy one is the business of `tests/fault_matrix_test.rs`. What each kind of fault
//! does is tested next to it, in `nemesis/`.

use super::*;
use crate::config::FaultMix;
use crate::history::TimedEvent;
use crate::test_support::{WORLD_NODE_COUNT, fault_descriptions, start_world};

/// How long the nemesis of these tests runs, in simulated time.
const CHAOS_DURATION: Duration = Duration::from_mins(1);

/// How many compute nodes the tests pretend the run has.
const COMPUTE_NODE_COUNT: usize = 2;

/// What a nemesis did in one run of these tests.
struct NemesisRun {
    /// The description of every fault it recorded, in order.
    descriptions: Vec<String>,
    /// Every event of the run's history, with its time.
    events: Vec<TimedEvent>,
}

/// Runs a nemesis with the given faults and seed on a new, idle world for [`CHAOS_DURATION`].
async fn faults_of_a_run(faults: FaultMix, seed: u64) -> NemesisRun {
    let world = start_world(seed).await;
    let config = SimulationConfig {
        node_count: WORLD_NODE_COUNT,
        compute_node_count: COMPUTE_NODE_COUNT,
        faults,
        ..SimulationConfig::with_seed(seed)
    };
    let (designated_writer, _) = watch::channel(0_usize);
    let mut crash_signals = Vec::new();
    for _compute_node in 0..COMPUTE_NODE_COUNT {
        let (crash_signal, _) = watch::channel(0_u64);
        crash_signals.push(crash_signal);
    }
    let nemesis = tokio::spawn(run_nemesis(
        Arc::clone(&world),
        config,
        DeterministicRandom::from_seed(seed),
        designated_writer,
        crash_signals,
    ));
    sleep(CHAOS_DURATION).await;
    nemesis.abort();
    drop(nemesis.await);
    NemesisRun {
        descriptions: fault_descriptions(&world.history),
        events: world.history.events(),
    }
}

/// Lists the descriptions that do not contain `expected_words`.
fn descriptions_without(descriptions: &[String], expected_words: &str) -> Vec<String> {
    let mut others = Vec::new();
    for description in descriptions {
        if !description.contains(expected_words) {
            others.push(description.clone());
        }
    }
    others
}

#[test]
fn fault_on_a_node_is_worded_with_the_node_and_the_duration() {
    let plan = FaultPlan {
        kind: FaultKind::NodeCrash,
        node_number: 4,
        lasts_for: Duration::from_millis(2_500),
    };

    assert_eq!(plan.describe("crashes"), "node 4 crashes for 2.5s");
}

#[tokio::test(start_paused = true)]
async fn nemesis_with_no_kind_enabled_does_nothing() {
    let run = faults_of_a_run(FaultMix::NONE, 1).await;

    assert_eq!(run.descriptions, Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn nemesis_injects_only_the_kind_that_is_enabled() {
    let only_partitions = FaultMix {
        partitions: true,
        ..FaultMix::NONE
    };

    let run = faults_of_a_run(only_partitions, 1).await;

    assert_eq!(
        descriptions_without(&run.descriptions, "is_cut: true"),
        Vec::<String>::new()
    );
}

/// With faults 700 ms apart on average, a minute holds about eighty-five of them.
#[tokio::test(start_paused = true)]
async fn nemesis_keeps_injecting_for_as_long_as_it_runs() {
    let only_metadata_outages = FaultMix {
        metadata_outages: true,
        ..FaultMix::NONE
    };

    let run = faults_of_a_run(only_metadata_outages, 1).await;

    assert!(run.descriptions.len() > 40, "{} faults", run.descriptions.len());
}

#[tokio::test(start_paused = true)]
async fn same_seed_gives_the_same_faults_at_the_same_times() {
    let first_run = faults_of_a_run(FaultMix::ALL, 7).await;
    let second_run = faults_of_a_run(FaultMix::ALL, 7).await;

    assert_eq!(first_run.events, second_run.events);
}

#[tokio::test(start_paused = true)]
async fn different_seeds_give_different_faults() {
    let first_run = faults_of_a_run(FaultMix::ALL, 7).await;
    let second_run = faults_of_a_run(FaultMix::ALL, 8).await;

    assert_ne!(first_run.descriptions, second_run.descriptions);
}

#[tokio::test(start_paused = true)]
async fn node_loses_its_disk_at_most_once_per_run() {
    let only_disk_loss = FaultMix {
        disk_loss: true,
        ..FaultMix::NONE
    };

    let run = faults_of_a_run(only_disk_loss, 1).await;

    assert_eq!(run.descriptions.len(), 1, "{:?}", run.descriptions);
}

#[tokio::test(start_paused = true)]
async fn full_mix_injects_faults_of_several_kinds() {
    let run = faults_of_a_run(FaultMix::ALL, 7).await;

    let crashes = run.descriptions.len() - descriptions_without(&run.descriptions, "crashes").len();
    let outages = run.descriptions.len() - descriptions_without(&run.descriptions, "is unreachable").len();
    assert!(crashes > 0, "no crash in {:?}", run.descriptions);
    assert!(outages > 0, "no outage in {:?}", run.descriptions);
}
