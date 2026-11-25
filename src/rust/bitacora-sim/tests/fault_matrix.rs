//! Runs the simulated fault matrix over a range of seeds.
//!
//! Continuous integration runs a fixed budget of seeds on every change. Set
//! `BITA_SIM_SEEDS` to run more, for example nightly. A seed that fails
//! prints everything needed to replay it, and is then added to
//! [`REGRESSION_SEEDS`] for good.

use std::time::Duration;

use bitacora_sim::{FaultMix, SimulationConfig, SimulationReport, run};

/// How many seeds each of the four fault-matrix tests runs by default.
const DEFAULT_SEEDS_PER_TEST: u64 = 6;

/// Seeds that once found a bug. Each stays here forever, with the bug it
/// found.
const REGRESSION_SEEDS: &[u64] = &[
    // Bit rot in an open segment made a replica restart with a shorter
    // copy that it took for a torn tail; recovery sealed below committed
    // records. Fixed by the flushed mark in the segment header.
    13,
    // A node rebuilt with an empty disk answered a fence in the name of
    // its old identity. Fixed by connecting to replicas by identity.
    67,
    // After retention deleted every segment, the next one reused the
    // identifier 1 and collided with copies not yet swept from replicas.
    74, 215,
];

fn seeds_per_test() -> u64 {
    std::env::var("BITA_SIM_SEEDS")
        .ok()
        .and_then(|budget| budget.parse::<u64>().ok())
        .map_or(DEFAULT_SEEDS_PER_TEST, |budget| budget.div_ceil(4))
}

fn assert_clean(report: &SimulationReport) {
    assert!(report.is_clean(), "{}", report.failure_summary());
}

/// Runs every seed of the budget that falls in `slice` of four, so that the
/// four tests below share the work and run in parallel.
fn run_fault_matrix_slice(slice: u64) {
    for seed in (0..seeds_per_test() * 4).filter(|seed| seed % 4 == slice) {
        assert_clean(&run(&SimulationConfig::with_seed(seed)));
    }
}

#[test]
fn fault_matrix_holds_the_invariants_slice_0() {
    run_fault_matrix_slice(0);
}

#[test]
fn fault_matrix_holds_the_invariants_slice_1() {
    run_fault_matrix_slice(1);
}

#[test]
fn fault_matrix_holds_the_invariants_slice_2() {
    run_fault_matrix_slice(2);
}

#[test]
fn fault_matrix_holds_the_invariants_slice_3() {
    run_fault_matrix_slice(3);
}

#[test]
fn seeds_that_once_found_a_bug_stay_fixed() {
    for seed in REGRESSION_SEEDS {
        assert_clean(&run(&SimulationConfig::with_seed(*seed)));
    }
}

/// Replays the single seed named by `BITA_SIM_SEED`, to reproduce a failure.
/// Add `--test_env=BITA_SIM_DUMP=1` to print the whole history.
///
/// ```sh
/// bazel test //src/rust/bitacora-sim:fault_matrix_test \
///     --test_env=BITA_SIM_SEED=1234 --test_arg=replays_the_seed --test_output=all
/// ```
#[test]
fn replays_the_seed_named_in_the_environment() {
    let Some(seed) = std::env::var("BITA_SIM_SEED")
        .ok()
        .and_then(|seed| seed.parse::<u64>().ok())
    else {
        return;
    };
    let report = run(&SimulationConfig::with_seed(seed));
    println!(
        "seed {seed}: {} committed appends, {} faults, {} events, fingerprint {:016x}",
        report.committed_appends,
        report.faults_injected,
        report.events.len(),
        report.fingerprint()
    );
    // With BITA_SIM_DUMP set, the whole history is printed; run the test
    // with --nocapture (or --test_output=all) to see it.
    if std::env::var_os("BITA_SIM_DUMP").is_some() {
        for timed in &report.events {
            println!("{:>10.3?} {:?}", timed.at, timed.event);
        }
    }
    assert_clean(&report);
}

#[test]
fn cluster_without_faults_commits_and_serves_everything() {
    let report = run(&SimulationConfig {
        faults: FaultMix::NONE,
        chaos_duration: Duration::from_secs(20),
        ..SimulationConfig::with_seed(1)
    });
    assert!(
        report.is_clean() && report.committed_appends > 1_000,
        "{} committed appends\n{}",
        report.committed_appends,
        report.failure_summary()
    );
}

/// Tiering is only tested if the run gets as far as reading a segment from
/// the object store. A change that stops that from happening, for example
/// truncation overtaking every upload, must not pass silently.
#[test]
fn simulation_reads_segments_from_the_object_store() {
    let report = run(&SimulationConfig {
        faults: FaultMix::NONE,
        chaos_duration: Duration::from_secs(20),
        ..SimulationConfig::with_seed(1)
    });
    assert!(
        report.object_store_reads > 0,
        "no read reached the object store\n{}",
        report.failure_summary()
    );
}

#[test]
fn same_seed_replays_the_same_history() {
    let config = SimulationConfig {
        chaos_duration: Duration::from_secs(20),
        ..SimulationConfig::with_seed(7)
    };
    let first_run = run(&config);
    let second_run = run(&config);
    assert!(
        first_run == second_run
            && first_run.fingerprint() == second_run.fingerprint()
            && first_run.faults_injected > 5,
        "runs differ, or only {} faults were injected",
        first_run.faults_injected
    );
}

#[test]
fn different_seeds_produce_different_histories() {
    let short = |seed| SimulationConfig {
        chaos_duration: Duration::from_secs(10),
        ..SimulationConfig::with_seed(seed)
    };
    assert_ne!(run(&short(1)).events, run(&short(2)).events);
}

/// Each kind of fault on its own, so that a failure names its cause.
#[test]
fn each_fault_alone_holds_the_invariants() {
    let only = |faults: FaultMix| SimulationConfig {
        faults,
        chaos_duration: Duration::from_secs(30),
        ..SimulationConfig::with_seed(3)
    };
    let none = FaultMix::NONE;
    for faults in [
        FaultMix {
            node_crashes: true,
            ..none
        },
        FaultMix {
            torn_writes: true,
            ..none
        },
        FaultMix {
            partitions: true,
            ..none
        },
        FaultMix {
            slow_links: true,
            ..none
        },
        FaultMix {
            lost_responses: true,
            ..none
        },
        FaultMix {
            flush_failures: true,
            ..none
        },
        FaultMix {
            metadata_outages: true,
            ..none
        },
        FaultMix {
            writer_crashes: true,
            ..none
        },
        FaultMix {
            writer_switches: true,
            ..none
        },
        FaultMix {
            silent_corruption: true,
            ..none
        },
        FaultMix {
            disk_loss: true,
            ..none
        },
        FaultMix {
            object_store_outages: true,
            ..none
        },
    ] {
        let report = run(&only(faults));
        assert!(
            report.is_clean(),
            "{faults:?}\n{}",
            report.failure_summary()
        );
    }
}
