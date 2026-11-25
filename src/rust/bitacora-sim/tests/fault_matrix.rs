//! Runs the simulated fault matrix over a range of seeds.
//!
//! Continuous integration runs a fixed budget of seeds on every change. Set
//! `BITA_SIM_SEEDS` to run more, for example nightly. A seed that fails
//! prints everything needed to replay it, and is then added to
//! [`REGRESSION_SEEDS`] for good.

use std::sync::OnceLock;
use std::time::Duration;

use bitacora_sim::{
    Event, FaultMix, InjectedBug, KvOutcome, SimulationConfig, SimulationReport, Violation, run,
};
use bitacora_types::SequenceNumber;

/// How many seeds each of the four fault-matrix tests runs by default.
const DEFAULT_SEEDS_PER_TEST: u64 = 6;

/// How many seeds a deliberately broken store gets to be caught in.
const SEEDS_TO_CATCH_A_BUG: u64 = 4;

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
/// Add `--test_env=BITA_SIM_DUMP=1` to print the whole history, and
/// `--test_env=BITA_SIM_TRACE=1` to print what the cluster itself logged.
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
    if std::env::var_os("BITA_SIM_TRACE").is_some() {
        bitacora_sim::print_log_lines_to_stderr();
    }
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

/// A run of twenty seconds without any fault. Several tests look at it; it
/// is made once.
fn run_without_faults() -> &'static SimulationReport {
    static REPORT: OnceLock<SimulationReport> = OnceLock::new();
    REPORT.get_or_init(|| {
        run(&SimulationConfig {
            faults: FaultMix::NONE,
            chaos_duration: Duration::from_secs(20),
            ..SimulationConfig::with_seed(1)
        })
    })
}

/// A standard run with every kind of fault. Several tests look at it; it is
/// made once.
fn run_with_every_fault() -> &'static SimulationReport {
    static REPORT: OnceLock<SimulationReport> = OnceLock::new();
    REPORT.get_or_init(|| run(&SimulationConfig::with_seed(2)))
}

#[test]
fn cluster_without_faults_commits_and_serves_everything() {
    let report = run_without_faults();
    assert!(
        report.is_clean() && report.committed_appends > 300,
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
    let report = run_without_faults();
    assert!(
        report.object_store_reads > 0,
        "no read reached the object store\n{}",
        report.failure_summary()
    );
}

/// How many calls to the key-value store in `report` ended with an outcome
/// that `is_counted` accepts.
fn kv_outcomes(report: &SimulationReport, is_counted: fn(&KvOutcome) -> bool) -> usize {
    report
        .events
        .iter()
        .filter(|timed| match &timed.event {
            Event::KvReturned { outcome, .. } => is_counted(outcome),
            _ => false,
        })
        .count()
}

#[test]
fn run_without_faults_answers_reads_and_makes_writes() {
    let report = run_without_faults();
    let reads_answered = kv_outcomes(report, |outcome| matches!(outcome, KvOutcome::Read { .. }));
    let writes_made = kv_outcomes(report, |outcome| matches!(outcome, KvOutcome::Written));
    assert!(
        reads_answered > 300 && writes_made > 300,
        "{reads_answered} reads answered, {writes_made} writes made"
    );
}

/// A run in which the faults left the store unusable throughout would pass
/// every check and test nothing.
#[test]
fn run_with_every_fault_still_answers_reads_and_makes_writes() {
    let report = run_with_every_fault();
    let reads_answered = kv_outcomes(report, |outcome| matches!(outcome, KvOutcome::Read { .. }));
    let writes_made = kv_outcomes(report, |outcome| matches!(outcome, KvOutcome::Written));
    assert!(
        reads_answered > 300 && writes_made > 300,
        "{reads_answered} reads answered, {writes_made} writes made"
    );
}

/// The store's checkpoints are what lets the log be truncated. A run in
/// which that never happened would not have tested the store against
/// truncation at all.
#[test]
fn log_is_truncated_behind_the_store_during_a_run() {
    let report = run_with_every_fault();
    // The audit reads the stream from wherever it still starts.
    let log_start_at_the_audit = report
        .events
        .iter()
        .rev()
        .find_map(|timed| match &timed.event {
            Event::ReaderStarted { from_sequence, .. } => Some(*from_sequence),
            _ => None,
        })
        .expect("the audit reads the stream");
    assert!(
        log_start_at_the_audit > SequenceNumber::new(100),
        "the log still starts at {log_start_at_the_audit}"
    );
}

/// Whether a run of `seed` in which the compute nodes make `injected_bug`
/// produces a history that the linearizability check rejects.
fn bug_is_caught(injected_bug: InjectedBug, seed: u64) -> bool {
    let report = run(&SimulationConfig {
        injected_bug,
        chaos_duration: Duration::from_secs(30),
        ..SimulationConfig::with_seed(seed)
    });
    report
        .violations
        .iter()
        .any(|violation| matches!(violation, Violation::NotLinearizable { .. }))
}

/// A check that passes every history proves nothing. These two tests break
/// the store's use on purpose and require the simulation to notice.
#[test]
fn reads_that_skip_the_barrier_are_caught() {
    let is_caught =
        (0..SEEDS_TO_CATCH_A_BUG).any(|seed| bug_is_caught(InjectedBug::ReadsSkipTheBarrier, seed));
    assert!(is_caught, "stale reads went unnoticed");
}

#[test]
fn writes_of_unknown_outcome_reported_as_done_are_caught() {
    let is_caught = (0..SEEDS_TO_CATCH_A_BUG)
        .any(|seed| bug_is_caught(InjectedBug::UnknownWritesReportedDone, seed));
    assert!(is_caught, "lost writes went unnoticed");
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
