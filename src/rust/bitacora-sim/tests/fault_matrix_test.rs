//! Runs the simulated fault matrix over a range of seeds, and tests the simulator itself.
//!
//! # What is covered
//!
//! - The fault matrix: the standard run, with every kind of fault, holds every invariant for a budget of seeds, and for
//!   each kind of fault on its own.
//! - Regression: every seed that once found a bug still passes.
//! - Replay: one seed named in the environment, with its history and the cluster's log lines on request.
//! - The simulator tests something: a run without faults makes progress and reads from the object store, a run with
//!   every fault still answers reads and makes writes, and the log is truncated behind the store.
//! - The checks catch what they are for: a store used wrongly on purpose is rejected.
//! - Determinism: one seed gives one history; two seeds give two.
//!
//! # How the fixtures work
//!
//! Continuous integration runs a fixed budget of seeds on every change, split over four tests so that they run in
//! parallel. The environment widens or narrows what runs:
//!
//! - `BITA_SIM_SEEDS=N` runs `N` seeds (rounded up to a multiple of four) instead of the default budget, in the four
//!   `fault_matrix_holds_the_invariants_slice_*` tests. Use it nightly, or before a risky change.
//! - `BITA_SIM_SEED=N` makes `replays_the_seed_named_in_the_environment` run seed `N` and print its fingerprint.
//!   Without it that test does nothing.
//! - `BITA_SIM_DUMP=1`, with `BITA_SIM_SEED`, prints the whole history of the replayed seed.
//! - `BITA_SIM_TRACE=1`, with `BITA_SIM_SEED`, prints what the cluster itself logged during the replay.
//!
//! A seed that fails prints everything needed to replay it, and is then added to [`REGRESSION_SEEDS`] for good, with
//! the bug it found.
//!
//! Runs that several tests look at are made once and shared: a run costs seconds, and the tests of one binary run in
//! parallel.

use std::sync::OnceLock;
use std::time::Duration;

use bitacora_sim::{Event, FaultMix, InjectedBug, KvOutcome, SimulationConfig, SimulationReport, Violation, run};
use bitacora_types::SequenceNumber;

/// How many seeds each of the four fault-matrix tests runs by default.
const DEFAULT_SEEDS_PER_TEST: u64 = 6;

/// How many tests the budget of seeds is split over, so that they run in parallel.
const SLICE_COUNT: u64 = 4;

/// How many seeds a deliberately broken store gets to be caught in.
const SEEDS_TO_CATCH_A_BUG: u64 = 4;

/// How many appends, answered reads and made writes a run must at least have for the test of it to mean something. A
/// healthy run has several times as many.
const ENOUGH_TO_MEAN_SOMETHING: usize = 300;

/// Seeds that once found a bug. Each stays here forever, with the bug it found.
const REGRESSION_SEEDS: &[u64] = &[
    // Bit rot in an open segment made a replica restart with a shorter copy that it took for a torn tail; recovery
    // sealed below committed records. Fixed by the flushed mark in the segment header.
    13,
    // A node rebuilt with an empty disk answered a fence in the name of its old identity. Fixed by connecting to
    // replicas by identity.
    67,
    // After retention deleted every segment, the next one reused the identifier 1 and collided with copies not yet
    // swept from replicas.
    74, 215,
    // A repair in place threw the damaged copy away before it had fetched the replacement. The fetch failed, and
    // records that were flushed and acknowledged on that replica were gone; bit rot on the only other copy then lost a
    // committed segment.
    179,
];

/// Reads how many seeds each fault-matrix test runs: a quarter of `BITA_SIM_SEEDS`, rounded up, or the default if the
/// variable is not set or is not a number.
fn seeds_per_test() -> u64 {
    let Ok(budget) = std::env::var("BITA_SIM_SEEDS") else {
        return DEFAULT_SEEDS_PER_TEST;
    };
    match budget.parse::<u64>() {
        Ok(seed_count) => seed_count.div_ceil(SLICE_COUNT),
        Err(_not_a_number) => DEFAULT_SEEDS_PER_TEST,
    }
}

/// Fails the test, with everything needed to replay the run, unless `report` is clean.
fn assert_clean(report: &SimulationReport) {
    assert!(report.is_clean(), "{}", report.failure_summary());
}

/// Runs every seed of the budget that falls in `slice` of four, so that the four tests below share the work and run in
/// parallel.
fn run_fault_matrix_slice(slice: u64) {
    for seed in 0..seeds_per_test() * SLICE_COUNT {
        if seed % SLICE_COUNT == slice {
            assert_clean(&run(&SimulationConfig::with_seed(seed)));
        }
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

/// Replays the single seed named by `BITA_SIM_SEED`, to reproduce a failure. Add `--test_env=BITA_SIM_DUMP=1` to print
/// the whole history, and `--test_env=BITA_SIM_TRACE=1` to print what the cluster itself logged.
///
/// ```sh
/// bazel test //src/rust/bitacora-sim:fault_matrix_test \
///     --test_env=BITA_SIM_SEED=1234 --test_arg=replays_the_seed --test_output=all
/// ```
#[test]
fn replays_the_seed_named_in_the_environment() {
    let Some(seed) = seed_named_in_the_environment() else {
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
    // With BITA_SIM_DUMP set, the whole history is printed; run the test with --nocapture (or --test_output=all) to see
    // it.
    if std::env::var_os("BITA_SIM_DUMP").is_some() {
        for timed in &report.events {
            println!("{:>10.3?} {:?}", timed.at, timed.event);
        }
    }
    assert_clean(&report);
}

/// Reads the seed to replay from `BITA_SIM_SEED`. `None` if the variable is not set or is not a number.
fn seed_named_in_the_environment() -> Option<u64> {
    let seed = std::env::var("BITA_SIM_SEED").ok()?;
    seed.parse::<u64>().ok()
}

/// A run of twenty seconds without any fault. Several tests look at it; it is made once.
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

/// A standard run with every kind of fault. Several tests look at it; it is made once.
fn run_with_every_fault() -> &'static SimulationReport {
    static REPORT: OnceLock<SimulationReport> = OnceLock::new();
    REPORT.get_or_init(|| run(&SimulationConfig::with_seed(2)))
}

#[test]
fn cluster_without_faults_commits_and_serves_everything() {
    let report = run_without_faults();

    assert_clean(report);
}

#[test]
fn cluster_without_faults_commits_many_appends() {
    let report = run_without_faults();

    assert!(
        report.committed_appends > ENOUGH_TO_MEAN_SOMETHING,
        "{} committed appends",
        report.committed_appends
    );
}

/// Tiering is only tested if the run gets as far as reading a segment from the object store. A change that stops that
/// from happening, for example truncation overtaking every upload, must not pass silently.
#[test]
fn simulation_reads_segments_from_the_object_store() {
    let report = run_without_faults();

    assert!(
        report.tiered_segment_reads > 0,
        "no segment was read from the object store\n{}",
        report.failure_summary()
    );
}

/// Counts the calls to the key-value store in `report` that ended with an outcome `is_counted` accepts.
fn count_kv_outcomes(report: &SimulationReport, is_counted: fn(&KvOutcome) -> bool) -> usize {
    let mut count = 0;
    for timed in &report.events {
        if let Event::KvReturned { outcome, .. } = &timed.event
            && is_counted(outcome)
        {
            count += 1;
        }
    }
    count
}

/// Says whether `outcome` is a read that was answered.
fn is_answered_read(outcome: &KvOutcome) -> bool {
    matches!(outcome, KvOutcome::Read { .. })
}

/// Says whether `outcome` is a write that was reported as done.
fn is_made_write(outcome: &KvOutcome) -> bool {
    matches!(outcome, KvOutcome::Written)
}

#[test]
fn run_without_faults_answers_reads() {
    let reads_answered = count_kv_outcomes(run_without_faults(), is_answered_read);

    assert!(
        reads_answered > ENOUGH_TO_MEAN_SOMETHING,
        "{reads_answered} reads answered"
    );
}

#[test]
fn run_without_faults_makes_writes() {
    let writes_made = count_kv_outcomes(run_without_faults(), is_made_write);

    assert!(writes_made > ENOUGH_TO_MEAN_SOMETHING, "{writes_made} writes made");
}

/// A run in which the faults left the store unusable throughout would pass every check and test nothing.
#[test]
fn run_with_every_fault_still_answers_reads() {
    let reads_answered = count_kv_outcomes(run_with_every_fault(), is_answered_read);

    assert!(
        reads_answered > ENOUGH_TO_MEAN_SOMETHING,
        "{reads_answered} reads answered"
    );
}

/// See `run_with_every_fault_still_answers_reads`: the same, for writes.
#[test]
fn run_with_every_fault_still_makes_writes() {
    let writes_made = count_kv_outcomes(run_with_every_fault(), is_made_write);

    assert!(writes_made > ENOUGH_TO_MEAN_SOMETHING, "{writes_made} writes made");
}

/// The store's checkpoints are what lets the log be truncated. A run in which that never happened would not have tested
/// the store against truncation at all.
#[test]
fn log_is_truncated_behind_the_store_during_a_run() {
    let report = run_with_every_fault();

    let log_start_at_the_audit = start_of_the_last_reading_session(report).expect("the audit reads the stream");

    assert!(
        log_start_at_the_audit > SequenceNumber::new(100),
        "the log still starts at {log_start_at_the_audit}"
    );
}

/// Finds where the last reading session of `report` started. That session is the audit's, which reads the stream from
/// wherever it still starts.
fn start_of_the_last_reading_session(report: &SimulationReport) -> Option<SequenceNumber> {
    for timed in report.events.iter().rev() {
        if let Event::ReaderStarted { from_sequence, .. } = &timed.event {
            return Some(*from_sequence);
        }
    }
    None
}

/// Says whether a run of `seed` in which the compute nodes make `injected_bug` produces a history that the
/// linearizability check rejects.
fn is_bug_caught(injected_bug: InjectedBug, seed: u64) -> bool {
    let report = run(&SimulationConfig {
        injected_bug,
        chaos_duration: Duration::from_secs(30),
        ..SimulationConfig::with_seed(seed)
    });
    for violation in &report.violations {
        if matches!(violation, Violation::NotLinearizable { .. }) {
            return true;
        }
    }
    false
}

/// Says whether any of the first [`SEEDS_TO_CATCH_A_BUG`] seeds catches `injected_bug`. Stops at the first seed that
/// does.
fn is_bug_caught_within_a_few_seeds(injected_bug: InjectedBug) -> bool {
    for seed in 0..SEEDS_TO_CATCH_A_BUG {
        if is_bug_caught(injected_bug, seed) {
            return true;
        }
    }
    false
}

/// A check that passes every history proves nothing. This test and the next break the store's use on purpose and
/// require the simulation to notice.
#[test]
fn reads_that_skip_the_barrier_are_caught() {
    let is_caught = is_bug_caught_within_a_few_seeds(InjectedBug::ReadsSkipTheBarrier);

    assert!(is_caught, "stale reads went unnoticed");
}

#[test]
fn writes_of_unknown_outcome_reported_as_done_are_caught() {
    let is_caught = is_bug_caught_within_a_few_seeds(InjectedBug::UnknownWritesReportedDone);

    assert!(is_caught, "lost writes went unnoticed");
}

/// Two runs of one configuration.
struct TwoRuns {
    /// The first run.
    first: SimulationReport,
    /// The second run, of the same configuration.
    second: SimulationReport,
}

/// Seed 7, run twice for twenty seconds of faults. Two tests look at the pair; it is made once.
fn seed_7_run_twice() -> &'static TwoRuns {
    static RUNS: OnceLock<TwoRuns> = OnceLock::new();
    RUNS.get_or_init(|| {
        let config = SimulationConfig {
            chaos_duration: Duration::from_secs(20),
            ..SimulationConfig::with_seed(7)
        };
        TwoRuns {
            first: run(&config),
            second: run(&config),
        }
    })
}

#[test]
fn same_seed_replays_the_same_history() {
    let runs = seed_7_run_twice();

    assert_eq!(runs.first.fingerprint(), runs.second.fingerprint());
    assert!(
        runs.first == runs.second,
        "the reports differ although the fingerprints do not"
    );
}

/// Two runs without faults would be equal for trivial reasons. The replayed run must have had faults in it.
#[test]
fn replayed_run_had_faults_in_it() {
    let runs = seed_7_run_twice();

    assert!(
        runs.first.faults_injected > 5,
        "only {} faults were injected",
        runs.first.faults_injected
    );
}

#[test]
fn different_seeds_produce_different_histories() {
    let short_run_of = |seed| SimulationConfig {
        chaos_duration: Duration::from_secs(10),
        ..SimulationConfig::with_seed(seed)
    };

    let first_history = run(&short_run_of(1)).events;
    let second_history = run(&short_run_of(2)).events;

    assert_ne!(first_history, second_history);
}

/// Every kind of fault on its own, in the order of the fields of `FaultMix`.
fn each_fault_alone() -> [FaultMix; 12] {
    let none = FaultMix::NONE;
    [
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
    ]
}

/// Each kind of fault on its own, so that a failure names its cause.
#[test]
fn each_fault_alone_holds_the_invariants() {
    for faults in each_fault_alone() {
        let report = run(&SimulationConfig {
            faults,
            chaos_duration: Duration::from_secs(30),
            ..SimulationConfig::with_seed(3)
        });

        assert!(report.is_clean(), "{faults:?}\n{}", report.failure_summary());
    }
}
