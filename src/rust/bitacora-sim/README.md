# bitacora-sim

Deterministic simulation of a whole bitacora cluster, with fault injection and a checker of what its
clients saw.

## What it owns

- **The run.** `run`: one seeded simulation of replicas, a controller, compute nodes that serve a
  key-value store, and readers, in one thread on a simulated clock, network and disk. `SimulationConfig`
  and `FaultMix` say what the run looks like.
- **The faults.** The nemesis: node crashes, torn writes, partitions, slow links, lost answers, refused
  flushes, outages of the metadata store and of the object store, crashed and replaced writers, bit rot,
  a lost disk.
- **The record.** `History`: every call a client made and what it was told, every record a reader was
  handed.
- **The verdict.** `check` and `check_audit` judge the log against the invariants of the design (I1 to
  I5); `check_linearizability` judges the key-value store. `SimulationReport` carries the violations,
  whether the healed cluster recovered, and a fingerprint of the run.
- **The seeds.** `tests/fault_matrix_test.rs`: the seeds that run on every change, and every seed that
  ever found a bug.

It owns no part of the system under test. The replicas, the client, the controller, the key-value
store and the materializer are the production crates; the cluster they run in is `bitacora-testkit`.

## Design

**Everything follows from the seed.** A run reads no system clock and no unseeded random source. The
runtime is single-threaded with a paused clock, and even its own random choices are seeded. The same
configuration therefore produces the same history on every machine, event for event, and a seed that
finds a bug finds it again as often as it takes to understand it.

**Every task has a random source of its own.** The run's source is forked once per task, in the order
the tasks are started. A task that draws one number more does not shift what the others draw.

**The order is the contract.** What a seed means is the order in which the run draws random numbers,
spawns tasks and awaits, down to the order in which the nemesis's channels are dropped when it is
stopped. The code that fixes that order (`simulation.rs`, `nemesis.rs`, `audit.rs`, the workloads) says
so in its file headers. A change there is not a refactoring: it changes every run, and a regression
seed may stop finding its bug. The fingerprint tells whether a change did.

**Chaos, then healing, then an audit.** While faults are injected the cluster may refuse work; it may
never be wrong. When the chaos ends every fault is removed, and the cluster must then recover within a
bounded time: commit a final record, serve every key and the whole stream back, and bring every sealed
segment to a full set of good copies or to the object store. Safety is required always, liveness only
once the faults are gone.

**Faults stay within what the design promises to survive.** Bit rot and the lost disk destroy data
that was acknowledged, so they always hit the same node of a run, and a disk is lost at most once: no
record ever loses more than one of its copies to the nemesis.

**The judge sees only what clients saw.** The checkers read the history and nothing else. They never
ask a replica or the metadata store what happened, so a bug that makes the system agree on a wrong
answer cannot hide.

**One rule per file.** The log checker is one pass over the history plus one file per invariant in
`src/checker/`. Each file states its rule in words before it states it in code.

**A check that passes everything proves nothing.** `InjectedBug` makes the compute nodes misuse the
store on purpose, and tests require the linearizability check to notice. Other tests require that a run
made progress, read from the object store and had its log truncated: a run in which nothing happened
would pass every check.

**The workload is a real application.** The compute nodes run bitacora-kv on the stream, so one run
exercises the log, the materializer runner, checkpoints, truncation, tiering and writer failover
together, and both the log and the store are judged.

## Invariants

- The same `SimulationConfig` produces the same `SimulationReport`.
- A run is clean only if no invariant was broken at any time and the healed cluster recovered.
- The names of `Event`, `AppendOutcome`, `KvCall`, `KvOutcome` and `TimedEvent`, of their variants and
  fields, and the wording of the fault descriptions are part of the fingerprint. They do not change.
- The standard run of `SimulationConfig::with_seed` and every timing constant of the crate are what the
  regression seeds were found with.
- Data that was flushed is destroyed on one node per run at most.
- The checkers allocate nothing per event beyond what their maps need, and copy no payload.
- A seed that found a bug is never removed from `REGRESSION_SEEDS`.

## Running it

The fault matrix is the Bazel target `//src/rust/bitacora-sim:fault_matrix_test`. Its test binary reads
four environment variables.

**Run the default budget**, as continuous integration does (24 seeds of the standard run, the
regression seeds, each fault alone, and the simulator's tests of itself):

```sh
bazel test //src/rust/bitacora-sim:fault_matrix_test
```

**Run more seeds.** `BITA_SIM_SEEDS` is the number of seeds, split over four tests that run in
parallel. Give the test more time:

```sh
bazel test //src/rust/bitacora-sim:fault_matrix_test \
    --test_env=BITA_SIM_SEEDS=2000 --test_timeout=3600
```

Or run the built binary directly, with only the tests that depend on the budget:

```sh
bazel build //src/rust/bitacora-sim:fault_matrix_test
BITA_SIM_SEEDS=240 bazel-bin/src/rust/bitacora-sim/fault_matrix_test \
    fault_matrix_holds each_fault --test-threads=4
```

**Replay one seed.** A failing run prints its seed. `BITA_SIM_SEED` makes the test
`replays_the_seed_named_in_the_environment` run that seed alone and print one line with its counts and
its fingerprint:

```sh
bazel test //src/rust/bitacora-sim:fault_matrix_test \
    --test_env=BITA_SIM_SEED=179 --test_arg=replays_the_seed --test_output=all
```

```sh
BITA_SIM_SEED=179 bazel-bin/src/rust/bitacora-sim/fault_matrix_test \
    replays_the_seed --exact replays_the_seed_named_in_the_environment --nocapture
```

```text
seed 179: 353 committed appends, 86 faults, 4387 events, fingerprint 8754a5cd75ff8e96
```

**Dump its history.** Add `BITA_SIM_DUMP=1` to print every event of the replayed seed with its
simulated time, one per line:

```sh
BITA_SIM_SEED=179 BITA_SIM_DUMP=1 bazel-bin/src/rust/bitacora-sim/fault_matrix_test \
    replays_the_seed --exact replays_the_seed_named_in_the_environment --nocapture
```

**Print the cluster's log lines.** Add `BITA_SIM_TRACE=1` to print, on standard error, what the
replicas, the writers and the controller logged during the replay, stamped with the same simulated time
as the history, so that the two can be read side by side:

```sh
BITA_SIM_SEED=179 BITA_SIM_DUMP=1 BITA_SIM_TRACE=1 \
    bazel-bin/src/rust/bitacora-sim/fault_matrix_test \
    replays_the_seed --exact replays_the_seed_named_in_the_environment --nocapture
```

**Tell whether a change altered behaviour.** Record the fingerprint line of a few seeds before the
change and compare after. Equal fingerprints mean the same events at the same simulated instants. A
change that is meant to alter behaviour changes them; a refactoring must not.

**When a seed fails.** Replay it, read the summary (the violations and the last forty events), dump
the history and the log lines if that is not enough, fix the bug, and add the seed to `REGRESSION_SEEDS`
in `tests/fault_matrix_test.rs` with a note of what it found.

## Files

| File | Holds |
| --- | --- |
| `src/lib.rs` | The crate's rationale and its public names |
| `src/config.rs` | `SimulationConfig`, `FaultMix` and the kinds of fault |
| `src/simulation.rs` | `run`, and the phases of one run from seed to report |
| `src/world.rs` | What the tasks of a run share: the cluster, the history, how clients are built |
| `src/background.rs` | What the cluster does on its own: registration, the controller, the supervisor, scrubbing |
| `src/kv_workload.rs` | The compute nodes that serve the key-value store, and `InjectedBug` |
| `src/reader_workload.rs` | The readers that follow the stream |
| `src/nemesis.rs` | The loop that draws and injects faults |
| `src/nemesis/process.rs` | Crashes, plain and torn |
| `src/nemesis/disk.rs` | A refused flush, bit rot, a lost disk |
| `src/nemesis/link.rs` | Partitions, slow links, lost answers |
| `src/nemesis/stores.rs` | Outages of the metadata store and of the object store |
| `src/nemesis/writer.rs` | A crashed writer, a replaced writer |
| `src/audit.rs` | Healing, and the audit of the healed cluster |
| `src/diagnosis.rs` | What a run says when its cluster does not recover |
| `src/history.rs` | `History` and the events it is made of |
| `src/checker.rs` | `Violation`, `check`, and the pass over the history |
| `src/checker/observations.rs` | What that pass collects for the rules |
| `src/checker/durability.rs` | I1: committed records stay; `check_audit` |
| `src/checker/uniqueness.rs` | I2: readers agree |
| `src/checker/prefix.rs` | I3: each reader sees a gapless prefix in order |
| `src/checker/single_writer.rs` | I4: one writer commits at a time |
| `src/checker/provenance.rs` | The stream holds what was appended, once |
| `src/linearizability.rs` | `check_linearizability`: the calls to the key-value store |
| `src/report.rs` | `SimulationReport`, its fingerprint, and the verdict of a run |
| `src/trace.rs` | `print_log_lines_to_stderr` |
| `src/test_support.rs` | Fixtures shared by the unit tests; compiled for tests only |
| `src/<file>_test.rs` | The unit tests of the file of the same name |
| `tests/fault_matrix_test.rs` | The fault matrix, the regression seeds, the replay, and the tests of the simulator itself |

The files in `src/nemesis/` are child modules of `nemesis.rs`, and those in `src/checker/` of
`checker.rs`. Safe truncation (I5) has no file: the audit and the materializer record the fact as an
event, and `checker.rs` turns each such event into a violation.

## Dependencies

Of the bitacora crates it depends on `bitacora-testkit` (the cluster), `bitacora-client`,
`bitacora-controller`, `bitacora-kv` and `bitacora-materializer` (what runs in the cluster),
`bitacora-meta`, `bitacora-artifact` and `bitacora-segment` (records, the object store, the disk) and
`bitacora-types`. It sits at the top of the layer order: nothing depends on it, and it is never linked
into a shipped binary. It must not depend on `bitacora-proto`, on a daemon, or on anything that opens a
socket or a file: a run that touches the outside world is no longer a function of its seed.
