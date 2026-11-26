# bita-soak

The soak tool: it drives a bitacora cluster for hours or days, round after round, and judges what its
clients saw with the checker of the deterministic simulator. It is what stands behind the exit gate of
the hardening milestone (design, section 18): a week of nightly runs on a real cluster with no invariant
violation. The user's reference is `docs/soak.md`.

## What it owns

- The round: fresh streams, a workload, faults, healing, the audit, the verdict, and the clean-up.
- The workload: one paced writer and one following reader per stream, recording everything they are told
  into a `bitacora_sim::History`.
- The nemesis: which fault comes next, on which target, and the check that a network cut is real.
- The fault interface, `FaultInjector`, and the rules about what may be hit.
- The lines of JSON it prints, its exit codes and its metrics.

It owns no invariant. What is correct is decided by `bitacora-sim`, and this tool hands it histories.

## Design

A run is cut into rounds because a history that covered a week would need the memory of a week. Each
round is a whole experiment, like one seed of the simulator: it creates its streams, drives them, lets the
cluster heal, reads every stream back from its low-water mark to its end, and judges. A clean round
deletes its streams and drops its histories; nothing of it reaches the next round but the totals.

The checker judges one stream per history, so every stream has a history of its own. The event types are
the simulator's and are not changed here: they are part of the simulator's fingerprint.

A writer has one append in flight and reopens its stream by force when it loses it, which is what an
application's failover does. The outcome of an append is recorded as the client reported it: committed
with its sequence number and epoch; unknown if the writer was fenced or lost contact; refused if the
record never got a sequence number. Unknown is never rounded to either side. The audit begins with a new
writer taking the stream over and appending one last record: the takeover settles every unknown append,
and the last record tells the audit's reader where the stream ends.

The nemesis injects at most one fault at a time, and none while the cluster is not healthy. A cut counts
only once the tool has seen it work: the replicas behind it must stop renewing their registration in the
metadata store (`cut_check.rs`). A cluster's management can accept a cut that its network never enforces,
and a week of clean rounds must not be read as a week of survived partitions. A cut that is not in effect
is removed, recorded nowhere, and its kind is given up for the rest of the run; every line the tool prints
from then on names the kind and the reason.

A round that cannot be audited is not clean. If its streams cannot be created, the cluster does not become
healthy, or a stream cannot be read back within the healing time, the round is `not_audited` and the run
ends as `not_recovered`: nothing is known about the invariants, and saying so is the only honest report.

Time is read through Tokio's clock and the cluster's clock only, so whole rounds are tested against the
in-process test cluster under a virtual clock, with a controller, renewed registrations and every kind of
fault (`test_support.rs`).

## Invariants

- A round is reported as clean only if every one of its streams was read back whole and the checker found
  nothing in any of them.
- A fault is counted and recorded only once it is known to have happened.
- No history outlives its round. The memory of a run is the memory of one round (`docs/soak.md` has the
  arithmetic).
- Standard output holds the lines of the rounds and of the run and nothing else.
- Exit codes: 0 every round was clean and the time is up, 1 the run could not start, 2 the command line is
  wrong, 3 a round broke an invariant, 4 the cluster did not recover, 5 the tool was told to stop.

## Files

| File | Holds |
| --- | --- |
| `src/main.rs` | The entry point: parse, connect, run, exit code |
| `src/arguments.rs` | The command line, as `clap` types |
| `src/cluster.rs` | What a run holds of the cluster: client, metadata store, clock, fault injector |
| `src/soak.rs` | The run: round after round, the lines, the summary |
| `src/round.rs` | One round, step by step |
| `src/workload.rs` | The writer and the reader of a stream |
| `src/nemesis.rs` | The loop that injects faults during a round |
| `src/faults.rs` | The faults and the interface that makes them happen |
| `src/targets.rs` | What a fault may hit, as pure rules |
| `src/cut_check.rs` | Whether a network cut is real |
| `src/health.rs` | When the cluster is whole again |
| `src/audit.rs` | Reading a stream back whole |
| `src/judge.rs` | The checker's verdict and the dump of a history |
| `src/report.rs` | The lines of JSON and the counts in them |
| `src/metrics.rs` | The same counts as metrics |
| `src/test_support.rs` | The in-process cluster and its faults, for the tests |

Each file's tests are next to it, in `<file>_test.rs`.

## Dependencies

`bitacora` for the client, `bitacora-sim` for the history and the checker, `bitacora-meta` and
`bitacora-types` for the registry of replicas and the clock, `bitacora-process` for the metrics endpoint
and the signals. The tests add `bitacora-testkit`, `bitacora-controller`, `bitacora-replica` and
`bitacora-segment`. Nothing depends on this crate, and nothing must.

## Not done

- Faults on a real cluster. This crate defines the interface and tests it against the in-process cluster;
  the program injects none yet.
- Two faults at once. The first version proves the simple case for a week before it tries the hard one.
- Losing a replica's disk. The replica then comes back under a new identity, so the rule for "healthy"
  (every replica the run started with is alive) would have to follow identities across a round, wait for
  the controller to rebuild the lost copies, and remove the dead identity from the registry.
- Reading from the object store. The tool connects without one. Its streams live for one round, which is
  far shorter than the window during which replicas keep a segment that was moved to the object store.
