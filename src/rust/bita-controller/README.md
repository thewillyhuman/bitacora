# bita-controller

The controller daemon: the process that looks after a bitacora cluster.

## What it owns

- The process around `bitacora-controller`: it connects the controller to the cluster's metadata
  store, replicas and object store, and runs rounds until it is told to stop.
- Its configuration file, and the checks that refuse one that cannot work.
- The metrics of the controller: whether this instance leads, how long a pass takes, what each pass
  did, and how many segments are short of copies.

Placement, repair, tiering and retention themselves live in `bitacora-controller`. This crate decides
nothing about them.

## Running it

```sh
bita-controller --config /etc/bitacora/controller.toml
```

`example.toml` is a complete configuration with every setting explained. A test keeps it valid.

Run two or three instances with the same configuration except `identity`. One leads; the others stand
by and take over within `leadership_duration_seconds` after the leader stops. A controller holds no
state of its own, so there is nothing to lose when one is killed.

If no controller runs, existing streams keep appending and reading. Rolls continue until standby
segments run out; repair, tiering and deletion pause.

| Exit code | Meaning | What a supervisor should do |
| --- | --- | --- |
| 0 | Stopped on SIGTERM or SIGINT, or printed help or version | Nothing |
| 1 | Could not start (its metrics port is taken, the object store cannot be opened) | Restart it |
| 2 | The command line or the configuration file is wrong | Wait for a person |

The observability port serves `/metrics`, `/healthz` and `/readyz`. `docs/operations/metrics.md` lists
the metrics and says how to read them across instances.

## Design

**One round at a time.** The daemon repeats: run one round, record what it did, rest for the pass
interval. A round takes or renews the leadership and, if this instance leads, makes one pass over
every stream. Rounds never overlap.

**Ready means a round completed.** `/readyz` answers 200 once a round completed without an error, as
leader or as standby: the instance then knows that it can reach the metadata store and where it
stands. A standby is ready. A later round that fails does not take readiness away; the shutdown signal
does.

**Shutdown is bounded.** The signal is noticed while resting and while a round runs. A running round
gets ten seconds to finish and is abandoned after that. Abandoning a round is safe, because every
write of a pass is conditional on what the pass saw.

**The leadership is not given up on shutdown.** It lapses by itself, and another instance takes over
then. Leadership only avoids duplicate work; two controllers that both believe they lead cannot
corrupt anything.

**Metrics come from the controller's reports.** The library returns what each round did as plain
data, and `metrics.rs` turns that into metrics. Requests to the metadata store and to the object store
are measured by the decorators of `bitacora-process`.

**The loop runs in tests.** `run_rounds` takes the controller as an argument, so the tests run it
against the in-process cluster of `bitacora-testkit` under paused time.

## Invariants

- Nothing is opened or bound before the whole configuration was checked.
- The leadership lasts at least two pass intervals.
- A replica is given up for lost only after it has counted as dead for a while.
- After the daemon returns, nothing of it is still running.

## Files

| File | Holds |
| --- | --- |
| `src/main.rs` | The entry point and the exit codes |
| `src/config.rs` | The configuration file and its validation |
| `src/daemon.rs` | Wiring, the loop of rounds, and the shutdown |
| `src/metrics.rs` | The controller's metrics |
| `example.toml` | A complete configuration |

Each `<file>.rs` has its tests next to it in `<file>_test.rs`.

## Dependencies

It depends on `bitacora` (cluster configuration and wiring), `bitacora-process` (process plumbing and
observability), `bitacora-controller`, `bitacora-meta`, `bitacora-artifact` and `bitacora-types`.
Nothing depends on it.
