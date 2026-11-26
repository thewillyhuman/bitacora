# bitacora-clusterkit

A bitacora cluster of real processes on one machine, for tests.

## What it owns

- `RealCluster`: one etcd, some `bita-replica` daemons and one `bita-controller` daemon, started on
  loopback ports of its own in a temporary directory, ready when `start` returns, and gone when
  dropped.
- `ChildProcess`: a child process that dies with the value that owns it, with its output in a log file.
- `Binaries`: where the programs are, or the decision to skip the test.
- `tests/real_cluster_test.rs`: the test of the design's last testing layer (section 15, "Layer 4: real
  clusters").

## Design

**Small on purpose.** The protocol is tested in the simulator and against the in-process cluster of
`bitacora-testkit`, deterministically and under injected faults. This crate tests what those cannot:
the daemons as programs, with configuration files, sockets, signals and exit codes, against a real
etcd. One scenario is enough to show that the pieces work together as processes.

**The configurations are an operator's.** The kit writes one TOML file per daemon, the same files an
operator would write, with intervals shortened so that a test does not wait: replicas register every
second, the controller makes a pass every 200 milliseconds and presumes a replica dead after three
seconds of silence.

**The host's disk does not decide.** Replicas refuse new segments at 85 percent of used disk and
appends at 95 by default. A developer's disk is often fuller than that, so the kit sets every
threshold to 100 percent.

**The ports are the cluster's own.** A cluster claims a block of ports from `bitacora-testports` and
keeps it until it is dropped. A port that is merely free when the cluster starts can be given to
another test before the daemon has bound it, and again while a replica is down between a kill and a
restart; a port of a claimed block cannot.

**Nothing waits by sleeping.** Starting a process polls its `/readyz` (etcd: its `/health`); every wait
has an allowance and fails with the process's log when it runs out.

**Programs come from environment variables.** `BITA_ETCD_BINARY`, `BITA_REPLICA_BINARY` and
`BITA_CONTROLLER_BINARY`. The Bazel target sets them from its `data`. Under Cargo the tests pass
without running unless all three are set, and say so on standard error; under Bazel a missing one is a
failure.

## Invariants

- No process outlives the value that owns it, also when a test panics.
- Everything a cluster writes is below one temporary directory, removed after its processes.
- No other test is given a port of a cluster while the cluster exists.
- A `RealCluster` that exists was fully ready when `start` returned.
- Nothing here is used by production code.

## Files

| File | Holds |
| --- | --- |
| `src/lib.rs` | The public surface |
| `src/process.rs` | `ChildProcess` |
| `src/binaries.rs` | `Binaries` and the environment variables |
| `src/cluster.rs` | `RealCluster`: starting etcd and the daemons, killing, restarting, stopping |
| `tests/real_cluster_test.rs` | The scenario, and what the daemons do with a wrong command line |

`src/process.rs` has its tests next to it in `src/process_test.rs`.

## Dependencies

It depends on `bitacora` (the cluster configuration), `bitacora-process` (the HTTP probe) and
`bitacora-testports` (the ports). Its test also uses `bitacora-meta` to read the stream's record. The
daemons are not Rust dependencies: the test runs them as programs. Only tests depend on this crate.
