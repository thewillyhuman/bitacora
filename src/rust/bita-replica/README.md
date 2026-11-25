# bita-replica

The replica daemon: one storage node of a bitacora cluster.

## What it owns

- The process around `bitacora-replica`: it opens the data directory, serves the replica over gRPC,
  and runs until it is told to stop.
- The replica's registration in the metadata store and its renewal.
- The schedule on which sealed segments are scrubbed.
- Its configuration file, and the checks that refuse one that cannot work.
- The metrics a replica reports about itself (copies held, damaged copies, drain state).

Everything about segments, epochs and the protocol lives in `bitacora-replica`. This crate decides
nothing about them.

## Running it

```sh
bita-replica --config /etc/bitacora/replica.toml
```

`example.toml` is a complete configuration with every setting explained. A test keeps it valid.

```sh
bita-replica --config /etc/bitacora/replica.toml --check-config
```

checks a configuration without starting: it reads and validates the file exactly as a start does, opens
nothing, and exits with 0 if the replica would start with it and with 2 if not. Use it before a restart
and wherever the file is generated.

One process serves one data directory. Deploy three or more across failure domains and give each a
different `replica_id`, `data_directory` and `advertised_address`; the `[cluster]` table is the same
for every process of a cluster.

| Exit code | Meaning | What a supervisor should do |
| --- | --- | --- |
| 0 | Stopped on SIGTERM or SIGINT, printed help or version, or checked a valid configuration | Nothing |
| 1 | Could not start, or had to stop (the disk refused a write, a port is taken) | Restart it |
| 2 | The command line or the configuration file is wrong | Wait for a person |

The observability port serves `/metrics`, `/healthz` and `/readyz`. `docs/operations/metrics.md` lists
the metrics.

## Design

**Ready means registered.** `/readyz` answers 200 once the first registration reached the metadata
store: from then on writers and the controller can find the replica. It answers 503 again as soon as
the replica starts to shut down. If etcd is down when the replica starts, the replica serves what it
has to clients that already know it, keeps trying to register, and is not ready until it succeeds. A
replica that loses etcd later stays ready.

**The registration survives a clean shutdown.** A replica that restarts for an upgrade comes back
with the same identity and the same data. Removing its registration on the way out would tell the
controller that its copies are gone and start re-replicating them. A replica that is gone for good is
removed by an operator, or given up by the controller after it has been silent for long.

**A refused write stops the process.** After a failed flush the durable content of a file is unknown,
so the replica must not go on acknowledging appends. The daemon exits with code 1; the restart rescans
the data directory, and the controller repairs what is short.

**The identity belongs to the data directory.** `replica_id` in the configuration is only a proposal
for an empty directory. A directory that was used before keeps the identity it has, and that is the
one the daemon serves and registers under.

**Shutdown is ordered and bounded.** Stop the loops, report not ready, stop the gRPC server (requests
in flight get ten seconds), stop the replica, stop the endpoint.

**The whole daemon runs in tests.** `daemon::run` takes its disk, metadata store, object store and
sockets as arguments, so the tests run it with in-memory stores on loopback ports. What only a real
process shows is covered by the test of a cluster of real processes.

## Invariants

- Nothing is opened, bound or written before the whole configuration was checked.
- The gRPC server answers under the identity of the data directory, which is also the registered one.
- After the daemon returns, nothing of it is still running.
- Scrubbing only reads.

## Files

| File | Holds |
| --- | --- |
| `src/main.rs` | The entry point and the exit codes |
| `src/config.rs` | The configuration file and its validation |
| `src/daemon.rs` | Wiring, the wait for a reason to stop, and the shutdown |
| `src/registration.rs` | The registration and its renewal loop |
| `src/scrub.rs` | The scrub schedule |
| `src/metrics.rs` | The daemon's own metrics |
| `src/test_support.rs` | What the tests share |
| `example.toml` | A complete configuration |

Each `<file>.rs` has its tests next to it in `<file>_test.rs`.

## Dependencies

It depends on `bitacora` (cluster configuration and wiring), `bitacora-process` (process plumbing and
observability), `bitacora-proto` (the gRPC server), `bitacora-replica`, `bitacora-segment` (the local
disk), `bitacora-meta`, `bitacora-artifact` and `bitacora-types`. Nothing depends on it.
