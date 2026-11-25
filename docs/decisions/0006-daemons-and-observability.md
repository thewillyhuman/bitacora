# 0006. Daemons share one process crate, and are observed at their injected interfaces

Status: accepted · 2026-11-25

## Context

A bitacora cluster is a few kinds of long-running process, and "every process
exposes Prometheus metrics, OpenTelemetry traces and structured JSON logs"
(section 14, "Observability"). The design names the signals that matter:
append latency, replica flush latency, replica lag, segments below N good
copies, failover duration, materializer lag, and the latency and availability
of the metadata store.

Two things constrain how this is built.

The library crates run in the deterministic simulator. They touch the clock,
the disk and the network only through injected interfaces, and that discipline
is what makes a seed replay a run exactly. A metrics library inside them would
be one more thing to keep deterministic, in every crate.

Each daemon needs the same plumbing around its real work: a configuration
file, logs, a reaction to SIGTERM, an HTTP endpoint for scraping and probing.
Written per daemon, it drifts.

## Decision

### One crate for what every daemon does

`src/rust/bitacora-process` holds the plumbing: the command line
(`--config <path>`), the configuration file loader (TOML), the log output, the
shutdown listener, the metrics registry and the HTTP endpoint. A daemon's
`main` is a short script over it. The crate knows nothing about segments or
epochs, and it is the only crate that depends on `prometheus-client`,
`tracing-subscriber` and `axum`.

The command line is parsed by hand. A daemon takes one argument, and an
argument parsing library for that would be the largest dependency of the
binary. The `bita` command-line tool, which has real subcommands, is another
matter.

Configuration files are TOML, read into types that refuse unknown fields. A
misspelled setting stops the daemon at start with the file, the line and the
field in the message, instead of silently keeping a default.

### Metrics are decorators at the injected interfaces

The library crates stay free of metrics. Every interface they reach the
outside world through gets a decorator in `bitacora-process` that forwards
each call unchanged and records it:

| Interface | Decorator | Signal of the design it gives |
| --- | --- | --- |
| `bitacora_segment::Disk` | `InstrumentedDisk` | Replica flush latency |
| `bitacora_types::replica_api::ReplicaApi` | `InstrumentedReplica` | What a replica is asked, how long it takes, how it refuses |
| `bitacora_meta::KeyValueStore` | `InstrumentedKeyValueStore` | Metadata store latency and availability |
| `bitacora_artifact::ArtifactStore` | `InstrumentedArtifactStore` | Object store latency and failures |

A daemon wraps each interface before handing it to the component it runs.
With or without the decorators the system behaves the same, which a test of
each decorator checks: results come back unchanged.

What a decorator cannot see, the component reports as plain data. The
controller returns what each round did (`Round`, `PassReport`), and its daemon
turns that into metrics, including the design's "segments below N good
copies".

### One naming scheme, and a reference that is checked

Every metric is `bitacora_<subsystem>_<what>`, with base units in the name
(`_seconds`, `_bytes`) and `_total` on counters. The three request-shaped
subsystems (`replica`, `metadata_store`, `object_store`) expose the same three
families: `_requests_total{operation}`,
`_request_duration_seconds{operation}` and
`_request_failures_total{operation,error}`. One dashboard panel and one alert
rule therefore fit all three.

Labels are small closed sets known at compile time: an operation, an error
kind, a direction. A stream name, a key or an address is never a label, so the
number of time series of a process is bounded whatever its clients do. The
price is that there is no per-stream metric.

Durations are classic histograms with one set of buckets, 100 microseconds to
about 105 seconds, doubling. One set is coarse for any single use, and it
means every duration in the system can be compared and aggregated.

`docs/operations/metrics.md` lists every metric. Names are an interface for
dashboards and alerts, so the test of each crate that registers metrics fails
when it exposes one the reference does not list.

### Liveness and readiness are separate

The endpoint serves `/metrics`, `/healthz` and `/readyz` on its own port.
`/healthz` answers as long as the process runs. `/readyz` follows a flag the
daemon sets when it can do its work and clears when the shutdown signal
arrives, so that a process that is starting or stopping is left out, not
restarted.

### The replica daemon

`bita-replica` (`src/rust/bita-replica`) is the process around the replica
library. Four decisions shape it.

Ready means registered. `/readyz` answers 200 once the first registration
reached the metadata store, because only then can writers and the controller
find the replica. If etcd is down at start, the replica serves the clients
that already know it and is not ready until it registers. A replica that
loses etcd later stays ready: it still serves what it has. Readiness goes
away for good when the shutdown signal arrives.

The registration survives a clean shutdown. A restart for an upgrade brings
the replica back with the same identity and the same data. Removing the
registration on the way out would tell the controller that the copies are
gone and start re-replicating them, which is exactly what a rolling upgrade
must not cause. The registry already has the two ways a replica leaves it:
an operator removes it, or the controller gives up its copies after it has
been silent for `replica_lost_after`.

A refused write or flush stops the process, with exit code 1. After a failed
flush the durable content of the file is unknown, and a retry can falsely
succeed (section 7). The restart rescans the data directory and the
controller repairs what is short.

The exit code tells a supervisor what to do: 0 for a stop that was asked for,
1 for a failure worth a restart, 2 for a command line or configuration file
that only a person can fix.

The daemon's `run` takes its disk, stores and sockets as arguments. That is
what lets the whole daemon run inside a test, on in-memory stores and
loopback ports, without etcd.

### The controller daemon

`bita-controller` (`src/rust/bita-controller`) runs the controller library's
rounds in a loop: one round, record what it did, rest for the pass interval.

Ready means a round completed without an error, as leader or as standby. The
instance then knows that it can reach the metadata store and where it stands.
A standby is ready on purpose: readiness says "this instance can do its job",
and standing by is the job of all instances but one. A later round that fails
does not take readiness away.

Shutdown is bounded. The signal is noticed while resting and while a round
runs; a running round gets ten seconds to finish and is abandoned after that.
Abandoning is safe because every write of a pass is conditional on what the
pass saw, which is the same property that makes two leaders harmless. A pass
can legitimately take much longer than a supervisor waits (a repair may run
for half an hour), so "finish the round first" was not an option.

The leadership is not given up on shutdown. It lapses after the leadership
duration and another instance takes over then. Giving it up explicitly would
make a planned failover faster by that duration; nothing needs that yet.

The configuration refuses two combinations that would work badly without
failing: a leadership shorter than two pass intervals, and a
`replica_lost_after` that is not longer than the liveness window.

The controller's metrics have one rule worth recording. Only the leader knows
how many segments miss copies, so a standby reports zero, and an instance
whose round failed keeps the last number it saw: a metadata store that is
briefly away must not make durability look fine. An instance whose round
failed reports that it is not leading, so "no instance leads" is one alert
for a cluster that nobody looks after, whatever the cause.

## What is not implemented

- **Traces.** No daemon emits OpenTelemetry traces. Log events carry the
  stream, the segment and the replica as fields, which is what is available to
  follow a request today.
- **TLS** on the endpoint, as on the gRPC transport.
- **Append latency per stream, replica lag and failover duration.** These are
  only known in the client library, which runs inside the application and has
  no metrics. The application measures its own appends, and `bita-bench` does
  so for benchmarks. A metrics hook in the client library is the obvious next
  step and needs its own decision, because that crate runs in the simulator.
- **Materializer lag and checkpoint age.** They belong to the materializer
  runner, which reports its lag as plain data; the process that hosts a runner
  will export it the way the controller daemon exports its rounds.
- **Per-stream metrics**, by the labelling rule above.

## Consequences

- A new injected interface needs a decorator here before a daemon can observe
  it. That is deliberate friction: it keeps the scheme uniform.
- The numbers of `bitacora_replica_*` are measured inside the replica process,
  between the gRPC server and the replica. They exclude the network, so they
  are a lower bound on what a client experiences.
- Renaming a metric is a breaking change for operators and is recorded in the
  changelog like one.
