# bitacora-process

What every bitacora daemon shares.

## What it owns

- The command line of a daemon: `--config <path>`, `--help`, `--version`.
- Reading the configuration file (TOML) into the daemon's configuration type.
- The log output: structured JSON on standard output, or readable text for development.
- Noticing SIGTERM and SIGINT.
- The metrics of a process: the registry, the naming scheme, and the decorators that measure what
  passes through the injected interfaces (disk, replica requests, metadata store, object store).
- The HTTP endpoint with `/metrics`, `/healthz` and `/readyz`, and a minimal client for it.

It holds no bitacora logic. Nothing here knows what a segment or an epoch is.

## Design

**Observability is attached at the injected interfaces.** The library crates (`bitacora-replica`,
`bitacora-client`, `bitacora-controller`, ...) contain no metrics library, so that the same code runs
in the deterministic simulator. They already receive their disk, their metadata store, their object
store and their transport as interfaces. A daemon wraps each of them in a decorator from this crate
before handing it over, and the decorator records requests, durations and failures. Removing the
decorators changes nothing but the numbers.

**One naming scheme.** Every metric is `bitacora_<subsystem>_<what>` with base units in the name
(`_seconds`, `_bytes`) and `_total` on counters. Labels are small closed sets known at compile time
(operation, error kind, direction); nothing a client chooses is ever a label. The scheme is described
in `src/metrics.rs`, and `docs/operations/metrics.md` lists every metric. A test fails when a metric
is exposed that the document does not list.

**The metrics library stays here.** Daemons register their own label-free metrics through
`MetricsRegistry::counter`, `gauge`, `duration_histogram` and get back small wrapper types. No other
crate depends on `prometheus-client`, `tracing-subscriber` or `axum`.

**Liveness and readiness are separate.** `/healthz` answers as long as the process runs. `/readyz`
follows a flag that each daemon sets when it can do its work and clears when it starts to shut down.

## Invariants

- A decorator never changes, delays or retries a call.
- Recording a metric never blocks and never fails.
- A new `Readiness` is not ready.
- After `ShutdownListener::install`, SIGTERM and SIGINT no longer kill the process, and a signal that
  arrives later is never lost.
- A configuration is loaded completely or not at all, and every error names the file.

## Files

| File | Holds |
| --- | --- |
| `src/lib.rs` | The public surface |
| `src/command_line.rs` | `parse_command_line`, `usage` |
| `src/config_file.rs` | `load_config_file` |
| `src/logging.rs` | `LogSettings`, `init_logging` |
| `src/shutdown.rs` | `ShutdownListener` |
| `src/metrics.rs` | `MetricsRegistry`, the metric types, the naming scheme, the request families |
| `src/instrumented_disk.rs` | `InstrumentedDisk` |
| `src/instrumented_replica.rs` | `InstrumentedReplica` |
| `src/instrumented_key_value_store.rs` | `InstrumentedKeyValueStore` |
| `src/instrumented_artifact_store.rs` | `InstrumentedArtifactStore` |
| `src/http_endpoint.rs` | `serve_observability`, `Readiness` |
| `src/probe.rs` | `http_get` |
| `src/metrics_reference.rs` | `undocumented_metrics`, the check of `docs/operations/metrics.md` against the code |

Each `<file>.rs` has its tests next to it in `<file>_test.rs`.

## Dependencies

It may depend on `bitacora-types`, `bitacora-segment`, `bitacora-meta` and `bitacora-artifact`, for the
interfaces it decorates. It must not depend on `bitacora-replica`, `bitacora-client` or
`bitacora-controller` (the replica is a test-only dependency). Only daemons and tools depend on it.
