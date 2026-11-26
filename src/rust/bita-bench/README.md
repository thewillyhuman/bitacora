# bita-bench

The benchmark tool: it drives a bitacora cluster with an append or a read workload, through the same
client library an application uses, and reports throughput and latency percentiles. The user's reference
is `docs/benchmark.md`.

## What it owns

- The workloads: appends with a chosen number in flight, optionally paced to a rate and spread over
  several streams; a range scan for reads.
- The measuring instruments: a fixed-bucket latency histogram and the schedule of a paced workload.
- The report: text for people, JSON with stable field names and units for regression tracking.

It owns no targets and judges no result. It states what was asked for and what was measured.

## Design

An append workload opens one writer per stream, because a stream has one writer, and shares it among as
many workers as there are appends in flight. Each worker takes the number of the next send from a shared
counter, waits until the send is due if the workload is paced, appends and waits for the commit. The
latency goes into the worker's own histogram, and the histograms are merged at the end: recording takes no
lock and no allocation, and the tool's memory does not grow with the length of a run.

The histogram counts whole microseconds in buckets that are never wider than 1/32 of their values, so a
reported percentile is at most 3.2 % above the true one and never below it. The maximum is exact.

A paced workload measures each append from the moment it was due, not from the moment it went out. If the
cluster stalls, the appends that queue up behind the stall are counted as slow. Measuring from the actual
send would hide the stalls a benchmark is supposed to find.

Time is read through Tokio's clock only, so the workloads are tested against the in-process test cluster
under a virtual clock: a "ten second" run takes no real time in the tests.

## Invariants

- Recording a latency never allocates.
- The payload of every record is one shared buffer.
- A writer opened by a workload is closed before the workload returns.
- The report goes to standard output and nothing else does.
- Exit codes: 0 the workload ran to its limit, 1 it could not run or stopped early, 2 the command line or
  the configuration is wrong.

## Files

| File | Holds |
| --- | --- |
| `src/main.rs` | The entry point: parse, find the cluster, connect, run, print |
| `src/arguments.rs` | The command line, as `clap` types |
| `src/workload.rs` | The append and read workloads and what they measure |
| `src/histogram.rs` | The latency histogram |
| `src/pacing.rs` | When each send of a paced workload is due |
| `src/report.rs` | Rates, percentiles and the two output forms |

Each file's tests are next to it, in `<file>_test.rs`.

## Dependencies

`bitacora` only: the tool sees a cluster exactly as an application does. The tests add
`bitacora-testkit`, `bitacora-meta` and `bitacora-types`. Nothing depends on this crate.

## Not done

- A fault schedule, which the design lists next to the workload options. Faults are injected by the
  deterministic simulator today, not by this tool.
- Reference runs against other systems. The design names Kafka and BookKeeper on the same hardware as
  points of comparison; that is a matter of running them, not of this tool.
