# 0007. Operator tooling: the `bita` command-line tool

Status: accepted · 2026-11-25

## Context

The design asks for a cluster that one person can run (section 14). Its routine procedures name a
command-line tool: `bita replica drain` before a restart, removal of a failed node from the registry,
and `bita metadata rebuild` as a last resort. It leaves the tool's shape open.

## Decision

### Commands are `bita <noun> <verb>`

The noun is what is acted on (`stream`, `replica`, `segment`, `controller`), the verb what is done to
it. An operator who knows one command can guess the others, and `bita <noun> --help` lists everything
that can be done to a thing. The user's reference is `docs/cli.md`; a test fails if a command exists
without a section there, or a section there names a command that does not exist.

### Every command is a function that returns a value

A command takes the cluster's interfaces (metadata store, replica connector, clock, optional object
store) and returns a result structure; rendering is separate. So the commands are tested against the
in-process test cluster under a virtual clock, with no etcd and no sockets, and the renderers are tested
as pure functions. `main` parses, finds the cluster, connects, runs and prints, and holds no logic worth
a test beyond that.

`stream tail` is the exception: it writes records as they commit.

### Text for people, JSON for scripts

The default output is aligned text. `--output json` prints the same result with `serde`, and the field
names of the result structures are the tool's interface: renaming one is a breaking change. The JSON
structures are the tool's own, not the metadata store's records, so that a change of the stored schema
does not silently change what scripts read.

### The cluster is never guessed

The etcd endpoints come from `--config`, `--etcd-endpoint` or the environment (`BITA_CONFIG`,
`BITA_ETCD_ENDPOINTS`, `BITA_ETCD_KEY_PREFIX`); an option wins over the environment, and both over the
file. Without an endpoint the tool exits with a usage error. There is no default of `localhost`: a tool
that deletes streams must not pick a cluster by accident.

Environment variables are read by the tool itself, not by `clap`, which keeps the resolution a pure
function of the options and a map, and testable without touching the process's environment.

### Exit codes

0 success, 1 the command failed, 2 the command line or the configuration is wrong and nothing was sent.
A script can therefore tell "the cluster said no" from "I called it wrongly".

### Destructive commands need a confirmation

`stream delete` does nothing without `--yes`. `replica remove` refuses a replica that renewed its
registration within the liveness window unless `--force` is given, because removing a live replica makes
repair copy data that was never lost.

### `stream append` is a smoke test

It becomes the stream's writer, which runs the takeover protocol and fences whoever held the stream,
appends, and steps down. That is the only way to append, since a stream has one writer; the help and the
reference say so plainly, and the default takeover policy waits for a live writer's lease.

### "Since when" for the controller

`controller status` shows since when the leader has led. The leadership record did not say; it now
carries `leading_since`, which a renewal keeps and a takeover sets. Records written before the field
existed read as "unknown".

### Metadata snapshots

The design copies the metadata store to the object store every five minutes (section 14). The controller
does it, at the end of its pass: it already runs on a timer and already has the object store.

A snapshot is one object:

```text
snapshot  magic 8 "BITAMETA" | format version u32 | taken at, unix milliseconds u64 | entry count u64
          | entry* | crc32c u32
entry     key length u32 | key, UTF-8 | value length u32 | value
```

Integers are little-endian and the checksum covers every byte before it. The values are the bytes the
store held, which are JSON records, so the snapshot format knows nothing about streams and a snapshot
can be restored by any version that reads those records. A snapshot is read in one listing of the store,
which both backends answer from one consistent view, so it never mixes two states.

Snapshots are named `metadata-snapshots/<unix milliseconds, 20 digits>`, so that a listing gives them
oldest first and the name says when each was taken. The newest 288 are kept by default, a day at the
default interval; older ones are deleted, and the newest is never deleted whatever the setting. Both
numbers are controller settings.

The controller is otherwise stateless, and stays so where it matters: it remembers when the newest
snapshot was taken only to save a listing of the object store on every pass. An instance that does not
know lists once. Two controllers that both believe they lead write at most one snapshot too many each.

A snapshot that cannot be written never fails a pass. It is logged and tried again on the next one.

### Restoring a snapshot

`bita metadata restore <snapshot>` writes the keys of a snapshot back, verbatim. It refuses a metadata
store that holds keys, because the result would mix two states; `--force` makes the store exactly what
the snapshot was. (Since [0020](0020-restore-into-a-lost-store.md), registrations of running replicas do
not count as keys here, and are kept.) The restore is several transactions, since etcd limits the size of one, so it is for a
cluster whose writers and controllers are stopped.

A snapshot is older than the cluster by up to one interval. A restored stream record can therefore name
an epoch that the replicas have already fenced, and miss segments opened since. That is not repaired by
the restore; it is what the rebuild is for, and the procedure is always restore, then rebuild.

### Rebuilding from the replicas

`bita metadata rebuild` reconstructs stream records from what replicas and the object store hold. To
make that possible a replica now reports, for every copy with a file, what the file's header says: the
segment's epoch and first sequence number. The object store's segment objects carry the same in their
footers, with the sealed end.

What is recovered:

- **The segment list.** A segment is listed if a file or an object says where it starts.
- **Sealed ends.** Where a replica or the object recorded the seal. Otherwise one below the start of the
  next segment found: a segment is only opened after its predecessor was sealed, and starts right after
  it. The last segment without a recorded seal is left open, and the next writer recovers it as after a
  crash. That may commit records the lost metadata had cut off as an uncommitted tail; it cannot
  contradict anything, because no later segment holds records.
- **Copies.** Every replica that reported a segment, as `unverified`, so that the controller checks each
  against the sealed end. A copy that was not found is recorded on a replica that does not exist, in
  state `bad`; the controller's existing repair then rebuilds it on a real one. No new mechanism was
  needed for "fewer copies than the replication asks for".
- **The epoch.** One above every epoch found in a header, a footer or a replica's fencing state, so that
  the next writer fences everything, whatever the lost metadata last said.
- **Identifiers.** The next segment identifier is above every segment seen, and the stream identifier
  counter is raised to the stream's, so that neither is reused.

What cannot be recovered, because it is stored nowhere but in the metadata store:

- **The name.** Segment files are addressed by stream identifier. The operator supplies
  `--name <stream id>=<name>`; a stream without one is not rebuilt. Storing names on replicas would put
  a second copy of a metadata fact where nothing keeps it true.
- **Replication, retention, materializers and checkpoints, the truncation request.** Assumed, and
  reported: three replicas with write quorum two, no retention, no materializers. With no materializer
  and no truncation request nothing is ever truncated, which is the safe direction.
- **The low-water mark.** The first sequence number still found.
- **Seal times.** Now, which only delays tiering and retention.

If the metadata store still has a record of the stream, as after a restore, these come from it, and its
low-water mark and next segment identifier are not moved back.

The plan is a pure function of the inventory, so every rule above is a unit test on hand-built
inventories. Assumptions are listed apart from problems. Problems (a replica that did not answer,
segments that do not connect, no complete copy of a sealed segment, an open segment on fewer replicas
than recovery needs, a stream without a name) stop `--apply` unless `--force` is given. The default is a
dry run.

The dangerous case is a replica that does not answer: its copies are not in the plan, and once a stream
is rebuilt the controller removes copies that the stream's record does not list. That is why an
unanswered replica is a problem and not an assumption.

### The benchmark tool

`bita-bench` drives a cluster through the client library, like an application, and reports throughput
and append-to-commit latency (design, section 16, "Benchmarks"). It reports what it measured and judges
nothing: the design's targets are proposals, and the tool must not turn into a place where they are
quietly restated as facts.

**The histogram is in-tree and fixed-size.** Latencies are counted in whole microseconds. Values below 32
have a bucket each; above that, every power of two is split into 32 buckets, up to 2^36 microseconds
(about 19 hours), 1024 buckets in all. A bucket is therefore never wider than 1/32 of its values, and a
quantile, reported as its bucket's upper bound, is at most 3.2 % above the true value and never below
it. The maximum is kept exactly. Recording is an index computation and an increment: no allocation, no
lock, since every worker has a histogram of its own and they are merged at the end. A histogram crate
would do the same with more configuration; this is seventy lines with the error bound as a unit test.

**All arithmetic is in integers.** Milliseconds, microseconds and bytes; rates are rounded down. The JSON
field names carry the units.

**A paced workload measures from the moment an append was due.** With `--rate`, sends are numbered and
send `n` is due at `n` intervals after the start, computed from the number so that nothing drifts.
Latency counts from that moment. Measuring from the actual send would leave out the time appends spend
queued behind a stall, which is the classic way a load generator hides the pauses it was meant to find.
Without `--rate` the workload is closed-loop and the reference says what that does and does not show.

**One writer per stream, many appends in flight.** A stream has one writer, so concurrency within a
stream is appends in flight on one writer, and `--streams` adds writers. The tool runs on a
multi-threaded runtime, the only binary that needs one, so that it is not itself limited to one core.

**Not done.** The design lists a fault schedule among the benchmark's options. Faults are injected by the
deterministic simulator; the benchmark does not inject any. Reference runs against Kafka and BookKeeper
are a matter of running them.

## Consequences

- The tool needs no etcd to be tested, and its tests take a fraction of a second.
- The benchmark's workloads are tested against the in-process cluster under a virtual clock, so its
  limits, pacing and accounting are covered without a real run. Its numbers on real hardware are covered
  by nothing but running it.
- A cluster with an object store can lose its whole metadata store and lose at most the last interval of
  metadata changes; what the replicas hold beyond that is found again by `bita metadata rebuild`.
- Without an object store there are no snapshots.
- The object store can only be named in the configuration file. `stream read` of a segment that replicas
  no longer hold needs that file.
- `replica drain` cannot complete a drain by itself: writers roll when they next append. It reports what
  still names the replica and can wait for it.
