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

## Consequences

- The tool needs no etcd to be tested, and its tests take a fraction of a second.
- A cluster with an object store can lose its whole metadata store and lose at most the last interval of
  metadata changes; what the replicas hold beyond that is found again by `bita metadata rebuild`.
- Without an object store there are no snapshots.
- The object store can only be named in the configuration file. `stream read` of a segment that replicas
  no longer hold needs that file.
- `replica drain` cannot complete a drain by itself: writers roll when they next append. It reports what
  still names the replica and can wait for it.
