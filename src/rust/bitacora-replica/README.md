# bitacora-replica

One storage node of the log: it appends batches to segment files, flushes them, and serves them back.

## What it owns

- **The node.** `Replica`: every segment copy on one disk, behind the `ReplicaApi` interface that
  writers, readers, the controller and other replicas call. It finds its copies when it is opened,
  routes each request to the copy it is about, and carries what belongs to the node as a whole: its
  identity, whether it is draining, whether it is stopped.
- **One copy of a segment, with its rules.** `SegmentCopy`: accept only batches that continue the copy,
  refuse writers that were fenced, serve nothing the writer has not declared durable, never forget a
  fence or a seal, and refuse everything once a checksum failed.
- **What a copy needs done to it later.** Repair from another replica, upload to the object store,
  verification by the scrubber, deletion.
- **Admission.** Whether the disk takes more data, by how full it is.

It owns no decision about the log. The writer assigns sequence numbers, the metadata store holds epochs
and segment lists, the controller decides what is repaired, uploaded and deleted (design, section 7).
The replica enforces the local rules that make those decisions safe, and nothing else. It owns no
transport and no process either: `bita-replica` is the daemon that serves it over gRPC.

## Design

**One task per segment copy.** Requests for one segment come from many callers at once, and a copy's
rules only hold if they are applied one at a time. Each copy is owned by a task of its own, and the rest
of the replica talks to it through a `SegmentHandle`: a queue of commands, each with a channel for its
answer. No lock guards a copy. A command that was queued runs to completion even if its caller hangs
up, so a dropped connection never leaves a write without its flush. Segments do not wait for each
other: a slow repair of one does not hold up an append to another.

**Waiting reads watch, they do not queue.** After every command the task publishes how far the copy is
readable. A read that may wait for records watches that value through its handle and sends its read
command only when there is something to read, so waiting readers cost the task nothing.

**A copy is two files and a health.** The records file holds the batches; the small state file next to
it holds the fenced epoch and the sealed end, and is replaced whole whenever one of them changes. The
copy's health is one value: sound, damaged (a checksum failed; the copy waits to be replaced), or
refused a write (the disk said no; the process must stop). A replica that never received a record for a
segment still keeps the state file, because a fence must hold even there.

**The identity belongs to the data directory.** An empty directory adopts the identity the replica is
opened with; a used one keeps its own. A node whose disk was wiped comes back under a new identity and
can never claim copies it no longer holds.

**Repair never destroys before it has replaced.** A copy that is only short is completed in place. A
copy that cannot be completed is rebuilt in a scratch file beside it and moved over it with one atomic
rename once it is whole. Until then the old copy is untouched: it may be damaged in one place and still
be the only copy of everything else.

**A refused write is reported as data.** When the disk refuses a write or a flush, the process can no
longer trust what it sees of its files and must stop. The replica cannot stop a process, so it tells
its host: `Replica::write_failure` for a host that looks, `Replica::write_failed` for one that waits.
It is a value and not a callback because every host (the daemon, the test kit, the simulator) needs
the same thing, to know that it happened, and decides what to do about it in its own code.

**Everything from outside is handed in.** The disk, the way to reach peers and the object store are
interfaces passed to `Replica::open`. The replica opens no file or socket itself and reads no clock but
the runtime's, which is what lets the test kit and the simulator crash it, cut it off and damage its
disk, running the same code as production.

**The hot path is three hops.** An append is: the admission check (one lock, no system call while the
last measurement of the disk is recent), one lookup of the segment's task, the queue hop to the task,
and `SegmentCopy::append`, which decodes the frames without copying them, writes and flushes. A read is
the lookup, the hop and one copy of the frames out of the file. The request is moved, never cloned.
Repair, upload, scrubbing, opening and deletion are rare and written to be followed, not to be fast.

## Invariants

- An append is acknowledged only after its flush returned.
- Requests for one segment are applied in arrival order, one at a time, by the one task that owns the
  copy.
- The fenced epoch of a copy never goes down, and the sealed end, once recorded, never changes. Both
  are on the disk before they are answered.
- An open copy serves a reader nothing above the durable sequence it last heard from the writer; after
  a restart that is nothing, until the writer speaks again. Only a writer that has fenced the copy at
  its own epoch reads the tail.
- A damaged copy answers nothing but its status. A repair makes it sound again.
- A copy whose disk refused a write answers nothing, is not repaired, and is reported to the host. It
  is read back from the disk and judged again when the replica is next opened.
- A repair never touches the old copy before the new one is complete and flushed.
- An upload never holds a segment: it reads its copy a chunk at a time, twice, once to send it and once
  to verify what the object store holds, and keeps a chunk or two of 8 MiB whatever the segment's size.
  It answers only for an object that it read back and found to be what its copy encodes to.
- The replica's identity never changes while it is open. No task is started after `stop`.
- Reads, fences, seals and deletions are never refused for lack of space or for draining.
- No lock is held across an `await`.

`docs/protocol/replication.md`, "A replica", states the same rules in protocol terms, and
`docs/protocol/disk-format.md` describes the data directory.

## Files

| File | Holds |
| --- | --- |
| `src/lib.rs` | The crate's rationale and its public names |
| `src/replica.rs` | `Replica`: opening, identity, the registry of tasks, stopping, scrubbing, write failures |
| `src/replica/api.rs` | The `ReplicaApi` implementation: what each operation checks and whom it hands to |
| `src/replica/admission.rs` | Whether the disk takes more data, and the cached measurement of its free space |
| `src/actor.rs` | The task that owns one copy, its commands, and `SegmentHandle` |
| `src/copy.rs` | `SegmentCopy`: its fields, loading, health, status, verification, deletion |
| `src/copy/append.rs` | Appends and heartbeats, and `Admission` |
| `src/copy/fence_seal.rs` | Fencing and sealing |
| `src/copy/read.rs` | What a read may be served |
| `src/copy/repair.rs` | Repair from another replica, and `RepairPace` |
| `src/upload.rs` | Upload of a sealed copy to the object store |
| `src/paths.rs` | The names of the files in the data directory |
| `src/settings.rs` | `ReplicaSettings` and its defaults |
| `src/write_failure.rs` | `WriteFailure` and the report the tasks record it in |
| `src/test_support.rs` | Fixtures shared by the tests; compiled for tests only |
| `src/<file>_test.rs` | The tests of the file of the same name |

The files in `src/copy/` are child modules of `copy.rs`, and those in `src/replica/` of `replica.rs`: each
adds methods to the parent's type and may use its private fields, which nothing else can.

## Dependencies

Of the bitacora crates it depends on `bitacora-types` (identifiers and the `ReplicaApi` interface),
`bitacora-segment` (segment files and the disk) and `bitacora-artifact` (the object store interface),
and must stay that way. It must not depend on `bitacora-meta`: a replica never reads the metadata
store, and registering it there is the daemon's job. It must not depend on `bitacora-proto`: the wire
protocol wraps the replica, not the other way round, so that tests and the simulator can call it
without a network.
