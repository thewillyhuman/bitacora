# bitacora-client

The client library: the stream writer, the stream reader and stream administration. The write and
failover protocols run here, inside the application that uses the log.

## What it owns

- `BitacoraClient`: the entry point. It is given a metadata store, a replica connector and a clock, and
  hands out the three handles below. `ClientSettings` tunes it.
- `StreamWriter`: `append`, `append_batch`, `durable_sequence`, `epoch`, `close`. It assigns sequence
  numbers, replicates batches, decides what is committed, rolls to a new segment and renews its lease.
  Opening one runs the takeover protocol. `WriterSettings` and `TakeoverPolicy` tune it; `AppendError`
  and `OpenError` are how it fails.
- `StreamReader` and `RecordStream`: `read_range`, `follow_from`, `low_water_mark`, and for a follower
  `next`, `next_batch`, `durable_sequence`. `ReaderSettings` tunes them; `ReadError` is how they fail.
- `StreamAdmin`: creating, describing, listing, truncating and deleting streams, and registering
  materializers, with `StreamOptions` and `StreamDescription`.

It does not own the wire protocol (`bitacora-proto` supplies a connector that speaks gRPC), the replica
(`bitacora-replica`), the metadata operations (`bitacora-meta`) or anything that happens on a schedule:
placing standby segments, repair, tiering and retention are the controller's.

## Design

bitacora has no server on the append path. The protocol is a library, so that an append costs one
network round trip to the replicas and one disk flush on each, and never touches the metadata store.

**Everything from outside is injected.** The metadata store, the connector and the clock come through
`BitacoraClient::new`; an object store is optional. The crate reads no wall clock and opens no socket.
This is what lets the simulator run the same code on virtual time with faults injected, and it is why
the timers here are tokio's (virtual in tests) while leases use the injected clock.

**The writer is plain bookkeeping plus three kinds of task.** `writer/state.rs` holds every decision as
synchronous code on one data structure: which batches are pending, what each replica acknowledged, what
is committed, whether to roll. It does no I/O and is tested without a runtime. Around it, the callers
of `append` enqueue and wait; one sender task per replica sends what that replica lacks; one driver task
renews the lease and rolls. They share the state behind a mutex that is never held across an `await`,
and wake each other through a watch channel that carries only a version number. One task per replica
means the slowest replica does not set the pace: a record commits when the fastest write quorum has it.

**A roll re-sends, a takeover repairs.** When a replica fails or the segment is full, the writer seals
its segment at the durable sequence with one conditional metadata write and re-encodes everything above
it for the next segment, whose replica set the controller placed beforehand. When a writer dies, the
next one claims the epoch, fences the old segment's replicas, hears from N − W + 1 of them, seals at the
highest end any of them holds, and continues on a new segment. Both are in `writer/driver.rs` and
`writer/takeover.rs`, written step by step under the step numbers of `docs/protocol/replication.md`.

**The reader is a cursor.** `reader/cursor.rs` fetches the records at a position: it finds the segment,
asks one replica, switches on failure, falls back to the object store for a tiered segment, and reads
the metadata again when what it knows leads nowhere. `read_range` and `follow_from` are loops around
it. What an answer means is decided in `reader/answer.rs`, without I/O, and includes verifying every
batch checksum: the reader does not trust a replica to have done it.

**Async stays at the edge.** Functions that wait on the network or a timer are `async`; everything they
decide is in synchronous functions next to them, with unit tests. The asynchronous code is kept in the
order of its awaits on purpose: under the simulator a different order is a different schedule.

`MetadataStore::update_stream` is called with closures here (truncation), as that interface asks for.
The public interface of this crate takes plain data.

## Invariants

- A record is reported as committed only after a write quorum has flushed it. A committed record keeps
  its sequence number and is never lost, through rolls and takeovers.
- One writer per stream. A writer that lost the stream cannot commit anything and learns it at its next
  request or metadata write; from then on every append fails with `AppendError::Fenced`.
- Sequence numbers are assigned once, at enqueue, in order. A record that has one is sent even if its
  caller stops waiting.
- A reader returns only committed records, in order, without gaps or repeats, each from a batch whose
  checksum it verified.
- The metadata store is not touched by an append, nor by a read that the reader's replica can serve.
- The append path (`append`, `WriterState::enqueue`, `plan_request`, `record_acknowledgement`, the
  sender loop) and the fetch path (`Cursor::fetch` on the preferred replica, `classify_response`) do
  nothing per record beyond encoding or decoding it: no copy of a payload, no extra lock, channel or
  `await`. Their shape is deliberate; the file headers say what each one costs.

## Files

| File | Holds |
| --- | --- |
| `src/lib.rs` | The crate's rationale and its public names |
| `src/client.rs` | `BitacoraClient` and `ClientSettings` |
| `src/context.rs` | The injected interfaces the handles share, and connecting to a segment's replicas |
| `src/settings.rs` | `WriterSettings`, `ReaderSettings`, `TakeoverPolicy` |
| `src/error.rs` | `AppendError`, `OpenError`, `ReadError` |
| `src/admin.rs` | `StreamAdmin`, `StreamOptions`, `StreamDescription` |
| `src/writer.rs` | `StreamWriter`: the handle, and what its tasks share |
| `src/writer/state.rs` | The writer's bookkeeping: pending batches, acknowledgements, the commit rule, rolls |
| `src/writer/sender.rs` | The task that keeps one replica up to date |
| `src/writer/driver.rs` | The task that renews the lease and rolls to a new segment |
| `src/writer/takeover.rs` | The failover protocol, and announcing a seal to replicas |
| `src/reader.rs` | `StreamReader` and `RecordStream` |
| `src/reader/cursor.rs` | A position in a stream and fetching the records at it |
| `src/reader/answer.rs` | What one replica's answer to a read means |
| `src/tiered.rs` | Reading a sealed segment from the object store |
| `src/<file>_test.rs` | The unit tests of the file of the same name |
| `tests/support/mod.rs` | The fixtures the end-to-end tests share |
| `tests/writer_test.rs` | The write path and the failover protocol, end to end |
| `tests/reader_test.rs` | Range reads and followers, end to end |
| `tests/tiered_read_test.rs` | Reading tiered segments, end to end |
| `tests/admin_test.rs` | Stream administration, end to end |

## Dependencies

It depends on `bitacora-types` (identifiers, the replica interface, the clock), `bitacora-meta` (the
metadata operations), `bitacora-segment` (the batch format and the layout of a tiered object) and
`bitacora-artifact` (the object store interface). It must not depend on `bitacora-proto`,
`bitacora-replica` or anything else that brings a transport or a disk: those are injected. Its tests
use `bitacora-testkit` for an in-process cluster.

`docs/protocol/replication.md` describes the same protocol for readers who do not read Rust. A change
to what the writer, the reader or a takeover does changes that document in the same commit.
`bitacora-model` checks the protocol exhaustively and `bitacora-sim` runs this code under injected
faults; a change here is not done until both still pass.
