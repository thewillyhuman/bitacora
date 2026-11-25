# bitacora-kv

The reference materializer: a small versioned key-value store on one stream
(design, section 12). It keeps the materializer framework honest and gives
the test suite a workload whose correctness can be checked from the outside.
It is not meant to compete with real databases.

## What it owns

- The record format of a key-value stream (`KvRecord`).
- The layout of keys on pages (`KvPages`).
- The front end that writes records and reads keys (`KvStore`).

It owns no storage of its own. Records go to the log through
`bitacora-client`; pages are kept by the page adapter of
`bitacora-materializer`.

## Design

| Operation | What happens |
| --- | --- |
| `put(key, value)` | One record is appended; returns its sequence once committed |
| `delete(key)` | The same, with a delete |
| `write(operations)` | Several changes in one record, applied together |
| `get(key)` | The latest committed value, linearizable |
| `get_as_of(key, sequence)` | The value as of an earlier sequence |
| `scan(from, to, sequence)` | The keys of a range at one sequence, in order |

Keys are hashed into 64 buckets with CRC-32C. A bucket is a page: its
key-value pairs, sorted. A record touches the buckets of its keys; applying it
to a bucket rewrites that page. The page adapter keeps every version of every
page, which is what `get_as_of` and `scan` read.

`KvStore::open` becomes the writer of the stream, then takes over and runs
the materializer. A store that is opened replaces whichever store was open.

### Linearizable reads

`get` appends a record without operations, a barrier, and waits for it to
commit at some sequence S. Then it waits until the materializer has applied S
and reads the key's bucket as of S.

A barrier that commits proves the store was still the writer while the read
was under way: a newer writer fences a recovery quorum of replicas before it
commits anything, and the barrier could then no longer reach a write quorum.
Every write that finished before the read began is therefore below S.

If the barrier cannot be committed, `get` fails with `NotCurrent`. It never
answers from pages it cannot vouch for. `get_as_of` and `scan` name their
sequence and need no barrier.

### Writes and retries

One call appends at most one record. The store never retries an append, and
the log deduplicates the writer's own resends by sequence number. A write
that fails with `OutcomeUnknown` may or may not be in the log. A caller that
tries again makes a second write; a caller for whom that matters reads the
key back first.

## Invariants

- A value returned by `get` was the committed value of the key at some moment
  between the call and its return.
- A write reported as done is in the log once, at the returned sequence.
- Reads are answered only from records the log has committed.
- The bucket count and the hash never change for an existing store.

## Which file holds what

| File | Holds |
| --- | --- |
| `record.rs` | `KvRecord`, `KvOperation` and their bytes |
| `pages.rs` | `KvPages`, the hash, the bucket page format |
| `store.rs` | `KvStore`, its settings and errors |

Tests sit next to the file they test, in `<file>_test.rs`.

## Not done

- Compare-and-set, and deduplication of retried writes.
- Read replicas: front ends that only tail the stream. `get_as_of` and
  `scan` are the reads such a front end would serve.
- A cache of hot pages in the front end.
- Confirming the writer's epoch for a read without appending a record.

## Dependencies

May depend on `bitacora-types`, `bitacora-artifact`, `bitacora-meta`,
`bitacora-client` and `bitacora-materializer`. Tests also use
`bitacora-testkit` and `bitacora-controller`.
