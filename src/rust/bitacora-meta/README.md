# bitacora-meta

The metadata store: the one place where bitacora uses consensus.

## What it owns

- `MetadataStore`: the operations of the protocol. Streams, a writer's epoch, lease and segments, a
  materializer's checkpoint, the replica registry, controller leadership, and getting the store back
  after it was lost. Its error type is `MetaError`.
- The records that are stored, and the JSON they are stored as: `StreamRecord`, `SegmentRecord`,
  `CheckpointRecord`, `ReplicaRecord`, `LeaderRecord`, `DeletedStreamRecord` and their parts.
- `KeyValueStore`: the interface a backend implements, with the types of a conditional transaction
  (`Revision`, `Condition`, `Mutation`, `TransactionOutcome`).
- `MemoryKeyValueStore`: the backend for tests and the simulator.
- Placement: `choose_replica_set` and `choose_additional_replica`, pure functions of the replica
  registry.
- `MetadataSnapshot`: the whole store as one object, and how snapshot objects are named.

It does not own a backend for a real cluster (that is `bitacora-meta-etcd`), and it does not decide when
anything happens: the writer, the controller and the materializers call it.

## Design

The append path needs no consensus, because the questions that need it are settled here, before and
after: who is the writer, which replicas hold a segment, where a sealed segment ends, how far a
materializer got. The store is touched when a segment rolls, a writer changes, a checkpoint advances or
the controller does its housekeeping, and never on an append or a read.

The crate has two layers.

`KeyValueStore` asks a backend for very little: a linearizable read of a key and of a key prefix, a
revision per key that never repeats, and a transaction that applies some writes only if some keys are
absent or still at a given revision. Anything that offers this can hold bitacora's metadata.

`MetadataStore` builds everything else on that, once, so that every backend behaves the same. Every
operation that changes a record is a read-modify-write: read the record with its revision, decide from
what was read, write on the condition that the revision is still the same, and start again if it is
not. The rules are in the deciding step. A writer's change is refused unless the stream's epoch is
still the writer's, and a materializer's commit unless its fencing token is still current. Because the
decision and the write are tied together by the revision, a process that was paused or cut off cannot
write from stale knowledge when it wakes up.

`MetadataStore::update_stream` takes the change as a function, although the workspace prefers plain data
to closures in public interfaces. It is the body of a transaction, it may run once per attempt, and each
caller (writer, controller, administration, operator tools) has its own.

Records are JSON, so that an operator can read them with the store's own tools, and their format is
held by `tests/records_golden_test.rs` to what schema version 1 wrote.

The crate knows nothing of networks, disks or clocks. Callers pass the time, which is what lets the
whole store run under deterministic simulation.

## Invariants

- No record that a decision depends on is written blindly: every such write is conditional on the
  revision the record was read at.
- An operation that returns an error other than `MetaError::Unavailable` has changed nothing, and every
  operation is safe to repeat. The one exception is `MetadataStore::restore`, which is several
  transactions and says so.
- Epochs, low-water marks, checkpoints, fencing tokens and segment identifiers never decrease.
- A stream identifier is never handed out twice, also not after its stream was deleted.
- At most the last segment of a stream is open, and a sealed end never changes.
- A stored value that cannot be decoded is reported with its key, never skipped.

## Files

| File | Holds |
| --- | --- |
| `src/lib.rs` | The crate's rationale and its public names |
| `src/key_value.rs` | The backend interface and the types of a transaction |
| `src/memory_store.rs` | The in-memory backend |
| `src/records.rs` | The records and their JSON |
| `src/keys.rs` | Where each record lives, and what a name may be |
| `src/error.rs` | `MetaError` |
| `src/store.rs` | `MetadataStore` and the pieces every operation is made of |
| `src/store/streams.rs` | Creating, reading, updating and deleting streams |
| `src/store/writers.rs` | Epochs, leases, and sealing, opening and rolling segments |
| `src/store/checkpoints.rs` | Materializers and their checkpoints |
| `src/store/replicas.rs` | The replica registry |
| `src/store/leadership.rs` | Which controller instance is active |
| `src/store/recovery.rs` | Snapshots, restoring one, adopting a rebuilt stream |
| `src/store/test_support.rs` | The fixtures the tests of the operations share |
| `src/placement.rs` | Choosing replicas for a segment |
| `src/snapshot.rs` | The snapshot format and the naming of snapshot objects |
| `src/<file>_test.rs` | The tests of the file of the same name |
| `tests/records_golden_test.rs` | The stored JSON of every record, held to schema version 1 |

## Dependencies

It depends on `bitacora-types` only, and must stay that way: the client, the replica, the controller,
the materializer and the tools all depend on it. Backends depend on it, never the other way round.

`docs/protocol/metadata.md` describes the same keys, records and transitions for readers who do not read
Rust. A change to a record or a key changes that document in the same commit.
