# bitacora-types

The vocabulary of bitacora: the types every other crate uses to say which stream, which segment, which
writer, which record and which replica, and the contract for talking to a replica.

## What it owns

- **Identifiers and positions.** `StreamId`, `SegmentId`, `SegmentAddress`, `Epoch`, `SequenceNumber`,
  `ReplicaId`: one type each, so that a segment identifier cannot be passed where a sequence number is
  meant.
- **Records.** `LogRecord`, `SequenceRange` and the limit on a record's size.
- **Replication settings.** `ReplicationSettings`: how many replicas, how many acknowledgements, and the
  recovery quorum that follows from the two.
- **Time.** `Timestamp` and the `Clock` interface, with the system clock and a clock for tests.
- **Randomness.** `DeterministicRandom`, the only random source in the workspace.
- **The replica contract.** `replica_api`: the `ReplicaApi` trait, the request and answer of each
  operation, `ReplicaError`, and `ReplicaConnector`, which turns a registered replica into a handle.

It owns no behaviour beyond validating these values and doing arithmetic on positions.

## Design

**One type per notion.** Everything numeric is a `u64` underneath, and the safety of the log rests on
comparing epochs with epochs and positions with positions. Wrapping each in its own type makes the
compiler refuse the mix-ups that would otherwise corrupt data quietly. The wrappers are `Copy` and
serialize as the plain number.

**Values with rules are built in one place.** A replica identifier, a range of records and the
replication settings can each only be built through a function that checks them, and `serde` goes
through the same function. Code that receives one does not check again.

**What simulation must control is injected.** The simulator runs the real code with a seed and must
replay any run exactly. So the three sources of non-determinism are things a caller passes in: the
clock (`Clock`), randomness (`DeterministicRandom`, built from a seed, in-tree so that recorded seeds
keep their meaning), and the network to a replica (`ReplicaApi` behind a `ReplicaConnector`). The same
trait is implemented by the replica itself, by the gRPC client and by the simulator's fault-injecting
link, which is what lets the protocol be tested without a socket.

**A replica is reached by identity.** `ReplicaConnector::connect` takes the identity the caller means
together with the address. A node whose disk was replaced returns at the same address under a new
identity and must never answer for the old one.

## Invariants

- `SequenceNumber::ZERO` never names a record; records are numbered from `SequenceNumber::FIRST`.
- A `ReplicaId`, a `SequenceRange` and a `ReplicationSettings` that exist are valid.
- A recovery quorum and a write quorum always share a replica: `(N − W + 1) + W > N`.
- A `DeterministicRandom` produces the same values for the same seed, forever.
- No safety property depends on a `Timestamp`.
- The serialized forms are part of the metadata schema and of the wire protocol.

## Files

| File | Holds |
| --- | --- |
| `src/lib.rs` | The crate's rationale and its public names |
| `src/identifiers.rs` | Stream, segment, epoch, sequence number, replica identifier |
| `src/records.rs` | A record, a range of records, the payload size limit |
| `src/replication.rs` | Replica count, write quorum, recovery quorum |
| `src/time.rs` | Timestamps and clocks |
| `src/random.rs` | The seeded random generator |
| `src/replica_api.rs` | The replica contract and the two connectors that need nothing else |
| `src/<file>_test.rs` | The tests of the file of the same name |

## Dependencies

It depends on no other bitacora crate, and must stay that way: every other crate depends on it. Its
third-party dependencies are `serde`, `thiserror`, `bytes` and `async-trait`.
