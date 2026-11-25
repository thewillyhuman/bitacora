# bitacora-model

An executable model of bitacora's append, roll and failover protocols, checked exhaustively.

## What it owns

- The protocol as a transition system: what a state is, which steps exist, what each step does.
- The invariants I1 to I4 of the design, as predicates on a state.
- A breadth-first explorer that visits every reachable state and returns a shortest counterexample.
- The rules of the protocol as data, including five variants broken on purpose that show the explorer
  notices a mistake.

It does not own the protocol's prose. That is in `docs/spec.md` (sections 5 and 9) and
`docs/protocol/replication.md`; this crate is where those are made precise enough to check.

## Design

The model describes one stream with three replicas and a write quorum of two, a metadata store that only
changes through conditional writes, a first writer that appends and may roll, and a recovering writer
that runs the eight failover steps and then appends. Messages can be lost, delayed, duplicated and
reordered; replicas can restart; a writer can stop between any two steps.

Three choices keep the state space small enough to explore in seconds:

- **Records are not stored.** Within a segment there is one possible record per sequence number, so a
  copy is described by how far it reaches.
- **The network is not a queue.** What is on its way is derived from the state: a replica may persist any
  prefix of what a writer has sent, a writer may learn any acknowledgement a replica has produced.
- **Replicas are interchangeable.** A state is identified up to the order of its replicas, which divides
  the number of states by up to six.

The explorer is in the crate, about a hundred lines, with no dependencies, threads, hashing or randomness,
so a check gives the same result on every run. Why it is not TLA+ or Stateright, what the model abstracts
away and what it found are in `docs/decisions/0001-protocol-model.md`.

A state is a `Copy` struct of small fixed-size arrays. Taking a step copies it; nothing is allocated
except the fingerprint that identifies a state to the explorer. The crate is compiled with optimizations
even in fast builds (`BUILD.bazel`, and the profile override in `src/rust/Cargo.toml`), because its tests
are pure computation over a few million states.

## Invariants

- The crate depends on nothing, in the workspace or outside it. A mistake in the implementation cannot
  hide a mistake in the model.
- A check is deterministic: the same model gives the same outcome, the same number of states and the same
  counterexample every time.
- `enabled_actions` lists steps in a fixed order. The order decides which of several shortest
  counterexamples is reported, so changing it changes what the tests see.
- A counterexample's trace, replayed, ends in a state that violates the counterexample's invariant.
- A change to a protocol step or to an invariant is made here first, with a decision record, then in the
  implementation.

## Files

| File | Holds |
| --- | --- |
| `src/lib.rs` | The crate's rationale and its public names |
| `src/protocol.rs` | `ProtocolModel`, its check and its outcome: the public interface |
| `src/rules.rs` | The rules the actors follow and the bounds of what is explored |
| `src/state.rs` | What a state is, and the fingerprint that identifies it |
| `src/transitions.rs` | The steps: which can happen in a state, and what each leads to |
| `src/invariants.rs` | I1 to I4 as predicates on a state |
| `src/explorer.rs` | The breadth-first search |
| `src/<file>_test.rs` | The tests of the file of the same name |

`src/protocol_test.rs` is where the protocol is actually checked: the three bounded configurations, the
reachability of every interesting state, and the five broken rules.

## Running it

```sh
bazel test //src/rust/bitacora-model/...
```

A failing check prints the invariant and the steps that lead to its violation.

## Dependencies

None, and it must stay that way. Nothing depends on it either: it is a test of the design, not a library
the implementation uses.
