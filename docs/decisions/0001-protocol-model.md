# 0001. The protocol model is a Rust crate with its own explorer

Status: accepted · 2026-11-25

## Context

The design asks for the append, roll and failover protocols to be written as
an executable model before and alongside the implementation, and for that
model to check invariants I1 to I4 over every interleaving of a writer, a
recovering writer and three replicas with crashes and message loss (section
15, layer 1; exit gate of milestone M0). It leaves the tool open: TLA+ or
Stateright (section 20).

## Decision

The model is the crate `src/rust/bitacora-model`. It contains the protocol as
a transition system and a small explicit-state, breadth-first explorer of its
own. It uses neither TLA+ nor Stateright, and it does not depend on any
implementation crate, so a mistake in one cannot hide a mistake in the other.

Why not TLA+. It is the better known notation, but it is a second language
and needs a Java toolchain to run TLC. A model in Rust is read and changed by
the same people with the same tools, and `bazel test //...` checks it on
every change with nothing else to install.

Why not Stateright. It is the natural Rust choice and the design names it.
It was ruled out by what it brings, before trying to build it under Bazel:
twelve direct dependencies, among them an HTTP server for its web explorer,
concurrent hash maps and a random number generator. The repository allows one
random source and bans hash maps for the sake of determinism, and
Stateright's checker runs on threads, so the order in which it visits states
is not reproducible. What the model needs from a checker is about a hundred
lines.

What the explorer does instead:

- It visits states breadth first, so a counterexample is a shortest one.
- It is deterministic: the same model is explored in the same order every
  time.
- It identifies a state by its exact content, not by a hash, so two different
  states are never confused.
- It treats the three replicas as interchangeable, which divides the number
  of states by up to six.

## What the model covers

One stream with three replicas and a write quorum of two (so a recovery
quorum of two), and a metadata store that changes only through conditional
writes, taken as atomic steps.

- **Append.** The first writer (epoch 1) assigns sequence numbers and sends
  records. A replica persists them only if it is not fenced against the
  writer, the segment is not sealed for it, and they continue its copy; then
  it acknowledges. The writer commits at the second-highest acknowledged end
  and reports the records up to it as committed.
- **Roll.** The first writer seals its segment at its durable sequence and
  opens the standby segment in one conditional write, then sends every record
  above the sealed end to the new segment under the same sequence number.
- **Failover.** The recovering writer (epoch 2) runs the eight steps of
  section 9: claim the epoch, read the segment list, fence, wait for a
  recovery quorum of answers, take the highest local end, repair the tail on
  the responders, seal conditionally, open a new segment. Then it appends
  records of its own.
- **Faults.** A request a writer has sent can reach any replica at any later
  time, any number of times, or never; so can an acknowledgement on its way
  back. That is message loss, delay, reordering and duplication. A fence
  answer either reaches the recovering writer or is lost. A replica restart
  keeps what was flushed and forgets what was only in memory. A writer crash
  is a writer that takes no further step: the explorer visits every order of
  steps, so every run in which a writer stops after any of its steps, with
  its requests still on their way, is visited without a dedicated action.

The invariants, checked in every reachable state:

- **I1 Durability.** Every record reported as committed is still part of the
  stream as the metadata store defines it, flushed on at least one replica,
  and no reader was handed another record in its place.
- **I2 Uniqueness.** No two reads, by any reader at any time, return
  different records at one sequence number.
- **I3 Prefix.** Segments tile the sequence space, and nothing that a reader
  could once read is ever taken back by a later seal.
- **I4 Single writer.** No sequence number is committed by two epochs; a
  fenced replica never persists an append from an older epoch; and a record
  the old writer assigned after the new one had fenced a recovery quorum is
  never reported as committed.

I4 is stated about records assigned after the takeover on purpose. The
design says the old writer "cannot commit" once a recovery quorum is fenced.
Taken literally that is not true, and the model shows it: an acknowledgement
that was already on its way when the fence arrived can still reach the old
writer, which then reports a commit after the fence. That is safe, because
the record was on a write quorum before the fence and the recovery therefore
found it. What the old writer cannot do is commit anything new.

## What it abstracts away

- Records are not stored. Within a segment there is one possible record per
  sequence number, so a copy is described by how far it reaches. Rule 1 of
  section 4 is therefore an assumption of the model, not a result.
- The durable sequence reaches every replica that would accept it at the
  moment of the commit. Replicas can only serve more this way, never less, so
  a violation of I2 or I3 cannot be missed.
- Only one takeover is modelled. A recovery that fails and is started again
  under a third epoch (section 9, "If recovery itself fails") is not covered.
- Repair of sealed segments, tiering, truncation, placement and leases are
  outside the model. Standby segments are a counter.
- The bounds are small: three replicas, up to three records for the first
  writer, one roll, one takeover, one record for the recovering writer.

## Results

Three bounded configurations of the protocol as designed hold all four
invariants:

| Configuration | States |
| --- | --- |
| Takeover at any point of two appends, replica restarts | 778,428 |
| Roll at any point of three appends, replica restarts | 1,431,141 |
| Roll raced against a takeover, one record each | 860,049 |

Five deliberately broken variants are each caught with a shortest
counterexample: commit one acknowledgement below the write quorum, replicas
that ignore fencing, replicas that acknowledge before flushing, recovery that
seals at the shortest responder, and recovery that proceeds below its quorum.

## How to run it

```sh
bazel test //src/rust/bitacora-model/...
```

or, with cargo, `cargo test -p bitacora-model` in `src/rust`. A failing check
prints the invariant and the steps that lead to its violation; the test
`old_writer_may_commit_on_acknowledgements_that_predate_the_fence` shows how
to replay such a trace step by step.

## Consequences

- A change to a protocol step or an invariant updates the model first, with
  a decision record, before the code (design, section 17).
- The checks are pure computation over a few million states, so this one
  crate is compiled with optimizations even in fast builds, in
  `bitacora-model/BUILD.bazel` and in the profile override of
  `src/rust/Cargo.toml`. They take a few seconds that way and over a minute
  without.
- The model and the deterministic simulation overlap by design: the model is
  exhaustive over a small abstract system, the simulation samples the real
  code under many more kinds of fault.
