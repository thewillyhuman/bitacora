# 0008. Simulation runs the real code over injected interfaces

Status: accepted · 2026-11-25

## Context

The design asks for deterministic simulation: the real client, replica and
controller code, run many times under injected faults, with a seed that
replays any run exactly (section 15, layer 2). It names madsim or turmoil as
the runtime that would replace the network, the disk and the clock
underneath unchanged code.

## Decision

The simulator does not replace anything underneath the code. Every crate
takes what it needs from the outside world as an interface, and the
simulator passes implementations it controls:

| Interface | Production | Simulation |
| --- | --- | --- |
| `Disk`, `DiskFile` | A local directory | Memory, with visible and durable content kept apart, so a crash loses exactly what was not flushed |
| `ReplicaConnector`, `ReplicaApi` | gRPC | In-process links that can be cut, slowed, or made to lose answers |
| `KeyValueStore` | etcd | Memory, with outages |
| `ArtifactStore` | An object store | Memory, with outages |
| `Clock` | The system clock | A clock tied to the runtime's virtual time |

Timers use `tokio::time`, which is virtual when the runtime is paused, and a
run uses a current-thread runtime, so tasks interleave only at `await`
points and in an order the runtime decides.

Why not madsim or turmoil. Both intercept at the level of sockets and
files, which would have forced the storage and transport layers to be
written against their shims, and would have tied the build to their forks
or feature flags of tokio. Interfaces were needed anyway, for unit tests and
for the choice of metadata store and object store. With them in place, a
simulated world is ordinary Rust in a test crate.

### Two things that made it deterministic

- The workspace's lints forbid the system clock, threads, direct file and
  socket access, and hash maps outside the few adapter modules. Iteration
  order and time therefore never depend on the machine.
- tokio makes some choices at random, for example which waiter of a `watch`
  channel wakes first. The simulator seeds them with
  `Builder::rng_seed`, which needs tokio built with `--cfg tokio_unstable`.
  The flag is set for the whole workspace, in `MODULE.bazel` for Bazel and
  in `src/rust/.cargo/config.toml` for Cargo.

One in-tree random number generator (`DeterministicRandom`, SplitMix64)
drives every choice of the simulation itself.

### Checking

Clients record what they did and saw in a history. A checker reads the
history and reports every violated invariant: a committed record that a
later read does not return, two different records at one sequence number, a
reader that saw a gap or went back, truncation past a checkpoint. After the
faults stop, the run also requires the cluster to recover: a writer opens
and commits, an audit reads the whole stream, and every sealed segment is
back at a full set of good copies or in the object store.

Each test run covers a fixed set of seeds; `BITA_SIM_SEEDS` widens it and
`BITA_SIM_SEED` replays one. A seed that finds a bug is kept as a regression
seed with a note of what it found.

## Consequences

- The simulator tests the protocol and the storage logic, not tonic, etcd or
  the kernel. Those are covered by tests against the real things: gRPC on
  loopback, a real etcd, real processes.
- A new way to reach the outside world needs an interface, or the lints
  refuse it. That is deliberate.
- The simulator found, among others: segment files that could exist without
  a valid header, a reader that spun without letting time advance, a rebuilt
  node answering for its old identity, and reuse of segment identifiers
  after truncation.
