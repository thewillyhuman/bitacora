# bitacora-testports

Loopback ports for tests that start real servers.

## What it owns

- `PortBlock`: fifty neighbouring loopback ports that belong to whoever claimed them, across processes,
  until the block is dropped.
- `take_shared_port`: the next port of one block that a whole test binary shares.
- The range the Rust tests take their ports from, 29000 to 30849.

It does not start anything. A test takes a port here and gives it to the server it starts.

## Design

A program that a test starts (etcd, an S3 server, a bitacora daemon) opens its own sockets, so the test
must name a free port for it. Binding port zero, reading the port and closing the socket finds a free
port but does not keep it: until the program has bound the port, the operating system may give it to the
next caller that asks for "any port", and tests that run in parallel all ask in the same instant. A test
of the cluster kit failed that way in 6 of 30 parallel runs.

**A range the operating system does not pick from.** Linux hands out ports from 32768 on and macOS from
49152 on. The blocks lie below both, so only a program that names a port can be in the range.

**The operating system keeps owners apart.** The first port of a block is its guard port. Claiming a
block is listening on its guard port on `127.0.0.1` and keeping the listener; only one socket on the
machine can do that. No lock file is left behind by a test that is killed, because a listener closes
with its process.

**Each port is checked before it is handed out.** A server that outlived a killed test may still listen
inside a block. Its port cannot be bound, and is passed over.

**Blocks are scarce, so a test binary shares one.** There are thirty-seven. A binary whose tests each
need a port or two takes them with `take_shared_port`, from one block it keeps until it ends. A test that
starts a whole cluster claims a block of its own and drops it with the cluster.

**A claim does not wait.** When every block is taken, `PortBlock::claim` panics and says so. That takes
more than thirty-seven test binaries that start servers at the same moment.

## The ranges of the repository

Three test suites take ports this way. Each has a range of its own, so they need no common rule.

| Range | Used by |
| --- | --- |
| 20000 to 28999 | `development/cluster_test.sh` |
| 29000 to 30849 | Rust tests, through this crate |
| 30850 to 32699 | Go tests, through `src/go/internal/testcluster/ports.go` |

## Invariants

- While a `PortBlock` lives, no other `PortBlock` in any process hands out one of its ports.
- A block hands out each of its ports at most once, and never its guard port.
- A port that is handed out could be bound a moment before.
- A server that is given one of these ports listens on `127.0.0.1`. The guard keeps out only those who
  bind the same address.

## Files

| File | Holds |
| --- | --- |
| `src/lib.rs` | The crate's rationale and its public names |
| `src/port_block.rs` | `PortBlock`, `take_shared_port`, the range and the order blocks are tried in |
| `src/port_block_test.rs` | The tests of `port_block.rs` |

## Dependencies

It depends on nothing but the standard library, and must stay that way: the tests of the lowest crates
use it. Only tests depend on this crate.
