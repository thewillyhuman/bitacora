# bitacora-testkit

An in-process bitacora cluster with switchable faults, for tests and for the simulator.

## What it owns

- **The cluster.** `Cluster`: storage nodes running the real replica code on in-memory disks, the
  in-memory metadata store they are registered in, the in-memory object store they upload sealed
  segments to, and one clock for all of them.
- **The machine around a replica.** `Node`: the disk and the identity that belongs to it, and what can
  happen to the machine: its process is killed, its unflushed bytes are lost or torn, its disk is
  replaced by an empty one, it starts again.
- **The network.** `Link` and `LinkFault`: the path to one node, which can be cut, slowed, or made to
  apply a request and lose its answer. The connector that resolves an address to a link.
- **Time.** `VirtualClock`: wall-clock time derived from the runtime's clock.
- **The shape of the cluster.** `FailureDomainLayout`: which zone and which host each node is in.

It owns no fault schedule and no workload. A test decides what goes wrong and when; `bitacora-sim`
decides it from a seed. It owns no code under test either: the replica, the metadata store and the
object store interface are the production ones.

## Design

**Replace the edges, run the real middle.** The production code is handed its disk, its network, its
stores and its clock as interfaces. The test kit implements those four and nothing else, so a test
exercises the code that ships. A fault is reported exactly as the real edge would report it: a cut link
is `ReplicaError::Unavailable`, a refused flush is an I/O error from the disk. Nothing in the code under
test can tell that it runs in a test.

**Nothing happens by itself.** The cluster starts no task and no timer, and holds no random state. A
node does not crash, restart or renew its registration unless the caller says so. That is what makes a
test's outcome a function of the test alone, and what lets the simulator own every random decision.

**One clock.** Timers run on the runtime's clock and timestamps are read from `VirtualClock`, which is
derived from it. Under a paused runtime a test advances both with one call, and a run takes no real
time.

**A request crosses a link in two steps.** `Link::carry_to_replica` takes the request to the far end
(cut, delay, node down, wrong identity), the replica runs it, and `Link::carry_back` brings the answer
back (lost answers). Each operation of the replica interface is those two calls around one call to the
replica, written out, so the path of a request can be read top to bottom. The link is looked at again
after a delay, because the world may have changed while the request was on its way.

**A node is a process and a storage, each recorded once.** Whether a replica runs is recorded in the
node's link, as `Running(replica)` or `Down`, because the link is what delivers to it. The disk and its
identity are recorded in the node and replaced together. A crashed node is `Down` on its old disk; a
node that lost its disk is `Down` on a new, empty one under a new identity; restarting is the same in
both cases.

**An address is not an identity.** A node keeps its address for life, but its identity belongs to its
disk: `replica-N`, then `replica-N-disk-1` after the first lost disk. The connector hands out links
that deliver only to the identity the caller asked for, so a rebuilt node never answers for the copies
its old identity held (`docs/decisions/0010-replica-identity.md`).

**Plain data in the public interface.** What a crash leaves on disk is a `CrashMode`, the fault of a
link is a `LinkFault`, the shape of a cluster is a `FailureDomainLayout`. No public function takes a
closure, so a test states its scenario in values a reader can see.

## Invariants

- The crate injects no fault on its own, starts no task and no timer, and draws no random number.
- Every fault of a link is reported as `ReplicaError::Unavailable`.
- A request refused on the way never reached the replica. A request whose answer was lost was applied
  in full.
- A replica runs behind a node's link if and only if the node is running.
- A node's identity changes exactly when its disk is lost, and never comes back. Its number, address,
  failure domains and link never change.
- The process is stopped before anything is done to its disk.
- `Cluster::nodes()[N]` is node `N`; nodes are started and registered in that order.
- No lock is held across an `await`.

## Files

| File | Holds |
| --- | --- |
| `src/lib.rs` | The crate's rationale and its public names |
| `src/cluster.rs` | `Cluster`: starting the nodes, the stores and the clock, renewing registrations |
| `src/layout.rs` | `FailureDomainLayout`: the zones and hosts of the nodes |
| `src/node.rs` | `Node`: crash, lost disk, restart, and looking at what its replica holds |
| `src/link.rs` | `Link` and `LinkFault`: the network path to a node, its faults, the process behind it |
| `src/connector.rs` | The connector that reaches nodes by address and checks the expected identity |
| `src/clock.rs` | `VirtualClock`: wall-clock time that follows the runtime's clock |
| `src/test_support.rs` | Fixtures shared by the tests; compiled for tests only |
| `src/<file>_test.rs` | The tests of the file of the same name |

## Dependencies

Of the bitacora crates it depends on `bitacora-types` (identifiers, the replica interface, the clock
interface), `bitacora-segment` (the in-memory disk), `bitacora-artifact` (the in-memory object store),
`bitacora-replica` (the replica it runs) and `bitacora-meta` (the metadata store it registers in), and
must stay that way. It must not depend on `bitacora-client`, `bitacora-controller` or anything above
them: those crates use the test kit in their own tests, and a dependency back would be a cycle. It is
never linked into a shipped binary.
