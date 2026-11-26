# bitacora-controller

The controller: the slow, cluster-wide work that keeps a bitacora cluster healthy over months.

## What it owns

- `Controller`: placement of standby segments, reconciliation and repair of the copies of sealed
  segments, tiering to the object store, truncation and retention, removal of leftover copies, and the
  periodic backup of the metadata store. `run` loops for ever, `run_round` does one round of that loop
  (take or renew the leadership, then one pass), `run_pass` does one pass over the cluster.
- `ControllerSettings`: how the controller paces itself and how it judges replicas.
- `PassReport` and `Round`: what one pass and one round did, for the process that hosts the controller.

It does not own the process (that is `bita-controller`), the rules of placement (`bitacora-meta`), or
anything a replica does when it is asked to seal, repair, upload or delete (`bitacora-replica`). It is
never on the append path: while no controller runs, streams keep appending and reading, and writers keep
rolling until the standby segments run out.

## Design

Everything the controller does is a **pass**: read the metadata store, ask the replicas what they hold,
and make conditional writes that bring the two closer to what they should be. A pass depends on nothing
the controller remembers from the pass before, so a controller can be stopped, restarted or replaced at
any point.

A pass starts with a **survey**: the registry of replicas, and what every live replica says it holds.
Everything the pass decides about replicas, it decides from that one picture. Then, for every stream, the
steps run in a fixed order:

1. **Standby**: make sure a replica set is placed for the stream's next segment.
2. **Reconciliation**: compare every copy of every sealed segment with the sealed end, and record it as
   good or bad.
3. **Repair**: have bad copies rebuilt from a good one.
4. **Tiering**: have segments that are ready uploaded to the object store.
5. **Release**: once the hot-read window is over, leave a tiered segment to the object store alone.
6. **Truncation**: advance the low-water mark and delete the segments below it.
7. **Deletion of tiered segments** below the mark: the object first, the record second.

After the streams, once per pass, the **leftover sweep** removes segment copies that no record refers to
any more, and a **metadata snapshot** is written to the object store if one is due.

Three decisions shape the code.

*No locks, only conditions.* Every step reads the stream's record again before it decides, and writes
with `MetadataStore::update_stream`, whose change is applied only if the part of the record the step
looked at is still what it saw. Two controllers that both believe they lead therefore cannot corrupt
anything; leadership only avoids duplicate work.

*Rules are pure functions.* Whether a standby may stay, what state a copy is in, when a segment is
uploaded, how far the mark may go, whether a copy is a leftover: each is a function of records and a
time, with no I/O, and is tested on hand-built records. The methods of `Controller` around them only
fetch, ask and write.

*Failures are left for the next pass.* A replica or an object store that refuses or does not answer in
time never fails a pass. It is logged, nothing is recorded, and the next pass finds the same work to do.
Only a metadata store that cannot be listed fails a pass, and a metadata store that fails in the middle
of one stream costs that stream this pass and no other.

Nothing here is a hot path. The code is written to be read.

## Invariants

- Every write to the metadata store is conditional on what the pass saw. A pass that is interrupted,
  repeated, or run by two controllers at once does no harm.
- The controller never touches an open segment. Only the segment's writer does.
- A copy is recorded as good only on evidence from its own replica, in the same pass.
- A segment's record never points at an object the controller has not itself seen in the object store,
  and local copies are released only while that object is present with its recorded size.
- A local copy is deleted only once no record refers to it: its segment's record is gone or names
  other replicas, or its stream was deleted. An object is deleted only once its segment is below the
  low-water mark or its stream was deleted, and before the record that names it, so no object is
  orphaned.
- Copies of a stream the metadata store knows nothing about are left alone, and so is a copy of an
  unlisted segment that is newer than what the stream lists and holds records at or above the low-water
  mark. They are what a metadata store restored from an older snapshot is rebuilt from.
- Every request to a replica or to the object store has a time limit.
- The order of the steps, and of the requests within a step, is part of the behaviour that the
  simulator replays. Changing it changes the history of every seed.
- The crate knows nothing of networks, disks or clocks: it reaches replicas, the object store and the
  time through the interfaces it is given.

## Files

| File | Holds |
| --- | --- |
| `src/lib.rs` | The crate's rationale and its public names |
| `src/controller.rs` | `Controller`, the pass and the order of its steps, and the plumbing every step shares |
| `src/controller/settings.rs` | `ControllerSettings` and its defaults |
| `src/controller/report.rs` | `PassReport`, `Round`, and which segments count as missing copies |
| `src/controller/round.rs` | Leadership and the loop around the pass: `run` and `run_round` |
| `src/controller/survey.rs` | The picture of the replicas a pass works from |
| `src/controller/standby.rs` | Keeping a replica set placed for every stream's next segment |
| `src/controller/reconciliation.rs` | Judging the copies of sealed segments good or bad |
| `src/controller/repair.rs` | Having bad copies rebuilt, in place or on another replica |
| `src/controller/tiering.rs` | Upload to the object store, release of local copies, deletion of objects |
| `src/controller/truncation.rs` | The low-water mark, retention, and deleting segments below the mark |
| `src/controller/leftovers.rs` | Removing copies no record refers to, and forgetting deleted streams |
| `src/controller/snapshot.rs` | The periodic backup of the metadata store |
| `src/controller/test_support.rs` | The fixtures the tests of the steps share |
| `src/<file>_test.rs` | The tests of the file of the same name |

## Dependencies

It depends on `bitacora-types` (identifiers, the clock, and the interface to a replica), `bitacora-meta`
(the metadata store, its records and placement) and `bitacora-artifact` (the interface to the object
store), and on nothing else of the workspace. It must not depend on `bitacora-replica` or
`bitacora-client`: it talks to replicas through `ReplicaApi` only, which is what lets the same controller
run against in-process replicas, the simulator and gRPC. Its tests may use `bitacora-testkit`,
`bitacora-client`, `bitacora-replica` and `bitacora-segment` to build a cluster to run passes against.

`docs/protocol/replication.md`, "The controller", describes the same passes for readers who do not read
Rust. A change to what a pass does changes that document in the same commit.
