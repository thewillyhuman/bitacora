# Replication rules

This page states what a writer, a reader and a replica must do. It is the
contract the executable model (`src/rust/bitacora-model`) checks and the
simulator (`src/rust/bitacora-sim`) attacks. The messages are defined in
[wire.md](wire.md), the metadata records in [metadata.md](metadata.md).

A stream has **N** replicas per segment and commits at **W**
acknowledgements. Recovery must hear from **N − W + 1** replicas, so that at
least one of them holds every committed record. By default N = 3 and W = 2.

## Two structural rules

1. **One segment, one epoch.** A writer never appends to its predecessor's
   segment. It seals that segment and opens a new one. Within a segment there
   is therefore only one possible record at each sequence number; copies can
   differ only in how long they are.
2. **A replica set never changes while its segment is open.** When a replica
   fails, the writer seals the segment and continues in a new one on a
   healthy set. Sealed segments are repaired in the background.

## What must always hold

| | Invariant |
| --- | --- |
| I1 | A record reported as committed appears in every later read of the stream, until it is truncated |
| I2 | No two readers ever observe different records at the same sequence number |
| I3 | Every reader observes a gapless prefix, in order, and the end of what it may read never moves back |
| I4 | At most one epoch can commit records at any moment |
| I5 | No record above the lowest registered checkpoint is ever deleted |

## A replica

For each segment copy a replica keeps:

| State | Durability |
| --- | --- |
| The records, and so the **local end**: the last sequence number it holds | Flushed before an append is acknowledged |
| The **fenced epoch**: it refuses writers below it | Flushed before a fence is acknowledged |
| The **sealed end**, once it learns it | Flushed when learned |
| The **known durable sequence**: the latest value heard from the writer | Memory only; safe to lose |

It makes no decisions about ordering, membership or leadership. Its rules:

**Append** (a segment descriptor, the writer's epoch, batch frames, the
writer's durable sequence):

1. Refuse if the writer's epoch is below the fenced epoch, or below the
   segment's own epoch.
2. Refuse if the segment is sealed.
3. Verify the checksum of every frame; refuse if one fails.
4. The frames must continue the copy: the first must start at the local end
   plus one, or at the segment's first sequence number if there is no copy
   yet. If they start later, refuse and report the local end, so that the
   writer resends from there. Frames the copy already holds are skipped and
   acknowledged again; a resend whose batches are cut differently from the
   stored ones is refused.
5. Write the new frames, flush, and only then answer with the new local end.
6. Remember the durable sequence if it is higher than the known one.

An append without frames is a heartbeat: it carries the durable sequence,
and is answered with the local end.

**Read**: serve whole batches from the requested sequence number, up to the
sealed end if the segment is sealed, otherwise up to the lower of the local
end and the known durable sequence. Never serve an uncommitted record. The
one exception is the recovering writer, which may read to the local end
after it has fenced the copy at its own epoch.

**Fence** (an epoch): refuse if the epoch is below the fenced epoch.
Otherwise store it, flush, and answer with the local end and the sealed end
if known. A replica without a copy of the segment remembers the fence all
the same, and answers that it has no copy.

**Seal** (a sealed end): store it, flush, discard every record above it, and
answer with the local end. Refuse if a different sealed end is already
stored: that can only happen if the metadata store was bypassed.

A replica whose disk refuses a write or a flush stops. After a failed flush
the kernel may have dropped the dirty pages, and a retry could falsely
succeed.

## A writer

There is one writer per stream. It is a library inside the application.

### Appending

1. **Assign.** Give each record the next sequence number. The records of one
   call form one batch.
2. **Send.** Send each replica of the open segment the batches it has not
   acknowledged, in order, with the segment descriptor, the writer's epoch
   and the current durable sequence. One request is in flight per replica;
   the next one carries whatever accumulated meanwhile. There is no timer
   that delays a batch.
3. **Commit.** The durable sequence is the W-th highest local end
   acknowledged by the N replicas. Report appends as committed, in order, as
   it passes them.
4. **Publish.** The durable sequence travels with the next request, or with
   a heartbeat when the stream is idle.

A writer holds a bounded amount of unacknowledged records; beyond that,
appending waits.

### Rolling

The writer rolls to a new segment when a replica has not answered for a
while, is too far behind the durable sequence, is full or is draining, or
when the segment has reached its size or age limit.

1. Stop committing on the current segment: the durable sequence stays at D
   until the roll is over. Appends are still accepted, and records may still
   reach the old replicas, but none above D is reported as committed there.
2. Write one conditional update to the stream record: "the last segment is
   sealed at the durable sequence D, and a new segment starts at D + 1 on the
   standby replica set". It is conditional on the writer's epoch.
3. Tell the old replicas the sealed end.
4. Send every record above D to the new segment, under the same sequence
   numbers, encoded for the new segment.

Step 4 is safe because no record above D was reported as committed and the
writer still holds them all. Steps 3 and 4 do not depend on each other. The
client library does step 4 first, because appends are waiting for it; step 3
is best effort, and the controller tells a replica that missed it.

### Losing the stream

A writer that is refused as fenced, or whose conditional update fails
because the epoch changed, has lost the stream. So has a writer whose open
segment a replica reports as sealed: only a newer writer's takeover seals a
segment that its writer has not sealed itself. It fails every pending and
later append and stops. An append whose outcome it never learned is
**indeterminate**: the record may or may not be in the stream. The
application reads the log to find out.

## Taking over a stream

A new writer takes over by raising the epoch. Whoever does it last wins.
By default it first waits for the previous writer's lease to lapse.

1. **Claim the epoch.** Conditionally raise the stream's epoch from E to
   E + 1. If that fails, another candidate is ahead; stop.
2. **Read the segment list**, after step 1. If the last segment is sealed, or
   there is none, go to step 8. Otherwise let S be the last segment.
3. **Fence.** Send "fence S at E + 1" to every replica of S.
4. **Wait for N − W + 1 answers.** A replica without a copy counts, with a
   local end one below the segment's first sequence number. A copy that
   reports itself damaged does not count.
5. **Choose the end.** The recovery end is the highest local end among the
   answers. If an answer carries the segment's sealed end, left by an
   earlier recovery that was interrupted, that is the recovery end instead: a
   seal is never changed.
6. **Repair the tail.** Read the records the shorter responders lack from the
   longest one and append them to the others, under epoch E + 1, until every
   responder holds S up to the recovery end.
7. **Seal.** Conditionally, on the epoch still being E + 1: "S is sealed at
   the recovery end". Then tell the replicas.
8. **Open.** Conditionally, on the same epoch: create the next segment,
   starting one past the last sealed end, on the standby replica set. Begin
   appending.

Why it is safe:

- A committed record is on at least W replicas, so one of any N − W + 1
  holds it, and step 5 takes the maximum (I1).
- At least N − W + 1 replicas are fenced, so the old writer can collect at
  most W − 1 acknowledgements, and its metadata updates fail on the epoch
  (I4).
- S was written by epoch E alone, so its copies differ only in length (I2).
- A record the old writer sent but never saw committed may end up below the
  recovery end. That is the indeterminate outcome, and it is allowed.
- A replica that did not answer may hold a longer tail. Those records were
  on at most W − 1 replicas, so they were never committed. It discards them
  when it learns the sealed end.

Every step can be repeated. A recovery that stops halfway is started again
from step 1 by the next candidate, under a higher epoch. Two attempts may
compute different recovery ends, but only one seal is written.

## A reader

1. Read the stream record. A position below the low-water mark is
   **truncated**: fail with the low-water mark, never silently.
2. Find the segment that holds the position and ask one of its replicas,
   skipping copies marked bad. Stay with a replica that answers; move to the
   next when it fails or has nothing.
3. Verify the checksum of every batch received, and that the records start
   exactly at the position asked for.
4. At the sealed end of a segment, move to the next one.
5. If no replica serves a sealed segment and the stream record names an
   object for it, read the object ([tiered-object.md](tiered-object.md)).
6. If nothing serves the position, read the stream record again: the segment
   may have been sealed, repaired, uploaded or followed by another.

A reader following the tail asks a replica of the open segment to hold the
request until records are readable, and alternates replicas, so that one cut
off from the writer does not stall it.

## The controller

Everything the controller does is a pass: read the metadata store, look at
the replicas, and make conditional updates. A pass that is interrupted,
repeated or run twice at once does no harm.

- **Standby.** Keep one replica set placed for every stream's next segment.
- **Reconciliation.** For every sealed segment, compare each copy with the
  sealed end, telling replicas that missed the seal. A complete copy becomes
  good; a short, damaged or lost one becomes bad.
- **Repair.** Have a bad copy rebuilt from a good one, in place or on
  another replica that keeps the segment's spread.
- **Tiering.** See [tiered-object.md](tiered-object.md) and the transitions
  in [metadata.md](metadata.md).
- **Truncation.** Raise the low-water mark to the lowest of every registered
  materializer's checkpoint plus one and the position the owner asked for,
  held back to the start of the first segment that must stay: the open one,
  or one sealed more recently than the retention period. Remove the segments
  wholly below it. Deletion is by whole segments (I5).
- **Cleanup.** Remove copies no segment record refers to any more.
