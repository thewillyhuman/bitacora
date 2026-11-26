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

### What an answer to an append means

| Answer | What the writer does |
| --- | --- |
| Success, with the local end | Counts the replica as holding the segment up to there |
| Out of order, with the local end | Sends again from the local end plus one. The replica holds less than the writer assumed, typically after a request whose answer was lost |
| Fenced, or sealed | Has lost the stream (below). Sealed during the writer's own roll: sends again after a pause |
| Unavailable, another gRPC status, or nothing in time | Sends the same request again after a pause. Rolls if the replica stays silent |
| Any other refusal: no capacity, draining, storage, invalid request | Gives up on the replica for this segment and rolls |

A roll needs a standby replica set. A writer that wants to roll and finds
none keeps appending to its segment, as long as W replicas of it answer,
and looks again later: the controller places one.

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
later append and stops.

The writer's own roll is the exception. The roll's update seals the open
segment in the stream's record, and from then on the controller may tell
the segment's replicas, before the writer has opened the next segment: it
reads the metadata store once more in between, which takes seconds when
that read goes to an etcd member that does not answer. So while a roll is
under way, a writer does not take "sealed" from the replicas of the segment
it is leaving as a verdict. If another writer did take the stream
meanwhile, the roll finds out: its update is refused because the epoch
changed, or the replicas of the segment it opens are fenced. An append whose outcome it never learned is
**indeterminate**: the record may or may not be in the stream. The
application reads the log to find out.

### Unknown outcomes of the writer's own updates

The update of a roll can fail without an answer. Until the writer knows
whether it was applied, it does not know which segment is open, and commits
nothing. It makes the same update again until it gets an answer;
[metadata.md](metadata.md) says what a repeated roll finds. An answer that
the epoch changed means the stream is lost; any other refusal means the
roll did not happen, and the writer goes on in its segment.

### Stepping down

A writer that is closed on purpose:

1. stops accepting appends and waits until the durable sequence has reached
   the last record it accepted;
2. seals its segment at the durable sequence, with one conditional write;
3. clears its lease, with another;
4. tells the replicas the sealed end.

Its successor then finds no open segment and no lease, and goes from step 1
of a takeover to step 8 without waiting and without recovery. A writer that
cannot finish these steps just stops: its successor recovers the segment.

### Timings

None of these is part of the protocol; a writer or a reader may choose
others. They are what the client libraries use unless told otherwise.

| Setting | Default |
| --- | --- |
| Heartbeat to a replica when there is nothing to send | 50 ms |
| A replica is given up on after it was silent for | 10 s |
| A replica is given up on when it is behind the durable sequence by | 64 MiB |
| Appending waits when the uncommitted records exceed | 64 MiB |
| Batch frames in one append request | About 4 MiB |
| Timeout of a request to a replica | 5 s |
| Pause before a request is sent again | 100 ms |
| A segment is rolled at | 1 GiB, or one hour |
| Length of the writer's lease, and how often it is renewed | 10 s, about every 3 s |
| Records in one answer to a reader | About 1 MiB |
| A reader at the tail lets a replica hold its request for | 1 s |

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

What the steps leave to the candidate:

- **The lease.** A candidate that was not told to force the takeover reads
  the stream record before step 1. If `writer` is set and its `expires_at`
  is later than the candidate's own clock, the candidate waits until then
  and reads again, or gives up if that is longer than it may wait. A writer
  renews its lease well before it lapses. A lease that cannot be renewed
  does not stop a writer: only the epoch does.
- **A fence that is refused.** A replica that refuses the fence as fenced
  holds a higher epoch. Another candidate is ahead, and this one stops, as
  after a failed claim. Any other failure is a replica that did not answer.
- **More answers than needed.** With N − W + 1 answers the candidate may
  go on. It may also wait a moment for the others: every further answer
  can only raise the recovery end, and so keep more of what the old writer
  sent. The client libraries wait 100 ms.
- **The requests of the repair.** The read from the longest responder
  carries the recovery epoch E + 1, which lets it reach past the durable
  sequence to the end of the copy. The append to a shorter responder
  carries the segment's descriptor as it is in the stream record, with the
  segment's epoch E, the writer epoch E + 1, and a durable sequence of 0:
  the candidate knows none, and the seal that follows says what may be
  served. A responder without a copy gets one from that append.
- **No standby.** If step 8 finds no standby replica set, the controller
  has not placed one yet. The candidate keeps its epoch, waits and tries
  the open again.

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
