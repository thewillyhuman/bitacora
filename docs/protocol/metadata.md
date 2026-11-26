# The metadata store

The metadata store is the one place where bitacora needs consensus. It holds
small records that change rarely: per stream, per materializer, per replica.
It is never on the append path.

## What is required of the store

A key-value store with:

- linearizable reads of one key and of a key prefix (a prefix read is one
  consistent snapshot);
- a **revision** per key that changes on every write of that key and is never
  repeated, not even after the key is deleted and created again;
- transactions: a set of conditions ("key K is absent", "key K is at revision
  R") and a set of writes and deletes that are applied atomically if and only
  if every condition holds.

Version 1 uses etcd. Keys below are relative to the cluster's prefix, by
default `/bitacora/`.

**Every change to a record that a decision was read from is a transaction
conditional on the revision at which the record was read.** A program reads
a record, computes the new one, and writes it only if nobody wrote in
between; otherwise it reads again. This is what makes every rule in
[replication.md](replication.md) that says "one conditional write" hold.

Three writes carry no condition, because nothing is decided from what they
replace: a replica writing its own record, the removal of a replica's
record, and the removal of a deleted-stream record. Restoring a snapshot
over a store that holds keys is the fourth. It is an operator's procedure,
run while nothing else uses the store.

## Keys

| Key | Value | Written by |
| --- | --- | --- |
| `streams/<name>` | Stream record | Writers, controller, administration |
| `checkpoints/<stream id>/<materializer>` | Checkpoint record | Materializers |
| `replicas/<replica id>` | Replica record | Replicas |
| `controller/leader` | Leader record | Controllers |
| `counters/stream-id` | Last stream identifier handed out, as a JSON number | Stream creation, and the adoption of a rebuilt stream with a higher identifier |
| `deleted-streams/<stream id>` | Deleted-stream record | Stream deletion, controller |

Values are JSON objects in UTF-8. Identifiers, sequence numbers and epochs
are JSON numbers. Timestamps are milliseconds since the Unix epoch, as JSON
numbers. A replica identifier is a string of 1 to 128 ASCII letters, digits,
`.`, `_` or `-`. An absent optional field is `null`.

### On etcd

Every program of a cluster meets the requirements above in the same way:

| Requirement | On etcd |
| --- | --- |
| A key | The cluster's prefix followed by the key of the table, with nothing in between: `/bitacora/streams/orders`. The prefix ends in `/` |
| A value | The record's JSON, as UTF-8 bytes. `counters/stream-id` is a JSON number alone, such as `7` |
| The revision of a key | Its `mod_revision` |
| "Key K is absent" | `create_revision(K) = 0` |
| "Key K is at revision R" | `mod_revision(K) = R` |
| A transaction | One `Txn`. The conditions are its comparisons, the writes and deletes are the operations of its success branch, and its failure branch is empty. An answer with `succeeded = false` means that a condition did not hold and nothing was written |
| A read of one key or of a prefix | One `Range`, linearizable, which is etcd's default. A prefix read returns the keys in order |

A request that fails or times out may or may not have been applied. The
caller reads the record again and decides from what it finds.

### Writing and reading a record

- An optional field without a value is written as `null`. A reader treats
  one that is missing the same way.
- A list is written as a JSON array, `[]` when it is empty, never `null`.
  The one list that can be `null` is `standby_replicas`, where `null` means
  that no replica set is placed.
- A program ignores a field it does not know, and refuses a record whose
  `schema_version` is higher than the highest it knows.
- A program that changes a record writes back the fields it knows. A field
  it does not know is lost on the way, which is why a field that must not
  be lost is introduced together with a higher `schema_version`.
- A higher `schema_version` therefore takes two releases: the first one can
  read it, and the next one writes it. Programs of the first release keep
  working while those of the second are rolled out.

## Stream record

```json
{
  "schema_version": 1,
  "stream_id": 7,
  "name": "orders",
  "replication": {"replica_count": 3, "write_quorum": 2},
  "retention": {"keep_sealed_for_millis": null},
  "epoch": 12,
  "writer": {"writer_identity": "compute-2", "expires_at": 1795000000000},
  "segments": [
    {
      "segment_id": 41,
      "epoch": 11,
      "first_sequence": 9001,
      "sealed": {"end": 9500, "at": 1794999990000},
      "copies": [
        {"replica_id": "replica-a", "state": "good"},
        {"replica_id": "replica-b", "state": "good"},
        {"replica_id": "replica-c", "state": "unverified"}
      ],
      "tiered": null
    },
    {
      "segment_id": 42,
      "epoch": 12,
      "first_sequence": 9501,
      "sealed": null,
      "copies": [
        {"replica_id": "replica-a", "state": "unverified"},
        {"replica_id": "replica-b", "state": "unverified"},
        {"replica_id": "replica-d", "state": "unverified"}
      ],
      "tiered": null
    }
  ],
  "next_segment_id": 43,
  "standby_replicas": ["replica-b", "replica-c", "replica-d"],
  "truncate_before": null,
  "low_water_mark": 9001,
  "materializers": ["pages"]
}
```

| Field | Meaning |
| --- | --- |
| `epoch` | The current epoch. Only the writer that raised it to this value may change the segment list for writing |
| `writer` | The current writer's lease. A courtesy: it keeps a second writer from taking a healthy stream by accident. Correctness depends on `epoch` alone |
| `segments` | Oldest first. At most the last one is open (`sealed` is `null`) |
| `segments[].epoch` | The only epoch that authors records in the segment |
| `segments[].sealed.end` | The final sequence number. One below `first_sequence` if the segment was sealed empty |
| `segments[].copies` | The replica set. Fixed while the segment is open. `state` is `unverified`, `good` or `bad`, and is only meaningful once the segment is sealed |
| `segments[].tiered` | `{"object_key", "size_bytes", "uploaded_at", "are_local_copies_dropped"}` once the segment is in the object store. When `are_local_copies_dropped` is true, `copies` is empty and the object is the only copy |
| `next_segment_id` | The identifier the next segment gets. Never decreases, so identifiers are not reused after segments are deleted |
| `standby_replicas` | A replica set placed in advance for the next segment, so that opening one does not wait for placement |
| `truncate_before` | A position the stream's owner asked to truncate before |
| `low_water_mark` | The first sequence number that can still be read. Never decreases |
| `materializers` | The registered materializers. Each one pins the log at its checkpoint |

### Transitions

Each of these is one conditional write of the stream record.

| Transition | Condition besides the revision | Effect |
| --- | --- | --- |
| Claim the epoch | `epoch` is still the value read | `epoch` + 1, `writer` set |
| Renew or release the lease | `epoch` is the caller's | `writer.expires_at` moved, or `writer` cleared |
| Seal the last segment | `epoch` is the caller's; the segment is the last and open | `sealed` set |
| Open a segment | `epoch` is the caller's; the last segment is sealed; `standby_replicas` is set | New segment with identifier `next_segment_id`, epoch `epoch`, first sequence "previous sealed end + 1" (or `low_water_mark` if there is no segment), copies = `standby_replicas`; `next_segment_id` + 1; `standby_replicas` cleared |
| Roll | As seal and open together | Both effects in one write |
| Place a standby | `standby_replicas` is what the controller saw | `standby_replicas` set |
| Judge or repair a copy | The copy is the one the controller saw | Its `state`, or its `replica_id` and `state`, changed |
| Record an upload | The segment is listed and not tiered | `tiered` set |
| Release a tiered segment | `tiered` is the upload that was verified | `copies` emptied, `are_local_copies_dropped` true |
| Truncate | `materializers` is what the controller saw | `low_water_mark` raised; sealed, untiered segments wholly below it removed |
| Remove a tiered segment | Its object was deleted first | The segment removed |
| Ask for truncation | None | `truncate_before` raised to the position asked for. A position at or below the recorded one changes nothing: it only moves forward |

A write whose answer was lost is made again, so a writer's transitions say
what happens when they find their own effect already in the record:

| Transition | Found in the record | Then |
| --- | --- | --- |
| Seal | The segment is sealed at the same end | Nothing is written; the seal succeeded |
| Seal | The segment is sealed at another end | Refused. A seal is never changed |
| Open | The last segment is open under the caller's epoch | Nothing is written; that segment is the answer |
| Roll | The last segment is the one after the segment to seal, open under the caller's epoch | Nothing is written; the roll happened, and that segment is the answer |

A claim of the epoch is the exception. A candidate that did not learn
whether its claim was applied cannot tell its own claim from another
candidate's, so it starts over and claims the next epoch.

A segment can be sealed empty, one below its `first_sequence`, and never
lower than that.

### Creating a stream

A stream's name, like a materializer's, is 1 to 255 ASCII letters, digits,
`.`, `_` or `-`, so that it can be part of a key. `replication.write_quorum`
is at least 1 and at most `replication.replica_count`.

Creating a stream is one transaction, conditional on `streams/<name>` being
absent and on `counters/stream-id` being at the revision read, or absent if
no stream was ever created. It writes:

- `counters/stream-id`: the identifier read plus one, 1 for the first stream;
- `streams/<name>`: a record with that identifier, `epoch` 0, `writer`
  `null`, no segments, `next_segment_id` 1, `low_water_mark` 1,
  `truncate_before` `null` and no materializers.

`standby_replicas` is `null`, or a replica set the creator chose by the
placement rules below. With `null`, the stream can be opened once the
controller has placed a standby, which it does in its next pass; a writer
that finds none waits and tries again.

### Registering a materializer

One transaction, conditional on the revision of the stream record: the name
is added to `materializers`, which is kept sorted, and
`checkpoints/<stream id>/<materializer>` is written with `checkpoint` one
below the stream's `low_water_mark`, `fencing_token` 0 and `manifest`
`null`. Because both are written together, the low-water mark cannot pass
a materializer between its registration and its first checkpoint.
Registering a name that is registered already changes nothing; in
particular, the checkpoint is not reset.

Unregistering removes the name and deletes the checkpoint record, in one
transaction. Unregistering a name that is not registered changes nothing.

## Checkpoint record

```json
{
  "schema_version": 1,
  "materializer": "pages",
  "checkpoint": 9000,
  "fencing_token": 4,
  "manifest": "…"
}
```

`checkpoint` is the materializer's durable statement "everything up to this
sequence number is in my artifacts". `manifest` is opaque to the log; it is
where the materializer says which artifacts those are.

An instance takes over a materializer by raising `fencing_token` by one, in a
conditional write. It commits a checkpoint with a conditional write that
also requires `fencing_token` to be the one it took. Two instances can
therefore run by mistake, but only the last one to take the token can
commit. `checkpoint` never decreases.

## Replica record

```json
{
  "schema_version": 1,
  "replica_id": "replica-a",
  "address": "10.0.0.11:7000",
  "failure_domains": ["zone-1", "rack-4", "host-11"],
  "capacity_bytes": 960000000000,
  "free_bytes": 412000000000,
  "is_draining": false,
  "heartbeat_at": 1795000000000
}
```

A replica writes its record when it starts and again on a timer. A replica
whose `heartbeat_at` is older than the liveness window receives no new
segments. Its sealed copies are given up, and rebuilt elsewhere, only after
it has been silent for much longer, so that a restart does not move data.

`failure_domains` are labels from the widest to the narrowest. Placement
never puts two copies of a segment on replicas that share their narrowest
label, and spreads over the wider ones when there are enough of them.

## Leader record

```json
{"holder": "controller-1", "expires_at": 1795000010000, "leading_since": 1794999000000}
```

A controller leads if it wrote this record and it has not expired, and takes
over when it has. `leading_since` is when the holder began to lead without a
lapse; a renewal keeps it. It may be absent in a record written by an older
controller. Leadership only avoids duplicate work: every write a
controller makes is conditional on what it read, so two controllers that
both believe they lead cannot corrupt anything.

## Deleted-stream record

```json
{"stream_id": 7, "name": "orders"}
```

Deleting a stream removes its stream record and its checkpoint records and
writes this note, in one transaction. The note tells the controller to
remove the stream's segment copies from every replica and its objects from
the object store; it deletes the note once every replica that could hold a
copy has answered and holds none, and the object store holds nothing of the
stream.

Segment copies of a stream that has neither a stream record nor a note are
left alone. They are what a metadata store restored from an older snapshot
would be rebuilt from.
