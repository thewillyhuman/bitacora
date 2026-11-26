# The `bita` command-line tool

`bita` is what an operator uses to look at a bitacora cluster and to carry out the routine procedures of
the design (section 14): create and inspect streams, drain a replica before a restart, remove a failed
one, see which controller leads, and recover a lost metadata store. Load testing has a tool of its own,
`bita-bench`, described in `benchmark.md`.

Commands are `bita <noun> <verb>`. `bita help`, `bita <noun> --help` and `bita <noun> <verb> --help`
describe every option. A test keeps this page and the tool in step: every command has a section here,
and every section here is a command.

## Finding the cluster

A cluster is found through its metadata store, so the tool needs the etcd endpoints. They come from a
configuration file, from options, or from the environment.

| Source | Configuration file | etcd endpoints | etcd key prefix |
| --- | --- | --- | --- |
| Option | `--config <FILE>` | `--etcd-endpoint <URL>`, repeated | `--etcd-key-prefix <PREFIX>` |
| Environment | `BITA_CONFIG` | `BITA_ETCD_ENDPOINTS`, separated by commas | `BITA_ETCD_KEY_PREFIX` |

For each value an option wins over the environment, and both win over the file. The key prefix defaults
to `/bitacora/`. Without any endpoint the tool stops with a usage error: it never guesses which cluster
to operate on.

The configuration file is TOML. It is the only way to name the object store, which `stream read` and
`stream tail` need for segments that replicas no longer hold:

```toml
etcd_endpoints = ["http://10.0.0.1:2379", "http://10.0.0.2:2379", "http://10.0.0.3:2379"]
etcd_key_prefix = "/bitacora/"

[object_store]
kind = "local_directory"
path = "/var/lib/bitacora/objects"
```

A misspelled setting is refused, not ignored.

## Output and exit codes

Results are printed to standard output as aligned text. `--output json` prints the same result as JSON
with stable field names, for scripts. Errors go to standard error, one line starting with `bita:`.

| Exit code | Meaning |
| --- | --- |
| 0 | The command succeeded. |
| 1 | The command failed: the cluster refused it or could not be reached. |
| 2 | The command line or the configuration is wrong. Nothing was sent. |

`--request-timeout <DURATION>` bounds every request to a replica (default `5s`). Durations are a whole
number and a unit: `250ms`, `30s`, `15m`, `24h`, `7d`.

## Streams

### `bita stream create`

Creates a stream and places its first segment, so that a writer can open it at once. If too few replicas
are registered, the stream is created anyway and the controller places the segment as soon as it can.

```console
$ bita stream create orders --replicas 3 --write-quorum 2 --keep-sealed-for 7d
NAME    ID  EPOCH  WRITER  REPLICATION  SEGMENTS  LOW-WATER
orders  1   0      -       3/2          0         1
```

`--replicas` (default 3) is how many replicas hold each segment, `--write-quorum` (default 2) how many
must store a record before it is committed. `--keep-sealed-for` keeps every sealed segment at least that
long; without it, records are deleted as soon as truncation allows.

### `bita stream list`

Lists every stream, sorted by name.

```console
$ bita stream list
NAME    ID  EPOCH  WRITER     REPLICATION  SEGMENTS  LOW-WATER
audit   2   1      -          3/2          4         1
orders  1   4      compute-1  3/2          12        1500
```

`WRITER` is the identity of the writer that holds the stream. `LOW-WATER` is the first sequence number
that can still be read.

### `bita stream describe`

Shows everything the metadata store knows about one stream, and the checkpoints of its materializers.

```console
$ bita stream describe orders
name:             orders
id:               1
epoch:            4
writer:           compute-1 (lease until 2026-11-25T22:03:44Z)
replication:      3 replicas, write quorum 2
retention:        sealed segments kept at least 604800s
low-water mark:   1500
truncate before:  -
segments:         12
next segment id:  40
standby replicas: replica-a replica-b replica-c

MATERIALIZER  CHECKPOINT  TOKEN  MANIFEST
pages         1499        2      -
```

### `bita stream delete`

Deletes a stream. The controller then removes its segments from the replicas and the object store. This
cannot be undone, so the command does nothing without `--yes`.

```console
$ bita stream delete orders
bita: deleting stream "orders" deletes all of its records and cannot be undone; pass --yes to confirm
$ bita stream delete orders --yes
stream orders deleted; the controller removes its segments
```

### `bita stream truncate-before`

Allows every record below a sequence number to be deleted. The position only moves forward. Records are
deleted by the controller, whole segments at a time, and only as far as the registered materializers'
checkpoints and the retention policy permit.

```console
$ bita stream truncate-before orders 1500
stream orders may be truncated before sequence 1500; the controller deletes whole segments below it
```

### `bita stream register-materializer`

Registers a materializer on a stream. From then on the log keeps every record above the materializer's
checkpoint: a registered materializer that stops checkpointing pins the log until it is unregistered.

```console
$ bita stream register-materializer orders pages
MATERIALIZER  CHECKPOINT  TOKEN  MANIFEST
pages         0           0      -
```

### `bita stream unregister-materializer`

Unregisters a materializer, so that it no longer holds records back.

```console
$ bita stream unregister-materializer orders pages
materializer pages unregistered from stream orders
```

### `bita stream append`

Becomes the stream's writer, appends records and steps down. Each argument is one record; without
arguments, every line of standard input is one record.

This is a smoke test, not a way to add records to a stream an application writes: a stream has one
writer, and this command takes its place. By default it waits for a live writer's lease to expire;
`--force` takes the stream at once and fences the writer that held it.

```console
$ bita stream append orders "first record" "second record"
appended 2 records to orders at sequences 1 to 2 under epoch 1
$ printf 'third\nfourth\n' | bita stream append orders
appended 2 records to orders at sequences 3 to 4 under epoch 2
```

### `bita stream read`

Reads committed records and stops. `--from` is the first sequence number to read (default: the oldest
record still kept), `--count` how many to read at most (default 100).

```console
$ bita stream read orders --from 2 --count 2
SEQUENCE  PAYLOAD
2         second record
3         third
```

Payloads are opaque bytes. The text shown replaces bytes that are not UTF-8 and escapes control
characters; `--output json` adds `payload_hex`, which is exact.

### `bita stream tail`

Follows a stream: prints every committed record as it commits, across segment rolls and writer changes,
until interrupted. `--from` is where to start (default: the oldest record still kept); `--count` stops
after that many records.

```console
$ bita stream tail orders --from 3
3  third
4  fourth
```

With `--output json`, every record is one JSON object on its own line.

## Replicas

### `bita replica list`

Lists the registered replicas. It reads the registry only, so it works while replicas are down.

```console
$ bita replica list
REPLICA    ADDRESS        ALIVE  HEARTBEAT  USED  CAPACITY  DRAINING  DOMAINS
replica-a  10.0.0.1:7000  yes    3s ago     41%   2.0 TiB   no        zone-a/rack-3
replica-b  10.0.0.2:7000  yes    1s ago     39%   2.0 TiB   no        zone-b/rack-1
replica-c  10.0.0.3:7000  no     9m ago     40%   2.0 TiB   no        zone-c/rack-7
```

A replica is alive if it renewed its registration within the last 15 seconds, the window placement uses
to decide whether it may receive new segments.

### `bita replica status`

Asks one replica what it reports about itself.

```console
$ bita replica status replica-a
replica:        replica-a
address:        10.0.0.1:7000
capacity:       2.0 TiB
free:           1.1 TiB
draining:       no
segment copies: 17
```

### `bita replica drain`

Drains a replica before maintenance. The replica refuses new appends from then on, and each writer that
meets the refusal rolls to its standby segment. The command reports the open segments, and the standby
sets, that still name the replica. `--wait <DURATION>` keeps looking until there are none.

```console
$ bita replica drain replica-a
replica replica-a is draining; writers roll away from it as they append

STREAM  OPEN SEGMENT
orders  39

streams whose standby set still names it: audit
$ bita replica drain replica-a --wait 2m
replica replica-a is draining and no open segment names it: it can be stopped
```

A writer rolls when it next appends, so an idle stream keeps its open segment on the replica; stopping
the replica then makes that writer roll, which is what a restart does anyway.

### `bita replica remove`

Removes a failed replica from the registry. The controller then treats its copies as lost and re-creates
them on other replicas. A replica that is still alive is refused unless `--force` is given: removing it
makes repair copy data that was never lost.

```console
$ bita replica remove replica-c
replica replica-c removed from the registry; the controller re-creates its copies
```

Nothing is deleted from the replica's disk. A replica that comes back with its data registers again.

## Segments

### `bita segment list`

Lists the segments of a stream, oldest first, as the metadata store records them.

```console
$ bita segment list orders
SEGMENT  EPOCH  FIRST  END   STATE     COPIES                                                          OBJECT
37       4      1400   1449  released                                                                  streams/1/segments/00000000000000000037 (48.2 KiB)
38       4      1450   1499  tiered    replica-a=good replica-b=good replica-c=good                    streams/1/segments/00000000000000000038 (51.0 KiB)
39       4      1500   -     open      replica-a=unverified replica-b=unverified replica-c=unverified  -
```

| State | Meaning |
| --- | --- |
| `open` | The writer is appending to it. |
| `sealed` | Complete and immutable, on replicas only. |
| `tiered` | Also uploaded to the object store; replicas still hold it. |
| `released` | In the object store alone; the replicas dropped their copies. |

A segment that lies entirely below the low-water mark is marked `(awaits deletion)`. Copy states are
what the controller last recorded: `unverified`, `good` or `bad`.

## Controller

### `bita controller status`

Shows which controller instance leads, since when, and until when its leadership is valid without
renewal.

```console
$ bita controller status
leader:      controller-1
state:       leading
since:       2026-11-25T20:00:00Z (2h)
valid until: 2026-11-25T22:03:54Z
```

If the state is `lapsed`, no controller leads: placement of new segments, repair, tiering and deletion
are paused, while appends and reads continue.

## Recovering the metadata store

The metadata store says which segments make up a stream. If every etcd member is lost, the records are
still on the replicas and in the object store, and these commands put the metadata back. The procedure:

1. Stop the writers and the controllers. Leave the replicas running.
2. Start an empty etcd.
3. If the cluster has an object store, restore the newest snapshot: `bita metadata restore latest`.
4. Look at what the replicas hold beyond the snapshot: `bita metadata rebuild`.
5. If the plan is right, write it: `bita metadata rebuild --apply`.
6. Start the controllers, then the writers.

Without an object store there are no snapshots, and the procedure starts at step 4. Say so in steps 4
and 5 with `--no-object-store`: the tool cannot tell a cluster that has no object store from a
configuration that leaves it out, and treats a missing one as a problem of the plan.

### `bita metadata snapshot list`

Lists the snapshots of the metadata store that the controller keeps in the object store, oldest first.
The controller writes one every five minutes and keeps the newest 288. The command does not touch the
metadata store, so it works while the store is lost.

```console
$ bita metadata snapshot list
SNAPSHOT                                 TAKEN AT              AGE  SIZE
metadata-snapshots/00000001798761299000  2026-12-31T23:54:59Z  9m   18.4 KiB
metadata-snapshots/00000001798761599000  2026-12-31T23:59:59Z  4m   18.6 KiB
```

### `bita metadata restore`

Writes a snapshot back into the metadata store. The argument is the snapshot's object key, or `latest`.
A snapshot that fails its checksum is not restored, not even in part.

```console
$ bita metadata restore latest --dry-run
snapshot:                 metadata-snapshots/00000001798761599000
taken at:                 2026-12-31T23:59:59Z
outcome:                  dry run: nothing was written
keys in the store before: 3
keys written:             39
keys deleted:             0
keys kept:                3
$ bita metadata restore latest
snapshot:                 metadata-snapshots/00000001798761599000
taken at:                 2026-12-31T23:59:59Z
outcome:                  restored
keys in the store before: 3
keys written:             39
keys deleted:             0
keys kept:                3
next: run `bita metadata rebuild` to pick up what changed after the snapshot
```

A lost metadata store is not empty for long. The replicas keep running, and each writes its registration
again within seconds, as the three above did. The restore leaves those registrations as they are, because
a replica's own statement is newer than the snapshot's, and writes every other key of the snapshot, the
registrations of replicas that have not come back among them.

A metadata store that holds anything else is in use, and is refused, because the result would mix two
states. `--force` restores anyway and makes the store exactly what the snapshot was: every other key is
deleted, and no registration is kept. A dry run says when force would be needed, and shows what it would
delete.

A snapshot is up to five minutes older than the cluster. A stream that rolled to a new segment or
changed its writer since then is behind in the restored store, and a writer cannot take it over until
`bita metadata rebuild --apply` has brought it up to date.

### `bita metadata rebuild`

The last resort: puts the stream records back together from what the replicas and the object store
hold. It asks every registered replica, and every replica named with `--replica <id>=<address>`, for its
segment copies, reads the footer of every segment object in the object store, and plans one record per
stream found. Without `--apply` it only prints the plan.

```console
$ bita metadata rebuild --name 1=orders
REPLICA    ADDRESS        ANSWERED  COPIES
replica-a  10.0.0.1:7000  yes       2
replica-b  10.0.0.2:7000  yes       2
replica-d  10.0.0.4:7000  yes       1
tiered segment objects found: 1

stream 1 "orders": not written
epoch 4, low-water mark 1, next segment id 4
SEGMENT  EPOCH  FIRST  END  STATE     COPIES                                                          OBJECT
1        1      1      3    released                                                                  streams/1/segments/00000000000000000001 (329 B)
2        2      4      6    sealed    replica-a=unverified replica-b=unverified rebuild-missing-3=bad  -
3        3      7      -    open      replica-a=unverified replica-b=unverified replica-d=unverified  -
assumptions:
  - segment 3: nothing recorded a seal; it is left open for the next writer to recover
  - replication is not stored on replicas: assumed 3 replicas with write quorum 2
  - segment 2: 2 of 3 copies were found; the others are recorded as missing, and the controller rebuilds them
  - seal times are not stored on replicas: sealed segments count as sealed now, which only delays their tiering and retention
  - retention policy and materializers are not stored on replicas: none is set, and nothing is truncated until materializers are registered again

dry run: nothing was written. Pass --apply to write this plan.
```

What the plan does with what it finds:

- A segment is sealed where a replica or its object recorded the seal. If nothing recorded it, it is
  sealed right before the start of the next segment. The last segment without a recorded seal is left
  open, and the next writer recovers it as after a crash.
- Every copy found is recorded as `unverified`, and the controller verifies it. A copy that was not found
  is recorded on a replica that does not exist, `rebuild-missing-<n>`, in state `bad`, which makes the
  controller rebuild it elsewhere.
- A segment with an object and no file on any replica is recorded as left to the object store.
- The epoch is set above every epoch found, so that the next writer fences everything.

A plan can have problems: a replica that did not answer, a stream without a name, an object store that
cannot be listed or is not configured. A plan with problems is not written; `--force` writes what could
be rebuilt all the same. `--no-object-store` says that the cluster has no object store, which removes
that one problem without forcing the others.

What cannot be recovered, because replicas do not store it:

| Lost | What the rebuild does |
| --- | --- |
| The stream's name | It must be given: `--name <stream id>=<name>`, once per stream. |
| The replication | Assumed three replicas with a write quorum of two. |
| The retention policy | None is set. |
| The registered materializers and their checkpoints | None is registered. Nothing is truncated until they are registered again. |
| The low-water mark | Set to the first sequence number still found. |
| When each segment was sealed | Now, which only delays tiering and retention. |

If the metadata store still has a record of the stream, for example after a restore, all of these are
taken from it instead, and its low-water mark and next segment identifier are never moved back.

The plan lists problems apart from assumptions: a replica that did not answer, segments that do not
connect, a sealed segment without a complete copy, an open segment found on too few replicas for a
writer to recover it, a stream without a name. A plan with problems is not applied unless `--force` is
given. A replica that does not answer is the one to fix first: its copies are missing from the plan, and
once the stream is rebuilt the controller removes copies the plan does not list.

A stream that is recorded as deleted is never rebuilt.
