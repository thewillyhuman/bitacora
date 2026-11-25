# The `bita` command-line tool

`bita` is what an operator uses to look at a bitacora cluster and to carry out the routine procedures of
the design (section 14): create and inspect streams, drain a replica before a restart, remove a failed
one, and see which controller leads.

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
