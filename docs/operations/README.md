# Operating a bitacora cluster

This guide is for the person who runs a cluster: what it is made of, how to deploy it, what to do on an
ordinary day and on a bad one. It says what to do and points to the page that has the detail.

| For | Read |
| --- | --- |
| Every command of the `bita` tool | [../cli.md](../cli.md) |
| Every metric | [metrics.md](metrics.md) |
| Every alert and what to do first | [alerts.md](alerts.md) |
| Machines with systemd | `src/systemd/README.md` |
| Kubernetes | `src/helm/bitacora/README.md` |
| Container images | `src/docker/README.md` |
| A cluster on your own machine | `development/README.md` |
| Why it works the way it does | [../spec.md](../spec.md), [../protocol/](../protocol/README.md) |

## What a cluster is made of

| Process | How many | Keeps | Listens on |
| --- | --- | --- | --- |
| `bita-replica` | Three or more, in different failure domains | The log, on a local disk | 7000 (gRPC), 9100 (metrics, probes) |
| `bita-controller` | Two or three; one leads | Nothing | 9101 (metrics, probes) |
| etcd | Three or five members | The metadata: streams, segments, replicas | its own |
| An object store | One bucket, S3 or compatible | Sealed segments and metadata snapshots | its own |
| Applications | Yours | Nothing bitacora needs | nothing |

The ports are those of the example configurations. An application links the client library, which
writes to the replicas directly: there is no proxy and no broker.

What keeps working when a part is down:

| Down | Still works | Stops |
| --- | --- | --- |
| One replica | Everything. Writers that used it roll to another set of replicas | Nothing, until a second replica of the same data is lost |
| Every controller | Appends and reads | New standby segments, repair, tiering, deletion. Rolls stop when the standby segments run out |
| etcd | Appends to open segments, reads | Rolls, taking over a stream after a writer fails, registration, the controller |
| The object store | Appends, reads of what replicas hold | Tiering, so disks fill; metadata snapshots; reads of segments that are only there |

## Before you deploy

These hold for every way of deploying, and nothing checks most of them for you.

- **One etcd key prefix and one object store per cluster, the same in every process.** The controller
  refuses to record an upload it cannot see itself, which catches a replica with another bucket. Nothing
  can catch two clusters that share a prefix by mistake.
- **Failure domains that are true.** A replica reports where it is, widest domain first (zone, rack,
  host), and the controller never puts two copies of a segment in the same narrowest domain. Three
  replicas that claim three hosts and share one disk shelf protect nothing.
- **Disks whose flush means it.** An append is acknowledged after the replica's flush returns. Use disks
  with power-loss protection, on a file system that honours `fdatasync`.
- **Clocks within a second or two of each other.** A replica stamps its registration with its own clock
  and the controller judges it with its own: a replica more than 15 seconds off looks dead, or alive
  when it is not. Run NTP or chrony everywhere.
- **A private network.** There is no TLS and no authentication between clients, replicas, controllers
  and etcd. Whoever reaches port 7000 can read and write every stream.
- **A lifecycle rule on the bucket, for uploads that were never finished.** A large object reaches an S3
  bucket as a multipart upload. If the process dies in the middle, or the store is unreachable when the
  process gives up, the parts that were sent stay in the bucket: no listing shows them, nothing in the
  cluster removes them, and they are billed. Have the bucket abort them itself:

  ```json
  {
    "Rules": [
      {
        "ID": "abort-incomplete-multipart-uploads",
        "Status": "Enabled",
        "Filter": { "Prefix": "" },
        "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 1 }
      }
    ]
  }
  ```

  ```sh
  aws s3api put-bucket-lifecycle-configuration --bucket <bucket> \
    --lifecycle-configuration file://lifecycle.json
  ```

  Amazon S3 and Ceph take this rule as it is; other services have their own name for it. One day is
  the shortest S3 offers and is safe: the rule counts from the start of an upload, and no upload of a
  segment runs that long. To see whether it is needed, `aws s3api list-multipart-uploads` shows what is
  waiting, and `bitacora_object_store_request_failures_total{operation="upload_abort"}` counts the
  uploads a process could not abort itself. A local directory as the object store needs nothing: it
  removes its own leftovers after a day.
- **Memory for uploads.** A replica uploads a sealed segment without holding it: it reads its copy
  8 MiB at a time and sends that on. One upload in progress takes about 16 MiB, and under 32 MiB when
  an application writes batches as large as a batch can be, whatever the size of the segment. The
  controller asks for one upload at a time, so a replica has one in progress, or two for a moment
  while the leading controller changes. A replica needs no memory set aside for segments.

## Deploying

Pick one; each page is complete.

| Where | How | Page |
| --- | --- | --- |
| Machines with systemd | An archive with the programs, the units and example configurations | `src/systemd/README.md` |
| Kubernetes | A Helm chart: replicas as a StatefulSet, controllers as a Deployment | `src/helm/bitacora/README.md` |
| Your own containers | Three images built from one Dockerfile | `src/docker/README.md` |
| Your laptop | `development/cluster.sh start` | `development/README.md` |

None of them deploys etcd or the object store. Start etcd first; replicas and controllers can then start
in any order.

Each daemon reads one TOML file. The example next to each program explains every setting
(`src/rust/bita-replica/example.toml`, `src/rust/bita-controller/example.toml`); the archive and the
images carry them. Two things about them:

- A setting that is misspelled, unknown or impossible stops the daemon at start with exit code 2 and a
  line that names it. Check a file before it reaches a running daemon:
  `bita-replica --config replica.toml --check-config`.
- A value in double quotes may be written as `"${NAME}"` and is taken from the environment. Credentials
  of the object store never go in the file: they are the usual `AWS_ACCESS_KEY_ID` and
  `AWS_SECRET_ACCESS_KEY` in the environment of the process.

### Is it working?

```console
$ curl -fsS http://replica-a.example.org:9100/readyz      # 200 once the replica has registered
$ curl -fsS http://controller-a.example.org:9101/readyz   # 200 once the controller has completed a round
$ bita replica list                                       # every replica, alive, in its domains
$ bita controller status                                  # one controller leads
$ bita stream create smoke-test
$ bita stream append smoke-test "hello"
$ bita stream read smoke-test
$ bita stream delete smoke-test --yes
```

`bita` finds the cluster through `--etcd-endpoint`, `BITA_ETCD_ENDPOINTS` or a configuration file
([../cli.md](../cli.md)). Then load the alert rules and the dashboard (below).

## Routine procedures

### Restart or upgrade one replica

```console
$ bita replica drain replica-a --wait 2m
replica replica-a is draining and no open segment names it: it can be stopped
```

Then stop it, do the work, start it, and wait for `/readyz` before touching the next one. Draining makes
writers roll away from the replica as they append; it is not required for safety, it only spares
writers the pause of noticing a dead replica. A drain lasts until the replica restarts: nothing has to
be undone.

Never have two replicas down on purpose at the same time. With three replicas and a write quorum of
two, the second one stops every stream that had its open segment on both.

### Upgrade a whole cluster

Controllers first, then replicas one failure domain at a time as above, then the applications. On
Kubernetes `helm upgrade` restarts one replica at a time and waits for each to be ready.

This order is the design's. No test of this repository upgrades a cluster from one version to another:
it was done by hand once, between two release candidates on a five-replica cluster, while a writer
appended; the writer never stopped and no record was lost. Read the changelog of a release before you
apply it.

A segment that is opened while a whole failure domain is away is placed in the domains that are left,
with two copies in one of them, and keeps that placement: the controller does not move the copies of a
segment to widen its spread. It does replace the standby of a stream, the replica set of its next
segment, as soon as a wider spread is possible, so only the segments opened during the outage are
affected. A zone with a single replica is away whenever that replica restarts: run at least two
replicas per zone if a restart must not narrow any segment.

Do not skip a release that the changelog marks as raising the schema version of the metadata records. A
program refuses a record of a version it does not read (`cannot be decoded: schema version … is newer than
…`), so every program must run the release that reads the new version before any runs the one that writes
it (`docs/protocol/metadata.md`, "Writing and reading a record").

### A replica's machine or disk failed

1. Decide whether it comes back with its data. If it does (a reboot, a network fault), wait: after five
   minutes without a registration the controller re-creates its copies elsewhere, and when the replica
   returns with its data it registers again and the surplus is cleaned up.
2. If the data is gone, tell the cluster now instead of in five minutes:

   ```console
   $ bita replica remove replica-a
   replica replica-a removed from the registry; the controller re-creates its copies
   ```

3. Bring up the replacement **under a new `replica_id`**, with an empty data directory.
4. Watch `bitacora_controller_segments_missing_copies` return to zero.

**Never start a replica on an empty disk under an identity that was used before.** A replica keeps its
identity in its data directory; the `replica_id` of the configuration only names an empty one. A replica
that lost its disk holds none of the copies its old identity held. If it answers under that identity, a
writer that recovers a stream after a failure can take its empty answer for the word of a replica that
used to hold committed records, and a committed record can be lost.

The daemon enforces this. On an empty data directory it asks the metadata store whether the cluster
already knows the proposed `replica_id`, as a registered replica or as the holder of a copy or a standby
in any stream, and if so it exits with code 2 and says how the identity is known. Give the directory a
new `replica_id`. While the metadata store cannot be reached, a replica on an empty directory does not
start at all (exit code 1; it starts once the question can be answered); a replica on a directory that
has its identity starts regardless. The Helm chart never runs into this: it puts the pod's UID in the
identity it proposes.

### Add capacity

Start more replicas, each with its own `replica_id` and true failure domains. New segments go first to
the replicas whose disk is emptier by a tenth or more, so new replicas fill before old ones grow. Among
replicas that are about as full, streams and their successive segments are spread evenly: every replica
of a zone gets its share, and none is left idle because another is a little emptier. Sealed segments are
not moved: there is no rebalancing. Old replicas empty as their segments are tiered to the object store
and truncated.

### Remove a replica for good

Drain it, stop it, `bita replica remove` it, and wait until
`bitacora_controller_segments_missing_copies` is back at zero before you wipe its disk: until then that
disk may hold the only other copy of something.

### Streams

`bita stream create`, `describe`, `truncate-before` and `delete` are in [../cli.md](../cli.md). Two
things to know:

- A stream is truncated up to the checkpoint of its slowest registered materializer, and no further. A
  materializer that is registered and stopped pins the log for good, on purpose. Unregister a dead one
  with `bita stream unregister-materializer`.
- Deleting a stream deletes its records everywhere, including the object store. There is no undo.

## Monitoring

- **Probes.** `/healthz` answers when the process is up, `/readyz` when it works: a replica has
  registered, a controller has completed a round.
- **Metrics.** `/metrics` on the same port. [metrics.md](metrics.md) lists every one.
- **Alerts.** Load `src/prometheus/bitacora-alerts.yaml`. [alerts.md](alerts.md) lists the alerts, what to
  do first, and what they cannot see: a daemon that is down, a failed flush, and everything only the
  application knows. Cover those with your platform's own alerts.
- **Dashboard.** Import `src/grafana/bitacora-dashboard.json` (`src/grafana/README.md`).
- **Logs.** One JSON object per line on standard output; `format = "text"` in `[log]` for reading by
  eye.

After any change, three numbers say whether the cluster is whole: one controller leads, no segment is
missing copies, no replica holds damaged copies. They are the first row of the dashboard.

## What is backed up, and what is not

| What | Where it is safe | From when |
| --- | --- | --- |
| A sealed segment | The object store | Fifteen minutes after it was sealed and fully replicated (`tiering_delay_seconds`) |
| The newest records | Only the replicas | Until their segment is sealed and tiered: up to an hour and fifteen minutes with the defaults |
| The metadata | Snapshots in the object store, every five minutes, the newest 288 | Also back up etcd itself |

Without an object store there is no backup of either: every record is on the replicas and nowhere else.

### etcd is lost

The records are still on the replicas and in the object store. [../cli.md](../cli.md), "Recovering the
metadata store", has the procedure: stop writers and controllers, start an empty etcd,
`bita metadata restore latest`, then `bita metadata rebuild` to pick up what happened after the
snapshot, look at the plan, and `--apply` it. Read the plan: it lists what it assumed and what it could
not find. A replica that did not answer is the thing to fix first.

### Replicas are lost beyond repair

If fewer copies of a segment are lost than its stream keeps, the controller repairs it and there is
nothing to do. If every copy of a segment is lost:

- A segment that was tiered is still readable from the object store, by any reader that is given the
  object store. A test of this repository loses every replica disk and reads the tiered records back.
- A segment that was not yet tiered is gone. Readers of that range fail; the alert
  `BitacoraSegmentsMissingCopiesForLong` fires.
- Writing to the stream again needs the replicas of its last, open segment to answer a new writer. If
  they are all gone, taking over the stream does not complete. Recovering from that is not automated and
  no test of this repository covers it: `bita metadata rebuild` shows what is left and marks such a
  stream as a problem; decide with that plan in front of you.

## What is not there yet

So that you do not look for it:

- TLS and authentication, anywhere.
- Rebalancing of sealed segments between replicas.
- Metrics from the client library: append latency, failover time, replica lag, materializer lag.
- Traces.
- An operator for Kubernetes: the chart does not replace volumes or scale for you.
- A published image or package: you build them.
