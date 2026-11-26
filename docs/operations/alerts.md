# Alerts

The default alert rules are in `src/prometheus/bitacora-alerts.yaml`. This page lists them, says what to
do first when one fires, and says what they cannot see. Each alert carries the longer explanation in its
own `description`; this page is the overview you route by.

A test keeps this page and the rules together: an alert that is not listed here, or listed with another
severity, fails it. The rules use only the metrics of [metrics.md](metrics.md).

## Severities

- **critical**: durability or the write path is at risk now. Page a person.
- **warning**: it becomes a problem if it is left alone. Open a ticket; look the same day.

## The alerts

"After" is how long the condition must hold before the alert fires.

### Replicas

| Alert | Severity | After | Fires when | First thing to do |
| --- | --- | --- | --- | --- |
| `BitacoraReplicaFlushSlow` | warning | 10 min | The 99th percentile of a replica's flush is above 10 ms | Look at that machine's disk; if it does not recover, `bita replica drain` and replace the disk |
| `BitacoraReplicaDiskErrors` | warning | 10 min | Disk operations of a replica keep failing | Read the kernel log; drain the replica before the disk fails |
| `BitacoraReplicaDiskFilling` | warning | 15 min | A replica's disk is more than 80% full | Check that tiering and truncation work; add replicas if they do |
| `BitacoraReplicaDiskAlmostFull` | critical | 5 min | A replica's disk is more than 90% full | Add replicas or free space now: at 95% the replica refuses every append |
| `BitacoraReplicaDamagedCopies` | warning | 30 min | A replica holds damaged copies that were not replaced | `bita controller status`, `bita replica list`: something keeps repair from running |
| `BitacoraReplicaRegistrationFailing` | warning | 5 min | A replica's registration renewals fail some of the time | Check etcd and the network between the replica and etcd |
| `BitacoraReplicaNotRegistered` | critical | 2 min | A running replica has not registered at all | Restore its connection to etcd within five minutes, or its copies are rebuilt elsewhere |

### Metadata store

| Alert | Severity | After | Fires when | First thing to do |
| --- | --- | --- | --- | --- |
| `BitacoraMetadataStoreUnavailable` | critical | 1 min | A process's last request to etcd failed | One instance: its network. All of them: etcd is down or has no quorum |
| `BitacoraMetadataStoreErrors` | warning | 10 min | More than 5% of a process's requests to etcd fail | Look for leader elections, a slow disk or a full database on etcd |
| `BitacoraMetadataStoreSlow` | warning | 10 min | The 99th percentile of requests to etcd is above 500 ms | Check disk latency and load of the etcd members |

### Object store

| Alert | Severity | After | Fires when | First thing to do |
| --- | --- | --- | --- | --- |
| `BitacoraObjectStoreUnavailable` | warning | 15 min | A process's requests to the object store fail as unavailable | Check the service, the endpoint and the credentials of that process |
| `BitacoraMetadataSnapshotsStopped` | warning | 15 min | The leading controller has written no metadata snapshot for 30 minutes | See the object store alerts; `bita metadata snapshot list` |

### Controller

| Alert | Severity | After | Fires when | First thing to do |
| --- | --- | --- | --- | --- |
| `BitacoraControllerAbsent` | critical | 5 min | No controller reports metrics at all | Start the controllers, or fix the scrape configuration |
| `BitacoraControllerNoLeader` | critical | 2 min | Controllers run and none leads | `bita controller status`; a controller that cannot lead cannot reach etcd |
| `BitacoraControllerRoundsFailing` | warning | 5 min | A controller's rounds fail | See the metadata store alerts |
| `BitacoraControllerStreamErrors` | warning | 30 min | Every pass leaves work undone on some stream | The controller's log names the stream and the reason |
| `BitacoraSegmentsMissingCopies` | warning | 15 min | Sealed segments have fewer good copies than they should | `bita replica list`: repair needs a live replica with room in another failure domain |
| `BitacoraSegmentsMissingCopiesForLong` | critical | 1 h | The same, for an hour: repair makes no progress | Bring back or add replicas; one more failure in the wrong place loses data |

## What the rules assume

- One scrape job per cluster, or one for its replicas and one for its controllers. Alerts about a cluster
  as a whole are grouped by `job`; alerts about one process carry its `instance`.
- The defaults of the daemons: a replica takes no new segment at 85% and refuses appends at 95%, renews
  its registration every 5 seconds, is passed over for new segments after 15 seconds of silence and
  given up after 5 minutes. If you configure other values, change the thresholds and texts that quote
  them. The test of the rules fails when the shipped defaults move.

## What the rules cannot see

These matter and have no alert here, because no daemon exposes a metric for them. Cover them with what
your platform already measures.

- **A daemon that is down.** A process that does not run exposes nothing. Alert on failed scrapes
  (`up == 0`) with the job names of your scrape configuration, and on restarts as your supervisor counts
  them (`NRestarts` of the systemd unit, the restart count of the pod).
- **A failed flush.** A replica whose disk refuses a flush stops at once, by design, usually before the
  failure is scraped. It shows as a restart of that replica and as the last line of its log.
- **Append latency, failover time and replica lag.** They are only known in the client library, inside
  the application, which has no metrics. Measure appends in the application; `bita-bench` does it for
  benchmarks.
- **Materializer lag and checkpoint age.** A materializer runs inside the application too. One that
  stops checkpointing pins the log, and the first sign here is `BitacoraReplicaDiskFilling`. Export the
  runner's progress from the application and alert on the age of its checkpoint.
- **etcd and the object store themselves.** The rules see them only as bitacora's processes do. Run the
  alerts those systems ship with as well.
