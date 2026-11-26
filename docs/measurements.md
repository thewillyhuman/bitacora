# Measurements

What bitacora did on real machines, and what it did not do. This page holds the runs that were made, with
enough about each to repeat it; [benchmark.md](benchmark.md) describes the tool that made them.

There is one environment so far, and it is not the one the design's targets assume. Read the numbers as
"bitacora on network block storage", not as what bitacora can do.

## The environment

- **Cluster.** Kubernetes 1.36 on virtual machines: six workers with 4 vCPUs and 7.4 GiB of memory, two in
  each of three zones of one region.
- **bitacora.** The chart with five replicas, so two zones had two replicas and one zone had one, and two
  controllers. Streams had three replicas and a write quorum of two.
- **Object store.** None in the first round of runs, so nothing was tiered. The second round added an S3
  server, one pod on a 100 GiB volume of the same kind, and had the controller upload a segment a minute
  after it was sealed and the replicas release it two minutes later.
- **Disks.** Every replica on a 100 GiB network block volume. Making a small write durable took 4.5 ms on
  average there, and under 26 ms for 99 in 100 (`bitacora_disk_flush_duration_seconds`). The design assumes
  local NVMe disks with power-loss protection, where that takes a fraction of a millisecond.
- **etcd.** Three members, one per zone, on the same kind of volume.
- **Load.** `bita-bench` in a pod on the sixth worker, which ran no replica, from the same image version as
  the cluster unless a run says otherwise.
- **Watching.** In the second round, a Prometheus in the cluster's own namespace that scraped the
  replicas, the controllers and etcd and evaluated the default alert rules.
- **Versions.** Release candidates `0.1.0-rc.1` (commit `84b8f76`), `0.1.0-rc.2` (`8637e7d`) and
  `0.1.0-rc.3` (`d09f17f`) in the first round; `0.1.0-rc.4` (`911b506`), `0.1.0-rc.5` (`1b0a2da`) and
  `0.1.0-rc.8` (`5e10b45`) in the second. All were installed as
  [development/kubernetes](../development/kubernetes/README.md) describes.

Every run lasted 60 seconds unless it says otherwise, and each was made once: there are no error bars.

## Appends

Records of 1 KiB unless the workload says otherwise, appended as fast as the cluster committed them.
Latency is from the call of `append` to its return. Version `0.1.0-rc.1`.

| Workload | Records/s | MiB/s | p50 | p99 | Worst |
| --- | --- | --- | --- | --- | --- |
| 1 append in flight | 120 | 0.1 | 7.7 ms | 18.4 ms | 33 ms |
| 16 appends in flight | 1,164 | 1.1 | 13.6 ms | 24.1 ms | 36 ms |
| 64 in flight | 4,333 | 4.2 | 14.6 ms | 26.1 ms | 42 ms |
| 256 in flight | 15,892 | 15.5 | 15.6 ms | 27.1 ms | 43 ms |
| 64 in flight, 16 records per append | 22,313 | 21.7 | 45 ms | 74 ms | 168 ms |
| 64 in flight, 64 records per append | 24,186 | 23.6 | 168 ms | 270 ms | 529 ms |
| 16 in flight, records of 64 KiB | 362 | 22.6 | 44 ms | 68 ms | 259 ms |
| 4 streams, each 64 in flight, 16 records per append | 54,487 | 53.2 | 76 ms | 115 ms | 590 ms |
| 16 streams, the same | 110,002 | 107.4 | 147 ms | 205 ms | 746 ms |

A scan of a stream that had just been written read 289,893 records a second, 283 MiB/s.

With one append in flight, an append took 7.7 ms where a flush alone took 4.5 ms: the rest is one network
round trip between zones and waiting for the second of three replicas. That is the design's account of
where the time goes (section 16), with a slower disk.

## What limits one stream

One stream stopped at about 23 MiB/s whatever the shape of its appends, while sixteen streams together
moved 107 MiB/s through the same five disks. The replicas' counters say why. During such a run each of the
stream's three replicas made about 80 flushes a second of 250 KiB each, 10 to 11 ms per flush, and spent
86% of its time flushing.

A writer sends each replica one request at a time, a request carried at most 256 KiB of records in these
versions (`max_request_bytes`, the batch size of the design), and a replica makes a request durable with
one flush.
So a stream moves 256 KiB per flush, and on this storage a flush of that size takes about 10 ms.

The same workload with other request sizes, on `0.1.0-rc.2`: one stream, records of 64 KiB, 128 appends in
flight.

| Request size | MiB/s | p50 | p99 | Flushes/s on the busiest replica | Per flush |
| --- | --- | --- | --- | --- | --- |
| 256 KiB, the default of these versions | 23.8 | 344 ms | 442 ms | 102 | 242 KiB in 8.3 ms |
| 1 MiB | 71.5 | 111 ms | 168 ms | 72 | 1,015 KiB in 9.9 ms |
| 4 MiB | 104.3 | 74 ms | 160 ms | 28 | 3,763 KiB in 21.1 ms |
| 8 MiB | 120.3 | 65 ms | 117 ms | 23 | 5,435 KiB in 19.8 ms |

A flush costs a few milliseconds whatever it carries and little more per megabyte, so larger requests move
more with fewer of them, and the appends that wait behind them wait less. A larger limit costs nothing
when there is little to send, because a request carries what has accumulated and no more: with a limit of
4 MiB, 16 appends in flight of 1 KiB records took 14.1 ms at p50 and 24.1 ms at p99, as with the default,
in requests of 10 KiB.

With a limit of 4 MiB, 256 appends in flight of 16 records of 1 KiB reached 98,052 records a second
(95.7 MiB/s, p50 41 ms), and four streams together 118.4 MiB/s (p50 33 ms).

Because of these runs, a request carries up to 4 MiB by default in the versions after `0.1.0-rc.3`
([decision record 0017](decisions/0017-request-size.md)). Where a flush takes a millisecond, 256 KiB per
flush would be some 250 MiB/s, so the old limit was not expected to bind on the disks the design assumes;
that was not measured.

With that default, on `0.1.0-rc.4`, one stream of 64 KiB records with 128 appends in flight moved
111.4 MiB/s at 70 ms p50 and 164 ms p99, and 16 appends in flight of 1 KiB records took 13.6 ms at p50
and 23.0 ms at p99, as before.

## Many streams

Records of 1 KiB, requests of up to 4 MiB, every stream written as fast as the cluster committed.

| Workload | Version | MiB/s | p50 | p99 | Worst |
| --- | --- | --- | --- | --- | --- |
| 16 streams, each 64 in flight, 16 records per append | `0.1.0-rc.4` | 233.6 | 68 ms | 104 ms | 429 ms |
| The same | `0.1.0-rc.5` | 240.3 | 63 ms | 123 ms | 602 ms |
| 64 streams, each 16 in flight, 16 records per append | `0.1.0-rc.4` | 76.9 | 213 ms | 590 ms | 779 ms |
| The same | `0.1.0-rc.5` | 128.8 | 129 ms | 209 ms | 711 ms |
| 128 streams, each 4 in flight, 1 record per append | `0.1.0-rc.4` | 1.5 | 336 ms | 721 ms | 931 ms |
| The same | `0.1.0-rc.5` | 1.9 | 270 ms | 500 ms | 880 ms |

Two things are in these numbers.

**Placement.** `0.1.0-rc.4` put every stream of a run on the same three replicas, one per zone, and left
the other two idle: it chose the replica with the most free bytes, and one replica of a zone was always a
few megabytes emptier than its neighbour. Since `0.1.0-rc.5` placement spreads over replicas that are
equally good ([decision record 0019](decisions/0019-placement-spread.md)), and the 64 streams moved two
thirds more with a p99 of 209 ms where it had been 590 ms. Placement still prefers a disk that is emptier
by a tenth of its size: in the run of 128 streams one of the two replicas of a zone, 13% full next to a
neighbour at 8%, was given no stream.

**The storage.** Many streams that each write little are slow here: 128 streams at 16 appends a second
each waited 270 ms for an append that takes 14 ms on a quiet cluster. Every open segment makes its own
writes durable, one flush per request, and a volume of this kind completed about 600 flushes a second in
total however many segments asked for them. On `0.1.0-rc.4` each loaded replica completed 620 to 630
flushes a second, each taking 180 ms on average, so some 113 were waiting at any time. The time an append
takes therefore grows with the number of streams that write to one replica at once. Sharing a flush
between the segments of a replica would remove that; bitacora does not do it. Whether it matters on
disks where a flush takes a fraction of a millisecond was not measured.

During these runs a replica used at most 0.57 of a core. Its own memory stayed between 100 and 150 MiB
with 208 streams open, as the kubelet reports resident memory. `kubectl top` showed up to 850 MiB for the
same process: that figure is the working set, which counts the page cache of the files just written.

## Against the targets of the design

Section 16 of the design proposes targets for three replicas on NVMe disks with power-loss protection.

| Measure | Target | Here | |
| --- | --- | --- | --- |
| Append latency, p50 | 2 ms | 7.7 ms | Missed: the flush alone takes 4.5 ms on this storage |
| Append latency, p99 | 10 ms | 18.4 ms | Missed, for the same reason |
| Sustained throughput per stream | 50 MiB/s | 111.4 MiB/s | Met with requests of 4 MiB, the default since `0.1.0-rc.4`; 23.8 MiB/s with the 256 KiB of the versions before |
| Tail reader delay behind commit | 5 ms | | Not measured: the tool has no such workload |
| Write unavailability on writer failover | 5 s | | Not measured: a forced takeover was over within the five seconds its pod took to start, run and stop |
| Write pause on a replica-failure roll | 200 ms | 157 ms | Met in the one run made with a replica that was killed. Not met when a zone was cut off the network instead: the worst append then took 15 to 31 s, see "A soak" |

None of this says whether the targets hold on the disks they were written for. That takes a run on such
disks.

## Failures and an upgrade

All on the cluster above, with 16 appends in flight of 1 KiB records unless the row says otherwise. After
each, the streams were read back in full.

| What was done | What happened |
| --- | --- |
| One of a stream's three replicas was killed 30 seconds into a 90 second run | All 76,635 appends committed and were read back. The worst append took 157 ms. The stream rolled to a new segment; the replica came back on its volume under its old identity, and every copy of both segments was good again |
| The leading controller was killed | Another controller led 9 seconds later. Writers are not affected by this: the controller is not on the write path |
| A second writer took a stream by force from a writer that was appending | The first writer stopped with "another writer has taken the stream" after 28,253 records; the second appended at 28,254 under the next epoch. The stream held 28,254 records |
| The cluster was upgraded from `0.1.0-rc.1` to `0.1.0-rc.2` with `helm upgrade`, one replica at a time, during a 240 second run by a writer of the older version | All 246,948 appends committed and were read back, at the rate of a quiet cluster. The worst append took 163 ms. The stream rolled six times |
| 33 streams were deleted, and later 338 | Every replica's disk went back to 0% used |
| A replica's volume was deleted together with its pod | The replica came back empty, under a new identity. The controller rebuilt the lost copies on the other replica of the same zone within about five minutes, and all 30,976 records of the stream were read back. The old identity was removed with `bita replica remove` |
| A replica was taken away for longer than the five minutes after which the controller gives a replica up | Its four copies were rebuilt on other replicas 325 seconds after it left. When it returned, its own copies, no longer part of any segment, were removed within ten seconds. Every stream held exactly the records it had |
| The metadata store was emptied on a cluster without an object store, with the controllers stopped | `bita metadata rebuild` put the five streams back together from what the replicas held. Every stream held exactly the records it had, and a new writer appended under the next epoch |
| Controllers were started against the emptied metadata store, by mistake, before it was rebuilt | They deleted nothing on the replicas. The rebuild that followed found every copy |
| The metadata store was emptied on a cluster with an object store, one append after the newest snapshot | The snapshot, a few minutes old, was restored, and the rebuild found what it lacked: the one segment written since, on three replicas. All six streams were read back whole, 387,054 records, all but the two newest from the object store, the record appended after the snapshot among them. A new writer appended under the next epoch |
| The same again on `0.1.0-rc.8`, with a snapshot taken while 208 streams were being deleted | The restore needed no force: it kept the five registrations the replicas had written since the loss and wrote the other 114 keys. The six streams that held data were read back whole, 387,055 records. The 97 streams that were deleted after the snapshot came back as records without data: the controller marked every copy as bad, and a read stopped with "every reachable copy of the sealed segment is incomplete". They were deleted again by hand |
| The cluster was upgraded four more times, to `0.1.0-rc.3`, `0.1.0-rc.4`, `0.1.0-rc.5` and `0.1.0-rc.8` | No stream lost a record |

No network was cut by hand. A policy that isolated one replica was accepted by the cluster and had no
effect: the replica kept renewing its registration, and new connections reached it. The soak below found
that this is about that one pod. Policies that cut a whole zone off held for every other pod they
selected, in all three zones. A cut on this cluster therefore has to be seen to be in effect before it
counts, which is what the soak tool does.

## A soak

`bita-soak` ([soak.md](soak.md)) writes and reads streams for days, kills pods meanwhile, and has the
simulator's checker judge what its clients were told.

Its alarm was tried first, on `0.1.0-rc.8`: a run that plants a commit that never happened. In a round of
90 seconds the tool committed 8,405 appends and read 16,804 records, the checker reported the planted
record as `CommittedRecordMissing`, and the run ended as `violated` with exit code 3. The tool also tried
a network cut, saw the replica behind it renew its registration, and gave cuts up for the run, naming
the reason in every line it printed.

A run of three days was started on the same version on 2026-11-26: four streams at twenty appends a
second each, a fault a minute, every segment uploaded a minute after it is sealed and released by the
replicas two minutes later, so that most of what the audit reads comes from the object store. Its result
is not in yet, and this page does not count it. Its first six rounds, of ten minutes each, are:

| Round | Appends committed | Records read | Killed: replicas, controllers, etcd members | Zones cut off | Longest append | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 50,278 | 100,552 | 2, 2, 2 | 2 | 25.6 s | Clean |
| 2 | 47,899 | 95,794 | 3, 2, 3 | 0 | 543 ms | Clean |
| 3 | 47,677 | 95,350 | 3, 1, 1 | 3 | 30.7 s | Clean |
| 4 | 46,961 | 93,918 | 1, 3, 2 | 2 | 15.4 s | Clean |
| 5 | 45,716 | 91,428 | 2, 2, 2 | 2 | 25.7 s | Clean |
| 6 | 45,937 | 91,870 | 3, 2, 1 | 2 | 30.7 s | Clean |

In one hour: 284,468 appends committed and every record read back twice, with 14 replicas, 12 controllers
and 11 etcd members killed and a zone cut off eleven times. No append was lost or left without an outcome,
and no writer lost its stream.

A zone that is cut off holds one or two of the five replicas and one of the three etcd members, for half
a minute. That the cuts were in effect is not only the tool's word: for their duration the Prometheus
could not scrape the processes of the zone, and none of them restarted. All three zones were cut. One
replica pod is the exception: a cut of that replica alone was seen not to be in effect, and the tool gave
that kind of fault up for the run; when its zone was cut, the Prometheus still reached it.

What a cut costs is time. With kills alone the longest append of a round took half a second. In every
round with a zone cut off it took 15 to 31 s, although two replicas of every stream and two etcd members
could be reached throughout.

### Where the time goes

The stall was reproduced on one machine, with five replicas and three etcd members as local processes
and one writer at 16 appends in flight. A process that is frozen is silent in the way a zone that is cut
off is: its connections stay open and nothing answers.

| Frozen for 30 to 45 s | Longest append |
| --- | --- |
| One replica of the open segment | 185 ms. The writer rolled away from it after its 10 s |
| One etcd member, a follower | 61 ms |
| The etcd leader | 74 ms |
| One replica of the open segment and one etcd member, as when a zone is cut off: ten runs | 0.2, 0.2, 5.3, 5.4, 10.2, 10.4, 10.4 and 30.6 s; in two runs the writer stopped |

Neither a silent replica nor a silent etcd member hurts by itself. Together they do. The writer rolls
away from the silent replica, and a roll makes three requests to the metadata store, one after the other,
while nothing commits: a read, the conditional write, and a read of the replica registry. The client keeps
sending requests to the silent etcd member, since nothing tells it that the member is gone, and a request
that lands there waits out its 5 s. A read is tried twice and may land there both times. The stalls are
therefore multiples of five seconds, on one machine and on the cluster alike. This is not fixed.

In two of the ten runs the writer did not stall but stopped, with "another writer has taken the stream",
while it was the only writer. That is a defect of the Rust client and is fixed; it is listed below.

## Tiering

On `0.1.0-rc.5`, with the object store described above.

| What was done | What happened |
| --- | --- |
| The object store was added to a cluster that held a stream of 246,948 records in seven sealed segments | All seven were uploaded within minutes and released by the replicas after the two minutes they are kept for readers. A reader that was not given the object store stopped with "the segment is in the object store, and this client was given none"; one that was read all 246,948 records at 211 MiB/s |
| 69,303 records of 64 KiB, 4.3 GiB, were appended to one stream at 108 MiB/s | The stream rolled into six segments of 332 MiB to 1.0 GiB, on all five replicas. All were uploaded and released. While they uploaded, `kubectl top` showed 93 to 195 MiB for a replica and 104 MiB for the S3 server. The stream was read back from the object store at 160.7 MiB/s, all 69,303 records |

The versions before `0.1.0-rc.5` read a whole segment into memory to upload it, which a segment of a
gigabyte makes a gigabyte per upload. Since then a segment is uploaded in parts of 8 MiB.

## Alerts

The default rules, 18 of them, were evaluated from the middle of the second round on: through the
tiering runs, the second run of many streams, the deletion of several hundred streams and the restore of
the metadata store. What they did:

| Alert | When | Right? |
| --- | --- | --- |
| `BitacoraReplicaFlushSlow` | Pending during every heavier benchmark; fired once, for a minute | Yes. Its threshold, 10 ms at p99, is the disk the design assumes; this storage is slower than that under load |
| `BitacoraControllerAbsent`, `BitacoraControllerNoLeader`, `BitacoraMetadataSnapshotsStopped` | Pending for the minutes in which the controllers were stopped for a drill or replaced by an upgrade | Yes. None fired, because no stop lasted as long as the rule waits |
| `BitacoraControllerStreamErrors` | Pending for eleven minutes while hundreds of streams were deleted | It pointed at a defect, listed below |
| `BitacoraReplicaRegistrationFailing` | Pending for six minutes during the same deletions | It pointed at another |
| `BitacoraMetadataStoreSlow` | Pending for five to nine minutes after a replica started; never fired | Noise. A process's first request to etcd took about 0.6 s here, and a replica makes one request every five seconds, so one slow request is the 99th percentile of five minutes. The rule waits ten minutes, which is just longer |
| `BitacoraSegmentsMissingCopies` | Pending for a minute or two during an upgrade and while 128 streams were created at once | Not looked into |

The drills with a lost disk and with a replica that was away were made before this Prometheus ran, so
whether the rules for a replica that is not registered and for segments that miss copies fire in time was
not seen.

## What these runs found

Things that no test on one machine had shown. The third is not fixed, and neither is the stall of a
zone that falls silent. Where a test now holds a fix, it is named.

- **A tool that had just started sometimes could not read the metadata store.** About one or two runs of
  `bita` in a hundred failed with "etcd did not answer the read within 5s", with etcd in good health: the first
  request went out before a connection was up. A read that fails is now made once more
  (`bitacora-meta-etcd`, `store_test.rs`).
- **A standby placed while a zone was away stayed that way.** During the upgrade, the one replica of one
  zone restarted, and a stream's standby, the replica set of its next segment, was placed in the two zones
  that were up, with two replicas in one of them. Nothing replaced it when the replica returned. The
  controller now replaces a standby whenever a set chosen now would span more failure domains
  (`bitacora-controller`, `standby_test.rs`). On the cluster, the standby in two zones became one in three
  when `0.1.0-rc.3`, the first version with the fix, was installed. A segment that was opened in that state keeps its placement;
  the [operations guide](operations/README.md) says what follows from that.
- **The request size bounds a stream where a flush is slow**, as described above. The default was raised.
  That many streams share the flushes of a volume is the same property seen from the other side, and is
  not changed.
- **Streams created together were all placed on the same replicas**, as described under "Many streams"
  (`bitacora-meta`, `placement_test.rs`).
- **A stream that was deleted while the controller looked at it counted as an error.** The alert for
  stream errors went pending during every large deletion (`bitacora-controller`).
- **A replica that was deleting segments could fail to list them**, and a listing is part of renewing its
  registration, so it missed renewals while it deleted (`bitacora-replica`).
- **The rebuild of the metadata store refused to write on a cluster without an object store**, because it
  could not tell such a cluster from a configuration that leaves the object store out.
  `bita metadata rebuild --no-object-store` says which it is.
- **An upload held the whole segment in memory**, as described under "Tiering".
- **A restore always needed `--force`.** The replicas keep running while the metadata store is lost, and
  each writes its registration again within seconds, so the store was never empty when the snapshot was
  restored. A restore now keeps those registrations and refuses only a store that holds something else
  ([decision record 0020](decisions/0020-restore-into-a-lost-store.md)).
- **A restore brought deleted streams back, and nothing said so.** A snapshot is minutes older than the
  cluster. The streams deleted in those minutes return as records whose segments are gone, and the rebuild
  planned only the streams it found. `bita metadata rebuild` now ends with a list of the records of which
  no data was found (`bita`, `rebuild_test.rs`).
- **A zone that falls silent stalls its writers for many seconds**, as described under "Where the time
  goes". A killed process refuses connections at once; a zone that is cut off answers nothing, and the
  client of the metadata store has no way to notice. Not fixed.
- **A writer could stop although nobody had taken its stream.** A roll seals the open segment in the
  stream's record, and the controller then tells the segment's replicas. If the roll was slow from there
  on, as it is with a silent etcd member, the writer still talked to those replicas, and took their
  answer, sealed, for another writer's takeover. A sender now waits when it is told so during the writer's
  own roll (`bitacora-client`, `tests/writer_test.rs`). The Go client was not affected.

## What was not measured

- Disks like those the design assumes, and therefore whether its targets hold.
- A reader's delay behind the writer, and the time a writer failover takes.
- The loss of a machine, and a partition of anything but a whole zone.
- More than 128 streams or more than five replicas.
- A run longer than a few minutes. The soak above is the first, and it has not finished.
- An object store other than one S3 server on one volume, and what happens when it is slow or away.
- Other systems on the same machines, which the design names as reference points.
