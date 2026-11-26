# Measurements

What bitacora did on real machines, and what it did not do. This page holds the runs that were made, with
enough about each to repeat it; [benchmark.md](benchmark.md) describes the tool that made them.

There is one environment so far, and it is not the one the design's targets assume. Read the numbers as
"bitacora on network block storage", not as what bitacora can do.

## The environment

- **Cluster.** Kubernetes 1.36 on virtual machines: six workers with 4 vCPUs and 7.4 GiB of memory, two in
  each of three zones of one region.
- **bitacora.** The chart with five replicas, so two zones had two replicas and one zone had one, and two
  controllers. No object store: nothing was tiered. Streams had three replicas and a write quorum of two.
- **Disks.** Every replica on a 100 GiB network block volume. Making a small write durable took 4.5 ms on
  average there, and under 26 ms for 99 in 100 (`bitacora_disk_flush_duration_seconds`). The design assumes
  local NVMe disks with power-loss protection, where that takes a fraction of a millisecond.
- **etcd.** Three members, one per zone, on the same kind of volume.
- **Load.** `bita-bench` in a pod on the sixth worker, which ran no replica, from the same image version as
  the cluster unless a run says otherwise.
- **Versions.** Release candidates `0.1.0-rc.1` (commit `84b8f76`), `0.1.0-rc.2` (`8637e7d`) and
  `0.1.0-rc.3` (`d09f17f`), installed as [development/kubernetes](../development/kubernetes/README.md)
  describes.

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

## Against the targets of the design

Section 16 of the design proposes targets for three replicas on NVMe disks with power-loss protection.

| Measure | Target | Here | |
| --- | --- | --- | --- |
| Append latency, p50 | 2 ms | 7.7 ms | Missed: the flush alone takes 4.5 ms on this storage |
| Append latency, p99 | 10 ms | 18.4 ms | Missed, for the same reason |
| Sustained throughput per stream | 50 MiB/s | 23.8 MiB/s | Missed with requests of 256 KiB; 104 MiB/s with 4 MiB, the default since |
| Tail reader delay behind commit | 5 ms | | Not measured: the tool has no such workload |
| Write unavailability on writer failover | 5 s | | Not measured: a forced takeover was over within the five seconds its pod took to start, run and stop |
| Write pause on a replica-failure roll | 200 ms | 157 ms | Met in the one run made: the worst append while a replica was killed |

None of this says whether the targets hold on the disks they were written for. That takes a run on such
disks.

## Failures and an upgrade

All on the cluster above, with 16 appends in flight of 1 KiB records. After each, the stream was read back
in full.

| What was done | What happened |
| --- | --- |
| One of a stream's three replicas was killed 30 seconds into a 90 second run | All 76,635 appends committed and were read back. The worst append took 157 ms. The stream rolled to a new segment; the replica came back on its volume under its old identity, and every copy of both segments was good again |
| The leading controller was killed | Another controller led 9 seconds later. Writers are not affected by this: the controller is not on the write path |
| A second writer took a stream by force from a writer that was appending | The first writer stopped with "another writer has taken the stream" after 28,253 records; the second appended at 28,254 under the next epoch. The stream held 28,254 records |
| The cluster was upgraded from `0.1.0-rc.1` to `0.1.0-rc.2` with `helm upgrade`, one replica at a time, during a 240 second run by a writer of the older version | All 246,948 appends committed and were read back, at the rate of a quiet cluster. The worst append took 163 ms. The stream rolled six times |
| 33 streams were deleted | Every replica's disk went back to 0% used |

## What these runs found

Three things that no test on one machine had shown. The first two are fixed; the tests that now hold them
are named.

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
- **The request size bounds a stream where a flush is slow**, as described above.

## What was not measured

- Disks like those the design assumes, and therefore whether its targets hold.
- A reader's delay behind the writer, and the time a writer failover takes.
- Tiering: the cluster had no object store.
- The loss of a machine or of a zone. A killed process comes back on its volume; a lost disk was not
  tried.
- More than 16 streams, more than five replicas, or a run longer than four minutes.
- Other systems on the same machines, which the design names as reference points.
