# The `bita-bench` benchmark tool

`bita-bench` drives a bitacora cluster with a workload and reports what it measured. It goes through the
same client library and the same network path as an application, so its numbers are what an application
would see.

The tool states what it measured and nothing else. It knows no targets and judges no result. The design's
performance targets (section 16) are proposals to be confirmed by running this tool on the hardware in
question; nothing in this page is a measurement of bitacora.

## Finding the cluster

`--config <FILE>` names a TOML file with the cluster configuration, the same file `bita` takes.
`--etcd-endpoint <URL>`, repeated for every member, and `--etcd-key-prefix <PREFIX>` give the same
directly and replace what the file says. The tool reads no environment variables: the command line of a
benchmark should say everything about the run.

In a container, the tool is the image `bita-bench`: a Kubernetes Job next to a cluster runs it with the
same arguments, for example `--etcd-endpoint http://etcd:2379 append bench --create`. Put the Job on a
node that runs no replica: the tool is part of what is measured.

## Output and exit codes

The report is printed to standard output, one value per line, or as JSON with `--output json`. The JSON
field names carry their units and are stable, so that two runs can be compared by a script.

| Exit code | Meaning |
| --- | --- |
| 0 | The workload ran to its limit. |
| 1 | The workload could not run, or stopped early because an append or a read failed. What was measured until then is still printed. |
| 2 | The command line or the configuration is wrong. |

## `bita-bench append`

Appends records to a stream and measures how long each append takes from the call to its commit on a
write quorum.

The tool becomes the stream's writer. A stream has one writer, so this fences whoever held the stream:
run it on streams of its own. By default it waits for a live writer's lease to expire; `--force` takes
the stream at once.

| Option | Default | Meaning |
| --- | --- | --- |
| `--record-bytes <BYTES>` | 1024 | The payload size of every record. |
| `--records-per-append <RECORDS>` | 1 | How many records one append carries. |
| `--appends-in-flight <APPENDS>` | 16 | How many appends are in flight at any moment, per stream. |
| `--seconds <SECONDS>` | 10 | Stop after this long. |
| `--records <RECORDS>` | | Stop after this many records per stream instead, rounded up to whole appends. |
| `--rate <RECORDS_PER_SECOND>` | | Pace each stream to this rate instead of appending as fast as the cluster commits. |
| `--streams <N>` | 1 | Write this many streams at once. With more than one, they are called `<STREAM>-0`, `<STREAM>-1`, ... |
| `--create` | | Create the streams that do not exist, with three replicas and a write quorum of two. |
| `--force` | | Take each stream from a live writer at once. |

`--seconds` and `--records` exclude each other.

```console
$ bita-bench --config cluster.toml append bench --create --seconds 30
append      stream bench, 1 stream(s), 16 appends in flight each, 1 record(s) of 1024 B per append, as fast as the cluster commits
elapsed     30.002 s
appends     <count>
records     <count> (<count> records/s)
throughput  <n> MiB/s
latency     p50 <n> ms  p90 <n> ms  p99 <n> ms  p99.9 <n> ms  max <n> ms
```

The same run as JSON has these fields:

```json
{
  "workload": "append",
  "stream": "bench",
  "streams": 1,
  "record_bytes": 1024,
  "records_per_append": 1,
  "appends_in_flight": 16,
  "target_records_per_second": null,
  "elapsed_milliseconds": 30002,
  "appends": 0,
  "records": 0,
  "payload_bytes": 0,
  "records_per_second": 0,
  "bytes_per_second": 0,
  "latency_microseconds": { "p50": 0, "p90": 0, "p99": 0, "p999": 0, "max": 0 },
  "failure": null
}
```

How to read the numbers:

- **Latency** is append-to-commit: from the call of `append` to its return, which happens once a write
  quorum has flushed the record. Percentiles come from a histogram whose buckets are never wider than
  1/32 of their values: a reported percentile is at most 3.2 % above the true one and never below it.
  `max` is exact.
- **With `--rate`**, latency is counted from the moment each append was due, not from the moment it went
  out. If the cluster stalls, the appends that queue up behind the stall count as slow, as they would for
  an application that wanted to append at that rate. If the cluster cannot keep up with the rate at all,
  latency grows for as long as the run lasts; that is the tool saying so.
- **Without `--rate`**, the workload is closed-loop: each of the appends in flight starts when the
  previous one committed. Throughput is then what the cluster takes at that concurrency, and latency says
  nothing about a stall longer than the run of one append.
- **Rates** are the committed totals divided by the elapsed time, rounded down, over all streams.
- **The tool is part of what is measured.** It runs on all cores of its machine; if those are busy, the
  cluster is not the limit. Run it on a machine of its own.

A workload that stops early, because an append failed, prints what it measured and a last line
`stopped     <reason>`, and exits with 1.

## `bita-bench read`

Scans a stream from a sequence number on and measures records and payload bytes per second.

| Option | Default | Meaning |
| --- | --- | --- |
| `--from <SEQUENCE>` | The oldest record still kept | Where the scan starts. |
| `--seconds <SECONDS>` | 10 | Stop after this long. |
| `--records <RECORDS>` | | Stop after this many records instead. |

The scan also ends at the end of the stream. It reads sealed segments from replicas, or from the object
store if the configuration names one and the replicas no longer hold the segment.

```console
$ bita-bench --config cluster.toml read bench --records 1000000
read        stream bench
elapsed     <n> s
records     <count> (<count> records/s)
throughput  <n> MiB/s
```

## What the tool does not do

- It injects no faults. The design lists a fault schedule among the benchmark's options; faults are
  exercised by the deterministic simulator instead.
- It does not compare with other systems. The design names Kafka with `acks=all` and BookKeeper on the
  same hardware as reference points; that comparison is made by running them.
