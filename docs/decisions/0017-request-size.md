# 0017. An append request carries up to 4 MiB

Status: accepted · 2026-11-26

## Context

The design closes a batch "after 1 ms or 256 KiB, whichever comes first"
(section 5). Record 0011 replaced the timer: a writer has one request in
flight per replica, and the next request carries what arrived in the
meantime. It kept 256 KiB as the most one request carries.

A replica makes a request durable with one flush. With one request in
flight per replica, a stream therefore moves one request per flush, and its
throughput is the request size divided by the time a flush takes.

The first cluster on real machines had network block storage, where a
flush of 256 KiB took about 10 ms ([measurements](../measurements.md)). One
stream of 64 KiB records with 128 appends in flight did this:

| Request size | MiB/s | p50 | p99 |
| --- | --- | --- | --- |
| 256 KiB | 23.8 | 344 ms | 442 ms |
| 1 MiB | 71.5 | 111 ms | 168 ms |
| 4 MiB | 104.3 | 74 ms | 160 ms |
| 8 MiB | 120.3 | 65 ms | 117 ms |

At 256 KiB the stream stayed under half of the design's target of 50 MiB/s,
with its replicas flushing 86% of the time. A flush there costs a few
milliseconds whatever it carries and little more per megabyte, so larger
requests move more with fewer flushes, and the appends that wait behind
them wait less.

A larger limit costs nothing when there is little to send, because a
request carries what has accumulated and no more: with a limit of 4 MiB,
16 appends in flight of 1 KiB records took 14.1 ms at p50 and 24.1 ms at
p99, as with 256 KiB, in requests of 10 KiB.

## Decision

Both client libraries send up to 4 MiB of batch frames in one request by
default. `WriterSettings::max_request_bytes` still sets it in the Rust
client; the Go client has it as a constant, like its other timings.

4 MiB and not 8, although 8 MiB moved 15% more: a request is the limit
plus up to one batch, the transport refuses a message over 16 MiB, and a
writer holds a request's bytes once per replica. 4 MiB meets the target
twice over and leaves that room.

## Not done

- **Several requests in flight per replica**, so that a replica can make
  the requests that arrived during one flush durable with the next. It
  would lift the same limit without larger requests, at the price of
  acknowledgements that arrive out of step with the sends and a replica
  that must order them. Nothing measured so far asks for it.
- **A limit that adapts** to how long flushes take. The fixed one already
  does not bind when there is little to send.

## Consequences

- A stream that is given more than it can commit has up to 4 MiB in flight
  to each replica: a writer under load holds up to three requests of that
  size per stream, and a replica one per segment that is being written.
- A candidate that takes a stream over copies the tail of the last segment
  in pieces of the same size.
- The numbers above come from one environment, in runs of a minute made
  once. Disks like those the design assumes, where a flush is some ten
  times faster, were not measured: there 256 KiB was not expected to bind,
  and whether 4 MiB changes anything is unknown.
- The design's batch size is no longer a number of the implementation. Its
  intent is kept: a busy stream fills its requests, an idle one sends at
  once.
