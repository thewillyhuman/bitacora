# 0011. Batches form without a timer

Status: accepted · 2026-11-25

## Context

The design closes a batch "after 1 ms or 256 KiB, whichever comes first"
(section 5). A linger timer trades latency for throughput: every append on
an idle stream waits a millisecond it does not need to.

## Decision

There is no timer. The records of one `append` or `append_batch` call form
one batch frame, and each replica has at most one request in flight. When a
request returns, the next one carries every frame that was enqueued in the
meantime, up to the request size limit of 256 KiB.

The effect is the batching the design wants, clocked by the replicas
themselves. An idle stream sends a record at once. A busy stream fills each
request with what arrived during the previous round trip, and one flush on
the replica covers all of it.

## Consequences

- Latency on an idle stream is one round trip and one flush, with nothing
  added.
- Batch boundaries are the caller's. An application that wants many small
  records in one checksummed batch calls `append_batch`.
- A frame is never split or merged after it is encoded, so a resend carries
  the same frames and a replica can recognise what it already holds.
- Throughput under load depends on the round-trip time instead of a fixed
  interval. The benchmark tool is the place to measure whether a small
  linger would ever pay off.
