# bitacora-metrics

The second materializer: a small time-series store on one stream. It appends
batches of samples to the log, serves recent samples from memory and older
ones from immutable blocks in the object store.

It exists to test the materializer framework from the outside (design,
section 18, milestone M6). bitacora-kv was written together with the
framework; this crate was written afterwards, against the framework's public
interface only, for the other kind of workload: data that is only ever added
and cut into immutable files. What that showed about the interface is in
`docs/decisions/0015-metrics-materializer.md`.

It is not a replacement for a real time-series database: no query language,
no retention by time, and blocks in its own format, not Prometheus's.

## What it owns

- The record format of a metrics stream (`encode_record`, `decode_record`).
- The block format: chunks, series index, postings, footer.
- The store that appends samples and answers queries (`MetricsStore`).
- A reader of the committed blocks for other processes (`BlockReader`).

It owns no storage of its own. Records go to the log through
`bitacora-client`; blocks are artifacts kept by the immutable-artifacts
adapter of `bitacora-materializer`.

The bytes are specified in `docs/protocol/metrics.md`.

## Design

| Operation | What happens |
| --- | --- |
| `append(batch)` | One record is appended; returns its sequence number once committed |
| `query(selector, time_range, position)` | The samples of the selected series in the range, from blocks and memory |
| `blocks()` | The catalog of blocks, in log order |
| `BlockReader::query(selector, time_range)` | The same query against the committed blocks only |

**The life of a sample.** A batch of samples is one log record. The store's
materializer applies committed records in order: it decodes each one and
adds its samples to the **head**, an ordered map in memory from label set to
samples. The record that fills the head ends a **block**: the samples up to
it are written as one object and leave the head. At a checkpoint, the part
of the head that is still filling is saved next to the blocks, so that the
log below the checkpoint can be truncated; the next store reads it back and
goes on where this one was. Neighbouring blocks of similar size are merged
into larger ones.

**One sample per series and timestamp.** If two records hold a sample for
the same series and timestamp, the later record wins. Samples may arrive in
any order. The rule makes appending a batch twice harmless, and it is what
lets a restarted store apply records again that it had applied before.

**Blocks follow the log, not the clock.** A block covers a range of sequence
numbers. Blocks never overlap in sequence numbers and may overlap in time. A
query reads them in log order, so the latest record always wins.

**When a block is cut.** A `BlockPolicy` gives two limits: a number of
samples and a span of time, measured by the samples' own timestamps. The
record that brings the head to either one ends the block. Nothing else cuts
a block: not a checkpoint, not a restart, not a clock. The same records
therefore give the same blocks, bit for bit, whenever checkpoints happen and
whichever store runs.

### What a query sees

A query sees every sample of every record up to one record, and nothing of
any later record. The answer names that record as `as_of`.

- `QueryPosition::Applied` takes whatever the materializer has applied when
  the query runs.
- `QueryPosition::AtLeast(sequence)` first waits until the materializer has
  applied that record. Passing the sequence number `append` returned gives
  **read-your-writes**. If the record is not applied within
  `MetricsSettings::query_wait`, the query fails with `NotApplied`: it gives
  no answer rather than one that lacks the samples.

For each series and timestamp the answer holds the sample of the latest
record up to `as_of`. Series come in ascending order of label set, samples
in increasing order of time, each timestamp once.

The answer does not depend on where the samples are. A query reads the head
first and the catalog second. If a block was cut in between, the catalog
already covers what the head held, and the answer comes from the blocks
alone. The head drops sealed samples a moment before their block is listed;
a query that looks in that moment sees that the catalog is short and looks
again. If a block the query wants was merged away, the query reads the
catalog again.

There is no linearizable read. A metrics store has many producers and no
single latest value a reader could be wrong about; read-your-writes by
sequence number is what a dashboard or a rule evaluation needs.

### What a query fetches

Nothing from a block whose samples all lie outside the query's time range:
the block's catalog entry carries its summary, with its first and last
timestamp. From every other block: the footer and the index, once, then
only the chunks of the selected series, with range reads. Chunks that lie
next to each other are fetched in one read. The index of every block that
was read is kept in memory.

### A reader that is not the store

`BlockReader` answers from the blocks of the last committed checkpoint and
from the part of the head that checkpoint saved, which has the layout of a
block. It shares nothing with the store but the metadata store and the
object store. Its answers are exact up to that checkpoint; it does not see
what the store applied since. It looks for a newer checkpoint when asked to
(`refresh`) and when it finds a block gone.

## Invariants

- A sample reported as appended is in the log once, at the returned
  sequence number.
- An answer with `as_of` = N holds exactly what the records 1 to N make of
  the selected series.
- The blocks of the catalog, and after them the saved head, cover every
  record up to the checkpoint, without gap or overlap.
- Where a block ends depends on the records alone.
- Nothing is returned from a block before the checksum that covers it was
  verified.
- Encoding is deterministic: the same samples give the same block, bit for
  bit.

## Which file holds what

| File | Holds |
| --- | --- |
| `sample.rs` | `Labels`, `Sample`, `SeriesSamples`, `TimeRange`, and the "later wins" merge |
| `record.rs` | The log record: `encode_record`, `decode_record` |
| `chunk.rs` | The samples of one series, compressed |
| `block.rs` | The block: encoding, index, range reads |
| `selector.rs` | `Selector` and `Matcher` |
| `head.rs` | The samples in memory |
| `builder.rs` | `BlockBuilder`: records to blocks, and merging; it owns the head |
| `materializer.rs` | `MetricsMaterializer`: the framework's catalog over the builder, and its name |
| `query.rs` | One answer from the head and the blocks |
| `store.rs` | `MetricsStore`, its settings and errors |
| `reader.rs` | `BlockReader` |

Tests sit next to the file they test, in `<file>_test.rs`. `test_support.rs`
holds what they share.

## Not done

- Retention: nothing deletes old blocks.
- Matchers by regular expression, and any query language.
- Blocks in the Prometheus TSDB format, and blocks aligned to time.
- An append gateway that batches the samples of many producers, and a
  producer in Go.
- A bound on the memory the cached block indexes take.
- Followers: stores that tail the stream without being the writer.

## Dependencies

May depend on `bitacora-types`, `bitacora-artifact`, `bitacora-meta`,
`bitacora-client` and `bitacora-materializer`, and only on what those crates
make public. Tests also use `bitacora-testkit` and `bitacora-controller`.
