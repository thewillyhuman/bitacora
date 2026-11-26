# 0015. The second materializer and what it found in the interface

Status: accepted · 2026-11-26

## Context

The materializer interface had one consumer, bitacora-kv, written by the
people who wrote the interface. The design names that as a risk (section
20: "an interface with one consumer is usually wrong") and answers it with
milestone M6: a second materializer, for metrics blocks, built through the
public interface only. Whatever it cannot do is fixed, and then the
interface is frozen for 1.0.

`src/rust/bitacora-metrics` is that materializer. It was first written
against the public interface of `bitacora-materializer`, `bitacora-client`,
`bitacora-artifact`, `bitacora-meta` and `bitacora-types` as they were,
with a workaround wherever the interface fell short. This record says what
was built, and lists every place where the interface was missing something,
was awkward, or forced a workaround.

## Decision

### What was built

A time-series store on one stream, on the adapter for immutable artifacts
(`ArtifactBuilder`, `ArtifactCatalog`), the adapter bitacora-kv does not
use.

- **A record is a batch of samples** for one or more series. Format version
  1: a version byte, then series, each with its sorted label set and its
  samples as a timestamp in milliseconds and the bits of a double. No
  checksum of its own; the log checksums what it stores.
- **The head** holds the samples of the records applied and not yet in a
  block, in memory, by series.
- **A block is one artifact**: one compressed chunk per series
  (delta-of-delta timestamps and XOR-ed values, as in the Gorilla paper), a
  series index, postings by label name and value, and a footer with the
  format version, the counts, the minimum and maximum time and the offsets.
  Chunks, index, postings and footer each have a CRC-32C. Format version 1.
- **Compaction** merges neighbouring blocks through `merge_artifacts`: the
  inputs are read whole and verified, merged in log order, and written as
  one block.

The bytes are specified in `docs/protocol/metrics.md`.

### One sample per series and timestamp; the later record wins

Samples arrive out of order and more than once: producers retry, and a
restarted materializer applies records again that it had applied before.
The store keeps at most one sample per series and timestamp, and the sample
of the later record replaces the earlier one. "Later" is the order of the
log, the only order every reader agrees on. The same rule is applied when
records enter the head, when blocks are merged and when a query joins
blocks and head, so all three give the same result as one pass over the
log. That is invariant I6 for this workload.

### Blocks follow the log, not the clock

The catalog of the artifacts adapter is a list of artifacts that cover
consecutive ranges of sequence numbers. A block is therefore the samples of
a range of records. Blocks do not overlap in sequence numbers and may
overlap in time. A query reads them in catalog order, which is log order,
so the rule above holds across blocks without comparing anything but
positions.

### What a query sees

A query sees every sample of every record up to one record and nothing of a
later one, and names that record in its answer (`as_of`).

- `QueryPosition::Applied`: up to whatever the materializer has applied.
- `QueryPosition::AtLeast(sequence)`: the query first waits until the
  materializer has applied that record. With the sequence number an append
  returned, this is read-your-writes. If the record is not applied in time
  the query fails; it never answers without it.

There is no linearizable read as in bitacora-kv. A metrics stream has many
producers and no single latest value a reader could be wrong about.

A query reads the head first and the catalog second. A block can be cut in
between. The head says which records it holds and the catalog says where it
ends, and that is enough: if the head reaches beyond the catalog, the answer
is the blocks with the head on top; otherwise the blocks alone. A block
that was merged away under a query makes the query read the catalog again.

`BlockReader` answers the same queries from the committed blocks alone, for
a process that is not the store. It does not see the head.

## What the interface could and could not do

Each row is one thing the metrics store needed. "Nothing missing" rows are
listed too: they are the parts of the interface the second consumer
confirmed.

| # | What was needed | What the interface offered | What was done | Status |
| --- | --- | --- | --- | --- |
| 1 | Queries must read the samples that are not in a block yet. | `ArtifactCatalog` owns the builder and shows only the catalog (`view()`). The unsealed samples are inside the builder, out of reach. | `MetricsMaterializer` wraps the catalog and keeps a second head for queries: every record is decoded twice and every unsealed sample is held twice (`materializer.rs`). | Open |
| 2 | A block is cut when it is full, by the samples' own count and time span, so that the same records give the same blocks (I6). | A checkpoint always seals everything the builder holds. `should_seal` can ask for a checkpoint earlier; it cannot keep the runner's timer from cutting a block. Block boundaries therefore follow the wall clock of the instance that happened to run. | Accepted in the first version: answers are the same whenever checkpoints happen, block contents are not. A store that checkpoints every minute cuts a block every minute. | Open |
| 3 | A query skips the blocks that hold no sample in its time range without fetching anything. | A catalog entry holds a range of sequence numbers, a key and a size. Nothing of the builder's. | The time range of a block is read from its footer: two range reads per block the first time a store or reader sees it, also for blocks the query does not need. The index is then cached (`query.rs`). | Open |
| 4 | The limits of a block come from configuration. | The framework creates the builder with `Default` and restores a materializer with a function that takes no settings. | The limits are constants of a type parameter, `BlockPolicy`, which runs through `BlockBuilder<P>`, `MetricsMaterializer<P>` and `MetricsStore<P>`. They cannot be chosen at run time. | Open |
| 5 | A process that is not the store reads the committed catalog. | Possible: `MetadataStore::get_stream`, `MetadataStore::get_checkpoint`, `ArtifactNamespace::read_only`, `decode_manifest`, `decode_catalog`. | `BlockReader::refresh` makes the five calls and handles the manifest being replaced between two of them. Every query node would repeat those thirty lines. | Open |
| 6 | A reader in another language is written from `docs/protocol`. | The layouts of the checkpoint manifest and of the catalog are only in the headers of `manifest.rs` and `artifact_catalog.rs`. | `docs/protocol/metrics.md` points at those files. | Open |
| 7 | A query node learns that the catalog changed. | Nothing. The checkpoint record can only be read. | `BlockReader` refreshes when asked and when it finds a block gone. | Open |
| 8 | Several stores serve queries with a head: one writes, the others follow. | `MaterializerRunner::start` always takes the fencing token, which stops the instance that had it. `ArtifactNamespace::read_only` exists "for a follower", but nothing runs one. | Not built. `BlockReader` serves blocks without a head. | Open |
| 9 | Old blocks are deleted after a retention time; blocks are re-cut by time. | `merge_artifacts` turns several neighbours into exactly one artifact. Nothing can drop an artifact or return several. | Not built. | Open |
| 10 | A block stays readable for queries that started before it was merged away. | The runner deletes what a manifest no longer names right after it commits that manifest. | A query that finds a block gone reads the catalog again (`query.rs`, `reader.rs`). | Open |
| 11 | The store reports why its materializer stopped. | `RunnerProgress` only says that the runner stopped; the reason is what `run` returns. | The store's task hands the reason to queries through a channel (`store.rs`), as bitacora-kv does. | Open |
| 12 | Read-your-writes. | `RunnerProgress::wait_until_applied`. | Used as is. | Nothing missing |
| 13 | Fetch a part of a block. | `ArtifactNamespace::get_artifact_range`, and the size of every artifact in its catalog entry. | Used as is. | Nothing missing |
| 14 | Checkpoint, restart, apply the log again from the checkpoint. | The runner. A restarted store restores the catalog and gets every record above the checkpoint again. | Used as is. | Nothing missing |
| 15 | A replaced store must not damage the catalog. | The fencing token in the checkpoint commit and in every artifact key. | Used as is. Tests replace a store in the middle of appends. | Nothing missing |
| 16 | The log is truncated below the checkpoint and never above it. | Registering the materializer holds truncation at its checkpoint. | Used as is. Tests truncate while queries run. | Nothing missing |
| 17 | Artifacts nobody names are deleted; named ones never are. | The sweep after every commit, and `sweep_orphans`. | Used as is. | Nothing missing |
| 18 | Report a record that cannot be applied, and a damaged block. | `MaterializerError::InvalidRecord` and `DamagedArtifact`. | Used as is. | Nothing missing |
| 19 | Count the reads a query makes, damage a block, make the store fail. | `MemoryArtifactStore::served_range_reads`, `corrupt_byte`, `set_unavailable`. | Used as is. | Nothing missing |

Row 2 is the one that matters most. The design asks that "`should_seal`
must be free to decide on its own criteria" (section 19, "Time-based
sealing"), and the adapter takes that choice away at every checkpoint.

Rows 6 and 7 are also asked for by the design: the catalog, "its format
and its change notifications are [...] part of the public interface from
M4" (section 19, "Common thread").

## Not done

- **A simulation workload.** The simulator runs one stream with bitacora-kv
  front ends, and its fault injection is tied to them. A second workload
  would need its own compute nodes and its own checker. The tests of this
  crate run on the same in-process cluster, with failover, truncation and
  the sweeper, but not under the simulator's fault matrix.
- **Blocks in the Prometheus TSDB format.** The design asks for it so that
  existing query engines read the blocks. Those blocks are aligned to time;
  the catalog is aligned to the log (row 9).
- **An append gateway and a Go producer.** A stream has one writer, so many
  producers need a process in front that batches for them (design, section
  19).
- **Retention, regular-expression matchers, a query language.**

## Consequences

- The artifacts adapter has a consumer, and its catalog was read by a
  process that is not the materializer.
- The parts of the framework that bitacora-kv exercised hold for a second
  workload unchanged: the runner, fencing, the sweeper, truncation.
- The adapter itself needs changes before the freeze. The rows marked open
  say which.
