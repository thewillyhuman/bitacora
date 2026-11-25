# bitacora — design document

Nov 25, 2026 · @Guillermo Facundo Colunga

## 1. Summary

bitacora is an open source, Rust-based storage layer that lets a data system separate compute from storage the way AWS Aurora does. It has two parts: a replicated, low-latency log, and a framework of materializers that turn that log into queryable state.

The log is the only thing a compute node writes to. Everything else (pages, blocks, index segments) is derived from it by materializers, which run and scale independently. A compute node therefore holds no durable state and can be replaced, added or failed over without copying data.

### Goals

- **G1. A durable, single-writer log.** Quorum-replicated appends on local SSDs across failure domains, with commit latency in the low milliseconds and no consensus round on the write path.
- **G2. A materializer framework.** One runtime that supports two shapes: versioned pages (the Aurora shape, for databases) and immutable artifacts covering a sequence range (the shape metrics and log systems use).
- **G3. One reference materializer in-tree.** A small versioned key-value store that proves the interface and drives the test suite.
- **G4. Correctness first.** Written invariants, a model-checked protocol, and deterministic simulation with fault injection from the first commit.
- **G5. Runs on ordinary infrastructure.** Virtual machines or Kubernetes with local disks, plus any S3-compatible object store.

### Non-goals for version 1

- Multiple concurrent writers on one stream.
- Transactions across streams.
- Replacing Kafka: no consumer groups, partitions or key compaction.
- The PostgreSQL integration itself. It becomes a separate project that depends on bitacora (section 19).
- Cross-region replication and Byzantine fault tolerance.

### Scope of this document

Sections 4 to 10 specify the log. Sections 11 and 12 specify the materializer framework and the reference materializer. Sections 13 to 18 cover API, operations, testing, performance, code layout and roadmap. Section 19 states what the PostgreSQL, metrics and logs materializers will need from the interface, without designing them.

## 2. Background and prior art

bitacora combines three proven ideas: Aurora's "the log is the database", BookKeeper's sealed segments with fencing, and Neon's split between a log service and a page service. No open system offers the log together with a workload-neutral materializer framework.

| System | What it is | What bitacora takes from it | Where bitacora differs |
| --- | --- | --- | --- |
| [Amazon Aurora](https://pdos.csail.mit.edu/6.824/papers/aurora.pdf) | Closed-source database whose storage service receives only redo records and materializes pages | A single writer assigns sequence numbers; quorum acknowledgements replace consensus on the write path; storage applies the log | Open source; the apply step is a plug-in, not MySQL or PostgreSQL redo |
| [Apache BookKeeper](https://bookkeeper.apache.org/docs/development/protocol/) | Open source replicated log segments ("ledgers") with fencing | Seal and roll instead of in-place membership change; fencing; metadata changed only by compare-and-swap | A stream is one continuous sequence; tiering and materializers are built in; Rust, no JVM |
| [Neon](https://neon.com/docs/introduction/architecture-overview) | Open source PostgreSQL storage: consensus-based WAL "safekeepers" plus a "pageserver" | The log service / page service split; page reads addressed by page and sequence; layered artifacts on object storage | Not tied to PostgreSQL; no consensus round per append |
| [Grafana Mimir ingest storage](https://grafana.com/docs/mimir/latest/get-started/about-grafana-mimir-architecture/about-ingest-storage-architecture/) | Metrics database that writes to Kafka and lets ingesters consume it to build blocks | Evidence that log plus materializer fits metrics | The log and the materializer share one truncation contract and one operational model |
| Corfu, Delos | Shared-log systems from research and from Meta | The log as the single primitive other systems build on | bitacora adds the materialization half |

### Why not reuse one of them

- BookKeeper is the closest match for the log alone, but it brings a JVM and ZooKeeper, and its unit is the ledger, so every user rebuilds streams, tiering and truncation on top.
- Kafka is built around partitions and consumer groups and favours throughput over per-record commit latency, which is the opposite trade-off from a database write-ahead log.
- Neon's log service is close to generic, but its page service embeds PostgreSQL redo. bitacora wants that boundary as a public interface.

The cost of not reusing them is real: the failover protocol in section 9 is new code that must earn trust through the testing in section 15.

## 3. Architecture overview

bitacora has one data path and one control path, and they never share a hop. A compute node appends to three replicas and commits at two acknowledgements; materializers tail the committed log and serve reads back to compute.

&#91;embedded content: bitacora architecture · compute, log, materializers, object store\]

The arrow on the left edge is the read path: compute asks a materializer for state as of a sequence number. The controller and the metadata store sit beside the data path. They are consulted when a segment rolls or a writer changes, never on an append or a read.

| Component | Role | Runs as |
| --- | --- | --- |
| Client library | Assigns sequence numbers, replicates batches, tracks the durable sequence, runs failover | Linked into the compute node |
| Replica | Appends batches to segment files, flushes, serves reads | One process per storage node |
| Metadata store | Holds epochs, segment lists and checkpoints with compare-and-swap | etcd in version 0 |
| Controller | Placement, repair, tiering, retention | Leader-elected process |
| Materializer runner | Tails the log, applies records, checkpoints | One active instance per stream and materializer |
| Object store | Cold log segments, artifacts, metadata snapshots | External, S3-compatible |

## 4. Data model and terminology

A stream is an ordered sequence of records, cut into segments, each stored on a fixed set of replicas. Everything else in the design is bookkeeping around those three things.

| Term | Definition |
| --- | --- |
| Stream | A named, totally ordered sequence of records with exactly one active writer. It is the unit a compute node owns: one database, one tenant's metrics, one index shard. |
| Record | An opaque payload of up to 4 MiB, plus a sequence number and a CRC32C checksum. bitacora never interprets payloads; materializers do. |
| Sequence number | A 64-bit integer, dense and starting at 1 within a stream, assigned by the writer. |
| Epoch | A 64-bit writer generation per stream, held in the metadata store and incremented on every writer change. All fencing rests on it. |
| Segment | A contiguous range of a stream's sequence numbers stored on one replica set. Target size 1 GiB or 1 hour, whichever comes first. |
| Replica set | The N replica nodes that hold a segment, spread across failure domains. Default N = 3 with write quorum W = 2. N = 6 with W = 4 across three zones gives Aurora-style durability. |
| Durable sequence | The highest sequence number D such that every record at or below D is stored on at least W replicas. Readers are only ever served records at or below D. |
| Checkpoint | A materializer's durable statement: "everything up to sequence C is now in my artifacts". |
| Low-water mark | The minimum checkpoint across a stream's registered materializers, further limited by retention policy. Segments entirely below it may be deleted. |

### Two structural rules

These two rules remove most of the complexity that replicated logs usually carry.

1. **One segment, one epoch.** A new writer never appends to its predecessor's segment. It seals that segment and opens a new one. Within a segment there is therefore only one possible record at each sequence number, and replicas never have to reconcile divergent histories. They can only differ in how long their copy is.
2. **A replica set never changes while its segment is open.** When a replica fails, the writer seals the segment at the durable sequence and rolls to a new segment on a healthy set. Sealed segments are repaired in the background, where there is no concurrency with appends.

### Quorum arithmetic

A record is committed at W acknowledgements. Recovery must hear from N − W + 1 replicas, which guarantees that at least one of them holds every committed record. For N = 3, W = 2 that is 2 replicas; for N = 6, W = 4 it is 3.

## 5. Write path

An append costs one network round trip to the replicas and one disk flush on each; the metadata store is never touched. The writer is a library inside the compute node, not a separate service.

1. **Assign.** The writer gives each record the next sequence number and groups records into batches. A batch closes after 1 ms or 256 KiB, whichever comes first.
2. **Send.** The writer sends each batch to all N replicas of the open segment in parallel, over one long-lived connection per replica. Every batch carries the stream, segment, epoch, first sequence number and the writer's current durable sequence.
3. **Persist.** A replica accepts the batch only if the epoch is not fenced, the segment is open, and the batch starts exactly where its local copy ends. It appends, flushes to disk, then acknowledges with its new local end.
4. **Commit.** The writer tracks the acknowledged end of each replica. The durable sequence is the W-th highest of those values. Appends complete, in order, as the durable sequence passes them.
5. **Publish.** The durable sequence travels to replicas on the next batch, or on a heartbeat every 50 ms when the stream is idle. Replicas use it as the limit for what they serve to readers.

### Flow control

The writer holds at most 64 MiB of unacknowledged records per stream. Beyond that, `append` waits. This bounds memory and gives the compute node a natural backpressure signal.

### A slow or failed replica

The writer keeps committing as long as W replicas answer. If a replica is unreachable for 10 seconds, or falls more than 64 MiB behind, the writer rolls:

1. It stops assigning records to the current segment.
2. It writes one compare-and-swap to the metadata store, conditional on its epoch: "segment S is sealed at durable sequence D; segment S+1 starts at D + 1 on replica set R′".
3. It tells the old replicas the sealed end. A replica holding records above D discards them.
4. It resends every record above D to the new segment, with the same sequence numbers.

Step 4 is safe because records above D were never reported as committed, and the same writer still holds them in memory. To keep a roll from waiting on placement, the controller pre-allocates one standby segment per active stream.

### What an append result means

| Result | Meaning |
| --- | --- |
| Success | The record is stored on at least W replicas and will be visible to every reader. |
| Fenced | Another writer has taken the stream. This writer must stop. The record may or may not have been committed. |
| Indeterminate | The writer lost contact before learning the outcome. The record may or may not have been committed. |

Indeterminate outcomes are unavoidable in any replicated log. Engines handle them the way they handle a crash between writing and acknowledging a commit: the log decides, and the engine reads it back after recovery.

## 6. Read path

Any single replica can serve a read, because a replica only serves records at or below the durable sequence, and rule 1 of section 4 guarantees its copy of those records is the only one that can exist.

### Reading a range

1. The reader asks the metadata store for the segments covering the range and caches the answer. Sealed segments never change, so only the entry for the open segment can go stale.
2. For each segment it picks a replica, preferring one in its own failure domain, and requests records from a starting sequence number.
3. The replica streams records up to the smaller of its local end and the durable sequence it knows.
4. The reader verifies each batch checksum. On a mismatch, timeout or short answer it switches to another replica and reports the first one to the controller.

### Following the tail

A materializer needs records as soon as they commit. It opens a subscription on a replica of the open segment. The replica pushes new records whenever its known durable sequence advances, which happens within one batch interval of the commit. When the segment is sealed, the replica sends the sealed end and the reader moves to the next segment.

A follower that has fallen behind reads sealed segments at full speed until it reaches the tail, then switches to push. There is one code path for both.

### Reading tiered segments

Segments that have moved to the object store are read with range requests through the same reader interface. A small local cache holds recently fetched blocks. Callers cannot tell where a segment lives, apart from latency.

### Guarantees to readers

- A reader sees a gapless prefix of the stream, in order.
- The durable sequence a reader observes never moves backwards, even when it switches replicas.
- A record once seen is never replaced by a different record at the same sequence number.
- A reader asking for a sequence number below the low-water mark gets an explicit `Truncated` error with the current low-water mark, never silence.

## 7. Replica node

A replica is a deliberately simple process: it appends batches to segment files, flushes them, and serves them back. It makes no decisions about ordering, membership or leadership.

### On-disk layout

Each segment is one preallocated file plus a small sidecar for state.

```text
<data_dir>/<stream_id>/<segment_id>.seg     records
<data_dir>/<stream_id>/<segment_id>.state   fenced epoch, sealed end

segment file
  header   magic, format version, stream id, segment id, epoch, first sequence
  batch*   length u32 | first sequence u64 | record count u32 | crc32c u32 | records
  record   length u32 | payload
```

The checksum covers the whole batch, including its header fields, so a batch written at the wrong offset or for the wrong stream is detected. A sparse in-memory index maps every 64 KiB of file to a sequence number; it is rebuilt by scanning on startup.

### Flush policy

- A batch is acknowledged only after `fdatasync` returns. There is no asynchronous-durability mode.
- Files are preallocated so that a flush does not also have to persist a size change.
- Concurrent batches for the same segment share one flush (group commit).
- A failed flush is fatal. The process exits and the segment copy is treated as suspect, because after a failed `fsync` the kernel may have dropped the dirty pages and a retry can falsely succeed.

Version 0 uses one file per segment. With thousands of active streams per node, that means thousands of independent flushes; a shared per-disk journal, as BookKeeper uses, is the planned answer and is listed as an open question in section 20.

### Per-segment state

| Field | Meaning | Durability |
| --- | --- | --- |
| Fenced epoch | Appends below this epoch are refused | Flushed before the fence is acknowledged |
| Local end | Last sequence number flushed in this copy | Implied by file contents |
| Known durable sequence | Latest value heard from the writer | In memory only; safe to lose |
| Sealed end | Final sequence number once sealed | Flushed when learned |

### Crash recovery

On startup the replica scans the last batches of every open segment and truncates at the first batch whose checksum fails. This can only remove a torn tail that was never acknowledged. Sealed segments are never truncated locally; a damaged sealed copy is reported and replaced from a peer.

### Integrity

- Checksums are verified when a batch arrives, before it is written, and again on every read.
- A background scrubber rereads sealed segments on a rolling schedule, weekly by default.
- Any mismatch marks that copy bad in the metadata store and triggers re-replication (section 10).

### Capacity and admission

A replica registers with the metadata store with its address, failure-domain labels and free space, and renews a heartbeat. When a disk passes 85% full it stops accepting new segments; at 95% it refuses appends, which makes writers roll elsewhere.

## 8. Metadata store and controller

The metadata store is the only place where bitacora needs consensus, and it is touched only when a segment rolls, a writer changes, or a checkpoint advances. Version 0 uses etcd behind a trait; an embedded Raft store can replace it later without protocol changes.

### What it holds

| Record | Contents | Changed when |
| --- | --- | --- |
| Stream | Name, id, replication settings, retention policy | Created, reconfigured |
| Writer | Current epoch, writer identity, lease expiry | Writer starts or fails over |
| Segment list | Per segment: id, first sequence, sealed end, replica set, state, object-store location | Roll, seal, repair, tiering, deletion |
| Checkpoints | Per materializer: name, checkpoint sequence, fencing token, artifact manifest pointer | Materializer checkpoints |
| Replica registry | Address, failure-domain labels, capacity, heartbeat | Replica joins, leaves, heartbeats |

A stream's metadata is a few kilobytes. Ten thousand streams fit comfortably in a small etcd cluster.

### Requirements on the store

- Linearizable reads and compare-and-swap on a single key or a small transaction.
- Watches, so readers and the controller learn about rolls without polling.
- Leases with expiry, used as a liveness hint only. No safety property depends on clocks.

Every mutation by a writer is conditional on "the epoch is still mine". Every mutation by a materializer is conditional on its fencing token. This is what makes a paused or partitioned process harmless when it wakes up.

### The controller

The controller is a separate, leader-elected process that reads and writes the metadata store. It holds no state of its own and is never on the append path. It is responsible for:

- **Placement.** Choosing replica sets that span failure domains and balance free space, and keeping one standby segment ready per active stream.
- **Failure detection.** Noticing replicas whose heartbeat has lapsed.
- **Repair.** Scheduling re-replication of sealed segments that have fewer than N good copies.
- **Tiering and retention.** Moving sealed segments to the object store and deleting segments below the low-water mark.
- **Reconciliation.** Telling replicas about seals they missed, so stray tails are discarded.

If the controller is down, existing streams keep appending and reading. Rolls continue until standby segments run out; repair, tiering and deletion pause.

### If the metadata store is unavailable

Appends and reads on open segments continue, because neither touches the store. Rolls, failovers and checkpoints block until it returns. This is the main availability dependency of the system and the reason the store must itself span failure domains.

## 9. Writer failover and recovery

A new writer takes over by raising the epoch, fencing the old segment's replicas, sealing that segment at the longest copy it can find, and opening a new segment. No committed record can be lost, and the old writer cannot commit anything after the fence.

bitacora does not choose which compute node becomes the writer. The engine's own high-availability logic decides that and calls `StreamWriter::open`. bitacora guarantees that whoever calls it last wins cleanly.

### Protocol

1. **Claim the epoch.** Compare-and-swap the stream's epoch from E to E + 1 in the metadata store, recording the new writer's identity. If the swap fails, someone else is ahead; stop.
2. **Read the segment list.** This read happens after step 1, so it reflects any roll the old writer completed before losing the epoch. Let S be the last segment, with replica set R.
3. **Fence.** Send `Fence(S, E + 1)` to every replica in R. A replica flushes the new fenced epoch to disk, refuses all later appends with a lower epoch, and replies with its local end.
4. **Wait for N − W + 1 replies.** Any committed record is on at least W replicas, so at least one responder holds it.
5. **Choose the end.** The recovery end is the highest local end among the responders.
6. **Repair the tail.** Copy missing records from the longest responder to the others, until every responder holds the segment up to the recovery end.
7. **Seal.** Compare-and-swap, conditional on the epoch still being E + 1: "S is sealed at the recovery end". Then tell the replicas.
8. **Open.** Create segment S + 1 starting at recovery end + 1 on a healthy replica set, with the same conditional write. Begin appending.

### Why it is safe

- **No committed record is lost.** Step 4's quorum overlaps every write quorum, and step 5 takes the maximum.
- **The old writer cannot commit.** At least N − W + 1 replicas are fenced, so it can collect at most W − 1 acknowledgements. Its metadata writes fail because they are conditional on epoch E.
- **Replicas cannot disagree on content.** Segment S was written by epoch E alone, so copies differ only in length (section 4, rule 1).
- **Records beyond the old durable sequence may survive.** A record the old writer sent but never saw committed can end up below the recovery end. That is the indeterminate outcome described in section 5, and it is allowed.
- **A replica that did not respond may hold a longer tail.** Those extra records were on at most W − 1 replicas, so they were never committed. The replica discards them when it learns the sealed end.

### If recovery itself fails

Every step is idempotent. If the recovering writer crashes, the next candidate starts again from step 1 with epoch E + 2. Two attempts may compute different recovery ends, but only one seal can be written, because step 7 is a single conditional write.

### Availability during recovery

Recovery needs N − W + 1 replicas of the old segment and the metadata store. If fewer than W of the old replicas are reachable, the segment is sealed with fewer than W copies and goes straight into repair (section 10). Durability is restored in the background while the stream is already accepting writes on the new segment.

### Leases

The current writer renews a 10-second lease in the metadata store every 3 seconds. By default `open` waits for the lease to lapse before step 1, which prevents accidental takeover of a healthy writer. `TakeoverPolicy::Force` skips the wait. The lease is a courtesy; correctness depends only on the epoch.

### Detecting loss of the stream

A fenced writer learns it on its next append or heartbeat, at most 50 ms later on an idle stream, and returns `Fenced` on all pending and future appends. The compute node must then stop serving writes.

## 10. Segment lifecycle

A segment is open for at most an hour and immutable for the rest of its life. Everything hard (repair, tiering, deletion) happens only to immutable segments.

| State | Entered when | What happens in it | Leaves when |
| --- | --- | --- | --- |
| Standby | Controller pre-allocates it for a stream | Nothing; replicas hold an empty file | The writer rolls into it |
| Open | Writer starts appending | Appends, tail reads | Size or age limit, replica failure, or writer failover |
| Sealed | Sealed end is written to the metadata store | Reads; repair if copies are missing or damaged | N good copies exist and the tiering delay has passed |
| Tiered | Upload to the object store is verified | Reads from local copies, then from the object store | The whole segment falls below the low-water mark |
| Deleted | Controller removes it | Space is reclaimed | End of life |

### Sealing

A segment is sealed by its own writer during a roll (section 5) or by the next writer during failover (section 9). In both cases the seal is one conditional write to the metadata store. Replicas learn the sealed end from the writer or, if they missed it, from the controller's reconciliation loop.

### Repair

1. The controller finds a sealed segment with fewer than N good copies.
2. It picks a target replica that preserves the failure-domain spread.
3. The target pulls the segment from a good copy, verifying batch checksums and the final sealed end.
4. The controller swaps the replica set in the metadata store.

Repair traffic is rate-limited per node so that it cannot starve appends.

### Tiering

Fifteen minutes after a segment is sealed and fully replicated, one replica uploads it to the object store as a single object with a block index in its footer, optionally compressed with zstd. After the upload's checksum is verified, the location is recorded. Local copies stay for a hot-read window, 24 hours by default, then are dropped.

The object store is therefore both the cold read path and the backup. A cluster that loses every replica node keeps all data older than the tiering delay.

### Truncation and retention

A stream's low-water mark is the minimum of:

- the checkpoints of all registered materializers, and
- the explicit `truncate_before` position, if the owner set one.

A retention policy can hold the mark back, for example "keep 7 days" to allow point-in-time recovery, but can never move it past a materializer's checkpoint. Deletion is whole-segment only. A registered materializer that stops checkpointing therefore pins the log, which is deliberate: bitacora raises an alert on checkpoint age, and an operator must unregister a dead materializer explicitly.

## 11. Materializer framework

A materializer is a deterministic function from a prefix of the log to queryable state. The framework supplies everything around that function: tailing the log, checkpointing, truncation, artifact storage and fencing.

### The core contract

```rust
/// Implemented once per workload. The runner owns the instance.
pub trait Materializer: Send + 'static {
    /// Apply committed records, in order. Must be deterministic:
    /// the same records always produce the same state.
    fn apply_records(
        &mut self,
        committed_records: &[LogRecord],
    ) -> Result<(), MaterializerError>;

    /// Persist everything applied so far and describe where it is.
    async fn write_checkpoint(
        &mut self,
        artifact_store: &dyn ArtifactStore,
    ) -> Result<CheckpointManifest, MaterializerError>;

    /// Rebuild state from the last committed checkpoint, if any.
    async fn restore_from_checkpoint(
        last_checkpoint: Option<CheckpointManifest>,
        artifact_store: &dyn ArtifactStore,
    ) -> Result<Self, MaterializerError>
    where
        Self: Sized;
}
```

### What the runner does

1. Reads the materializer's checkpoint from the metadata store and takes a fencing token for it.
2. Calls `restore_from_checkpoint`, then subscribes to the stream at checkpoint + 1.
3. Feeds committed records to `apply_records` in order.
4. Calls `write_checkpoint` when enough bytes or time have passed (64 MiB or 60 seconds by default).
5. Commits the checkpoint with one conditional write: new manifest pointer and new checkpoint sequence, valid only if the fencing token still matches.
6. Reports lag, which is the durable sequence minus the applied sequence.

Step 5 is what advances the stream's low-water mark. Artifacts are written under unique names before the commit, so a crash between steps 4 and 5 leaves only orphan files, which a sweeper deletes.

### Two shapes on top of the core

Most workloads fit one of two patterns. The framework ships an adapter for each, so an integrator implements a smaller trait.

**Versioned pages**, for engines that update pages in place and log redo records:

```rust
pub trait PageMaterializer: Send + Sync + 'static {
    type PageId: Ord + Clone + Send + Sync;

    /// Which pages does this record change?
    fn pages_touched(&self, log_record: &LogRecord) -> Vec<Self::PageId>;

    /// Produce the next version of one page.
    fn apply_record(
        &self,
        page_id: &Self::PageId,
        page_before: Option<&[u8]>,
        log_record: &LogRecord,
    ) -> Result<Vec<u8>, MaterializerError>;
}
```

The adapter, `PageStore`, keeps per-page chains of records in memory and flushes them as immutable layer files: delta layers holding records for a sequence range, and image layers holding full pages at one sequence. This follows Neon's layering. It answers `read_page(page_id, as_of_sequence)` by taking the newest image at or below the requested sequence and replaying the deltas after it. A read for a sequence the store has not reached yet waits, with a timeout.

**Immutable artifacts**, for append-only data that compacts into files:

```rust
pub trait ArtifactBuilder: Send + 'static {
    fn add_records(
        &mut self,
        committed_records: &[LogRecord],
    ) -> Result<(), MaterializerError>;

    /// Has enough accumulated to cut an artifact?
    fn should_seal(&self) -> bool;

    /// Write one immutable artifact covering exactly this range.
    async fn seal_artifact(
        &mut self,
        covered_range: SequenceRange,
        artifact_store: &dyn ArtifactStore,
    ) -> Result<ArtifactDescriptor, MaterializerError>;
}
```

The adapter, `ArtifactCatalog`, tracks which artifact covers which sequence range, exposes the catalog to query nodes, and schedules compaction of small artifacts into larger ones.

### Artifact store

```rust
#[async_trait]
pub trait ArtifactStore: Send + Sync {
    async fn put_object(&self, object_key: &str, contents: Bytes) -> Result<()>;
    async fn get_range(&self, object_key: &str, byte_range: Range<u64>) -> Result<Bytes>;
    async fn delete_object(&self, object_key: &str) -> Result<()>;
    async fn list_prefix(&self, key_prefix: &str) -> Result<Vec<ObjectInfo>>;
}
```

Backends: local filesystem for tests, and any S3-compatible store through the `object_store` crate. A distributed file system such as kolos can be added behind the same trait.

### Rules every materializer must follow

- **Determinism.** State at sequence N depends only on records 1 to N. No clocks, no randomness, no reads of external systems inside `apply_records`.
- **Replay safety.** After a crash the runner replays from the last checkpoint. Records above the checkpoint will be applied again to restored state.
- **One active instance.** The fencing token ensures only one instance per stream and materializer name can commit checkpoints.

### Read scaling and point-in-time reads

Any number of read-only followers can tail the same stream and share the same artifacts, the way Aurora readers share the storage volume. Followers never checkpoint. Because the page adapter keeps versions, a read at any sequence above the retention horizon is a point-in-time read; restoring a database to an earlier moment is opening a new stream that starts from an old checkpoint.

### Backpressure

If a materializer's lag exceeds a configured bound, the writer can be told to slow down. This is off by default and matters mainly for the page shape, where a compute node reads its own recent writes through the materializer.

## 12. Reference materializer: bitacora-kv

bitacora-kv is a small versioned key-value store built on the page shape. It exists to keep the interface honest and to give the test suite a workload whose correctness can be checked mechanically. It is not meant to compete with real databases.

### What it does

| Operation | Behaviour |
| --- | --- |
| `put(key, value)` | Appends a record, waits for commit, returns the commit sequence |
| `delete(key)` | Same, with a tombstone |
| `get(key)` | Reads the latest committed value |
| `get_as_of(key, sequence)` | Reads the value as of an earlier sequence |
| `scan(key_range, sequence)` | Ordered range read at one sequence |

### How it maps onto bitacora

- **Compute node.** A stateless front end that owns the stream's writer. It validates requests, appends records, and keeps a cache of hot pages.
- **Log record.** One put or delete, or a batch of them applied atomically.
- **Page.** Keys are hashed into fixed buckets; a bucket is a page holding a sorted run of key-value pairs. `pages_touched` returns the buckets of the keys in the record. `apply_record` merges the changes into the bucket.
- **Read path.** A cache miss calls `read_page(bucket, sequence)` on the page store, the equivalent of Aurora's compute node fetching a page from storage.
- **Read replicas.** Extra front ends tail the stream to keep their caches current and never write.

### What it exercises

- Read-your-writes across the compute and storage boundary.
- Checkpointing, restart and replay.
- Truncation driven by a real checkpoint.
- Point-in-time reads and garbage collection of old page versions.
- Writer failover with a live workload: a new front end opens the stream and resumes at the recovery end.

Because its operations are single-key reads and writes with known results, a recorded history of bitacora-kv operations can be checked for linearizability, which makes it the main workload for the simulation tests in section 15.

## 13. Client API and wire protocol

The client library is where the write protocol lives, so its API is small but its implementation is not. The protocol is specified independently of Rust so that C and Go clients can follow.

### Using the Rust client

```rust
let bitacora_cluster = BitacoraClient::connect(cluster_config).await?;

// Runs the takeover protocol of section 9, then returns a live writer.
let mut stream_writer = bitacora_cluster
    .open_writer("orders-database", TakeoverPolicy::WaitForLeaseExpiry)
    .await?;

// Resolves once the record is flushed on a write quorum.
let commit_sequence = stream_writer
    .append(Bytes::from(redo_record))
    .await?;

// Any number of readers can follow the same stream.
let mut committed_records = bitacora_cluster
    .open_reader("orders-database")
    .follow_from(SequenceNumber::new(1))
    .await?;

while let Some(next_record) = committed_records.next().await {
    let log_record = next_record?;
    apply_to_local_state(log_record.sequence, &log_record.payload);
}
```

### API surface

| Handle | Operations |
| --- | --- |
| `StreamWriter` | `append`, `append_batch`, `durable_sequence`, `close` |
| `StreamReader` | `read_range`, `follow_from`, `low_water_mark` |
| `StreamAdmin` | `create_stream`, `describe_stream`, `truncate_before`, `delete_stream`, `register_materializer`, `unregister_materializer` |

`append` returns a future per record, and futures resolve in sequence order. Dropping a future does not cancel the append; once a record has a sequence number it will be sent.

### Wire protocol

Version 0 uses gRPC with Protocol Buffers for everything, through `tonic`.

| Service | Calls | Used by |
| --- | --- | --- |
| Replica data | `AppendStream` (bidirectional), `ReadStream` (server streaming), `Fence`, `Seal` | Writers, readers |
| Replica transfer | `FetchSegment`, `UploadSegment` | Other replicas, controller |
| Replica admin | `Status`, `Drain` | Controller, operators |

Clients talk to the metadata store directly with its own client library. If profiling shows gRPC framing on the append path is a bottleneck, `AppendStream` can move to a custom framed protocol without changing semantics.

### Other languages

- **C.** A C ABI over the Rust client, with a generated header. PostgreSQL integration needs this.
- **Go.** A native Go client over the same gRPC protocol. The Prometheus and Mimir ecosystem is Go, and cgo would be a poor fit there.

Both depend on the protocol document in `docs/protocol/` being the source of truth, not the Rust code.

### Versioning

- The wire protocol version is negotiated on connect; servers support the current and previous version.
- The segment file header carries a format version; replicas read all older formats and write the newest.
- Metadata records carry a schema version and are migrated lazily on write.

## 14. Operations

A bitacora cluster is three kinds of process plus two dependencies, and it should be runnable by one person.

### What you deploy

| Component | Count | State | Notes |
| --- | --- | --- | --- |
| `bita-replica` | 3 or more, across failure domains | Local SSD | One process per node, one data directory per disk |
| `bita-controller` | 2 or 3, one active | None | Leader-elected through the metadata store |
| Materializer runners | One active per stream and materializer | Cache only | Scale with the number of streams |
| Metadata store | 3 or 5 etcd members | Small | Must span failure domains |
| Object store | External | Durable | Any S3-compatible service |

### Deployment targets

- **Kubernetes.** Replicas as a StatefulSet with local persistent volumes and topology spread constraints; a Helm chart first, an operator later.
- **Virtual machines.** Static binaries with systemd units and one configuration file each, so they fit Puppet or any other configuration management.

Failure-domain labels (zone, rack, host) come from configuration or node labels. Placement refuses to put two copies of a segment in the same domain at the lowest level that has enough members.

### Routine procedures

| Task | Procedure |
| --- | --- |
| Restart or upgrade a replica | `bita replica drain`, which makes writers roll away from it; restart; it rejoins and receives new segments |
| Replace a failed node | Remove it from the registry; repair re-creates its sealed copies elsewhere |
| Add capacity | Start new replicas; new segments land on them first; an optional rebalance moves sealed segments |
| Rolling upgrade | Controllers, then replicas one failure domain at a time, then clients |
| Disaster recovery | Restore the metadata snapshot from the object store; replicas re-register; tiered segments are immediately readable |

The metadata store is snapshotted to the object store every 5 minutes. As a last resort, `bita metadata rebuild` reconstructs segment lists by scanning segment headers on replicas and in the object store.

### Observability

Every process exposes Prometheus metrics, OpenTelemetry traces and structured JSON logs. The signals that matter:

| Signal | Why it matters |
| --- | --- |
| Append latency, p50 and p99, per stream | The user-facing number |
| Replica flush latency | Usually the cause when append latency degrades |
| Replica lag behind the durable sequence | Predicts rolls |
| Segments below N good copies, and for how long | Durability at risk |
| Failover duration | Availability of writes |
| Materializer lag and checkpoint age | Read freshness, and whether the log is pinned |
| Metadata store latency and availability | Blocks rolls and failovers |

Default alert rules and a Grafana dashboard ship in the repository.

### Security

- Mutual TLS between all components, with certificate rotation without restart.
- Stream-level authorization tokens checked by replicas, from the second release.
- Encryption at rest through the object store's server-side encryption and encrypted local volumes. Per-stream envelope encryption is a later option.
- Payloads are opaque to bitacora, so an engine can encrypt records before appending.

## 15. Correctness and testing

bitacora's value is that people trust it with their only copy of committed data, so testing is designed before the code. Six invariants define "correct", and four layers of testing attack them.

### Invariants

| Id | Invariant |
| --- | --- |
| I1 Durability | A record reported as committed appears in every later read of the stream, until it is truncated. |
| I2 Uniqueness | No two readers ever observe different records at the same stream and sequence number. |
| I3 Prefix | Every reader observes a gapless prefix, in order, and its durable sequence never decreases. |
| I4 Single writer | At most one epoch can commit records at any moment. |
| I5 Safe truncation | No record above the minimum registered checkpoint is ever deleted. |
| I6 Determinism | A materializer's state at sequence N is a function of records 1 to N only. |

### Layer 1: model checking the protocol

The append, roll and failover protocols are written as an executable model before implementation, in TLA+ or in Stateright, a Rust model checker. The model explores all interleavings of a writer, a recovering writer and three replicas with crashes and message loss, and checks I1 to I4. The model lives in `specs/` and is updated with every protocol change.

### Layer 2: deterministic simulation

The whole system (replicas, controller, clients, materializers, bitacora-kv) runs in one thread on a simulated clock, network and disk, driven by a random seed. A failing seed reproduces the failure exactly.

- **Framework.** [madsim](https://docs.rs/crate/madsim/0.2.6) is the first choice because it ships simulated drop-in versions of `tokio`, `tonic` and the etcd client, which are exactly bitacora's dependencies. [turmoil](https://github.com/tokio-rs/turmoil) from the Tokio project is the alternative.
- **Design consequence.** All time, randomness, network and disk access go through injected interfaces from the first commit. Nothing calls the system clock or opens a socket directly.
- **Faults injected.** Process crash and restart; network partition, delay, reordering and duplication; torn writes, flush errors, silent corruption, full and slow disks; metadata store outages; long process pauses that outlive leases.
- **Checking.** Every append call and result, and every record read, is logged to a history. A checker verifies I1 to I3 and I5 on the history, and checks bitacora-kv histories for linearizability.
- **Policy.** Every seed that finds a bug becomes a permanent regression test. Continuous integration runs a fixed budget of fresh seeds on every change and a much larger budget nightly.

### Layer 3: component tests

- Property tests for the segment format, index and crash-recovery scan: any byte prefix of a valid file must recover to a valid prefix of its batches.
- Fuzzing of every parser that reads bytes from disk or the network.
- Determinism tests for materializers: apply the same records with different batch boundaries and checkpoint timings, and require identical state.

### Layer 4: real clusters

A nightly job runs a real cluster under a fault schedule: `kill -9`, network faults with `tc`, disk faults with device-mapper targets, and clock jumps. The same history checker runs on the results. This layer catches what simulation cannot: real kernel and file system behaviour around flushes.

### Performance regression

A benchmark suite runs on fixed hardware on every release candidate and fails the release if append latency or throughput regresses beyond 10% against the previous release.

## 16. Performance targets

These are proposed targets, not measurements. They assume three replicas in one region on NVMe disks with power-loss protection, 1 KiB records, and a write quorum of 2. They are to be confirmed or revised at the end of milestone 2.

| Measure | Target |
| --- | --- |
| Append latency, p50 | ≤ 2 ms |
| Append latency, p99 | ≤ 10 ms |
| Sustained throughput per stream | ≥ 50 MiB/s |
| Tail reader delay behind commit | ≤ 5 ms |
| Write unavailability on writer failover, after detection | ≤ 5 s |
| Write pause on a replica-failure roll | ≤ 200 ms |

### Where the time goes

An append costs one batch interval (up to 1 ms), one network round trip, and the second-fastest of three disk flushes. Flush latency dominates, which is why power-loss-protected disks matter more than CPU or network speed. On consumer SSDs without it, a flush can take several milliseconds and the p50 target will not be met.

### Scale targets for the first stable release

- 10,000 streams per cluster, 1,000 concurrently active.
- 100 replica nodes.
- Repair of a failed node's sealed segments limited by a configurable bandwidth cap, not by coordination.

### Benchmarks

`bita-bench` drives a cluster with configurable record size, batch settings, stream count and fault schedule, and reports latency histograms. Kafka with `acks=all` and BookKeeper on the same hardware serve as reference points, to show where bitacora stands and not to claim a win.

## 17. Repository layout

One repository, one Cargo workspace, one crate per component. The log crates never depend on the materializer crates, so the log can be released and used alone.

```text
bitacora/
  crates/
    bitacora-types          ids, sequence numbers, records, errors
    bitacora-proto          Protocol Buffers and generated gRPC code
    bitacora-segment        segment file format, index, recovery scan, scrubber
    bitacora-replica        replica server            (binary: bita-replica)
    bitacora-meta           MetadataStore trait, etcd backend, in-memory backend
    bitacora-controller     placement, repair, tiering (binary: bita-controller)
    bitacora-client         writer, reader, admin
    bitacora-artifact       ArtifactStore trait and backends
    bitacora-materializer   core trait, runner, page and artifact adapters
    bitacora-kv             reference materializer and its front end
    bitacora-sim            simulation harness, fault injection, history checker
    bitacora-bench          load generator            (binary: bita-bench)
    bitacora-cli            command-line tool         (binary: bita)
  specs/                    protocol models
  docs/
    design/                 this document, kept current
    protocol/               language-neutral wire and disk formats
    decisions/              one short record per significant decision
  deploy/
    helm/
    systemd/
```

### Naming

The project, the repository and every crate use the full name, `bitacora`. Everything typed at a shell uses the short form: `bita` for the command-line tool, and `bita-replica`, `bita-controller` and `bita-bench` for the other binaries. Rust types use the full name, as in `BitacoraClient`. Configuration files, metric names and environment variables take the `bita_` prefix, to match what operators type.

### Dependency direction

`bitacora-types` ← `bitacora-segment`, `bitacora-proto` ← `bitacora-replica`, `bitacora-meta` ← `bitacora-client`, `bitacora-controller` ← `bitacora-materializer` ← `bitacora-kv`. `bitacora-sim` depends on everything and nothing depends on it.

### Conventions

- **Licence.** Apache-2.0, for its patent grant and compatibility with the surrounding ecosystem.
- **I/O discipline.** No crate except thin adapter modules may touch the clock, the network, the disk or a random source directly. A lint in continuous integration enforces it.
- **Cancellation safety.** State-changing operations on a replica run in their own task to completion; request handlers await a result channel. A dropped request can never leave a half-applied change.
- **Unsafe code.** Forbidden by default; allowed per module with a written justification, expected only in the I/O layer.
- **Naming.** Descriptive names over abbreviations throughout, in code and in the protocol.
- **Decisions.** Anything that changes a format, a protocol step or an invariant gets a decision record and a model update before the code.

## 18. Roadmap

Seven milestones, each closed by a test that can fail. No dates are set; the order and the gates are the commitment.

| Milestone | Delivers | Exit gate |
| --- | --- | --- |
| M0 Specification | Protocol document for append, roll and failover; executable model; simulation harness skeleton with injected clock, network and disk | The model checks I1 to I4 with crashes and message loss and finds no violation |
| M1 Single-node log | Segment format, replica server, client writer and reader against one replica | Property tests pass: any crash point recovers to a valid prefix; fuzzers run clean |
| M2 Replicated log | Quorum append, durable sequence, roll on replica failure, writer failover, etcd metadata backend | Simulation runs the full fault matrix with no invariant violation; performance targets of section 16 measured |
| M3 Lifecycle | Controller, placement, repair, tiering to object store, truncation, `bita` CLI | A simulated cluster loses any one node at any time and returns to N good copies with no data loss |
| M4 Materializers | Core trait and runner, page adapter, artifact adapter, artifact store, bitacora-kv | bitacora-kv histories are linearizable under the fault matrix, including failover and truncation |
| M5 Hardening | Real-cluster fault tests, Helm chart and systemd units, metrics and dashboards, upgrade tests, first tagged release | A week of nightly real-cluster runs with no invariant violation; a rolling upgrade test passes |
| M6 Second materializer | A metrics-block materializer built by using only the public interface; C and Go clients | Whatever the second materializer could not do through the interface is fixed, then the interface is frozen for 1.0 |

### After 1.0

- The PostgreSQL integration starts as its own project (section 19).
- The embedded metadata store replaces the etcd dependency for small deployments.
- The shared per-disk journal, if stream counts require it.

### What to build first inside M0

1. The simulation harness and the injected I/O interfaces. Everything after depends on them.
2. The model of failover. It is the cheapest place to find the expensive bugs.
3. The history checker, tested against deliberately broken toy implementations so that it is known to catch violations.

## 19. Future materializers

The three target workloads are not designed here. This section records what each will demand from bitacora, so that version 1 does not close a door.

| Workload | A stream is | A record is | Materialized form | Shape |
| --- | --- | --- | --- | --- |
| PostgreSQL | One database cluster | One WAL record | Versioned 8 KiB pages | Pages |
| Prometheus metrics | One tenant shard | One batch of samples | TSDB blocks | Artifacts |
| Logs and search | One index shard | One batch of documents | Immutable index segments | Artifacts |

### PostgreSQL

A separate project with three parts: a patched PostgreSQL or extension that ships WAL to bitacora and reads pages from the page store; a page materializer that applies WAL by running PostgreSQL's own redo code in a sandboxed helper process, as Neon does; and replica support that keeps reader caches current from the stream.

What it needs from bitacora:

- **An engine position per record.** PostgreSQL addresses the log by byte position (LSN), not by record count. Records need an optional, monotonic 64-bit engine position that the log indexes, so that "read page as of LSN" maps to a sequence number.
- **Large records.** WAL records can exceed 4 MiB. The client must split and reassemble them transparently.
- **Non-page state.** Transaction status, relation sizes and similar data are not ordinary pages. The page id type must be general enough to model them as special pages.
- **Low-latency page reads.** Single-digit milliseconds at p99, which means a local SSD cache of layer files next to the page store.
- **The C client.**

### Prometheus metrics

The materializer consumes sample batches and cuts standard TSDB blocks, so that existing queriers can read them unchanged. Recent data is served from an in-memory head that followers build by tailing the stream. This is the architecture Mimir's ingest storage already uses with Kafka in the log's place.

What it needs from bitacora:

- **Many producers per stream.** Ingestion comes from many stateless front ends, but a stream has one writer. This needs an append gateway: a small service that owns the writer and accepts appends from many clients. It should be designed with M6.
- **Time-based sealing.** Blocks are cut by sample time, not by sequence range, and late samples arrive. `should_seal` must be free to decide on its own criteria, with the covered sequence range recorded only for truncation.
- **The Go client.**

### Logs and search

The materializer builds immutable index segments from document batches and merges them in the background, the model Lucene-based engines already follow. Followers hold the newest documents in memory for near-real-time search.

What it needs from bitacora:

- **Streaming uploads.** Index segments reach gigabytes. The artifact store needs a multipart streaming write next to `put_object`.
- **Deletes and updates.** Represented as tombstone records and applied at merge time, which the compaction hook must allow.
- **Compaction as a first-class operation.** Replacing several artifacts with one must be a single atomic catalog change.

### Common thread

All three need the catalog of artifacts to be readable by query nodes that are not the materializer. That catalog, its format and its change notifications are therefore part of the public interface from M4.

## 20. Risks and open questions

The largest risk is not technical: a design this broad stalls unless the log ships alone first. The table lists what could go wrong and what the plan does about it.

| Risk | Why it matters | Mitigation |
| --- | --- | --- |
| The `bita` command shares its name with an existing tool | Naming the project and crates `bitacora` avoids the crates.io collision with the existing [`bita`](https://docs.rs/crate/bita/0.14.0/source) crate, a differential file synchronization tool. That crate also installs a `bita` binary, so the two cannot both be on one machine's path under that name | Accepted: the two tools serve different users. If a distribution objects, packages install the command as `bitacora` with `bita` as an alias |
| A protocol bug loses committed data | It would end trust in the project | Model first, simulation from the first commit, real-cluster fault tests before any release |
| Scope creep toward PostgreSQL | It is the most attractive goal and by far the most expensive | Separate project, started only after 1.0 |
| The interface fits only bitacora-kv | An interface with one consumer is usually wrong | M6 builds a second materializer through the public interface before the freeze |
| The etcd dependency deters small users | An extra system to run | The trait isolates it; an embedded store follows 1.0 |
| One file and one flush per open segment | Thousands of active streams per node mean thousands of flushes | Measure at M2; a shared per-disk journal is the known answer |
| Object-storage-native designs make an SSD fleet look heavy | Systems that write straight to object storage need no replica nodes | Position bitacora on commit latency; keep the replica interface narrow enough that an object-store-backed replica is possible |
| A single maintainer | Review and continuity | Protocol document, decision records and reproducible seeds make the project legible to contributors early |

### Open questions

- [ ] TLA+ or Stateright for the protocol model.
- [ ] madsim or turmoil for simulation; decide with a one-week spike on each at M0.
- [ ] File per segment or shared journal for version 0.
- [ ] Whether the engine position index of section 19 belongs in the log or in the page adapter.
- [ ] Whether the append gateway is part of bitacora or of each materializer project.
- [ ] Whether segments are compressed on local disk or only when tiered.
- [ ] Stream branching: a new stream that shares a sealed prefix with its parent is cheap with immutable segments and gives database branching. Decide whether it is a 1.0 feature.
- [ ] Buffered writes with `fdatasync`, or direct I/O with io\_uring, as the default flush path.

## Sources

- [Amazon Aurora: Design Considerations for High Throughput Cloud-Native Relational Databases](https://pdos.csail.mit.edu/6.824/papers/aurora.pdf), SIGMOD 2017
- [The BookKeeper protocol](https://bookkeeper.apache.org/docs/development/protocol/), Apache BookKeeper documentation
- [Neon architecture overview](https://neon.com/docs/introduction/architecture-overview) and [Architecture decisions in Neon](https://neon.com/blog/architecture-decisions-in-neon)
- [About ingest storage architecture](https://grafana.com/docs/mimir/latest/get-started/about-grafana-mimir-architecture/about-ingest-storage-architecture/), Grafana Mimir documentation
- [madsim](https://docs.rs/crate/madsim/0.2.6) and [turmoil](https://github.com/tokio-rs/turmoil), deterministic simulation frameworks for Rust
- [Deterministic simulation testing for async Rust](https://s2.dev/blog/dst), S2
- [bita 0.14.0 on docs.rs](https://docs.rs/crate/bita/0.14.0/source), the existing crate whose name the command shares

The design itself (protocol steps, defaults, targets) is original to this document and has not been implemented or measured. Descriptions of Corfu, Delos and Aurora's PostgreSQL edition are from general knowledge of the published work, not from pages checked for this document.
