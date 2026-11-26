# bitacora-materializer

The materializer framework: turns a prefix of a stream into state that can be
queried, and makes it safe to truncate the log behind it (design, section 11).

## What it owns

- The contract a workload implements (`Materializer`).
- The loop that runs a materializer: tailing, checkpointing, fencing
  (`MaterializerRunner`).
- A materializer's corner of the artifact store (`ArtifactNamespace`) and the
  deletion of what is no longer used in it (`sweep_orphans`).
- The way a process that does not run the materializer reads what it last
  committed (`read_committed_checkpoint`).
- The two adapters most workloads use instead of the bare contract:
  `PageStore` for engines that update pages and log redo records, and
  `ArtifactCatalog` for append-only data that compacts into files.

It does not own the log, the metadata records or the artifact store itself;
it uses them through `bitacora-client`, `bitacora-meta` and
`bitacora-artifact`.

## Design

A materializer is a deterministic function from the records 1 to N to state.
The runner owns the instance and is the only thing that calls it:

1. It raises the materializer's fencing token in the metadata store and reads
   the checkpoint that comes with it.
2. It restores the instance from the manifest the checkpoint points at and
   follows the stream from the next record.
3. It applies committed records in order.
4. When enough bytes or time have passed, the instance writes a checkpoint:
   new artifacts, and a manifest that names every artifact in use.
5. The runner commits the checkpoint with one conditional write that is valid
   only while the token is still its own.
6. It publishes how far it has applied and committed and, when it stops,
   why (`RunnerProgress`). A front end waits on that handle and needs no
   channel of its own into the runner's task.

Three decisions shape the rest:

- **A materializer never sees the raw artifact store.** It writes through an
  `ArtifactNamespace`, which can add an artifact under a fresh name and read
  artifacts, and nothing else. Names start with the writer's fencing token,
  so no two instances can produce the same name and a replaced instance
  cannot overwrite its successor's files.
- **The manifest is an artifact; the metadata store holds a pointer.** A
  manifest may name thousands of artifacts; the checkpoint record stays a
  few bytes.
- **Garbage is defined by the manifest.** An artifact that the committed
  manifest does not name, written with a token lower than the current one,
  can never be named again and is deleted. Artifacts written with the
  current token may belong to a checkpoint about to be committed; only the
  runner that holds the token deletes those, right after its own commit.

### Versioned pages

`PageStore` implements the contract for an engine that only says which pages
a record touches and how a record changes one page (`PageMaterializer`). It
keeps the history of the pages as immutable layers, oldest first:

```text
delta 1..40 | delta 41..90 | image at 90 | delta 91..130 | records in memory
```

A delta layer holds the records that changed pages over a sequence range; an
image layer holds every page in full as of one sequence. A checkpoint flushes
the records in memory into a delta layer and, every few delta layers, writes
an image. A read as of sequence N takes the page from the newest image at or
below N and applies the page's later records up to N. Layers older than the
oldest image kept are dropped; the sequence of that image is the horizon,
below which pages can no longer be read.

### Immutable artifacts

`ArtifactCatalog` implements the contract for a workload that only says how
records become a file (`ArtifactBuilder`). The catalog is the list of files in
log order, each covering a range of sequence numbers. The builder decides
when a file is cut and after which record. A checkpoint seals the files that
are due. What the builder holds beyond them must be durable too, because the
log may be truncated up to the checkpoint: a builder that can save its
pending records has them saved, in an artifact the manifest names next to
the catalog, and the next instance reads them back; a builder that cannot
has them sealed into one more file, however small. The checkpoint then
merges the oldest run of neighbours of similar size into one larger file, at
most one run per checkpoint. Merging reads the files, never the log, because
the log below the checkpoint may be gone. Files a merge replaced are dropped
from the manifest and deleted by the sweep after the commit.

A catalog entry also carries a summary of its file, written by the builder
and opaque to the framework: what a reader should know before it fetches
anything, such as the span of time the file covers.

The records not sealed yet are in the builder only. `ArtifactCatalog::builder`
gives the builder, so that a front end can take from it whatever it shares
with readers: a workload that serves its newest data from memory keeps that
data behind a shared handle in its builder.

### Readers that are not the runner

The artifacts are plain objects, and a query node or a tool may read them
without running the materializer. Such a process opens the namespace with
`ArtifactNamespace::read_only` and calls `read_committed_checkpoint`, which
reads the checkpoint record and the manifest it names. The runner may commit
a newer checkpoint between the two reads and delete the manifest; the
function then reads the record again. For a catalog, `decode_catalog` turns
the manifest's details into the list of files. The stored forms are
specified in `docs/protocol/checkpoint-manifest.md`.

A reader holds the catalog of the moment it looked. A file it names can be
merged away afterwards; a reader that finds one gone reads the committed
checkpoint again.

## Invariants

- A checkpoint is committed only after its artifacts and manifest are stored.
- The committed checkpoint never passes the last record applied and never
  moves backwards.
- Only the holder of the current fencing token commits. A runner that learns
  it was replaced stops and cannot commit again.
- Records are applied exactly once per run, in order, without gaps.
- An artifact key is never written twice.
- The sweeper never deletes an artifact that the committed manifest names or
  that its writer may still commit.
- A page read at any sequence from the horizon to the last record applied
  sees exactly the records up to it, whatever was flushed or dropped since.
- What `read_committed_checkpoint` returns was committed, and reading it
  writes nothing.
- An artifact catalog, with the saved pending records after it if there are
  any, covers every record from the first one applied to the last
  checkpoint, without gap or overlap, also across crashes and merges.
- A builder that cuts at boundaries it derives from the records, and saves
  its pending records, cuts the same artifacts whenever checkpoints happen.

## Which file holds what

| File | Holds |
| --- | --- |
| `contract.rs` | `Materializer`, `CheckpointManifest`, `MaterializerError` |
| `namespace.rs` | `ArtifactNamespace` and the naming of artifacts |
| `manifest.rs` | The stored form of a manifest |
| `runner.rs` | `MaterializerRunner`: taking over, the loop, checkpoints and commits; `RunnerError` |
| `runner/settings.rs` | `RunnerDependencies` and `RunnerSettings`, with the default pacing |
| `runner/progress.rs` | `RunnerProgress`, `Progress`, `RunnerStopped`, and the runner's end of them |
| `committed.rs` | `read_committed_checkpoint`, for a process that is not the runner |
| `sweeper.rs` | `sweep_orphans` and the rule for what is garbage |
| `layer.rs` | The layer files of the page adapter |
| `page_store.rs` | `PageMaterializer`, `PageStore`, `PageReader`, `LayeringPolicy` |
| `page_store/state.rs` | The layer list, the read plan, the layering rules and the stored details |
| `page_store/shared.rs` | What the store and its readers share: the lock and every read of a layer |
| `artifact_catalog.rs` | `ArtifactBuilder`, `ArtifactCatalog`, `CatalogView` |
| `artifact_catalog/compaction.rs` | `CompactionPolicy` and the rule that picks what is merged |
| `artifact_catalog/stored.rs` | `StoredCatalog`, `decode_catalog`: the catalog as a manifest holds it |
| `test_harness.rs` | The in-process cluster the tests of this crate share |

Tests sit next to the file they test, in `<file>_test.rs`. `page_store/shared.rs` has none of its own: it is all
fetching and locking, and is tested through the store in `page_store_test.rs`.

## Dependencies

May depend on `bitacora-types`, `bitacora-artifact`, `bitacora-meta` and
`bitacora-client`. Nothing below those depends on this crate. Tests also use
`bitacora-testkit` and `bitacora-controller`.
