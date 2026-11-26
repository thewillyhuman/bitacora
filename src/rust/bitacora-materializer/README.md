# bitacora-materializer

The materializer framework: turns a prefix of a stream into state that can be
queried, and makes it safe to truncate the log behind it (design, section 11).

## What it owns

- The contract a workload implements (`Materializer`).
- The loop that runs a materializer: tailing, checkpointing, fencing
  (`MaterializerRunner`).
- A materializer's corner of the artifact store (`ArtifactNamespace`) and the
  deletion of what is no longer used in it (`sweep_orphans`).
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
6. It publishes how far it has applied and committed.

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
log order, each covering a range of sequence numbers. A checkpoint seals what
the builder holds into one more file, then merges the oldest run of
neighbours of similar size into one larger file, at most one run per
checkpoint. Merging reads the files, never the log, because the log below the
checkpoint may be gone. Files a merge replaced are dropped from the manifest
and deleted by the sweep after the commit.

The records not sealed yet are in the builder only. `ArtifactCatalog::builder`
gives the builder, so that a front end can take from it whatever it shares
with readers: a workload that serves its newest data from memory keeps that
data behind a shared handle in its builder.

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
- An artifact catalog covers every record from the first one applied to the
  last checkpoint, without gap or overlap, also across crashes and merges.

## Which file holds what

| File | Holds |
| --- | --- |
| `contract.rs` | `Materializer`, `CheckpointManifest`, `MaterializerError` |
| `namespace.rs` | `ArtifactNamespace` and the naming of artifacts |
| `manifest.rs` | The stored form of a manifest |
| `runner.rs` | `MaterializerRunner` and its progress reporting |
| `sweeper.rs` | `sweep_orphans` and the rule for what is garbage |
| `layer.rs` | The layer files of the page adapter |
| `page_store.rs` | `PageMaterializer`, `PageStore`, `PageReader` |
| `artifact_catalog.rs` | `ArtifactBuilder`, `ArtifactCatalog`, `CatalogView` |
| `test_harness.rs` | The in-process cluster the tests of this crate share |

Tests sit next to the file they test, in `<file>_test.rs`.

## Dependencies

May depend on `bitacora-types`, `bitacora-artifact`, `bitacora-meta` and
`bitacora-client`. Nothing below those depends on this crate. Tests also use
`bitacora-testkit` and `bitacora-controller`.
