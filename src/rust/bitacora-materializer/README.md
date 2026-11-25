# bitacora-materializer

The materializer framework: turns a prefix of a stream into state that can be
queried, and makes it safe to truncate the log behind it (design, section 11).

## What it owns

- The contract a workload implements (`Materializer`).
- The loop that runs a materializer: tailing, checkpointing, fencing
  (`MaterializerRunner`).
- A materializer's corner of the artifact store (`ArtifactNamespace`) and the
  deletion of what is no longer used in it (`sweep_orphans`).

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

## Which file holds what

| File | Holds |
| --- | --- |
| `contract.rs` | `Materializer`, `CheckpointManifest`, `MaterializerError` |
| `namespace.rs` | `ArtifactNamespace` and the naming of artifacts |
| `manifest.rs` | The stored form of a manifest |
| `runner.rs` | `MaterializerRunner` and its progress reporting |
| `sweeper.rs` | `sweep_orphans` and the rule for what is garbage |
| `test_harness.rs` | The in-process cluster the tests of this crate share |

Tests sit next to the file they test, in `<file>_test.rs`.

## Dependencies

May depend on `bitacora-types`, `bitacora-artifact`, `bitacora-meta` and
`bitacora-client`. Nothing below those depends on this crate. Tests also use
`bitacora-testkit` and `bitacora-controller`.
