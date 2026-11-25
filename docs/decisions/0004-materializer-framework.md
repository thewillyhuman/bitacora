# 0004. How the materializer framework fills in what the design leaves open

Status: accepted · 2026-11-25

## Context

The design gives the materializer framework its contract, the six steps of
the runner and the two adapters (section 11), and the reference materializer
bitacora-kv (section 12). It leaves open what a checkpoint manifest holds,
how artifacts get "unique names", which orphan files a sweeper may delete,
what layer files look like, how compaction rebuilds artifacts, and how
bitacora-kv reads are made linearizable. This record states the choices made
in `src/rust/bitacora-materializer`, `src/rust/bitacora-kv` and
`src/rust/bitacora-sim`, one section per choice.

## A materializer writes through a namespace, not the artifact store

The design passes `&dyn ArtifactStore` to `write_checkpoint` and
`restore_from_checkpoint`. The implementation passes `&ArtifactNamespace`:
a view that can add an artifact under a fresh name and read artifacts, and
cannot overwrite, delete or list.

Why. "Artifacts are written under unique names before the commit" is what
makes a crash between writing and committing harmless, and what keeps a
replaced instance from overwriting its successor's files. With the raw store
that is a rule every materializer must remember. With the namespace it is a
property of the interface.

An artifact key is `<fencing token, 20 digits>/<number, 10 digits>-<label>`
under the prefix `materializers/<stream id>/<materializer>/`. The fencing
token is raised in the metadata store at every takeover, so no two runners
share one; the number counts what one runner wrote. Names are unique without
a clock or a random source, which simulation needs.

## The manifest is an artifact; the checkpoint record points at it

`CheckpointManifest` has two fields: `artifact_keys`, every artifact the
checkpoint needs, and `details`, bytes only the materializer understands.
The runner stores it as one more artifact (`manifest.rs` gives the layout:
magic, version, checkpoint sequence, keys, details, CRC-32C) and commits its
key in the checkpoint record's `manifest` field.

Why. A manifest can name thousands of artifacts and the metadata store is
for small records. The framework needs to know which artifacts are in use,
so that part is structured; the rest is the materializer's business. The
checkpoint sequence is repeated in the manifest so that a pointer leading to
the manifest of another checkpoint is noticed at restore.

## What the sweeper may delete

An artifact may be deleted if the committed manifest does not name it and it
was written with a fencing token lower than the current one.

Why that is safe. Only the holder of the current token C can commit. What it
commits names artifacts of the manifest it restored from, which was
committed, and artifacts it wrote, which carry C. An artifact with a lower
token that the committed manifest does not name can therefore never be named
again. A commit or a takeover while the sweep runs does not change that.

Artifacts with token C that no manifest names may belong to the checkpoint
being written. An outside sweep (`sweep_orphans`) never touches them. The
runner deletes them itself right after each of its own commits, before it
writes anything new.

This needs one rule from materializers: a manifest names only artifacts that
the previous committed manifest named or that this instance wrote since. An
artifact dropped from a manifest is gone for good.

If the committed manifest cannot be read, the sweeper deletes nothing.
Objects whose key the namespace did not produce are left alone.

Not done: the controller does not run the sweep yet. A runner cleans up
after every commit, and `sweep_orphans` can be called by an operator or a
test; orphans of a materializer that never runs again stay until someone
calls it.

## The runner does not register the materializer

`MaterializerRunner::start` fails if the materializer is not registered on
the stream. Registering is an explicit administrative step.

Why. Registration is what pins the log. A runner that registered itself
would, after an operator unregistered a materializer and the log moved on,
come back with an empty state and no record of what it missed.

## The runner keeps applying while a checkpoint cannot be made

If the artifact store or the metadata store cannot be reached during a
checkpoint, the runner logs it, waits `retry_interval`, and tries again. It
keeps applying records meanwhile. The checkpoint that finally commits covers
everything applied by then.

Why. Reads of the materialized state should not stall because a checkpoint
is late. The cost is that the artifacts of a failed attempt are orphans
until the next successful commit.

## One addition to the core contract

`Materializer::wants_checkpoint` (default `false`) lets an instance ask for
a checkpoint before the runner's byte and time thresholds are reached. The
artifact adapter uses it for `ArtifactBuilder::should_seal`.
