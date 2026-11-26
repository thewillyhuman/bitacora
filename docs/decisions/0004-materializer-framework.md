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

## Layer files of the page adapter

One layout serves delta and image layers: a fixed-size header, an index
from page identifier to blob, then the blobs (`layer.rs` gives the bytes).
Header, index and every blob carry their own CRC-32C. A blob is the page
itself in an image layer, and the page's records in a delta layer.

Why. A page read wants one page out of a layer that may hold thousands. With
the header first and of fixed size, a reader fetches the header, then the
index, then exactly one blob, and can check each on its own. The page store
keeps indexes it has read in memory, so later reads cost one range request
per layer.

Choices kept simple on purpose:

- **An image layer holds every page**, not only the pages that changed. Then
  one rule collects garbage: keep the `images_kept` newest images and drop
  every layer older than the oldest of them. Its sequence is the horizon;
  reads below it fail with an explicit error. The price is rewriting cold
  pages at every image. An engine with a large, mostly cold page space would
  split images by page range as Neon does; the layer format already allows
  an image that holds only some pages.
- **An image is written after a fixed number of delta layers** (four by
  default), at a checkpoint. There is no background compaction.
- **A record's effect on a page is computed when the page is read or imaged,
  not when the record is applied.** Applying only appends to in-memory
  chains, because `apply_records` is synchronous and the page before the
  record may be in the artifact store. `apply_record` should fail only for
  records its engine can never have written.
- **`pages_touched` may refuse a record.** The design has it return the pages
  alone. Here it returns a result, so that a record the engine cannot even
  read, for example one written by a newer version, stops the runner when it
  is applied instead of being skipped or found out at some later read.
- **`PageMaterializer` must be `Default` and encode its page identifiers.**
  The contract restores a materializer with a static function, so the logic
  cannot be handed in; it is created. Anything that shapes pages must be a
  constant of the type, which is also what keeps it the same in every
  process that reads the artifacts.

Not done: compression of layers, a cache of page images in the page store
(the compute node caches hot pages), and read-only followers that tail the
log without checkpointing.

## Compaction merges artifacts; it does not rebuild from the log

`ArtifactBuilder` has a fourth method next to the three of the design:
`merge_artifacts(inputs, artifacts)`, which writes one artifact holding what
several neighbouring artifacts hold together. `ArtifactCatalog` calls it for
the oldest run of `merge_fan_in` neighbours in the same size tier, at most
once per checkpoint.

Why a merge and not "ask the builder to rebuild a range". A rebuild would
read the log again, and the log below the committed checkpoint may already
be truncated: the artifacts are then the only copy of those records.

Why size tiers. A checkpoint must persist everything applied, so it seals
whatever the builder holds, however little. Frequent checkpoints therefore
make small artifacts. (Since decision 0015 a builder can save what it holds
at a checkpoint without sealing it, and then cuts its artifacts where it
chooses. This paragraph describes a builder that does not.) Merging
neighbours of similar size keeps the number of artifacts logarithmic in the
data and rewrites each record a bounded number of times. Only neighbours
are merged, so the catalog stays in log order.

Why one merge per checkpoint. It bounds what a checkpoint does. A backlog of
merges is worked off over the following checkpoints, also those that have
nothing new to seal.

The catalog never deletes. A merged-away artifact is dropped from the next
manifest and the sweep after that manifest's commit deletes it, so a query
that still reads the old artifact loses it only after the catalog that
replaces it is durable.

## bitacora-kv reads are made linearizable with a barrier record

`KvStore::get` appends a record without operations, the barrier, and waits
for it to commit at a sequence S. It then waits until the materializer has
applied S and reads the key's bucket as of S.

Why S is safe under writer failover. A write that finished before the read
began is in the log below S: if this store committed it, it got a lower
sequence number; if a newer writer committed it, that writer had fenced a
recovery quorum and sealed this store's segment first, and the barrier,
appended later, cannot reach a write quorum of unfenced replicas. So a
barrier that commits proves the store was still the writer while the read
was under way. This is the commit rule of the log (invariants I1 and I4)
used as a leadership check; nothing new has to be trusted.

What a read does when it cannot. If the barrier does not commit, `get`
returns `NotCurrent` and no value. If the materializer does not reach S in
`read_wait`, it returns an error too. It never answers from pages it cannot
vouch for. `get_as_of` and `scan` read at a sequence the caller names, are
not linearizable, and need no barrier.

Cost and alternative. Every linearizable read adds one small record to the
log. The alternative is a round of heartbeats that confirms the writer's
epoch on a write quorum without appending, as a read index does in Raft. It
needs support in the writer that the client library does not have yet.

Writes. One call appends one record and the store never retries, so a write
is applied at most once. A write that returns `OutcomeUnknown` may or may not
be in the log; a caller that repeats it makes a second write. The store does
not deduplicate and has no compare-and-set: both are left out.

The record format, the bucket page format, the bucket count (64) and the
hash (CRC-32C of the key) are constants of the crate, not settings. Changing
any of them for an existing store would silently lose keys.

## Linearizability is checked by an in-tree search, per key

The simulator's compute nodes are bitacora-kv front ends. Every call and its
result is recorded, and `check_linearizability` judges the record: for each
key it searches for an order of the calls in which a call that finished
before another began comes first and every read returns the value of the
last write before it. The search is that of Wing and Gong with Lowe's
memory of visited states.

Why in-tree. The model is one register per key, the search is about two
hundred lines, and the repository already has the rule that tests add no
dependency that is not deterministic. Porcupine and Knossos are the known
tools; neither is a Rust library that could run inside `bazel test`.

Why per key. Keys are independent, so their histories can be judged apart.
That keeps each search to a few hundred calls.

How writes of unknown outcome are treated. A put or delete whose caller was
told "unknown", or whose client crashed, may take effect at any moment after
it began, including long after its caller gave up, or never. The search may
place it anywhere after its start or leave it out. An unknown put whose
value no read ever returned is dropped before the search: no read depends
on it, so leaving it out of an order never breaks the order. This is sound
because every put of a simulation writes a value of its own, and it is what
keeps the search fast. Unknown deletes are kept, and are tried last.

The search has a budget of states. Running out of it is reported as a
violation of its own kind, never as a pass.

How the two checkers share one stream. A put is also recorded as an append,
with the payload the store will write, so the log checker (I1 to I4) still
sees every record that carries data. Barriers and deletes have the same
payload every time; they are declared to the history as fillers, which the
log checker does not attribute to an append but still holds to "readers
agree" and "readers see a gapless prefix".

How the checker is shown to work. Besides unit tests on hand-written
histories, two deliberate mistakes can be switched on in the simulated
compute nodes (`InjectedBug`): reads that skip the barrier, and writes of
unknown outcome reported as done. The fault matrix requires each to be
caught within a few seeds.

Not simulated yet: outages of the artifact store, and a read replica that
tails the stream without writing.

## One addition to the core contract

`Materializer::wants_checkpoint` (default `false`) lets an instance ask for
a checkpoint before the runner's byte and time thresholds are reached. The
artifact adapter uses it for `ArtifactBuilder::should_seal`.
