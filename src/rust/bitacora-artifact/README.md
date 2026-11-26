# bitacora-artifact

The artifact store: immutable objects addressed by key.

## What it owns

- The interface every object store is used through: `ArtifactStore`, with its error type
  `ArtifactError`, the listing entry `ObjectInfo`, `ObjectUpload` for an object that is handed over
  piece by piece, and `validate_key`, the one rule for what a key is.
- Two backends that need no network: `MemoryArtifactStore` for tests and the simulator, and
  `LocalArtifactStore` for a development cluster on one machine.
- `contract::check`: what any backend must do, as a function a backend runs against itself in its tests.

It does not own what is stored. Keys and contents are opaque: the layout of a tiered segment belongs to
`bitacora-segment`, the layout of a checkpoint to the materializer that writes it.

## Design

Tiered log segments and materializer artifacts are written once and never changed, so the interface has
five operations on whole objects: put, get, get a byte range, delete, list by key prefix. There are no
directories, no appends and no renames, because an object store has none, and code written against more
would not run on one.

A sixth operation, `start_upload`, stores an object that its writer does not hold: a sealed segment is a
gibibyte, and a replica that built it in memory to call `put_object` needed twice that. The writer hands
the upload its pieces in order, of any size, and then either completes it or aborts it. The object is
under its key from the completion on and at no moment before, so an upload that fails, is aborted or is
simply dropped stores nothing, and a reader never meets half an object. That is the same promise
`put_object` makes, kept over many calls instead of one; what a backend may keep in memory meanwhile is
bounded by its own part size and not by the object.

| Backend | Where the pieces go | Completion | Abort | Left by a process that dies |
| --- | --- | --- | --- | --- |
| Memory | A buffer of the upload: this backend holds whole objects anyway | The buffer becomes the object | The buffer is dropped | Nothing |
| Local directory | A scratch file in `scratch/`, written as the pieces come | Sync, then rename onto the object | The scratch file is removed | A scratch file, removed by a store that is opened once it is a day old |
| S3 (`bitacora-artifact-s3`) | Parts of 8 MiB of a multipart upload | The service makes the parts the object | The service drops the parts | Parts at the service, for a lifecycle rule of the bucket |

What an upload took may be a gigabyte, so every end but a completion removes it, not only an abort: a
completion that fails does what an abort does before it returns its error, and an upload that is dropped,
as happens when the request it serves is cancelled, is aborted in the background, because a drop cannot
wait. Only an abort says whether the removal worked.

The same code must run in the simulator, on a laptop and in production. Prose in a trait's documentation
does not keep three implementations alike; one executable contract does. Each backend has a test that
hands an empty store to `contract::check`, and a difference between backends is a bug in one of them.

A backend that needs a network client lives in a crate of its own, so that nothing which only needs the
interface or the in-memory store compiles an HTTP stack.

The in-memory store is also where faults are injected: an outage, a store that never answers, a flipped
bit in a stored object. Tests of everything above this crate use those switches instead of writing their
own test doubles.

## Invariants

- A reader sees either the old object or the new one under a key, never a mixture and never a prefix.
- An upload that is not completed stores nothing under its key, and changes nothing that is there.
- Every operation is safe to repeat. After `ArtifactError::Unavailable` it may or may not have happened.
- A key that `validate_key` accepts cannot name anything outside the store, in any backend.
- A listing is sorted by key and matches its prefix character by character.
- Every backend passes `contract::check`.

## Files

| File | Holds |
| --- | --- |
| `src/lib.rs` | The crate's rationale and its public names |
| `src/store.rs` | The interface, the errors and the rule for keys |
| `src/contract.rs` | The check every backend runs against itself |
| `src/memory_store.rs` | The in-memory backend and its fault switches |
| `src/local_store.rs` | The backend that keeps objects in a local directory |
| `src/<file>_test.rs` | The tests of the file of the same name |

## Dependencies

It depends on no other bitacora crate, and must stay that way: the segment, replica, client, controller
and materializer crates all depend on it. Backends for real object stores depend on it, never the other
way round.
