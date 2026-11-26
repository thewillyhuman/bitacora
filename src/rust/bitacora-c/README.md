# bitacora-c

The C interface of the bitacora client: the Rust client behind functions a C program can call.

How to build against it and how to use it is in [`docs/clients/c.md`](../../../docs/clients/c.md).
This file is for whoever changes the crate.

## What it owns

- The exported functions, `bitacora_*`, and the types C sees: five opaque handles, three structures
  that carry their own size, one batch entry, and `bitacora_status`.
- The mapping from every error of the Rust client to a status (`src/status.rs`).
- The boundary every call passes through: argument checks, panic guard, last error message.
- The generator of `src/c/include/bitacora.h` and the test that holds the checked-in header to it.
- The test that runs the C programs of `src/c` against a cluster of real processes.

The C side lives in `src/c`: the header, the example, the C tests.

## Design

**One implementation of the protocol.** The crate adds no behaviour. A function checks its arguments,
hands one call of the Rust client to the client's threads, waits, and translates the outcome.

**Blocking calls on an owned runtime.** Each client owns a Tokio runtime. The work of a call is
spawned there; the calling thread only waits. No thread is ever started from the caller's thread after
the connect, which is what lets a host control the signal mask of every library thread.

**Statuses are for decisions, messages for logs.** A status says what the caller should do. The text
that explains it is kept per thread. A truncation hands the low-water mark back as a number.

**The header is generated.** The doc comments on exported items become the header, so they are written
for a C reader and contain no Rust. Rust examples live in each file's header comment.

The reasons are in [decision 0014](../../../docs/decisions/0014-c-abi.md).

## Invariants

- No panic unwinds into C: every entry point runs inside `run_guarded` or `run_guarded_value`.
- No pointer from C is dereferenced before it was checked for null.
- Every case of `AppendError`, `OpenError`, `ReadError`, `ConnectError` and `MetaError` has a status.
  `src/status.rs` denies wildcard arms, so a new case does not compile until it is given one.
- A status keeps its number. New ones go at the end.
- An output is validated before the call does its work and written after.
- The work of a call owns what it uses and holds no share of the client's core, so the runtime is
  never dropped from one of its own threads.
- `src/c/include/bitacora.h` is exactly what the source generates.

## Changing the interface

1. Change the Rust source. Write the doc comment for a C programmer.
2. Run `hack/update_c_header.sh` and commit the header with the change.
3. Add the function to `src/c/tests/errors_test.c`, and to `src/c/tests/end_to_end.c` if it talks to
   the cluster.
4. Decide whether `BITACORA_ABI_VERSION` changes: only if a program compiled against the old header
   could misbehave. Adding a function, a status at the end, or a field at the end of a sized structure
   does not change it.

A new source file that exports something must be added to `INTERFACE_SOURCE_FILES` in
`tests/header_test.rs`, or the header will not contain it.

## Files

| File | Holds |
| --- | --- |
| `src/lib.rs` | The public surface |
| `src/status.rs` | `BitacoraStatus`, `Failure`, the mapping of the client's errors |
| `src/guard.rs` | The panic guard around every entry point |
| `src/last_error.rs` | The per-thread message and `bitacora_last_error_message` |
| `src/arguments.rs` | Every way a pointer from C becomes something safe; sized structures |
| `src/client.rs` | Connecting; the runtime; how calls wait |
| `src/writer.rs` | The writer handle: open, append, batch, durable sequence, close |
| `src/reader.rs` | The reader and follower handles: read a range, follow, low-water mark |
| `src/records.rs` | The records a read returns and their accessors |
| `src/admin.rs` | Streams: create, describe, truncate, delete, materializers |
| `src/version.rs` | The release and the interface version |
| `src/test_support.rs` | What the unit tests share: a client of a cluster that is not there |
| `cbindgen.toml` | How the header is generated: names, order, the comment at its top |
| `tests/header_test.rs` | The checked-in header against the generated one |
| `tests/c_programs_test.rs` | The C programs against a real cluster; the documented example |

Each source file has its tests next to it in `<file>_test.rs`.

## Tests

| What | Where | Needs |
| --- | --- | --- |
| Status mapping, argument checks, sized structures, guard | `src/*_test.rs` | Nothing |
| The examples in the documentation of each file | doc tests | Nothing |
| Header in step with the source | `tests/header_test.rs` | Nothing |
| A C program gets statuses, not crashes | `src/c/tests/errors_test.c` | A C compiler |
| The life of a stream, in C | `src/c/tests/end_to_end.c` | A C compiler; etcd and the daemons, which Bazel provides |

Under Cargo the tests that need compiled C programs pass without running and say so; Bazel runs
everything.

## Dependencies

It depends on `bitacora` for the client, and names `bitacora-meta` and `bitacora-types` for the types
of the client's interface that the facade does not export. `tokio` provides the runtime and `toml`
reads the cluster configuration. `cbindgen` is a test dependency only: nothing that ships contains it.
