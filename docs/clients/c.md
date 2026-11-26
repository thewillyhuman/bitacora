# The C client

bitacora's client library is written in Rust. The C client is that same
library behind functions a C program can call: one header,
[`src/c/include/bitacora.h`](../../src/c/include/bitacora.h), and one
library, `libbitacora_c`, built from `src/rust/bitacora-c`. It is how a
database that is not written in Rust, PostgreSQL first of all, uses the log.

There is no second implementation of the write protocol in C. What a call
does is what the Rust client does; this page is about what is particular to
calling it from C. The header documents every function, structure and status
and is the reference. Why the interface looks the way it does is in
[decision 0014](../decisions/0014-c-abi.md).

## Build the library

The library comes in two forms with the same contents: a static archive,
`libbitacora_c.a`, and a shared library, `libbitacora_c.so` on Linux and
`libbitacora_c.dylib` on macOS.

With Bazel, which is what the project is built and tested with:

```sh
bazel build --config=release \
    //src/rust/bitacora-c:bitacora_c_static \
    //src/rust/bitacora-c:bitacora_c_shared
ls bazel-bin/src/rust/bitacora-c/libbitacora_c.*
```

With Cargo, which needs `protoc` on the `PATH`:

```sh
cd src/rust
cargo build --release -p bitacora-c
ls target/release/libbitacora_c.*
```

The header is checked in. Reading it, or compiling against it, needs no Rust
tooling.

## Link a program

**Statically.** Name the archive, then the system libraries the archive
needs. An archive cannot name them itself.

```sh
# Linux
cc -std=c11 -I src/c/include program.c libbitacora_c.a \
    -lgcc_s -lutil -lrt -lpthread -lm -ldl -lc -o program

# macOS
cc -std=c11 -I src/c/include program.c libbitacora_c.a \
    -framework Security -framework CoreFoundation -liconv -lSystem -lc -lm -o program
```

Those lists are what the compiler reports for this version. Ask it again
after an upgrade:

```sh
cd src/rust
cargo rustc -p bitacora-c --lib --crate-type staticlib -- --print native-static-libs
```

The archive holds the Rust standard library and every dependency, so it is
large; the linker takes from it only what the program uses. A program that
already links another Rust static library should use the shared library
instead: two archives that each contain the Rust standard library can clash.

**Dynamically.** Name the library; it records what it needs itself.

```sh
cc -std=c11 -I src/c/include program.c -L /path/to/the/library -lbitacora_c -o program
```

The loader has to find the library when the program starts: install it in a
directory the loader searches, or set an rpath. A program that loads the
library at run time should compare `bitacora_abi_version()` with
`BITACORA_ABI_VERSION` before any other call, to learn whether it got the
library it was compiled for.

**From Bazel.** Depend on `//src/c:bitacora` for the static library or on
`//src/c:bitacora_shared` for the shared one, and include `"bitacora.h"`.

The header is plain C11, compiles without warnings under
`-Wall -Wextra -pedantic`, and can be included from C++.

## A complete program

This is [`src/c/examples/quickstart.c`](../../src/c/examples/quickstart.c).
It connects, creates a stream, appends three records as the stream's writer
and reads them back.

```c
/*
 * quickstart.c - the smallest complete program that uses bitacora from C.
 *
 * It connects to a cluster, creates a stream, becomes its writer, appends
 * three records, and reads them back.
 *
 *     quickstart <cluster configuration file> [stream name]
 *
 * It exits with 0 if all of that worked, with 1 if a step failed, after
 * saying which one and why, and with 2 if it was called the wrong way.
 */

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "bitacora.h"

/* The longest an append may take before the program stops waiting for it. */
#define APPEND_TIMEOUT_MILLISECONDS 5000

/* Says which step failed and why. The status decides; the message is for people. */
static void report_failure(const char *step, bitacora_status status)
{
    fprintf(stderr, "%s failed with status %d: %s\n", step, (int)status,
            bitacora_last_error_message());
}

int main(int argument_count, char **arguments)
{
    static const char *const messages[] = {"first", "second", "third"};
    const size_t message_count = sizeof(messages) / sizeof(messages[0]);
    const char *stream_name = "quickstart";
    bitacora_client *client = NULL;
    bitacora_writer *writer = NULL;
    bitacora_reader *reader = NULL;
    bitacora_records *records = NULL;
    bitacora_status status;
    uint64_t first_appended = 0;
    uint64_t last_appended = 0;
    int exit_code = 1;
    size_t index;

    if (argument_count < 2 || argument_count > 3) {
        fprintf(stderr, "usage: %s <cluster configuration file> [stream name]\n", arguments[0]);
        return 2;
    }
    if (argument_count == 3) {
        stream_name = arguments[2];
    }

    /* One client per process is enough. It starts its own threads and sends nothing yet. */
    status = bitacora_client_connect_file(arguments[1], NULL, &client);
    if (status != BITACORA_STATUS_OK) {
        report_failure("connecting", status);
        goto cleanup;
    }

    /* A stream is created once. A second run finds it there, which is fine. */
    status = bitacora_stream_create(client, stream_name, NULL);
    if (status != BITACORA_STATUS_OK && status != BITACORA_STATUS_ALREADY_EXISTS) {
        report_failure("creating the stream", status);
        goto cleanup;
    }

    /*
     * A stream has one writer at a time. Opening one waits for the lease of the
     * writer before it to lapse; a writer that was closed has given its lease up.
     */
    status = bitacora_writer_open(client, stream_name, BITACORA_TAKEOVER_WAIT_FOR_LEASE_EXPIRY,
                                  &writer);
    if (status != BITACORA_STATUS_OK) {
        report_failure("opening the writer", status);
        goto cleanup;
    }

    /* An append returns once the record is committed, with its sequence number. */
    for (index = 0; index < message_count; index++) {
        const char *message = messages[index];

        status = bitacora_writer_append(writer, (const uint8_t *)message, strlen(message),
                                        APPEND_TIMEOUT_MILLISECONDS, &last_appended);
        if (status != BITACORA_STATUS_OK) {
            /*
             * FENCED: another writer took the stream, and this one must stop.
             * INDETERMINATE or TIMED_OUT: the record may or may not be in the
             * log. Open a new writer and read the log to find out.
             */
            report_failure("appending", status);
            goto cleanup;
        }
        if (index == 0) {
            first_appended = last_appended;
        }
        printf("appended \"%s\" as record %" PRIu64 "\n", message, last_appended);
    }

    /* Closing seals what was written and gives the stream up for the next writer. */
    status = bitacora_writer_close(writer);
    if (status != BITACORA_STATUS_OK) {
        report_failure("closing the writer", status);
        goto cleanup;
    }

    /* Readers need no writer and do not disturb one. */
    status = bitacora_reader_open(client, stream_name, &reader);
    if (status != BITACORA_STATUS_OK) {
        report_failure("opening the reader", status);
        goto cleanup;
    }
    status = bitacora_reader_read_range(reader, first_appended, last_appended, &records, NULL);
    if (status != BITACORA_STATUS_OK) {
        report_failure("reading", status);
        goto cleanup;
    }
    for (index = 0; index < bitacora_records_count(records); index++) {
        uint64_t sequence = 0;
        const uint8_t *payload = NULL;
        size_t payload_length = 0;

        status = bitacora_records_get(records, index, &sequence, &payload, &payload_length);
        if (status != BITACORA_STATUS_OK) {
            report_failure("looking at a record", status);
            goto cleanup;
        }
        /* The payload is bytes, not a string: it has a length and no terminator. */
        printf("record %" PRIu64 ": %.*s\n", sequence, (int)payload_length, (const char *)payload);
    }
    exit_code = 0;

cleanup:
    /* Every handle is freed exactly once. Freeing a null pointer does nothing. */
    bitacora_records_free(records);
    bitacora_reader_free(reader);
    bitacora_writer_free(writer);
    bitacora_client_free(client);
    return exit_code;
}
```

Run it with the configuration file of a cluster, the same file the daemons
and `bita` read:

```sh
quickstart /etc/bitacora/cluster.toml
```

```text
appended "first" as record 1
appended "second" as record 2
appended "third" as record 3
record 1: first
record 2: second
record 3: third
```

The program above, and its output, are held to this page by a test: it
compiles the file, runs it against a cluster of real processes, and fails if
the page shows anything else.

## How calls behave

**Every call blocks.** A call returns when it has an answer. There are no
callbacks, no futures and no event loop to integrate with. Calls that wait
for the cluster to do something take a timeout in milliseconds;
`BITACORA_WAIT_WITHOUT_LIMIT` waits for as long as it takes.

**The work is done on the client's threads.** `bitacora_client_connect_file`
and `bitacora_client_connect_text` start a small pool of threads, two unless
`worker_threads` says otherwise. A call hands its work to them and waits.
The calling thread checks arguments, waits and copies results; it never runs
the protocol. Heartbeats, lease renewals and the sending of records go on
between calls.

**The library owns what it returns; it copies what it is given.** A handle
belongs to the caller until the caller passes it to its `_free` function,
exactly once. Text and bytes passed in are copied before the call returns.
A payload pointer from `bitacora_records_get` points into the records and is
valid until `bitacora_records_free`.

**Structures carry their size.** `bitacora_client_options`,
`bitacora_stream_options` and `bitacora_stream_description` begin with
`struct_size`. Set it to `sizeof` of the structure and zero the rest:

```c
bitacora_stream_options options;

memset(&options, 0, sizeof(options));
options.struct_size = (uint32_t)sizeof(options);
options.replica_count = 5;
options.write_quorum = 3;
```

That field is what lets a later version of the library add fields while
programs compiled against this header keep working.

## Errors

Every function that can fail returns a `bitacora_status`. Decide on the
status. `bitacora_last_error_message()` returns the explanation of the last
failure on the calling thread, for a log; it is valid until that thread's
next call.

| Status | Meaning | What to do |
| --- | --- | --- |
| `BITACORA_STATUS_OK` | The call did what it was asked. | |
| `BITACORA_STATUS_INVALID_ARGUMENT` | A null pointer where one is required, text that is not UTF-8, a range or a configuration that can never work, a record that is too large. Nothing was done. | Fix the program. |
| `BITACORA_STATUS_NOT_FOUND` | The stream or the materializer does not exist. | |
| `BITACORA_STATUS_ALREADY_EXISTS` | A stream has this name already. | Often fine: creating is done once. |
| `BITACORA_STATUS_UNAVAILABLE` | The cluster could not do it now. Never returned by an append. | Try again later. |
| `BITACORA_STATUS_TIMED_OUT` | The wait the caller allowed has passed. For an append the record may still commit. | Treat an append like `INDETERMINATE`. |
| `BITACORA_STATUS_FENCED` | Another writer has taken the stream. | Stop writing. Free the writer. |
| `BITACORA_STATUS_INDETERMINATE` | The append may or may not have committed. | Open a new writer and read the log. |
| `BITACORA_STATUS_TRUNCATED` | The records asked for were deleted. The call reports the low-water mark. | Resume from the mark, or rebuild from a snapshot. |
| `BITACORA_STATUS_NOTHING_NEW` | No record arrived within the wait. Not an error. | Call again. |
| `BITACORA_STATUS_CLOSED` | The writer was closed, or the follower has ended. | Free the handle. |
| `BITACORA_STATUS_LEASE_HELD` | A writer that is alive holds the stream, and the caller chose to wait. | Try again later, or take over by force. |
| `BITACORA_STATUS_INTERNAL` | A defect in bitacora. | Free the handle; report the message. |

The numbers are stable. New statuses are added at the end, so treat a number
the program does not know as a failure.

Three of them carry the design's promises and deserve the attention of
whoever writes the integration:

- **Fenced** is how a writer learns that it is no longer the writer. It is
  final for that handle. The record whose append returned it may or may not
  be in the log.
- **Indeterminate**, and an append that **timed out**, mean the library does
  not know. It never guesses. The log decides: a new writer recovers the
  stream, and what is then read back is what was committed.
- **Truncated** is never reported as an empty result. A reader that asks for
  deleted records is told so, with the first sequence number it can still
  read, in `low_water_mark_out`.

A panic inside the library is caught at the boundary and returned as
`BITACORA_STATUS_INTERNAL`. It does not unwind into C.

## Threads

| Handle | From several threads at once |
| --- | --- |
| `bitacora_client` | Every function. |
| `bitacora_writer` | `append`, `append_batch`, `durable_sequence`. `close` and `free` need the writer to themselves; `close` is refused while appends are in flight. |
| `bitacora_reader` | Every function. |
| `bitacora_follower` | One call at a time; calls from several threads take turns. |
| `bitacora_records` | The accessors. `free` needs the records to itself. |

Freeing any handle needs that handle to itself. Handles opened from a client
keep working after the client handle is freed; the threads stop when the
last handle is gone. The last error message belongs to the calling thread.

## Processes, fork and signals

This section is for programs like a PostgreSQL backend: one process per
connection, created by `fork`, driven by signals.

**Do not use a client across `fork`.** The child gets a copy of the handles
and none of the threads behind them. A call on an inherited handle, freeing
included, can wait forever for threads that do not exist. So:

- Connect in the process that will use the client, after it was forked. In
  PostgreSQL that is the backend or the auxiliary process that writes, never
  the postmaster.
- A child that inherited handles must forget them: no calls, no `_free`.
- `fork` followed at once by `exec` is unaffected.

**The library installs no signal handlers and changes no signal masks.** Its
threads are all started by the thread that connects, or by a thread
descended from it, and so they start with the signal mask the connecting
thread has while it connects. No later call starts a thread from the calling
thread. A program whose handlers must run on its own thread therefore blocks
its signals around the connect, and only there:

```c
sigset_t all_signals;
sigset_t previous_mask;

sigfillset(&all_signals);
pthread_sigmask(SIG_BLOCK, &all_signals, &previous_mask);
status = bitacora_client_connect_file(configuration_path, NULL, &client);
pthread_sigmask(SIG_SETMASK, &previous_mask, NULL);
```

Without that, a signal sent to the process may be delivered to one of the
library's threads, and the handler runs there.

**A signal does not interrupt a call.** A call returns when it has an answer
or when its timeout has passed, never early with something like `EINTR`. A
program that has to react to signals gives its calls a timeout and looks at
its flags between them. A PostgreSQL backend calls `CHECK_FOR_INTERRUPTS()`
there.

**A signal handler must not call the library, and must not jump out of a
call.** No function here is async-signal-safe. A handler that leaves with
`siglongjmp` while its thread is inside a call skips the cleanup of that
call. Handlers that only set a flag, as PostgreSQL's do, are fine.

**Memory.** The library allocates with the system allocator, not with the
caller's. In PostgreSQL terms: nothing it returns lives in a memory context,
so nothing is released at the end of a transaction or on `ereport(ERROR)`.
Free handles in a callback that runs on error as well, such as a resource
owner or a `PG_TRY` block.

## What is not there

- **No asynchronous or callback interface.** Calls block. To wait on several
  things, use threads or timeouts.
- **No append without a copy.** A payload is copied once on its way in.
  Reads lend the library's memory and copy nothing.
- **No TLS and no authentication** between the client and the replicas, as
  in the rest of bitacora today. The cluster belongs on a private network.
- **No configuration structure.** A cluster is described by the TOML file
  every bitacora program reads, passed as a path or as text. Timeouts and
  limits of the client other than the number of threads and the writer's
  name are the library's defaults.
- **No materializer framework.** A stream's materializers can be registered
  and unregistered; writing one in C is not supported. Follow the stream and
  keep the checkpoint yourself, or write the materializer in Rust.
- **No listing of streams, no checkpoints, no metrics.** Use `bita` and the
  daemons' metrics for those.
- **No Windows.** The library is built and tested on Linux and macOS.

## Compatibility

`BITACORA_ABI_VERSION` changes only when a program compiled against the old
header could misbehave with the new library. Adding functions, adding
statuses at the end and adding fields at the end of a structure with
`struct_size` do not change it.

The header is generated from the Rust source by `hack/update_c_header.sh`
and checked in. A test fails when the two differ, so the header in the
repository always describes the library built from the same commit.
