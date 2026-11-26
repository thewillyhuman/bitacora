# 0002. The replica protocol is unary gRPC, with errors in the response

Status: accepted · 2026-11-25

## Context

The design asks for gRPC with Protocol Buffers between a replica and
everyone who talks to it, through `tonic` (section 13, "Wire protocol"). It
names three services and sketches two streaming calls: a bidirectional
`AppendStream` and a server-streaming `ReadStream`. It also asks that the
protocol definition, not the Rust code, be the source of truth for the C and
Go clients to come.

Since then the implementation settled on `ReplicaApi`
(`bitacora-types/src/replica_api.rs`): nine request and response operations
that a replica implements and that writers, readers and the controller call.
The model, the simulator and every test exercise the protocol through that
interface. The transport has to carry it across machines without changing
what any operation means.

## Decision

The protocol is `src/proto/bitacora/v1/replica.proto`, package `bitacora.v1`.
The crate `src/rust/bitacora-proto` holds the code generated from it, the
conversions to and from the domain types, a client (`GrpcReplicaConnector`)
and a server (`serve_replica`).

### One unary call per operation

Each method of `ReplicaApi` is one unary call, grouped into the three
services the design names:

| Service | Calls |
| --- | --- |
| `ReplicaData` | `Append`, `Read`, `Fence`, `Seal` |
| `ReplicaTransfer` | `RepairSegment`, `UploadSegment` |
| `ReplicaAdmin` | `Status`, `Drain`, `ListSegments`, `DeleteSegment` |

This deviates from the design in two ways.

There is no `AppendStream` and no `ReadStream`. A stream would add a second
set of rules on top of the ones the model checks: what an acknowledgement on
a stream covers, what a broken stream says about the requests in flight.
With unary calls the transport adds nothing: a request has one answer, and a
retry is the same request again, which every operation already tolerates.
HTTP/2 multiplexes the calls of a writer over one connection, so pipelining
appends does not need a stream either. The design allows for this:
`AppendStream` "can move to a custom framed protocol without changing
semantics". If profiling shows the per-call overhead on the append path
matters, a streaming call can be added next to `Append` in the same package.

`ReplicaTransfer` has `RepairSegment` instead of `FetchSegment`. Repair
became one order to the replica that lacks records; that replica pulls them
from a peer with `Read`. `UploadSegment` is the design's: the controller
asks a replica to upload its copy of a sealed segment to the object store.

### Batch frames are opaque bytes

Records travel as `bytes`, holding batch frames exactly as they are stored
in a segment file. The transport never looks inside. A replica verifies the
checksum of every frame it receives and a reader that of every frame it is
served, so the checksum covers the whole path from the writer's memory to
the reader's. In Rust the field is a reference-counted buffer: a frame is
never copied out of the message it arrived in.

### A refusal is an answer; anything else is unavailability

Every response has an `outcome` that is either `success` or `error`, and
`error` is a `ReplicaError` message with one sub-message per variant. A call
that reached the replica always ends with the gRPC status OK.

The alternative is the gRPC status with the error in its details. It was
ruled out for three reasons. Callers act on the exact variant and its
fields: a writer told `Fenced` stops for good, one told `OutOfOrder` sends
again from the position in the error. Status details are a convention on
top of a trailer, read differently by each gRPC library, and a client in C
or Go would have to follow our reading of it. Second, a proxy between the
two may replace a status with its own, but passes a response body through.
Third, it keeps two things apart that mean different things: the replica
said no, and nobody said anything.

So a client reads a gRPC status other than OK, whatever its code, as
`ReplicaError::Unavailable`: connection refused or lost, a timeout, a
cancellation. It reads an answer it cannot interpret the same way, such as
an error of a kind added by a newer server. `Unavailable` is the one error
that promises nothing about whether the request was applied, which is the
truth in all of these cases.

A server answers a request it cannot convert, one with a required field
missing for example, with `ReplicaError::InvalidRequest`, without calling
the replica.

### Identity travels in a header

Every request carries the identity of the replica it is meant for in the
`bitacora-replica-id` metadata header. A server compares it with its own
identity before anything else and refuses a missing or different one with
the status `FAILED_PRECONDITION`, which the client reads as unavailable.

This is what `ReplicaConnector` requires (commit "types: connect to a
replica by identity"): a node whose disk was replaced returns at the same
address under a new identity and holds none of the copies the old one held.
If it answered in the old one's name, a recovering writer would take its
empty copies for real ones. A header, rather than a field in each request,
makes the check one piece of code in front of every call, including the
calls added later.

### The package is the version

The protocol version is the package name, `bitacora.v1`. An incompatible
change gets a new package, served next to the old one while clients move,
which is how a server supports the current and the previous version
(section 13, "Versioning"). There is no negotiation call.

### Messages are at most 16 MiB

gRPC accepts 4 MiB by default, exactly the largest record payload and so
too small for the frame around it. Both sides send and accept 16 MiB: a
batch frame is at most 8 MiB plus its header. A read's answer exceeds its
size cap by less than one batch, so the server lowers the cap to 4 MiB to
keep every answer under the limit.

### The code is generated by a build script, without protoc

`build.rs` compiles the protocol file with `protox`, a Protocol Buffers
compiler written in Rust, and generates the messages and stubs with
`tonic-prost-build`. Cargo runs the script as usual. Bazel runs the same
script through `cargo_build_script`, with the protocol files as its inputs,
so both builds generate the same code from the same file, and neither needs
a `protoc` binary on the machine or a toolchain to provide one.

The generated code is not checked in. TLS and compression are left out of
the `tonic` features until they are used.

## Consequences

- The transport adds no state and no rules, so what the model and the
  simulator establish about `ReplicaApi` holds over gRPC. The transport's
  own tests cover what it does add: conversions, identity, sizes and the
  mapping of failures.
- gRPC metrics and logs show a refused append as a successful call. How
  often a replica refuses, and why, has to come from the replica's own
  metrics.
- Adding a variant to `ReplicaError` means adding a message and a kind to
  the protocol file. The conversions do not compile until both exist. An
  older client reads the new kind as unavailable and keeps retrying, so a
  new variant must reach the clients before the servers send it.
- A request larger than 16 MiB fails as unavailable and would be retried
  for ever. The writer's request size, 4 MiB plus one batch by default
  (record 0017), must stay under the limit.
- `ListSegments` answers in one message, which holds a few hundred thousand
  segment copies. A replica that holds more needs the call to be paged.
- The clean build grows by the gRPC stack, about 110 crates. Building them
  took a minute and a half under Bazel on a laptop; they are cached
  afterwards.
- A C or Go client is generated from the same file and follows the rules
  written at its top.
