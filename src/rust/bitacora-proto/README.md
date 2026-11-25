# bitacora-proto

The wire protocol of bitacora and its gRPC transport.

## What it owns

- The Rust code generated from the protocol definitions in
  [`src/proto`](../../proto), which are the language-neutral source of truth
  of the protocol.
- The conversions between the generated messages and the domain types of
  `bitacora-types`.
- The two ends of the transport: `GrpcReplicaConnector`, which turns a
  replica's registration into a `dyn ReplicaApi` that talks gRPC, and
  `serve_replica`, which serves any `dyn ReplicaApi` on a listening socket.

It is the only crate of the workspace that opens a socket or depends on
`tonic` and `prost`.

## Design

The transport adds nothing to the protocol. A replica behind a socket
answers exactly what it answers in process, so everything the model and the
simulator establish about `ReplicaApi` holds over the network. Three rules
make that true, and [decision 0002](../../../docs/decisions/0002-grpc-transport.md)
gives the reasons for each:

- **Identity.** Every request names the replica it is meant for in the
  `bitacora-replica-id` header. A server refuses a request that is not for
  it, before the request reaches the replica.
- **A refusal is an answer.** A `ReplicaError` travels inside the response
  message, variant by variant and field by field.
- **Anything else is unavailability.** A gRPC status other than OK, or an
  answer that cannot be read, becomes `ReplicaError::Unavailable`.

## Invariants

- A request that does not name a server's replica never reaches it.
- A domain value converted to its message and back is the same value.
- No conversion panics on input from the network. A malformed request is
  answered with `ReplicaError::InvalidRequest`; a malformed answer is read
  as `ReplicaError::Unavailable`.
- A call through `GrpcReplicaConnector` fails only with the replica's own
  error or with `ReplicaError::Unavailable`.

## Which file holds what

| File | Holds |
| --- | --- |
| `build.rs` | Generates the Rust code from the protocol definitions, at build time and without `protoc`. |
| `src/lib.rs` | The generated module `v1`, the public names, and the two constants of the protocol. |
| `src/conversions.rs` | Values shared by several messages, and `MalformedMessage`. |
| `src/error_conversions.rs` | `ReplicaError` to and from its message. |
| `src/request_conversions.rs` | The request of every call, in both directions. |
| `src/response_conversions.rs` | The answer of every call, in both directions. |
| `src/client.rs` | `GrpcReplicaConnector` and the handle it returns. |
| `src/server.rs` | `serve_replica` and the identity check. |
| `src/*_test.rs` | The unit tests of the file of the same name. |
| `tests/replica_transport_test.rs` | A real replica served on loopback and driven through the connector. |
| `tests/cluster_transport_test.rs` | A writer, a reader and a takeover on three replicas served on loopback. |

## Dependencies

It depends on `bitacora-types` and on nothing else in the workspace. The
binaries depend on it; the domain crates never do. Its tests also use
`bitacora-segment`, `bitacora-replica`, `bitacora-meta` and
`bitacora-client`, to put real replicas and a real client on both ends.
