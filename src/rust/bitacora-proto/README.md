# bitacora-proto

The wire protocol of bitacora.

## What it owns

- The Rust code generated from the protocol definitions in
  [`src/proto`](../../proto), which are the language-neutral source of truth
  of the protocol.
- The conversions between the generated messages and the domain types of
  `bitacora-types`.

It is the only crate of the workspace that depends on `tonic` and `prost`.

## Design

The protocol carries `ReplicaApi` across machines without changing what any
operation means. Three rules make that true, and
[decision 0002](../../../docs/decisions/0002-grpc-transport.md) gives the
reasons for each:

- **Identity.** Every request names the replica it is meant for in the
  `bitacora-replica-id` header. A server refuses a request that is not for
  it, before the request reaches the replica.
- **A refusal is an answer.** A `ReplicaError` travels inside the response
  message, variant by variant and field by field.
- **Anything else is unavailability.** A gRPC status other than OK, or an
  answer that cannot be read, becomes `ReplicaError::Unavailable`.

## Invariants

- A domain value converted to its message and back is the same value.
- No conversion panics on input from the network. What cannot be converted
  is reported as a `MalformedMessage` that names the field at fault.

## Which file holds what

| File | Holds |
| --- | --- |
| `build.rs` | Generates the Rust code from the protocol definitions, at build time and without `protoc`. |
| `src/lib.rs` | The generated module `v1`, the public names, and the two constants of the protocol. |
| `src/conversions.rs` | Values shared by several messages, and `MalformedMessage`. |
| `src/error_conversions.rs` | `ReplicaError` to and from its message. |
| `src/request_conversions.rs` | The request of every call, in both directions. |
| `src/*_test.rs` | The unit tests of the file of the same name. |

## Dependencies

It depends on `bitacora-types` and on nothing else in the workspace. The
domain crates never depend on it.
