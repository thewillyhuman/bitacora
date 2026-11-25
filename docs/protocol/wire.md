# The wire protocol

A replica is reached over gRPC. The services, messages and every field are
defined and documented in one file, which is the source of truth:

```text
src/proto/bitacora/v1/replica.proto
```

Generate a client from that file. This page only summarizes what it says.

## Services

| Service | Calls | Used by |
| --- | --- | --- |
| `ReplicaData` | `Append`, `Read`, `Fence`, `Seal` | Writers, readers |
| `ReplicaTransfer` | `RepairSegment`, `UploadSegment` | The controller |
| `ReplicaAdmin` | `Status`, `Drain`, `ListSegments`, `DeleteSegment` | The controller, operators |

Every call is unary. What each one must do is in
[replication.md](replication.md).

## Rules for every call

- **Identity.** A request carries the identity of the replica it is meant
  for in the metadata header `bitacora-replica-id`. A replica refuses a
  request whose header is missing or names another replica, with the gRPC
  status `FAILED_PRECONDITION`. A node whose disk was replaced comes back at
  the same address under a new identity; it must never answer for the old
  one, whose copies it no longer holds.
- **Outcomes.** A replica that received a request answers with the status
  `OK` and a response whose `outcome` is `success` or `error`. A refusal is
  an answer, so it travels in the response as a `ReplicaError`.
- **Transport failures.** Any other status, and any response that cannot be
  interpreted, means that no answer arrived: the replica may or may not have
  applied the request. Treat both as "unavailable".
- **Retries.** Every call is safe to send again.
- **Batch frames** travel as opaque bytes, exactly as they are stored
  ([disk-format.md](disk-format.md)).
- **Sizes.** A message is at most 16 MiB. A record payload is at most 4 MiB.
- **Addresses** are `host:port`.

## The metadata store

Clients talk to the metadata store directly, with its own client library.
Version 1 uses etcd; the keys and records are in [metadata.md](metadata.md).

## Not in version 1

- TLS and authentication.
- Streaming calls. The design mentions a bidirectional `AppendStream`; unary
  calls over one HTTP/2 connection are used instead (decision record
  `docs/decisions/0002-grpc-transport.md`).
