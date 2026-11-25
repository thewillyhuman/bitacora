# The bitacora protocol

These documents are the source of truth for everything two bitacora programs
have to agree on: the bytes on a replica's disk, the objects in the object
store, the records in the metadata store, the messages on the wire, and the
rules a writer, a reader and a replica follow. A client in another language
is written from these pages, not from the Rust code. Where the Rust code and
a page disagree, that is a bug in one of them.

| Document | What it defines |
| --- | --- |
| [replication.md](replication.md) | The rules: appending, committing, rolling, taking over, reading |
| [disk-format.md](disk-format.md) | A replica's data directory: segment files and their state |
| [tiered-object.md](tiered-object.md) | A sealed segment in the object store |
| [metadata.md](metadata.md) | The keys and records of the metadata store |
| [wire.md](wire.md) | The gRPC services a replica offers |

## Conventions

- All integers in binary formats are unsigned and little-endian.
- Checksums are CRC-32C (Castagnoli).
- A **sequence number** identifies a record within its stream. The first
  record of a stream is 1. The value 0 means "before the first record" and
  never names a record.
- An **epoch** identifies one writer's tenure of a stream. Epochs only grow.
- A **segment** is a contiguous run of a stream's records, written by one
  epoch and stored on a fixed set of replicas. Segment identifiers start at 1
  within a stream and are never reused.
- A **stream identifier** is assigned when a stream is created and is never
  reused, not even after the stream is deleted and its name taken again.

## Versions

Every format carries its own version, so that each can change alone:

| Format | Where the version is | Current |
| --- | --- | --- |
| Segment file | Header | 1 |
| Segment state file | Header | 1 |
| Tiered object | Footer | 1 |
| Metadata records | `schema_version` field | 1 |
| Wire protocol | Protobuf package (`bitacora.v1`) | 1 |

A program refuses a version newer than the ones it knows. It never guesses.
