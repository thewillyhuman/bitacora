# bitacora-artifact-s3

The S3 backend of the bitacora artifact store.

## What it owns

The implementation of `bitacora_artifact::ArtifactStore` that a real cluster
runs on: tiered log segments and materializer artifacts in a bucket of any
S3-compatible service (Amazon S3, Ceph, a gateway onto a file system).

It owns nothing else. What an artifact store is, the in-memory and directory
backends and the contract every backend must honour belong to
`bitacora-artifact`.

## Design

The store is a thin adapter over the `object_store` crate, in two parts.

`store.rs` translates between the two interfaces, and that is where the
thinking is, because they disagree where it matters to callers:

| | S3 and `object_store` | `ArtifactStore` | What the store does |
| --- | --- | --- | --- |
| A range that ends past the object | Returns the bytes there are | `RangeOutOfBounds` with the real size | One ranged `GET`; its answer carries the object's size, so a short answer is recognised at no extra cost |
| A range that starts past the object | Refuses, without the size | `RangeOutOfBounds` with the real size | After a failed `GET`, one `HEAD` to tell this case from an outage |
| An empty range | Cannot be asked for | Empty bytes if it lies inside the object | One `HEAD` |
| A listing prefix | Whole path components | Characters | Lists the directory the prefix names and keeps the keys that start with the prefix |
| Deleting a missing object | Depends on the service | Succeeds | Succeeds |
| A missing bucket | `404`, like a missing object | An outage | Reads the error code in the answer and reports `Unavailable` |
| An object handed over in pieces | A multipart upload: parts of at least 5 MiB except the last, at most 10,000 of them | Pieces of any size | Gathers the pieces into parts of 8 MiB and sends each when it is full; an object below one part is one plain `PUT` |

An upload keeps what it was handed since the last full part, which is less
than 8 MiB once a piece has been taken, and sends one part at a time. That
is the whole of its memory, whatever the size of the object: a segment of
1 GiB is 128 parts. The object is in the bucket when the service has
completed the multipart upload and not before. An upload that lost a part
refuses to go on, so that an object with a hole in it is never completed.

The service keeps the parts of a multipart upload until it is completed or
aborted. The store therefore aborts every upload that ends any other way
than by a completion: one that is aborted, one whose completion fails, and
one that is dropped, which is what happens when the request it serves is
cancelled. A drop cannot wait, so that abort runs in a task of its own.

One thing only the service can do. A multipart upload whose process died,
or whose abort did not reach the service, leaves its parts in the bucket:
no listing shows them, and they are billed.
**Give the bucket a lifecycle rule that aborts incomplete multipart uploads
after a day or more** (`AbortIncompleteMultipartUpload` with
`DaysAfterInitiation`); `docs/operations/README.md` has the rule.

`s3.rs` builds the S3 client from `S3Settings`: timeouts, bounded retries
(two, for server errors and broken connections), one plain `DELETE` per
object, TLS from rustls with ring. Opening a store sends nothing; a wrong
bucket or wrong credentials show on the first request.

Credentials are never in the settings. `S3ArtifactStore::open` takes them
from the environment like every AWS tool: access keys, a web identity token
(Kubernetes service accounts), or the container and instance metadata
services. `open_with_credentials` is for tests and for a program that holds
a key pair itself; that type hides its secret when printed.

Why a crate and not a module of `bitacora-artifact`: the S3 client brings an
HTTP client and a TLS stack, about 125 locked packages. The simulator, the
test kit and everything else that uses the in-memory store would compile all
of it for nothing. See `docs/decisions/0005-s3-backend.md`.

## Invariants

- The store behaves like every other artifact store. It passes
  `bitacora_artifact::contract::check` over an in-memory service and against
  a real S3 server.
- No operation outlives `S3Settings::request_timeout`. For an upload, each
  part and the completion are one operation each.
- An upload holds less than one part of 8 MiB between two pieces, and every
  part but the last has exactly that size.
- An upload that could not send a part is never completed.
- An upload that sent a part and does not end in a completion asks the
  service to drop its parts, once.
- A setting always wins over an environment variable about the same thing.
- A missing bucket is never reported as a missing object (one documented
  exception: an empty range, which is answered from a `HEAD`).
- No secret is part of a printable value.
- An object in the bucket has exactly the name the caller gave it, under the
  configured key prefix. Keys are not encoded.

## Which file holds what

| File | Holds |
| --- | --- |
| `src/lib.rs` | The public names and the overview |
| `src/settings.rs` | `S3Settings`, `S3Credentials`, `InvalidS3Settings` and the validation |
| `src/s3.rs` | `S3ArtifactStore::open` and `open_with_credentials`: the S3 client and its configuration |
| `src/store.rs` | `S3ArtifactStore` and the translation of each operation |
| `src/*_test.rs` | Unit tests, without a network: the store runs over `object_store`'s in-memory store |
| `tests/s3/` | The same store against a real S3 server |

## Tests

The unit tests need nothing. The tests in `tests/s3/` start the
[Versity S3 Gateway](https://github.com/versity/versitygw) on a loopback
port that no other test is given (`bitacora-testports`). Bazel fetches a
pinned release and passes its path in `BITA_S3_SERVER_BINARY`; nothing is
downloaded while a test runs. Under Cargo, set the variable to the path of a
`versitygw` program; without it those tests pass without running and say so
once.

## Dependencies

May depend on `bitacora-artifact` and on third-party crates; its tests also
use `bitacora-testports`. Nothing in the workspace may depend on it except
the binaries that open an S3 store.

```text
bitacora-artifact  <-  bitacora-artifact-s3  <-  binaries
```

## Not covered

- Parts sent side by side. An upload sends one part at a time, which keeps
  its memory at one part and costs throughput on a fast link.
- An object given to `put_object` is still one `PUT`: it is held in memory by
  its caller and cannot exceed S3's limit of 5 GiB for a single request.
  Large objects go through `start_upload`.
- Removing the parts of multipart uploads that were never finished. That is
  the bucket's lifecycle rule, see above.
- Server-side encryption settings and object versioning. A bucket's own
  default encryption applies as usual.
- Listing by characters costs a listing of the whole directory the prefix
  names. Prefixes that end at a `/` are exact.
