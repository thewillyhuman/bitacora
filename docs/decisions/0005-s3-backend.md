# 0005. The S3 backend is a crate of its own, tested against a real S3 server

Status: accepted · 2026-11-25

## Context

Tiered log segments and materializer artifacts live in an object store, behind
the `bitacora_artifact::ArtifactStore` interface (section 11). The interface
has an in-memory backend, which every test and the simulator use, and a
directory backend for development. What was missing is the backend a real
cluster runs on: "any S3-compatible store through the `object_store` crate".

The design lists the backends inside `bitacora-artifact` (section 17).

## Decision

The backend is the crate `src/rust/bitacora-artifact-s3`. It implements
`ArtifactStore` over `object_store` and contains nothing else.

Why a crate and not a module of `bitacora-artifact`. The S3 client brings an
HTTP client and a TLS stack: 124 locked packages, of which about 78 are built
on any one platform. The replica, the controller, the client library, the
test kit and the simulator all depend on `bitacora-artifact`, and in a test
none of them talks to S3. As a module, all of them would compile that stack to
use an in-memory map; as a crate, only the binaries that open an S3 store do.
This is the same deviation from the crate list of section 17 as the etcd
backend (record 0003), for the same reason. The dependency points one way:
`bitacora-artifact-s3` depends on `bitacora-artifact`.

### How a call maps to S3

The store is an adapter over `Arc<dyn object_store::ObjectStore>`, so that
its logic can be tested against `object_store`'s in-memory store, plus a
constructor that builds the S3 client from settings.

- **Keys** are used as they are, under a configured key prefix that is
  removed again from listings. `object_store` can percent-encode keys; the
  store does not let it, so an object has exactly the name the caller chose.
  The price is that keys with ASCII control characters are refused as
  invalid, which S3 advises against anyway.
- **`put_object`** is one `PUT`. S3 replaces an object atomically.
- **`get_range`** has to report a range outside the object as
  `RangeOutOfBounds` with the object's real size, and S3 does not help: it
  answers a range that ends past the object with the bytes there are, and
  refuses one that starts past it (`416`) without saying how large the
  object is. The store sends one ranged `GET`. Its answer carries the size of
  the whole object in `Content-Range`, so a short answer is recognised and
  reported with no second request. Only when the `GET` fails does the store
  send a `HEAD`, to tell a range that starts past the end from an outage. An
  empty range cannot be asked for over HTTP and is answered from a `HEAD`
  alone. The normal case, which is every read a tiered-segment reader makes,
  costs exactly one request.
- **`list_prefix`** lists by characters, as the interface says, while
  `object_store` lists whole path components. The store lists the directory
  the prefix names and keeps the keys that start with the prefix, then sorts.
  A prefix that ends at a `/` is exact; one that ends inside a name costs a
  listing of that name's whole directory.
- **`delete_object`** is one plain `DELETE` and succeeds for an object that
  is not there. S3's bulk deletion is switched off: some S3-compatible
  services lack it, and the store deletes one object at a time.
- **A missing bucket** answers `404` exactly like a missing object. Taking it
  for a missing object would be dangerous: a reader would conclude that a
  tiered segment is gone when only the configuration is wrong. The store
  looks for the `NoSuchBucket` error code in the answer and reports an outage
  with the reason. The check is on the text of the client's error, because
  `object_store` exposes nothing more structured; the code is part of the S3
  protocol, and a test against a real server pins it. One corner remains: a
  `HEAD` has no body, so an empty range read in a missing bucket says
  `NotFound`.
- Everything else, a timeout included, is `Unavailable` with what was
  attempted, on what, and what the client reported.

### Credentials

`S3Settings` has no credential fields, so a configuration can be printed or
logged without care. `S3ArtifactStore::open` takes credentials from the
environment through `AmazonS3Builder::from_env`: `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`, a web identity token (what a Kubernetes service
account provides), or the container and instance metadata services.
Everything the settings name is applied after the environment and wins over
it. `open_with_credentials` takes a key pair from the caller, for tests and
for a program that holds its keys itself; the type hides the secret when it
is printed.

### Timeouts and retries

Every HTTP request may take `request_timeout` (five minutes by default, as an
upload of a whole segment is one request) and opening a connection
`connect_timeout` (five seconds). On top, the store bounds each operation as
a whole with `request_timeout`, so nothing outlives it whatever the client
does. Callers that expect a quick answer keep their own shorter deadline.

`object_store` retries a request that fails with a server error or a broken
connection. Its default is ten retries over three minutes; the store sets two
retries and no retry after `request_timeout`. That absorbs the hiccups every
object store has. An outage is not hidden: the layers above (the
controller's passes, a reader's retry loop) already decide what to do about
one.

### TLS and the choice of ring

`object_store`'s `aws` feature brings aws-lc-rs for request signing and for
TLS: a large C library with a slow build. The crate asks for the same client
on ring instead (`aws-base`, `reqwest` and `ring` features, and reqwest with
`rustls-no-provider`). ring still compiles some C and assembly, so a C
compiler is needed, but nothing else; it builds under Bazel on macOS and
Linux with what those machines have. rustls wants its provider installed once
per process, so opening a store installs ring unless the program has
installed one already. Certificates are verified against the platform's
trusted roots.

## How the tests get a real S3 server

The adapter is thin, so what can be wrong with it is an assumption about how
an S3 service answers. Two layers of tests:

- **Unit tests** run the store over `object_store`'s in-memory store, which
  has S3's semantics for ranges and listings: the shared contract, key
  prefixes, every way a range can lie outside an object, and a client that
  fails or never answers (under paused time). 73 tests, no network.
- **`tests/s3/`** runs the store against a real server: the contract again,
  ranges at the edges of a 5 MiB object, keys that need care in a URL, a
  missing bucket on every operation, wrong credentials, and a server that is
  down, stops, or never answers. 30 tests in about a second.

The server is the Versity S3 Gateway 1.11.0 with its POSIX backend: one
program, a download of about 33 MB, that serves a directory over the S3
protocol, with request signing, and whose buckets are the directories below
its root. It keeps object metadata in a second directory (`--sidecar`), so
the tests do not depend on extended attributes in the file system. Bazel
downloads the release for the platform (macOS arm64, Linux x86-64, Linux
arm64), pinned by the checksums of the release's `checksums.txt`, and passes
its path in `BITA_S3_SERVER_BINARY`. The test starts it on a free loopback
port with its data in the test's temporary directory and kills it when the
last test using it ends. Nothing is downloaded while a test runs.

MinIO would have been the obvious server. It no longer publishes binaries:
its download address answers `410 Gone`.

Under Cargo the variable is set by hand. When it is not, those tests pass
without running and say so once on standard error; under Bazel a missing
program is a failure.

## Not covered

- **Uploads in several parts.** An object is one `PUT`: it is held in memory
  and limited to 5 GiB. The design lists a streaming upload next to
  `put_object` as future work (section 19, "Logs and search").
- **Server-side encryption settings** (SSE-KMS, SSE-C) and **object
  versioning**. A bucket's default encryption applies as usual.
- **Amazon S3 itself.** The tests run against one S3-compatible server.
  Virtual-hosted addressing, the web identity and metadata credential
  providers and TLS to a real endpoint are `object_store`'s own, tested
  there, and not exercised here.
- **Listing cost.** `list_prefix` with a prefix that is a whole key, which
  the controller uses to confirm an upload, lists the segment directory of
  the stream. That is one request per thousand segments; if it ever matters,
  the interface needs a way to ask for one object's size.
