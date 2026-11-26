# 0003. The etcd backend is a crate of its own, tested against a real etcd

Status: accepted · 2026-11-25

## Context

The metadata store is the one place where bitacora needs consensus, and
version 0 uses etcd behind a trait (section 8). The trait already exists:
`bitacora_meta::KeyValueStore`, with linearizable reads and conditional
transactions, and an in-memory implementation that every test and the
simulator use. What was missing is the implementation a real cluster runs on.

The design lists the etcd backend inside `bitacora-meta` (section 17).

## Decision

The backend is the crate `src/rust/bitacora-meta-etcd`. It implements
`KeyValueStore` with the `etcd-client` crate and contains nothing else: every
record, epoch and lease stays in `bitacora-meta`, so both backends behave the
same by construction.

Why a crate and not a module of `bitacora-meta`. The etcd client brings a
gRPC stack: about a hundred crates. The simulator, the test kit, the
controller and the client library all depend on `bitacora-meta` and none of
them talks to etcd. As a module, all of them would compile the gRPC stack to
use an in-memory map; as a crate, only the binaries that connect to etcd do.
This is the one deviation from the crate list of section 17. The dependency
points one way: `bitacora-meta-etcd` depends on `bitacora-meta`.

How a call maps to etcd:

- `get` is a linearizable range over one key, and the revision it returns is
  the key's mod revision. etcd raises its revision on every write and never
  reuses one, so a key that is deleted and created again gets a higher one.
- `list` is one linearizable range over the prefix, sorted by key. One
  request is one consistent view.
- `transact` is one etcd transaction. "The key is absent" compares the create
  revision with zero; "the key has this revision" compares the mod revision.
- Every key gets a configured prefix on the way in and loses it on the way
  out, so several clusters can share one etcd.
- Every request has a timeout, and every failure is `StoreUnavailable`. The
  store never repeats a transaction: one that failed may have been applied,
  and only the layer above can read again and decide. A read changes
  nothing, so one that failed is made once more. That was added after the
  first deployment on a real cluster, where about one tool in a hundred
  that had just started failed on its first read, with etcd in good health.

## How the tests get a real etcd

The store is a thin adapter, so what can be wrong with it is an assumption
about etcd. A fake would repeat the assumptions; the tests start the real
program.

Bazel downloads the etcd release for the platform (3.6.17 for macOS arm64,
Linux x86-64 and Linux arm64), pinned by the checksums of the release's
`SHA256SUMS` file, and passes its path to the test in `BITA_ETCD_BINARY`. The
test starts etcd on loopback ports that no other test is given
(`bitacora-testports`), with its data in the test's temporary directory, and
kills it when the last test using it finishes. Nothing is
downloaded while a test runs. The tests share one etcd, each under its own
key prefix; the three that kill etcd or start it late use one of their own.
The target runs 48 tests in about two seconds.

Under Cargo the variable is set by hand. When it is not, the tests that need
etcd pass without running and say so on standard error. Under Bazel a missing
etcd is a failure, so continuous integration cannot go green by skipping.

`etcd-client` generates its code from etcd's protocol files when it is built,
and that needs `protoc`. No version or feature of it ships generated code.
Bazel therefore also downloads a pinned `protoc` (36.2, same three platforms)
and hands it to that one build script. A developer who builds with Cargo
needs `protoc` on the path.

## What the tests cover

- The behaviours the in-memory store is tested for, under the same names:
  absent keys, revisions that change and never repeat, conditions that hold
  and fail, a failed transaction that changes nothing, sorted listings.
- Corners that only exist on etcd: revision zero on a missing key, a
  revision etcd can never assign, a listing larger than the 4 MiB a gRPC
  client accepts by default, two writers racing to create one key.
- Key prefixes: stores with different prefixes do not see each other, stores
  with the same prefix share their data.
- The typed layer on top: stream creation, epoch claims including two at the
  same moment, open, seal and roll of a segment, checkpoint commits with a
  superseded token, and controller leadership.
- Unavailability: nothing listening, a server that accepts and never
  answers, an etcd killed after it answered, and a store that works again
  once etcd is back.

## What is not covered

- No TLS and no authentication. An `https` endpoint is refused when the
  store is built. A production cluster needs both before it leaves a trusted
  network.
- No watches. The design lists watches among what the store must offer
  (section 8), but nothing in the implementation uses them yet. The trait
  has no watch, so the backend has none. They come with their first user.
- No etcd leases. Writer leases and controller leadership are timestamps
  inside records, compared with the caller's clock, on both backends.
- No connection keep-alive. A connection that dies without a reset is
  noticed by the request timeout, one request at a time.
- No multi-member etcd in the tests. They run one member, so behaviour
  during a leader election or a partition of etcd itself is not exercised.
- etcd's limits apply and surface as `StoreUnavailable`: a transaction may
  not write one key twice, and has at most 128 operations and 1.5 MiB by
  default. Nothing in bitacora comes near them today.
- A listing is one response held in memory. That is fine for thousands of
  streams and wrong for millions.

## How to run it

```
bazel test //src/rust/bitacora-meta-etcd/...
```

With Cargo, from `src/rust`:

```
BITA_ETCD_BINARY=/path/to/etcd cargo test -p bitacora-meta-etcd
```
