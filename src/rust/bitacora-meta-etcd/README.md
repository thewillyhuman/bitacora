# bitacora-meta-etcd

The etcd backend of the bitacora metadata store.

## What it owns

- `EtcdKeyValueStore`: the implementation of `bitacora_meta::KeyValueStore` that a real cluster runs on.
- `EtcdSettings`: the endpoints, the key prefix and the two timeouts a store is built from, with their
  defaults. `InvalidEtcdSettings` says why a set of settings was refused.

It owns nothing else. Records, epochs, leases, checkpoints and every rule about them belong to
`bitacora-meta`, which builds them once on the interface, so that the in-memory backend and this one
behave the same.

## Design

The store is a thin adapter: each method of the interface is exactly one etcd request.

| Interface | etcd request |
| --- | --- |
| `get` | A linearizable range over one key |
| `list` | A linearizable range over a prefix, sorted by key |
| `transact` | A transaction: comparisons, then puts and deletes |

A revision of the interface is the key's mod revision. etcd raises its revision on every write and never
reuses one, so a key that is deleted and created again gets a higher revision than before, which is what
the interface asks for. "The key is absent" compares the create revision with zero; "the key is at this
revision" compares the mod revision. A revision etcd can never have given to a key, zero included, turns
into a comparison that holds for no key, so the condition fails and the request does not.

One request is one consistent view, so a listing needs no paging. The price is that it arrives as one
response, and the response size limit of the gRPC client is lifted for that reason.

Every key gets the configured prefix on the way in and loses it on the way out. Several clusters can
share one etcd, and callers never see the prefix.

Connecting sends nothing. Connections are opened when the first request needs them and reopened after
they break, so a process can start while etcd is down and works once etcd is there.

Why a crate and not a module of `bitacora-meta`: the etcd client brings a gRPC stack. The simulator, the
test kit, the controller and the client library all depend on `bitacora-meta`, none of them talks to
etcd, and as a module all of them would compile that stack for nothing. See
`docs/decisions/0003-etcd-backend-crate.md`.

## Invariants

- Every request is bounded by the request timeout, whether etcd is slow, unreachable or gone.
- Every failure of a request is reported as `bitacora_meta::StoreUnavailable`. Nothing panics on what
  etcd answers.
- Nothing is retried. A transaction that failed may or may not have been applied; only the layer above
  can read again and decide.
- Settings are checked in one place, when a store is built from them, and a store is never built from
  settings that were refused.
- The crate holds no bitacora logic.

## Files

| File | Holds |
| --- | --- |
| `src/lib.rs` | The crate's rationale and its public names |
| `src/settings.rs` | `EtcdSettings`, its defaults and its validation, and `InvalidEtcdSettings` |
| `src/store.rs` | `EtcdKeyValueStore` and how each call maps to etcd |
| `src/<file>_test.rs` | The tests of the file of the same name, which need no etcd |
| `tests/etcd/main.rs` | The root of the tests that run against a real etcd |
| `tests/etcd/etcd_process.rs` | Starting, sharing and stopping the etcd the tests talk to |
| `tests/etcd/writes.rs` | Shorthand for the writes tests set the stage with |
| `tests/etcd/key_value_contract.rs` | The behaviours every backend must have, checked on etcd |
| `tests/etcd/metadata_store.rs` | A representative set of `MetadataStore` operations on etcd |
| `tests/etcd/unavailability.rs` | What a caller sees when etcd is unreachable, silent or gone |

## Tests

The unit tests need nothing. The tests in `tests/etcd/` start a real etcd on loopback ports that no
other test is given (`bitacora-testports`), with its data in the test's temporary directory, and stop
it when the last test using it finishes. Bazel fetches a pinned etcd release and passes its path in
`BITA_ETCD_BINARY`; nothing is downloaded while a test runs. Under Cargo, set the variable to the path
of an etcd program; without it those tests pass without running and say so once.

## Dependencies

It depends on `bitacora-meta` and on third-party crates; its tests also use `bitacora-testports`. One
crate depends on it: `bitacora`, which picks the real backends for an application and for the daemons
and tools of this repository. Nothing else may, so that a crate which only needs the interface or the
in-memory store never compiles a gRPC client.

```text
bitacora-types  <-  bitacora-meta  <-  bitacora-meta-etcd  <-  bitacora  <-  binaries
```

## Not covered

- TLS and authentication. An `https` endpoint is refused when the store is built.
- A transaction that writes one key twice, which etcd refuses and the in-memory store applies. Nothing
  in bitacora writes one.
