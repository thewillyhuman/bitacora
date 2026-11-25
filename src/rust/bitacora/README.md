# bitacora

The crate an application depends on to use a bitacora cluster.

## What it owns

- The cluster configuration (`ClusterConfig`, `ObjectStoreConfig`): where etcd is, which key prefix the
  cluster uses there, and which object store sealed segments are tiered to.
- `connect`: from that configuration to a `BitacoraClient`.
- The same wiring in parts (`connect_metadata`, `open_object_store`, `replica_connector`) for the
  daemons and tools of this repository, so that every process reads the configuration the same way.

It re-exports the client's API, so an application needs no other bitacora crate.

## Design

`bitacora-client` takes its metadata store, transport, clock and object store as injected interfaces,
which is what lets it run in the deterministic simulator. This crate picks the real ones: etcd, gRPC,
the system clock and the configured object store. It adds no behaviour of its own.

Connecting sends nothing. A wrong or unreachable etcd endpoint shows up on the first request; only a
configuration that can never work is refused when connecting.

## Invariants

- Every process of one deployment is given the same etcd key prefix and the same object store.
- No protocol logic lives here.

## Files

| File | Holds |
| --- | --- |
| `src/lib.rs` | The re-exported API |
| `src/cluster.rs` | The configuration and the connecting |
| `src/cluster_test.rs` | Their tests |

## Dependencies

It may depend on `bitacora-client`, `bitacora-meta`, `bitacora-meta-etcd`, `bitacora-proto`,
`bitacora-artifact`, `bitacora-artifact-s3` and `bitacora-types`. Only binaries and other applications depend on it.
