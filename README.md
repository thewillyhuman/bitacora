# bitacora

The log is the database. bitacora is a storage layer, written in Rust, that
lets a data system separate compute from storage the way Amazon Aurora does.
It has two parts:

- a **replicated, low-latency log**: quorum-replicated appends on local disks
  across failure domains, with one writer per stream and no consensus round
  on the write path;
- a **materializer framework** that turns that log into queryable state:
  versioned pages for databases, immutable artifacts for metrics and logs.

The log is the only thing a compute node writes to. Everything else is
derived from it, so a compute node holds no durable state and can be
replaced, added or failed over without copying data.

The full design is in [docs/spec.md](docs/spec.md). Decisions made while
implementing it are recorded in [docs/decisions](docs/decisions).

## Repository layout

| Path | Contents |
| --- | --- |
| `docs/` | The design, the wire and disk formats, decision records, guides |
| `src/docker/` | The container images of the daemons and of the `bita` tool |
| `src/helm/` | The Helm chart that runs a cluster on Kubernetes |
| `src/proto/` | The wire protocol, as language-neutral Protocol Buffers definitions |
| `src/rust/` | The Cargo workspace: one crate per component |
| `src/c/` | The C client: its header, an example and the C tests; see [docs/clients/c.md](docs/clients/c.md) |
| `src/systemd/` | Units that run the daemons on a machine, and the archive that installs them |
| `development/` | Scripts and files for working on bitacora locally |
| `hack/` | Scripts that manage the repository: checks, changelog, releases |
| `.chloggen/` | Pending changelog entries, one small file per change |

## Building and testing

Bazel builds, lints and tests everything with a hermetic Rust toolchain; the
only prerequisite is [Bazelisk](https://github.com/bazelbuild/bazelisk).

```sh
hack/check.sh                 # format, then build, lint and test everything
bazel test //...              # build and test without linting
bazel test --config=lint //...  # what continuous integration runs
```

`Cargo.toml` stays the source of truth for dependencies and lint levels, so
`cargo` and rust-analyzer work in `src/rust/` as in any Cargo workspace.
Unlike Bazel, they use what the machine has: the etcd client needs `protoc`
on the path to build, the S3 client needs a C compiler, and the tests that
need a server run only when a variable names its program: `BITA_ETCD_BINARY`
for etcd, `BITA_S3_SERVER_BINARY` for the Versity S3 Gateway. The test that
runs a cluster of real processes also needs `BITA_REPLICA_BINARY` and
`BITA_CONTROLLER_BINARY` to name the two daemons, for example the ones in
`target/debug`.

## Changelog and releases

Every user-visible change adds an entry with `hack/chloggen.sh new -f <name>`.
`hack/create_release.sh X.Y.Z` collects the entries into `CHANGELOG.md`, sets
the version, tags, pushes and publishes the release with those notes.

## Licence

Apache-2.0. See [LICENSE](LICENSE).
