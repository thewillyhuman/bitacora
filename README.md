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
implementing it are recorded in [docs/decisions](docs/decisions). To run a
cluster, start with [docs/operations](docs/operations/README.md). The rest
of the documentation is listed in [docs/README.md](docs/README.md).

## Status

Every milestone of the design's roadmap (section 18) is written and tested
here, on one machine and in continuous integration, and release candidates
have run on one Kubernetes cluster for a day of benchmarks and drills. A
soak of three days is running there and has not finished. No release is
tagged, and nothing is frozen: formats and interfaces can still change.

| Milestone | In the repository | Not done, or not validated |
| --- | --- | --- |
| M0 Specification | An executable model of append, roll and failover, searched exhaustively; the protocol documents in [docs/protocol](docs/protocol/README.md) | |
| M1 Single-node log | Segment format, replica, writer and reader; crash and torn-write tests; a fuzz test of the decoder | |
| M2 Replicated log | Quorum appends, rolls, writer failover, the etcd backend; a deterministic simulation that runs a fault matrix on every build | The performance targets of section 16: [measured](docs/measurements.md) on network block storage only, where the latency targets are missed; not on the disks the targets assume |
| M3 Lifecycle | Controller with placement, repair, tiering to an object store, truncation and retention; the `bita` tool. On the cluster, segments of a gigabyte were tiered, released and read back, and a lost metadata store was restored and rebuilt, [as measured](docs/measurements.md) | An object store that is slow or away; any object store but one S3 server on one volume |
| M4 Materializers | The framework, both adapters and bitacora-kv, whose histories are checked for linearizability under the fault matrix | |
| M5 Hardening | Daemons, Helm chart, systemd units, container images, metrics, alert rules, a dashboard, and tests on a cluster of real processes. The chart ran a cluster on Kubernetes that [survived](docs/measurements.md) a killed replica, a killed controller, a writer takeover, upgrades under load, a lost disk, a replica away for minutes and the loss of its metadata store, each done once or twice by hand | The units never ran under systemd. The first soak has not finished, and there are no nightly runs yet. In its first hour a zone was cut off the network eleven times and nothing was lost, but appends stalled for up to 31 s each time. No other partition was made on real machines. No automated upgrade test, no TLS, no tagged release, only release candidates |
| M6 Second materializer | A metrics-block materializer built on the public interface; the C and the Go client | Five gaps of the materializer interface are open ([record 0015](docs/decisions/0015-metrics-materializer.md)), so it is not frozen |

## Try it on one machine

`development/cluster.sh` runs etcd, three replicas and a controller on
loopback ports, with a local directory as object store. It builds what it
runs with Bazel, and keeps everything under one state directory.

```console
$ development/cluster.sh start
The cluster is ready: etcd, 3 replicas and a controller.
$ development/cluster.sh bita stream create orders
$ development/cluster.sh bita stream append orders "first record" "second record"
appended 2 records to orders at sequences 1 to 2 under epoch 1
$ development/cluster.sh bita stream read orders
SEQUENCE  PAYLOAD
1         first record
2         second record
$ development/cluster.sh reset    # stop everything and delete its state
```

`development/cluster.sh help` lists the other commands, and
[docs/cli.md](docs/cli.md) everything `bita` does.

## Clients

| Language | Where | What it is |
| --- | --- | --- |
| Rust | `src/rust/bitacora` | The client library: writers, readers and stream administration |
| C | `src/c/`, [docs/clients/c.md](docs/clients/c.md) | Blocking functions over the Rust library, as a static library and a header |
| Go | `src/go/`, [docs/clients/go.md](docs/clients/go.md) | A second implementation, written from the protocol documents and tested against the Rust daemons |

## Repository layout

| Path | Contents |
| --- | --- |
| `docs/` | The design, the wire and disk formats, decision records, guides |
| `src/docker/` | The container images of the daemons and of the `bita` tool |
| `src/grafana/` | The dashboard of a cluster |
| `src/helm/` | The Helm chart that runs a cluster on Kubernetes |
| `src/prometheus/` | The default alert rules |
| `src/proto/` | The wire protocol, as language-neutral Protocol Buffers definitions |
| `src/rust/` | The Cargo workspace: one crate per component, see below |
| `src/c/` | The C client: its header, an example and the C tests |
| `src/go/` | The Go client: one Go module |
| `src/systemd/` | Units that run the daemons on a machine, and the archive that installs them |
| `development/` | Scripts and files for working on bitacora locally |
| `hack/` | Scripts that manage the repository: checks, changelog, releases |
| `.chloggen/` | Pending changelog entries, one small file per change |

## The Rust crates

Each crate has a README that says what it owns, how it is built and which
crates it may use. Dependencies point one way: a crate uses only crates
from the rows above its own, except in its tests.

| Layer | Crates | Hold |
| --- | --- | --- |
| Vocabulary | `bitacora-types`, `bitacora-artifact`, `bitacora-model` | Identifiers, positions, the replica interface; the object store interface; the executable model. No I/O |
| Formats and stores | `bitacora-segment`, `bitacora-meta`, `bitacora-proto`, `bitacora-artifact-s3` | The segment file and the tiered object; the metadata records and their rules; the wire protocol; the S3 backend |
| Components | `bitacora-replica`, `bitacora-client`, `bitacora-controller`, `bitacora-meta-etcd`, `bitacora-process` | One storage node; writer, reader and administration; placement, repair, tiering and retention; the etcd backend; what every daemon shares |
| What applications use | `bitacora`, `bitacora-materializer` | The client that connects to a cluster; the materializer framework |
| Programs and stores | `bita-replica`, `bita-controller`, `bita`, `bita-bench`, `bitacora-kv`, `bitacora-metrics`, `bitacora-c` | The daemons and tools; the two materializers; the C interface |
| Test support | `bitacora-testports` | Loopback ports for tests that start real servers, in blocks that one test owns |
| Test clusters | `bitacora-testkit`, `bitacora-clusterkit` | A cluster inside one process with switchable faults; a cluster of real processes |
| Simulation | `bitacora-sim` | The deterministic simulation: a workload, a fault schedule and a checker of the history, all from one seed |
| Soak | `bita-soak` | The simulation's idea on a real cluster: a workload under faults for days, judged round by round by the simulation's checker |

## Building and testing

Bazel builds, lints and tests everything with hermetic Rust and Go
toolchains; the only prerequisite is
[Bazelisk](https://github.com/bazelbuild/bazelisk).

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
the version, tags, pushes and publishes the release with those notes. The
release commit is tagged twice: `vX.Y.Z`, and `src/go/vX.Y.Z` for the Go
module.

Every tag `vX.Y.Z` also gets its container images, published at
`ghcr.io/thewillyhuman/bitacora`. To try a build on a cluster before it is
released, push a tag `vX.Y.Z-rc.N` by hand: a release candidate gets the
images and nothing else, no release and no notes.

## Licence

Apache-2.0. See [LICENSE](LICENSE).
