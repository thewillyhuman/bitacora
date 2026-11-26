# The bitacora documentation

Everything written about bitacora that is not next to the code it
describes. Each crate, chart and script directory has a README of its own;
this directory holds what spans them.

| Start here | To |
| --- | --- |
| [spec.md](spec.md) | Understand the design: what bitacora is for, how it works and why |
| [operations/README.md](operations/README.md) | Run a cluster: install, configure, upgrade, and what to do when something fails |
| [cli.md](cli.md) | Look up a command of the `bita` tool |
| [clients/c.md](clients/c.md), [clients/go.md](clients/go.md) | Use bitacora from C or from Go |
| [protocol/README.md](protocol/README.md) | Write a program that talks to a cluster, or read what is on a disk |
| [benchmark.md](benchmark.md) | Measure a cluster with `bita-bench` |
| [measurements.md](measurements.md) | See what was measured on real machines, and what was not |

## Reference

| Page | Holds |
| --- | --- |
| [operations/metrics.md](operations/metrics.md) | Every metric the daemons expose |
| [operations/alerts.md](operations/alerts.md) | Every default alert, what it means and what to do |
| [protocol/](protocol/README.md) | The replication rules, the wire protocol, and the formats of segment files, tiered objects, metadata records, checkpoint manifests and metrics blocks |

The command reference, the metrics reference, the page about the alerts and
the examples of both client pages are compared with the code by tests, so
they cannot fall behind it.

## Decision records

The design leaves questions open, and implementing it answered some of them
differently. Each record says what was decided, what else was considered
and what follows from it.

| Record | Decision |
| --- | --- |
| [0001](decisions/0001-protocol-model.md) | The protocol model is a Rust crate with its own explorer |
| [0002](decisions/0002-grpc-transport.md) | The replica protocol is unary gRPC, with errors in the response |
| [0003](decisions/0003-etcd-backend-crate.md) | The etcd backend is a crate of its own, tested against a real etcd |
| [0004](decisions/0004-materializer-framework.md) | How the materializer framework fills in what the design leaves open |
| [0005](decisions/0005-s3-backend.md) | The S3 backend is a crate of its own, tested against a real S3 server |
| [0006](decisions/0006-daemons-and-observability.md) | Daemons share one process crate, and are observed at their injected interfaces |
| [0007](decisions/0007-operator-tooling.md) | Operator tooling: the `bita` command-line tool |
| [0008](decisions/0008-deterministic-simulation.md) | Simulation runs the real code over injected interfaces |
| [0009](decisions/0009-flushed-mark.md) | A segment file records how far it was flushed |
| [0010](decisions/0010-replica-identity.md) | A replica is reached by identity, not by address |
| [0011](decisions/0011-batching.md) | Batches form without a timer |
| [0012](decisions/0012-tiering.md) | How segments move to the object store and leave it |
| [0013](decisions/0013-repository-conventions.md) | Repository conventions that differ from the design |
| [0014](decisions/0014-c-abi.md) | The C interface of the client |
| [0015](decisions/0015-metrics-materializer.md) | The second materializer and what it found in the interface |
| [0016](decisions/0016-go-client.md) | A native Go client, written from the protocol documents |
| [0017](decisions/0017-request-size.md) | An append request carries up to 4 MiB |
| [0019](decisions/0019-placement-spread.md) | Placement spreads over replicas that are equally good |
