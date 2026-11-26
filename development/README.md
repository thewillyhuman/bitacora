# Local development

What you need to run bitacora on your own machine while working on it.

## A cluster in one command

`cluster.sh` runs a whole cluster from the programs Bazel builds: one etcd, three replicas and a
controller, all on `127.0.0.1`, with a local directory as object store. The only prerequisite is
[Bazelisk](https://github.com/bazelbuild/bazelisk), as for everything else in this repository.

From a fresh clone to a record appended and read back:

```console
$ development/cluster.sh start
The cluster is ready: etcd, 3 replicas and a controller.
State, logs and configuration: /tmp/bitacora-dev-you
Use it:   development/cluster.sh bita replica list
Directly: /…/bita --config /tmp/bitacora-dev-you/cluster.toml replica list

$ development/cluster.sh bita stream create demo
NAME  ID  EPOCH  WRITER  REPLICATION  SEGMENTS  LOW-WATER
demo  1   0      -       3/2          0         1

$ development/cluster.sh bita stream append demo "hello"
appended 1 records to demo at sequences 1 to 1 under epoch 1

$ development/cluster.sh bita stream read demo
SEQUENCE  PAYLOAD
1         hello
```

The first `start` builds the programs, which takes a few minutes; after that it takes seconds.
`development/cluster.sh bita …` is the `bita` tool pointed at the cluster; `docs/cli.md` describes
every command. An application finds the cluster through the same generated file, `cluster.toml` in the
state directory.

| Command | What it does |
| --- | --- |
| `start` | Starts whatever is not running and waits until every process is ready |
| `status` | Says what runs, with process ids, ports and log files |
| `logs [-f] <name>` | Prints the log of `etcd`, `controller` or `replica-<n>`; `-f` follows it |
| `bita <args>…` | Runs the `bita` tool against the cluster |
| `stop` | Stops everything and keeps the data, so the next `start` continues where it left off |
| `reset` | Stops everything and deletes the state directory |

### What runs where

Everything lives under one state directory and nothing is written outside it.

| Variable | Default | Meaning |
| --- | --- | --- |
| `BITA_DEV_STATE` | `${TMPDIR:-/tmp}/bitacora-dev-$USER` | The state directory: data, logs, configuration, pid files |
| `BITA_DEV_REPLICAS` | `3` | How many replicas to run, 1 to 9 |
| `BITA_DEV_BASE_PORT` | `27000` | The first port; see below |

With the default base port: etcd listens on 27000 (clients) and 27001 (peers), replica *n* on 27010+*n*
(gRPC) and 27020+*n* (metrics and probes), the controller on 27030 (metrics and probes). Two clusters
run side by side with two state directories and two base ports.

```text
<state>/
  cluster.toml               what bita and applications use to find the cluster
  objects/                   the object store
  etcd/{data,log,pid}
  replica-0/{replica.toml,data,log,pid}
  controller/{controller.toml,log,pid}
```

The configuration files are written by the script on every start; edit the script, not the files.

### How it differs from a real cluster

- **Disk thresholds are off.** A replica normally refuses new segments at 85% disk usage and every
  append at 95%. A developer's disk is often fuller than that, so here both are 100.
- **Delays are short.** Segments are tiered to the object store 30 seconds after they are sealed and
  leave the replicas two minutes later, instead of fifteen minutes and a day, so that tiering can be
  watched.
- **One etcd member, one machine.** Nothing here tests the loss of a machine. To see a failure, kill a
  process (`kill -9 $(cat <state>/replica-1/pid)`), watch `bita segment list demo`, and run `start`
  again to bring it back.

### Using other builds

Run directly, the script hands itself to `bazel run //development:cluster`, which builds the programs
and tells the script where they are. To run a cluster from other builds, name all four programs:

```sh
BITA_ETCD_BINARY=/usr/bin/etcd \
BITA_REPLICA_BINARY=src/rust/target/debug/bita-replica \
BITA_CONTROLLER_BINARY=src/rust/target/debug/bita-controller \
BITA_BINARY=src/rust/target/debug/bita \
development/cluster.sh start
```

## A cluster on Kubernetes

A machine of your own shows that the code works; it does not show how it behaves on real disks, across
zones, or under a real network. [kubernetes/](kubernetes/README.md) has what it takes to put a build on
a Kubernetes cluster you have: a test etcd, values for the chart, and a script that runs `bita` and the
benchmark next to the cluster.

## How this directory is tested

- `//development:cluster_test` runs `cluster.sh` for real: it starts a cluster in a temporary
  directory, creates a stream, appends and reads through `bita`, checks the replicas and the controller,
  stops, starts again on the old state, resets, and checks that nothing is left running. It takes about
  ten seconds.
- `//development:shellcheck_test` lints the scripts with the pinned `shellcheck`.
