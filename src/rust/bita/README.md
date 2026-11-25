# bita

The command-line tool an operator uses to look at a bitacora cluster and to carry out the routine
procedures of the design (section 14). The user's reference is `docs/cli.md`.

## What it owns

- The command line: `bita <noun> <verb>`, its options and its help.
- How the cluster is found: a configuration file, options or environment variables.
- How results are shown: aligned text for people, JSON with stable field names for scripts.
- The exit codes: 0 success, 1 failure, 2 wrong command line or configuration.

It owns no protocol logic. Everything it does to a cluster goes through the client library, the
metadata store's own operations and the replica interface.

## Design

Every command is a function over the cluster's injected interfaces (metadata store, replica connector,
clock, optional object store) that returns a value. The value is then rendered as text or JSON. `main`
only parses, finds the cluster, connects, runs and prints.

That shape is what makes the tool testable: the commands run against the in-process test cluster under
a virtual clock, and the renderers are pure functions tested on values built by hand. `stream tail` is
the one exception to "returns a value": it writes records to its output as they commit.

A command never prints. What it has to say is in its result or in its error.

## Invariants

- Parsing has no side effect, and a wrong command line sends nothing to the cluster.
- The tool never guesses a cluster: without an etcd endpoint it stops.
- Destructive commands need a confirmation (`stream delete --yes`, `replica remove --force` for a live
  replica).
- Results go to standard output and nothing else does.
- `docs/cli.md` has a section for every command and for nothing else; a test checks both.

## Files

| File | Holds |
| --- | --- |
| `src/main.rs` | The entry point: parse, find the cluster, connect, run, print |
| `src/arguments.rs` | The whole command line, as `clap` types |
| `src/cluster.rs` | Finding the cluster, and the interfaces every command works through |
| `src/commands.rs` | Running a parsed command; every kind of result and error |
| `src/commands/stream.rs` | `bita stream ...` |
| `src/commands/replica.rs` | `bita replica ...` |
| `src/commands/segment.rs` | `bita segment ...` |
| `src/commands/controller.rs` | `bita controller ...` |
| `src/commands/testing.rs` | The in-process cluster the command tests share |
| `src/output.rs` | Tables and the notations for times, sizes and payloads |
| `src/duration.rs` | Durations as an operator types them |

Each file's tests are next to it, in `<file>_test.rs`.

## Dependencies

`bitacora` (the client and the connections), `bitacora-meta`, `bitacora-types` and `bitacora-artifact`
for the interfaces the commands take. The tests add `bitacora-testkit` and `bitacora-controller`. Nothing
depends on this crate.
