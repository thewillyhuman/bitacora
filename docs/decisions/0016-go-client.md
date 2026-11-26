# 0016. A native Go client, written from the protocol documents

Status: accepted · 2026-11-26

## Context

Section 13 of the design promises a native Go client over the same gRPC
protocol, and milestone M6 asks for it. The Rust client is the reference
implementation; the C client (record 0014) is that same library behind a C
interface.

A second implementation is also the only real test of the claim in
`docs/protocol/README.md`: that a client in another language is written
from those pages, not from the Rust code. So the Go client was written from
the pages, the Rust code was read only where a page left a question open,
and every such place was recorded and the page fixed.

## Decisions

**A second implementation, not a binding.** A Go program that links the C
library needs cgo, a C toolchain and a library built for its platform, and
gives up cross-compilation, static binaries and the Go scheduler's view of
blocked calls. The protocol is small enough to implement twice: the Go
client is about 4,300 lines with its comments, without its tests and the
generated stubs. The price is that a change to the protocol now has two
clients to change, which is what the interop tests are for.

**One module in a subdirectory.** The module is
`github.com/thewillyhuman/bitacora/src/go`, following the repository's
layout by language (record 0013); applications import its `bitacora`
package. A module in a subdirectory is released with tags of the form
`src/go/vX.Y.Z`, and `hack/create_release.sh` puts one on every release
commit next to `vX.Y.Z`, so the module and the daemons share their
versions.

**Go 1.26.** It is the oldest release Go still supports. `go.mod` promises
it and Bazel pins `1.26.11`, so what is built is what is promised.

**The stubs are checked in.** `go get` knows nothing of Bazel or protoc, so
`internal/wire/*.pb.go` are in the repository. They are generated with
protoc as pinned in `MODULE.bazel` and `protoc-gen-go` and
`protoc-gen-go-grpc` at the versions of `go.mod`, where they are `tool`
dependencies. A Bazel rule generates them again and a test compares; a
protocol file that changes without its stubs fails `bazel test`.
`hack/update_go_stubs.sh` copies the generated files over the checked-in
ones. The protocol file names no Go package: the mapping is an option of the
generator, so that a file every language shares carries no language's
concerns.

**grpc-go, protobuf-go and the etcd client.** They are the reference
implementations of what the protocol is made of, and what a Go program that
talks to etcd already depends on. The etcd client is given a logger that
discards: a library returns its errors and prints nothing.

**Bazel builds it with everything else.** `rules_go` with a downloaded
toolchain, `go_deps` reading `go.mod`, and build files written by hand:
there are ten of them, and a generator would be one more tool to pin.
`go vet` runs on every package as it compiles, through `nogo`. A test fails
on a file that `gofmt` would change. Go is built without cgo, so it needs
no C toolchain. Two of the etcd client's dependencies ship generated Go
files next to their protocol files; Bazel is told to use the Go files, as
`go build` does, instead of compiling protoc from source to generate them
again.

**The API is Go's, not a transliteration.** Every call that waits takes a
context. `Follow` is an iterator for `range`. Errors wrap one of nine
values for `errors.Is`, and the one error with something to say,
`TruncatedError`, is a type for `errors.As`. A writer's goroutines end with
`Close`, and tests look at the stacks of all goroutines to see that they
did. The client has no global state. Go's context adds one thing the Rust
client does not have: an append whose context ends while the record awaits
commit is reported as indeterminate, with the sequence number the record
was given, because that is exactly what it is.

**The writer's bookkeeping does no I/O.** Which records are assigned, what
each replica acknowledged, what is committed and when to roll is one type
that takes the time as an argument. The commit rule and the roll rules are
tested there, in microseconds and without a clock. Goroutines around it do
the sending.

**Two layers of tests.** The package is tested against a metadata store in
memory and fake replicas on loopback ports that follow the replica's rules
and fail when told to. That is fast and reaches every fault, and it proves
nothing about the Rust side. The interop tests start the pinned etcd, three
`bita-replica` and one `bita-controller`, and put the Rust tools on one
side and the Go client on the other: each reads what the other wrote, each
takes a stream from the other and the loser sees that it was fenced, a
replica is killed under a Go writer, the controller tiers, releases,
truncates and deletes what Go reads and administers.

## What the client does not do

- It reads tiered segments from a local directory only. There is no S3
  object store; the interface for one is a single method.
- `CreateStream` does not place the first segment. The controller does, one
  pass later, and `OpenWriter` waits for it.
- It registers and unregisters materializers but does not run one.
- It does not use the `ReplicaAdmin` and `ReplicaTransfer` services.
- It exports no metrics.

## What the protocol documents did not say

Each of these was found by needing it, answered from the Rust code, and
written into the page by the commit named. None changes the protocol.

| Gap | Page | Fixed by |
| --- | --- | --- |
| How the store's requirements map to etcd: the key is the prefix plus the key, the revision is the mod revision, "absent" is a create revision of 0, a transaction is one `Txn` with an empty failure branch | `metadata.md` | `docs: say how the metadata store maps to etcd` |
| How a record is written: a list is `[]` and never `null`, except `standby_replicas`; unknown fields are ignored and lost on a rewrite; a newer `schema_version` is refused | `metadata.md` | `docs: say how metadata records are written` |
| Which names and which replication settings are valid | `metadata.md` | `docs: describe how a stream is administered` |
| How a stream is created: the two keys the transaction is conditional on, what the counter and the new record hold, that the standby may be left to the controller | `metadata.md` | same |
| How a materializer is registered and unregistered, and where its checkpoint starts | `metadata.md` | same |
| How an owner asks for truncation, and that the position only moves forward | `metadata.md` | same |
| What a seal, an open and a roll do when they find their own effect in the record, which the "repeat until the outcome is known" of the replication rules depends on; how low a seal can be | `metadata.md` | `docs: say what a repeated stream write finds` |
| What a writer does with each kind of refusal: send from elsewhere, send again, roll, or stop | `replication.md` | `docs: say what a writer does with each answer` |
| What a writer does when it wants to roll and no standby is placed, and when the roll's update gets no answer | `replication.md` | same |
| How a writer steps down so that its successor needs no recovery | `replication.md` | same |
| The timings the client libraries use by default | `replication.md` | same |
| How long a candidate respects a lease, and against which clock | `replication.md` | `docs: fill in what a takeover leaves open` |
| What a refused fence means, and whether to wait for more answers than needed | `replication.md` | same |
| Which epoch goes in which field of the repair's read and append, and what durable sequence the append carries | `replication.md` | same |
| What a candidate does when there is no standby segment to open | `replication.md` | same |
| Where a local-directory object store keeps an object | `object-store.md` | `docs: say where a local object store keeps objects` |

One place where the Rust code and a page disagreed was found: the
conventions say that a program refuses a format version newer than it
knows, and the Rust metadata store did not look at `schema_version` when it
read a record. The Go client refused from the start. The Rust records
refuse too since the change that followed this work (`meta: refuse records
of a newer schema`).

## Consequences

- The build needs a second toolchain. Bazel downloads the Go SDK (about
  65 MB) and 22 Go modules once. Compiling the Go standard library, gRPC,
  the etcd client and the client itself from nothing took about a minute on
  the machine this was written on. With a warm cache, an unchanged Go tree
  costs nothing.
- The Go tests add nine unit and check targets and the interop target,
  whose tests each start a cluster of their own and run side by side:
  together under twenty seconds of test time, and less than ten on the
  clock, because Bazel runs the targets in parallel.
- A change to `replica.proto` needs `hack/update_go_stubs.sh`. A change to
  a record, a transition or a rule needs the Go client changed too; the
  golden bytes and JSON it shares with the Rust tests, and the interop
  tests, are what notices when it was not.
- The protocol pages are now enough to write a client from, as far as one
  more client can show.
