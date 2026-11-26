# The Go client of bitacora

A native Go client: it speaks the same gRPC protocol to replicas and keeps
the same records in etcd as the Rust client, so the two can be used on the
same cluster and on the same stream. It was written from the protocol
documents in [docs/protocol](../../docs/protocol), not from the Rust code.

This page is for people who change it.

## Layout

One Go module, `github.com/thewillyhuman/bitacora/src/go`.

| Directory | What it is |
| --- | --- |
| `internal/wire/` | The gRPC stubs, generated from `src/proto` and checked in |
| `internal/frame/` | The batch frame: how records travel and are stored, under one CRC-32C |
| `internal/tiered/` | The footer and block index of a segment in the object store |
| `internal/metadata/` | The records in etcd, their keys, and the conditional writes that change them |
| `internal/replica/` | The replica service from the caller's side: typed requests and refusals |
| `internal/testcluster/` | Starts etcd and the Rust daemons for tests |

Dependencies point downwards only: `metadata` and `frame` know nothing of
gRPC, `replica` knows nothing of etcd.

## Building and testing

Bazel builds and tests the module with everything else:

```console
bazel test //src/go/...
```

The Go toolchain is pinned in `MODULE.bazel` and downloaded by Bazel.
`go.mod` and `go.sum` are the source of truth for dependencies; Bazel reads
them. After changing them, run `bazel mod tidy`. `BUILD.bazel` files are
written by hand: a new file or import is added to its package's.

`go build ./...` and `go test ./...` work here as in any Go module, with a
Go of the version `go.mod` names or later. Tests that need etcd or the Rust
daemons are skipped there with a message, unless the environment variables
in `internal/testcluster/process.go` name the programs.

Two checks run as part of `bazel test`:

- `go vet` runs on every package as it is compiled; a finding fails the
  build.
- `//src/go:gofmt_test` fails if a file is not formatted by `gofmt`.

## The generated stubs

`internal/wire/*.pb.go` are generated from
`src/proto/bitacora/v1/replica.proto` and checked in, so that the module
builds with plain `go build` and `go get`. Bazel generates them again with
the pinned tools (protoc from `MODULE.bazel`, the two plugins at the
versions of `go.mod`) and compares; a protocol file that changed without
its stubs fails the test. To bring them in step:

```console
hack/update_go_stubs.sh
```
