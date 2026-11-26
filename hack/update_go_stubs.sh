#!/usr/bin/env bash
# Generates the Go gRPC stubs again from src/proto and replaces the checked-in
# copies in src/go/internal/wire. Run it after changing a protocol file or
# the version of a generator in src/go/go.mod; `bazel test` fails until the
# checked-in stubs match what the pinned tools generate.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

bazel build //src/go/internal/wire:generated_stubs
generated="$(bazel info bazel-bin)/src/go/internal/wire/generated/bitacora/v1"
# Build outputs are read-only; the checked-in copies must not be.
install -m 0644 "${generated}/replica.pb.go" "${generated}/replica_grpc.pb.go" src/go/internal/wire/
