# 0013. Repository conventions that differ from the design

Status: accepted · 2026-11-25

## Context

Section 17 of the design sketches a repository layout and conventions. A
few of them were changed, each for a reason worth writing down once.

## Decisions

**Sources are grouped by language.** The design puts crates at the top
level. Here everything lives under `src/<language>/`: `src/rust/` for the
Cargo workspace, `src/proto/` for the protocol files, and so on for
deployment files. `docs/` holds documentation, `hack/` the scripts that
manage the repository, `development/` what a developer needs to run a
cluster locally.

**Bazel builds and tests; Cargo stays the source of truth for
dependencies.** `MODULE.bazel` reads `src/rust/Cargo.toml` and
`Cargo.lock` through `crate_universe`, and lints come from the same
manifest, so the two builds cannot drift apart and editors that speak Cargo
keep working. `hack/check.sh` is the one command that formats, lints and
tests; `hack/commit.sh` commits only if it passes.

**More crates than the design lists.** A backend that pulls in a network
client is a crate of its own (`bitacora-meta-etcd`, the S3 artifact store,
`bitacora-proto`), so that the simulator and the unit tests of everything
else do not compile it. `bitacora-testkit` holds the in-process cluster
that several crates test against. `bitacora-model` is the executable
protocol model. `bitacora` is what applications depend on.

**The changelog refers to roadmap milestones.** chloggen wants an issue
number on every entry. This repository has no issue tracker, so the number
is the roadmap milestone (0 to 6, section 18 of the design), rendered as
`M<n>` by a custom template.

**No external simulation runtime, model checker or linger timer.** See
records 0008, 0001 and 0011.

## Consequences

- A contributor needs Bazel (through bazelisk) and nothing else for the
  full check; Cargo works for everyday editing, with the limits the README
  states.
- Every new crate is listed in three places: the Cargo workspace,
  `MODULE.bazel`, and its own `BUILD.bazel`.
