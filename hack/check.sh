#!/usr/bin/env bash
# Formats the Rust sources, then builds, lints and tests the given targets
# (everything by default). This is the loop to run before every commit; it is
# the same check continuous integration runs, plus the formatting step.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

bazel run --ui_event_filters=-info,-stdout,-stderr --noshow_progress \
    @rules_rust//:rustfmt
bazel test --config=lint "${@:-//...}"
