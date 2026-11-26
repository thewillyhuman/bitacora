#!/usr/bin/env bash
# Lints the Dockerfile with the pinned hadolint. Every finding fails the
# test, down to matters of style; hadolint.yaml lists the few rules that are
# switched off, each with its reason.
#
# Usage: hadolint_test.sh <hadolint> <hadolint.yaml> <Dockerfile>
set -euo pipefail

hadolint="${1:?usage: hadolint_test.sh <hadolint> <hadolint.yaml> <Dockerfile>}"
configuration="${2:?the configuration of hadolint is missing}"
dockerfile="${3:?the Dockerfile is missing}"

# A linter that cannot start must not look like a Dockerfile without faults.
if ! "${hadolint}" --version >/dev/null 2>&1; then
    echo "FAILED: the pinned hadolint does not run on this machine." >&2
    echo "On a Mac with Apple silicon it is the build for Intel, which needs Rosetta:" >&2
    echo "    softwareupdate --install-rosetta --agree-to-license" >&2
    exit 1
fi

"${hadolint}" --config "${configuration}" --failure-threshold style "${dockerfile}"
echo "PASSED"
