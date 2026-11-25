#!/usr/bin/env bash
# Runs hack/check.sh and commits everything that is staged or modified, but
# only if the check passes. The commit message is read from standard input.
#
# Usage: hack/commit.sh [bazel target patterns...] < message
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

check_log="$(mktemp)"
trap 'rm -f "${check_log}"' EXIT

if ! ./hack/check.sh "$@" >"${check_log}" 2>&1; then
    grep -v '^\[' "${check_log}" | grep -A 20 -E '^error|^warning: |FAILED|panicked' | head -150 || true
    tail -8 "${check_log}"
    echo "check failed: nothing was committed" >&2
    exit 1
fi
tail -12 "${check_log}" | grep -E 'PASSED|FAILED|Executed' || true

git add -A
git commit -q -F -
git log -1 --format='%h %s'
title_length="$(git log -1 --format=%s | wc -c)"
if [ "${title_length}" -gt 51 ]; then
    echo "commit title is longer than 50 characters" >&2
    exit 1
fi
