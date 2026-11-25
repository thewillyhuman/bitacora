#!/usr/bin/env bash
# Lints shell scripts with the pinned shellcheck. Used through the
# `shellcheck_test` rule of //hack/tools:defs.bzl, which names shellcheck in
# SHELLCHECK and the scripts as arguments.
#
# Usage: shellcheck_test.sh <script>...
set -euo pipefail

if (($# == 0)); then
    echo "shellcheck_test.sh: no scripts were given" >&2
    exit 2
fi

# Every script is treated as bash, whatever its first line says, and sources
# are not followed: a test lints the scripts it names and nothing else.
exec "${SHELLCHECK:?SHELLCHECK must name the shellcheck program}" --shell=bash --severity=style "$@"
