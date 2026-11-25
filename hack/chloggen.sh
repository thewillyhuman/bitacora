#!/usr/bin/env bash
# Runs chloggen at the version this repository pins, with its configuration.
#
# Usage:
#   hack/chloggen.sh new -f <entry-name>   start a changelog entry
#   hack/chloggen.sh validate              check every pending entry
#   hack/chloggen.sh update -v vX.Y.Z      collect entries into CHANGELOG.md
#
# Set CHLOGGEN to use another chloggen binary; tests do.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

readonly CHLOGGEN_VERSION="v0.28.1"

if [ -n "${CHLOGGEN:-}" ]; then
    exec "${CHLOGGEN}" --config .chloggen/config.yaml "$@"
fi
exec go run "go.opentelemetry.io/build-tools/chloggen@${CHLOGGEN_VERSION}" \
    --config .chloggen/config.yaml "$@"
