#!/usr/bin/env bash
# Writes src/c/include/bitacora.h again from the Rust source of the C
# interface.
#
# Usage:
#   hack/update_c_header.sh
#
# The header is generated, never edited: it is what C programs compile against
# and src/rust/bitacora-c is what they link, so the two must agree to the last
# argument. A test, //src/rust/bitacora-c:header_test, fails when the checked-in
# header is not what the source generates. Run this script after changing a
# function of the C interface or its documentation, and commit the result with
# the change.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# The test writes the header instead of comparing it when this is set. It
# finds the source tree through `bazel run`.
BITA_UPDATE_C_HEADER=1 bazel run //src/rust/bitacora-c:header_test
