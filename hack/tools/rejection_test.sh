#!/usr/bin/env bash
# Tests a checker: runs it on an input with known mistakes and requires it to
# fail and to name every one of them. A checker that passes everything
# proves nothing.
#
# Usage: rejection_test.sh <findings file> <checker> [<checker argument>...]
#
# The findings file has one phrase per line, one for each mistake the input
# has. Every phrase must appear in what the checker prints. Bazel calls this
# script through the `rejection_test` rule of //hack/tools:defs.bzl.
set -euo pipefail

if (($# < 2)); then
    sed -n '2,/^set -euo/{/^set -euo/d;s/^# \{0,1\}//;p;}' "${BASH_SOURCE[0]}" >&2
    exit 2
fi

findings_file="$1"
checker="$2"
shift 2

if report="$("${checker}" "$@" 2>&1)"; then
    echo "FAILED: the checker accepted an input with known mistakes" >&2
    exit 1
fi

expected=0
missed=0
while IFS= read -r finding; do
    if [[ -z "${finding}" ]]; then
        continue
    fi
    expected=$((expected + 1))
    case "${report}" in
        *"${finding}"*) ;;
        *)
            echo "FAILED: the checker did not report: ${finding}" >&2
            missed=$((missed + 1))
            ;;
    esac
done <"${findings_file}"

if ((expected == 0)); then
    echo "FAILED: ${findings_file} names no mistake to look for" >&2
    exit 1
fi
if ((missed > 0)); then
    echo "The checker reported:" >&2
    echo "${report}" >&2
    exit 1
fi
echo "PASSED: the checker found all ${expected} mistakes"
