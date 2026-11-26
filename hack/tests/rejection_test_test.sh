#!/usr/bin/env bash
# Tests hack/tools/rejection_test.sh on the cases it must tell apart: a
# checker that names the mistake, one that fails without naming it, one that
# accepts the input, and a findings file with nothing in it.
#
# Usage: rejection_test_test.sh <rejection_test.sh>
set -euo pipefail

rejection_test="${1:?usage: rejection_test_test.sh <rejection_test.sh>}"
scratch="${TEST_TMPDIR:?the test needs a temporary directory}"

printf 'a known mistake\n' >"${scratch}/findings"
: >"${scratch}/no_findings"
printf '#!/bin/sh\necho "found: a known mistake" >&2\nexit 1\n' >"${scratch}/sharp_checker"
printf '#!/bin/sh\necho "found nothing of note" >&2\nexit 1\n' >"${scratch}/blind_checker"
printf '#!/bin/sh\nexit 0\n' >"${scratch}/lenient_checker"
chmod +x "${scratch}/sharp_checker" "${scratch}/blind_checker" "${scratch}/lenient_checker"

# Runs the rejection test and requires an exit code and a phrase in its output.
expect() {
    local expected_exit="$1" expected_phrase="$2" output actual_exit=0
    shift 2
    output="$("${rejection_test}" "$@" 2>&1)" || actual_exit=$?
    if ((actual_exit != expected_exit)); then
        echo "FAILED: $* exited with ${actual_exit}, expected ${expected_exit}: ${output}" >&2
        exit 1
    fi
    case "${output}" in
        *"${expected_phrase}"*) ;;
        *)
            echo "FAILED: $* did not say '${expected_phrase}': ${output}" >&2
            exit 1
            ;;
    esac
}

expect 0 "found all 1 mistakes" "${scratch}/findings" "${scratch}/sharp_checker"
expect 1 "did not report: a known mistake" "${scratch}/findings" "${scratch}/blind_checker"
expect 1 "accepted an input with known mistakes" "${scratch}/findings" "${scratch}/lenient_checker"
expect 1 "names no mistake to look for" "${scratch}/no_findings" "${scratch}/sharp_checker"
expect 2 "Usage: rejection_test.sh"
echo "PASSED"
