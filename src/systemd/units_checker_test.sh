#!/usr/bin/env bash
# Tests the test of the units: hands units_test.sh a unit with known
# mistakes and requires it to fail and to name every one of them. A checker
# that passes everything proves nothing.
#
# Usage: units_checker_test.sh <units_test.sh> <package.tar.gz> <broken unit>
set -euo pipefail

units_test="${1:?usage: units_checker_test.sh <units_test.sh> <package.tar.gz> <broken unit>}"
package="${2:?the package is missing}"
broken_unit="${3:?the broken unit is missing}"

if report="$("${units_test}" "${package}" "${broken_unit}" 2>&1)"; then
    echo "FAILED: the checker accepted a unit with known mistakes" >&2
    exit 1
fi

# One line per mistake that //src/systemd:broken_replica_unit puts in.
expected_findings=(
    "unknown directive NoNewPrivilges"
    "NoNewPrivileges is not switched on"
    "has no option --verbose"
    "the daemon runs as root"
    "exit code 2, a wrong configuration, would be restarted for ever"
    "TimeoutStopSec is not longer than"
    "outside /var/lib/elsewhere"
)

missed=0
for finding in "${expected_findings[@]}"; do
    case "${report}" in
        *"${finding}"*) ;;
        *)
            echo "FAILED: the checker did not report: ${finding}" >&2
            missed=$((missed + 1))
            ;;
    esac
done

if ((missed > 0)); then
    echo "The checker reported:" >&2
    echo "${report}" >&2
    exit 1
fi
echo "PASSED: the checker found all ${#expected_findings[@]} mistakes"
