#!/usr/bin/env bash
# Tests development/cluster.sh by running a real cluster with it: starts
# etcd, three replicas and a controller in a temporary directory, uses the
# cluster through the bita tool as the README tells a newcomer to, stops it,
# and checks that nothing is left running or lying around.
#
# Bazel names the four programs in BITA_ETCD_BINARY, BITA_REPLICA_BINARY,
# BITA_CONTROLLER_BINARY and BITA_BINARY.
set -euo pipefail

readonly cluster="development/cluster.sh"

state="${TEST_TMPDIR:?the test needs a temporary directory}/cluster"
export BITA_DEV_STATE="${state}"
# A range of ports nothing else on the machine is likely to use: below the
# ports the system hands out by itself. The cluster uses fewer than forty
# ports from its base, so the bases are forty apart: two copies of this test
# that start at the same moment have neighbouring process numbers, and
# their ranges must not overlap.
export BITA_DEV_BASE_PORT="$((20000 + ($$ % 225) * 40))"

current_step="nothing yet"

fail() {
    echo "FAILED while ${current_step}: $*" >&2
    "${cluster}" status >&2 || true
    exit 1
}

# Whatever happens, do not leave daemons behind.
cleanup() {
    "${cluster}" stop >/dev/null 2>&1 || true
}
trap cleanup EXIT

assert_contains() {
    local text="$1" expected="$2"
    case "${text}" in
        *"${expected}"*) ;;
        *) fail "expected to find '${expected}' in: ${text}" ;;
    esac
}

assert_not_contains() {
    local text="$1" unexpected="$2"
    case "${text}" in
        *"${unexpected}"*) fail "did not expect '${unexpected}' in: ${text}" ;;
        *) ;;
    esac
}

# How many lines of a text contain a word.
count_lines_with() {
    local text="$1" word="$2"
    printf '%s\n' "${text}" | grep -c -- "${word}" || true
}

current_step="starting the cluster"
started="$("${cluster}" start)" || fail "start failed: ${started}"
assert_contains "${started}" "The cluster is ready"
assert_contains "${started}" "--config ${state}/cluster.toml"

current_step="asking for the status of a running cluster"
status="$("${cluster}" status)"
[[ "$(count_lines_with "${status}" " ready ")" == 5 ]] || fail "expected five ready processes: ${status}"
pids_while_running="$(cat "${state}"/*/pid)"

current_step="starting a cluster that already runs"
"${cluster}" start >/dev/null || fail "a second start failed"
[[ "$(cat "${state}"/*/pid)" == "${pids_while_running}" ]] || fail "a second start replaced running processes"

current_step="creating a stream"
created="$("${cluster}" bita stream create demo)" || fail "stream create failed"
assert_contains "${created}" "demo"

current_step="appending records"
appended="$("${cluster}" bita stream append demo "first record" "second record")" || fail "append failed"
assert_contains "${appended}" "appended 2 records to demo"

current_step="appending records from standard input"
appended="$(printf 'third record\n' | "${cluster}" bita stream append demo)" || fail "append from stdin failed"
assert_contains "${appended}" "appended 1 record"

current_step="reading the records back"
read_back="$("${cluster}" bita stream read demo)" || fail "read failed"
assert_contains "${read_back}" "first record"
assert_contains "${read_back}" "second record"
assert_contains "${read_back}" "third record"

current_step="listing the replicas"
replicas="$("${cluster}" bita replica list)" || fail "replica list failed"
[[ "$(count_lines_with "${replicas}" " yes ")" == 3 ]] || fail "expected three live replicas: ${replicas}"

current_step="asking which controller leads"
leader="$("${cluster}" bita controller status)" || fail "controller status failed"
assert_contains "${leader}" "leading"

current_step="reading a log"
controller_log="$("${cluster}" logs controller)" || fail "logs failed"
[[ -n "${controller_log}" ]] || fail "the controller wrote no log"

current_step="asking for the log of a process that does not exist"
if "${cluster}" logs replica-9 >/dev/null 2>&1; then
    fail "logs accepted a process that does not exist"
fi

current_step="stopping the cluster"
stopped="$("${cluster}" stop 2>&1)" || fail "stop failed: ${stopped}"
assert_not_contains "${stopped}" "was killed"
for pid in ${pids_while_running}; do
    if kill -0 "${pid}" 2>/dev/null && [[ "$(ps -o stat= -p "${pid}" 2>/dev/null)" != Z* ]]; then
        fail "process ${pid} still runs after stop"
    fi
done
status="$("${cluster}" status)"
[[ "$(count_lines_with "${status}" " stopped ")" == 5 ]] || fail "expected five stopped processes: ${status}"

current_step="starting the cluster again on its old state"
"${cluster}" start >/dev/null || fail "restart failed"
read_back="$("${cluster}" bita stream read demo)" || fail "read after restart failed"
assert_contains "${read_back}" "third record"

current_step="resetting the cluster"
"${cluster}" reset >/dev/null || fail "reset failed"
[[ ! -e "${state}" ]] || fail "reset left ${state} behind"

current_step="resetting a directory that is not a cluster"
mkdir -p "${state}"
echo "precious" >"${state}/not-a-cluster"
if "${cluster}" reset >/dev/null 2>&1; then
    fail "reset deleted a directory that is not the state of a cluster"
fi
[[ -f "${state}/not-a-cluster" ]] || fail "reset deleted a file of a directory that is not a cluster"

current_step="asking for something the script does not do"
if "${cluster}" explode >/dev/null 2>&1; then
    fail "an unknown command was accepted"
fi

echo "PASSED"
