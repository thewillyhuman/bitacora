#!/usr/bin/env bash
# Tests development/kubernetes/run.sh without a cluster: a stand-in for
# kubectl writes down how it was called and what it was given, and answers
# with the pod phase the test chose.
#
# Usage: run_test.sh <run.sh>
set -euo pipefail

run="${1:?usage: run_test.sh <run.sh>}"
scratch="${TEST_TMPDIR:?the test needs a temporary directory}"

# The stand-in: one line per call, the manifest of an `apply`, the phase of a
# `get`, a report for `logs`.
calls="${scratch}/calls"
manifest="${scratch}/manifest"
phase="${scratch}/phase"
cat >"${scratch}/kubectl" <<STANDIN
#!/bin/sh
echo "\$*" >>"${calls}"
case " \$* " in
    *" apply "*) cat >"${manifest}" ;;
    *" get pod "*) cat "${phase}" ;;
    *" logs "*) echo "the report of the tool" ;;
esac
STANDIN
chmod +x "${scratch}/kubectl"
export KUBECTL="${scratch}/kubectl"

fail() {
    echo "FAILED: $*" >&2
    exit 1
}

# Starts a test from nothing: no calls, no manifest, a pod that succeeds, and
# only the two settings the script requires.
fresh() {
    : >"${calls}"
    : >"${manifest}"
    echo Succeeded >"${phase}"
    unset BITA_KUBE_NAMESPACE BITA_IMAGE_REPOSITORY BITA_ETCD_ENDPOINTS BITA_RUN_TIMEOUT_SECONDS
    export BITA_KUBE_CONTEXT=test-context BITA_VERSION=1.2.3
}

expect_in_manifest() {
    local phrase
    for phrase in "$@"; do
        grep -q -F -- "${phrase}" "${manifest}" || fail "the pod lacks '${phrase}': $(cat "${manifest}")"
    done
}

echo "--- bita runs in a pod of its image, with etcd first and the arguments after"
fresh
output="$("${run}" bita replica list)" || fail "a pod that succeeded was reported as failed"
expect_in_manifest "image: ghcr.io/thewillyhuman/bitacora/bita:1.2.3" \
    '"--etcd-endpoint", "http://etcd-0.etcd-headless:2379"' \
    '"http://etcd-2.etcd-headless:2379", "replica", "list"]' \
    "runAsNonRoot: true" "readOnlyRootFilesystem: true"
[[ "${output}" == "the report of the tool" ]] || fail "the output is not the tool's: ${output}"
if grep -q podAntiAffinity "${manifest}"; then
    fail "the bita pod avoids replicas, which only a benchmark needs"
fi

echo "--- every call names the context and the namespace"
while IFS= read -r call; do
    [[ "${call}" == "--context test-context --namespace bitacora "* ]] ||
        fail "a call does not start with the context and the namespace: ${call}"
done <"${calls}"
[[ -s "${calls}" ]] || fail "kubectl was never called"

echo "--- the pod is removed afterwards"
grep -q " delete pod bita-" "${calls}" || fail "the pod was not removed: $(cat "${calls}")"

echo "--- a benchmark gets CPUs of its own on a node without a replica"
fresh
"${run}" bench append demo --create >/dev/null || fail "the benchmark was reported as failed"
expect_in_manifest "image: ghcr.io/thewillyhuman/bitacora/bita-bench:1.2.3" "podAntiAffinity" \
    "matchLabels: {app.kubernetes.io/component: replica}" 'cpu: "2"' '"append", "demo", "--create"]'

echo "--- an argument with spaces and quotes stays one argument"
fresh
"${run}" bita stream append demo 'say "hi" there' >/dev/null
expect_in_manifest '"stream", "append", "demo", "say \"hi\" there"]'

echo "--- the namespace, the repository and the etcd members can be changed"
fresh
BITA_KUBE_NAMESPACE=storage BITA_IMAGE_REPOSITORY=registry.example.org/team \
    BITA_ETCD_ENDPOINTS="http://etcd.example.org:2379" "${run}" bita stream list >/dev/null
expect_in_manifest "image: registry.example.org/team/bita:1.2.3" \
    '["--etcd-endpoint", "http://etcd.example.org:2379", "stream", "list"]'
grep -q -- "--context test-context --namespace storage apply" "${calls}" || fail "the namespace was not used"

echo "--- a tool that failed is reported, with what it printed"
fresh
echo Failed >"${phase}"
exit_code=0
output="$("${run}" bita stream describe missing)" || exit_code=$?
((exit_code == 1)) || fail "a failed pod did not give exit code 1 but ${exit_code}"
[[ "${output}" == "the report of the tool" ]] || fail "the output of a failed tool was dropped"

echo "--- a tool that does not finish is given up on and its pod removed"
fresh
echo Pending >"${phase}"
exit_code=0
message="$(BITA_RUN_TIMEOUT_SECONDS=1 "${run}" bench append demo 2>&1 >/dev/null)" || exit_code=$?
((exit_code == 124)) || fail "a pod that never finished did not give exit code 124 but ${exit_code}"
[[ "${message}" == *"did not finish within 1 seconds; its pod was Pending"* ]] || fail "no reason given: ${message}"
grep -q " delete pod bita-bench-" "${calls}" || fail "the pod of a tool that was given up on stayed"

echo "--- without a context or a version nothing is started"
for missing in BITA_KUBE_CONTEXT BITA_VERSION; do
    fresh
    unset "${missing}"
    exit_code=0
    message="$("${run}" bita replica list 2>&1)" || exit_code=$?
    ((exit_code == 2)) || fail "a missing ${missing} did not give exit code 2 but ${exit_code}"
    [[ "${message}" == *"${missing} must name"* ]] || fail "the missing setting is not named: ${message}"
    [[ ! -s "${calls}" ]] || fail "kubectl was called without ${missing}"
done

echo "--- what is neither bita nor bench is refused"
fresh
exit_code=0
"${run}" replica list >/dev/null 2>&1 || exit_code=$?
((exit_code == 2)) || fail "an unknown tool did not give exit code 2 but ${exit_code}"
[[ ! -s "${calls}" ]] || fail "kubectl was called for an unknown tool"

echo "--- help is printed"
help="$("${run}" --help)"
[[ "${help}" == *"Usage: development/kubernetes/run.sh bita"* ]] || fail "no usage text: ${help}"

echo "PASSED"
