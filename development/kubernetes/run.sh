#!/usr/bin/env bash
# Runs the bita tool or the benchmark tool in a pod next to a bitacora
# cluster on Kubernetes, prints what it printed, and removes the pod.
#
# Usage: development/kubernetes/run.sh bita <arguments of bita>...
#        development/kubernetes/run.sh bench <arguments of bita-bench>...
#
# The tool is told where etcd is; give it everything else, for example
# `run.sh bita replica list` or `run.sh bench append demo --create`.
#
# A benchmark pod asks for two CPUs and refuses to share a node with a
# replica: the tool is part of what it measures. Leave one node without a
# replica, or the pod stays pending until the time is up.
#
# The script exits with 0 if the tool succeeded, with 1 if it failed, and
# with 124 if it did not finish in time.
#
# Environment:
#   BITA_KUBE_CONTEXT         The kubectl context of the cluster. Required, and
#                             passed to every call: the script never uses the
#                             context that happens to be current.
#   BITA_VERSION              The version of the images, such as 0.1.0-rc.1.
#                             Required.
#   BITA_KUBE_NAMESPACE       The namespace of the cluster. Default: bitacora
#   BITA_IMAGE_REPOSITORY     What comes before the image name.
#                             Default: ghcr.io/thewillyhuman/bitacora
#   BITA_ETCD_ENDPOINTS       The etcd members, separated by spaces. Default:
#                             the three members of etcd.yaml in this directory.
#   BITA_RUN_TIMEOUT_SECONDS  How long the tool may take. Default: 900
#   KUBECTL                   The kubectl to use. Default: kubectl
set -euo pipefail

fail() {
    echo "run: $*" >&2
    exit 2
}

usage() {
    sed -n '2,/^set -euo/{/^set -euo/d;s/^# \{0,1\}//;p;}' "${BASH_SOURCE[0]}"
}

case "${1:-}" in
    bita) program=bita ;;
    bench) program=bita-bench ;;
    -h | --help | help)
        usage
        exit 0
        ;;
    *)
        usage >&2
        exit 2
        ;;
esac
shift

readonly context="${BITA_KUBE_CONTEXT:-}"
readonly version="${BITA_VERSION:-}"
[[ -n "${context}" ]] || fail "BITA_KUBE_CONTEXT must name the kubectl context of the cluster"
[[ -n "${version}" ]] || fail "BITA_VERSION must name the version of the images"
readonly namespace="${BITA_KUBE_NAMESPACE:-bitacora}"
readonly image="${BITA_IMAGE_REPOSITORY:-ghcr.io/thewillyhuman/bitacora}/${program}:${version}"
readonly etcd_endpoints="${BITA_ETCD_ENDPOINTS:-http://etcd-0.etcd-headless:2379 http://etcd-1.etcd-headless:2379 http://etcd-2.etcd-headless:2379}"
readonly timeout_seconds="${BITA_RUN_TIMEOUT_SECONDS:-900}"
readonly kubectl="${KUBECTL:-kubectl}"

in_cluster() {
    "${kubectl}" --context "${context}" --namespace "${namespace}" "$@"
}

# Prints a string as a JSON string, which is also a YAML string.
json_string() {
    local text="$1"
    [[ "${text}" != *$'\n'* ]] || fail "an argument with a line break cannot be passed on"
    text="${text//\\/\\\\}"
    text="${text//\"/\\\"}"
    printf '"%s"' "${text}"
}

# The arguments of the tool as a JSON list: where etcd is, then what the
# caller gave.
arguments=""
for endpoint in ${etcd_endpoints}; do
    arguments+="$(json_string --etcd-endpoint), $(json_string "${endpoint}"), "
done
for argument in "$@"; do
    arguments+="$(json_string "${argument}"), "
done
arguments="[${arguments%, }]"

# What only a benchmark pod has: CPUs of its own, on a node without a replica.
placement=""
resources="requests: {cpu: 100m, memory: 64Mi}"
if [[ "${program}" == bita-bench ]]; then
    placement="affinity:
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        - topologyKey: kubernetes.io/hostname
          labelSelector:
            matchLabels: {app.kubernetes.io/component: replica}"
    resources="requests: {cpu: \"2\", memory: 512Mi}"
fi

readonly pod="${program}-$$-${RANDOM}"
in_cluster apply --filename - >/dev/null <<MANIFEST
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  labels: {app.kubernetes.io/name: ${program}}
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  securityContext:
    runAsNonRoot: true
    runAsUser: 65532
    runAsGroup: 65532
    seccompProfile: {type: RuntimeDefault}
  ${placement}
  containers:
    - name: ${program}
      image: ${image}
      args: ${arguments}
      resources:
        ${resources}
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities: {drop: [ALL]}
MANIFEST

# The pod is removed however the script ends, also when it is interrupted.
# The function is called by the trap below, which the linter does not follow.
# shellcheck disable=SC2329
remove_pod() {
    in_cluster delete pod "${pod}" --wait=false >/dev/null 2>&1 || true
}
trap remove_pod EXIT

phase=""
deadline=$((SECONDS + timeout_seconds))
while ((SECONDS < deadline)); do
    phase="$(in_cluster get pod "${pod}" --output jsonpath='{.status.phase}' 2>/dev/null || true)"
    case "${phase}" in
        Succeeded | Failed) break ;;
    esac
    sleep 2
done

in_cluster logs "${pod}" || true
case "${phase}" in
    Succeeded) exit 0 ;;
    Failed) exit 1 ;;
    *)
        echo "run: ${program} did not finish within ${timeout_seconds} seconds; its pod was ${phase:-not found}" >&2
        exit 124
        ;;
esac
