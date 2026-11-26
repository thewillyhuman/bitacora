#!/usr/bin/env bash
# Runs the bita tool or the benchmark tool in a pod next to a bitacora
# cluster on Kubernetes, prints what it printed, and removes the pod.
#
# Usage: development/kubernetes/run.sh bita <arguments of bita>...
#        development/kubernetes/run.sh bench <arguments of bita-bench>...
#
# The tool is told where etcd is; give it everything else, for example
# `run.sh bita replica list` or `run.sh bench append demo --create`. A tool
# that must reach the object store, to read a segment that the replicas no
# longer hold or to work with metadata snapshots, needs more than etcd: name
# a cluster configuration file and the Secret with the object store's
# credentials (see the environment below).
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
#   BITA_CLUSTER_CONFIG       A cluster configuration file on this machine, as
#                             docs/cli.md describes it. The tool is started
#                             with it instead of with the etcd members: the
#                             file is the only way to name an object store.
#   BITA_CREDENTIALS_SECRET   A Secret of the namespace whose keys become the
#                             environment of the tool, such as the access key
#                             of the object store.
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
readonly cluster_config="${BITA_CLUSTER_CONFIG:-}"
readonly credentials_secret="${BITA_CREDENTIALS_SECRET:-}"
[[ -z "${cluster_config}" || -r "${cluster_config}" ]] ||
    fail "BITA_CLUSTER_CONFIG names ${cluster_config}, which cannot be read"
readonly pod="${program}-$$-${RANDOM}"

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

# The arguments of the tool as a JSON list: where the cluster is, then what
# the caller gave. With a configuration file the etcd members are left out:
# an option would win over the file.
arguments=""
if [[ -n "${cluster_config}" ]]; then
    arguments+="$(json_string --config), $(json_string /etc/bitacora/cluster.toml), "
else
    for endpoint in ${etcd_endpoints}; do
        arguments+="$(json_string --etcd-endpoint), $(json_string "${endpoint}"), "
    done
fi
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

# What only a tool with a configuration file or with credentials has. The
# file travels as a ConfigMap of the pod's name, which lives as long as the
# pod.
environment=""
if [[ -n "${credentials_secret}" ]]; then
    environment="envFrom:
        - secretRef: {name: ${credentials_secret}}"
fi
config_mount=""
config_volume=""
if [[ -n "${cluster_config}" ]]; then
    config_mount="volumeMounts:
        - {name: cluster-config, mountPath: /etc/bitacora, readOnly: true}"
    config_volume="volumes:
    - name: cluster-config
      configMap: {name: ${pod}}"
fi

# Whatever the script made in the cluster is removed however it ends, also
# when it is interrupted. The function is called by the trap below, which the
# linter does not follow.
# shellcheck disable=SC2329
remove_what_was_made() {
    in_cluster delete pod "${pod}" --wait=false >/dev/null 2>&1 || true
    if [[ -n "${cluster_config}" ]]; then
        in_cluster delete configmap "${pod}" --wait=false >/dev/null 2>&1 || true
    fi
}
trap remove_what_was_made EXIT

if [[ -n "${cluster_config}" ]]; then
    in_cluster create configmap "${pod}" --from-file=cluster.toml="${cluster_config}" >/dev/null
fi
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
      ${environment}
      ${config_mount}
  ${config_volume}
MANIFEST

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
