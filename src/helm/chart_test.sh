#!/usr/bin/env bash
# Tests the Helm chart without a cluster. It lints the chart and renders it
# with its default values and with every values file of its ci/ directory,
# then checks what was rendered: the configuration files are accepted by the
# daemons themselves, every variable they refer to is set by the pod, and
# the manifests have the replicas, probes, spreading, budgets and security
# settings an operator relies on. It also checks values.yaml against the
# templates, and that values which cannot work are refused.
#
# Usage: chart_test.sh
#
# Environment (Bazel sets all of it, see BUILD.bazel):
#   HELM                    the helm program
#   CHART                   the Chart.yaml of the chart to check
#   MODULE                  the MODULE.bazel of the repository
#   DOCKERFILE              the Dockerfile of the images the chart runs
#   REPLICA_CONFIG          the example configuration of bita-replica
#   CONTROLLER_CONFIG       the example configuration of bita-controller
#   BITA_REPLICA_BINARY     the two daemons, as Bazel builds them
#   BITA_CONTROLLER_BINARY
#
# This script is full of "${NAME}" in single quotes on purpose: that is text
# of the configuration files the chart renders, not a variable of the script.
# shellcheck disable=SC2016
set -euo pipefail

: "${HELM:?}" "${CHART:?}" "${MODULE:?}" "${DOCKERFILE:?}" "${REPLICA_CONFIG:?}" "${CONTROLLER_CONFIG:?}"
: "${BITA_REPLICA_BINARY:?}" "${BITA_CONTROLLER_BINARY:?}"

chart="$(dirname "${CHART}")"
scratch="${TEST_TMPDIR:?the test needs a temporary directory}"

# helm keeps its own files out of the home directory of whoever runs the test.
export HELM_CACHE_HOME="${scratch}/helm/cache"
export HELM_CONFIG_HOME="${scratch}/helm/config"
export HELM_DATA_HOME="${scratch}/helm/data"

# The namespace and release name the chart is rendered with, and the name of
# the chart, under which helm writes what it renders.
readonly namespace="storage"
readonly release="bitacora"
readonly chart_name="bitacora"

# The daemons give themselves this long to shut down; a pod must get longer.
readonly daemon_shutdown_seconds=10

# A UID as Kubernetes gives one to every pod it creates.
readonly pod_uid="0f2b8d5e-6c1a-4e3f-9b7d-2a4c6e8f0b1d"

failures=0
current_case=""
# The directory with one rendered file per template of the current case.
rendered=""

problem() {
    echo "FAILED (${current_case}): $*" >&2
    failures=$((failures + 1))
}

# ---------------------------------------------------------------------------
# Rendering.
# ---------------------------------------------------------------------------

# Lints and renders the chart for one case. Further arguments go to helm.
# Each template is rendered to a file of its own, and next to it a copy
# without indentation, which is what the checks below read: `has` then means
# "this file has exactly this line, however deep".
render() {
    current_case="$1"
    shift
    local output="${scratch}/rendered/${current_case}" file
    rendered="${output}/${chart_name}/templates"
    if ! "${HELM}" lint --strict "${chart}" "$@" >"${scratch}/lint.log" 2>&1; then
        problem "helm lint --strict refuses the chart: $(cat "${scratch}/lint.log")"
        return 1
    fi
    if ! "${HELM}" template "${release}" "${chart}" --namespace "${namespace}" --output-dir "${output}" "$@" \
        >"${scratch}/template.log" 2>&1; then
        problem "helm template fails: $(cat "${scratch}/template.log")"
        return 1
    fi
    for file in "${rendered}"/*.yaml; do
        sed 's/^ *//' "${file}" >"${file}.lines"
    done
}

# Whether a rendered template has a line, indentation aside.
has() {
    [[ -f "${rendered}/$1.lines" ]] && grep -q -x -F -- "$2" "${rendered}/$1.lines"
}

# Requires a rendered template to have each of the lines given.
expect() {
    local template="$1" line
    shift
    for line in "$@"; do
        has "${template}" "${line}" || problem "${template} has no line '${line}'"
    done
}

# Requires a rendered template not to mention a phrase at all.
expect_absent() {
    local template="$1" phrase="$2"
    if [[ -f "${rendered}/${template}" ]] && grep -q -F -- "${phrase}" "${rendered}/${template}"; then
        problem "${template} mentions '${phrase}'"
    fi
}

# Prints the number that follows a key in a rendered template.
number_of() {
    sed -n -E "/^$2: ([0-9]+)$/{s//\1/p;q;}" "${rendered}/$1.lines"
}

# Writes the configuration file a ConfigMap carries to a file of its own.
extract_config() {
    local template="$1" key="$2" target="$3"
    awk -v header="  ${key}: |" '
        in_file && /^    / { print substr($0, 5); next }
        in_file && /^ *$/ { print ""; next }
        in_file { in_file = 0 }
        $0 == header { in_file = 1 }
    ' "${rendered}/${template}" >"${target}"
    [[ -s "${target}" ]] || problem "${template} carries no ${key}"
}

# ---------------------------------------------------------------------------
# Checks every case must pass.
# ---------------------------------------------------------------------------

# Checks the configuration of one daemon as its pod would see it: every
# variable the file refers to is set by the pod, the daemon accepts the file
# with those variables, and refuses it when one of them is missing.
check_config() {
    local daemon="$1" program="$2" config_template="$3" key="$4" workload="$5"
    local config="${scratch}/${current_case}-${key}" variable variables="" first_variable=""
    extract_config "${config_template}" "${key}" "${config}"

    while IFS= read -r variable; do
        variables+=" ${variable}"
        has "${workload}" "- name: ${variable}" ||
            problem "${key} refers to ${variable}, which the ${daemon} pod does not set"
    done < <(grep -v '^ *#' "${config}" | grep -o -E '\$\{[A-Z_]+\}' | tr -d '${}' | LC_ALL=C sort -u)
    first_variable="${variables# }"
    first_variable="${first_variable%% *}"

    # The values a pod would have. A variable the file does not use is harmless.
    if ! env BITA_POD_NAME="${release}-${daemon}-0" BITA_POD_UID="${pod_uid}" BITA_NODE_NAME=node-a BITA_ZONE=zone-a \
        "${program}" --config "${config}" --check-config >"${scratch}/check.log" 2>&1; then
        problem "bita-${daemon} refuses the configuration the chart renders: $(cat "${scratch}/check.log")"
    fi
    # A pod without one of the variables must not start as somebody else.
    if [[ -n "${first_variable}" ]] && (
        export BITA_POD_NAME=pod BITA_POD_UID="${pod_uid}" BITA_NODE_NAME=node-a BITA_ZONE=zone-a
        unset "${first_variable}"
        "${program}" --config "${config}" --check-config >/dev/null 2>&1
    ); then
        problem "bita-${daemon} accepts its configuration without ${first_variable}"
    fi
}

# Checks one workload: probes on the paths the daemons serve, on a port the
# container names; time to stop; and the security settings.
check_workload() {
    local daemon="$1" workload="$2" grace image_user
    expect "${workload}" "path: /healthz" "- name: metrics"
    has "${workload}" "path: /readyz" || problem "${workload} has no readiness probe on /readyz"
    has "${workload}" "startupProbe:" || problem "${workload} has no startup probe"
    has "${workload}" "livenessProbe:" || problem "${workload} has no liveness probe"

    grace="$(number_of "${workload}" terminationGracePeriodSeconds)"
    if [[ -z "${grace}" ]]; then
        problem "${workload} does not say how long a ${daemon} may take to stop"
    elif ((grace <= daemon_shutdown_seconds)); then
        problem "a ${daemon} is given ${grace} seconds to stop, and needs up to ${daemon_shutdown_seconds}"
    fi

    expect "${workload}" "runAsNonRoot: true" "readOnlyRootFilesystem: true" "allowPrivilegeEscalation: false" \
        "automountServiceAccountToken: false"
    # The user of the pod must be the user the image was built for.
    image_user="$(sed -n -E 's/^USER ([0-9]+):[0-9]+$/\1/p' "${DOCKERFILE}" | tail -n 1)"
    has "${workload}" "runAsUser: ${image_user}" ||
        problem "a ${daemon} runs as user $(number_of "${workload}" runAsUser), but the images run as ${image_user}"
    has "${workload}" "fsGroup: ${image_user}" || problem "${workload} does not give volumes to group ${image_user}"
}

# Checks that the ports of a pod and of its Service are the ports its
# configuration listens on.
check_ports() {
    local workload="$1" service="$2" config="$3" setting port
    shift 3
    for setting in "$@"; do
        port="$(sed -n -E "s/^${setting} = \".*:([0-9]+)\"$/\1/p" "${config}")"
        has "${workload}" "containerPort: ${port}" ||
            problem "${workload} does not open port ${port}, where ${setting} listens"
        has "${service}" "port: ${port}" || problem "${service} does not expose port ${port} of ${setting}"
    done
}

# Checks what must hold however the chart is configured.
check_always() {
    local replica_config="${scratch}/${current_case}-replica.toml"
    local controller_config="${scratch}/${current_case}-controller.toml"

    check_config replica "${BITA_REPLICA_BINARY}" replica-configmap.yaml replica.toml replica-statefulset.yaml
    check_config controller "${BITA_CONTROLLER_BINARY}" controller-configmap.yaml controller.toml \
        controller-deployment.yaml
    check_workload replica replica-statefulset.yaml
    check_workload controller controller-deployment.yaml
    check_ports replica-statefulset.yaml replica-service.yaml "${replica_config}" listen_address \
        metrics_listen_address
    check_ports controller-deployment.yaml controller-service.yaml "${controller_config}" metrics_listen_address

    # Never two replicas on one node, and never more than one taken down.
    if ! has replica-statefulset.yaml "requiredDuringSchedulingIgnoredDuringExecution:" ||
        ! has replica-statefulset.yaml "- topologyKey: kubernetes.io/hostname"; then
        problem "the chart does not keep replicas on different nodes"
    fi
    has replica-poddisruptionbudget.yaml "maxUnavailable: 1" ||
        problem "the chart lets more than one replica be taken down at a time"
    has controller-poddisruptionbudget.yaml "maxUnavailable: 1" ||
        problem "the chart lets more than one controller be taken down at a time"

    # A replica keeps its name and its volume, and advertises the name the
    # headless Service gives it.
    expect replica-statefulset.yaml "serviceName: ${release}-replica" "mountPath: /var/lib/bitacora" \
        "- /etc/bitacora/replica.toml" "mountPath: /etc/bitacora"
    expect replica-service.yaml "name: ${release}-replica" "clusterIP: None" "publishNotReadyAddresses: true"
    expect controller-deployment.yaml "- /etc/bitacora/controller.toml" "mountPath: /etc/bitacora"
    grep -q -F "advertised_address = \"\${BITA_POD_NAME}.${release}-replica.${namespace}.svc." "${replica_config}" ||
        problem "a replica does not advertise its name in the headless Service"
    grep -q -x -F 'data_directory = "/var/lib/bitacora/replica"' "${replica_config}" ||
        problem "a replica does not keep its data in its volume"

    # A data directory keeps the identity it was first opened with, so the
    # identity the chart proposes only counts on an empty volume. It must
    # then be one no replica ever had: a replica that lost its volume holds
    # none of the copies its old identity held, and must not answer for it.
    # The pod's name alone comes back with the new volume; its UID does not.
    grep -q -x -F 'replica_id = "${BITA_POD_NAME}-${BITA_POD_UID}"' "${replica_config}" ||
        problem "a replica that lost its volume would come back under its old identity"
    expect replica-statefulset.yaml "fieldPath: metadata.uid"

    # The chart never creates a Secret, and a ConfigMap never has a credential.
    if grep -l -E '^kind: Secret$' "${rendered}"/*.yaml >/dev/null 2>&1; then
        problem "the chart creates a Secret"
    fi
    expect_absent replica-configmap.yaml AWS_
    expect_absent controller-configmap.yaml AWS_
}

# ---------------------------------------------------------------------------
# The cases.
# ---------------------------------------------------------------------------

# Checks that every setting the chart writes with its default value has the
# value of the daemon's own example configuration, so that installing the
# chart without values means running the daemons with their defaults.
check_defaults_match() {
    local config="$1" example="$2" line
    while IFS= read -r line; do
        grep -q -x -F -- "${line}" "${example}" ||
            problem "the chart's default '${line}' is not what $(basename "$(dirname "${example}")")/example.toml has"
    done < <(grep -E '^[a-z_]+ = [0-9]+$|^(level|format) = ' "${config}")
}

case_defaults() {
    local chart_version
    render defaults || return 0
    check_always
    chart_version="$(sed -n -E 's/^appVersion: "(.*)"$/\1/p' "${CHART}")"
    expect replica-statefulset.yaml "replicas: 3" "podManagementPolicy: Parallel" "storage: 100Gi" \
        "image: \"bitacora/bita-replica:${chart_version}\"" \
        "topologyKey: topology.kubernetes.io/zone" "whenUnsatisfiable: DoNotSchedule" \
        "fieldPath: metadata.labels['topology.kubernetes.io/zone']" "fieldPath: spec.nodeName"
    expect controller-deployment.yaml "replicas: 2" "image: \"bitacora/bita-controller:${chart_version}\""
    expect replica-configmap.yaml 'failure_domains = ["${BITA_ZONE}", "${BITA_NODE_NAME}"]' \
        'etcd_endpoints = ["http://etcd:2379"]'
    expect_absent replica-configmap.yaml "object_store"
    expect_absent replica-statefulset.yaml "storageClassName"
    [[ ! -f "${rendered}/servicemonitor.yaml" ]] || problem "a ServiceMonitor is created without being asked for"
    check_defaults_match "${scratch}/defaults-replica.toml" "${REPLICA_CONFIG}"
    check_defaults_match "${scratch}/defaults-controller.toml" "${CONTROLLER_CONFIG}"

    # Kubernetes sets the variable of a label the pod does not have to the
    # empty string. A replica must refuse to start without a zone, not join
    # the cluster as if it had one.
    if env BITA_POD_NAME=pod BITA_POD_UID="${pod_uid}" BITA_NODE_NAME=node-a BITA_ZONE= "${BITA_REPLICA_BINARY}" \
        --config "${scratch}/defaults-replica.toml" --check-config >/dev/null 2>&1; then
        problem "a replica starts although its node has no zone"
    fi
}

case_s3_compatible() {
    local workload
    render s3-compatible --values "${chart}/ci/s3-compatible-values.yaml" || return 0
    check_always
    for workload in replica-configmap.yaml controller-configmap.yaml; do
        expect "${workload}" "[cluster.object_store]" 'kind = "s3"' 'bucket = "bitacora-segments"' \
            'endpoint = "https://s3.example.org"' 'key_prefix = "production/"' \
            'etcd_endpoints = ["http://etcd-0.etcd:2379","http://etcd-1.etcd:2379","http://etcd-2.etcd:2379"]' \
            'etcd_key_prefix = "/bitacora/production/"'
    done
    # The access key reaches both daemons from the operator's Secret.
    for workload in replica-statefulset.yaml controller-deployment.yaml; do
        expect "${workload}" "- name: AWS_ACCESS_KEY_ID" "- name: AWS_SECRET_ACCESS_KEY" "secretKeyRef:" \
            "name: bitacora-s3" "key: access-key" "key: secret-key"
    done
}

case_larger_cluster() {
    render larger-cluster --values "${chart}/ci/larger-cluster-values.yaml" || return 0
    check_always
    expect replica-statefulset.yaml "replicas: 5" "storage: 500Gi" "storageClassName: local-nvme" \
        'image: "registry.example.org/storage/bita-replica:1.4.0"' "- name: registry-example-org" \
        "terminationGracePeriodSeconds: 60" 'cpu: "2"' "memory: 4Gi"
    expect controller-deployment.yaml "replicas: 3" 'image: "registry.example.org/storage/bita-controller:1.4.0"' \
        "- name: registry-example-org"
    expect controller-configmap.yaml "replica_lost_after_seconds = 600"
}

case_service_monitor() {
    render service-monitor --values "${chart}/ci/service-monitor-values.yaml" || return 0
    check_always
    expect servicemonitor.yaml "kind: ServiceMonitor" "- port: metrics" "path: /metrics" "interval: 15s" \
        "release: prometheus" "- ${namespace}"
    # The monitor finds the pods through a port called `metrics` on both Services.
    expect replica-service.yaml "- name: metrics"
    expect controller-service.yaml "- name: metrics"
}

case_single_zone() {
    render single-zone --values "${chart}/ci/single-zone-values.yaml" || return 0
    check_always
    expect replica-configmap.yaml 'failure_domains = ["${BITA_NODE_NAME}"]'
    expect_absent replica-statefulset.yaml "topology.kubernetes.io/zone"
    expect_absent replica-statefulset.yaml "BITA_ZONE"
    # Without a zone the replica needs no zone to start.
    if ! env -u BITA_ZONE BITA_POD_NAME=pod BITA_POD_UID="${pod_uid}" BITA_NODE_NAME=node-a "${BITA_REPLICA_BINARY}" \
        --config "${scratch}/single-zone-replica.toml" --check-config >/dev/null 2>&1; then
        problem "a replica that is not zone aware still needs a zone"
    fi
}

# Requires helm to refuse to render the chart with the options given, and
# to say why.
expect_refusal() {
    local expected="$1" message
    shift
    if message="$("${HELM}" template "${release}" "${chart}" --namespace "${namespace}" "$@" 2>&1)"; then
        problem "helm template accepts $*"
    elif [[ "${message}" != *"${expected}"* ]]; then
        problem "the refusal of $* does not say '${expected}': ${message}"
    fi
}

# Checks that values which cannot work stop the installation with a
# message, instead of producing pods that cannot start.
case_refused_values() {
    current_case="refused values"
    expect_refusal "objectStore.s3.bucket is required" --set objectStore.kind=s3
    expect_refusal "objectStore.kind must be none or s3, not gcs" --set objectStore.kind=gcs
    expect_refusal "etcd.endpoints must name at least one etcd endpoint" --set-json 'etcd.endpoints=[]'
}

# ---------------------------------------------------------------------------
# values.yaml and Chart.yaml.
# ---------------------------------------------------------------------------

# Prints every key of values.yaml as "<kind> <documented> <path>": kind is
# `leaf` for a key with a value and `parent` for a key with keys below it,
# documented is `yes` when a comment stands directly above the key.
values_keys() {
    awk '
        /^ *$/ { above_is_comment = 0; next }
        /^ *#/ { above_is_comment = 1; next }
        /^ *- / { kind[count] = "leaf"; above_is_comment = 0; next }
        {
            level = (match($0, /[^ ]/) - 1) / 2
            key = $0
            sub(/^ */, "", key)
            value = key
            sub(/:.*$/, "", key)
            sub(/^[^:]*: ?/, "", value)
            name[level] = key
            path = name[0]
            for (i = 1; i <= level; i++) path = path "." name[i]
            count++
            paths[count] = path
            kind[count] = (value == "" ? "parent" : "leaf")
            documented[count] = (above_is_comment ? "yes" : "no")
            above_is_comment = 0
        }
        END { for (i = 1; i <= count; i++) print kind[i], documented[i], paths[i] }
    ' "${chart}/values.yaml"
}

# Checks values.yaml against the templates, both ways: every value is
# explained and used, and every value a template uses exists. A template that
# misspells a value renders an empty string without any complaint from helm.
# The templates are named one by one, not searched with `grep -r`: under
# Bazel they are symbolic links, which a recursive grep does not follow.
check_values() {
    current_case="values.yaml"
    local kind documented path known_paths=" " used pattern
    while read -r kind documented path; do
        known_paths+="${path} "
        if [[ "${kind}" != leaf ]]; then
            continue
        fi
        [[ "${documented}" == yes ]] || problem "${path} is not documented: no comment stands above it"
        pattern="\\.Values\\.${path//./\\.}([^A-Za-z0-9.]|\$)"
        grep -q -E -- "${pattern}" "${chart}/templates"/* || problem "${path} is not used by any template"
    done < <(values_keys)

    while IFS= read -r used; do
        [[ "${known_paths}" == *" ${used} "* ]] ||
            problem "a template uses .Values.${used}, which values.yaml does not have"
    done < <(grep -h -o -E '\.Values\.[A-Za-z0-9.]+' "${chart}/templates"/* | sed 's/^\.Values\.//' | LC_ALL=C sort -u)
}

# Checks what hack/create_release.sh needs from Chart.yaml: one `version`
# line and one `appVersion` line to rewrite, both equal to the version of
# the repository.
check_chart_version() {
    current_case="Chart.yaml"
    local module_version chart_version app_version
    module_version="$(sed -n -E '/^ *version = "([^"]+)",?$/{s//\1/p;q;}' "${MODULE}")"
    chart_version="$(sed -n -E 's/^version: (.+)$/\1/p' "${CHART}")"
    app_version="$(sed -n -E 's/^appVersion: "(.+)"$/\1/p' "${CHART}")"
    [[ "${chart_version}" == "${module_version}" ]] ||
        problem "version is '${chart_version}', but MODULE.bazel says ${module_version}"
    [[ "${app_version}" == "${module_version}" ]] ||
        problem "appVersion is '${app_version}', but MODULE.bazel says ${module_version}"
}

check_chart_version
check_values
case_defaults
case_s3_compatible
case_larger_cluster
case_service_monitor
case_single_zone
case_refused_values

if ((failures > 0)); then
    echo "${failures} problems found" >&2
    exit 1
fi
echo "PASSED"
