#!/usr/bin/env bash
# Runs chart_test.sh on a copy of the chart that was broken on purpose, one
# mistake of each kind the test looks for. //src/helm:chart_rejection_test
# requires that run to fail and to name every mistake; BUILD.bazel lists
# what it must say.
#
# Usage: broken_chart_test.sh <chart_test.sh>
#
# The environment is that of chart_test.sh; CHART names the sound chart.
set -euo pipefail

chart_test="${1:?usage: broken_chart_test.sh <chart_test.sh>}"
sound_chart="$(dirname "${CHART:?}")"
broken_chart="${TEST_TMPDIR:?the test needs a temporary directory}/broken/bitacora"

mkdir -p "$(dirname "${broken_chart}")"
cp -R -L "${sound_chart}" "${broken_chart}"
chmod -R u+w "${broken_chart}"

# Rewrites one file of the broken chart with a sed expression. A mistake
# that no longer applies, because the chart changed, must not go unnoticed.
break_file() {
    local file="${broken_chart}/$1" expression="$2"
    sed -e "${expression}" "${file}" >"${file}.new"
    if cmp -s "${file}" "${file}.new"; then
        echo "broken_chart_test: '${expression}' changes nothing in $1" >&2
        exit 2
    fi
    mv "${file}.new" "${file}"
}

# A probe on a path the daemons do not serve.
break_file templates/_helpers.tpl 's|path: /readyz|path: /ready|'
# A pod that runs as another user than the image was built for.
break_file templates/_helpers.tpl 's|runAsUser: 65532|runAsUser: 1000|'
# Two replicas may be taken down at once.
break_file templates/replica-poddisruptionbudget.yaml 's|maxUnavailable: 1|maxUnavailable: 2|'
# The pod no longer sets a variable its configuration refers to.
break_file templates/replica-statefulset.yaml 's|name: BITA_NODE_NAME|name: BITA_NODE|'
# A replica is named after its pod alone, so one that lost its volume comes
# back as the replica it was. The "${NAME}" here is text of the configuration
# file, not a variable of this script.
# shellcheck disable=SC2016
break_file templates/replica-configmap.yaml 's|replica_id = "${BITA_POD_NAME}-${BITA_POD_UID}"|replica_id = "${BITA_POD_NAME}"|'
# Replicas may share a node.
break_file templates/replica-statefulset.yaml 's|requiredDuringScheduling|preferredDuringScheduling|'
# A template misspells a value.
break_file templates/replica-statefulset.yaml 's|\.Values\.replica\.replicas |.Values.replica.replicaCount |'
# A controller is killed before it can stop.
break_file templates/controller-deployment.yaml 's|^\(      terminationGracePeriodSeconds:\) .*$|\1 5|'
# A setting the controller does not have.
break_file templates/controller-configmap.yaml 's|pass_interval_milliseconds|pass_interval_ms|'
# A default that is not the daemon's.
break_file values.yaml 's|^  scrubIntervalSeconds: 604800$|  scrubIntervalSeconds: 3600|'
# A value nobody explains and nothing uses.
break_file values.yaml 's|^  priorityClassName: ""$|  priorityClassName: ""\
  spare: 1|'
# A version that is not the repository'"'"'s.
break_file Chart.yaml 's|^appVersion: .*$|appVersion: "9.9.9"|'

CHART="${broken_chart}/Chart.yaml" exec "${chart_test}"
