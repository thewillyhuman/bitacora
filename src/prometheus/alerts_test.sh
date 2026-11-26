#!/usr/bin/env bash
# Tests the alert rules without a Prometheus server. It lints them with the
# pinned pint, which parses every expression and template, then checks what
# a linter cannot know: that every metric and label value the rules use is
# in the metrics reference, that the page that describes the alerts lists
# exactly the alerts that exist, with their severities, and that the
# defaults of the daemons that the rules quote are still the defaults.
#
# Usage: alerts_test.sh
#
# Environment (Bazel sets all of it, see BUILD.bazel):
#   PINT                the pint program
#   PINT_CONFIG         its configuration, pint.hcl
#   RULES               the rule file to check
#   METRICS_REFERENCE   docs/operations/metrics.md
#   ALERTS_PAGE         docs/operations/alerts.md
#   METRICS_LIBRARY     hack/tools/metrics_reference.sh
#   REPLICA_CONFIG      the example configuration of bita-replica
#   CONTROLLER_CONFIG   the example configuration of bita-controller
set -euo pipefail

: "${PINT:?}" "${PINT_CONFIG:?}" "${RULES:?}" "${METRICS_REFERENCE:?}" "${ALERTS_PAGE:?}" "${METRICS_LIBRARY:?}"
: "${REPLICA_CONFIG:?}" "${CONTROLLER_CONFIG:?}"

# The library is linted where it lives, by //hack:shellcheck_test.
# shellcheck source=/dev/null
source "${METRICS_LIBRARY}"

scratch="${TEST_TMPDIR:?the test needs a temporary directory}"

failures=0

problem() {
    echo "FAILED: $*" >&2
    failures=$((failures + 1))
}

# The rules without their comments: what Prometheus reads.
rules_without_comments() {
    grep -vE '^[[:space:]]*#' "${RULES}"
}

# Prints "<alert> <severity>" for every alert of the rule file, with "none"
# for an alert that has no severity.
alerts_of_the_rules() {
    awk '
        function finish() { if (alert != "") print alert, "none"; alert = "" }
        $1 == "-" && $2 == "alert:" { finish(); alert = $3 }
        $1 == "severity:" && alert != "" { print alert, $2; alert = "" }
        END { finish() }
    ' "${RULES}" | sort
}

# Prints "<alert> <severity>" for every row of the tables of the alerts page.
alerts_of_the_page() {
    # The backticks are Markdown's, around the name of the alert.
    # shellcheck disable=SC2016
    sed -n -E 's/^\| `(Bitacora[A-Za-z]+)` \| (warning|critical) \|.*$/\1 \2/p' "${ALERTS_PAGE}" | sort
}

# pint parses the file as Prometheus does, every expression as PromQL and
# every annotation as a template, and applies the rules of pint.hcl.
check_with_pint() {
    local report="${scratch}/pint.log"
    if ! "${PINT}" --no-color --offline --config "${PINT_CONFIG}" lint --min-severity info --fail-on info \
        "${RULES}" >"${report}" 2>&1; then
        problem "pint refuses the rules: $(grep -v '^level=INFO' "${report}")"
    fi
}

check_metrics() {
    local metric selector
    while IFS= read -r metric; do
        [[ -n "${metric}" ]] || continue
        problem "the rules use ${metric}, which docs/operations/metrics.md does not list"
    done < <(rules_without_comments | undocumented_metrics "${METRICS_REFERENCE}")
    while IFS= read -r selector; do
        [[ -n "${selector}" ]] || continue
        problem "the rules select ${selector}, a value docs/operations/metrics.md does not have"
    done < <(rules_without_comments | undocumented_label_values "${METRICS_REFERENCE}")
}

check_alerts_page() {
    local alert severity described
    local in_rules in_page
    in_rules="$(alerts_of_the_rules)"
    in_page="$(alerts_of_the_page)"
    if [[ -z "${in_rules}" ]]; then
        problem "the rule file defines no alert"
        return
    fi
    while read -r alert severity; do
        described="$(awk -v alert="${alert}" '$1 == alert { print $2 }' <<<"${in_page}")"
        if [[ -z "${described}" ]]; then
            problem "${alert} is not described in docs/operations/alerts.md"
        elif [[ "${described}" != "${severity}" ]]; then
            problem "${alert} is ${severity} in the rules and ${described} in docs/operations/alerts.md"
        fi
    done <<<"${in_rules}"
    while read -r alert severity; do
        [[ -n "${alert}" ]] || continue
        if ! awk -v alert="${alert}" '$1 == alert { found = 1 } END { exit !found }' <<<"${in_rules}"; then
            problem "docs/operations/alerts.md describes ${alert}, which the rules do not have"
        fi
    done <<<"${in_page}"
}

# Prints the value of a setting of an example configuration file.
setting() {
    local file="$1" name="$2"
    sed -n -E "s/^${name} = (.+)$/\1/p" "${file}"
}

# The rules and their texts quote these defaults. When one moves, the rules
# that quote it have to be read again.
check_quoted_defaults() {
    local name file quoted actual
    while read -r name file quoted; do
        actual="$(setting "${file}" "${name}")"
        if [[ "${actual}" != "${quoted}" ]]; then
            problem "the rules quote ${name} = ${quoted}, but the example configuration says '${actual}'"
        fi
    done <<EOF
refuse_new_segments_at_used_percent ${REPLICA_CONFIG} 85
refuse_appends_at_used_percent ${REPLICA_CONFIG} 95
registration_interval_seconds ${REPLICA_CONFIG} 5
replica_lost_after_seconds ${CONTROLLER_CONFIG} 300
liveness_window_seconds ${CONTROLLER_CONFIG} 15
EOF
}

check_with_pint
check_metrics
check_alerts_page
check_quoted_defaults

if ((failures > 0)); then
    echo "${failures} problems found" >&2
    exit 1
fi
echo "PASSED"
