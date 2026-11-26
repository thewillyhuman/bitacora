#!/usr/bin/env bash
# Tests the Grafana dashboard without a Grafana or a Prometheus server. It
# reads the dashboard with the pinned jq and checks that every panel can be
# understood and laid out, that every query is valid PromQL, selects the
# cluster chosen at the top and uses only metrics and label values of the
# metrics reference, and that no metric of the reference is left out.
#
# Usage: dashboard_test.sh
#
# Environment (Bazel sets all of it, see BUILD.bazel):
#   JQ                  the jq program
#   PINT                the pint program, used here as a PromQL parser
#   DASHBOARD           the dashboard to check
#   METRICS_REFERENCE   docs/operations/metrics.md
#   METRICS_LIBRARY     hack/tools/metrics_reference.sh
#
# The jq programs below are full of "$name" in single quotes on purpose:
# those are variables of jq and of Grafana, not of this script.
# shellcheck disable=SC2016
set -euo pipefail

: "${JQ:?}" "${PINT:?}" "${DASHBOARD:?}" "${METRICS_REFERENCE:?}" "${METRICS_LIBRARY:?}"

# The library is linted where it lives, by //hack:shellcheck_test.
# shellcheck source=/dev/null
source "${METRICS_LIBRARY}"

scratch="${TEST_TMPDIR:?the test needs a temporary directory}"

# How wide Grafana's grid is, in columns.
readonly grid_columns=24
# The selector every query must carry, so that the dashboard shows the
# cluster chosen with its `job` variable and no other.
readonly job_selector='job=~"$job"'

failures=0

problem() {
    echo "FAILED: $*" >&2
    failures=$((failures + 1))
}

# Runs a jq program on the dashboard and prints its raw output.
query_dashboard() {
    "${JQ}" -r "$1" "${DASHBOARD}"
}

# Prints "<panel title>\t<query>" for every query of the dashboard.
queries() {
    query_dashboard '.panels[] | select(.type != "row") | .title as $title | .targets[]? | [$title, .expr] | @tsv'
}

check_shape() {
    local variable line
    if ! "${JQ}" -e . "${DASHBOARD}" >/dev/null 2>"${scratch}/jq.log"; then
        problem "the dashboard is not valid JSON: $(cat "${scratch}/jq.log")"
        return 1
    fi
    [[ "$(query_dashboard '.uid')" == "bitacora" ]] ||
        problem "the dashboard's uid is not 'bitacora', so links to it and updates of it break"
    for variable in datasource job; do
        if [[ "$("${JQ}" -r --arg name "${variable}" '[.templating.list[] | select(.name == $name)] | length' \
            "${DASHBOARD}")" != "1" ]]; then
            problem "the dashboard has no variable '${variable}'"
        fi
    done
    while IFS= read -r line; do
        [[ -n "${line}" ]] || continue
        problem "${line}"
    done < <("${JQ}" -r --argjson grid_columns "${grid_columns}" '
        (.panels | group_by(.id)[] | select(length > 1) | "panel id \(.[0].id) is used \(length) times"),
        (.panels[] | select(.type != "row") |
            (select((.title // "") == "") | "panel \(.id) has no title"),
            (select((.description // "") == "") | "the panel \"\(.title)\" has no description"),
            (select(.datasource.uid != "${datasource}") |
                "the panel \"\(.title)\" does not use the data source chosen at the top"),
            (select((.targets // []) | length == 0) | "the panel \"\(.title)\" has no query"),
            (select(.gridPos.x + .gridPos.w > $grid_columns) |
                "the panel \"\(.title)\" reaches past column \($grid_columns)"))' "${DASHBOARD}")
}

check_queries() {
    local title expression metrics selectors
    while IFS=$'\t' read -r title expression; do
        [[ -n "${title}" ]] || continue
        if [[ -z "${expression}" ]]; then
            problem "the panel \"${title}\" has an empty query"
            continue
        fi
        # Every metric of a query has a selector of its own, and each must
        # name the job. Counting both is enough to find one that does not.
        # grep fails when it finds nothing, which here is an answer: zero.
        metrics="$(grep -oE 'bitacora_[a-z0-9_]+' <<<"${expression}" | wc -l || true)"
        selectors="$(grep -oF "${job_selector}" <<<"${expression}" | wc -l || true)"
        if ((metrics != selectors)); then
            problem "a query of the panel \"${title}\" does not select the job for every metric: ${expression}"
        fi
    done < <(queries)
}

check_metrics() {
    local metric selector
    while IFS= read -r metric; do
        [[ -n "${metric}" ]] || continue
        problem "the dashboard uses ${metric}, which docs/operations/metrics.md does not list"
    done < <(queries | undocumented_metrics "${METRICS_REFERENCE}")
    while IFS= read -r selector; do
        [[ -n "${selector}" ]] || continue
        problem "the dashboard selects ${selector}, a value docs/operations/metrics.md does not have"
    done < <(queries | undocumented_label_values "${METRICS_REFERENCE}")
    while IFS= read -r metric; do
        [[ -n "${metric}" ]] || continue
        problem "no panel shows ${metric}, which docs/operations/metrics.md lists"
    done < <(comm -13 <(queries | used_metrics "${METRICS_REFERENCE}") <(documented_metrics "${METRICS_REFERENCE}"))
}

# pint only reads rule files, so every query becomes a recording rule, with
# Grafana's variables replaced by values Prometheus can parse. Only the
# syntax check runs: the others are about rules, and these are not rules.
check_promql() {
    local rules="${scratch}/dashboard-queries.yaml" report="${scratch}/pint.log" number=0 title expression
    {
        echo "groups:"
        echo "  - name: dashboard"
        echo "    rules:"
        while IFS=$'\t' read -r title expression; do
            [[ -n "${expression}" ]] || continue
            number=$((number + 1))
            expression="${expression//\$__rate_interval/5m}"
            expression="${expression//\$job/bitacora}"
            echo "      # ${title}"
            echo "      - record: bitacora:dashboard_query_${number}"
            echo "        expr: |"
            echo "          ${expression}"
        done < <(queries)
    } >"${rules}"
    printf 'parser {\n  relaxed = [ ".*" ]\n}\n' >"${scratch}/pint.hcl"
    if ! "${PINT}" --no-color --offline --config "${scratch}/pint.hcl" --enabled promql/syntax lint \
        --min-severity info --fail-on info "${rules}" >"${report}" 2>&1; then
        problem "a query of the dashboard is not valid PromQL: $(grep -v '^level=INFO' "${report}")"
    fi
}

if check_shape; then
    check_queries
    check_metrics
    check_promql
fi

if ((failures > 0)); then
    echo "${failures} problems found" >&2
    exit 1
fi
echo "PASSED"
