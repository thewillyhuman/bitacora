#!/usr/bin/env bash
# Functions that check uses of bitacora's metrics against the metrics
# reference, docs/operations/metrics.md. Alert rules and dashboards are
# written against the names in that page; the tests of both source this file
# so that a renamed metric, or a label value that does not exist, fails a
# test and not an operator at three in the morning.
#
# Usage: source this file, then call its functions. It defines functions
# only and changes no shell option.
#
# The backticks in single quotes below are Markdown's, which is what the
# reference is written in; they are not commands for the shell to run.
# shellcheck disable=SC2016

# Prints every metric name the reference lists, one per line, sorted.
#
# A name is listed when the page has it between backticks. Patterns such as
# `bitacora_<subsystem>_requests_total` are not names and are not printed;
# the page spells out the names they stand for.
#
# Arguments: the path of the metrics reference.
documented_metrics() {
    local reference="$1"
    grep -oE '`bitacora_[a-z0-9_]+`' "${reference}" | tr -d '`' | sort -u
}

# Prints every bitacora metric name in the text on standard input, one per
# line, sorted. A name with the suffix of a histogram series (`_bucket`,
# `_sum`, `_count`) is printed as the histogram it belongs to when the
# reference lists that histogram and not the name itself.
#
# Arguments: the path of the metrics reference.
used_metrics() {
    local reference="$1" documented name base
    documented="$(documented_metrics "${reference}")"
    while IFS= read -r name; do
        base="${name%_bucket}"
        base="${base%_sum}"
        base="${base%_count}"
        if ! grep -qxF "${name}" <<<"${documented}" && grep -qxF "${base}" <<<"${documented}"; then
            echo "${base}"
        else
            echo "${name}"
        fi
    done < <(grep -oE 'bitacora_[a-z0-9_]+' || true) | sort -u
}

# Prints every metric name in the text on standard input that the reference
# does not list, one per line.
#
# Arguments: the path of the metrics reference.
undocumented_metrics() {
    local reference="$1"
    comm -23 <(used_metrics "${reference}") <(documented_metrics "${reference}")
}

# Prints every label selector of the text on standard input whose value the
# reference does not have, one per line, as `label="value"`.
#
# The labels of bitacora's metrics are small closed sets, and the reference
# has every value between backticks. Only selectors that name one value are
# checked: `operation="append"`, not `job=~"$job"`.
#
# Arguments: the path of the metrics reference.
undocumented_label_values() {
    local reference="$1" selector value
    while IFS= read -r selector; do
        value="${selector#*=\"}"
        value="${value%\"}"
        if ! grep -qF "\`${value}\`" "${reference}"; then
            echo "${selector}"
        fi
    done < <(grep -oE '(operation|error|direction)="[a-z_]+"' | sort -u || true)
}
