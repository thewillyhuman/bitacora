#!/usr/bin/env bash
# Checks development/kubernetes/soak.yaml without a cluster: the Role grants
# exactly what bita-soak asks of the Kubernetes API, the Job starts the tool
# with arguments it accepts and as the service account the Role is bound to,
# the histories go to a mounted volume, and a failed run is not started
# again.
#
# The manifest is read line by line, not parsed: it is written so that each
# thing checked here is on one line.
#
# Reads from the environment:
#   SOAK_MANIFEST       the manifest
#   BITA_SOAK_BINARY    the tool, as Bazel builds it
set -euo pipefail

: "${SOAK_MANIFEST:?}" "${BITA_SOAK_BINARY:?}"

failures=0

fail() {
    echo "FAIL: $*" >&2
    failures=$((failures + 1))
}

# Prints what follows `<key>: ` on the first line that has it in the
# document of kind <kind>.
field_of() {
    awk -v kind="$1" -v key="$2" '
        /^---/ { current = "" }
        /^kind: / { current = $2 }
        current == kind && index($0, key ": ") {
            sub(".*" key ": *", "")
            print
            exit
        }
    ' "${SOAK_MANIFEST}"
}

# Prints the lines of the manifest that follow the line `<key>:` and are
# list items, without their indentation and their dash.
items_under() {
    awk -v key="$1" '
        in_list && $1 != "-" && $1 !~ /^#/ { exit }
        in_list && $1 == "-" { sub("^ *- ", ""); print }
        $1 == key ":" && NF == 1 { in_list = 1 }
    ' "${SOAK_MANIFEST}"
}

# The Role: what the tool is allowed is what it asks for, and nothing more.
check_role() {
    local granted needed
    granted="$(items_under rules)"
    needed='{apiGroups: [""], resources: [pods], verbs: [list, delete]}
{apiGroups: [networking.k8s.io], resources: [networkpolicies], verbs: [create, delete]}'
    [[ "${granted}" == "${needed}" ]] ||
        fail "the Role does not grant exactly what bita-soak asks of the API: it grants '${granted}'"
}

# The service account: the Job runs as the one the Role is bound to.
check_service_account() {
    local account runs_as role
    account="$(field_of ServiceAccount name)"
    role="$(field_of Role name)"
    runs_as="$(field_of Job serviceAccountName)"
    [[ -n "${account}" ]] || fail "the manifest has no ServiceAccount"
    [[ "${runs_as}" == "${account}" ]] ||
        fail "the Job runs as the service account '${runs_as}', which the Role is not bound to"
    grep -q -F "subjects: [{kind: ServiceAccount, name: ${account}, namespace: " "${SOAK_MANIFEST}" ||
        fail "the RoleBinding does not bind the service account '${account}'"
    grep -q -F "roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: ${role}}" "${SOAK_MANIFEST}" ||
        fail "the RoleBinding does not bind the Role '${role}'"
}

# The arguments: the tool accepts every one of them. Its parser reads them
# in order, so a mistake is reported before --help is reached.
check_arguments() {
    local arguments=() argument report dump_directory
    while IFS= read -r argument; do
        arguments+=("${argument}")
    done < <(items_under args)
    if [[ "${#arguments[@]}" -eq 0 ]]; then
        fail "the Job starts bita-soak without arguments"
        return
    fi
    if ! report="$("${BITA_SOAK_BINARY}" "${arguments[@]}" --help 2>&1 >/dev/null)"; then
        fail "the Job starts bita-soak with arguments it does not accept: $(head -n 1 <<<"${report}")"
    fi
    [[ " ${arguments[*]} " == *" --etcd-endpoint="* ]] || fail "the Job names no etcd endpoint"
    dump_directory="$(tr ' ' '\n' <<<"${arguments[*]}" | sed -n 's/^--dump-directory=//p')"
    grep -q -F "mountPath: ${dump_directory}}" "${SOAK_MANIFEST}" ||
        fail "the histories are written to '${dump_directory}', which is not a mounted volume"
}

# The Job: the image is the tool's, with the version left to whoever applies
# it; a failed run stays failed; and the tool is given time to remove a cut.
check_job() {
    local image grace
    image="$(field_of Job image)"
    [[ "${image}" == */bita-soak:BITA_VERSION ]] ||
        fail "the image '${image}' is not bita-soak with the placeholder BITA_VERSION as its version"
    [[ "$(field_of Job backoffLimit)" == 0 && "$(field_of Job restartPolicy)" == Never ]] ||
        fail "a run that failed would be started again"
    grace="$(field_of Job terminationGracePeriodSeconds)"
    [[ "${grace:-0}" -ge 60 ]] ||
        fail "the tool is given ${grace:-no} seconds to stop, too few to remove a network cut"
}

check_role
check_service_account
check_arguments
check_job

if [[ "${failures}" -gt 0 ]]; then
    echo "${failures} problem(s) in ${SOAK_MANIFEST}" >&2
    exit 1
fi
echo "the soak manifest grants what the tool asks for and starts it with arguments it accepts"
