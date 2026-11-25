#!/usr/bin/env bash
# Creates a release: collects the pending changelog entries into
# CHANGELOG.md, sets the version everywhere it is written, commits, tags,
# pushes, and publishes a GitHub release whose notes are the new changelog
# section.
#
# Usage: hack/create_release.sh [--dry-run] X.Y.Z
#
#   --dry-run   Do everything locally, print the release notes and what would
#               be pushed and published, then put the working tree back.
#
# The script refuses to run unless the working tree is clean, the current
# branch is the release branch and the tag does not exist yet, so a failed
# run can simply be started again.
#
# Environment:
#   BITA_RELEASE_BRANCH  the branch releases are cut from (default: main)
#   BITA_RELEASE_REMOTE  the remote to push to (default: origin)
#   GH                   the GitHub CLI to publish with (default: gh)
#   CARGO                the cargo that refreshes Cargo.lock (default: cargo)
#   CHLOGGEN             see hack/chloggen.sh
set -euo pipefail

readonly RELEASE_BRANCH="${BITA_RELEASE_BRANCH:-main}"
readonly RELEASE_REMOTE="${BITA_RELEASE_REMOTE:-origin}"
readonly GH="${GH:-gh}"
readonly CARGO="${CARGO:-cargo}"

fail() {
    echo "create_release: $*" >&2
    exit 1
}

usage() {
    sed -n '2,/^set -euo/{/^set -euo/d;s/^# \{0,1\}//;p;}' "${BASH_SOURCE[0]}"
}

is_dry_run=false
version=""
for argument in "$@"; do
    case "${argument}" in
        --dry-run) is_dry_run=true ;;
        -h | --help)
            usage
            exit 0
            ;;
        -*) fail "unknown option ${argument}; see --help" ;;
        *)
            [ -z "${version}" ] || fail "expected one version, got '${version}' and '${argument}'"
            version="${argument}"
            ;;
    esac
done

[ -n "${version}" ] || fail "missing version; usage: create_release.sh [--dry-run] X.Y.Z"
[[ "${version}" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] ||
    fail "'${version}' is not a version of the form X.Y.Z"
readonly version
readonly tag="v${version}"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# ---- Preconditions: nothing below runs on a surprising repository state.

[ -z "$(git status --porcelain)" ] ||
    fail "the working tree has uncommitted changes; commit or stash them first"
current_branch="$(git rev-parse --abbrev-ref HEAD)"
[ "${current_branch}" = "${RELEASE_BRANCH}" ] ||
    fail "releases are cut from '${RELEASE_BRANCH}', but the current branch is '${current_branch}'"
if git rev-parse --quiet --verify "refs/tags/${tag}" >/dev/null; then
    fail "tag ${tag} exists already"
fi
pending_entries="$(find .chloggen -maxdepth 1 -name '*.yaml' ! -name 'config.yaml' ! -name 'TEMPLATE.yaml' | wc -l)"
[ "${pending_entries}" -gt 0 ] ||
    fail "there are no changelog entries under .chloggen/, so there is nothing to release"

restore_working_tree() {
    git reset --quiet --hard HEAD
    git clean --quiet -fd .chloggen
}
if "${is_dry_run}"; then
    trap restore_working_tree EXIT
fi

# ---- Changelog: move the pending entries into a section for this version.

./hack/chloggen.sh update --version "${tag}"

# ---- Version: every place that states it.

set_version() {
    local file="$1" pattern="$2" replacement="$3"
    [ -f "${file}" ] || return 0
    grep -Eq "${pattern}" "${file}" || fail "${file} has no line matching '${pattern}'"
    # A temporary file keeps this portable between GNU and BSD sed.
    sed -E "s|${pattern}|${replacement}|" "${file}" >"${file}.new"
    mv "${file}.new" "${file}"
}

# The workspace version, and the versions of the first-party crates that
# depend on each other.
set_version src/rust/Cargo.toml \
    '^version = "[^"]+"$' \
    "version = \"${version}\""
sed -E "s|^(bitacora(-[a-z0-9-]+)? = \{ path = \"[^\"]+\", version = )\"[^\"]+\"|\1\"${version}\"|" \
    src/rust/Cargo.toml >src/rust/Cargo.toml.new
mv src/rust/Cargo.toml.new src/rust/Cargo.toml
# The Bazel module version is the first `version =` in the file.
awk -v version="${version}" '
    !done && /^    version = "/ { sub(/"[^"]+"/, "\"" version "\""); done = 1 }
    { print }
' MODULE.bazel >MODULE.bazel.new
mv MODULE.bazel.new MODULE.bazel
set_version src/helm/bitacora/Chart.yaml '^version: .+$' "version: ${version}"
set_version src/helm/bitacora/Chart.yaml '^appVersion: .+$' "appVersion: \"${version}\""

# Cargo.lock records the versions of the workspace's own crates.
(cd src/rust && "${CARGO}" update --workspace --offline --quiet)

# ---- Release notes: the section chloggen just wrote.

release_notes="$(mktemp)"
awk -v heading="## ${tag}" '
    $0 == heading { in_section = 1; next }
    in_section && /^## / { exit }
    in_section { print }
' CHANGELOG.md >"${release_notes}"
[ -s "${release_notes}" ] || fail "CHANGELOG.md has no section for ${tag} after the update"

if "${is_dry_run}"; then
    echo "--- release notes for ${tag}"
    cat "${release_notes}"
    echo "--- files that would be committed as 'release: ${tag}'"
    git status --short
    echo "--- dry run: would tag ${tag}, push ${RELEASE_BRANCH} and the tag to ${RELEASE_REMOTE}, and publish the release"
    rm -f "${release_notes}"
    exit 0
fi

# ---- Commit, tag, push, publish.

git add -A
git commit --quiet --signoff --message "release: ${tag}" \
    --message "Release notes are in CHANGELOG.md under the heading '${tag}'."
git tag --annotate "${tag}" --message "bitacora ${tag}"
git push --quiet "${RELEASE_REMOTE}" "${RELEASE_BRANCH}" "${tag}"
"${GH}" release create "${tag}" --title "${tag}" --notes-file "${release_notes}" --verify-tag
rm -f "${release_notes}"
echo "released ${tag}"
