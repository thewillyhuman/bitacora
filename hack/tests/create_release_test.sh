#!/usr/bin/env bash
# Tests hack/create_release.sh against a throwaway repository, with the
# tools that reach outside (chloggen, cargo, gh) replaced by small stand-ins
# and a local bare repository as the remote.
set -euo pipefail

readonly SCRIPT_UNDER_TEST="${TEST_SRCDIR}/${TEST_WORKSPACE}/hack/create_release.sh"
readonly CHLOGGEN_WRAPPER="${TEST_SRCDIR}/${TEST_WORKSPACE}/hack/chloggen.sh"

failures=0

fail() {
    echo "FAIL: ${current_test}: $*" >&2
    failures=$((failures + 1))
}

assert_contains() {
    local haystack="$1" needle="$2"
    case "${haystack}" in
        *"${needle}"*) ;;
        *) fail "expected to find '${needle}' in: ${haystack}" ;;
    esac
}

assert_equals() {
    local actual="$1" expected="$2"
    [ "${actual}" = "${expected}" ] || fail "expected '${expected}', got '${actual}'"
}

# Builds a repository that looks like bitacora's, with one pending
# changelog entry, and prints its path.
new_repository() {
    local sandbox repository
    sandbox="$(mktemp -d "${TEST_TMPDIR}/release.XXXXXX")"
    repository="${sandbox}/repository"
    git init --quiet --bare "${sandbox}/remote.git"
    mkdir -p "${repository}/hack" "${repository}/.chloggen" \
        "${repository}/src/rust" "${repository}/src/helm/bitacora" "${sandbox}/bin"
    cp "${SCRIPT_UNDER_TEST}" "${CHLOGGEN_WRAPPER}" "${repository}/hack/"

    cat >"${repository}/src/rust/Cargo.toml" <<'EOF'
[workspace.package]
version = "0.0.0"
edition = "2024"

[workspace.dependencies]
bitacora-types = { path = "bitacora-types", version = "0.0.0" }
bitacora-meta-etcd = { path = "bitacora-meta-etcd", version = "0.0.0" }
serde = { version = "1", features = ["derive"] }
EOF
    cat >"${repository}/MODULE.bazel" <<'EOF'
module(
    name = "bitacora",
    version = "0.0.0",
)

bazel_dep(name = "rules_rust", version = "0.75.0")
EOF
    printf 'name: bitacora\nversion: 0.0.0\nappVersion: "0.0.0"\n' \
        >"${repository}/src/helm/bitacora/Chart.yaml"
    printf '# Changelog\n\n<!-- next version -->\n\n## v0.0.9\n\n- `segment`: Older change. (M1)\n' \
        >"${repository}/CHANGELOG.md"
    printf 'change_type: enhancement\ncomponent: replica\nnote: "Flush faster."\nissues: [2]\n' \
        >"${repository}/.chloggen/flush-faster.yaml"
    : >"${repository}/.chloggen/config.yaml"
    : >"${repository}/.chloggen/TEMPLATE.yaml"

    # Stands in for chloggen: writes a section for the version it is given
    # and consumes the entries, which is all the release script relies on.
    cat >"${sandbox}/bin/chloggen" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
while [ "$#" -gt 0 ]; do
    case "$1" in
        --version) release_tag="$2"; shift ;;
    esac
    shift
done
awk -v section="## ${release_tag}\n\n### Enhancements\n\n- \`replica\`: Flush faster. (M2)\n" '
    { print }
    /<!-- next version -->/ { print ""; printf "%s", section }
' CHANGELOG.md >CHANGELOG.md.new
mv CHANGELOG.md.new CHANGELOG.md
find .chloggen -name '*.yaml' ! -name config.yaml ! -name TEMPLATE.yaml -delete
EOF
    # Stands in for cargo: only records that the lockfile refresh was asked.
    printf '#!/usr/bin/env bash\necho "cargo $*" >>"%s/calls.log"\n' "${sandbox}" \
        >"${sandbox}/bin/cargo"
    # Stands in for gh: records its arguments and the notes it was handed.
    cat >"${sandbox}/bin/gh" <<EOF
#!/usr/bin/env bash
echo "gh \$*" >>"${sandbox}/calls.log"
while [ "\$#" -gt 0 ]; do
    if [ "\$1" = "--notes-file" ]; then cp "\$2" "${sandbox}/published-notes.md"; fi
    shift
done
EOF
    chmod +x "${sandbox}/bin/"*

    (
        cd "${repository}"
        git init --quiet --initial-branch main
        git config user.name "Release Tester"
        git config user.email "release-tester@example.invalid"
        git add -A
        git commit --quiet --message "initial state"
        git remote add origin "${sandbox}/remote.git"
    )
    echo "${repository}"
}

# Runs the release script in `repository` with the stand-in tools.
release() {
    local repository="$1"
    shift
    local sandbox
    sandbox="$(dirname "${repository}")"
    (
        cd "${repository}"
        CHLOGGEN="${sandbox}/bin/chloggen" CARGO="${sandbox}/bin/cargo" GH="${sandbox}/bin/gh" \
            ./hack/create_release.sh "$@" 2>&1
    )
}

# ---- tests

current_test="release sets the version everywhere it is written"
repository="$(new_repository)"
release "${repository}" 1.2.3 >/dev/null || fail "the release failed"
assert_contains "$(cat "${repository}/src/rust/Cargo.toml")" 'version = "1.2.3"'
assert_contains "$(cat "${repository}/src/rust/Cargo.toml")" \
    'bitacora-types = { path = "bitacora-types", version = "1.2.3" }'
assert_contains "$(cat "${repository}/src/rust/Cargo.toml")" \
    'bitacora-meta-etcd = { path = "bitacora-meta-etcd", version = "1.2.3" }'
assert_contains "$(cat "${repository}/src/rust/Cargo.toml")" 'serde = { version = "1"'
assert_contains "$(cat "${repository}/MODULE.bazel")" 'version = "1.2.3",'
assert_contains "$(cat "${repository}/MODULE.bazel")" 'bazel_dep(name = "rules_rust", version = "0.75.0")'
assert_contains "$(cat "${repository}/src/helm/bitacora/Chart.yaml")" 'version: 1.2.3'
assert_contains "$(cat "${repository}/src/helm/bitacora/Chart.yaml")" 'appVersion: "1.2.3"'

current_test="release commits, tags and pushes"
sandbox="$(dirname "${repository}")"
assert_equals "$(git -C "${repository}" log -1 --format=%s)" "release: v1.2.3"
assert_contains "$(git -C "${repository}" log -1 --format=%b)" "Signed-off-by: Release Tester"
assert_equals "$(git -C "${repository}" status --porcelain)" ""
assert_equals "$(git -C "${sandbox}/remote.git" tag --list)" "v1.2.3"
assert_equals "$(git -C "${sandbox}/remote.git" log -1 --format=%s main)" "release: v1.2.3"

current_test="release publishes exactly the new changelog section"
assert_contains "$(cat "${sandbox}/calls.log")" "gh release create v1.2.3 --title v1.2.3"
assert_contains "$(cat "${sandbox}/published-notes.md")" "Flush faster. (M2)"
case "$(cat "${sandbox}/published-notes.md")" in
    *"Older change"*) fail "the notes include an older release" ;;
esac

current_test="release consumes the pending entries and refreshes the lockfile"
[ ! -e "${repository}/.chloggen/flush-faster.yaml" ] || fail "the entry was not consumed"
assert_contains "$(cat "${sandbox}/calls.log")" "cargo update --workspace --offline"

current_test="dry run changes nothing"
repository="$(new_repository)"
sandbox="$(dirname "${repository}")"
head_before="$(git -C "${repository}" rev-parse HEAD)"
output="$(release "${repository}" --dry-run 1.2.3)" || fail "the dry run failed"
assert_contains "${output}" "Flush faster. (M2)"
assert_equals "$(git -C "${repository}" rev-parse HEAD)" "${head_before}"
assert_equals "$(git -C "${repository}" status --porcelain)" ""
assert_equals "$(git -C "${repository}" tag --list)" ""
[ -e "${repository}/.chloggen/flush-faster.yaml" ] || fail "the entry was consumed"
[ ! -e "${sandbox}/published-notes.md" ] || fail "a release was published"

current_test="malformed versions are refused"
repository="$(new_repository)"
for malformed in "" "1.2" "v1.2.3" "1.2.3-rc1" "01.2.3" "1.2.x"; do
    if output="$(release "${repository}" ${malformed:+"${malformed}"})"; then
        fail "version '${malformed}' was accepted"
    fi
done
assert_equals "$(git -C "${repository}" log -1 --format=%s)" "initial state"

current_test="a dirty working tree is refused"
repository="$(new_repository)"
echo "uncommitted" >>"${repository}/MODULE.bazel"
if output="$(release "${repository}" 1.2.3)"; then
    fail "the release ran on a dirty tree"
fi
assert_contains "${output}" "uncommitted changes"

current_test="another branch is refused"
repository="$(new_repository)"
git -C "${repository}" checkout --quiet -b feature
if output="$(release "${repository}" 1.2.3)"; then
    fail "the release ran on a feature branch"
fi
assert_contains "${output}" "releases are cut from 'main'"

current_test="an existing tag is refused"
repository="$(new_repository)"
git -C "${repository}" tag v1.2.3
if output="$(release "${repository}" 1.2.3)"; then
    fail "the release reused a tag"
fi
assert_contains "${output}" "tag v1.2.3 exists already"

current_test="a release without changelog entries is refused"
repository="$(new_repository)"
git -C "${repository}" rm --quiet .chloggen/flush-faster.yaml
git -C "${repository}" commit --quiet --message "consume the entry"
if output="$(release "${repository}" 1.2.3)"; then
    fail "an empty release was created"
fi
assert_contains "${output}" "nothing to release"

if [ "${failures}" -gt 0 ]; then
    echo "${failures} assertion(s) failed" >&2
    exit 1
fi
echo "all release tests passed"
