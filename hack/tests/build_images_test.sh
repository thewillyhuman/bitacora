#!/usr/bin/env bash
# Tests hack/build_images.sh without a container runtime: a stand-in for
# `docker` writes down how it was called, and the test checks that the three
# images are built from the right file, stage, version and tag.
#
# Usage: build_images_test.sh <build_images.sh>
set -euo pipefail

build_images="${1:?usage: build_images_test.sh <build_images.sh>}"
scratch="${TEST_TMPDIR:?the test needs a temporary directory}"

# A repository with just what the script reads.
repository="${scratch}/repository"
mkdir -p "${repository}/hack" "${repository}/src/docker"
cp "${build_images}" "${repository}/hack/build_images.sh"
printf 'module(\n    name = "bitacora",\n    version = "1.2.3",\n)\n\nbazel_dep(name = "rules_rust", version = "0.75.0")\n' \
    >"${repository}/MODULE.bazel"
: >"${repository}/src/docker/Dockerfile"

# The stand-in: one line per call, with the arguments it was given.
calls="${scratch}/calls"
cat >"${scratch}/container-tool" <<EOF
#!/bin/sh
echo "\$*" >>"${calls}"
EOF
chmod +x "${scratch}/container-tool"
export BITA_CONTAINER_TOOL="${scratch}/container-tool"

fail() {
    echo "FAILED: $*" >&2
    exit 1
}

# Requires a line of the calls file to contain every phrase given.
expect_call() {
    local line_number="$1" line phrase
    shift
    line="$(sed -n "${line_number}p" "${calls}")"
    for phrase in "$@"; do
        case "${line}" in
            *"${phrase}"*) ;;
            *) fail "call ${line_number} lacks '${phrase}': ${line}" ;;
        esac
    done
}

echo "--- without a version, the version of MODULE.bazel is used"
: >"${calls}"
output="$("${repository}/hack/build_images.sh" 2>/dev/null)"
[[ "$(wc -l <"${calls}" | tr -d ' ')" == 3 ]] || fail "expected three builds, got: $(cat "${calls}")"
expect_call 1 "build" "--file ${repository}/src/docker/Dockerfile" "--target bita-replica" \
    "--build-arg VERSION=1.2.3" "--tag bitacora/bita-replica:1.2.3"
expect_call 2 "--target bita-controller" "--tag bitacora/bita-controller:1.2.3"
expect_call 3 "--target bita " "--tag bitacora/bita:1.2.3"
# The build context is the root of the repository, and comes last.
[[ "$(sed -n 1p "${calls}")" == *" ${repository}" ]] || fail "the build context is not the repository"
[[ "$(sed -n 1p "${calls}")" != *"--platform"* ]] || fail "a platform was given without being asked for"
expected_tags="bitacora/bita-replica:1.2.3
bitacora/bita-controller:1.2.3
bitacora/bita:1.2.3"
[[ "${output}" == "${expected_tags}" ]] || fail "the tags printed are not the tags built: ${output}"

echo "--- a version, a repository and a platform are honoured"
: >"${calls}"
BITA_IMAGE_REPOSITORY=registry.example.org/storage BITA_IMAGE_PLATFORM=linux/amd64 \
    "${repository}/hack/build_images.sh" 2.0.0 >/dev/null 2>&1
expect_call 1 "--build-arg VERSION=2.0.0" "--tag registry.example.org/storage/bita-replica:2.0.0" \
    "--platform linux/amd64"

echo "--- the version of a release candidate is honoured"
: >"${calls}"
"${repository}/hack/build_images.sh" 2.0.0-rc.3 >/dev/null 2>&1 || fail "a release candidate was refused"
expect_call 1 "--build-arg VERSION=2.0.0-rc.3" "--tag bitacora/bita-replica:2.0.0-rc.3"

echo "--- what is not a version is refused before anything is built"
: >"${calls}"
for not_a_version in latest 2.0.0-rc 2.0.0-rc.x 2.0.0-beta.1 2.0.0-rc.1.2; do
    exit_code=0
    "${repository}/hack/build_images.sh" "${not_a_version}" >/dev/null 2>&1 || exit_code=$?
    ((exit_code == 2)) || fail "'${not_a_version}' was not refused with exit code 2 but ${exit_code}"
done
[[ ! -s "${calls}" ]] || fail "an image was built for a version that was refused"
exit_code=0
"${repository}/hack/build_images.sh" 1.0.0 2.0.0 >/dev/null 2>&1 || exit_code=$?
((exit_code == 2)) || fail "two versions were not refused with exit code 2 but ${exit_code}"
exit_code=0
"${repository}/hack/build_images.sh" --push >/dev/null 2>&1 || exit_code=$?
((exit_code == 2)) || fail "an unknown option was not refused with exit code 2 but ${exit_code}"

echo "--- a failed build stops the script"
printf '#!/bin/sh\nexit 1\n' >"${scratch}/container-tool"
exit_code=0
"${repository}/hack/build_images.sh" >/dev/null 2>&1 || exit_code=$?
((exit_code != 0)) || fail "the script succeeded although the build failed"

echo "--- a missing container tool is named"
exit_code=0
message="$(BITA_CONTAINER_TOOL="${scratch}/no-such-tool" "${repository}/hack/build_images.sh" 2>&1)" || exit_code=$?
((exit_code == 1)) || fail "a missing tool did not fail with exit code 1 but ${exit_code}"
[[ "${message}" == *"no-such-tool is not installed"* ]] || fail "the missing tool is not named: ${message}"

echo "--- help is printed"
help="$("${repository}/hack/build_images.sh" --help)"
[[ "${help}" == *"Usage: hack/build_images.sh"* ]] || fail "no usage text: ${help}"

echo "PASSED"
