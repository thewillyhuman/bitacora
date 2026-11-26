#!/usr/bin/env bash
# Tests the Dockerfile against what it packs, without building an image:
# every image starts a program the build stage really builds, with options
# that program has, as a numbered user that is not root, and exposes the
# ports and owns the directories the daemons' example configurations name.
#
# Usage: dockerfile_test.sh
#
# Environment (Bazel sets all of it, see BUILD.bazel):
#   DOCKERFILE              the Dockerfile to check
#   DOCKERIGNORE            the ignore file of that Dockerfile
#   BUILD_IMAGES            hack/build_images.sh, which names the images
#   BAZELRC                 the .bazelrc of the repository
#   REPLICA_CONFIG          the example configuration of bita-replica
#   CONTROLLER_CONFIG       the example configuration of bita-controller
#   BITA_REPLICA_BINARY     the four programs, as Bazel builds them
#   BITA_CONTROLLER_BINARY
#   BITA_BINARY
#   BITA_BENCH_BINARY
set -euo pipefail

: "${DOCKERFILE:?}" "${DOCKERIGNORE:?}" "${BUILD_IMAGES:?}" "${BAZELRC:?}"
: "${REPLICA_CONFIG:?}" "${CONTROLLER_CONFIG:?}"
: "${BITA_REPLICA_BINARY:?}" "${BITA_CONTROLLER_BINARY:?}" "${BITA_BINARY:?}" "${BITA_BENCH_BINARY:?}"

failures=0

problem() {
    echo "FAILED: $*" >&2
    failures=$((failures + 1))
}

# Prints the instructions of one stage, one per line: comments dropped,
# continuation lines joined, runs of spaces squeezed.
instructions_of() {
    local wanted_stage="$1" line instruction="" stage=""
    while IFS= read -r line || [[ -n "${line}" ]]; do
        if [[ "${line}" =~ ^[[:space:]]*# ]]; then
            continue
        fi
        if [[ "${line}" == *\\ ]]; then
            instruction+="${line%\\} "
            continue
        fi
        instruction+="${line}"
        while [[ "${instruction}" == *"  "* ]]; do
            instruction="${instruction//  / }"
        done
        instruction="${instruction# }"
        if [[ "${instruction}" =~ ^FROM\ .*\ AS\ ([a-z0-9-]+)$ ]]; then
            stage="${BASH_REMATCH[1]}"
        fi
        if [[ "${stage}" == "${wanted_stage}" && -n "${instruction}" ]]; then
            echo "${instruction}"
        fi
        instruction=""
    done <"${DOCKERFILE}"
}

# Prints what a stage is built from: an image, or the name of another stage.
base_of() {
    instructions_of "$1" | sed -n -E 's/^FROM ([^ ]+) AS .*$/\1/p'
}

# Prints the argument of the last instruction of a kind in a stage, looking
# in the stage it is built from when the stage itself has none.
setting_of() {
    local stage="$1" keyword="$2" value base
    value="$(instructions_of "${stage}" | sed -n -E "s/^${keyword} (.*)$/\1/p" | tail -n 1)"
    if [[ -n "${value}" ]]; then
        echo "${value}"
        return
    fi
    base="$(base_of "${stage}")"
    if [[ -n "${base}" && -n "$(instructions_of "${base}")" ]]; then
        setting_of "${base}" "${keyword}"
    fi
}

# Prints the port of an address setting of a configuration file.
port_of() {
    sed -n -E "s/^$2 = \".*:([0-9]+)\"$/\1/p" "$1"
}

# Prints words sorted on one line, to compare two sets.
sorted() {
    tr ' ' '\n' <<<"$*" | sed '/^$/d' | LC_ALL=C sort | tr '\n' ' '
}

# Checks what the four images have in common: the program is the one the
# build stage builds, it is what the image starts, and the image does not
# run as root.
check_image() {
    local image="$1" program="$2" stage_instructions build_instructions built_path user
    stage_instructions="$(instructions_of "${image}")"
    if [[ -z "${stage_instructions}" ]]; then
        problem "there is no stage named ${image}"
        return
    fi
    [[ "$(base_of "${image}")" == runtime ]] || problem "${image} is not built from the runtime stage"

    # Bazel writes a program to bazel-bin/<its path in the repository>, and
    # that path is how the test is handed the program.
    built_path="bazel-bin/${program}"
    build_instructions="$(instructions_of build)"
    [[ "${build_instructions}" == *" //src/rust/${image} "* ]] ||
        problem "the build stage does not build //src/rust/${image}"
    [[ "${build_instructions}" == *"${built_path} "* ]] ||
        problem "the build stage does not copy ${built_path} out of Bazel's output"
    grep -q -E "^COPY --from=build [^ ]+/out/${image} /usr/bin/${image}$" <<<"${stage_instructions}" ||
        problem "${image} does not copy its program to /usr/bin/${image}"
    grep -q -F "ENTRYPOINT [\"/usr/bin/${image}\"]" <<<"${stage_instructions}" ||
        problem "${image} does not start /usr/bin/${image}"

    user="$(setting_of "${image}" USER)"
    case "${user}" in
        "" | root | root:* | 0 | 0:*) problem "${image} runs as root" ;;
        *[!0-9:]*) problem "${image} names its user (${user}); Kubernetes can only check a number" ;;
    esac
}

# Checks what a daemon's image adds: the options it is started with exist,
# and the ports it exposes are those its example configuration listens on.
check_daemon() {
    local image="$1" program="$2" config_file="$3" command help option setting expected_ports=""
    shift 3
    command="$(setting_of "${image}" CMD)"
    help="$("${program}" --help)"
    while IFS= read -r option; do
        [[ "${help}" == *"${option}"* ]] || problem "${image} is started with ${option}, which it does not have"
    done < <(grep -o -E -- '--[a-z-]+' <<<"${command}" || true)
    [[ "${command}" == *'"--config", "/etc/bitacora/'*'.toml"'* ]] ||
        problem "${image} is not started with a configuration file in /etc/bitacora"

    for setting in "$@"; do
        expected_ports+=" $(port_of "${config_file}" "${setting}")"
    done
    [[ "$(sorted "$(setting_of "${image}" EXPOSE)")" == "$(sorted "${expected_ports}")" ]] ||
        problem "${image} exposes '$(setting_of "${image}" EXPOSE)', but its example configuration listens on${expected_ports}"
}

# Checks that the replica can write where its example configuration keeps
# data: that directory is inside the volume of the image, and the image
# gives the volume to the user it runs as.
check_replica_volume() {
    local volume data_directory user
    volume="$(setting_of bita-replica VOLUME)"
    data_directory="$(sed -n -E 's/^data_directory = "(.*)"$/\1/p' "${REPLICA_CONFIG}")"
    user="$(setting_of bita-replica USER)"
    if [[ -z "${volume}" ]]; then
        problem "bita-replica declares no volume for its data"
        return
    fi
    case "${data_directory}" in
        "${volume}" | "${volume}/"*) ;;
        *) problem "the example data directory ${data_directory} is outside the volume ${volume}" ;;
    esac
    # Not a pipeline into `grep -q`: grep stops reading at the first match,
    # and under pipefail the writer it cuts off would count as a failure.
    grep -q -E "^COPY --from=build --chown=${user} [^ ]+ ${volume}$" <<<"$(instructions_of bita-replica)" ||
        problem "bita-replica does not give ${volume} to the user it runs as (${user})"
}

# Checks the stage that compiles: a release build, with a Bazel that was
# verified after it was downloaded.
check_build_stage() {
    local build_instructions
    build_instructions="$(instructions_of build)"
    if [[ "${build_instructions}" != *"bazel build --config=release "* ]]; then
        problem "the programs are not built with --config=release"
    fi
    grep -q -E '^build:release ' "${BAZELRC}" || problem ".bazelrc has no release configuration"
    [[ "${build_instructions}" == *"sha256sum --check"* ]] ||
        problem "Bazelisk is downloaded without checking its checksum"
    [[ "$(grep -c -E '^ARG BAZELISK_SHA256_[A-Z0-9]+=[0-9a-f]{64}$' "${DOCKERFILE}")" == 2 ]] ||
        problem "the Dockerfile does not pin a checksum of Bazelisk for each of the two architectures"
}

# Checks that the images hack/build_images.sh builds are the images the
# Dockerfile defines, no more and no fewer.
check_image_list() {
    local built defined
    built="$(sed -n -E 's/^readonly IMAGES=\((.*)\)$/\1/p' "${BUILD_IMAGES}")"
    defined="$(sed -n -E 's/^FROM runtime AS ([a-z0-9-]+)$/\1/p' "${DOCKERFILE}" | tr '\n' ' ')"
    [[ -n "${built}" ]] || problem "hack/build_images.sh does not list the images it builds"
    [[ "$(sorted "${built}")" == "$(sorted "${defined}")" ]] ||
        problem "hack/build_images.sh builds '${built}', but the Dockerfile defines '${defined% }'"
}

# Checks that the build context leaves out what must not be read: a
# developer's Bazel output is large and was built for another machine.
check_ignore_file() {
    local pattern
    for pattern in '.git' 'bazel-*' 'user.bazelrc'; do
        grep -q -x -F -- "${pattern}" "${DOCKERIGNORE}" ||
            problem "the build context does not leave out ${pattern}"
    done
}

check_build_stage
check_image bita-replica "${BITA_REPLICA_BINARY}"
check_image bita-controller "${BITA_CONTROLLER_BINARY}"
check_image bita "${BITA_BINARY}"
check_image bita-bench "${BITA_BENCH_BINARY}"
check_daemon bita-replica "${BITA_REPLICA_BINARY}" "${REPLICA_CONFIG}" listen_address metrics_listen_address
check_daemon bita-controller "${BITA_CONTROLLER_BINARY}" "${CONTROLLER_CONFIG}" metrics_listen_address
check_replica_volume
check_image_list
check_ignore_file

if ((failures > 0)); then
    echo "${failures} problems found" >&2
    exit 1
fi
echo "PASSED"
