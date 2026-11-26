#!/usr/bin/env bash
# Builds the container images of bitacora and tags them with a version.
#
# Usage: hack/build_images.sh [X.Y.Z | X.Y.Z-rc.N]
#
# Builds bita-replica, bita-controller, bita and bita-bench from
# src/docker/Dockerfile and tags each `<repository>/<name>:<version>`. Without a version, the one
# written in MODULE.bazel is used. A version that ends in `-rc.N` is a
# release candidate: images to try out before X.Y.Z is released. Nothing is
# pushed: the script prints the tags, and `docker push` takes them to the
# registry a cluster pulls from.
#
# Environment:
#   BITA_IMAGE_REPOSITORY  what comes before the image name (default: bitacora)
#   BITA_IMAGE_PLATFORM    the platform to build for, such as linux/amd64
#                          (default: the platform of the machine)
#   BITA_CONTAINER_TOOL    the program that builds images (default: docker)
set -euo pipefail

# The images to build: each is a stage of the Dockerfile with the same name.
readonly IMAGES=(bita-replica bita-controller bita bita-bench)

readonly IMAGE_REPOSITORY="${BITA_IMAGE_REPOSITORY:-bitacora}"
readonly IMAGE_PLATFORM="${BITA_IMAGE_PLATFORM:-}"
readonly CONTAINER_TOOL="${BITA_CONTAINER_TOOL:-docker}"

fail() {
    echo "build_images: $*" >&2
    exit 1
}

usage() {
    sed -n '2,/^set -euo/{/^set -euo/d;s/^# \{0,1\}//;p;}' "${BASH_SOURCE[0]}"
}

version=""
for argument in "$@"; do
    case "${argument}" in
        -h | --help)
            usage
            exit 0
            ;;
        -*)
            usage >&2
            exit 2
            ;;
        *)
            if [[ -n "${version}" ]]; then
                usage >&2
                exit 2
            fi
            version="${argument}"
            ;;
    esac
done

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly repository_root

if [[ -z "${version}" ]]; then
    # The first `version = "..."` of MODULE.bazel is the module's own.
    version="$(sed -n -E '/^ *version = "([^"]+)",?$/{s//\1/p;q;}' "${repository_root}/MODULE.bazel")"
    [[ -n "${version}" ]] || fail "MODULE.bazel does not say which version this is"
fi
if [[ ! "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-rc\.[0-9]+)?$ ]]; then
    echo "build_images: ${version} is not a version of the form X.Y.Z or X.Y.Z-rc.N" >&2
    exit 2
fi

command -v "${CONTAINER_TOOL}" >/dev/null || fail "${CONTAINER_TOOL} is not installed"

platform_options=()
if [[ -n "${IMAGE_PLATFORM}" ]]; then
    platform_options=(--platform "${IMAGE_PLATFORM}")
fi

for image in "${IMAGES[@]}"; do
    tag="${IMAGE_REPOSITORY}/${image}:${version}"
    echo "build_images: building ${tag}" >&2
    # The four builds share the stage that compiles, which all but the first
    # find already built. An empty array expands to nothing here; the
    # form below also does so on the bash 3.2 of macOS.
    "${CONTAINER_TOOL}" build \
        --file "${repository_root}/src/docker/Dockerfile" \
        --target "${image}" \
        --build-arg "VERSION=${version}" \
        --tag "${tag}" \
        ${platform_options[@]+"${platform_options[@]}"} \
        "${repository_root}"
done

for image in "${IMAGES[@]}"; do
    echo "${IMAGE_REPOSITORY}/${image}:${version}"
done
