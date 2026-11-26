# syntax=docker/dockerfile:1
#
# The container images of bitacora: one for each daemon and one for each of
# the bita tool, the benchmark tool and the soak tool, all five from one Bazel
# release build.
#
#   docker build --file src/docker/Dockerfile --target bita-replica \
#       --tag bitacora/bita-replica:0.1.0 .
#
# The build context is the root of the repository. hack/build_images.sh
# builds and tags all five; README.md in this directory says how to run
# them.

# The Debian release of the image that builds and of the images that run.
# They must be the same: a program needs the C library of the release it was
# built on, or a newer one.
ARG DEBIAN_RELEASE=13

# ---------------------------------------------------------------------------
# build: compiles the five programs.
# ---------------------------------------------------------------------------
FROM debian:${DEBIAN_RELEASE}-slim AS build

SHELL ["/bin/bash", "-o", "errexit", "-o", "nounset", "-o", "pipefail", "-c"]

# What Bazel takes from the machine: a C and C++ compiler, to link the
# programs and to build the C code some dependencies carry. Everything else,
# the Rust toolchain included, Bazel downloads by checksum.
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl g++ gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*

# Bazelisk, which reads .bazelversion and runs that Bazel. The checksums are
# the ones published next to the release.
ARG BAZELISK_VERSION=1.30.0
ARG BAZELISK_SHA256_AMD64=a248ae2b2b62b5baebb748f6fba7203bff1a27e5d01b55399a8406744cb539e8
ARG BAZELISK_SHA256_ARM64=b2bcc040c213b44586b121bda6d983c35df121b917c3a7a66fb16f2fa0df1a4f
RUN architecture="$(dpkg --print-architecture)" \
    && case "${architecture}" in \
        amd64) checksum="${BAZELISK_SHA256_AMD64}" ;; \
        arm64) checksum="${BAZELISK_SHA256_ARM64}" ;; \
        *) echo "no Bazelisk is pinned for ${architecture}" >&2; exit 1 ;; \
    esac \
    && curl --fail --silent --show-error --location --output /usr/local/bin/bazel \
        "https://github.com/bazelbuild/bazelisk/releases/download/v${BAZELISK_VERSION}/bazelisk-linux-${architecture}" \
    && echo "${checksum}  /usr/local/bin/bazel" | sha256sum --check - \
    && chmod 0755 /usr/local/bin/bazel

# Bazel does not need root, so the build does not have it.
RUN useradd --create-home --uid 1000 --user-group builder
USER 1000:1000
WORKDIR /home/builder/bitacora
COPY --chown=1000:1000 . .

# Bazel keeps what it downloads and builds in a cache mount, so the next
# build of this image only compiles what changed. A cache mount is not part
# of the image, which is why the programs are copied out of it in the same
# step. `state` is an empty directory for the replica image to own.
#
# The Bazel server is left running on purpose: it ends with the step. Asking
# it to stop with `bazel shutdown` fails here, because nothing in a build
# container collects the process once it has exited.
RUN --mount=type=cache,target=/home/builder/.cache,uid=1000,gid=1000,sharing=locked \
    bazel build --config=release \
        //src/rust/bita-replica //src/rust/bita-controller //src/rust/bita //src/rust/bita-bench \
        //src/rust/bita-soak \
    && mkdir -p /home/builder/out/state \
    && cp bazel-bin/src/rust/bita-replica/bita-replica \
        bazel-bin/src/rust/bita-controller/bita-controller \
        bazel-bin/src/rust/bita/bita \
        bazel-bin/src/rust/bita-bench/bita-bench \
        bazel-bin/src/rust/bita-soak/bita-soak \
        /home/builder/out/

# ---------------------------------------------------------------------------
# runtime: what the five images share. It has the C library and the
# certificate authorities and nothing else: no shell, no package manager.
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/cc-debian${DEBIAN_RELEASE}:nonroot AS runtime

ARG VERSION=0.0.0
LABEL org.opencontainers.image.source="https://github.com/thewillyhuman/bitacora" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}"

# The `nonroot` user of the base image, by number: Kubernetes can only check
# `runAsNonRoot` against a number.
USER 65532:65532

# ---------------------------------------------------------------------------
# bita-replica: one storage node. Mount its configuration at
# /etc/bitacora/replica.toml and a volume at /var/lib/bitacora.
# ---------------------------------------------------------------------------
FROM runtime AS bita-replica

LABEL org.opencontainers.image.title="bita-replica" \
      org.opencontainers.image.description="bitacora replica: one storage node of a cluster"

COPY --from=build /home/builder/out/bita-replica /usr/bin/bita-replica
COPY --from=build --chown=65532:65532 /home/builder/out/state /var/lib/bitacora

# The data directory. A volume that is created for this image takes the
# owner of the directory above.
VOLUME /var/lib/bitacora

# 7000: the replica protocol (gRPC). 9100: /metrics, /healthz and /readyz.
EXPOSE 7000 9100

ENTRYPOINT ["/usr/bin/bita-replica"]
CMD ["--config", "/etc/bitacora/replica.toml"]

# ---------------------------------------------------------------------------
# bita-controller: looks after a cluster. Mount its configuration at
# /etc/bitacora/controller.toml. It keeps no state of its own.
# ---------------------------------------------------------------------------
FROM runtime AS bita-controller

LABEL org.opencontainers.image.title="bita-controller" \
      org.opencontainers.image.description="bitacora controller: placement, repair and tiering for a cluster"

COPY --from=build /home/builder/out/bita-controller /usr/bin/bita-controller

# 9101: /metrics, /healthz and /readyz.
EXPOSE 9101

ENTRYPOINT ["/usr/bin/bita-controller"]
CMD ["--config", "/etc/bitacora/controller.toml"]

# ---------------------------------------------------------------------------
# bita: the command-line tool, for a job or a one-off pod next to a cluster.
# ---------------------------------------------------------------------------
FROM runtime AS bita

LABEL org.opencontainers.image.title="bita" \
      org.opencontainers.image.description="bitacora command-line tool"

COPY --from=build /home/builder/out/bita /usr/bin/bita

ENTRYPOINT ["/usr/bin/bita"]
CMD ["--help"]

# ---------------------------------------------------------------------------
# bita-bench: the benchmark tool, for a job next to a cluster. It measures
# from where it runs, so give it a machine the daemons do not share.
# ---------------------------------------------------------------------------
FROM runtime AS bita-bench

LABEL org.opencontainers.image.title="bita-bench" \
      org.opencontainers.image.description="bitacora benchmark tool"

COPY --from=build /home/builder/out/bita-bench /usr/bin/bita-bench

ENTRYPOINT ["/usr/bin/bita-bench"]
CMD ["--help"]

# ---------------------------------------------------------------------------
# bita-soak: the soak tool, for a job next to a cluster. It injects faults
# through the Kubernetes API, so it needs a pod with a service account.
# ---------------------------------------------------------------------------
FROM runtime AS bita-soak

LABEL org.opencontainers.image.title="bita-soak" \
      org.opencontainers.image.description="bitacora soak tool"

COPY --from=build /home/builder/out/bita-soak /usr/bin/bita-soak

ENTRYPOINT ["/usr/bin/bita-soak"]
CMD ["--help"]
