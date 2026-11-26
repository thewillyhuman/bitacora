# Container images

One `Dockerfile` builds the four images of bitacora, each a stage of it:

| Image | Starts | Ports | Mount |
| --- | --- | --- | --- |
| `bita-replica` | `/usr/bin/bita-replica --config /etc/bitacora/replica.toml` | 7000 (gRPC), 9100 (metrics, probes) | the configuration file; a volume at `/var/lib/bitacora` |
| `bita-controller` | `/usr/bin/bita-controller --config /etc/bitacora/controller.toml` | 9101 (metrics, probes) | the configuration file |
| `bita` | `/usr/bin/bita` | none | nothing; pass `--etcd-endpoint` or mount a configuration file |
| `bita-bench` | `/usr/bin/bita-bench` | none | nothing; pass `--etcd-endpoint` or mount a configuration file |

| File | What it is |
| --- | --- |
| `Dockerfile` | The `build` stage compiles with Bazel (`--config=release`); `runtime` is what the images share |
| `Dockerfile.dockerignore` | What the build must not read from the repository |
| `hadolint.yaml` | The settings of the linter, with the reason for each rule that is off |

## Building

```sh
hack/build_images.sh 0.1.0
```

builds `bitacora/bita-replica:0.1.0`, `bitacora/bita-controller:0.1.0`, `bitacora/bita:0.1.0` and
`bitacora/bita-bench:0.1.0` for the machine's platform and prints the four tags. `BITA_IMAGE_REPOSITORY` changes what comes before the
image name, `BITA_IMAGE_PLATFORM` the platform (`linux/amd64` or `linux/arm64`), and
`BITA_CONTAINER_TOOL` the program that builds. Nothing is pushed, and no registry has these images
yet: push the tags to the registry your cluster pulls from.

The first build downloads the Rust toolchain and compiles everything: about four minutes on a recent
laptop. Bazel's caches live in a BuildKit cache mount, so later builds compile only what changed. The
build needs BuildKit, which every Docker since 23.0 uses by default. Each image is about 75 MB.

## What is in an image

The images that run are [distroless](https://github.com/GoogleContainerTools/distroless): the C
library, the certificate authorities (an S3 object store is reached over TLS) and the one program. There
is no shell and no package manager, so `docker exec … sh` does not work; use
`kubectl debug --image=…` or a second container to look around.

The process runs as user and group 65532, never as root, and the user is a number so that Kubernetes
can enforce `runAsNonRoot`. The replica image owns `/var/lib/bitacora`, so a volume created for it is
writable. A volume that already exists with another owner is not: on Kubernetes set
`fsGroup: 65532`, as the Helm chart does.

## Probes

The images have no `HEALTHCHECK`. There is no program in them that could make the request, and
Kubernetes ignores the instruction anyway. Probe the port the daemon serves metrics on instead:

| Path | Answers 200 when | Use as |
| --- | --- | --- |
| `/healthz` | the process is up and serving | liveness |
| `/readyz` | the replica has registered in the cluster; the controller has completed a round | readiness |

## Exit codes

A daemon exits with 0 when it was asked to stop, with 1 when it could not start or had to stop, and
with 2 when its command line or configuration is wrong. A container runtime restarts all of them
alike; `docker logs` or `kubectl logs --previous` shows a line that names the file and the setting when
the code was 2. To check a configuration before it reaches a cluster:

```sh
docker run --rm --volume "$PWD/replica.toml:/etc/bitacora/replica.toml:ro" \
    bitacora/bita-replica:0.1.0 --config /etc/bitacora/replica.toml --check-config
```

## What these images do not do

- **No image is pinned by digest.** The base images are named by tag (`debian:13-slim`,
  `gcr.io/distroless/cc-debian13:nonroot`), so a rebuild picks up their security fixes and is not
  reproducible bit for bit.
- **No image is signed or scanned, and none is published** by this repository.
- **One platform per build.** `BITA_IMAGE_PLATFORM` builds for another platform through emulation,
  which is slow; there is no multi-platform manifest.

## How this directory is tested

No test builds an image: that needs a container runtime, which the tests of this repository must not.
The images were built and started by hand when this directory was written; after a change to the
`Dockerfile` that the tests below cannot judge, such as a new base image, do so again with
`hack/build_images.sh` and `docker run --rm bitacora/bita-replica:<version> --version`.

- `//src/docker:hadolint_test` lints the `Dockerfile` with the pinned hadolint; every finding fails,
  down to matters of style.
- `//src/docker:dockerfile_test` checks the `Dockerfile` against what it packs: each image copies and
  starts a program the build stage really builds, from a target that exists; the options a daemon is
  started with are in its own `--help`; the exposed ports are those its example configuration listens
  on; the replica's example data directory is inside the volume, which belongs to the user the image
  runs as; no image runs as root or under a name; the build is a release build with a Bazelisk whose
  checksum is verified; the images are exactly those `hack/build_images.sh` builds.
- `//src/docker:dockerfile_rejection_test` hands the same checker a `Dockerfile` with twelve known
  mistakes and requires each to be named.
- `//hack:build_images_test` runs `hack/build_images.sh` with a stand-in for `docker` and checks the
  file, stage, version and tag of each build.
- `//src/docker:shellcheck_test` lints the scripts.

hadolint takes two to four seconds, the others about a second.
