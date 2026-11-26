# Helm

| Path | What it is |
| --- | --- |
| `bitacora/` | The chart that runs a bitacora cluster on Kubernetes; its `README.md` is the operator's guide |
| `bitacora/ci/` | Complete values files, one per kind of installation; each is rendered by the test |
| `chart_test.sh` | The test of the chart |
| `broken_chart_test.sh` | Breaks a copy of the chart on purpose, to test the test |

`hack/create_release.sh` sets `version` and `appVersion` in `bitacora/Chart.yaml` to the version it
releases.

## How the chart is tested

Without a cluster, with the pinned `helm`:

- `//src/helm:chart_test` runs `helm lint --strict` and `helm template` with the default values and
  with every file of `bitacora/ci/`, then checks what was rendered:
  - the two configuration files are handed to the real `bita-replica` and `bita-controller` with
    `--check-config`, with the environment a pod would have, and must be accepted; every variable they
    refer to must be one the pod sets; a replica whose zone is empty must refuse to start;
  - every setting the chart writes with its default value must have the value of the daemon's own
    `example.toml`;
  - probes are on `/healthz` and `/readyz`, on a port the container names; pods get longer to stop than
    the daemons need; pods run as the user the images are built for, not as root, with a read-only
    root file system; the ports of pods and Services are the ports the configuration listens on;
  - replicas never share a node, are spread over zones, and at most one may be taken down at a time;
  - no Secret is created and no ConfigMap holds a credential; the S3 access key reaches the pods from
    the operator's Secret;
  - each values file changes what it says it changes;
  - values that cannot work are refused with a message;
  - every value of `values.yaml` has a comment above it and is used by a template, and every value a
    template uses exists in `values.yaml` (helm renders a misspelled value as nothing, silently);
  - `Chart.yaml` carries the version of the repository.
- `//src/helm:chart_rejection_test` runs the same test on a chart with twelve known mistakes and
  requires each to be named.
- `//src/helm:shellcheck_test` lints the two scripts.

Each takes two to three seconds.

What this does not test: the chart has never been installed. Nothing here checks the manifests against
the Kubernetes API schema, that pods get scheduled, that the zone label really reaches a pod, or that a
rolling upgrade keeps a cluster available. Those need a cluster, which a hermetic test does not have.
Before the first production use, install it on a real cluster, drain a node, and upgrade it once.
