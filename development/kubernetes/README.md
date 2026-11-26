# Trying bitacora on a Kubernetes cluster

What you need to put a build of bitacora on a cluster you have, use it, and measure it. It is for trying
things out: the etcd here has no TLS, no authentication and no backups, and nothing is tiered to an
object store. [docs/operations](../../docs/operations/README.md) is the guide for a cluster that matters.

| File | What it is |
| --- | --- |
| `etcd.yaml` | A three-member etcd, one member per zone, each on a 10 GiB volume |
| `values.yaml` | Values of the chart for that etcd: three replicas with 100 GiB each, no object store |
| `run.sh` | Runs `bita` or `bita-bench` in a pod next to the cluster and prints its output |

## The images

A cluster pulls the images of a tagged version from `ghcr.io/thewillyhuman/bitacora`. To try a commit
that is not released, tag it as a release candidate and push the tag; the images are there about ten
minutes later, and nothing else comes of the tag:

```sh
git tag --annotate v0.1.0-rc.1 --message "bitacora v0.1.0-rc.1, a release candidate"
git push origin v0.1.0-rc.1
```

## From nothing to a record read back

Name the cluster in every command. A kubeconfig often holds more than one, and the current one is not
always the one you mean, which is why `run.sh` refuses to start without `BITA_KUBE_CONTEXT`.

```sh
export BITA_KUBE_CONTEXT=<the kubectl context of the cluster>
export BITA_VERSION=0.1.0-rc.1

kubectl --context "$BITA_KUBE_CONTEXT" create namespace bitacora
kubectl --context "$BITA_KUBE_CONTEXT" --namespace bitacora apply --filename development/kubernetes/etcd.yaml
kubectl --context "$BITA_KUBE_CONTEXT" --namespace bitacora rollout status statefulset/etcd

helm install bitacora src/helm/bitacora --kube-context "$BITA_KUBE_CONTEXT" --namespace bitacora \
    --values development/kubernetes/values.yaml --set image.tag="$BITA_VERSION"
kubectl --context "$BITA_KUBE_CONTEXT" --namespace bitacora rollout status statefulset/bitacora-replica

development/kubernetes/run.sh bita replica list
development/kubernetes/run.sh bita stream create demo
development/kubernetes/run.sh bita stream append demo "hello"
development/kubernetes/run.sh bita stream read demo
```

What to change for your cluster:

- **Volumes.** Both files take the default StorageClass. Name another with
  `--set replica.storage.storageClassName=<class>` for the replicas, and with a `storageClassName` under
  `volumeClaimTemplates` in `etcd.yaml` for etcd. The whole setup takes six volumes and 330 GiB.
- **Replicas.** `--set replica.replicas=<n>` runs more than three. Each needs a node of its own, and the
  chart spreads them evenly over the zones.
- **Images through a mirror.** Change `image` in `etcd.yaml` and `--set image.repository=<mirror path>`.
- **No zones.** Remove `topologySpreadConstraints` from `etcd.yaml` and add `--set replica.zoneAware=false`.

## Measuring

`run.sh bench` runs [`bita-bench`](../../docs/benchmark.md) with the arguments it is given:

```sh
development/kubernetes/run.sh bench append demo-bench --create --seconds 60
development/kubernetes/run.sh bench read demo-bench --seconds 60
```

The benchmark pod asks for two CPUs and does not share a node with a replica, because the tool is part
of what it measures. So keep one node without a replica: on a cluster with six nodes, run five. The pod
stays pending, and the script gives up after `BITA_RUN_TIMEOUT_SECONDS`, if there is no such node.

A replica reports how long its disk takes to make a write durable, which is what an append mostly waits
for. Read it without exposing anything, through the API server:

```sh
kubectl --context "$BITA_KUBE_CONTEXT" get --raw \
    /api/v1/namespaces/bitacora/pods/bitacora-replica-0:9100/proxy/metrics | grep disk_flush_duration
```

## Removing it

```sh
helm uninstall bitacora --kube-context "$BITA_KUBE_CONTEXT" --namespace bitacora
kubectl --context "$BITA_KUBE_CONTEXT" delete namespace bitacora
```

Deleting the namespace deletes the volume claims of etcd and of the replicas with it. Whether the
volumes behind them go too is up to the reclaim policy of their StorageClass.

## How this directory is tested

- `//development/kubernetes:run_test` runs `run.sh` with a stand-in for `kubectl`: every call names the
  context and the namespace, the pod is the right image with the right arguments and placement, it is
  removed afterwards, and the exit code says how the tool ended.
- `//src/helm:chart_test` renders the chart with `values.yaml` and hands the rendered configuration to
  the daemons, like it does for the chart's own examples.
- `etcd.yaml` has no test: it was applied to a real cluster when this directory was written. Apply it
  again, with `--dry-run=server` first, after changing it.
