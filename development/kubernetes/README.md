# Trying bitacora on a Kubernetes cluster

What you need to put a build of bitacora on a cluster you have, use it, and measure it. It is for trying
things out: the etcd here has no TLS, no authentication and no backups, and nothing is tiered to an
object store. [docs/operations](../../docs/operations/README.md) is the guide for a cluster that matters.

| File | What it is |
| --- | --- |
| `etcd.yaml` | A three-member etcd, one member per zone, each on a 10 GiB volume |
| `values.yaml` | Values of the chart for that etcd: three replicas with 100 GiB each, no object store |
| `run.sh` | Runs `bita` or `bita-bench` in a pod next to the cluster and prints its output |
| `s3.yaml` | An S3 server on one 100 GiB volume, so that segments can be tiered |
| `values-s3.yaml` | Values of the chart that point it at that server, on top of `values.yaml` |
| `cluster-s3.toml` | The cluster configuration for a tool that must reach that server |
| `prometheus.yaml` | A Prometheus of the namespace's own that evaluates the alert rules of `src/prometheus` |
| `soak.yaml` | A Job that runs `bita-soak` next to the cluster for a night, with the permissions its faults need |

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
  `volumeClaimTemplates` in `etcd.yaml` for etcd, and likewise in `s3.yaml` and `prometheus.yaml`. The
  cluster and its etcd take six volumes and 330 GiB; the S3 server and the Prometheus add one each, 100
  and 10 GiB.
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

## Tiering

Without an object store every segment stays on the replicas. `s3.yaml` runs one for a test, and
`values-s3.yaml` points the chart at it and shortens the waits, so that a sealed segment is uploaded after
a minute and its copies on the replicas are released two minutes later:

```sh
kubectl --context "$BITA_KUBE_CONTEXT" --namespace bitacora create secret generic bitacora-s3 \
    --from-literal=AWS_ACCESS_KEY_ID="$(openssl rand -hex 12)" \
    --from-literal=AWS_SECRET_ACCESS_KEY="$(openssl rand -hex 24)"
kubectl --context "$BITA_KUBE_CONTEXT" --namespace bitacora apply --filename development/kubernetes/s3.yaml

helm upgrade bitacora src/helm/bitacora --kube-context "$BITA_KUBE_CONTEXT" --namespace bitacora \
    --values development/kubernetes/values.yaml --values development/kubernetes/values-s3.yaml \
    --set image.tag="$BITA_VERSION"

development/kubernetes/run.sh bita segment list demo-bench
```

The listing names the object of each segment once it is uploaded, and its state turns from `tiered` to
`released` when the replicas let go of their copies. From then on a tool needs the object store to read
the segment, and so its configuration and its credentials, which the etcd endpoints alone do not give:

```sh
export BITA_CLUSTER_CONFIG=development/kubernetes/cluster-s3.toml
export BITA_CREDENTIALS_SECRET=bitacora-s3

development/kubernetes/run.sh bench read demo-bench --seconds 60
development/kubernetes/run.sh bita metadata snapshot list
```

Without them a read of a released segment stops with "the segment is in the object store, and this client
was given none".

## Watching it

`prometheus.yaml` runs a Prometheus that scrapes the replicas, the controllers and etcd, and evaluates the
default alert rules. It is the namespace's own on purpose. The chart can create a `ServiceMonitor`, but
where a shared Prometheus selects every `ServiceMonitor` of the cluster, that sends a test's metrics to a
monitoring service other people rely on.

```sh
kubectl --context "$BITA_KUBE_CONTEXT" --namespace bitacora create configmap bitacora-alerts \
    --from-file=src/prometheus/bitacora-alerts.yaml
kubectl --context "$BITA_KUBE_CONTEXT" --namespace bitacora apply --filename development/kubernetes/prometheus.yaml
kubectl --context "$BITA_KUBE_CONTEXT" --namespace bitacora port-forward service/prometheus 9090
```

The alert that says a flush takes more than 10 ms quotes the disks of the design. On network block
storage it fires under load, and it is right to.

## Soaking

[`bita-soak`](../../docs/soak.md) drives the cluster for hours, kills its pods and cuts them off the
network, and has the simulator's checker judge what clients saw. `soak.yaml` runs it as a Job for eight
hours, with a service account that may do what the faults need and nothing else: list and delete pods,
create and delete network policies.

```sh
sed "s/BITA_VERSION/$BITA_VERSION/" development/kubernetes/soak.yaml |
    kubectl --context "$BITA_KUBE_CONTEXT" --namespace bitacora apply --filename -
kubectl --context "$BITA_KUBE_CONTEXT" --namespace bitacora logs --follow job/bita-soak
```

The log has one line of JSON per round on standard output and a summary at the end. The run was clean if
the Job succeeded; the exit code of its pod says how it ended otherwise (`docs/soak.md` lists them):

```sh
kubectl --context "$BITA_KUBE_CONTEXT" --namespace bitacora get pods --selector job-name=bita-soak \
    --output jsonpath='{.items[0].status.containerStatuses[0].state.terminated.exitCode}'
```

What to know before the first night:

- **Try the alarm once.** Add `- --drill-lost-record` to the arguments and apply: the first round must
  end as `violated` and the pod with exit code 3. It says nothing about the cluster; it shows that a
  violation reaches whoever watches the Job. Remove the argument afterwards.
- **Network cuts need a network that enforces policies.** Where it does not, the tool finds out within
  a minute, says so, gives the cuts up and goes on with the kills. Every line then lists the kinds under
  `faults_disabled`. A clean night on such a cluster is a night of kills survived, not of partitions.
- **Three replicas are enough, more are better.** A stream has three copies. With exactly three replicas
  a writer that needs a new segment while one replica is down may have to wait for it, which slows a
  round down and breaks nothing.
- **Another namespace.** The namespace is written once in `soak.yaml`, in the subject of the
  RoleBinding. Change it there and in the command.
- **A soak without faults** needs no permissions: `- --no-faults` in the arguments.
- **After a run that was not clean** the streams of the last round are kept, the pod stays up for a day,
  and the histories of the round are in `/dumps` in the pod. The image has no shell, so they are read
  through a debug container that shares the tool's processes:

  ```sh
  kubectl --context "$BITA_KUBE_CONTEXT" --namespace bitacora debug <pod> --image=busybox \
      --target=bita-soak --profile=general --stdin --tty -- ls /proc/1/root/dumps
  ```

- **Before the next run** delete the Job: `kubectl ... delete job bita-soak`. A tool that was killed
  outright may have left a network cut; the next run removes it when it starts, and
  `kubectl ... delete networkpolicy bita-soak-cut-replica bita-soak-cut-etcd` does it by hand.

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
- `//development/kubernetes:run_test` also covers a tool that is given a cluster configuration and a
  Secret: the file reaches the pod as a ConfigMap of the pod's name, the tool is started with it, and
  the ConfigMap goes with the pod.
- `//src/helm:chart_test` renders the chart with `values.yaml`, and with `values-s3.yaml` on top, and
  hands the rendered configuration to the daemons, like it does for the chart's own examples.
- `s3.yaml`, `prometheus.yaml` and `cluster-s3.toml` have no test: they were used on a real cluster,
  where segments were tiered, released and read back, and the alert rules were evaluated.
- `//development/kubernetes:soak_test` checks `soak.yaml` against the tool: the Role grants exactly the
  four permissions the tool uses, the Job starts the tool with arguments it accepts and as the service
  account the Role is bound to, the histories go to a mounted volume, and a failed run is not started
  again. `//development/kubernetes:soak_rejection_test` hands the same checker a manifest with eight
  known mistakes and requires each to be named. Neither needs a cluster, and neither shows that a
  cluster accepts the manifest. `soak.yaml` and the commands of the section above had not been applied
  to a cluster when they were written: apply with `--dry-run=server` first.
- `etcd.yaml` has no test: it was applied to a real cluster when this directory was written. Apply it
  again, with `--dry-run=server` first, after changing it.
