# bitacora Helm chart

Runs one bitacora cluster on Kubernetes: the replicas, which store the log, and the controllers, which
place, repair and tier it.

| Resource | Name | What it is |
| --- | --- | --- |
| StatefulSet | `<release>-replica` | The replicas, three by default, each with a volume of its own |
| Service (headless) | `<release>-replica` | Gives each replica the DNS name it advertises |
| Deployment | `<release>-controller` | The controllers, two by default; one leads |
| Service (headless) | `<release>-controller` | Lets Prometheus find each controller |
| ConfigMap | `<release>-replica`, `<release>-controller` | The configuration file of each daemon |
| PodDisruptionBudget | `<release>-replica`, `<release>-controller` | At most one of each down on purpose |
| ServiceAccount | `<release>` | Carries the annotation that gives pods access to the object store |
| ServiceMonitor | `<release>` | Only with `serviceMonitor.enabled`, for the Prometheus Operator |

## What you must provide

The chart deliberately runs bitacora and nothing else.

1. **The images, in a registry your cluster pulls from.** Those of every tagged version, release
   candidates included, are published at `ghcr.io/thewillyhuman/bitacora`, which is the default of
   `image.repository`; set `image.tag` to the version, such as `0.1.0-rc.1`. For a build of your own, run
   `hack/build_images.sh <version>`, push the images, and set `image.repository` to what comes before the
   image name.
2. **An etcd cluster** (`etcd.endpoints`). It holds the metadata: which segments make up each stream and
   which replicas exist. Give it three or five members across zones and its own backups. Two releases
   that share an etcd need different `etcd.keyPrefix`.
3. **An object store**, unless every segment may stay on the replicas (`objectStore.kind: none`, the
   default). For S3 or an S3-compatible service set `objectStore.kind: s3` and `objectStore.s3.bucket`;
   the bucket must exist.
4. **Credentials for it, in a Secret you create** (`objectStore.s3.credentials.existingSecret`). The
   chart reads two keys of that Secret into the environment of the pods. It never creates a Secret and
   never writes a credential into a ConfigMap. Leave the Secret out if the platform gives pods
   credentials, and put the annotation that does it in `serviceAccount.annotations`.
5. **Nodes that say which zone they are in**: the label `topology.kubernetes.io/zone`, and Kubernetes
   1.35 or later. See the next section; without them set `replica.zoneAware: false`.
6. **At least as many nodes as replicas.** Two replicas never share a node, so a fourth replica on a
   cluster of three nodes stays `Pending`.
7. **A StorageClass** for the replicas' volumes (`replica.storage`). Appends are as fast as its fsync.

## How a replica learns who and where it is

A replica must report its failure domains, widest first, so that the controller never puts two copies
of a segment where one failure takes both. On machines an operator writes them in the configuration
file. In a StatefulSet all pods share one file, so the chart writes variables instead:

```toml
replica_id = "${BITA_POD_NAME}-${BITA_POD_UID}"
advertised_address = "${BITA_POD_NAME}.<release>-replica.<namespace>.svc.cluster.local:7000"
failure_domains = ["${BITA_ZONE}", "${BITA_NODE_NAME}"]
```

and the replica replaces each `"${NAME}"` with the environment variable of that name when it reads the
file. The pod sets the variables from what Kubernetes knows about it, through the downward API:

| Variable | Comes from | Value |
| --- | --- | --- |
| `BITA_POD_NAME` | `metadata.name` | `<release>-replica-0`: stable, because it is a StatefulSet |
| `BITA_POD_UID` | `metadata.uid` | Different for every pod Kubernetes creates, also one that takes an old name |
| `BITA_NODE_NAME` | `spec.nodeName` | The node the pod was scheduled to |
| `BITA_ZONE` | the pod's own label `topology.kubernetes.io/zone` | The zone of that node |

There is no init container, no wrapper script and no access to the Kubernetes API.

The identity of a replica belongs to its volume, not to its pod. A replica stores its identity in its
data directory the first time it starts there and keeps it for good; `replica_id` in the file only
names an empty directory. That is why the identity has the pod's UID in it: a replica that restarts on
its volume ignores the new UID and stays who it was, and a replica that lost its volume gets an identity
nobody had before. It must: it holds none of the segment copies its old identity held, and if it
answered under that identity, a writer recovering a stream could take its empty answer for the word of a
replica that used to hold committed records. `bita replica list` therefore shows identities such as
`bitacora-replica-0-0f2b8d5e-6c1a-4e3f-9b7d-2a4c6e8f0b1d`; the address column has the pod's name.

A pod can read its own labels but not those of its node. `BITA_ZONE` works because Kubernetes copies
`topology.kubernetes.io/zone` from the node onto the pod when it schedules it. That is the
`PodTopologyLabels` admission plugin: on by default since Kubernetes 1.35, behind the
`PodTopologyLabelsAdmission` feature gate in 1.33 and 1.34, absent before. It copies the label once,
when the pod is bound to the node, so a node must have its zone label before replicas are scheduled to
it.

If the label is not there, the variable is empty, and the replica refuses to start instead of joining
as if it had a zone. It exits with 2 and logs:

```text
setting `failure_domains` is invalid: a label is empty
```

With `replica.zoneAware: false` the chart leaves the zone out everywhere: replicas report only their
node, are kept on different nodes, and are not spread over zones. That is right for a cluster in one
zone. In a cluster with several zones it lets all copies of a segment land in one of them.

## Installing

```sh
kubectl create namespace storage
kubectl --namespace storage create secret generic bitacora-s3 \
    --from-literal=AWS_ACCESS_KEY_ID=… --from-literal=AWS_SECRET_ACCESS_KEY=…

helm install bitacora src/helm/bitacora --namespace storage \
    --set image.repository=registry.example.org/storage \
    --set 'etcd.endpoints={http://etcd-0.etcd:2379,http://etcd-1.etcd:2379,http://etcd-2.etcd:2379}' \
    --set objectStore.kind=s3 \
    --set objectStore.s3.bucket=bitacora-segments \
    --set objectStore.s3.endpoint=https://s3.example.org \
    --set objectStore.s3.credentials.existingSecret=bitacora-s3
```

`values.yaml` explains every value; `helm show values src/helm/bitacora` prints it. The files in `ci/`
are complete examples: an S3-compatible object store, a larger cluster, a ServiceMonitor, a single
zone.

Values that cannot work stop the installation with a message: an object store of kind `s3` without a
bucket, a kind the daemons do not know, no etcd endpoint. A value the chart does not know, such as a
misspelled name, is ignored without a message, as in every chart without a schema; compare with
`helm show values` when a setting seems to have no effect.

The notes printed after the installation show how to watch the pods become ready and how to run
`bita` against the cluster.

## What an operator must know

- **Ready means working.** A replica is ready once it has registered in etcd, a controller once it has
  completed a round. A pod that runs but is not ready cannot reach etcd, or is not given a zone.
- **A configuration the daemon refuses makes the pod restart with exit code 2.** Kubernetes restarts it
  with growing pauses; `kubectl logs --previous` names the setting.
- **Upgrades and changes of configuration restart one replica at a time**, and wait for it to be ready
  again before the next. A change of values changes the checksum annotation of the pods, which is what
  triggers the restart.
- **Node drains take one replica at a time**, because of the PodDisruptionBudget. A drain that hangs is
  waiting for the previous replica to register again.
- **A pod is given 30 seconds to stop** (`terminationGracePeriodSeconds`); the daemons need at most ten.
- **Removing a replica is not `replica.replicas` minus one.** Lowering the number deletes the pod with
  the highest ordinal and keeps its volume. First drain that replica (`bita replica drain`), lower the
  number, then `bita replica remove` it so that the controller re-creates its copies elsewhere, and
  delete its PersistentVolumeClaim when it has.
- **A replica whose volume is lost comes back as a new replica.** The pod keeps its name, the new volume
  is empty, and the replica registers under a new identity. The old identity stays in the registry
  without a heartbeat; the controller gives it up after five minutes and re-creates its copies. Run
  `bita replica remove <old identity>` to start that at once and to take it off the list.
- **Clients must be inside the cluster network.** Replicas advertise their in-cluster DNS names; the
  chart creates no Ingress, LoadBalancer or NodePort.

## What the chart does not do

- It does not deploy etcd, an object store, or Prometheus.
- No TLS and no authentication between clients, replicas, controllers and etcd: bitacora has neither
  yet. Keep the namespace behind a NetworkPolicy of your own.
- No NetworkPolicy, no autoscaling, no alert rules as a `PrometheusRule`, no schema for the values.
- No operator: nothing replaces a failed replica's volume or rebalances data for you.
- It has never been installed on a cluster by a test of this repository: see `src/helm/README.md` for
  what is tested and what is not.
