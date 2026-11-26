# The `bita-soak` tool

`bita-soak` drives a bitacora cluster for hours or days, breaks it on purpose, and judges what its clients
saw. It is the idea of the deterministic simulator applied to a real cluster: a workload of writers and
readers, a nemesis that injects faults, a history of everything the clients were told, and the
simulator's own checker as the judge. It exists for the exit gate of the hardening milestone (design,
section 18): a week of nightly runs on a real cluster with no invariant violation.

This page is the reference: the options, the faults, the lines the tool prints, its exit codes and its
metrics. Why the tool is built the way it is, is in
[decision record 0018](decisions/0018-soak-tool.md) and in the README of the crate,
`src/rust/bita-soak/README.md`. `development/kubernetes/soak.yaml` runs it as a Job.

## What a run does

A run is a sequence of rounds. Each round is an experiment of its own:

1. It creates its streams, named `<run>-<round>-<number>`.
2. Every stream gets one writer and one reader. The writer appends records with unique payloads at a
   steady pace and takes the stream back by force whenever it loses it, as an application's failover
   would. The reader follows the stream. Both record what they are told: every append with its outcome,
   every record read.
3. Meanwhile the nemesis injects faults, one at a time (see [Faults](#faults)).
4. When the round's time is up, the nemesis stops, every network cut is removed, and the round waits
   until the cluster is healthy: every replica that was alive when the run started has registered within
   the last 15 seconds, and a controller leads.
5. The writers stop. For every stream, a new writer takes the stream over and appends one last record,
   which settles every append whose outcome nobody learned; then the stream is read from its low-water
   mark to that record. This is the audit.
6. The history of every stream is judged by `bitacora_sim::check` and `bitacora_sim::check_audit`: no
   committed record is missing, no record appears that nobody appended, no reader saw the stream
   differently from another, sequence numbers are dense and in order.

A clean round deletes its streams and forgets its history, and the next round begins. A round that is
not clean ends the run.

An append whose outcome the writer did not learn is recorded as unknown, never as failed and never as
done: the checker accepts such a record in the stream and accepts its absence.

## Options

| Option | Default | Meaning |
| --- | --- | --- |
| `--etcd-endpoint <URL>` | required | The client URL of an etcd member that holds the cluster's metadata. Repeat it for every member. |
| `--etcd-key-prefix <PREFIX>` | `/bitacora/` | The text in front of every key of this cluster in etcd. |
| `--run <NAME>` | `soak-<unix seconds>` | The name of the run, which starts the name of every stream it makes. Two runs on one cluster need two names. |
| `--seconds <SECONDS>` | `3600` | How long the run lasts. A round that has begun is finished, so a run ends up to one round later. |
| `--round-seconds <SECONDS>` | `600` | How long the workload and the nemesis of one round run. |
| `--streams <COUNT>` | `4` | How many streams a round writes, each with a writer and a reader of its own. |
| `--rate <APPENDS_PER_SECOND>` | `20` | How many appends a writer makes per second, at most. A writer has one append in flight at a time. |
| `--record-bytes <BYTES>` | `64` | The payload size of every record. A payload is never shorter than its unique part. |
| `--heal-seconds <SECONDS>` | `600` | How long the cluster is given to become healthy after a round's faults, and each stream to be read back. |
| `--faults <KIND,...>` | all five kinds | The kinds of fault to inject, separated by commas. A kind that is named twice is twice as likely. |
| `--no-faults` | off | Inject no faults: rounds are plain workloads, judged. The tool then needs no access to the Kubernetes API. |
| `--fault-interval-seconds <SECONDS>` | `60` | How long the nemesis waits between two faults. |
| `--cut-seconds <SECONDS>` | `30` | How long a network cut stays in place once it was seen to be in effect. |
| `--registration-interval-seconds <SECONDS>` | `5` | How often a replica renews its registration in etcd: the chart's `replica.registrationIntervalSeconds`. The check of a network cut is timed by it. |
| `--retry-cuts-not-in-effect` | off | Keep trying a kind of network cut that was seen not to be in effect, instead of giving the kind up for the rest of the run. |
| `--namespace <NAMESPACE>` | the tool's own | The namespace the cluster's pods run in. |
| `--replica-selector <LABELS>` | `app.kubernetes.io/name=bitacora,app.kubernetes.io/component=replica` | The labels every replica pod carries, as `name=value` pairs separated by commas. |
| `--etcd-selector <LABELS>` | `app.kubernetes.io/name=etcd` | The labels every pod of the etcd cluster carries. |
| `--zone-label <LABEL>` | `topology.kubernetes.io/zone` | The name of the pod label that holds a pod's zone. |
| `--dump-directory <DIRECTORY>` | `/tmp` | Where the histories of a round that is not clean are written. It must exist. |
| `--metrics-listen-address <ADDRESS>` | none | The address to serve `/metrics` on, such as `0.0.0.0:9102`. Without it no metrics are served. |
| `--linger-seconds <SECONDS>` | `0` | How long the tool stays up after a run that broke an invariant or did not recover, so that its files can be copied out of its pod and its metrics scraped. |
| `--seed <NUMBER>` | start time in milliseconds | The seed of every random choice of the run. It is printed to standard error when the run starts. |
| `--drill-lost-record` | off | A drill: the first round claims a commit that never happened. See below. |

`--seconds`, `--round-seconds`, `--streams`, `--rate`, `--fault-interval-seconds` and
`--registration-interval-seconds` must be at least 1.
The tool reads nothing but its command line: no environment variable, no configuration file. The one
exception is what every pod is given to reach the Kubernetes API, which the tool uses unless it runs with
`--no-faults`.

The defaults are modest on purpose. A soak is about days of correct behaviour, not about load: four
streams at twenty appends a second are 80 appends a second for the whole cluster.

## Faults

The tool runs in a pod next to the cluster and injects faults through the Kubernetes API, with the
service account of its pod.

| Kind | What the tool does | How it ends |
| --- | --- | --- |
| `kill-replica` | Deletes the pod of one replica with a grace period of zero: the process is killed, not asked to stop. | The StatefulSet starts the pod again, with its volume. |
| `kill-controller` | Deletes the pod of the controller that leads, the same way. | The Deployment starts a new pod; a controller takes the leadership once the old one has expired. |
| `kill-etcd-member` | Deletes the pod of one etcd member, the same way. | Its StatefulSet starts it again. |
| `cut-replica` | Creates a NetworkPolicy that selects the pod of one replica and allows nothing in or out. | The tool deletes the policy after `--cut-seconds`. |
| `cut-zone` | Creates two such policies: one for the replicas of one zone, one for the etcd members of that zone. | The tool deletes both policies after `--cut-seconds`. |

The rules of the nemesis:

- **One fault at a time.** A fault is due every `--fault-interval-seconds`. If the cluster is not healthy
  then, the fault before is still being recovered from, and nothing is injected.
- **Nothing the design does not claim to survive.** An etcd member is killed only if there are at least
  three. A zone is cut only if the replicas are in at least three zones, every replica registered a
  zone, and the zone holds less than half of the etcd members.
- **Nothing that did not happen is counted.** A kill counts once the API server has deleted the pod; a
  pod that is not there is an error, not a fault. A cut counts once it was seen to be in effect.

### What the tool needs to know about the cluster

The targets are found from what the chart guarantees:

- A replica's pod is the first label of the address the replica registered in etcd.
- The leading controller's pod is the name the controller wrote into its leadership record.
- A replica's zone is its first failure domain, when the chart's `replica.zoneAware` is on.
- The etcd members are the pods with the labels of `--etcd-selector`; their zone is the pod label
  `--zone-label`. `development/kubernetes/etcd.yaml` labels them as the default expects.

### Network cuts need a cluster that enforces network policies

A NetworkPolicy without rules isolates the pods it selects only if the cluster's network plugin enforces
network policies. Where it does not, the API server accepts the policy, the pod is described as isolated,
and every packet still goes through. A soak that counted such cuts would report a week of clean rounds as
a week of survived partitions that never happened.

So the tool checks every cut before it counts. A replica that is cut off cannot reach etcd, and its
registration there stops being renewed. After creating the policies the tool reads the registry once a
second:

- If no replica behind the cut has renewed its registration for three registration intervals (15 seconds
  with the chart's default), the cut is in effect. Only now is it counted, recorded in the histories, and
  left in place for `--cut-seconds`.
- If nine intervals pass (45 seconds) and a replica behind the cut is still renewing, the cut is not in
  effect. The tool deletes the policies, records nothing, counts nothing, and says so on standard error:
  `bita-soak: network cuts are not enforced on this cluster: ...`. The kind is given up for the rest of
  the run, and every line the tool prints from then on lists it under `faults_disabled` with the reason.
  With `--retry-cuts-not-in-effect` the kind is tried again at a later interval instead; a cut still
  counts only when it was seen to be in effect.
- If the registry cannot be read, or the tool is told to stop, the tool has no evidence either way: the
  cut is removed and not counted, and the kind is tried again.

On a cluster that does not enforce policies the kills are the faults that run. A `faults_disabled` that is
not empty means: this run says nothing about the kinds it lists.

The check is only as good as the interval it is timed by. If the chart's
`replica.registrationIntervalSeconds` was raised, pass the same number as
`--registration-interval-seconds`: with replicas that register every minute, fifteen seconds of silence
would prove nothing.

Keep `--cut-seconds` plus the nine intervals of the check well below the controller's
`replicaLostAfterSeconds` (300 by default). A replica that is cut off for longer is given up, and its
copies are rebuilt elsewhere.

The kubelet's probes come from the node, and most network plugins let them through a policy. Where they do
not, a cut that outlasts the liveness probe restarts the replica, and the cut has become a kill.

### How a cut is removed

A cut is removed when its time is up, when the round ends, and when the tool is told to stop by SIGTERM
or SIGINT. A tool that is killed outright leaves its policies behind, so every run begins by removing
them. They have fixed names, `bita-soak-cut-replica` and `bita-soak-cut-etcd`, and carry the label
`app.kubernetes.io/managed-by: bita-soak`; to remove them by hand:

```sh
kubectl --namespace <namespace> delete networkpolicy bita-soak-cut-replica bita-soak-cut-etcd
```

Because of the fixed names, one soak per namespace at a time.

### Permissions

The service account of the tool's pod needs exactly this, in the cluster's namespace:

| API group | Resource | Verbs | For |
| --- | --- | --- | --- |
| core | `pods` | `list`, `delete` | Finding the etcd members; the kills. |
| `networking.k8s.io` | `networkpolicies` | `create`, `delete` | The cuts and their removal. |

`development/kubernetes/soak.yaml` has the Role, and a test keeps it at exactly these four. A run begins
by deleting the policies of an earlier run, so a missing permission ends it within a minute with exit
code 1 and the API server's answer.

Every call to the API has a time limit of ten seconds and is sent up to three times. The service account's
token is read from its file for every call, because Kubernetes replaces it every hour.

## Output

Standard output holds one line of JSON per round and one for the run, and nothing else. Everything else
goes to standard error, each line starting with `bita-soak:`. A line is printed as soon as its round
ends, so the log is complete up to the moment the tool dies, whatever kills it.

A round:

```json
{"kind":"round","run":"soak-1790000000","round":1,"started_at_unix_millis":1790000000123,"ended_at_unix_millis":1790000603456,"streams":4,"appends_committed":47012,"appends_unknown":9,"appends_refused":0,"records_read":94020,"faults":{"cut_replica":0,"cut_zone":0,"kill_controller":3,"kill_etcd_member":2,"kill_replica":3},"faults_not_injected":2,"faults_disabled":{"cut_replica":"network cuts are not enforced on this cluster: replica bitacora-replica-2 is cut off the network, but replica bitacora-replica-2 renewed its registration 1.2s ago, from behind the cut"},"takeovers":9,"worst_append_latency_milliseconds":4212,"events":188091,"verdict":"clean","violations":[],"reason":null,"dumps":[],"drill":false}
```

| Field | Meaning |
| --- | --- |
| `kind` | `round`. |
| `run`, `round` | The name of the run and the number of the round within it, from 1. |
| `started_at_unix_millis`, `ended_at_unix_millis` | When the round began and ended. |
| `streams` | How many streams the round wrote. |
| `appends_committed` | Appends the cluster reported as committed, the audit's last record of every stream included. |
| `appends_unknown` | Appends that ended without their writer learning the outcome: the writer was fenced, lost contact, or was stopped in the middle. |
| `appends_refused` | Appends that were refused before they got a sequence number. |
| `records_read` | Records handed to the readers and to the audit. A clean round reads every record about twice. |
| `faults` | How many faults of each kind were injected. A cut is counted only if it was seen to be in effect. |
| `faults_not_injected` | How many times a fault was due and none was injected: the cluster was not healthy yet, it offered no target, the API refused, every kind asked for was given up, or a network cut was not in effect. |
| `faults_disabled` | The kinds of fault that were asked for and given up, each with the reason. A kind listed here did not happen: the cluster was not shown to survive it. |
| `takeovers` | How many times a writer had to open its stream again. |
| `worst_append_latency_milliseconds` | The longest time between an append and its commit. |
| `events` | How many events the histories of the round held when it was judged. |
| `verdict` | `clean`, `violated`, `not_audited` or `interrupted`. See below. |
| `violations` | What the checker found, each text starting with the stream. Empty unless the verdict is `violated`. |
| `reason` | Why the round was not audited, or which cut an interrupted round left in place; otherwise `null`. |
| `dumps` | The files the histories were written to. Empty for a clean or interrupted round. |
| `drill` | Whether this round planted the lost record of `--drill-lost-record`. |

| Verdict of a round | Meaning | What the round leaves |
| --- | --- | --- |
| `clean` | Every stream was read back whole and the checker found nothing. | Nothing: the streams are deleted. |
| `violated` | The checker found a broken invariant. | The streams, and one file per stream with its history. |
| `not_audited` | The cluster did not recover within `--heal-seconds`: the streams could not be created, a cut could not be removed, the cluster did not become healthy, or a stream could not be read back. Nothing is known about the invariants. | The streams, and one file per stream with its history. |
| `interrupted` | The tool was told to stop. | Nothing: the streams are deleted. |

A round is never reported as clean unless every one of its streams was audited.

The run:

```json
{"kind":"summary","run":"soak-1790000000","verdict":"clean","rounds":6,"clean_rounds":6,"appends_committed":282072,"appends_unknown":51,"appends_refused":0,"records_read":564120,"faults":{"cut_replica":0,"cut_zone":0,"kill_controller":17,"kill_etcd_member":14,"kill_replica":19},"faults_disabled":{"cut_replica":"network cuts are not enforced on this cluster: ..."},"takeovers":51,"worst_append_latency_milliseconds":4212,"reason":null}
```

The fields are the sums over the rounds, the worst latency of any round, the kinds of fault that were
given up during the run, and the verdict of the run with its reason. A run was clean if and only if its
last line is a summary whose `verdict` is `clean`; a log without a summary is the log of a tool that was
killed. What a clean run survived is in `faults`, and what it did not try is in `faults_disabled`.

A dump holds one event per line, in the format of the simulator's `BITA_SIM_DUMP`: the time since the
round began, then the event.

```text
     1.250s AppendInvoked { operation: 24, payload: b"soak-1790000000-1-0/24........................................." }
     1.262s AppendFinished { operation: 24, outcome: Committed { sequence: SequenceNumber(25), epoch: Epoch(1) } }
    61.004s Fault { description: "replica bitacora-replica-2 is killed" }
```

## Exit codes

| Exit code | Verdict of the run | Meaning |
| --- | --- | --- |
| 0 | `clean` | The run lasted as long as it was asked to and every round was clean. |
| 1 | `could_not_start` | The cluster or the Kubernetes API could not be reached or used, or no replica was registered and alive. |
| 2 | none | The command line is wrong. Nothing was done. |
| 3 | `violated` | A round broke an invariant. The run ended there. |
| 4 | `not_recovered` | A round could not be audited. The run ended there. |
| 5 | `interrupted` | The tool was told to stop by SIGTERM or SIGINT. It removed its cuts, finished in order and printed its lines. |

## The drill

`--drill-lost-record` makes the first round claim, in its history, a commit that never happened. The
cluster is not touched. The round must end as `violated` and the run with exit code 3: that proves that
the alarm is wired, from the checker to whatever watches the exit code, and says nothing about the
cluster. The line of a drill round has `"drill":true`. Run it once after setting a soak up, and never as
the nightly run.

## Metrics

With `--metrics-listen-address` the tool serves `/metrics` in the text format of the daemons. The counters
are raised once per round, when the round ends.

| Metric | Counts |
| --- | --- |
| `bitacora_soak_rounds_clean_total` | Rounds that were clean. |
| `bitacora_soak_rounds_violated_total` | Rounds that broke an invariant. |
| `bitacora_soak_rounds_not_audited_total` | Rounds after which the cluster did not recover in time. |
| `bitacora_soak_appends_committed_total` | Appends reported as committed. |
| `bitacora_soak_appends_unknown_total` | Appends that ended without their writer learning the outcome. |
| `bitacora_soak_appends_refused_total` | Appends that were refused. |
| `bitacora_soak_records_read_total` | Records handed to readers. |
| `bitacora_soak_takeovers_total` | Times a writer took a stream over. |
| `bitacora_soak_faults_kill_replica_total` | Replica processes killed. |
| `bitacora_soak_faults_kill_controller_total` | Leading controllers killed. |
| `bitacora_soak_faults_kill_etcd_member_total` | etcd members killed. |
| `bitacora_soak_faults_cut_replica_total` | Replicas cut off the network, counted once the cut was seen to be in effect. |
| `bitacora_soak_faults_cut_zone_total` | Zones cut off the network, counted once the cut was seen to be in effect. |

A run that is not clean ends the process, and with it the metrics, unless `--linger-seconds` keeps it up.
The signal to alert on is the exit code. A test keeps this table and the tool in step.

## Memory

The tool keeps the history of the round in play and nothing of the rounds before it. Per append a history
holds an event for the call, one for its return, and two for the record being read (by the reader and by
the audit). An event is 96 bytes, and the payload is kept up to three times: about 0.7 KiB per append at
the default record size, and about 2 KiB while the round is judged, when the events are copied for the
checker and indexed by it. A default round is 4 streams × 20 appends a second × 600 seconds = 48 000
appends, which is about 35 MiB while it runs and about 100 MiB while it is judged. That is the peak for a
run of any length: a week of rounds needs what one round needs.

The numbers grow with `--streams`, `--rate`, `--round-seconds` and `--record-bytes`. To stay within
1 GiB, keep `streams × rate × round-seconds` under about 400 000 appends at the default record size.
