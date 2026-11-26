# 0018. A soak tool judges a real cluster with the simulator's checker

Status: accepted · 2026-11-26

## Context

The exit gate of the hardening milestone is "a week of nightly real-cluster
runs with no invariant violation" (design, section 18). The deterministic
simulator (record 0008) runs the real code under injected faults and judges
every run, but in a world it controls: memory for disks, links for the
network, one process. `bita-bench` drives a real cluster and judges nothing.
Nothing could drive a real cluster for days, break it, and say whether what
its clients saw was correct.

The design lists a fault schedule among the options of the benchmark tool
(section 16). It does not say who decides that a run was correct.

## Decision

**The judge is the simulator's.** `bita-soak` records what its clients are
told into `bitacora_sim::History` and hands it to `bitacora_sim::check` and
`check_audit`. There is one definition of a correct history, and a rule
added to it for the simulator holds for real clusters the next night. The
event types are not changed: they are part of the simulator's fingerprint.

**A run is a sequence of rounds.** A round creates its streams, drives
them under faults, lets the cluster heal, reads every stream back from its
low-water mark to its end, and judges. A clean round deletes its streams and
drops its histories. A history of a week would need the memory of a week,
and streams of a week the disks; a round needs what a round needs. A round
that is not clean ends the run: its streams and its histories are the
evidence, and more rounds would write over the state that explains it.

**Unknown stays unknown, and not audited is a verdict.** An append whose
writer did not learn the outcome is recorded as unknown, and the audit
begins with a takeover that settles it. A round whose streams could not all
be read back within the healing time says so, as `not_audited`, and ends
the run as `not_recovered`. The tool never reports a round as clean that it
did not audit.

**A program of its own, below the simulation.** It depends on
`bitacora-sim`, and nothing depends on it. The benchmark tool states what
it measured and owns no verdict; a tool that kills pods needs permissions a
benchmark must not have.

**Faults are four calls to the Kubernetes API.** List pods, delete a pod,
create a network policy, delete a network policy, with the service account
of the tool's pod, over the HTTP client and the TLS implementation the S3
backend already builds. A kill is the deletion of a pod with a grace period
of zero. A cut is a `networking.k8s.io/v1` NetworkPolicy that selects the
pods and allows nothing. The targets come from what the chart guarantees: a
replica's pod is the first label of its registered address, the leading
controller's pod is the name in the leadership record, a zone is a
replica's first failure domain and a pod's zone label.

**The policies of a cut have fixed names.** Removing every cut is then two
deletions by name, which also removes what an earlier run left behind when
it was killed, and the tool needs no permission to list or to delete by
label. They carry a label that says who made them, for a person who finds
one. The price is one soak per namespace at a time.

**A cut counts only once the tool has seen it work.** A cluster can accept
a NetworkPolicy without rules, describe the pod as isolated, and deliver
every packet, because its network plugin does not enforce policies. That
was found on a real cluster before the first soak. So after creating a cut
the tool watches the registry of replicas. If the replicas behind the cut miss three renewals of their
registration in a row, the cut is in effect, and only then is it counted,
recorded in the histories and left in place for its duration. If they are
still renewing after nine intervals, the cut is deleted, recorded nowhere,
and its kind is given up for the rest of the run; every line of output from
then on lists the kind and the evidence under `faults_disabled`. A week of
clean rounds on such a cluster reads as what it was: a week of kills.

**One fault at a time, and none the design does not claim to survive.** No
fault is injected while the cluster is not healthy. An etcd member is
killed only if there are three; a zone is cut only if the replicas are in
three zones and the zone holds less than half of the etcd members.

## Considered

- **A fault schedule in `bita-bench`**, as the design lists it. A benchmark
  with faults still has no judge, and it would need the permissions.
- **A Kubernetes client library.** A few hundred crates for four requests.
- **`kubectl` in the image.** A second program to ship and keep in step with
  clusters, a shell to run it, and text to parse.
- **A chaos operator.** Another thing to install with permissions over the
  whole cluster, and faults the history would not know about.
- **Cuts with packet filters from a privileged container.** Works without
  policy enforcement, but needs privileged pods on every node. The policy
  is the standard mechanism, and the check makes it safe where it does
  nothing.
- **Recognising the policies by label only.** Needs list or
  delete-collection permissions; fixed names need neither.
- **One history for the whole run.** Simpler, and several gigabytes a day.

## Not done

- **Losing a replica's disk**: deleting the pod's volume claim and the pod,
  so that the replica comes back empty under a new identity. It was tried
  by hand and works. It does not fit the round as it is: "healthy" means
  that every replica the run started with is alive, and here an identity
  goes for good and another appears; the round would have to wait until
  the controller has given the old identity up (300 s by default, half a
  round) and rebuilt its copies, and then remove the dead identity as
  `bita replica remove` does. It also needs a fifth permission. It is the
  next fault to add, as a kind with a recovery of its own.
- **Two faults at once.** The simple case first, for a week.
- **Faults below Kubernetes**: a slow disk, a paused process, a clock that
  jumps. They need access to the nodes.
- **A schedule.** `development/kubernetes/soak.yaml` is a Job for one
  night; whoever wants a week applies it seven times, or wraps it.
- **A test against a real API server.** That the server's certificate is
  accepted through the cluster's authority, and that a network plugin
  enforces a policy, is only seen on a cluster.

## Consequences

- The gate is checkable from logs: a night was clean if the last line of
  its log is a summary with the verdict `clean`, which is also the exit
  code 0 of the Job. What it survived is in `faults`, and what it did not
  try is in `faults_disabled`.
- The service account of the tool may delete any pod of the namespace. It
  belongs in a namespace that holds the cluster under test and nothing
  else.
- The tool knows how the chart names things. A deployment that registers
  other addresses, or whose controllers do not lead under their pod's name,
  needs other targets; the kills then fail as "no such pod" and are not
  counted.
- The check of a cut is timed by the replicas' registration interval, which
  the tool is told and cannot see. Told a shorter interval than the real
  one, it would take a silence between two renewals for a cut.
- A cut lasts longer than asked: the check comes first, and takes three
  registration intervals.
- A round is not a seed. The same seed makes the same choices only as long
  as the cluster answers the same way, which a real one does not.
