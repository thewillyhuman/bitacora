//! The faults of a soak, on Kubernetes.
//!
//! # Why this file exists
//!
//! The nemesis speaks of pods and zones; Kubernetes has to be told in its own terms. A kill is the deletion of a
//! pod without a grace period, which the pod's `StatefulSet` or `Deployment` answers with a new pod. A cut is a
//! network policy that selects the pods and allows nothing in or out. This file is that translation, and the one
//! place that knows how the chart labels its pods.
//!
//! # What it contains
//!
//! - [`KubernetesTargets`]: the namespace and the labels by which the cluster's pods are found.
//! - [`KubernetesFaults`]: the fault interface over the Kubernetes API.
//! - [`ServiceAccount`]: what a pod is given to reach the API server, read from its mount.
//! - [`parse_labels`]: reads `name=value,name=value` from the command line.
//!
//! # How it works
//!
//! A cut of one replica is one policy that selects the replica's pod by the label a `StatefulSet` gives each of
//! its pods. A cut of a zone is two policies, one for the replicas and one for the members of the metadata store,
//! each selecting by the zone label. The policies have fixed names, so removing every cut is two deletions by
//! name: the tool needs no permission to list policies, and a cut that an earlier run left behind is removed like
//! one of this run. They also carry a label that says who made them, for a person who finds one.
//!
//! A policy with no rules isolates the pods it selects, but only on a cluster whose network plugin enforces
//! network policies. Where it does not, the API server accepts the policy and nothing changes. This file cannot
//! tell; `cut_check.rs` can, and the nemesis asks it before a cut counts.
//!
//! The tool's own pod carries none of the labels the policies select, so a cut never isolates the tool.
//!
//! # Invariants
//!
//! - A kill of a pod that does not exist is an error, never a fault that happened.
//! - Removing the cuts removes both policies, whatever was cut, and succeeds when neither is there.
//! - Nothing here selects a pod by anything but the labels of [`KubernetesTargets`].
//!
//! # See also
//!
//! - `kubernetes_api.rs`: the calls.
//! - `faults.rs`: the interface implemented here.
//! - `development/kubernetes/soak.yaml`: the service account and the permissions these calls need.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;

use crate::faults::{EtcdMember, Fault, FaultError, FaultInjector};
use crate::kubernetes_api::{ApiClient, ApiError, ApiSettings, Removal};

/// The name of the policy that cuts replicas off: one replica, or those of a zone.
pub(crate) const REPLICA_CUT_POLICY: &str = "bita-soak-cut-replica";

/// The name of the policy that cuts the members of the metadata store of a zone off.
pub(crate) const ETCD_CUT_POLICY: &str = "bita-soak-cut-etcd";

/// The label every policy of the tool carries, and its value: who made this.
const MADE_BY_LABEL: (&str, &str) = ("app.kubernetes.io/managed-by", "bita-soak");

/// The label by which a `StatefulSet` names each of its pods.
const POD_NAME_LABEL: &str = "statefulset.kubernetes.io/pod-name";

/// Where a pod finds what it needs to reach the API server.
pub(crate) const SERVICE_ACCOUNT_DIRECTORY: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// How long one attempt of a call to the API server may take.
const ATTEMPT_TIME_LIMIT: Duration = Duration::from_secs(10);

/// How many times a call is sent before it fails.
const ATTEMPTS: u32 = 3;

/// How long a call waits between two attempts.
const RETRY_PAUSE: Duration = Duration::from_secs(1);

/// The namespace and the labels by which the cluster's pods are found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KubernetesTargets {
    /// The namespace the cluster runs in.
    pub(crate) namespace: String,
    /// The labels every replica pod carries.
    pub(crate) replica_labels: BTreeMap<String, String>,
    /// The labels every pod of the metadata store carries.
    pub(crate) etcd_labels: BTreeMap<String, String>,
    /// The name of the label that holds a pod's zone.
    pub(crate) zone_label: String,
}

/// What a pod is given to reach the API server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ServiceAccount {
    /// The file that holds the account's token.
    pub(crate) token_file: PathBuf,
    /// The certificate authority of the cluster, in PEM.
    pub(crate) certificate_authority_pem: String,
    /// The namespace the pod runs in.
    pub(crate) namespace: String,
}

impl ServiceAccount {
    /// Reads the service account that is mounted at `directory`.
    ///
    /// # Errors
    ///
    /// Returns the text to show if the certificate authority or the namespace cannot be read: the tool does not
    /// run in a pod, or the pod was told not to mount its service account.
    pub(crate) fn read_from(directory: &Path) -> Result<Self, String> {
        Ok(Self {
            token_file: directory.join("token"),
            certificate_authority_pem: read_mounted_file(&directory.join("ca.crt"))?,
            namespace: read_mounted_file(&directory.join("namespace"))?.trim().to_owned(),
        })
    }

    /// The settings of a client for the API server at `host` and `port`, which is where the environment of every
    /// pod says it is.
    pub(crate) fn api_settings(&self, host: &str, port: &str) -> ApiSettings {
        // An IPv6 address needs brackets to be told from the port.
        let host_in_an_address = if host.contains(':') {
            format!("[{host}]")
        } else {
            host.to_owned()
        };
        ApiSettings {
            base_url: format!("https://{host_in_an_address}:{port}"),
            token_file: self.token_file.clone(),
            certificate_authority_pem: self.certificate_authority_pem.clone(),
            attempt_time_limit: ATTEMPT_TIME_LIMIT,
            attempts: ATTEMPTS,
            retry_pause: RETRY_PAUSE,
        }
    }
}

/// Reads one file of a service account mount.
fn read_mounted_file(path: &Path) -> Result<String, String> {
    match std::fs::read_to_string(path) {
        Ok(content) => Ok(content),
        Err(io_error) => Err(format!("{} cannot be read: {io_error}", path.display())),
    }
}

/// Reads labels as they are written on a command line: `name=value` pairs separated by commas.
///
/// # Errors
///
/// Returns the text to show if a pair has no `=`, an empty name, or an empty value, or if there is no pair at all:
/// a selector that selects every pod must not be the result of a typing mistake.
pub(crate) fn parse_labels(text: &str) -> Result<BTreeMap<String, String>, String> {
    let mut labels = BTreeMap::new();
    for pair in text.split(',') {
        let Some((name, value)) = pair.split_once('=') else {
            return Err(format!("{pair:?} is not name=value"));
        };
        if name.trim().is_empty() || value.trim().is_empty() {
            return Err(format!("{pair:?} is not name=value"));
        }
        labels.insert(name.trim().to_owned(), value.trim().to_owned());
    }
    Ok(labels)
}

/// The fault interface over the Kubernetes API.
#[derive(Clone, Debug)]
pub(crate) struct KubernetesFaults {
    /// The calls.
    api: ApiClient,
    /// Where the cluster's pods are and how they are labelled.
    targets: KubernetesTargets,
}

impl KubernetesFaults {
    /// The faults of the cluster that `targets` describes, made through `api`.
    pub(crate) const fn new(api: ApiClient, targets: KubernetesTargets) -> Self {
        Self { api, targets }
    }

    /// Deletes the pod `pod` without a grace period.
    async fn kill(&self, pod: &str) -> Result<(), FaultError> {
        match self.api.delete_pod(&self.targets.namespace, pod).await {
            Ok(Removal::Removed) => Ok(()),
            Ok(Removal::NothingThere) => Err(FaultError {
                reason: format!("there is no pod called {pod} in namespace {}", self.targets.namespace),
            }),
            Err(api_error) => Err(fault_error(&api_error)),
        }
    }

    /// Creates the policy `name`, which isolates the pods that carry `labels` and the label `extra`.
    async fn isolate(
        &self,
        name: &str,
        labels: &BTreeMap<String, String>,
        extra: (&str, &str),
    ) -> Result<(), FaultError> {
        let mut selected = labels.clone();
        selected.insert(extra.0.to_owned(), extra.1.to_owned());
        let policy = isolating_policy(name, &selected);
        match self.api.create_network_policy(&self.targets.namespace, &policy).await {
            Ok(()) => Ok(()),
            Err(api_error) => Err(fault_error(&api_error)),
        }
    }
}

/// The network policy called `name` that selects the pods with `pod_labels` and allows them nothing: no rule for
/// traffic in, none for traffic out.
fn isolating_policy(name: &str, pod_labels: &BTreeMap<String, String>) -> serde_json::Value {
    json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "name": name,
            "labels": {(MADE_BY_LABEL.0): MADE_BY_LABEL.1},
        },
        "spec": {
            "podSelector": {"matchLabels": pod_labels},
            "policyTypes": ["Ingress", "Egress"],
        },
    })
}

/// The fault error that says what `api_error` says.
fn fault_error(api_error: &ApiError) -> FaultError {
    FaultError {
        reason: api_error.to_string(),
    }
}

#[async_trait]
impl FaultInjector for KubernetesFaults {
    /// Deletes the pod of a kill, or creates the policies of a cut.
    async fn inject(&self, fault: &Fault) -> Result<(), FaultError> {
        let targets = &self.targets;
        match fault {
            Fault::KillReplica { pod } | Fault::KillController { pod } | Fault::KillEtcdMember { pod } => {
                self.kill(pod).await
            }
            Fault::CutReplica { pod } => {
                self.isolate(REPLICA_CUT_POLICY, &targets.replica_labels, (POD_NAME_LABEL, pod))
                    .await
            }
            Fault::CutZone { zone } => {
                let in_the_zone = (targets.zone_label.as_str(), zone.as_str());
                self.isolate(REPLICA_CUT_POLICY, &targets.replica_labels, in_the_zone)
                    .await?;
                self.isolate(ETCD_CUT_POLICY, &targets.etcd_labels, in_the_zone).await
            }
        }
    }

    /// Deletes both policies. One that is not there is as good as one that was deleted.
    async fn remove_cuts(&self) -> Result<(), FaultError> {
        for policy in [REPLICA_CUT_POLICY, ETCD_CUT_POLICY] {
            if let Err(api_error) = self.api.delete_network_policy(&self.targets.namespace, policy).await {
                return Err(fault_error(&api_error));
            }
        }
        Ok(())
    }

    /// Lists the pods of the metadata store, each with the zone its label names.
    async fn etcd_members(&self) -> Result<Vec<EtcdMember>, FaultError> {
        let pods = match self
            .api
            .list_pods(&self.targets.namespace, &self.targets.etcd_labels)
            .await
        {
            Ok(pods) => pods,
            Err(api_error) => return Err(fault_error(&api_error)),
        };
        let mut members = Vec::with_capacity(pods.len());
        for pod in pods {
            members.push(EtcdMember {
                zone: pod.labels.get(&self.targets.zone_label).cloned(),
                pod: pod.name,
            });
        }
        Ok(members)
    }
}

#[cfg(test)]
#[path = "kubernetes_faults_test.rs"]
mod tests;
