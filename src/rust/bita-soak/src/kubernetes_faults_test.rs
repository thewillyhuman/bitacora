//! Tests of the faults on Kubernetes.
//!
//! # What is covered
//!
//! - A kill deletes the pod without a grace period; a kill of a pod that is not there is an error.
//! - A cut of a replica creates one policy that selects the replica's pod and allows nothing; a cut of a zone
//!   creates one for the replicas of the zone and one for its members of the metadata store.
//! - Removing the cuts deletes both policies by name, also when neither is there; a refusal is an error.
//! - The members of the metadata store are the pods with its labels, each with the zone its label names.
//! - A cut that the API server accepts and the network ignores: the nemesis sees the replica keep registering,
//!   deletes the policies, counts nothing and gives the kind up.
//! - Labels from the command line, and what a pod's service account mount gives.
//!
//! # How the fixtures work
//!
//! `Kubernetes` is the stand-in API server of `kubernetes_stand_in.rs` with the fault interface over it, for a
//! cluster in the namespace `storage` labelled as the chart labels one. The tests run in real time, because the
//! requests are real. The last test adds the in-process cluster of `test_support.rs` as the metadata store the
//! nemesis reads, also in real time: its replicas register every two seconds, so the test takes a few.

// The tests write the files a pod would find mounted.
#![allow(clippy::disallowed_methods)]

use std::collections::BTreeSet;
use std::sync::Arc;

use bitacora_types::ReplicaId;
use tempfile::TempDir;
use tokio::sync::watch;
use tokio::time::Instant;

use super::*;
use crate::cluster::SoakedCluster;
use crate::cut_check::CutCheckSettings;
use crate::faults::FaultKind;
use crate::health::{alive_replicas, wait_until_healthy};
use crate::kubernetes_stand_in::{StandIn, TEST_AUTHORITY_PEM};
use crate::nemesis::{Nemesis, NemesisMemory, NemesisSettings};
use crate::test_support::InProcessCluster;
use crate::workload::SoakedStream;

/// The stand-in API server and the faults of a cluster behind it.
struct Kubernetes {
    /// The server.
    stand_in: StandIn,
    /// The faults under test.
    faults: KubernetesFaults,
    /// The directory of the token file, kept for as long as the faults are used.
    _directory: TempDir,
}

impl Kubernetes {
    /// Starts the server and the faults of a cluster in the namespace `storage`.
    async fn start() -> Self {
        let stand_in = StandIn::start().await;
        let directory = tempfile::tempdir().expect("a temporary directory");
        let token_file = directory.path().join("token");
        std::fs::write(&token_file, "token-1").expect("the token is written");
        let api = ApiClient::new(stand_in.settings(&token_file)).expect("settings that work");
        Self {
            stand_in,
            faults: KubernetesFaults::new(api, targets()),
            _directory: directory,
        }
    }

    /// The body of the request at `position`, as JSON.
    fn body_of_request(&self, position: usize) -> serde_json::Value {
        serde_json::from_str(&self.stand_in.seen()[position].body).expect("a body in JSON")
    }
}

/// A cluster in the namespace `storage`, labelled as the chart and `development/kubernetes/etcd.yaml` label one.
fn targets() -> KubernetesTargets {
    KubernetesTargets {
        namespace: "storage".to_owned(),
        replica_labels: BTreeMap::from([("app.kubernetes.io/component".to_owned(), "replica".to_owned())]),
        etcd_labels: BTreeMap::from([("app.kubernetes.io/name".to_owned(), "etcd".to_owned())]),
        zone_label: "topology.kubernetes.io/zone".to_owned(),
    }
}

/// The path of the network policies of the namespace.
const POLICIES: &str = "/apis/networking.k8s.io/v1/namespaces/storage/networkpolicies";

#[tokio::test]
async fn kill_of_a_replica_deletes_its_pod_without_a_grace_period() {
    let kubernetes = Kubernetes::start().await;
    let kill = Fault::KillReplica {
        pod: "bitacora-replica-1".to_owned(),
    };

    let injected = kubernetes.faults.inject(&kill).await;

    assert_eq!(injected, Ok(()));
    let seen = kubernetes.stand_in.seen();
    assert_eq!(
        seen[0].line(),
        "DELETE /api/v1/namespaces/storage/pods/bitacora-replica-1"
    );
    assert_eq!(seen[0].query, "gracePeriodSeconds=0");
}

#[tokio::test]
async fn kill_of_a_controller_and_of_an_etcd_member_delete_their_pods() {
    let kubernetes = Kubernetes::start().await;
    let kill_controller = Fault::KillController {
        pod: "bitacora-controller-5964774ccd-4krn8".to_owned(),
    };
    let kill_member = Fault::KillEtcdMember {
        pod: "etcd-2".to_owned(),
    };

    kubernetes
        .faults
        .inject(&kill_controller)
        .await
        .expect("the pod is deleted");
    kubernetes
        .faults
        .inject(&kill_member)
        .await
        .expect("the pod is deleted");

    assert_eq!(
        kubernetes.stand_in.seen_lines(),
        [
            "DELETE /api/v1/namespaces/storage/pods/bitacora-controller-5964774ccd-4krn8",
            "DELETE /api/v1/namespaces/storage/pods/etcd-2",
        ]
    );
}

/// A fault that did not happen must not be reported as one that did.
#[tokio::test]
async fn kill_of_a_pod_that_is_not_there_is_an_error() {
    let kubernetes = Kubernetes::start().await;
    kubernetes.stand_in.answer_next_with(404, r#"{"reason":"NotFound"}"#);
    let kill = Fault::KillReplica {
        pod: "bitacora-replica-9".to_owned(),
    };

    let injected = kubernetes.faults.inject(&kill).await;

    assert_eq!(
        injected,
        Err(FaultError {
            reason: "there is no pod called bitacora-replica-9 in namespace storage".to_owned(),
        })
    );
}

#[tokio::test]
async fn cut_of_a_replica_creates_one_policy_that_selects_its_pod_and_allows_nothing() {
    let kubernetes = Kubernetes::start().await;
    let cut = Fault::CutReplica {
        pod: "bitacora-replica-1".to_owned(),
    };

    let injected = kubernetes.faults.inject(&cut).await;

    assert_eq!(injected, Ok(()));
    assert_eq!(kubernetes.stand_in.seen_lines(), [format!("POST {POLICIES}")]);
    assert_eq!(
        kubernetes.body_of_request(0),
        json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {
                "name": "bita-soak-cut-replica",
                "labels": {"app.kubernetes.io/managed-by": "bita-soak"},
            },
            "spec": {
                "podSelector": {"matchLabels": {
                    "app.kubernetes.io/component": "replica",
                    "statefulset.kubernetes.io/pod-name": "bitacora-replica-1",
                }},
                "policyTypes": ["Ingress", "Egress"],
            },
        })
    );
}

#[tokio::test]
async fn cut_of_a_zone_creates_a_policy_for_its_replicas_and_one_for_its_etcd_members() {
    let kubernetes = Kubernetes::start().await;
    let cut = Fault::CutZone {
        zone: "zone-b".to_owned(),
    };

    let injected = kubernetes.faults.inject(&cut).await;

    assert_eq!(injected, Ok(()));
    assert_eq!(
        kubernetes.body_of_request(0)["spec"]["podSelector"]["matchLabels"],
        json!({"app.kubernetes.io/component": "replica", "topology.kubernetes.io/zone": "zone-b"})
    );
    assert_eq!(
        kubernetes.body_of_request(1)["spec"]["podSelector"]["matchLabels"],
        json!({"app.kubernetes.io/name": "etcd", "topology.kubernetes.io/zone": "zone-b"})
    );
}

#[tokio::test]
async fn policies_of_a_zone_cut_have_the_two_fixed_names() {
    let kubernetes = Kubernetes::start().await;
    let cut = Fault::CutZone {
        zone: "zone-b".to_owned(),
    };

    kubernetes.faults.inject(&cut).await.expect("the policies are created");

    assert_eq!(
        kubernetes.body_of_request(0)["metadata"]["name"],
        "bita-soak-cut-replica"
    );
    assert_eq!(kubernetes.body_of_request(1)["metadata"]["name"], "bita-soak-cut-etcd");
}

#[tokio::test]
async fn cut_that_the_api_server_refuses_is_an_error() {
    let kubernetes = Kubernetes::start().await;
    kubernetes
        .stand_in
        .answer_next_with(403, r#"{"message":"networkpolicies is forbidden"}"#);
    let cut = Fault::CutReplica {
        pod: "bitacora-replica-1".to_owned(),
    };

    let injected = kubernetes.faults.inject(&cut).await;

    let fault_error = injected.expect_err("the service account may not create policies");
    assert!(
        fault_error.reason.starts_with("the Kubernetes API answered 403"),
        "{fault_error}"
    );
}

#[tokio::test]
async fn removing_the_cuts_deletes_both_policies_by_name() {
    let kubernetes = Kubernetes::start().await;

    let removed = kubernetes.faults.remove_cuts().await;

    assert_eq!(removed, Ok(()));
    assert_eq!(
        kubernetes.stand_in.seen_lines(),
        [
            format!("DELETE {POLICIES}/bita-soak-cut-replica"),
            format!("DELETE {POLICIES}/bita-soak-cut-etcd"),
        ]
    );
}

/// What a run does when it starts, and what every round does when it heals: mostly there is nothing to remove.
#[tokio::test]
async fn removing_the_cuts_succeeds_when_there_are_none() {
    let kubernetes = Kubernetes::start().await;
    kubernetes.stand_in.answer_next_with(404, r#"{"reason":"NotFound"}"#);
    kubernetes.stand_in.answer_next_with(404, r#"{"reason":"NotFound"}"#);

    let removed = kubernetes.faults.remove_cuts().await;

    assert_eq!(removed, Ok(()));
}

#[tokio::test]
async fn removal_that_the_api_server_refuses_is_an_error() {
    let kubernetes = Kubernetes::start().await;
    kubernetes
        .stand_in
        .answer_next_with(403, r#"{"message":"networkpolicies is forbidden"}"#);

    let removed = kubernetes.faults.remove_cuts().await;

    let fault_error = removed.expect_err("the service account may not delete policies");
    assert!(
        fault_error.reason.starts_with("the Kubernetes API answered 403"),
        "{fault_error}"
    );
}

#[tokio::test]
async fn etcd_members_are_the_pods_with_the_labels_of_the_metadata_store() {
    let kubernetes = Kubernetes::start().await;
    kubernetes.stand_in.answer_next_with(
        200,
        r#"{"items":[
            {"metadata":{"name":"etcd-0","labels":{"topology.kubernetes.io/zone":"zone-a"}}},
            {"metadata":{"name":"etcd-1","labels":{"app.kubernetes.io/name":"etcd"}}}]}"#,
    );

    let members = kubernetes.faults.etcd_members().await;

    assert_eq!(
        members,
        Ok(vec![
            EtcdMember {
                pod: "etcd-0".to_owned(),
                zone: Some("zone-a".to_owned()),
            },
            EtcdMember {
                pod: "etcd-1".to_owned(),
                zone: None,
            },
        ])
    );
    assert_eq!(
        kubernetes.stand_in.seen()[0].query,
        "labelSelector=app.kubernetes.io%2Fname%3Detcd"
    );
}

/// The finding that made the check necessary: a cluster whose network plugin does not enforce policies accepts
/// the policy, and the replica behind it goes on as before. The nemesis must see that, take the policy away,
/// count nothing and stop asking.
#[tokio::test]
async fn cut_that_the_api_server_accepts_and_the_network_ignores_is_removed_and_given_up() {
    let kubernetes = Kubernetes::start().await;
    let in_process = InProcessCluster::start().await;
    let cluster = SoakedCluster {
        faults: Arc::new(kubernetes.faults.clone()),
        ..in_process.soaked.clone()
    };
    let expected_replicas: BTreeSet<ReplicaId> = alive_replicas(&cluster).await.expect("the registry answers");
    wait_until_healthy(&cluster, &expected_replicas, Duration::from_secs(20))
        .await
        .expect("a controller leads");
    let settings = NemesisSettings {
        kinds: vec![FaultKind::CutReplica],
        fault_interval: Duration::from_millis(50),
        cut_duration: Duration::from_millis(50),
        cut_check: CutCheckSettings {
            quiet_period: Duration::from_secs(3),
            time_limit: Duration::from_secs(5),
        },
        retries_cuts_not_in_effect: false,
    };
    let streams = [SoakedStream::new("soak-test-1-0".to_owned())];
    let mut memory = NemesisMemory::from_seed(7);
    let (_told_to_stop, mut stop) = watch::channel(false);

    let nemesis = Nemesis::new(&cluster, &settings, &expected_replicas, &streams);
    let tally = nemesis
        .run(&mut memory, Instant::now() + Duration::from_millis(200), &mut stop)
        .await;

    assert_eq!(
        kubernetes.stand_in.seen_lines(),
        [
            "GET /api/v1/namespaces/storage/pods".to_owned(),
            format!("POST {POLICIES}"),
            format!("DELETE {POLICIES}/bita-soak-cut-replica"),
            format!("DELETE {POLICIES}/bita-soak-cut-etcd"),
        ]
    );
    assert_eq!(tally.injected, BTreeMap::new());
    assert!(memory.disabled.contains_key(&FaultKind::CutReplica), "{memory:?}");
    assert_eq!(streams[0].history.events().len(), 0);
}

#[test]
fn labels_are_read_as_pairs_separated_by_commas() {
    let labels = parse_labels("app.kubernetes.io/name=bitacora, app.kubernetes.io/component=replica");

    assert_eq!(
        labels,
        Ok(BTreeMap::from([
            ("app.kubernetes.io/component".to_owned(), "replica".to_owned()),
            ("app.kubernetes.io/name".to_owned(), "bitacora".to_owned()),
        ]))
    );
}

/// A selector that selects every pod of the namespace must not be the result of a typing mistake.
#[test]
fn labels_without_a_name_or_a_value_are_refused() {
    assert!(parse_labels("").is_err());
    assert!(parse_labels("replica").is_err());
    assert!(parse_labels("=replica").is_err());
    assert!(parse_labels("component=").is_err());
    assert!(parse_labels("component=replica,").is_err());
}

#[test]
fn service_account_is_read_from_its_mount() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    std::fs::write(directory.path().join("ca.crt"), TEST_AUTHORITY_PEM).expect("the authority is written");
    std::fs::write(directory.path().join("namespace"), "storage\n").expect("the namespace is written");

    let service_account = ServiceAccount::read_from(directory.path());

    assert_eq!(
        service_account,
        Ok(ServiceAccount {
            token_file: directory.path().join("token"),
            certificate_authority_pem: TEST_AUTHORITY_PEM.to_owned(),
            namespace: "storage".to_owned(),
        })
    );
}

#[test]
fn directory_that_is_not_a_service_account_mount_is_an_error_that_names_the_file() {
    let directory = tempfile::tempdir().expect("a temporary directory");

    let service_account = ServiceAccount::read_from(directory.path());

    let reason = service_account.expect_err("nothing is mounted there");
    assert!(reason.contains("ca.crt cannot be read"), "{reason}");
}

/// A service account with nothing in it but what the tests of the address need.
fn some_service_account() -> ServiceAccount {
    ServiceAccount {
        token_file: PathBuf::from("/var/run/secrets/kubernetes.io/serviceaccount/token"),
        certificate_authority_pem: TEST_AUTHORITY_PEM.to_owned(),
        namespace: "storage".to_owned(),
    }
}

#[test]
fn api_server_is_reached_over_https_at_the_host_and_port_of_the_environment() {
    let settings = some_service_account().api_settings("10.96.0.1", "443");

    assert_eq!(settings.base_url, "https://10.96.0.1:443");
    assert_eq!(
        settings.token_file,
        PathBuf::from("/var/run/secrets/kubernetes.io/serviceaccount/token")
    );
}

#[test]
fn ipv6_address_of_the_api_server_is_put_in_brackets() {
    let settings = some_service_account().api_settings("fd00:10:96::1", "443");

    assert_eq!(settings.base_url, "https://[fd00:10:96::1]:443");
}

#[test]
fn every_call_is_bounded_in_time_and_in_attempts() {
    let settings = some_service_account().api_settings("10.96.0.1", "443");

    assert_eq!(
        (settings.attempt_time_limit, settings.attempts, settings.retry_pause),
        (Duration::from_secs(10), 3, Duration::from_secs(1))
    );
}
