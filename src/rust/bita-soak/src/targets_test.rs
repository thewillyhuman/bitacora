//! Tests of the rules about what a fault may hit.
//!
//! # What is covered
//!
//! - The pod behind a registered address, for the addresses of the chart and of the in-process test cluster.
//! - Each kind of fault names a target the view lists, and none when the view offers nothing.
//! - A zone is cut only with replicas in three zones or more, only if every replica has a zone, and only if the
//!   metadata store keeps its majority.
//! - A member of the metadata store is killed only if there are three or more.
//! - The replicas a cut hides: the one in the pod, or all of the zone; a kill hides none.
//!
//! # How the fixtures work
//!
//! `replica_in` registers a replica the way the chart does: its pod as the first label of its address, its zone and
//! its node as failure domains. `six_replicas_in_three_zones` and `one_member_per_zone` are a cluster as
//! `development/kubernetes` installs one. Every choice is made with the same seed; the tests do not depend on which
//! target is picked, only on what it may be.

use bitacora_meta::RECORD_SCHEMA_VERSION;
use bitacora_types::{ReplicaId, Timestamp};

use super::*;

/// The seed of every choice in these tests.
const SEED: u64 = 7;

/// A replica called `pod`, registered as the chart registers one, in `zone` on a node of its own.
fn replica_in(pod: &str, zone: &str) -> ReplicaRecord {
    ReplicaRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        replica_id: ReplicaId::new(pod).expect("a valid identifier"),
        address: format!("{pod}.bitacora-replica.storage.svc.cluster.local:7000"),
        failure_domains: vec![zone.to_owned(), format!("node-of-{pod}")],
        capacity_bytes: 1_000,
        free_bytes: 900,
        is_draining: false,
        heartbeat_at: Timestamp::from_unix_millis(0),
    }
}

/// Six replicas, two in each of the zones `zone-a`, `zone-b` and `zone-c`.
fn six_replicas_in_three_zones() -> Vec<ReplicaRecord> {
    let mut replicas = Vec::new();
    for (number, zone) in ["zone-a", "zone-b", "zone-c", "zone-a", "zone-b", "zone-c"]
        .iter()
        .enumerate()
    {
        replicas.push(replica_in(&format!("bitacora-replica-{number}"), zone));
    }
    replicas
}

/// The member of the metadata store called `pod`, in `zone`.
fn member_in(pod: &str, zone: &str) -> EtcdMember {
    EtcdMember {
        pod: pod.to_owned(),
        zone: Some(zone.to_owned()),
    }
}

/// Three members of the metadata store, one in each zone.
fn one_member_per_zone() -> Vec<EtcdMember> {
    vec![
        member_in("etcd-0", "zone-a"),
        member_in("etcd-1", "zone-b"),
        member_in("etcd-2", "zone-c"),
    ]
}

/// A cluster as `development/kubernetes` installs one, with a controller leading.
fn whole_cluster() -> ClusterView {
    ClusterView {
        replicas: six_replicas_in_three_zones(),
        leading_controller: Some("bitacora-controller-5964774ccd-4krn8".to_owned()),
        etcd_members: one_member_per_zone(),
    }
}

/// The fault of `kind` chosen for `view`.
fn chosen(kind: FaultKind, view: &ClusterView) -> Option<Fault> {
    choose_fault(kind, view, &mut DeterministicRandom::from_seed(SEED))
}

/// The names of the pods of the replicas in `view`.
fn replica_pods(view: &ClusterView) -> Vec<String> {
    let mut pods = Vec::new();
    for replica in &view.replicas {
        pods.push(pod_of_address(&replica.address).to_owned());
    }
    pods
}

#[test]
fn pod_of_an_address_of_the_chart_is_its_first_label() {
    let pod = pod_of_address("bitacora-replica-2.bitacora-replica.storage.svc.cluster.local:7000");

    assert_eq!(pod, "bitacora-replica-2");
}

#[test]
fn pod_of_an_address_without_a_domain_is_what_precedes_the_port() {
    let pod = pod_of_address("replica-1:7000");

    assert_eq!(pod, "replica-1");
}

#[test]
fn pod_of_a_bare_name_is_the_name() {
    let pod = pod_of_address("replica-1");

    assert_eq!(pod, "replica-1");
}

#[test]
fn killed_replica_is_one_of_the_registered_ones() {
    let view = whole_cluster();

    let fault = chosen(FaultKind::KillReplica, &view);

    let Some(Fault::KillReplica { pod }) = fault else {
        panic!("no replica was chosen: {fault:?}");
    };
    assert!(replica_pods(&view).contains(&pod), "{pod}");
}

#[test]
fn cut_replica_is_one_of_the_registered_ones() {
    let view = whole_cluster();

    let fault = chosen(FaultKind::CutReplica, &view);

    let Some(Fault::CutReplica { pod }) = fault else {
        panic!("no replica was chosen: {fault:?}");
    };
    assert!(replica_pods(&view).contains(&pod), "{pod}");
}

#[test]
fn no_replica_is_hit_when_none_is_registered() {
    let view = ClusterView::default();

    assert_eq!(chosen(FaultKind::KillReplica, &view), None);
    assert_eq!(chosen(FaultKind::CutReplica, &view), None);
}

#[test]
fn killed_controller_is_the_one_that_leads() {
    let fault = chosen(FaultKind::KillController, &whole_cluster());

    assert_eq!(
        fault,
        Some(Fault::KillController {
            pod: "bitacora-controller-5964774ccd-4krn8".to_owned()
        })
    );
}

#[test]
fn no_controller_is_killed_while_none_leads() {
    let mut view = whole_cluster();
    view.leading_controller = None;

    assert_eq!(chosen(FaultKind::KillController, &view), None);
}

#[test]
fn killed_etcd_member_is_one_of_the_listed_ones() {
    let fault = chosen(FaultKind::KillEtcdMember, &whole_cluster());

    let Some(Fault::KillEtcdMember { pod }) = fault else {
        panic!("no member was chosen: {fault:?}");
    };
    assert!(["etcd-0", "etcd-1", "etcd-2"].contains(&pod.as_str()), "{pod}");
}

/// Two members have no majority without one of them.
#[test]
fn no_etcd_member_is_killed_when_there_are_fewer_than_three() {
    let mut view = whole_cluster();
    view.etcd_members.pop();

    assert_eq!(chosen(FaultKind::KillEtcdMember, &view), None);
}

#[test]
fn cut_zone_is_one_the_replicas_are_in() {
    let fault = chosen(FaultKind::CutZone, &whole_cluster());

    let Some(Fault::CutZone { zone }) = fault else {
        panic!("no zone was chosen: {fault:?}");
    };
    assert!(["zone-a", "zone-b", "zone-c"].contains(&zone.as_str()), "{zone}");
}

/// With two zones, a stream has two of its three copies in one of them: cutting that zone takes its write quorum.
#[test]
fn no_zone_is_cut_when_the_replicas_are_in_fewer_than_three() {
    let mut view = whole_cluster();
    view.replicas = vec![
        replica_in("bitacora-replica-0", "zone-a"),
        replica_in("bitacora-replica-1", "zone-b"),
        replica_in("bitacora-replica-2", "zone-a"),
    ];

    assert_eq!(chosen(FaultKind::CutZone, &view), None);
}

#[test]
fn no_zone_is_cut_when_a_replica_registered_only_its_node() {
    let mut view = whole_cluster();
    view.replicas[0].failure_domains = vec!["node-of-bitacora-replica-0".to_owned()];

    assert_eq!(chosen(FaultKind::CutZone, &view), None);
}

/// Two of three members in one zone: without that zone the metadata store has no majority.
#[test]
fn zone_that_holds_the_majority_of_the_metadata_store_is_never_cut() {
    let mut view = whole_cluster();
    view.etcd_members = vec![
        member_in("etcd-0", "zone-a"),
        member_in("etcd-1", "zone-a"),
        member_in("etcd-2", "zone-b"),
    ];

    assert_eq!(zones_that_may_be_cut(&view), ["zone-b", "zone-c"]);
}

#[test]
fn member_without_a_known_zone_counts_as_lost_with_every_zone() {
    let mut view = whole_cluster();
    view.etcd_members[0].zone = None;
    view.etcd_members[1].zone = None;

    assert_eq!(zones_that_may_be_cut(&view), Vec::<String>::new());
}

/// A metadata store that is not managed next to the cluster is not touched by a cut, so every zone may go.
#[test]
fn every_zone_may_be_cut_when_no_etcd_member_is_listed() {
    let mut view = whole_cluster();
    view.etcd_members = Vec::new();

    assert_eq!(zones_that_may_be_cut(&view), ["zone-a", "zone-b", "zone-c"]);
}

/// The names of the replicas that `fault` hides in `view`.
fn names_cut_by(fault: &Fault, view: &ClusterView) -> Vec<String> {
    let mut names = Vec::new();
    for replica_id in replicas_cut_by(fault, view) {
        names.push(replica_id.to_string());
    }
    names
}

#[test]
fn cut_of_a_replica_hides_the_replica_in_that_pod() {
    let cut = Fault::CutReplica {
        pod: "bitacora-replica-4".to_owned(),
    };

    let hidden = names_cut_by(&cut, &whole_cluster());

    assert_eq!(hidden, ["bitacora-replica-4"]);
}

#[test]
fn cut_of_a_zone_hides_every_replica_in_it() {
    let cut = Fault::CutZone {
        zone: "zone-b".to_owned(),
    };

    let hidden = names_cut_by(&cut, &whole_cluster());

    assert_eq!(hidden, ["bitacora-replica-1", "bitacora-replica-4"]);
}

#[test]
fn kill_hides_no_replica() {
    let kill = Fault::KillReplica {
        pod: "bitacora-replica-4".to_owned(),
    };

    let hidden = names_cut_by(&kill, &whole_cluster());

    assert_eq!(hidden, Vec::<String>::new());
}

/// A cut that names a pod nobody registered from hides nothing the tool could watch.
#[test]
fn cut_of_a_pod_without_a_replica_hides_nothing() {
    let cut = Fault::CutReplica {
        pod: "bitacora-replica-9".to_owned(),
    };

    let hidden = names_cut_by(&cut, &whole_cluster());

    assert_eq!(hidden, Vec::<String>::new());
}
