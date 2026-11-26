//! Tests of the faults as data and of the injector of a run without faults.
//!
//! # What is covered
//!
//! - Every fault says its kind, whether it stays in place, and what it does in words.
//! - The injector of a run without faults refuses a fault, removes nothing and lists nothing.
//!
//! # How the fixtures work
//!
//! The faults are built by hand, with the names a cluster on Kubernetes would have.

use super::*;

/// One fault of every kind, in the order of [`FaultKind::ALL`].
fn one_fault_of_every_kind() -> [Fault; 5] {
    [
        Fault::KillReplica {
            pod: "bitacora-replica-1".to_owned(),
        },
        Fault::KillController {
            pod: "bitacora-controller-7d9".to_owned(),
        },
        Fault::KillEtcdMember {
            pod: "etcd-2".to_owned(),
        },
        Fault::CutReplica {
            pod: "bitacora-replica-0".to_owned(),
        },
        Fault::CutZone {
            zone: "zone-b".to_owned(),
        },
    ]
}

#[test]
fn every_fault_says_its_kind() {
    let mut kinds = Vec::new();
    for fault in one_fault_of_every_kind() {
        kinds.push(fault.kind());
    }

    assert_eq!(kinds, FaultKind::ALL);
}

#[test]
fn only_cuts_stay_in_place() {
    let mut faults_that_stay = Vec::new();
    for fault in one_fault_of_every_kind() {
        if fault.is_cut() {
            faults_that_stay.push(fault.kind());
        }
    }

    assert_eq!(faults_that_stay, [FaultKind::CutReplica, FaultKind::CutZone]);
}

#[test]
fn fault_is_described_with_its_target() {
    let fault = Fault::CutZone {
        zone: "zone-b".to_owned(),
    };

    assert_eq!(fault.description(), "zone zone-b is cut off the network");
}

#[test]
fn kinds_are_reported_under_names_a_script_can_use() {
    let mut names = Vec::new();
    for kind in FaultKind::ALL {
        names.push(kind.report_name());
    }

    assert_eq!(
        names,
        [
            "kill_replica",
            "kill_controller",
            "kill_etcd_member",
            "cut_replica",
            "cut_zone"
        ]
    );
}

#[tokio::test]
async fn run_without_faults_refuses_a_fault() {
    let fault = Fault::KillReplica {
        pod: "bitacora-replica-1".to_owned(),
    };

    let refused = NoFaults.inject(&fault).await;

    assert!(refused.is_err(), "{refused:?}");
}

#[tokio::test]
async fn run_without_faults_has_no_cut_to_remove() {
    let removed = NoFaults.remove_cuts().await;

    assert_eq!(removed, Ok(()));
}

#[tokio::test]
async fn run_without_faults_knows_no_member_to_hit() {
    let members = NoFaults.etcd_members().await;

    assert_eq!(members, Ok(Vec::new()));
}
