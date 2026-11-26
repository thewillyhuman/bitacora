//! Tests of repair.
//!
//! # What is covered
//!
//! - A short copy and a damaged copy are rebuilt where they are.
//! - A copy on a lost replica is rebuilt on another one, which keeps the segment's spread across failure domains.
//! - Nothing is repaired without a good copy to repair from, or without a replica to repair onto.
//! - The two small selections repair makes from a segment's record, as pure functions.
//!
//! # How the fixtures work
//!
//! `cluster_with_a_sealed_segment` from the shared test support gives one sealed segment; a first pass judges its
//! copies, and a test then destroys one with `lose_node` or `damage_first_segment_on`. One pass both marks a copy
//! bad and repairs it, because repair runs after reconciliation.

use bitacora_meta::CopyRecord;
use bitacora_replica::ReplicaSettings;
use bitacora_testkit::{Cluster, FailureDomainLayout};
use bitacora_types::SequenceNumber;

use super::super::test_support::{
    Writes, append, cluster_with_a_sealed_segment, copy_states, create_stream, damage_first_segment_on,
    first_sealed_segment, first_segment_address, lose_node, open_writer, renew_registrations, replica_id, run_pass,
    stream,
};
use super::*;

/// The segment file size the testkit uses for its small clusters.
const SMALL_SEGMENT_FILE_BYTES: u64 = 4 * 1024;

/// A sealed segment with copies on `replica-a`, `replica-b` and `replica-c` in the given states.
fn segment_with_copies(states: [CopyState; 3]) -> SegmentRecord {
    let mut segment = first_sealed_segment();
    segment.copies = vec![
        CopyRecord {
            replica_id: replica_id("replica-a"),
            state: states[0],
        },
        CopyRecord {
            replica_id: replica_id("replica-b"),
            state: states[1],
        },
        CopyRecord {
            replica_id: replica_id("replica-c"),
            state: states[2],
        },
    ];
    segment
}

/// The replicas that hold the copies of the first segment of the test stream, in replica-set order.
async fn holders_of_the_first_segment(cluster: &Cluster) -> Vec<ReplicaId> {
    let stream = stream(cluster).await;
    let mut holders = Vec::new();
    for copy in &stream.segments[0].copies {
        holders.push(copy.replica_id.clone());
    }
    holders
}

#[test]
fn bad_copies_are_found_by_their_position() {
    let segment = segment_with_copies([CopyState::Bad, CopyState::Good, CopyState::Bad]);

    assert_eq!(indexes_of_bad_copies(&segment), [0, 2]);
}

#[test]
fn unverified_copies_are_not_repaired() {
    let segment = segment_with_copies([CopyState::Unverified, CopyState::Good, CopyState::Unverified]);

    assert_eq!(indexes_of_bad_copies(&segment), Vec::<usize>::new());
}

#[test]
fn new_replica_is_spread_away_from_the_copies_that_stay() {
    let segment = segment_with_copies([CopyState::Good, CopyState::Bad, CopyState::Good]);

    let kept_replicas = replicas_of_the_other_copies(&segment, 1);

    assert_eq!(kept_replicas, [replica_id("replica-a"), replica_id("replica-c")]);
}

#[tokio::test(start_paused = true)]
async fn short_copy_is_completed_in_place() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::OneRecordThenTwoThatTheThirdNodeMisses).await;

    let report = run_pass(&cluster).await;

    let segment = first_segment_address(&cluster).await;
    assert_eq!(report.copies_repaired, 1);
    assert_eq!(copy_states(&cluster).await, [CopyState::Good; 3]);
    assert_eq!(
        cluster.nodes()[2].local_end(segment).await,
        Some(SequenceNumber::new(3))
    );
}

#[tokio::test(start_paused = true)]
async fn damaged_copy_is_rebuilt_in_place() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::ThreeRecords).await;
    run_pass(&cluster).await;
    damage_first_segment_on(&cluster, 0).await;

    run_pass(&cluster).await;

    let replica = cluster.nodes()[0].replica().expect("the node is running");
    assert_eq!(copy_states(&cluster).await, [CopyState::Good; 3]);
    assert_eq!(replica.scrub_sealed_segments().await, Ok(0));
}

#[tokio::test(start_paused = true)]
async fn copy_on_a_lost_replica_is_rebuilt_on_another_one() {
    let cluster = cluster_with_a_sealed_segment(4, Writes::ThreeRecords).await;
    run_pass(&cluster).await;
    lose_node(&cluster, 2).await;

    let report = run_pass(&cluster).await;

    let segment = first_segment_address(&cluster).await;
    assert_eq!(report.copies_repaired, 1);
    assert_eq!(
        holders_of_the_first_segment(&cluster).await,
        cluster.replica_ids(&[0, 1, 3])
    );
    assert_eq!(copy_states(&cluster).await, [CopyState::Good; 3]);
    assert_eq!(
        cluster.nodes()[3].local_end(segment).await,
        Some(SequenceNumber::new(3))
    );
}

#[tokio::test(start_paused = true)]
async fn segment_without_a_good_copy_is_left_alone() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::ThreeRecords).await;
    run_pass(&cluster).await;
    for node_number in 0..3 {
        damage_first_segment_on(&cluster, node_number).await;
    }

    let report = run_pass(&cluster).await;

    assert_eq!(report.copies_repaired, 0);
    assert_eq!(copy_states(&cluster).await, [CopyState::Bad; 3]);
}

#[tokio::test(start_paused = true)]
async fn lost_copy_without_a_spare_replica_is_not_repaired() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::ThreeRecords).await;
    run_pass(&cluster).await;
    lose_node(&cluster, 2).await;

    let while_lost = run_pass(&cluster).await;

    assert_eq!(while_lost.copies_repaired, 0);
    assert_eq!(copy_states(&cluster).await[2], CopyState::Bad);
}

#[tokio::test(start_paused = true)]
async fn lost_copy_without_a_spare_replica_is_good_again_when_its_replica_returns() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::ThreeRecords).await;
    run_pass(&cluster).await;
    lose_node(&cluster, 2).await;
    run_pass(&cluster).await;
    cluster.nodes()[2].restart().await;
    renew_registrations(&cluster).await;

    run_pass(&cluster).await;

    assert_eq!(copy_states(&cluster).await, [CopyState::Good; 3]);
}

/// The layout of the six nodes of the zoned cluster: two nodes in each of three zones, each node its own host.
/// Nodes 1 and 4 share zone 1.
const THREE_ZONES: FailureDomainLayout = FailureDomainLayout::HostsInZones { zone_count: 3 };

#[tokio::test(start_paused = true)]
async fn replacement_copy_stays_in_the_failure_domain_that_lost_one() {
    let small_segment_files = ReplicaSettings {
        preallocated_segment_bytes: SMALL_SEGMENT_FILE_BYTES,
        ..ReplicaSettings::default()
    };
    let cluster = Cluster::start_with(6, small_segment_files, THREE_ZONES).await;
    create_stream(&cluster).await;
    run_pass(&cluster).await;
    let writer = open_writer(&cluster).await;
    append(&writer, 3).await;
    writer.close().await.expect("closing seals the segment");
    run_pass(&cluster).await;
    let node_in_zone_1 = cluster.replica_ids(&[1])[0].clone();
    let lost_copy_index = holders_of_the_first_segment(&cluster)
        .await
        .iter()
        .position(|holder| *holder == node_in_zone_1)
        .expect("the first replica set holds one copy per zone");
    lose_node(&cluster, 1).await;

    run_pass(&cluster).await;

    let other_node_in_zone_1 = cluster.replica_ids(&[4])[0].clone();
    assert_eq!(
        holders_of_the_first_segment(&cluster).await[lost_copy_index],
        other_node_in_zone_1
    );
}
