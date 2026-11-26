//! Tests of reconciliation.
//!
//! # What is covered
//!
//! - The rule for a copy whose replica knows the seal, as a pure function: complete, short, damaged, and the copy of
//!   a segment that was sealed empty.
//! - The step against a cluster: complete copies become good and are not confirmed twice; a replica that missed the
//!   seal is told it; a short copy, a damaged copy, and a copy on a removed replica become bad; a replica that does
//!   not answer keeps its copies until it has been silent for long; an open segment is not judged.
//!
//! # How the fixtures work
//!
//! `cluster_with_a_sealed_segment(3, writes)` from the shared test support gives one sealed segment whose copies
//! nobody has judged yet; `writes` names what the third node missed. The pure tests use `second_sealed_segment`, a
//! hand-built segment that holds records 10 to 19.

use bitacora_testkit::Cluster;
use bitacora_types::replica_api::ReplicaApi;

use super::super::test_support::{
    Writes, append, cluster_with_a_sealed_segment, copy_states, create_stream, damage_first_segment_on,
    first_segment_address, lose_node, open_writer, renew_registrations, run_pass, second_sealed_segment,
};
use super::*;

/// The last record of [`second_sealed_segment`], the segment the pure tests judge copies of.
const SEALED_END: SequenceNumber = SequenceNumber::new(19);

/// The sealed ends that the third node of `cluster` knows, one per segment it holds something of.
async fn seals_known_by_the_third_node(cluster: &Cluster) -> Vec<Option<SequenceNumber>> {
    let statuses = cluster.nodes()[2]
        .replica()
        .expect("the node is running")
        .list_segments()
        .await
        .expect("the replica is running");
    let mut sealed_ends = Vec::new();
    for status in &statuses {
        sealed_ends.push(status.sealed_end);
    }
    sealed_ends
}

#[test]
fn copy_that_ends_at_the_sealed_end_is_good() {
    let state = state_of_a_copy(&second_sealed_segment(), SEALED_END, Some(SEALED_END), false);

    assert_eq!(state, CopyState::Good);
}

#[test]
fn copy_that_ends_before_the_sealed_end_is_bad() {
    let state = state_of_a_copy(
        &second_sealed_segment(),
        SEALED_END,
        Some(SequenceNumber::new(15)),
        false,
    );

    assert_eq!(state, CopyState::Bad);
}

#[test]
fn complete_copy_that_is_damaged_is_bad() {
    let state = state_of_a_copy(&second_sealed_segment(), SEALED_END, Some(SEALED_END), true);

    assert_eq!(state, CopyState::Bad);
}

#[test]
fn replica_without_records_is_bad_for_a_segment_that_holds_some() {
    let state = state_of_a_copy(&second_sealed_segment(), SEALED_END, None, false);

    assert_eq!(state, CopyState::Bad);
}

#[test]
fn replica_without_records_is_good_for_a_segment_sealed_empty() {
    let end_of_an_empty_segment = SequenceNumber::new(9);

    let state = state_of_a_copy(&second_sealed_segment(), end_of_an_empty_segment, None, false);

    assert_eq!(state, CopyState::Good);
}

#[tokio::test(start_paused = true)]
async fn complete_copies_of_a_sealed_segment_become_good() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::ThreeRecords).await;

    let report = run_pass(&cluster).await;

    assert_eq!(report.copies_confirmed, 3);
    assert_eq!(copy_states(&cluster).await, [CopyState::Good; 3]);
}

#[tokio::test(start_paused = true)]
async fn confirmed_copies_are_not_confirmed_again() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::ThreeRecords).await;
    run_pass(&cluster).await;

    let second_pass = run_pass(&cluster).await;

    assert_eq!(second_pass.copies_confirmed, 0);
}

#[tokio::test(start_paused = true)]
async fn replica_that_missed_the_seal_serves_what_it_holds_before_the_pass() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::ThreeRecordsThenTheThirdNodeMissesTheSeal).await;
    let segment = first_segment_address(&cluster).await;

    let readable_before_the_pass = cluster.nodes()[2].readable_sequences(segment).await;

    assert_eq!(readable_before_the_pass, [1, 2, 3]);
}

#[tokio::test(start_paused = true)]
async fn replica_that_missed_the_seal_learns_it_from_the_controller() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::ThreeRecordsThenTheThirdNodeMissesTheSeal).await;

    run_pass(&cluster).await;

    assert_eq!(
        seals_known_by_the_third_node(&cluster).await,
        [Some(SequenceNumber::new(3))]
    );
    assert_eq!(copy_states(&cluster).await[2], CopyState::Good);
}

#[tokio::test(start_paused = true)]
async fn short_copy_of_a_sealed_segment_is_found_bad() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::OneRecordThenTwoThatTheThirdNodeMisses).await;

    let report = run_pass(&cluster).await;

    assert_eq!(report.copies_confirmed, 2);
    assert_eq!(report.copies_marked_bad, 1);
}

#[tokio::test(start_paused = true)]
async fn copy_on_a_replica_that_does_not_answer_stays_unverified_at_first() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::ThreeRecords).await;
    cluster.nodes()[2].set_reachable(false);

    run_pass(&cluster).await;

    assert_eq!(copy_states(&cluster).await[2], CopyState::Unverified);
}

#[tokio::test(start_paused = true)]
async fn copy_on_a_replica_silent_for_long_becomes_bad() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::ThreeRecords).await;
    run_pass(&cluster).await;
    lose_node(&cluster, 2).await;

    run_pass(&cluster).await;

    assert_eq!(copy_states(&cluster).await[2], CopyState::Bad);
}

#[tokio::test(start_paused = true)]
async fn copy_given_up_for_lost_is_good_again_when_its_replica_returns() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::ThreeRecords).await;
    lose_node(&cluster, 2).await;
    run_pass(&cluster).await;
    cluster.nodes()[2].restart().await;
    renew_registrations(&cluster).await;

    run_pass(&cluster).await;

    assert_eq!(copy_states(&cluster).await[2], CopyState::Good);
}

#[tokio::test(start_paused = true)]
async fn copy_on_a_replica_removed_from_the_registry_becomes_bad() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::ThreeRecords).await;
    cluster
        .metadata()
        .remove_replica(&cluster.nodes()[2].replica_id())
        .await
        .expect("the metadata store is available");

    run_pass(&cluster).await;

    assert_eq!(copy_states(&cluster).await[2], CopyState::Bad);
}

#[tokio::test(start_paused = true)]
async fn copy_found_damaged_by_the_scrubber_is_found_bad() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::ThreeRecords).await;
    run_pass(&cluster).await;
    damage_first_segment_on(&cluster, 0).await;

    let report = run_pass(&cluster).await;

    assert_eq!(report.copies_marked_bad, 1);
}

#[tokio::test(start_paused = true)]
async fn segment_sealed_empty_is_complete_on_every_replica() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::Nothing).await;

    run_pass(&cluster).await;

    assert_eq!(copy_states(&cluster).await, [CopyState::Good; 3]);
}

#[tokio::test(start_paused = true)]
async fn open_segment_is_not_judged() {
    let cluster = Cluster::start(3).await;
    create_stream(&cluster).await;
    run_pass(&cluster).await;
    let writer = open_writer(&cluster).await;
    append(&writer, 3).await;

    run_pass(&cluster).await;

    assert_eq!(copy_states(&cluster).await, [CopyState::Unverified; 3]);
}
