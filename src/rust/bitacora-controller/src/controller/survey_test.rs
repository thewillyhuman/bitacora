//! Tests of the survey.
//!
//! # What is covered
//!
//! - Who is in a survey: every registered replica, with an answer from each one that is alive and reachable, and no
//!   answer from one that is cut off or whose registration lapsed.
//! - What a survey says a replica holds, and how "holds nothing" differs from "did not answer".
//! - Which replica is picked to read a sealed segment from.
//!
//! # How the fixtures work
//!
//! A controller with the default settings surveys the testkit cluster. `cluster_with_a_sealed_segment` gives the
//! replicas one segment to report.

use bitacora_testkit::Cluster;

use super::super::ControllerSettings;
use super::super::test_support::{
    CONTROLLER_IDENTITY, Writes, cluster_with_a_sealed_segment, controller_named, first_segment_address, run_pass,
    stream,
};
use super::*;

/// Takes a survey of `cluster` with a controller that has the default settings.
async fn survey_of(cluster: &Cluster) -> Survey {
    controller_named(cluster, CONTROLLER_IDENTITY)
        .survey()
        .await
        .expect("the metadata store of the test cluster is available")
}

#[tokio::test(start_paused = true)]
async fn every_registered_replica_is_in_the_survey() {
    let cluster = Cluster::start(3).await;

    let survey = survey_of(&cluster).await;

    assert_eq!(survey.registered_replicas.len(), 3);
}

#[tokio::test(start_paused = true)]
async fn reachable_replica_answers_the_survey() {
    let cluster = Cluster::start(3).await;

    let survey = survey_of(&cluster).await;

    assert!(survey.has_answered(&cluster.nodes()[0].replica_id()));
}

#[tokio::test(start_paused = true)]
async fn replica_that_is_cut_off_is_registered_but_has_not_answered() {
    let cluster = Cluster::start(3).await;
    cluster.nodes()[1].set_reachable(false);

    let survey = survey_of(&cluster).await;

    let cut_off_replica = cluster.nodes()[1].replica_id();
    assert!(survey.registration(&cut_off_replica).is_some());
    assert!(!survey.has_answered(&cut_off_replica));
}

#[tokio::test(start_paused = true)]
async fn replica_whose_registration_lapsed_is_not_asked() {
    let cluster = Cluster::start(3).await;
    let liveness_window = ControllerSettings::default().placement.liveness_window;
    tokio::time::advance(liveness_window * 2).await;

    let survey = survey_of(&cluster).await;

    assert!(!survey.has_answered(&cluster.nodes()[0].replica_id()));
}

#[tokio::test(start_paused = true)]
async fn replica_that_is_not_registered_has_no_registration() {
    let cluster = Cluster::start(3).await;

    let survey = survey_of(&cluster).await;

    let stranger = ReplicaId::new("replica-unknown").expect("a valid identifier");
    assert!(survey.registration(&stranger).is_none());
}

#[tokio::test(start_paused = true)]
async fn replica_without_copies_answers_that_it_holds_nothing() {
    let cluster = Cluster::start(3).await;

    let survey = survey_of(&cluster).await;

    let held_copies = survey
        .copies_held_by(&cluster.nodes()[0].replica_id())
        .expect("the replica answered");
    assert!(held_copies.is_empty());
}

#[tokio::test(start_paused = true)]
async fn survey_lists_the_copies_a_replica_holds() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::ThreeRecords).await;
    let segment = first_segment_address(&cluster).await;

    let survey = survey_of(&cluster).await;

    let held_copies = survey
        .copies_held_by(&cluster.nodes()[0].replica_id())
        .expect("the replica answered");
    assert!(held_copies.contains_key(&segment));
}

#[tokio::test(start_paused = true)]
async fn replica_that_did_not_answer_has_no_list_of_copies() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::ThreeRecords).await;
    cluster.nodes()[0].set_reachable(false);

    let survey = survey_of(&cluster).await;

    assert!(survey.copies_held_by(&cluster.nodes()[0].replica_id()).is_none());
}

#[tokio::test(start_paused = true)]
async fn segment_is_read_from_the_first_replica_with_a_good_copy() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::ThreeRecords).await;
    run_pass(&cluster).await;
    let sealed_segment = stream(&cluster).await.segments[0].clone();

    let survey = survey_of(&cluster).await;

    let holder = survey
        .holder_of_a_good_copy(&sealed_segment)
        .expect("every copy is good and every replica answers");
    assert_eq!(holder.replica_id, sealed_segment.copies[0].replica_id);
}

#[tokio::test(start_paused = true)]
async fn replica_that_did_not_answer_is_passed_over_as_a_source() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::ThreeRecords).await;
    run_pass(&cluster).await;
    let sealed_segment = stream(&cluster).await.segments[0].clone();
    cluster.nodes()[0].set_reachable(false);

    let survey = survey_of(&cluster).await;

    let holder = survey
        .holder_of_a_good_copy(&sealed_segment)
        .expect("two good copies are on replicas that answer");
    assert_eq!(holder.replica_id, sealed_segment.copies[1].replica_id);
}

#[tokio::test(start_paused = true)]
async fn segment_whose_copies_nobody_verified_has_no_source() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::ThreeRecords).await;
    let unverified_segment = stream(&cluster).await.segments[0].clone();

    let survey = survey_of(&cluster).await;

    assert!(survey.holder_of_a_good_copy(&unverified_segment).is_none());
}
