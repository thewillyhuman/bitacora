//! Tests of what a pass reports.
//!
//! # What is covered
//!
//! - Which segments count as missing copies, as a pure function: a segment with a bad copy, and the three kinds that
//!   never count (open, left to the object store, below the low-water mark).
//! - The count against a cluster: nothing is missing when every copy is good, a lost copy that cannot be rebuilt is
//!   counted, and a lost copy that was rebuilt in the same pass is not.
//!
//! # How the fixtures work
//!
//! The pure tests take `stream_record` from the shared test support and give its segments three copies, since its
//! replication is three. The cluster tests use `cluster_with_a_sealed_segment`.

use bitacora_meta::{CopyRecord, TieredRecord};
use bitacora_types::{SequenceNumber, Timestamp};

use super::super::test_support::{
    Writes, cluster_with_a_sealed_segment, lose_node, replica_id, run_pass, stream_record,
};
use super::*;

/// The stream of [`stream_record`] with three copies of every segment, all in the state `state`.
fn stream_with_copies_in(state: CopyState) -> StreamRecord {
    let mut stream = stream_record();
    for segment in &mut stream.segments {
        segment.copies = Vec::new();
        for name in ["replica-a", "replica-b", "replica-c"] {
            segment.copies.push(CopyRecord {
                replica_id: replica_id(name),
                state,
            });
        }
    }
    stream
}

#[test]
fn stream_whose_copies_are_all_good_misses_none() {
    let stream = stream_with_copies_in(CopyState::Good);

    assert_eq!(segments_missing_copies(&stream), 0);
}

#[test]
fn sealed_segment_with_a_bad_copy_is_missing_copies() {
    let mut stream = stream_with_copies_in(CopyState::Good);
    stream.segments[0].copies[1].state = CopyState::Bad;

    assert_eq!(segments_missing_copies(&stream), 1);
}

#[test]
fn sealed_segment_with_an_unverified_copy_is_missing_copies() {
    let mut stream = stream_with_copies_in(CopyState::Good);
    stream.segments[1].copies[2].state = CopyState::Unverified;

    assert_eq!(segments_missing_copies(&stream), 1);
}

#[test]
fn open_segment_is_never_counted() {
    let mut stream = stream_with_copies_in(CopyState::Good);
    stream.segments[2].copies[0].state = CopyState::Bad;

    assert_eq!(segments_missing_copies(&stream), 0);
}

#[test]
fn segment_left_to_the_object_store_is_not_counted() {
    let mut stream = stream_with_copies_in(CopyState::Good);
    stream.segments[0].copies.clear();
    stream.segments[0].tiered = Some(TieredRecord {
        object_key: "streams/7/segments/00000000000000000001".to_owned(),
        size_bytes: 100,
        uploaded_at: Timestamp::from_unix_millis(5_000),
        are_local_copies_dropped: true,
    });

    assert_eq!(segments_missing_copies(&stream), 0);
}

#[test]
fn segment_below_the_low_water_mark_is_not_counted() {
    let mut stream = stream_with_copies_in(CopyState::Good);
    stream.segments[0].copies[0].state = CopyState::Bad;
    stream.low_water_mark = SequenceNumber::new(10);

    assert_eq!(segments_missing_copies(&stream), 0);
}

#[test]
fn good_copies_are_counted_one_by_one() {
    let mut stream = stream_with_copies_in(CopyState::Good);
    stream.segments[0].copies[1].state = CopyState::Bad;

    assert_eq!(good_copy_count(&stream.segments[0]), 2);
}

#[tokio::test(start_paused = true)]
async fn pass_counts_no_segment_as_missing_copies_when_all_are_good() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::ThreeRecords).await;

    let report = run_pass(&cluster).await;

    assert_eq!(report.segments_missing_copies, 0);
}

#[tokio::test(start_paused = true)]
async fn pass_counts_a_segment_whose_copy_is_lost_and_cannot_be_rebuilt() {
    let cluster = cluster_with_a_sealed_segment(3, Writes::ThreeRecords).await;
    run_pass(&cluster).await;
    lose_node(&cluster, 2).await;

    let report = run_pass(&cluster).await;

    assert_eq!(report.segments_missing_copies, 1);
}

#[tokio::test(start_paused = true)]
async fn pass_does_not_count_a_segment_whose_copy_was_rebuilt() {
    let cluster = cluster_with_a_sealed_segment(4, Writes::ThreeRecords).await;
    run_pass(&cluster).await;
    lose_node(&cluster, 2).await;

    let report = run_pass(&cluster).await;

    assert_eq!(report.copies_repaired, 1);
    assert_eq!(report.segments_missing_copies, 0);
}
