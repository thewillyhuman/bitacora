//! Tests of how a copy takes appends and heartbeats.
//!
//! # What is covered
//!
//! - An append is acknowledged with the copy's new end, and what was acknowledged survives a crash.
//! - Batches that leave a gap, are cut differently from the stored ones, or fail their checksum are
//!   refused; batches that are merely sent again are acknowledged again.
//! - A writer older than the segment, or one that describes another segment than the copy's, is refused.
//! - A nearly full disk refuses to create a records file and still appends to one that exists.
//! - A flush the disk refuses is never acknowledged, and leaves a copy the restarted replica can use.
//! - A heartbeat to a replica without a copy reports an empty copy, and a heartbeat to a copy whose
//!   flushed mark is current touches no disk.
//!
//! # How the fixtures work
//!
//! The fixtures come from `crate::test_support`: the copy under test is the one of segment 3 of stream 7
//! on a `MemoryDisk`, and `copy_with_records` gives it records 100 to 109 in two batches of five. A crash
//! is the disk forgetting what was not flushed, followed by loading the copy again.

use bitacora_segment::MemoryDisk;
use bitacora_types::Epoch;
use bitacora_types::replica_api::SegmentStatus;
use bytes::Bytes;

use super::*;
use crate::test_support::{
    BYTE_IN_A_FRAME, FIRST_SEQUENCE, NEARLY_FULL, ROOMY, append_request, batch, copy_with_records, frames_of,
    heartbeat, new_copy, read_request, readable_sequences, reload,
};

/// The position before the first record of the segment under test: the end of a copy that holds nothing.
const END_OF_AN_EMPTY_COPY: u64 = FIRST_SEQUENCE - 1;

/// What a restarted replica reports about a copy whose very first append met a disk that refused to
/// flush.
///
/// A refused flush stops the replica. What it leaves behind must be a copy the restarted replica can use,
/// not one that looks damaged.
async fn status_after_a_first_append_whose_flush_failed() -> SegmentStatus {
    let disk = MemoryDisk::new();
    disk.fail_flushes(true);
    let _refused = new_copy(&disk)
        .append(&append_request(&[batch(100, 5)], 0), ROOMY)
        .await;
    disk.crash();
    disk.fail_flushes(false);
    reload(&disk).await.status()
}

// Appends.

#[tokio::test]
async fn append_acknowledges_the_new_local_end() {
    let disk = MemoryDisk::new();
    let mut copy = new_copy(&disk);

    let response = copy.append(&append_request(&[batch(100, 5)], 0), ROOMY).await;

    assert_eq!(
        response,
        Ok(AppendResponse {
            local_end: SequenceNumber::new(104)
        })
    );
}

#[tokio::test]
async fn acknowledged_records_survive_a_crash() {
    let disk = MemoryDisk::new();
    copy_with_records(&disk).await;

    disk.crash();

    assert_eq!(reload(&disk).await.status().local_end, Some(SequenceNumber::new(109)));
}

#[tokio::test]
async fn append_leaving_a_gap_reports_where_to_resend_from() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;

    let refused = copy.append(&append_request(&[batch(112, 1)], 0), ROOMY).await;

    assert_eq!(
        refused,
        Err(ReplicaError::OutOfOrder {
            first_sequence: SequenceNumber::new(112),
            local_end: SequenceNumber::new(109),
        })
    );
}

#[tokio::test]
async fn first_append_not_starting_at_the_segment_start_is_refused() {
    let disk = MemoryDisk::new();
    let mut copy = new_copy(&disk);

    let refused = copy.append(&append_request(&[batch(105, 5)], 0), ROOMY).await;

    assert_eq!(
        refused,
        Err(ReplicaError::OutOfOrder {
            first_sequence: SequenceNumber::new(105),
            local_end: SequenceNumber::new(END_OF_AN_EMPTY_COPY),
        })
    );
}

#[tokio::test]
async fn refused_first_append_creates_no_file() {
    let disk = MemoryDisk::new();
    let mut copy = new_copy(&disk);

    let _refused = copy.append(&append_request(&[batch(105, 5)], 0), ROOMY).await;

    assert!(!copy.is_stored());
}

#[tokio::test]
async fn resent_batches_are_acknowledged_again() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;

    let response = copy
        .append(&append_request(&[batch(100, 5), batch(105, 5)], 0), ROOMY)
        .await;

    assert_eq!(
        response,
        Ok(AppendResponse {
            local_end: SequenceNumber::new(109)
        })
    );
}

#[tokio::test]
async fn resend_overlapping_the_end_stores_only_the_new_batches() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;

    copy.append(&append_request(&[batch(105, 5), batch(110, 2)], 111), ROOMY)
        .await
        .expect("a resend that continues the copy is accepted");

    let expected_sequences: Vec<u64> = (100..=111).collect();
    assert_eq!(
        readable_sequences(&mut copy, &read_request(FIRST_SEQUENCE)).await,
        expected_sequences
    );
}

#[tokio::test]
async fn resend_with_different_batch_boundaries_is_refused() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;

    let refused = copy.append(&append_request(&[batch(108, 4)], 0), ROOMY).await;

    assert!(matches!(refused, Err(ReplicaError::InvalidRequest { .. })));
}

#[tokio::test]
async fn frames_failing_their_checksum_are_refused() {
    let disk = MemoryDisk::new();
    let mut damaged_frames = frames_of(&[batch(100, 5)]).to_vec();
    damaged_frames[BYTE_IN_A_FRAME] ^= 0x01;
    let request = AppendRequest {
        batch_frames: Bytes::from(damaged_frames),
        ..heartbeat(0)
    };

    let refused = new_copy(&disk).append(&request, ROOMY).await;

    assert!(matches!(refused, Err(ReplicaError::InvalidRequest { .. })));
}

// Who may write, and to what.

#[tokio::test]
async fn append_with_a_descriptor_that_contradicts_the_copy_is_refused() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    let mut request = append_request(&[batch(110, 1)], 0);
    request.segment.first_sequence = SequenceNumber::new(90);

    let refused = copy.append(&request, ROOMY).await;

    assert!(matches!(refused, Err(ReplicaError::InvalidRequest { .. })));
}

#[tokio::test]
async fn writer_older_than_the_segment_is_refused() {
    let disk = MemoryDisk::new();
    let request = AppendRequest {
        writer_epoch: Epoch::new(4),
        ..append_request(&[batch(100, 5)], 0)
    };

    let refused = new_copy(&disk).append(&request, ROOMY).await;

    assert!(matches!(refused, Err(ReplicaError::InvalidRequest { .. })));
}

// Admission.

#[tokio::test]
async fn nearly_full_disk_refuses_to_create_a_segment() {
    let disk = MemoryDisk::new();
    let mut copy = new_copy(&disk);

    let refused = copy.append(&append_request(&[batch(100, 5)], 0), NEARLY_FULL).await;

    assert_eq!(refused, Err(ReplicaError::NoCapacity { used_percent: 90 }));
}

#[tokio::test]
async fn nearly_full_disk_still_appends_to_an_existing_segment() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;

    let response = copy.append(&append_request(&[batch(110, 1)], 0), NEARLY_FULL).await;

    assert!(response.is_ok(), "{response:?}");
}

// A flush the disk refuses.

#[tokio::test]
async fn append_whose_flush_fails_is_not_acknowledged() {
    let disk = MemoryDisk::new();
    disk.fail_flushes(true);
    let mut copy = new_copy(&disk);

    let failed = copy.append(&append_request(&[batch(100, 5)], 0), ROOMY).await;

    assert!(matches!(failed, Err(ReplicaError::Storage { .. })), "{failed:?}");
}

#[tokio::test]
async fn first_append_whose_flush_fails_leaves_a_copy_that_is_not_damaged() {
    let status = status_after_a_first_append_whose_flush_failed().await;

    assert!(!status.is_damaged, "{status:?}");
}

#[tokio::test]
async fn first_append_whose_flush_fails_leaves_an_empty_copy() {
    let status = status_after_a_first_append_whose_flush_failed().await;

    assert_eq!(status.local_end, Some(SequenceNumber::new(END_OF_AN_EMPTY_COPY)));
}

// Heartbeats.

#[tokio::test]
async fn heartbeat_to_a_replica_without_a_copy_reports_an_empty_copy() {
    let disk = MemoryDisk::new();
    let mut copy = new_copy(&disk);

    let response = copy.append(&heartbeat(0), ROOMY).await;

    assert_eq!(
        response,
        Ok(AppendResponse {
            local_end: SequenceNumber::new(END_OF_AN_EMPTY_COPY)
        })
    );
}

#[tokio::test]
async fn heartbeat_to_a_replica_without_a_copy_creates_no_file() {
    let disk = MemoryDisk::new();
    let mut copy = new_copy(&disk);

    copy.append(&heartbeat(0), ROOMY)
        .await
        .expect("a heartbeat needs no copy");

    assert!(!copy.is_stored());
}

#[tokio::test]
async fn heartbeat_to_a_copy_whose_mark_is_current_flushes_nothing() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    copy.append(&heartbeat(109), ROOMY)
        .await
        .expect("the first heartbeat brings the mark up to date");
    disk.fail_flushes(true);

    let second_heartbeat = copy.append(&heartbeat(109), ROOMY).await;

    assert!(second_heartbeat.is_ok(), "{second_heartbeat:?}");
}

#[tokio::test]
async fn heartbeat_whose_flush_of_the_mark_fails_is_refused() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    disk.fail_flushes(true);

    let failed = copy.append(&heartbeat(109), ROOMY).await;

    assert!(matches!(failed, Err(ReplicaError::Storage { .. })), "{failed:?}");
}

// Resent batches, on their own.

#[test]
fn batches_the_copy_holds_are_dropped_from_a_resend() {
    let resent = [batch(100, 5), batch(105, 5)];

    let new_batches = SegmentCopy::batches_after(&resent, SequenceNumber::new(104));

    assert_eq!(new_batches, Ok(&resent[1..]));
}

#[test]
fn batch_that_straddles_the_end_of_the_copy_is_refused() {
    let resent = [batch(100, 5)];

    let refused = SegmentCopy::batches_after(&resent, SequenceNumber::new(102));

    assert!(matches!(refused, Err(ReplicaError::InvalidRequest { .. })));
}
