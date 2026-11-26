//! Tests of the operations a replica offers to other programs.
//!
//! # What is covered
//!
//! - Appended records are readable once durable; a segment the replica does not hold is not found; a
//!   refused append leaves no segment behind.
//! - A waiting read returns as soon as there is something to read, gives up empty after its wait, is
//!   capped by the replica, and does not wait at the end of a sealed segment or for a recovering writer.
//! - A deleted segment is gone, can be deleted again, and can be written again.
//! - The listing reports what the header of each records file says; the status reports the identity, the
//!   disk, the draining switch and the number of segments.
//!
//! Repairs are tested in `copy/repair_test.rs`, uploads in `upload_test.rs`, admission in
//! `admission_test.rs`, and stopping in `replica_test.rs`.
//!
//! # How the fixtures work
//!
//! The fixtures come from `crate::test_support`: a replica on a `MemoryDisk`, and requests for segment 3
//! of stream 7. The tests of waiting reads run on a paused clock, where time only moves when every task
//! is waiting for it, so a wait takes exactly as long as the replica made it. [`TimedRead`] is a read
//! with the time it took.

use std::sync::Arc;
use std::time::Duration;

use bitacora_segment::MemoryDisk;
use bitacora_types::SequenceNumber;
use tokio::time::Instant;

use super::*;
use crate::test_support::{
    FIRST_SEQUENCE, NEXT_EPOCH, SEGMENT, SEGMENT_EPOCH, append_request, batch, descriptor, fence_request, heartbeat,
    open_replica, read_request, recovery_read_request, seal_request, sequences_in, small_segments,
    waiting_read_request,
};

/// How long after a reader starts waiting the records become durable, in the tests where they do.
const DELAY_BEFORE_RECORDS_BECOME_DURABLE: Duration = Duration::from_millis(50);

/// The capacity of the disk in the tests of the status, in bytes.
const DISK_CAPACITY_BYTES: u64 = 1_000_000;

/// A read a replica answered, and how long the answer took.
struct TimedRead {
    /// What the replica answered.
    response: ReadResponse,
    /// How long the replica took to answer, on the paused clock of the test.
    waited: Duration,
}

/// Sends `request` to `replica` and measures how long the answer takes.
async fn timed_read(replica: &Replica, request: ReadRequest) -> TimedRead {
    let asked_at = Instant::now();
    let response = replica.read(request).await.expect("a running replica serves the read");
    TimedRead {
        response,
        waited: asked_at.elapsed(),
    }
}

/// A replica holding records 100 to 104 of [`SEGMENT`], of which `durable_sequence` and below are
/// declared durable. Pass 0 for none.
async fn replica_with_records(durable_sequence: u64) -> Replica {
    let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
    replica
        .append(append_request(&[batch(100, 5)], durable_sequence))
        .await
        .expect("an empty replica accepts the first batch");
    replica
}

/// A replica holding records 100 to 104 of [`SEGMENT`], sealed there.
async fn replica_with_a_sealed_segment() -> Replica {
    let replica = replica_with_records(0).await;
    replica
        .seal(seal_request(104))
        .await
        .expect("a seal at the copy's end is accepted");
    replica
}

/// The read of a reader that waits up to ten seconds for records which become durable
/// [`DELAY_BEFORE_RECORDS_BECOME_DURABLE`] after it started waiting.
async fn read_that_waited_for_records_to_become_durable() -> TimedRead {
    let replica = Arc::new(replica_with_records(0).await);
    let reading_replica = Arc::clone(&replica);
    let waiting_reader = tokio::spawn(async move {
        let patient_read = waiting_read_request(FIRST_SEQUENCE, Duration::from_secs(10));
        timed_read(&reading_replica, patient_read).await
    });
    tokio::time::sleep(DELAY_BEFORE_RECORDS_BECOME_DURABLE).await;
    replica
        .append(heartbeat(104))
        .await
        .expect("a sound copy accepts a heartbeat");
    waiting_reader.await.expect("the reader's task finishes")
}

/// The status of a replica named `replica-a` on a disk of [`DISK_CAPACITY_BYTES`] that holds one segment.
async fn status_of_a_replica_with_one_segment() -> ReplicaStatus {
    let replica = open_replica(&MemoryDisk::with_capacity(DISK_CAPACITY_BYTES), "replica-a").await;
    replica
        .append(append_request(&[batch(100, 5)], 0))
        .await
        .expect("an empty replica accepts the first batch");
    replica.status().await.expect("a running replica reports its status")
}

// Appends and reads.

#[tokio::test]
async fn appended_records_are_readable_once_durable() {
    let replica = replica_with_records(104).await;

    let response = replica
        .read(read_request(FIRST_SEQUENCE))
        .await
        .expect("a running replica serves the read");

    assert_eq!(sequences_in(&response.batch_frames), [100, 101, 102, 103, 104]);
}

#[tokio::test]
async fn read_of_an_unknown_segment_finds_nothing() {
    let replica = open_replica(&MemoryDisk::new(), "replica-a").await;

    let not_found = replica.read(read_request(FIRST_SEQUENCE)).await;

    assert_eq!(not_found, Err(ReplicaError::SegmentNotFound { segment: SEGMENT }));
}

#[tokio::test]
async fn refused_append_does_not_list_a_segment() {
    let replica = open_replica(&MemoryDisk::new(), "replica-a").await;

    let _refused = replica.append(append_request(&[batch(105, 1)], 0)).await;

    assert_eq!(replica.list_segments().await, Ok(Vec::new()));
}

// Waiting reads.

#[tokio::test(start_paused = true)]
async fn waiting_read_returns_the_records_that_became_durable() {
    let read = read_that_waited_for_records_to_become_durable().await;

    assert_eq!(sequences_in(&read.response.batch_frames), [100, 101, 102, 103, 104]);
}

#[tokio::test(start_paused = true)]
async fn waiting_read_returns_as_soon_as_records_become_durable() {
    let read = read_that_waited_for_records_to_become_durable().await;

    assert_eq!(read.waited, DELAY_BEFORE_RECORDS_BECOME_DURABLE);
}

#[tokio::test(start_paused = true)]
async fn waiting_read_gives_up_after_its_wait() {
    let replica = replica_with_records(0).await;
    let requested_wait = Duration::from_secs(2);

    let read = timed_read(&replica, waiting_read_request(FIRST_SEQUENCE, requested_wait)).await;

    assert_eq!(read.waited, requested_wait);
}

#[tokio::test(start_paused = true)]
async fn waiting_read_that_gave_up_returns_no_records() {
    let replica = replica_with_records(0).await;

    let read = timed_read(&replica, waiting_read_request(FIRST_SEQUENCE, Duration::from_secs(2))).await;

    assert!(read.response.batch_frames.is_empty());
}

#[tokio::test(start_paused = true)]
async fn waiting_read_is_capped_by_the_replica() {
    let replica = replica_with_records(0).await;
    let wait_beyond_the_cap = Duration::from_secs(3_600);

    let read = timed_read(&replica, waiting_read_request(FIRST_SEQUENCE, wait_beyond_the_cap)).await;

    assert_eq!(read.waited, small_segments().longest_read_wait);
}

#[tokio::test(start_paused = true)]
async fn waiting_read_at_the_end_of_a_sealed_segment_returns_at_once() {
    let replica = replica_with_a_sealed_segment().await;

    let read = timed_read(&replica, waiting_read_request(105, Duration::from_secs(10))).await;

    assert_eq!(read.waited, Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn waiting_read_at_the_end_of_a_sealed_segment_reports_the_sealed_end() {
    let replica = replica_with_a_sealed_segment().await;

    let read = timed_read(&replica, waiting_read_request(105, Duration::from_secs(10))).await;

    assert_eq!(read.response.sealed_end, Some(SequenceNumber::new(104)));
}

#[tokio::test(start_paused = true)]
async fn read_of_a_recovering_writer_does_not_wait() {
    let replica = replica_with_records(0).await;
    replica
        .fence(fence_request(SEGMENT, NEXT_EPOCH))
        .await
        .expect("a sound copy accepts a newer epoch");
    let recovery_read_past_the_end = ReadRequest {
        from_sequence: SequenceNumber::new(105),
        wait_for_records: Some(Duration::from_secs(10)),
        ..recovery_read_request(NEXT_EPOCH)
    };

    let read = timed_read(&replica, recovery_read_past_the_end).await;

    assert_eq!(read.waited, Duration::ZERO);
}

// Deleting.

#[tokio::test]
async fn deleted_segment_is_not_listed() {
    let replica = replica_with_records(104).await;

    replica
        .delete_segment(SEGMENT)
        .await
        .expect("a working disk removes the files");

    assert_eq!(replica.list_segments().await, Ok(Vec::new()));
}

#[tokio::test]
async fn deleted_segment_is_not_readable() {
    let replica = replica_with_records(104).await;

    replica
        .delete_segment(SEGMENT)
        .await
        .expect("a working disk removes the files");

    let not_found = replica.read(read_request(FIRST_SEQUENCE)).await;
    assert_eq!(not_found, Err(ReplicaError::SegmentNotFound { segment: SEGMENT }));
}

#[tokio::test]
async fn deleting_an_unknown_segment_succeeds() {
    let replica = open_replica(&MemoryDisk::new(), "replica-a").await;

    let deleted = replica.delete_segment(SEGMENT).await;

    assert_eq!(deleted, Ok(()));
}

#[tokio::test]
async fn deleted_segment_can_be_written_again() {
    let replica = replica_with_records(0).await;
    replica
        .delete_segment(SEGMENT)
        .await
        .expect("a working disk removes the files");

    let response = replica.append(append_request(&[batch(100, 2)], 0)).await;

    assert_eq!(
        response,
        Ok(AppendResponse {
            local_end: SequenceNumber::new(101)
        })
    );
}

// Listing.

/// The header is what lets a stream's segment list be reconstructed from the replicas if the metadata
/// store is lost.
#[tokio::test]
async fn listed_copy_with_a_file_reports_what_its_header_says() {
    let replica = replica_with_records(0).await;

    let listed = replica
        .list_segments()
        .await
        .expect("a running replica lists its segments");

    assert_eq!(listed[0].descriptor, Some(descriptor()));
}

#[tokio::test]
async fn listed_copy_without_a_file_reports_no_header() {
    let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
    replica
        .fence(fence_request(SEGMENT, SEGMENT_EPOCH))
        .await
        .expect("a replica accepts a fence for a segment it never saw");

    let listed = replica
        .list_segments()
        .await
        .expect("a running replica lists its segments");

    assert_eq!(listed[0].descriptor, None);
}

// Status and draining.

#[tokio::test]
async fn status_reports_the_identity_of_the_replica() {
    let status = status_of_a_replica_with_one_segment().await;

    assert_eq!(status.replica_id.as_str(), "replica-a");
}

#[tokio::test]
async fn status_reports_the_capacity_of_the_disk() {
    let status = status_of_a_replica_with_one_segment().await;

    assert_eq!(status.capacity_bytes, DISK_CAPACITY_BYTES);
}

#[tokio::test]
async fn status_reports_less_free_space_once_a_segment_is_stored() {
    let status = status_of_a_replica_with_one_segment().await;

    assert!(status.free_bytes < DISK_CAPACITY_BYTES, "{status:?}");
}

#[tokio::test]
async fn status_counts_the_stored_segments() {
    let status = status_of_a_replica_with_one_segment().await;

    assert_eq!(status.segment_count, 1);
}

#[tokio::test]
async fn status_of_a_replica_nobody_drained_says_it_is_not_draining() {
    let status = status_of_a_replica_with_one_segment().await;

    assert!(!status.is_draining);
}

#[tokio::test]
async fn status_of_a_drained_replica_says_it_is_draining() {
    let replica = open_replica(&MemoryDisk::new(), "replica-a").await;

    replica.drain().await.expect("a running replica drains");

    let status = replica.status().await.expect("a running replica reports its status");
    assert!(status.is_draining);
}
