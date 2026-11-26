//! Tests of the replica as a node: its identity, opening, stopping, scrubbing, and what it tells its
//! host when the disk refuses a write.
//!
//! # What is covered
//!
//! - An empty data directory adopts the proposed identity; a used one keeps its first; an unusable
//!   identity keeps the replica from opening.
//! - A restarted replica finds its segments again: their records, their fences, their headers. What an
//!   interrupted repair left behind is removed.
//! - A stopped replica refuses every request, fails the ones that were waiting, and leaves what it had
//!   flushed for the next start.
//! - The scrubber counts the sealed copies it finds damaged.
//! - A write the disk refuses is never acknowledged, makes the copy refuse later requests, and is
//!   reported to the host: by `write_failure` for one that looks, by `write_failed` for one that waits.
//!
//! The operations other programs call are tested in `replica/api_test.rs`, admission in
//! `replica/admission_test.rs`.
//!
//! # How the fixtures work
//!
//! The fixtures come from `crate::test_support`. A replica runs on a `MemoryDisk`; a restart is stopping
//! or dropping it, letting the disk forget what was not flushed, and opening a replica on the same disk.

use std::time::Duration;

use bitacora_segment::MemoryDisk;
use bitacora_types::replica_api::{ReplicaApi, SegmentStatus};
use bitacora_types::{Epoch, SequenceNumber};

use super::*;
use crate::test_support::{
    BYTE_IN_THE_FIRST_BATCH, CopySummary, FIRST_SEQUENCE, OTHER_SEGMENT, REPAIR_SCRATCH_FILE, SEGMENT, SEGMENT_FILE,
    append_request, append_request_to, batch, batch_of, copy_of, fence_request, no_peers, open_replica, read_request,
    seal_request, small_segments, waiting_read_request,
};

/// The epoch [`OTHER_SEGMENT`] is fenced at in the tests of restarting.
const FENCE_OF_THE_OTHER_SEGMENT: Epoch = Epoch::new(9);

/// What a replica lists after a restart, if before it held records 100 to 104 of [`SEGMENT`] and a fence
/// at [`FENCE_OF_THE_OTHER_SEGMENT`] for [`OTHER_SEGMENT`], of which it never received a record.
async fn segments_listed_after_a_restart() -> Vec<SegmentStatus> {
    let disk = MemoryDisk::new();
    let replica = open_replica(&disk, "replica-a").await;
    replica
        .append(append_request(&[batch(100, 5)], 0))
        .await
        .expect("an empty replica accepts the first batch");
    replica
        .fence(fence_request(OTHER_SEGMENT, FENCE_OF_THE_OTHER_SEGMENT))
        .await
        .expect("a replica accepts a fence for a segment it never saw");
    drop(replica);
    disk.crash();

    open_replica(&disk, "replica-a")
        .await
        .list_segments()
        .await
        .expect("a running replica lists its segments")
}

/// A replica holding records 100 to 104 of [`SEGMENT`], sealed there.
async fn replica_with_a_sealed_copy(disk: &MemoryDisk) -> Replica {
    let replica = open_replica(disk, "replica-a").await;
    replica
        .append(append_request(&[batch(100, 5)], 0))
        .await
        .expect("an empty replica accepts the first batch");
    replica
        .seal(seal_request(104))
        .await
        .expect("a seal at the copy's end is accepted");
    replica
}

/// A replica holding records 100 to 104 of [`SEGMENT`] on a disk that has started to refuse flushes.
async fn replica_on_a_disk_that_refuses_flushes(disk: &MemoryDisk) -> Replica {
    let replica = open_replica(disk, "replica-a").await;
    replica
        .append(append_request(&[batch(100, 5)], 0))
        .await
        .expect("an empty replica accepts the first batch");
    disk.fail_flushes(true);
    replica
}

// Identity.

#[tokio::test]
async fn empty_disk_adopts_the_proposed_identity() {
    let replica = open_replica(&MemoryDisk::new(), "replica-a").await;

    assert_eq!(replica.replica_id().as_str(), "replica-a");
}

#[tokio::test]
async fn used_disk_keeps_its_first_identity() {
    let disk = MemoryDisk::new();
    open_replica(&disk, "replica-a").await;

    let reopened = open_replica(&disk, "replica-b").await;

    assert_eq!(reopened.replica_id().as_str(), "replica-a");
}

#[tokio::test]
async fn disk_with_an_unusable_identity_is_refused() {
    let disk = MemoryDisk::new();
    disk.write_file_atomically(REPLICA_ID_PATH, Bytes::from_static(b"not/valid"))
        .await
        .expect("a working disk stores the file");
    let proposed_replica_id = ReplicaId::new("replica-a").expect("the name is a valid identifier");

    let opened = Replica::open(Arc::new(disk), proposed_replica_id, small_segments(), no_peers()).await;

    assert!(matches!(opened, Err(ReplicaError::Storage { .. })));
}

// Opening a used data directory.

#[tokio::test]
async fn restarted_replica_lists_its_segments_in_the_order_of_their_addresses() {
    let listed = segments_listed_after_a_restart().await;

    let mut listed_addresses = Vec::new();
    for status in &listed {
        listed_addresses.push(status.address);
    }
    assert_eq!(listed_addresses, [SEGMENT, OTHER_SEGMENT]);
}

#[tokio::test]
async fn restarted_replica_still_holds_its_records() {
    let listed = segments_listed_after_a_restart().await;

    assert_eq!(listed[0].local_end, Some(SequenceNumber::new(104)));
}

#[tokio::test]
async fn restarted_replica_remembers_a_fence_for_a_segment_it_has_no_records_of() {
    let listed = segments_listed_after_a_restart().await;

    assert_eq!(listed[1].fenced_epoch, FENCE_OF_THE_OTHER_SEGMENT);
}

#[tokio::test]
async fn restarted_replica_has_no_records_of_a_segment_it_only_fenced() {
    let listed = segments_listed_after_a_restart().await;

    assert_eq!(listed[1].local_end, None);
}

#[tokio::test]
async fn reported_header_survives_a_restart() {
    let disk = MemoryDisk::new();
    let replica = open_replica(&disk, "replica-a").await;
    replica
        .append(append_request(&[batch(100, 5)], 0))
        .await
        .expect("an empty replica accepts the first batch");
    let before_the_restart = replica
        .list_segments()
        .await
        .expect("a running replica lists its segments");
    replica.stop();

    let restarted = open_replica(&disk, "replica-a").await;

    let after_the_restart = restarted
        .list_segments()
        .await
        .expect("a running replica lists its segments");
    assert_eq!(after_the_restart[0].descriptor, before_the_restart[0].descriptor);
}

#[tokio::test]
async fn scratch_file_of_an_interrupted_repair_is_removed_at_startup() {
    let disk = MemoryDisk::new();
    disk.write_file_atomically(REPAIR_SCRATCH_FILE, Bytes::from_static(b"half a copy"))
        .await
        .expect("a working disk stores the leftover");

    let _replica = open_replica(&disk, "replica-a").await;

    let stored_paths = disk.list_files().await.expect("a working disk lists its files");
    assert_eq!(stored_paths, [REPLICA_ID_PATH]);
}

// Stopping.

#[tokio::test]
async fn stopped_replica_refuses_appends() {
    let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
    replica.stop();

    let refused = replica.append(append_request(&[batch(100, 5)], 0)).await;

    assert!(matches!(refused, Err(ReplicaError::Unavailable { .. })), "{refused:?}");
}

#[tokio::test]
async fn stopped_replica_refuses_reads() {
    let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
    replica
        .append(append_request(&[batch(100, 5)], 104))
        .await
        .expect("an empty replica accepts the first batch");
    replica.stop();

    let refused = replica.read(read_request(FIRST_SEQUENCE)).await;

    assert!(matches!(refused, Err(ReplicaError::Unavailable { .. })), "{refused:?}");
}

#[tokio::test]
async fn stopped_replica_refuses_to_list_its_segments() {
    let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
    replica.stop();

    let refused = replica.list_segments().await;

    assert!(matches!(refused, Err(ReplicaError::Unavailable { .. })), "{refused:?}");
}

#[tokio::test]
async fn stopping_twice_is_harmless() {
    let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
    replica.stop();

    replica.stop();

    let refused = replica.list_segments().await;
    assert!(matches!(refused, Err(ReplicaError::Unavailable { .. })), "{refused:?}");
}

#[tokio::test]
async fn stopped_replica_leaves_flushed_records_for_the_next_start() {
    let disk = MemoryDisk::new();
    let replica = open_replica(&disk, "replica-a").await;
    replica
        .append(append_request(&[batch(100, 5)], 0))
        .await
        .expect("an empty replica accepts the first batch");
    replica.stop();
    disk.crash();

    let restarted = open_replica(&disk, "replica-a").await;

    assert_eq!(copy_of(&restarted).await.local_end, Some(104));
}

#[tokio::test(start_paused = true)]
async fn request_waiting_on_a_stopped_replica_fails_instead_of_hanging() {
    let replica = Arc::new(open_replica(&MemoryDisk::new(), "replica-a").await);
    replica
        .append(append_request(&[batch(100, 5)], 0))
        .await
        .expect("an empty replica accepts the first batch");
    let reading_replica = Arc::clone(&replica);
    let waiting_reader = tokio::spawn(async move {
        reading_replica
            .read(waiting_read_request(FIRST_SEQUENCE, Duration::from_secs(10)))
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    replica.stop();

    let answer = waiting_reader.await.expect("the reader's task finishes");
    assert!(matches!(answer, Err(ReplicaError::Unavailable { .. })), "{answer:?}");
}

// Scrubbing.

#[tokio::test]
async fn scrub_finds_a_corrupted_sealed_copy() {
    let disk = MemoryDisk::new();
    let replica = replica_with_a_sealed_copy(&disk).await;
    disk.corrupt_byte(SEGMENT_FILE, BYTE_IN_THE_FIRST_BATCH, 0x20);

    let damaged_copies = replica.scrub_sealed_segments().await;

    assert_eq!(damaged_copies, Ok(1));
}

#[tokio::test]
async fn scrub_of_intact_copies_finds_nothing() {
    let replica = replica_with_a_sealed_copy(&MemoryDisk::new()).await;

    let damaged_copies = replica.scrub_sealed_segments().await;

    assert_eq!(damaged_copies, Ok(0));
}

#[tokio::test]
async fn scrub_does_not_reread_an_open_copy() {
    let disk = MemoryDisk::new();
    let replica = open_replica(&disk, "replica-a").await;
    replica
        .append(append_request(&[batch(100, 5)], 0))
        .await
        .expect("an empty replica accepts the first batch");
    disk.corrupt_byte(SEGMENT_FILE, BYTE_IN_THE_FIRST_BATCH, 0x20);

    let damaged_copies = replica.scrub_sealed_segments().await;

    assert_eq!(damaged_copies, Ok(0));
}

// A write the disk refuses.

#[tokio::test]
async fn append_whose_flush_fails_is_never_acknowledged() {
    let disk = MemoryDisk::new();
    let replica = replica_on_a_disk_that_refuses_flushes(&disk).await;

    let failed = replica.append(append_request(&[batch(105, 1)], 0)).await;

    assert!(matches!(failed, Err(ReplicaError::Storage { .. })), "{failed:?}");
}

#[tokio::test]
async fn copy_whose_flush_failed_refuses_the_same_append_once_the_disk_recovers() {
    let disk = MemoryDisk::new();
    let replica = replica_on_a_disk_that_refuses_flushes(&disk).await;
    let _failed = replica.append(append_request(&[batch(105, 1)], 0)).await;
    disk.fail_flushes(false);

    let after_the_failure = replica.append(append_request(&[batch(105, 1)], 0)).await;

    assert!(
        matches!(after_the_failure, Err(ReplicaError::Storage { .. })),
        "{after_the_failure:?}"
    );
}

#[tokio::test]
async fn replica_on_a_working_disk_reports_no_write_failure() {
    let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
    replica
        .append(append_request(&[batch(100, 5)], 0))
        .await
        .expect("an empty replica accepts the first batch");

    assert_eq!(replica.write_failure(), None);
}

#[tokio::test]
async fn failed_flush_is_reported_to_the_host() {
    let disk = MemoryDisk::new();
    let replica = replica_on_a_disk_that_refuses_flushes(&disk).await;

    let _failed = replica.append(append_request(&[batch(105, 1)], 0)).await;

    let reported = replica.write_failure().expect("the refused flush is reported");
    assert_eq!(reported.segment, SEGMENT);
}

#[tokio::test]
async fn first_failed_flush_stays_the_one_reported() {
    let disk = MemoryDisk::new();
    let replica = open_replica(&disk, "replica-a").await;
    for segment in [SEGMENT, OTHER_SEGMENT] {
        replica
            .append(append_request_to(segment, &[batch_of(segment, 100, 5)], 0))
            .await
            .expect("an empty replica accepts the first batch of a segment");
    }
    disk.fail_flushes(true);
    let _first_failure = replica.append(append_request(&[batch(105, 1)], 0)).await;

    let _second_failure = replica
        .append(append_request_to(OTHER_SEGMENT, &[batch_of(OTHER_SEGMENT, 105, 1)], 0))
        .await;

    let reported = replica.write_failure().expect("the refused flush is reported");
    assert_eq!(reported.segment, SEGMENT);
}

#[tokio::test]
async fn host_waiting_for_a_write_failure_gets_the_failed_flush() {
    let disk = MemoryDisk::new();
    let replica = Arc::new(replica_on_a_disk_that_refuses_flushes(&disk).await);
    let watched_replica = Arc::clone(&replica);
    let waiting_host = tokio::spawn(async move { watched_replica.write_failed().await });

    let _failed = replica.append(append_request(&[batch(105, 1)], 0)).await;

    let reported = waiting_host.await.expect("the host's task finishes");
    assert_eq!(reported.segment, SEGMENT);
}

#[tokio::test]
async fn copy_whose_disk_refused_a_write_serves_its_flushed_records_after_a_restart() {
    let disk = MemoryDisk::new();
    let replica = replica_on_a_disk_that_refuses_flushes(&disk).await;
    let _refused_append = replica.append(append_request(&[batch(105, 5)], 0)).await;
    replica.stop();
    disk.fail_flushes(false);
    disk.crash();

    let restarted = open_replica(&disk, "replica-a").await;

    let flushed_records_only = CopySummary {
        local_end: Some(104),
        sealed_end: None,
        is_damaged: false,
    };
    assert_eq!(copy_of(&restarted).await, flushed_records_only);
}
