//! Tests of a segment copy as a whole: loading, health, verification and deletion.
//!
//! # What is covered
//!
//! - What counts as stored: records, or a fence or seal alone, but not a copy nobody wrote to.
//! - A copy whose header, state or flushed records are unreadable loads as damaged, and stays unusable.
//! - The scrubber marks a copy with a flipped bit as damaged and leaves an intact one alone.
//! - A damaged copy refuses appends, fences, seals and reads.
//! - A write the disk refuses is remembered as such, which is what stops the replica.
//! - A deleted copy leaves nothing on the disk.
//!
//! The operations themselves are tested next to their code, in `copy/*_test.rs`.
//!
//! # How the fixtures work
//!
//! The fixtures come from `crate::test_support`. The copy under test holds records 100 to 109 in two
//! batches of five. Damage is a bit flipped on the `MemoryDisk`, either in a record of the first batch
//! or in the header of a file; `damaged_copy` is a copy the scrubber has found such a bit in.

use bitacora_segment::MemoryDisk;

use super::*;
use crate::test_support::{
    BYTE_IN_THE_FIRST_BATCH, NEXT_EPOCH, ROOMY, SEGMENT_FILE, SEGMENT_STATE_FILE, append_request, batch,
    copy_with_records, heartbeat, new_copy, read_request, reload,
};

/// An offset inside the header of a records file. Flipping a bit there makes the header fail its
/// checksum.
const BYTE_IN_THE_FILE_HEADER: usize = 20;

/// An offset inside a state file. Flipping a bit there makes the state fail its checksum.
const BYTE_IN_THE_STATE_FILE: usize = 15;

/// A copy with records 100 to 109 in which the scrubber has found a flipped bit.
async fn damaged_copy(disk: &MemoryDisk) -> SegmentCopy {
    let mut copy = copy_with_records(disk).await;
    disk.corrupt_byte(SEGMENT_FILE, BYTE_IN_THE_FIRST_BATCH, 0x10);
    copy.verify().await;
    copy
}

/// A copy with records 100 to 109 whose last append met a disk that refused to flush.
async fn copy_refused_a_flush(disk: &MemoryDisk) -> SegmentCopy {
    let mut copy = copy_with_records(disk).await;
    disk.fail_flushes(true);
    let _refused = copy.append(&append_request(&[batch(110, 1)], 0), ROOMY).await;
    copy
}

/// Loads the copy again after a crash that followed damage to records the copy had flushed and the
/// writer had declared durable.
///
/// Such damage must not come back as a healthy, shorter copy: a recovering writer would count on it to
/// hold everything committed.
async fn copy_reloaded_after_damage_to_flushed_records() -> SegmentCopy {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    copy.append(&heartbeat(109), ROOMY)
        .await
        .expect("a sound copy accepts a heartbeat");
    disk.corrupt_byte(SEGMENT_FILE, BYTE_IN_THE_FIRST_BATCH, 0x01);
    disk.crash();
    reload(&disk).await
}

// What counts as stored.

#[tokio::test]
async fn copy_nobody_wrote_to_is_not_stored() {
    let disk = MemoryDisk::new();

    let copy = new_copy(&disk);

    assert!(!copy.is_stored());
}

#[tokio::test]
async fn copy_with_records_is_stored() {
    let disk = MemoryDisk::new();

    let copy = copy_with_records(&disk).await;

    assert!(copy.is_stored());
}

#[tokio::test]
async fn copy_with_only_a_fence_is_stored() {
    let disk = MemoryDisk::new();
    let mut copy = new_copy(&disk);

    copy.fence(NEXT_EPOCH)
        .await
        .expect("a replica without a copy accepts a fence");

    assert!(copy.is_stored());
}

#[tokio::test]
async fn copy_with_only_a_fence_is_still_stored_after_a_restart() {
    let disk = MemoryDisk::new();
    new_copy(&disk)
        .fence(NEXT_EPOCH)
        .await
        .expect("a replica without a copy accepts a fence");
    disk.crash();

    let reloaded = reload(&disk).await;

    assert!(reloaded.is_stored());
}

#[tokio::test]
async fn copy_loaded_from_an_empty_disk_is_not_stored() {
    let disk = MemoryDisk::new();

    let loaded = reload(&disk).await;

    assert!(!loaded.is_stored());
}

// Loading files that cannot be read.

#[tokio::test]
async fn copy_with_an_unreadable_header_loads_as_damaged() {
    let disk = MemoryDisk::new();
    copy_with_records(&disk).await;
    disk.corrupt_byte(SEGMENT_FILE, BYTE_IN_THE_FILE_HEADER, 0x01);

    let reloaded = reload(&disk).await;

    assert!(reloaded.status().is_damaged);
}

#[tokio::test]
async fn copy_with_an_unreadable_state_loads_as_damaged() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    copy.fence(NEXT_EPOCH)
        .await
        .expect("a sound copy accepts a newer epoch");
    disk.corrupt_byte(SEGMENT_STATE_FILE, BYTE_IN_THE_STATE_FILE, 0x01);

    let reloaded = reload(&disk).await;

    assert!(reloaded.status().is_damaged);
}

#[tokio::test]
async fn copy_whose_flushed_records_were_damaged_loads_as_damaged() {
    let reloaded = copy_reloaded_after_damage_to_flushed_records().await;

    assert!(reloaded.status().is_damaged);
}

#[tokio::test]
async fn copy_whose_flushed_records_were_damaged_refuses_a_recovering_writer() {
    let mut reloaded = copy_reloaded_after_damage_to_flushed_records().await;

    let refused = reloaded.fence(NEXT_EPOCH).await;

    assert!(matches!(refused, Err(ReplicaError::Storage { .. })), "{refused:?}");
}

// Verification.

#[tokio::test]
async fn scrub_marks_a_corrupted_copy_as_damaged() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    disk.corrupt_byte(SEGMENT_FILE, BYTE_IN_THE_FIRST_BATCH, 0x10);

    copy.verify().await;

    assert!(copy.status().is_damaged);
}

#[tokio::test]
async fn scrub_leaves_an_intact_copy_alone() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;

    copy.verify().await;

    assert!(!copy.status().is_damaged);
}

#[tokio::test]
async fn scrub_of_a_copy_without_records_finds_nothing() {
    let disk = MemoryDisk::new();
    let mut copy = new_copy(&disk);

    copy.verify().await;

    assert!(!copy.status().is_damaged);
}

// A damaged copy.

#[tokio::test]
async fn damaged_copy_refuses_appends() {
    let disk = MemoryDisk::new();
    let mut copy = damaged_copy(&disk).await;

    let refused = copy.append(&append_request(&[batch(110, 1)], 0), ROOMY).await;

    assert!(matches!(refused, Err(ReplicaError::Storage { .. })), "{refused:?}");
}

#[tokio::test]
async fn damaged_copy_refuses_fences() {
    let disk = MemoryDisk::new();
    let mut copy = damaged_copy(&disk).await;

    let refused = copy.fence(NEXT_EPOCH).await;

    assert!(matches!(refused, Err(ReplicaError::Storage { .. })), "{refused:?}");
}

#[tokio::test]
async fn damaged_copy_refuses_seals() {
    let disk = MemoryDisk::new();
    let mut copy = damaged_copy(&disk).await;

    let refused = copy.seal(SequenceNumber::new(109)).await;

    assert!(matches!(refused, Err(ReplicaError::Storage { .. })), "{refused:?}");
}

#[tokio::test]
async fn damaged_copy_refuses_reads() {
    let disk = MemoryDisk::new();
    let mut copy = damaged_copy(&disk).await;

    let refused = copy.read(&read_request(100)).await;

    assert!(matches!(refused, Err(ReplicaError::Storage { .. })), "{refused:?}");
}

#[tokio::test]
async fn damage_found_by_the_scrubber_is_not_a_refused_write() {
    let disk = MemoryDisk::new();

    let copy = damaged_copy(&disk).await;

    assert_eq!(copy.refused_write(), None);
}

// A write the disk refused.

#[tokio::test]
async fn sound_copy_has_no_refused_write() {
    let disk = MemoryDisk::new();

    let copy = copy_with_records(&disk).await;

    assert_eq!(copy.refused_write(), None);
}

#[tokio::test]
async fn refused_flush_is_remembered_as_a_refused_write() {
    let disk = MemoryDisk::new();

    let copy = copy_refused_a_flush(&disk).await;

    assert!(copy.refused_write().is_some());
}

#[tokio::test]
async fn copy_refused_a_write_reports_itself_damaged() {
    let disk = MemoryDisk::new();

    let copy = copy_refused_a_flush(&disk).await;

    assert!(copy.status().is_damaged);
}

#[tokio::test]
async fn copy_refused_a_write_refuses_later_appends_even_if_the_disk_recovers() {
    let disk = MemoryDisk::new();
    let mut copy = copy_refused_a_flush(&disk).await;
    disk.fail_flushes(false);

    let refused = copy.append(&append_request(&[batch(110, 1)], 0), ROOMY).await;

    assert!(matches!(refused, Err(ReplicaError::Storage { .. })), "{refused:?}");
}

#[tokio::test]
async fn copy_refused_a_write_stays_one_when_the_scrubber_also_finds_damage() {
    let disk = MemoryDisk::new();
    let mut copy = copy_refused_a_flush(&disk).await;
    disk.corrupt_byte(SEGMENT_FILE, BYTE_IN_THE_FIRST_BATCH, 0x10);

    copy.verify().await;

    assert!(copy.refused_write().is_some());
}

// Deletion.

#[tokio::test]
async fn deleted_copy_leaves_nothing_on_disk() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    copy.seal(SequenceNumber::new(109))
        .await
        .expect("a seal at the copy's end is accepted");

    copy.delete().await.expect("a working disk removes the files");

    let stored_paths = disk.list_files().await.expect("a working disk lists its files");
    assert_eq!(stored_paths, Vec::<String>::new());
}

#[tokio::test]
async fn deleted_copy_is_not_stored() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;

    copy.delete().await.expect("a working disk removes the files");

    assert!(!copy.is_stored());
}
