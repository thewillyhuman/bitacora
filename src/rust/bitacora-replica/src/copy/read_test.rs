//! Tests of what a copy serves to a read.
//!
//! # What is covered
//!
//! - Readers see only what the writer declared durable; a heartbeat moves that forward, never backwards,
//!   and never beyond what the copy holds.
//! - A restarted copy serves nothing until it hears from the writer again.
//! - A recovering writer reads the uncommitted tail, but only once it has fenced the copy, and only at
//!   the epoch it fenced it at.
//! - A read of a sealed copy reports the sealed end; a read of a segment without records finds nothing.
//!
//! # How the fixtures work
//!
//! The fixtures come from `crate::test_support`. The copy under test holds records 100 to 109 in two
//! batches of five, written in epoch 5, of which 100 to 104 are declared durable: 105 to 109 are the
//! uncommitted tail. `read_request` is a reader's read, `recovery_read_request` the read of a writer that
//! takes over.

use bitacora_segment::MemoryDisk;

use super::*;
use crate::test_support::{
    FIRST_SEQUENCE, NEXT_EPOCH, ROOMY, SEGMENT, SEGMENT_EPOCH, copy_with_records, heartbeat, new_copy, read_request,
    readable_sequences, recovery_read_request, reload,
};

// What readers see.

#[tokio::test]
async fn readers_see_only_what_the_writer_declared_durable() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;

    let served_sequences = readable_sequences(&mut copy, &read_request(FIRST_SEQUENCE)).await;

    let durable_sequences: Vec<u64> = (100..=104).collect();
    assert_eq!(served_sequences, durable_sequences);
}

#[tokio::test]
async fn heartbeat_advances_what_readers_see() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;

    copy.append(&heartbeat(109), ROOMY)
        .await
        .expect("a sound copy accepts a heartbeat");

    assert_eq!(copy.readable_end(), Some(SequenceNumber::new(109)));
}

#[tokio::test]
async fn durable_sequence_never_moves_backwards() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;

    copy.append(&heartbeat(100), ROOMY)
        .await
        .expect("a sound copy accepts a heartbeat");

    assert_eq!(copy.readable_end(), Some(SequenceNumber::new(104)));
}

#[tokio::test]
async fn durable_sequence_beyond_the_copy_is_capped_at_its_end() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;

    copy.append(&heartbeat(500), ROOMY)
        .await
        .expect("a sound copy accepts a heartbeat");

    assert_eq!(copy.readable_end(), Some(SequenceNumber::new(109)));
}

#[tokio::test]
async fn copy_without_records_has_no_readable_end() {
    let disk = MemoryDisk::new();
    let copy = new_copy(&disk);

    assert_eq!(copy.readable_end(), None);
}

#[tokio::test]
async fn restarted_copy_serves_nothing_until_it_hears_from_the_writer() {
    let disk = MemoryDisk::new();
    copy_with_records(&disk).await;
    disk.crash();
    let mut reloaded = reload(&disk).await;

    let served_sequences = readable_sequences(&mut reloaded, &read_request(FIRST_SEQUENCE)).await;

    assert_eq!(served_sequences, Vec::<u64>::new());
}

#[tokio::test]
async fn read_reports_how_far_it_was_allowed_to_go() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;

    let response = copy
        .read(&read_request(FIRST_SEQUENCE))
        .await
        .expect("a sound copy serves a read");

    assert_eq!(response.readable_end, SequenceNumber::new(104));
}

// What a recovering writer sees.

#[tokio::test]
async fn recovering_writer_reads_past_the_durable_sequence() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    copy.fence(NEXT_EPOCH)
        .await
        .expect("a sound copy accepts a newer epoch");

    let served_sequences = readable_sequences(&mut copy, &recovery_read_request(NEXT_EPOCH)).await;

    let every_sequence: Vec<u64> = (100..=109).collect();
    assert_eq!(served_sequences, every_sequence);
}

#[tokio::test]
async fn uncommitted_tail_is_hidden_from_anyone_who_did_not_fence() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    copy.fence(NEXT_EPOCH)
        .await
        .expect("a sound copy accepts a newer epoch");

    let refused = copy.read(&recovery_read_request(SEGMENT_EPOCH)).await;

    assert_eq!(
        refused,
        Err(ReplicaError::Fenced {
            request_epoch: SEGMENT_EPOCH,
            fenced_epoch: NEXT_EPOCH,
        })
    );
}

#[tokio::test]
async fn uncommitted_tail_is_hidden_until_the_copy_is_fenced() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;

    let refused = copy.read(&recovery_read_request(NEXT_EPOCH)).await;

    assert!(matches!(refused, Err(ReplicaError::InvalidRequest { .. })));
}

// Sealed copies, and copies without records.

#[tokio::test]
async fn read_reports_the_sealed_end() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    copy.seal(SequenceNumber::new(109))
        .await
        .expect("a seal at the copy's end is accepted");

    let response = copy
        .read(&read_request(110))
        .await
        .expect("a sealed copy serves a read at its end");

    assert_eq!(response.sealed_end, Some(SequenceNumber::new(109)));
}

#[tokio::test]
async fn read_at_the_end_of_a_sealed_copy_returns_no_records() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    copy.seal(SequenceNumber::new(109))
        .await
        .expect("a seal at the copy's end is accepted");

    let response = copy
        .read(&read_request(110))
        .await
        .expect("a sealed copy serves a read at its end");

    assert!(response.batch_frames.is_empty());
}

#[tokio::test]
async fn read_of_a_segment_without_data_finds_nothing() {
    let disk = MemoryDisk::new();
    let mut copy = new_copy(&disk);
    copy.fence(NEXT_EPOCH)
        .await
        .expect("a replica without a copy accepts a fence");

    let not_found = copy.read(&read_request(100)).await;

    assert_eq!(not_found, Err(ReplicaError::SegmentNotFound { segment: SEGMENT }));
}
