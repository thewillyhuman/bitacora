//! Tests of fencing and sealing a copy.
//!
//! # What is covered
//!
//! - A fence reports the copy's end, refuses the old writer and its heartbeats from then on, accepts the
//!   new writer, survives a crash, can be repeated, and never goes back to a lower epoch.
//! - A replica that never saw the segment gives the promise all the same, and keeps it after a crash.
//! - A seal discards the records above the sealed end, makes the rest readable, refuses later appends,
//!   survives a crash, can be repeated, and conflicts with a seal at another end.
//! - A seal inside a batch is refused and changes nothing; a seal beyond a short copy reports how short
//!   the copy is; a seal gives the preallocated space back.
//!
//! # How the fixtures work
//!
//! The fixtures come from `crate::test_support`. The copy under test holds records 100 to 109 in two
//! batches of five, written in epoch 5, of which 100 to 104 are declared durable; `NEXT_EPOCH` is the
//! epoch of the writer that takes over. A crash is the disk forgetting what was not flushed, followed by
//! loading the copy again.

use bitacora_segment::MemoryDisk;
use bitacora_types::replica_api::AppendRequest;

use super::*;
use crate::test_support::{
    FIRST_SEQUENCE, NEXT_EPOCH, ROOMY, SEGMENT_EPOCH, SEGMENT_FILE, append_request, batch, copy_with_records,
    heartbeat, new_copy, read_request, readable_sequences, reload,
};

// Fencing.

#[tokio::test]
async fn fence_reports_the_end_of_the_copy() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;

    let fenced = copy.fence(NEXT_EPOCH).await;

    assert_eq!(
        fenced,
        Ok(FenceResponse {
            local_end: Some(SequenceNumber::new(109)),
            sealed_end: None,
        })
    );
}

#[tokio::test]
async fn fenced_copy_refuses_the_old_writer() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    copy.fence(NEXT_EPOCH)
        .await
        .expect("a sound copy accepts a newer epoch");

    let refused = copy.append(&append_request(&[batch(110, 1)], 0), ROOMY).await;

    assert_eq!(
        refused,
        Err(ReplicaError::Fenced {
            request_epoch: SEGMENT_EPOCH,
            fenced_epoch: NEXT_EPOCH,
        })
    );
}

#[tokio::test]
async fn fenced_copy_refuses_the_old_writers_heartbeat() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    copy.fence(NEXT_EPOCH)
        .await
        .expect("a sound copy accepts a newer epoch");

    let refused = copy.append(&heartbeat(109), ROOMY).await;

    assert!(matches!(refused, Err(ReplicaError::Fenced { .. })));
}

#[tokio::test]
async fn fence_survives_a_crash() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    copy.fence(NEXT_EPOCH)
        .await
        .expect("a sound copy accepts a newer epoch");
    disk.crash();

    let refused = reload(&disk)
        .await
        .append(&append_request(&[batch(110, 1)], 0), ROOMY)
        .await;

    assert!(matches!(refused, Err(ReplicaError::Fenced { .. })));
}

#[tokio::test]
async fn fenced_copy_accepts_the_recovering_writer() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    copy.fence(NEXT_EPOCH)
        .await
        .expect("a sound copy accepts a newer epoch");
    let request_of_the_new_writer = AppendRequest {
        writer_epoch: NEXT_EPOCH,
        ..append_request(&[batch(110, 1)], 0)
    };

    let accepted = copy.append(&request_of_the_new_writer, ROOMY).await;

    assert!(accepted.is_ok(), "{accepted:?}");
}

#[tokio::test]
async fn fence_with_an_older_epoch_is_refused() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    copy.fence(Epoch::new(8))
        .await
        .expect("a sound copy accepts a newer epoch");

    let refused = copy.fence(Epoch::new(7)).await;

    assert_eq!(
        refused,
        Err(ReplicaError::Fenced {
            request_epoch: Epoch::new(7),
            fenced_epoch: Epoch::new(8),
        })
    );
}

#[tokio::test]
async fn fence_can_be_repeated() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    let first_fence = copy.fence(NEXT_EPOCH).await;

    let second_fence = copy.fence(NEXT_EPOCH).await;

    assert_eq!(second_fence, first_fence);
}

#[tokio::test]
async fn fence_of_a_segment_the_replica_never_saw_reports_no_copy() {
    let disk = MemoryDisk::new();
    let mut copy = new_copy(&disk);

    let fenced = copy.fence(NEXT_EPOCH).await;

    assert_eq!(
        fenced,
        Ok(FenceResponse {
            local_end: None,
            sealed_end: None,
        })
    );
}

/// A replica that was cut off while the segment was written must still refuse the old writer once it
/// has promised the new one.
#[tokio::test]
async fn fence_of_an_unseen_segment_still_blocks_the_old_writer_after_a_crash() {
    let disk = MemoryDisk::new();
    new_copy(&disk)
        .fence(NEXT_EPOCH)
        .await
        .expect("a replica without a copy accepts a fence");
    disk.crash();

    let refused = reload(&disk)
        .await
        .append(&append_request(&[batch(100, 5)], 0), ROOMY)
        .await;

    assert!(matches!(refused, Err(ReplicaError::Fenced { .. })));
}

// Sealing.

#[tokio::test]
async fn seal_discards_records_above_the_sealed_end() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;

    copy.seal(SequenceNumber::new(104))
        .await
        .expect("a seal at a batch boundary is accepted");

    let expected_sequences: Vec<u64> = (100..=104).collect();
    assert_eq!(
        readable_sequences(&mut copy, &read_request(FIRST_SEQUENCE)).await,
        expected_sequences
    );
}

#[tokio::test]
async fn seal_makes_the_whole_segment_readable() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;

    copy.seal(SequenceNumber::new(109))
        .await
        .expect("a seal at the copy's end is accepted");

    let expected_sequences: Vec<u64> = (100..=109).collect();
    assert_eq!(
        readable_sequences(&mut copy, &read_request(FIRST_SEQUENCE)).await,
        expected_sequences
    );
}

#[tokio::test]
async fn sealed_copy_refuses_appends() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    copy.seal(SequenceNumber::new(109))
        .await
        .expect("a seal at the copy's end is accepted");

    let refused = copy.append(&append_request(&[batch(110, 1)], 0), ROOMY).await;

    assert_eq!(
        refused,
        Err(ReplicaError::Sealed {
            sealed_end: SequenceNumber::new(109)
        })
    );
}

#[tokio::test]
async fn seal_survives_a_crash() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    copy.seal(SequenceNumber::new(104))
        .await
        .expect("a seal at a batch boundary is accepted");

    disk.crash();

    assert_eq!(reload(&disk).await.status().sealed_end, Some(SequenceNumber::new(104)));
}

#[tokio::test]
async fn records_discarded_by_a_seal_stay_discarded_after_a_crash() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    copy.seal(SequenceNumber::new(104))
        .await
        .expect("a seal at a batch boundary is accepted");

    disk.crash();

    assert_eq!(reload(&disk).await.status().local_end, Some(SequenceNumber::new(104)));
}

#[tokio::test]
async fn seal_can_be_repeated() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    copy.seal(SequenceNumber::new(104))
        .await
        .expect("a seal at a batch boundary is accepted");

    let second_seal = copy.seal(SequenceNumber::new(104)).await;

    assert_eq!(
        second_seal,
        Ok(SealResponse {
            local_end: Some(SequenceNumber::new(104))
        })
    );
}

#[tokio::test]
async fn seal_at_a_different_end_is_a_conflict() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;
    copy.seal(SequenceNumber::new(104))
        .await
        .expect("a seal at a batch boundary is accepted");

    let refused = copy.seal(SequenceNumber::new(109)).await;

    assert_eq!(
        refused,
        Err(ReplicaError::ConflictingSeal {
            existing_sealed_end: SequenceNumber::new(104),
            requested_sealed_end: SequenceNumber::new(109),
        })
    );
}

#[tokio::test]
async fn seal_inside_a_batch_is_refused() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;

    let refused = copy.seal(SequenceNumber::new(106)).await;

    assert!(matches!(refused, Err(ReplicaError::InvalidRequest { .. })));
}

#[tokio::test]
async fn seal_inside_a_batch_leaves_the_copy_open() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;

    let _refused = copy.seal(SequenceNumber::new(106)).await;

    assert_eq!(copy.status().sealed_end, None);
}

#[tokio::test]
async fn seal_inside_a_batch_discards_no_record() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;

    let _refused = copy.seal(SequenceNumber::new(106)).await;

    assert_eq!(copy.status().local_end, Some(SequenceNumber::new(109)));
}

#[tokio::test]
async fn seal_beyond_a_short_copy_reports_how_short_it_is() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;

    let sealed = copy.seal(SequenceNumber::new(120)).await;

    assert_eq!(
        sealed,
        Ok(SealResponse {
            local_end: Some(SequenceNumber::new(109))
        })
    );
}

#[tokio::test]
async fn seal_of_a_segment_the_replica_never_saw_blocks_later_appends() {
    let disk = MemoryDisk::new();
    let mut copy = new_copy(&disk);
    copy.seal(SequenceNumber::new(104))
        .await
        .expect("a replica without a copy accepts a seal");

    let refused = copy.append(&append_request(&[batch(100, 5)], 0), ROOMY).await;

    assert!(matches!(refused, Err(ReplicaError::Sealed { .. })));
}

#[tokio::test]
async fn seal_releases_the_preallocated_space() {
    let disk = MemoryDisk::new();
    let mut copy = copy_with_records(&disk).await;

    copy.seal(SequenceNumber::new(109))
        .await
        .expect("a seal at the copy's end is accepted");

    let stored_file = disk.durable_content(SEGMENT_FILE).expect("the records file exists");
    let stored_bytes = u64::try_from(stored_file.len()).expect("a test file is small");
    assert_eq!(stored_bytes, copy.status().size_bytes);
}
