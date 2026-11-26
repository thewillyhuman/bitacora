//! Tests of repairing a copy from another replica.
//!
//! # What is covered
//!
//! - A replica without a copy, and one with a short copy, end up with the whole sealed segment, sealed,
//!   and keep it across a restart.
//! - A copy that cannot be completed (an uncommitted tail past the sealed end, damage) is replaced by a
//!   new one, which keeps the fence of the old one and leaves no scratch file behind.
//! - A repair that fails (the source cannot be reached, or holds less than the sealed end) leaves a
//!   damaged copy exactly as it was: it may be the only copy of the records it did not lose.
//! - A copy whose disk refused a write is not repaired at all.
//! - A segment sealed empty is repaired by recording the seal.
//! - A repair pauses in proportion to what it copied, so that it stays within its rate limit.
//!
//! # How the fixtures work
//!
//! A repair needs a source, so these tests drive whole replicas through their public interface, with the
//! fixtures of `crate::test_support`. The source holds records 100 to 109 of the segment in two batches
//! of five, sealed where the test says. The target reads from it in chunks so small that every repair
//! takes several reads. `copy_of` shows what the target holds afterwards.

use std::sync::Arc;

use bitacora_segment::{Disk, MemoryDisk};
use bitacora_types::Epoch;
use bitacora_types::replica_api::ReplicaConnector;
use tokio::time::Instant;

use super::*;
use crate::test_support::{
    BYTE_IN_THE_FIRST_BATCH, CopySummary, FIRST_SEQUENCE, REPAIR_SCRATCH_FILE, SEGMENT, SEGMENT_EPOCH, SEGMENT_FILE,
    append_request, batch, copy_of, fence_request, heartbeat, no_peers, open_replica_with, open_target, read_request,
    repair_request, seal_request, sequences_in, small_segments, source_sealed_at,
};
use crate::{Replica, ReplicaSettings};

/// What a target reports once it holds the whole segment sealed at 109.
const WHOLE_SEGMENT_SEALED_AT_109: CopySummary = CopySummary {
    local_end: Some(109),
    sealed_end: Some(109),
    is_damaged: false,
};

/// The epoch a target is fenced at in the tests that check a repair keeps the fence: well above the
/// segment's own.
const LATER_EPOCH: Epoch = Epoch::new(9);

/// A pace with the given rate limit, in bytes per second. The other two values play no part in pausing.
fn pace_limited_to(bytes_per_second: u64) -> RepairPace {
    RepairPace {
        chunk_bytes: 1024,
        bytes_per_second,
        request_timeout: Duration::from_secs(1),
    }
}

/// A target that holds records 100 to 104 of the segment sealed at 109, with a bit flipped in its first
/// batch that the scrubber has found.
async fn damaged_target(disk: &MemoryDisk, connector: Arc<dyn ReplicaConnector>) -> Replica {
    let target = open_target(disk, connector).await;
    target
        .append(append_request(&[batch(100, 5)], 0))
        .await
        .expect("an empty target accepts the first batch");
    target
        .seal(seal_request(109))
        .await
        .expect("a short copy accepts a seal beyond its end");
    disk.corrupt_byte(SEGMENT_FILE, BYTE_IN_THE_FIRST_BATCH, 0x20);
    target.scrub_sealed_segments().await.expect("a running replica scrubs");
    target
}

/// A target that holds records 100 to 104, is fenced at [`LATER_EPOCH`], and has found a flipped bit in
/// its first batch while serving a read.
async fn fenced_target_damaged_while_reading(disk: &MemoryDisk, connector: Arc<dyn ReplicaConnector>) -> Replica {
    let target = open_target(disk, connector).await;
    target
        .append(append_request(&[batch(100, 5)], 0))
        .await
        .expect("an empty target accepts the first batch");
    // A heartbeat that makes the records readable.
    target
        .append(heartbeat(104))
        .await
        .expect("a sound copy accepts a heartbeat");
    target
        .fence(fence_request(SEGMENT, LATER_EPOCH))
        .await
        .expect("a sound copy accepts a newer epoch");
    disk.corrupt_byte(SEGMENT_FILE, BYTE_IN_THE_FIRST_BATCH, 0x20);
    // Reading the damaged batch is what makes the replica notice.
    let _read_of_the_damage = target.read(read_request(FIRST_SEQUENCE)).await;
    target
}

/// A target that holds records 100 to 104 and whose disk refused to flush the next batch. The disk
/// keeps refusing.
async fn target_refused_a_flush(disk: &MemoryDisk, connector: Arc<dyn ReplicaConnector>) -> Replica {
    let target = open_target(disk, connector).await;
    target
        .append(append_request(&[batch(100, 5)], 0))
        .await
        .expect("an empty target accepts the first batch");
    disk.fail_flushes(true);
    let _refused_append = target.append(append_request(&[batch(105, 5)], 0)).await;
    target
}

// Pacing.

#[test]
fn unlimited_repair_never_pauses() {
    let unlimited = pace_limited_to(0);

    assert_eq!(unlimited.pause_after(1 << 30), Duration::ZERO);
}

#[test]
fn limited_repair_pauses_in_proportion_to_what_it_copied() {
    let one_megabyte_per_second = pace_limited_to(1_000_000);

    assert_eq!(one_megabyte_per_second.pause_after(250_000), Duration::from_millis(250));
}

#[tokio::test(start_paused = true)]
async fn repair_is_slowed_to_its_rate_limit() {
    let connector = source_sealed_at(109).await;
    let slow_repairs = ReplicaSettings {
        repair_bytes_per_second: 100,
        ..small_segments()
    };
    let target = open_replica_with(&MemoryDisk::new(), "target", slow_repairs, connector).await;
    let started_at = Instant::now();

    target
        .repair_segment(repair_request(109))
        .await
        .expect("a reachable source repairs the target");

    let repair_duration = started_at.elapsed();
    assert!(
        repair_duration >= Duration::from_secs(1),
        "copied in {repair_duration:?}"
    );
}

// Completing a copy in place.

#[tokio::test]
async fn repair_copies_a_sealed_segment_to_a_replica_without_one() {
    let connector = source_sealed_at(109).await;
    let target = open_target(&MemoryDisk::new(), connector).await;

    target
        .repair_segment(repair_request(109))
        .await
        .expect("a reachable source repairs the target");

    let readable = target
        .read(read_request(FIRST_SEQUENCE))
        .await
        .expect("a repaired copy serves reads");
    let every_sequence: Vec<u64> = (100..=109).collect();
    assert_eq!(sequences_in(&readable.batch_frames), every_sequence);
}

#[tokio::test]
async fn repair_reports_the_end_of_the_repaired_copy() {
    let connector = source_sealed_at(109).await;
    let target = open_target(&MemoryDisk::new(), connector).await;

    let repaired = target.repair_segment(repair_request(109)).await;

    assert_eq!(
        repaired,
        Ok(RepairResponse {
            local_end: SequenceNumber::new(109)
        })
    );
}

#[tokio::test]
async fn repaired_copy_is_sealed_and_survives_a_restart() {
    let connector = source_sealed_at(109).await;
    let disk = MemoryDisk::new();
    let target = open_target(&disk, Arc::clone(&connector)).await;
    target
        .repair_segment(repair_request(109))
        .await
        .expect("a reachable source repairs the target");
    drop(target);
    disk.crash();

    let restarted = open_target(&disk, connector).await;

    assert_eq!(copy_of(&restarted).await, WHOLE_SEGMENT_SEALED_AT_109);
}

#[tokio::test]
async fn repair_completes_a_short_copy_in_place() {
    let connector = source_sealed_at(109).await;
    let target = open_target(&MemoryDisk::new(), connector).await;
    target
        .append(append_request(&[batch(100, 5)], 0))
        .await
        .expect("an empty target accepts the first batch");

    target
        .repair_segment(repair_request(109))
        .await
        .expect("a reachable source repairs the target");

    assert_eq!(copy_of(&target).await, WHOLE_SEGMENT_SEALED_AT_109);
}

#[tokio::test]
async fn repair_of_a_segment_sealed_empty_only_records_the_seal() {
    let connector = source_sealed_at(99).await;
    let target = open_target(&MemoryDisk::new(), connector).await;

    target
        .repair_segment(repair_request(99))
        .await
        .expect("a segment sealed empty needs nothing from the source");

    let sealed_without_records = CopySummary {
        local_end: None,
        sealed_end: Some(99),
        is_damaged: false,
    };
    assert_eq!(copy_of(&target).await, sealed_without_records);
}

#[tokio::test]
async fn repair_of_a_segment_sealed_empty_reports_the_position_before_its_first_record() {
    let connector = source_sealed_at(99).await;
    let target = open_target(&MemoryDisk::new(), connector).await;

    let repaired = target.repair_segment(repair_request(99)).await;

    assert_eq!(
        repaired,
        Ok(RepairResponse {
            local_end: SequenceNumber::new(99)
        })
    );
}

// Replacing a copy that cannot be completed.

#[tokio::test]
async fn repair_replaces_a_copy_with_an_uncommitted_tail() {
    let connector = source_sealed_at(104).await;
    let target = open_target(&MemoryDisk::new(), connector).await;
    for first_sequence in [100, 105] {
        target
            .append(append_request(&[batch(first_sequence, 5)], 0))
            .await
            .expect("the target accepts consecutive batches");
    }

    target
        .repair_segment(repair_request(104))
        .await
        .expect("a reachable source repairs the target");

    let whole_segment_sealed_at_104 = CopySummary {
        local_end: Some(104),
        sealed_end: Some(104),
        is_damaged: false,
    };
    assert_eq!(copy_of(&target).await, whole_segment_sealed_at_104);
}

#[tokio::test]
async fn scrubbed_copy_with_a_flipped_bit_is_damaged() {
    let target = damaged_target(&MemoryDisk::new(), no_peers()).await;

    assert!(copy_of(&target).await.is_damaged);
}

#[tokio::test]
async fn repair_replaces_a_damaged_copy() {
    let connector = source_sealed_at(109).await;
    let target = damaged_target(&MemoryDisk::new(), connector).await;

    target
        .repair_segment(repair_request(109))
        .await
        .expect("a reachable source repairs the target");

    assert_eq!(copy_of(&target).await, WHOLE_SEGMENT_SEALED_AT_109);
}

#[tokio::test]
async fn repair_of_a_damaged_copy_leaves_no_scratch_file() {
    let connector = source_sealed_at(109).await;
    let disk = MemoryDisk::new();
    let target = damaged_target(&disk, connector).await;

    target
        .repair_segment(repair_request(109))
        .await
        .expect("a reachable source repairs the target");

    let stored_paths = disk.list_files().await.expect("a working disk lists its files");
    assert!(
        !stored_paths.contains(&REPAIR_SCRATCH_FILE.to_owned()),
        "{stored_paths:?}"
    );
}

#[tokio::test]
async fn copy_rebuilt_by_repair_survives_a_restart() {
    let connector = source_sealed_at(109).await;
    let disk = MemoryDisk::new();
    let target = damaged_target(&disk, Arc::clone(&connector)).await;
    target
        .repair_segment(repair_request(109))
        .await
        .expect("a reachable source repairs the target");
    target.stop();
    disk.crash();

    let restarted = open_target(&disk, connector).await;

    assert_eq!(copy_of(&restarted).await, WHOLE_SEGMENT_SEALED_AT_109);
}

#[tokio::test]
async fn read_of_a_damaged_batch_marks_the_copy_damaged() {
    let target = fenced_target_damaged_while_reading(&MemoryDisk::new(), no_peers()).await;

    assert!(copy_of(&target).await.is_damaged);
}

/// A fence must never be forgotten, whatever happens to the records.
#[tokio::test]
async fn copy_rebuilt_by_repair_keeps_its_fence() {
    let connector = source_sealed_at(109).await;
    let target = fenced_target_damaged_while_reading(&MemoryDisk::new(), connector).await;

    target
        .repair_segment(repair_request(109))
        .await
        .expect("a reachable source repairs the target");

    let statuses = target
        .list_segments()
        .await
        .expect("a running replica lists its segments");
    assert_eq!(statuses[0].fenced_epoch, LATER_EPOCH);
}

#[tokio::test]
async fn fenced_copy_rebuilt_by_repair_holds_the_whole_segment() {
    let connector = source_sealed_at(109).await;
    let target = fenced_target_damaged_while_reading(&MemoryDisk::new(), connector).await;

    target
        .repair_segment(repair_request(109))
        .await
        .expect("a reachable source repairs the target");

    assert_eq!(copy_of(&target).await, WHOLE_SEGMENT_SEALED_AT_109);
}

// Repairs that fail.

#[tokio::test]
async fn repair_fails_when_the_source_cannot_be_reached() {
    let target = open_target(&MemoryDisk::new(), no_peers()).await;

    let failed = target.repair_segment(repair_request(109)).await;

    assert!(matches!(failed, Err(ReplicaError::Unavailable { .. })), "{failed:?}");
}

#[tokio::test]
async fn repair_fails_when_the_source_holds_less_than_the_sealed_end() {
    let connector = source_sealed_at(109).await;
    let target = open_target(&MemoryDisk::new(), connector).await;

    let failed = target.repair_segment(repair_request(120)).await;

    assert!(matches!(failed, Err(ReplicaError::Unavailable { .. })), "{failed:?}");
}

#[tokio::test]
async fn repair_of_a_damaged_copy_fails_when_the_source_cannot_be_reached() {
    let target = damaged_target(&MemoryDisk::new(), no_peers()).await;

    let failed = target.repair_segment(repair_request(109)).await;

    assert!(matches!(failed, Err(ReplicaError::Unavailable { .. })), "{failed:?}");
}

/// A damaged copy may still be the only copy of every record it did not lose. A repair that cannot
/// finish must not cost them.
#[tokio::test]
async fn repair_that_cannot_reach_its_source_leaves_a_damaged_copy_as_it_was() {
    let disk = MemoryDisk::new();
    let target = damaged_target(&disk, no_peers()).await;
    let copy_before = disk.durable_content(SEGMENT_FILE);

    let _failed = target.repair_segment(repair_request(109)).await;

    assert_eq!(disk.durable_content(SEGMENT_FILE), copy_before);
}

#[tokio::test]
async fn repair_of_a_damaged_copy_fails_when_the_source_ends_early() {
    let connector = source_sealed_at(109).await;
    let target = damaged_target(&MemoryDisk::new(), connector).await;

    // The source holds records up to 109, so a repair to 119 gets the first batches and then finds
    // nothing more.
    let failed = target.repair_segment(repair_request(119)).await;

    assert!(matches!(failed, Err(ReplicaError::Unavailable { .. })), "{failed:?}");
}

#[tokio::test]
async fn repair_cut_short_by_its_source_leaves_a_damaged_copy_as_it_was() {
    let connector = source_sealed_at(109).await;
    let disk = MemoryDisk::new();
    let target = damaged_target(&disk, connector).await;
    let copy_before = disk.durable_content(SEGMENT_FILE);

    let _failed = target.repair_segment(repair_request(119)).await;

    assert_eq!(disk.durable_content(SEGMENT_FILE), copy_before);
}

#[tokio::test]
async fn failed_repair_of_a_damaged_copy_leaves_no_scratch_file() {
    let disk = MemoryDisk::new();
    let target = damaged_target(&disk, no_peers()).await;

    let _failed = target.repair_segment(repair_request(109)).await;

    let stored_paths = disk.list_files().await.expect("a working disk lists its files");
    assert!(
        !stored_paths.contains(&REPAIR_SCRATCH_FILE.to_owned()),
        "{stored_paths:?}"
    );
}

#[tokio::test]
async fn repair_cut_short_by_its_source_leaves_no_scratch_file() {
    let connector = source_sealed_at(109).await;
    let disk = MemoryDisk::new();
    let target = damaged_target(&disk, connector).await;

    let _failed = target.repair_segment(repair_request(119)).await;

    let stored_paths = disk.list_files().await.expect("a working disk lists its files");
    assert!(
        !stored_paths.contains(&REPAIR_SCRATCH_FILE.to_owned()),
        "{stored_paths:?}"
    );
}

// A copy whose disk refused a write.

/// After a refused flush the process cannot trust what it sees of the file. Repairing then would replace
/// a copy that is most likely fine on disk with whatever the source can give at that moment.
#[tokio::test]
async fn copy_whose_disk_refused_a_write_is_not_repaired() {
    let connector = source_sealed_at(109).await;
    let target = target_refused_a_flush(&MemoryDisk::new(), connector).await;

    let refused = target.repair_segment(repair_request(109)).await;

    assert!(matches!(refused, Err(ReplicaError::Storage { .. })), "{refused:?}");
}

#[tokio::test]
async fn copy_whose_disk_refused_a_write_stays_on_disk_as_it_was_flushed() {
    let connector = source_sealed_at(109).await;
    let disk = MemoryDisk::new();
    let target = open_target(&disk, connector).await;
    target
        .append(append_request(&[batch(100, 5)], 0))
        .await
        .expect("an empty target accepts the first batch");
    let copy_as_flushed = disk.durable_content(SEGMENT_FILE);
    disk.fail_flushes(true);
    let _refused_append = target.append(append_request(&[batch(105, 5)], 0)).await;

    let _refused_repair = target.repair_segment(repair_request(109)).await;

    assert_eq!(disk.durable_content(SEGMENT_FILE), copy_as_flushed);
}

// The segment's epoch.

#[tokio::test]
async fn repaired_copy_is_fenced_at_the_epoch_of_its_segment() {
    let connector = source_sealed_at(109).await;
    let target = open_target(&MemoryDisk::new(), connector).await;

    target
        .repair_segment(repair_request(109))
        .await
        .expect("a reachable source repairs the target");

    let statuses = target
        .list_segments()
        .await
        .expect("a running replica lists its segments");
    assert_eq!(statuses[0].fenced_epoch, SEGMENT_EPOCH);
}
