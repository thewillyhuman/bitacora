//! Tests of the faults of a node's disk.
//!
//! # What is covered
//!
//! - A refused flush hits the planned node: its next append fails, and the fault is recorded.
//! - Bit rot flips exactly one bit of one stored segment file of the unlucky node and is recorded with the file and the
//!   offset; a node without segment files is left alone.
//! - A lost disk takes the unlucky node down under a new identity, brings it back after the planned time, and is
//!   recorded.
//!
//! # How the fixtures work
//!
//! Each test starts a small world of the test support and calls a fault function as the nemesis loop would.
//! `store_one_record_on` gives a node a segment file to damage. The content of a disk is read through
//! `MemoryDisk::durable_content`, which is what survives a crash.

use bitacora_types::replica_api::ReplicaError;

use super::*;
use crate::config::FaultKind;
use crate::test_support::{fault_descriptions, start_world, store_one_record_on, try_to_store_one_record_on};

/// How long the faults of these tests last.
const LASTS_FOR: Duration = Duration::from_secs(3);

/// A moment just after a fault of these tests is undone.
const JUST_AFTER_THE_END: Duration = Duration::from_millis(3_001);

/// The seed of the random source handed to the fault.
const SEED: u64 = 5;

/// A plan for a refused flush on node `node_number` that lasts [`LASTS_FOR`].
fn refused_flush_on(node_number: usize) -> FaultPlan {
    FaultPlan {
        kind: FaultKind::FlushFailure,
        node_number,
        lasts_for: LASTS_FOR,
    }
}

/// One segment file as a disk holds it.
#[derive(Debug, PartialEq, Eq)]
struct StoredFile {
    /// The path of the file on the disk.
    path: String,
    /// The flushed content of the file.
    content: Vec<u8>,
}

/// Lists the segment files on the unlucky node's disk, with their flushed content.
async fn segment_files_of_the_unlucky_node(world: &World) -> Vec<StoredFile> {
    let disk = world.cluster.nodes()[world.unlucky_node].disk();
    let mut segment_files = Vec::new();
    for path in disk.list_files().await.expect("a memory disk lists its files") {
        if path.ends_with(SEGMENT_FILE_SUFFIX) {
            let content = disk.durable_content(&path).expect("a listed file has content");
            segment_files.push(StoredFile { path, content });
        }
    }
    segment_files
}

/// Counts the bits in which two byte strings of the same length differ.
fn differing_bits(before: &[u8], after: &[u8]) -> u32 {
    let mut count = 0;
    for (byte_before, byte_after) in before.iter().zip(after) {
        count += (byte_before ^ byte_after).count_ones();
    }
    count
}

#[tokio::test(start_paused = true)]
async fn refused_flush_makes_the_planned_node_fail_its_next_append() {
    let world = start_world(1).await;

    fail_next_flush(&world, refused_flush_on(2));
    let appended = try_to_store_one_record_on(&world, 2).await;

    assert!(matches!(appended, Err(ReplicaError::Storage { .. })), "{appended:?}");
}

/// The supervisor of `background.rs` looks at this to know which replica to restart.
#[tokio::test(start_paused = true)]
async fn node_whose_flush_was_refused_reports_a_failed_write() {
    let world = start_world(1).await;

    fail_next_flush(&world, refused_flush_on(2));
    let _ = try_to_store_one_record_on(&world, 2).await;

    assert!(world.cluster.nodes()[2].has_failed_write());
}

#[tokio::test(start_paused = true)]
async fn refused_flush_leaves_the_other_nodes_alone() {
    let world = start_world(1).await;

    fail_next_flush(&world, refused_flush_on(2));
    let appended_elsewhere = try_to_store_one_record_on(&world, 0).await;

    assert_eq!(appended_elsewhere, Ok(()));
}

#[tokio::test(start_paused = true)]
async fn refused_flush_is_recorded_with_its_node_and_duration() {
    let world = start_world(1).await;

    fail_next_flush(&world, refused_flush_on(2));

    assert_eq!(
        fault_descriptions(&world.history),
        ["node 2 disk fails its next flush for 3s"]
    );
}

#[tokio::test(start_paused = true)]
async fn bit_rot_flips_one_bit_of_a_stored_segment_file() {
    let world = start_world(1).await;
    store_one_record_on(&world, world.unlucky_node).await;
    let before = segment_files_of_the_unlucky_node(&world).await;
    let mut random = DeterministicRandom::from_seed(SEED);

    corrupt_a_stored_byte(&world, &mut random).await;

    let after = segment_files_of_the_unlucky_node(&world).await;
    assert_eq!(differing_bits(&before[0].content, &after[0].content), 1);
}

#[tokio::test(start_paused = true)]
async fn bit_rot_is_recorded_with_the_node_the_offset_and_the_file() {
    let world = start_world(1).await;
    store_one_record_on(&world, world.unlucky_node).await;
    let segment_files = segment_files_of_the_unlucky_node(&world).await;
    let mut random = DeterministicRandom::from_seed(SEED);

    corrupt_a_stored_byte(&world, &mut random).await;

    // One file to choose from, so the only draw that shows is the offset.
    let mut expected = DeterministicRandom::from_seed(SEED);
    let _ = expected.choose(&segment_files);
    let offset = expected.index(segment_files[0].content.len());
    assert_eq!(
        fault_descriptions(&world.history),
        [format!(
            "node {} silently corrupts byte {offset} of {}",
            world.unlucky_node, segment_files[0].path
        )]
    );
}

#[tokio::test(start_paused = true)]
async fn bit_rot_leaves_a_node_without_segment_files_alone() {
    let world = start_world(1).await;
    let mut random = DeterministicRandom::from_seed(SEED);

    corrupt_a_stored_byte(&world, &mut random).await;

    assert_eq!(fault_descriptions(&world.history), Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn bit_rot_stays_on_the_unlucky_node() {
    let world = start_world(1).await;
    let another_node = (world.unlucky_node + 1) % world.cluster.nodes().len();
    store_one_record_on(&world, another_node).await;
    let mut random = DeterministicRandom::from_seed(SEED);

    corrupt_a_stored_byte(&world, &mut random).await;

    assert_eq!(fault_descriptions(&world.history), Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn lost_disk_takes_the_unlucky_node_down() {
    let world = start_world(1).await;
    let mut repairs = JoinSet::new();

    lose_disk(&world, LASTS_FOR, &mut repairs);

    assert!(!world.cluster.nodes()[world.unlucky_node].is_running());
}

#[tokio::test(start_paused = true)]
async fn lost_disk_gives_the_unlucky_node_a_new_identity() {
    let world = start_world(1).await;
    let identity_before = world.cluster.nodes()[world.unlucky_node].replica_id();
    let mut repairs = JoinSet::new();

    lose_disk(&world, LASTS_FOR, &mut repairs);

    assert_ne!(world.cluster.nodes()[world.unlucky_node].replica_id(), identity_before);
}

#[tokio::test(start_paused = true)]
async fn node_that_lost_its_disk_returns_after_the_planned_time() {
    let world = start_world(1).await;
    let mut repairs = JoinSet::new();
    lose_disk(&world, LASTS_FOR, &mut repairs);

    sleep(JUST_AFTER_THE_END).await;

    assert!(world.cluster.nodes()[world.unlucky_node].is_running());
}

#[tokio::test(start_paused = true)]
async fn node_that_lost_its_disk_returns_empty() {
    let world = start_world(1).await;
    store_one_record_on(&world, world.unlucky_node).await;
    let mut repairs = JoinSet::new();
    lose_disk(&world, LASTS_FOR, &mut repairs);

    sleep(JUST_AFTER_THE_END).await;

    assert_eq!(segment_files_of_the_unlucky_node(&world).await, Vec::new());
}

#[tokio::test(start_paused = true)]
async fn lost_disk_is_recorded_with_the_unlucky_node() {
    let world = start_world(1).await;
    let mut repairs = JoinSet::new();

    lose_disk(&world, LASTS_FOR, &mut repairs);

    assert_eq!(
        fault_descriptions(&world.history),
        [format!("node {} loses its disk for good", world.unlucky_node)]
    );
}
