//! Tests of admission: when a replica takes more data.
//!
//! # What is covered
//!
//! - A disk past the first threshold takes no new segments and still takes appends to the ones it has.
//! - A disk past the second threshold refuses every append, and says how full it is.
//! - The disk is not measured again while the last measurement is recent.
//! - A draining replica refuses appends and repairs, and still serves reads and takes fences.
//!
//! # How the fixtures work
//!
//! The thresholds are the defaults: 85 and 95 percent. [`replica_on_a_disk_filled_with`] opens a replica
//! on a disk of 100 000 bytes, has it store one segment, which makes it measure the disk while it is
//! nearly empty, and then fills the disk with an unrelated file. Time is paused in these tests, so the
//! replica keeps that first measurement until a test lets it go stale.

use bitacora_segment::{Disk, MemoryDisk};
use bitacora_types::replica_api::{AppendRequest, ReplicaApi};
use bytes::Bytes;

use super::*;
use crate::test_support::{
    FIRST_SEQUENCE, NEXT_EPOCH, OTHER_SEGMENT, SEGMENT, append_request, append_request_to, batch, batch_of,
    fence_request, open_replica, open_target, read_request, repair_request, small_segments, source_sealed_at,
};

/// The capacity of the disk in the tests of the thresholds, in bytes. A round number, so that a number
/// of used bytes reads as a share of the disk.
const DISK_CAPACITY_BYTES: u64 = 100_000;

/// Opens a replica on a disk of [`DISK_CAPACITY_BYTES`], has it store records 100 to 104 of [`SEGMENT`],
/// and then takes up `ballast_bytes` of the disk with an unrelated file.
///
/// The replica measured its disk for the append, before the ballast was there.
async fn replica_on_a_disk_filled_with(ballast_bytes: u64) -> Replica {
    let disk = MemoryDisk::with_capacity(DISK_CAPACITY_BYTES);
    let replica = open_replica(&disk, "replica-a").await;
    replica
        .append(append_request(&[batch(100, 5)], 0))
        .await
        .expect("an empty replica accepts the first batch");
    disk.create_file("ballast", Bytes::new(), ballast_bytes)
        .await
        .expect("the disk has room for the ballast");
    replica
}

/// Lets as much time pass as makes a replica measure its disk again for the next write.
async fn let_the_last_measurement_go_stale() {
    tokio::time::advance(small_segments().space_refresh_interval).await;
}

/// An append of one record to [`OTHER_SEGMENT`], which no replica in these tests holds yet.
fn append_to_a_new_segment() -> AppendRequest {
    append_request_to(OTHER_SEGMENT, &[batch_of(OTHER_SEGMENT, 100, 1)], 0)
}

// The two thresholds.

#[tokio::test(start_paused = true)]
async fn disk_past_the_first_threshold_refuses_new_segments() {
    let replica = replica_on_a_disk_filled_with(88_000).await;
    let_the_last_measurement_go_stale().await;

    let refused = replica.append(append_to_a_new_segment()).await;

    assert!(matches!(refused, Err(ReplicaError::NoCapacity { .. })), "{refused:?}");
}

#[tokio::test(start_paused = true)]
async fn disk_past_the_first_threshold_still_appends_to_existing_segments() {
    let replica = replica_on_a_disk_filled_with(88_000).await;
    let_the_last_measurement_go_stale().await;

    let accepted = replica.append(append_request(&[batch(105, 1)], 0)).await;

    assert!(accepted.is_ok(), "{accepted:?}");
}

#[tokio::test(start_paused = true)]
async fn disk_past_the_second_threshold_refuses_every_append() {
    let replica = replica_on_a_disk_filled_with(96_000).await;
    let_the_last_measurement_go_stale().await;

    let refused = replica.append(append_request(&[batch(105, 1)], 0)).await;

    assert!(
        matches!(refused, Err(ReplicaError::NoCapacity { used_percent }) if used_percent >= 95),
        "{refused:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn disk_is_not_measured_again_while_the_last_measurement_is_recent() {
    let replica = replica_on_a_disk_filled_with(88_000).await;

    // The replica still goes by what it measured before the disk filled up.
    let accepted = replica.append(append_to_a_new_segment()).await;

    assert!(accepted.is_ok(), "{accepted:?}");
}

// Draining.

#[tokio::test]
async fn draining_replica_refuses_appends() {
    let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
    replica.drain().await.expect("a running replica drains");

    let refused = replica.append(append_request(&[batch(100, 5)], 0)).await;

    assert_eq!(refused, Err(ReplicaError::Draining));
}

#[tokio::test]
async fn draining_replica_serves_reads() {
    let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
    replica
        .append(append_request(&[batch(100, 5)], 104))
        .await
        .expect("an empty replica accepts the first batch");
    replica.drain().await.expect("a running replica drains");

    let served = replica.read(read_request(FIRST_SEQUENCE)).await;

    assert!(served.is_ok(), "{served:?}");
}

#[tokio::test]
async fn draining_replica_takes_a_fence() {
    let replica = open_replica(&MemoryDisk::new(), "replica-a").await;
    replica.drain().await.expect("a running replica drains");

    let fenced = replica.fence(fence_request(SEGMENT, NEXT_EPOCH)).await;

    assert!(fenced.is_ok(), "{fenced:?}");
}

#[tokio::test]
async fn draining_replica_takes_no_repairs() {
    let connector = source_sealed_at(109).await;
    let target = open_target(&MemoryDisk::new(), connector).await;
    target.drain().await.expect("a running replica drains");

    let refused = target.repair_segment(repair_request(109)).await;

    assert_eq!(refused, Err(ReplicaError::Draining));
}
