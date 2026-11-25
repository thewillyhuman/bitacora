//! Tests of reading tiered segments, end to end against real replicas and an in-memory object store.
//!
//! # What is covered
//!
//! - A segment that only the object store holds is read like any other, by range reads and by followers,
//!   alone and together with the segments around it.
//! - While replicas still hold a tiered segment they are asked first; the object store is the fallback.
//! - A missing, unreachable or damaged object fails the read as unavailable. It never yields wrong records.
//!
//! # How the fixtures work
//!
//! `cluster_with_a_sealed_segment` leaves records 1 to 6 in a sealed first segment and records 7 to 9 in
//! the open second one. `tier_first_segment` then does the controller's part of tiering by hand: it has a
//! replica upload the segment and records the object in the metadata store, with or without dropping the
//! local copies.

use std::sync::Arc;
use std::time::Duration;

use bitacora_meta::TieredRecord;
use bitacora_types::replica_api::{ReplicaApi, UploadRequest};
use bitacora_types::{SegmentId, SequenceNumber, Timestamp};

use super::harness::{Cluster, STREAM, settle};
use super::reader::{append_numbered, open_writer, range, sequences, take};
use crate::{ReadError, StreamReader, StreamWriter};

/// Long enough for heartbeats to tell every replica what is durable.
const HEARTBEAT_SETTLE: Duration = Duration::from_millis(200);

/// What tiering leaves on the replicas.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LocalCopies {
    /// The replicas still hold the segment: the hot-read window is open.
    Kept,
    /// The replicas dropped the segment: only the object store holds it.
    Dropped,
}

/// A cluster whose stream has records 1 to 6 in a sealed first segment and 7 to 9 in the open second one.
/// The writer is returned so that the second segment stays open.
async fn cluster_with_a_sealed_segment() -> (Cluster, StreamWriter) {
    let cluster = Cluster::start(3).await;
    let first_writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&first_writer, 6).await;
    first_writer.close().await.expect("closes");
    cluster.place_standby(&[0, 1, 2]).await;
    let second_writer = open_writer(&cluster, "compute-2").await;
    append_numbered(&second_writer, 3).await;
    settle(HEARTBEAT_SETTLE).await;
    (cluster, second_writer)
}

/// The key the first segment is uploaded under.
async fn first_segment_key(cluster: &Cluster) -> String {
    format!("streams/{}/segments/1", cluster.stream().await.stream_id)
}

/// Does the controller's part of tiering the first segment: one replica uploads it, and the metadata
/// store learns where it went and whether the local copies are gone.
async fn tier_first_segment(cluster: &Cluster, local_copies: LocalCopies) {
    let stream = cluster.stream().await;
    let first_segment = stream.segments[0].clone();
    let object_key = first_segment_key(cluster).await;
    let uploaded = cluster.nodes()[0]
        .link()
        .upload_segment(UploadRequest {
            segment: first_segment.descriptor(stream.stream_id),
            sealed_end: first_segment.sealed_end().expect("the segment is sealed"),
            object_key: object_key.clone(),
        })
        .await
        .expect("the replica uploads the segment");
    cluster
        .metadata()
        .update_stream(STREAM, |record| {
            let listed_segment = record
                .segments
                .iter_mut()
                .find(|segment| segment.segment_id == SegmentId::FIRST)
                .expect("the first segment is listed");
            listed_segment.tiered = Some(TieredRecord {
                object_key: object_key.clone(),
                size_bytes: uploaded.size_bytes,
                uploaded_at: Timestamp::from_unix_millis(0),
                are_local_copies_dropped: local_copies == LocalCopies::Dropped,
            });
            if local_copies == LocalCopies::Dropped {
                listed_segment.copies.clear();
            }
            Ok(())
        })
        .await
        .expect("the metadata store records the object");
}

/// A reader that was given the cluster's object store.
fn reader_with_object_store(cluster: &Cluster) -> StreamReader {
    cluster
        .client("reader")
        .with_artifact_store(Arc::new(cluster.artifact_store().clone()))
        .open_reader(STREAM)
}

#[tokio::test(start_paused = true)]
async fn segment_that_only_the_object_store_holds_is_read() {
    let (cluster, _writer) = cluster_with_a_sealed_segment().await;
    tier_first_segment(&cluster, LocalCopies::Dropped).await;

    let records = reader_with_object_store(&cluster)
        .read_range(range(1, 6))
        .await
        .expect("reads");

    assert_eq!(sequences(&records), [1, 2, 3, 4, 5, 6]);
}

#[tokio::test(start_paused = true)]
async fn records_read_from_the_object_store_carry_their_payloads() {
    let (cluster, _writer) = cluster_with_a_sealed_segment().await;
    tier_first_segment(&cluster, LocalCopies::Dropped).await;

    let records = reader_with_object_store(&cluster)
        .read_range(range(4, 4))
        .await
        .expect("reads");

    assert_eq!(records[0].payload.as_ref(), b"record-4");
}

#[tokio::test(start_paused = true)]
async fn range_read_can_start_in_the_middle_of_a_tiered_segment() {
    let (cluster, _writer) = cluster_with_a_sealed_segment().await;
    tier_first_segment(&cluster, LocalCopies::Dropped).await;

    let records = reader_with_object_store(&cluster)
        .read_range(range(4, 6))
        .await
        .expect("reads");

    assert_eq!(sequences(&records), [4, 5, 6]);
}

#[tokio::test(start_paused = true)]
async fn range_read_crosses_from_a_tiered_segment_into_the_open_one() {
    let (cluster, _writer) = cluster_with_a_sealed_segment().await;
    tier_first_segment(&cluster, LocalCopies::Dropped).await;

    let records = reader_with_object_store(&cluster)
        .read_range(range(1, 9))
        .await
        .expect("reads");

    assert_eq!(sequences(&records), [1, 2, 3, 4, 5, 6, 7, 8, 9]);
}

#[tokio::test(start_paused = true)]
async fn follower_continues_from_a_tiered_segment_into_the_open_one() {
    let (cluster, writer) = cluster_with_a_sealed_segment().await;
    tier_first_segment(&cluster, LocalCopies::Dropped).await;
    let mut followed = reader_with_object_store(&cluster)
        .follow_from(SequenceNumber::FIRST)
        .await
        .expect("follows");

    let caught_up = take(&mut followed, 9).await;
    append_numbered(&writer, 1).await;
    let live = take(&mut followed, 1).await;

    assert_eq!(caught_up, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
    assert_eq!(live, [10]);
}

#[tokio::test(start_paused = true)]
async fn tiered_segment_is_read_from_the_object_store_when_no_replica_answers() {
    let (cluster, _writer) = cluster_with_a_sealed_segment().await;
    tier_first_segment(&cluster, LocalCopies::Kept).await;
    for node in cluster.nodes() {
        node.set_reachable(false);
    }

    let records = reader_with_object_store(&cluster)
        .read_range(range(1, 6))
        .await
        .expect("reads");

    assert_eq!(sequences(&records), [1, 2, 3, 4, 5, 6]);
}

#[tokio::test(start_paused = true)]
async fn replicas_that_still_hold_a_tiered_segment_serve_it_without_the_object_store() {
    let (cluster, _writer) = cluster_with_a_sealed_segment().await;
    tier_first_segment(&cluster, LocalCopies::Kept).await;
    cluster.artifact_store().set_unavailable(true);

    let records = reader_with_object_store(&cluster)
        .read_range(range(1, 6))
        .await
        .expect("reads");

    assert_eq!(sequences(&records), [1, 2, 3, 4, 5, 6]);
}

#[tokio::test(start_paused = true)]
async fn reader_without_an_object_store_cannot_read_a_segment_the_replicas_dropped() {
    let (cluster, _writer) = cluster_with_a_sealed_segment().await;
    tier_first_segment(&cluster, LocalCopies::Dropped).await;
    let reader_without_object_store = cluster.client("reader").open_reader(STREAM);

    let read = reader_without_object_store.read_range(range(1, 6)).await;

    assert!(
        matches!(read, Err(ReadError::Unavailable { .. })),
        "{read:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn range_read_fails_while_the_object_store_is_down() {
    let (cluster, _writer) = cluster_with_a_sealed_segment().await;
    tier_first_segment(&cluster, LocalCopies::Dropped).await;
    cluster.artifact_store().set_unavailable(true);

    let read = reader_with_object_store(&cluster)
        .read_range(range(1, 6))
        .await;

    assert!(
        matches!(read, Err(ReadError::Unavailable { .. })),
        "{read:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn follower_resumes_when_the_object_store_returns() {
    let (cluster, _writer) = cluster_with_a_sealed_segment().await;
    tier_first_segment(&cluster, LocalCopies::Dropped).await;
    cluster.artifact_store().set_unavailable(true);
    let mut followed = reader_with_object_store(&cluster)
        .follow_from(SequenceNumber::FIRST)
        .await
        .expect("follows");
    let following = tokio::spawn(async move { take(&mut followed, 9).await });
    settle(Duration::from_secs(30)).await;

    cluster.artifact_store().set_unavailable(false);
    let taken = following.await.expect("the follower does not panic");

    assert_eq!(taken, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
}

#[tokio::test(start_paused = true)]
async fn damaged_object_fails_the_read_instead_of_yielding_wrong_records() {
    let (cluster, _writer) = cluster_with_a_sealed_segment().await;
    tier_first_segment(&cluster, LocalCopies::Dropped).await;
    let offset_inside_the_first_batch = 30;
    cluster.artifact_store().corrupt_byte(
        &first_segment_key(&cluster).await,
        offset_inside_the_first_batch,
        0x01,
    );

    let read = reader_with_object_store(&cluster)
        .read_range(range(1, 6))
        .await;

    assert!(
        matches!(read, Err(ReadError::Unavailable { .. })),
        "{read:?}"
    );
}
