//! Tests of the read path, end to end.
//!
//! # What is covered
//!
//! - Range reads: order, payloads, the last committed record, an empty stream, segment boundaries, a replica that is
//!   down or holds a short sealed copy, no replica at all, a stream that does not exist.
//! - Truncation: what a range read and a follower report below the low-water mark, and the mark itself.
//! - Following: records committed before and after the follower started, waiting without spinning, never seeing an
//!   uncommitted record, and going on across a roll, a writer change, a replica cut off from the writer and a metadata
//!   outage.
//! - Batches and known lag: `next_batch` and `durable_sequence`.
//!
//! Segments that were uploaded to the object store are covered in `tiered_read_test.rs`.
//!
//! # How the fixtures work
//!
//! `support::Cluster` is three or more real replicas on in-memory disks with one stream, `orders`. Records are appended
//! with payloads that name their sequence numbers, and tests compare the sequence numbers a reader returned. A replica
//! learns what it may serve from the writer's next request, so a test that reads right after appending first lets a
//! heartbeat pass with `settle`. Time is virtual: a follower that should wait is given five seconds to prove it, at no
//! cost.

mod support;

use std::sync::Arc;
use std::time::Duration;

use bitacora_client::{ReadError, RecordStream, StreamReader, WriterSettings};
use bitacora_types::SequenceNumber;
use bitacora_types::replica_api::ReplicaApi;
use bytes::Bytes;
use tokio::time::{Instant, timeout};

use support::{Cluster, HEARTBEAT_SETTLE, STREAM, append_numbered, open_writer, range, sequences, settle, take};

/// A reader of [`STREAM`].
fn reader(cluster: &Cluster) -> StreamReader {
    cluster.client("reader").open_reader(STREAM)
}

/// Follows [`STREAM`] from `from_sequence`.
async fn follow_from(cluster: &Cluster, from_sequence: SequenceNumber) -> RecordStream {
    reader(cluster)
        .follow_from(from_sequence)
        .await
        .expect("the stream exists and the store is up")
}

/// Does the controller's job of moving the low-water mark of [`STREAM`] to `low_water_mark`.
async fn truncate_below(cluster: &Cluster, low_water_mark: u64) {
    cluster
        .metadata()
        .update_stream(STREAM, |stream| {
            stream.low_water_mark = SequenceNumber::new(low_water_mark);
            Ok(())
        })
        .await
        .expect("the stream exists and the store is up");
}

#[tokio::test(start_paused = true)]
async fn range_read_returns_the_committed_records_in_order() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 5).await;
    settle(HEARTBEAT_SETTLE).await;

    let records = reader(&cluster)
        .read_range(range(2, 4))
        .await
        .expect("a replica serves the range");

    assert_eq!(sequences(&records), [2, 3, 4]);
}

#[tokio::test(start_paused = true)]
async fn range_read_returns_the_payloads_that_were_appended() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 3).await;
    settle(HEARTBEAT_SETTLE).await;

    let records = reader(&cluster)
        .read_range(range(3, 3))
        .await
        .expect("a replica serves the range");

    assert_eq!(records[0].payload, Bytes::from("record-3"));
}

#[tokio::test(start_paused = true)]
async fn range_read_stops_at_the_last_committed_record() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 3).await;
    settle(HEARTBEAT_SETTLE).await;

    let records = reader(&cluster)
        .read_range(range(2, 100))
        .await
        .expect("a replica serves the range");

    assert_eq!(sequences(&records), [2, 3]);
}

#[tokio::test(start_paused = true)]
async fn range_read_of_an_empty_stream_is_empty() {
    let cluster = Cluster::start(3).await;

    let records = reader(&cluster)
        .read_range(range(1, 10))
        .await
        .expect("an empty stream is readable");

    assert_eq!(sequences(&records), Vec::<u64>::new());
}

#[tokio::test(start_paused = true)]
async fn range_read_crosses_segment_boundaries() {
    let cluster = Cluster::start(3).await;
    let first_writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&first_writer, 3).await;
    first_writer
        .close()
        .await
        .expect("the records commit and the segment seals");
    cluster.place_standby(&[0, 1, 2]).await;
    let second_writer = open_writer(&cluster, "compute-2").await;
    append_numbered(&second_writer, 3).await;
    settle(HEARTBEAT_SETTLE).await;

    let records = reader(&cluster)
        .read_range(range(1, 6))
        .await
        .expect("replicas serve both segments");

    assert_eq!(sequences(&records), [1, 2, 3, 4, 5, 6]);
}

#[tokio::test(start_paused = true)]
async fn range_read_uses_another_replica_when_one_is_down() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 3).await;
    settle(HEARTBEAT_SETTLE).await;
    cluster.nodes()[0].set_reachable(false);

    let records = reader(&cluster)
        .read_range(range(1, 3))
        .await
        .expect("two replicas still serve the range");

    assert_eq!(sequences(&records), [1, 2, 3]);
}

#[tokio::test(start_paused = true)]
async fn range_read_skips_a_replica_whose_sealed_copy_is_short() {
    let cluster = Cluster::start(4).await;
    let writer = open_writer(&cluster, "compute-1").await;
    cluster.place_standby(&[1, 2, 3]).await;
    append_numbered(&writer, 2).await;
    settle(HEARTBEAT_SETTLE).await;
    // Replica 0 misses records 3 to 5 and the seal that follows.
    cluster.nodes()[0].set_reachable(false);
    append_numbered(&writer, 3).await;
    settle(WriterSettings::default().replica_failure_timeout + Duration::from_secs(1)).await;
    cluster.nodes()[0].set_reachable(true);

    let records = reader(&cluster)
        .read_range(range(1, 5))
        .await
        .expect("two replicas hold the whole sealed segment");

    assert_eq!(sequences(&records), [1, 2, 3, 4, 5]);
    assert_eq!(
        cluster.stream().await.segments[0].sealed_end(),
        Some(SequenceNumber::new(5))
    );
}

#[tokio::test(start_paused = true)]
async fn range_read_fails_when_no_replica_of_the_segment_answers() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 3).await;
    for node in cluster.nodes() {
        node.set_reachable(false);
    }

    let read = reader(&cluster).read_range(range(1, 3)).await;

    assert!(matches!(read, Err(ReadError::Unavailable { .. })), "{read:?}");
}

#[tokio::test(start_paused = true)]
async fn reading_a_stream_that_does_not_exist_fails() {
    let cluster = Cluster::start(3).await;

    let read = cluster
        .client("reader")
        .open_reader("no-such-stream")
        .read_range(range(1, 3))
        .await;

    assert!(matches!(read, Err(ReadError::Metadata(_))));
}

#[tokio::test(start_paused = true)]
async fn read_below_the_low_water_mark_reports_where_reading_can_resume() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 5).await;
    truncate_below(&cluster, 4).await;

    let read = reader(&cluster).read_range(range(2, 5)).await;

    assert_eq!(
        read,
        Err(ReadError::Truncated {
            low_water_mark: SequenceNumber::new(4)
        })
    );
}

#[tokio::test(start_paused = true)]
async fn reader_reports_the_low_water_mark() {
    let cluster = Cluster::start(3).await;
    truncate_below(&cluster, 4).await;

    let low_water_mark = reader(&cluster).low_water_mark().await;

    assert_eq!(low_water_mark, Ok(SequenceNumber::new(4)));
}

#[tokio::test(start_paused = true)]
async fn follower_below_the_low_water_mark_gets_the_error_then_the_end() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 5).await;
    truncate_below(&cluster, 4).await;
    let mut followed = follow_from(&cluster, SequenceNumber::new(2)).await;

    let first = followed.next().await;
    let second = followed.next().await;

    assert_eq!(
        first,
        Some(Err(ReadError::Truncated {
            low_water_mark: SequenceNumber::new(4)
        }))
    );
    assert_eq!(second, None);
}

#[tokio::test(start_paused = true)]
async fn follower_receives_records_committed_before_it_started() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 4).await;

    let mut followed = follow_from(&cluster, SequenceNumber::new(2)).await;

    assert_eq!(take(&mut followed, 3).await, [2, 3, 4]);
}

#[tokio::test(start_paused = true)]
async fn follower_asking_for_the_empty_position_starts_at_the_first_record() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 2).await;

    let mut followed = follow_from(&cluster, SequenceNumber::ZERO).await;

    assert_eq!(take(&mut followed, 2).await, [1, 2]);
}

/// A follower with nothing to ask must wait between looks at the metadata store. Under a paused clock a follower that
/// does not would never let time advance, and this test would hang instead of timing out.
#[tokio::test(start_paused = true)]
async fn follower_of_a_stream_nobody_has_written_waits_instead_of_spinning() {
    let cluster = Cluster::start(3).await;
    let mut followed = follow_from(&cluster, SequenceNumber::FIRST).await;

    let nothing_yet = timeout(Duration::from_secs(5), followed.next()).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 1).await;

    assert!(nothing_yet.is_err(), "the follower saw {nothing_yet:?}");
    assert_eq!(take(&mut followed, 1).await, [1]);
}

#[tokio::test(start_paused = true)]
async fn follower_waits_between_two_writers_instead_of_spinning() {
    let cluster = Cluster::start(3).await;
    let first_writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&first_writer, 2).await;
    first_writer
        .close()
        .await
        .expect("the records commit and the segment seals");
    let mut followed = follow_from(&cluster, SequenceNumber::FIRST).await;

    let before_the_gap = take(&mut followed, 2).await;
    let in_the_gap = timeout(Duration::from_secs(5), followed.next()).await;

    assert_eq!(before_the_gap, [1, 2]);
    assert!(in_the_gap.is_err(), "the follower saw {in_the_gap:?}");
}

#[tokio::test(start_paused = true)]
async fn follower_receives_a_record_shortly_after_it_commits() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    let mut followed = follow_from(&cluster, SequenceNumber::FIRST).await;
    let waiting = tokio::spawn(async move {
        let record = followed.next().await;
        (record, Instant::now())
    });
    settle(Duration::from_secs(3)).await;

    let appended_at = Instant::now();
    append_numbered(&writer, 1).await;
    let (record, received_at) = waiting.await.expect("the follower's task is not cancelled");

    let received = record
        .expect("a followed stream does not end")
        .expect("the follower's position was not truncated");
    let delay = received_at.duration_since(appended_at);
    assert_eq!(received.sequence, SequenceNumber::FIRST);
    assert!(delay <= Duration::from_millis(100), "delay {delay:?}");
}

#[tokio::test(start_paused = true)]
async fn follower_is_never_served_an_uncommitted_record() {
    let cluster = Cluster::start(3).await;
    let writer = Arc::new(open_writer(&cluster, "compute-1").await);
    append_numbered(&writer, 2).await;
    cluster.nodes()[1].set_reachable(false);
    cluster.nodes()[2].set_reachable(false);
    let _in_flight = {
        let writer = Arc::clone(&writer);
        tokio::spawn(async move { writer.append(Bytes::from("reaches one replica only")).await })
    };
    settle(HEARTBEAT_SETTLE).await;
    let mut followed = follow_from(&cluster, SequenceNumber::FIRST).await;

    let committed = take(&mut followed, 2).await;
    let uncommitted = timeout(Duration::from_secs(5), followed.next()).await;

    assert_eq!(committed, [1, 2]);
    assert!(uncommitted.is_err(), "the follower saw {uncommitted:?}");
}

#[tokio::test(start_paused = true)]
async fn follower_continues_across_a_roll() {
    let cluster = Cluster::start(4).await;
    let writer = open_writer(&cluster, "compute-1").await;
    cluster.place_standby(&[0, 1, 3]).await;
    let mut followed = follow_from(&cluster, SequenceNumber::FIRST).await;
    append_numbered(&writer, 3).await;
    cluster.nodes()[2].set_reachable(false);
    settle(WriterSettings::default().replica_failure_timeout + Duration::from_secs(1)).await;
    append_numbered(&writer, 3).await;

    let taken = take(&mut followed, 6).await;

    assert_eq!(taken, [1, 2, 3, 4, 5, 6]);
    assert_eq!(cluster.stream().await.segments.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn follower_continues_across_a_writer_change() {
    let cluster = Cluster::start(3).await;
    let first_writer = open_writer(&cluster, "compute-1").await;
    let mut followed = follow_from(&cluster, SequenceNumber::FIRST).await;
    append_numbered(&first_writer, 3).await;
    drop(first_writer);
    cluster.place_standby(&[0, 1, 2]).await;
    let second_writer = open_writer(&cluster, "compute-2").await;
    append_numbered(&second_writer, 2).await;

    let taken = take(&mut followed, 5).await;

    assert_eq!(taken, [1, 2, 3, 4, 5]);
}

#[tokio::test(start_paused = true)]
async fn follower_switches_away_from_a_replica_cut_off_from_the_writer() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 2).await;
    settle(HEARTBEAT_SETTLE).await;
    let mut followed = follow_from(&cluster, SequenceNumber::FIRST).await;
    let caught_up = take(&mut followed, 2).await;

    // The follower's replica stops hearing from the writer but still answers reads, so it looks alive and merely idle.
    cluster.nodes()[0]
        .replica()
        .expect("the node is running")
        .drain()
        .await
        .expect("a running replica can be drained");
    append_numbered(&writer, 2).await;
    let after_the_cut = take(&mut followed, 2).await;

    assert_eq!(caught_up, [1, 2]);
    assert_eq!(after_the_cut, [3, 4]);
}

#[tokio::test(start_paused = true)]
async fn follower_rides_out_a_metadata_outage() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    let mut followed = follow_from(&cluster, SequenceNumber::FIRST).await;
    append_numbered(&writer, 2).await;
    let before_the_outage = take(&mut followed, 2).await;

    cluster.key_value_store().set_unavailable(true);
    let during_the_outage = timeout(Duration::from_secs(3), followed.next()).await;
    cluster.key_value_store().set_unavailable(false);
    append_numbered(&writer, 1).await;
    let after_the_outage = take(&mut followed, 1).await;

    assert_eq!(before_the_outage, [1, 2]);
    assert!(during_the_outage.is_err(), "the follower yielded {during_the_outage:?}");
    assert_eq!(after_the_outage, [3]);
}

#[tokio::test(start_paused = true)]
async fn follower_hands_out_the_records_at_hand_as_one_batch() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 5).await;
    settle(HEARTBEAT_SETTLE).await;
    let mut followed = follow_from(&cluster, SequenceNumber::FIRST).await;

    let batch = followed
        .next_batch()
        .await
        .expect("a followed stream does not end")
        .expect("the follower's position was not truncated");

    assert_eq!(sequences(&batch), [1, 2, 3, 4, 5]);
}

#[tokio::test(start_paused = true)]
async fn batch_after_a_single_record_holds_the_rest() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 4).await;
    settle(HEARTBEAT_SETTLE).await;
    let mut followed = follow_from(&cluster, SequenceNumber::FIRST).await;
    take(&mut followed, 1).await;

    let rest = followed
        .next_batch()
        .await
        .expect("a followed stream does not end")
        .expect("the follower's position was not truncated");

    assert_eq!(sequences(&rest), [2, 3, 4]);
}

#[tokio::test(start_paused = true)]
async fn batch_below_the_low_water_mark_is_the_truncation_error() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 5).await;
    truncate_below(&cluster, 4).await;
    let mut followed = follow_from(&cluster, SequenceNumber::new(2)).await;

    let batch = followed.next_batch().await;

    assert_eq!(
        batch,
        Some(Err(ReadError::Truncated {
            low_water_mark: SequenceNumber::new(4)
        }))
    );
}

#[tokio::test(start_paused = true)]
async fn follower_that_has_read_nothing_knows_no_committed_record() {
    let cluster = Cluster::start(3).await;

    let followed = follow_from(&cluster, SequenceNumber::FIRST).await;

    assert_eq!(followed.durable_sequence(), SequenceNumber::ZERO);
}

#[tokio::test(start_paused = true)]
async fn follower_learns_how_far_the_stream_is_committed_from_its_first_read() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 5).await;
    settle(HEARTBEAT_SETTLE).await;
    let mut followed = follow_from(&cluster, SequenceNumber::FIRST).await;

    take(&mut followed, 1).await;

    assert_eq!(followed.durable_sequence(), SequenceNumber::new(5));
}
