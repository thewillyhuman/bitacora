//! The read path, end to end against real replicas on in-memory disks.

use std::sync::Arc;
use std::time::Duration;

use bitacora_types::replica_api::ReplicaApi;
use bitacora_types::{LogRecord, SequenceNumber, SequenceRange};
use bytes::Bytes;
use tokio::time::{Instant, timeout};

use super::harness::{Cluster, STREAM, settle};
use crate::{ReadError, RecordStream, StreamReader, StreamWriter, TakeoverPolicy, WriterSettings};

const HEARTBEAT_SETTLE: Duration = Duration::from_millis(200);

pub(super) fn range(first: u64, last: u64) -> SequenceRange {
    SequenceRange::new(SequenceNumber::new(first), SequenceNumber::new(last)).expect("valid range")
}

pub(super) async fn open_writer(cluster: &Cluster, writer_identity: &str) -> StreamWriter {
    cluster
        .client(writer_identity)
        .open_writer(STREAM, TakeoverPolicy::Force)
        .await
        .expect("the writer opens")
}

fn reader(cluster: &Cluster) -> StreamReader {
    cluster.client("reader").open_reader(STREAM)
}

/// Appends records whose payload names their own sequence number.
pub(super) async fn append_numbered(writer: &StreamWriter, count: u64) {
    let first = writer.durable_sequence().get() + 1;
    for sequence in first..first + count {
        writer
            .append(Bytes::from(format!("record-{sequence}")))
            .await
            .expect("the record commits");
    }
}

pub(super) fn sequences(records: &[LogRecord]) -> Vec<u64> {
    records.iter().map(|record| record.sequence.get()).collect()
}

/// Takes the next `count` records of a following stream.
pub(super) async fn take(stream: &mut RecordStream, count: usize) -> Vec<u64> {
    let mut taken = Vec::new();
    while taken.len() < count {
        let record = stream
            .next()
            .await
            .expect("the stream goes on")
            .expect("the record is readable");
        taken.push(record.sequence.get());
    }
    taken
}

// ---- bounded reads ----

#[tokio::test(start_paused = true)]
async fn range_read_returns_the_committed_records_in_order() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 5).await;
    settle(HEARTBEAT_SETTLE).await;
    let records = reader(&cluster)
        .read_range(range(2, 4))
        .await
        .expect("reads");
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
        .expect("reads");
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
        .expect("reads");
    assert_eq!(sequences(&records), [2, 3]);
}

#[tokio::test(start_paused = true)]
async fn range_read_of_an_empty_stream_is_empty() {
    let cluster = Cluster::start(3).await;
    let records = reader(&cluster)
        .read_range(range(1, 10))
        .await
        .expect("reads");
    assert_eq!(sequences(&records), Vec::<u64>::new());
}

#[tokio::test(start_paused = true)]
async fn range_read_crosses_segment_boundaries() {
    let cluster = Cluster::start(3).await;
    let first_writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&first_writer, 3).await;
    first_writer.close().await.expect("closes");
    cluster.place_standby(&[0, 1, 2]).await;
    let second_writer = open_writer(&cluster, "compute-2").await;
    append_numbered(&second_writer, 3).await;
    settle(HEARTBEAT_SETTLE).await;

    let records = reader(&cluster)
        .read_range(range(1, 6))
        .await
        .expect("reads");
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
        .expect("reads");
    assert_eq!(sequences(&records), [1, 2, 3]);
}

#[tokio::test(start_paused = true)]
async fn range_read_skips_a_replica_whose_sealed_copy_is_short() {
    let cluster = Cluster::start(4).await;
    let writer = open_writer(&cluster, "compute-1").await;
    cluster.place_standby(&[1, 2, 3]).await;
    append_numbered(&writer, 2).await;
    settle(HEARTBEAT_SETTLE).await;
    // Replica 0 misses records 3..=5 and the seal that follows.
    cluster.nodes()[0].set_reachable(false);
    append_numbered(&writer, 3).await;
    settle(WriterSettings::default().replica_failure_timeout + Duration::from_secs(1)).await;
    cluster.nodes()[0].set_reachable(true);

    let records = reader(&cluster)
        .read_range(range(1, 5))
        .await
        .expect("reads");
    assert_eq!(
        (
            sequences(&records),
            cluster.stream().await.segments[0].sealed_end()
        ),
        (vec![1, 2, 3, 4, 5], Some(SequenceNumber::new(5)))
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
    assert!(
        matches!(read, Err(ReadError::Unavailable { .. })),
        "{read:?}"
    );
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

// ---- truncation ----

async fn truncate_below(cluster: &Cluster, low_water_mark: u64) {
    cluster
        .metadata()
        .update_stream(STREAM, |stream| {
            stream.low_water_mark = SequenceNumber::new(low_water_mark);
            Ok(())
        })
        .await
        .expect("the mark moves");
}

#[tokio::test(start_paused = true)]
async fn read_below_the_low_water_mark_reports_where_reading_can_resume() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 5).await;
    truncate_below(&cluster, 4).await;
    assert_eq!(
        reader(&cluster).read_range(range(2, 5)).await,
        Err(ReadError::Truncated {
            low_water_mark: SequenceNumber::new(4)
        })
    );
}

#[tokio::test(start_paused = true)]
async fn reader_reports_the_low_water_mark() {
    let cluster = Cluster::start(3).await;
    truncate_below(&cluster, 4).await;
    assert_eq!(
        reader(&cluster).low_water_mark().await,
        Ok(SequenceNumber::new(4))
    );
}

#[tokio::test(start_paused = true)]
async fn follower_below_the_low_water_mark_gets_the_error_then_the_end() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 5).await;
    truncate_below(&cluster, 4).await;
    let mut following = reader(&cluster)
        .follow_from(SequenceNumber::new(2))
        .await
        .expect("follows");
    let first = following.next().await;
    let second = following.next().await;
    assert!(
        first
            == Some(Err(ReadError::Truncated {
                low_water_mark: SequenceNumber::new(4)
            }))
            && second.is_none(),
        "first: {first:?}"
    );
}

// ---- following ----

#[tokio::test(start_paused = true)]
async fn follower_receives_records_committed_before_it_started() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 4).await;
    let mut following = reader(&cluster)
        .follow_from(SequenceNumber::new(2))
        .await
        .expect("follows");
    assert_eq!(take(&mut following, 3).await, [2, 3, 4]);
}

#[tokio::test(start_paused = true)]
async fn follower_asking_for_the_empty_position_starts_at_the_first_record() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 2).await;
    let mut following = reader(&cluster)
        .follow_from(SequenceNumber::ZERO)
        .await
        .expect("follows");
    assert_eq!(take(&mut following, 2).await, [1, 2]);
}

/// A follower with nothing to ask must wait between looks at the metadata
/// store. Under a paused clock a follower that does not would never let
/// time advance, and this test would hang instead of timing out.
#[tokio::test(start_paused = true)]
async fn follower_of_a_stream_nobody_has_written_waits_instead_of_spinning() {
    let cluster = Cluster::start(3).await;
    let mut following = reader(&cluster)
        .follow_from(SequenceNumber::FIRST)
        .await
        .expect("follows");
    let nothing_yet = timeout(Duration::from_secs(5), following.next()).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 1).await;
    assert!(nothing_yet.is_err() && take(&mut following, 1).await == [1]);
}

#[tokio::test(start_paused = true)]
async fn follower_waits_between_two_writers_instead_of_spinning() {
    let cluster = Cluster::start(3).await;
    let first_writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&first_writer, 2).await;
    first_writer.close().await.expect("closes");
    let mut following = reader(&cluster)
        .follow_from(SequenceNumber::FIRST)
        .await
        .expect("follows");
    let before_the_gap = take(&mut following, 2).await;
    let in_the_gap = timeout(Duration::from_secs(5), following.next()).await;
    assert!(before_the_gap == [1, 2] && in_the_gap.is_err());
}

#[tokio::test(start_paused = true)]
async fn follower_receives_a_record_shortly_after_it_commits() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    let mut following = reader(&cluster)
        .follow_from(SequenceNumber::FIRST)
        .await
        .expect("follows");
    let waiting = tokio::spawn(async move {
        let record = following.next().await;
        (record, Instant::now())
    });
    settle(Duration::from_secs(3)).await;

    let appended_at = Instant::now();
    append_numbered(&writer, 1).await;
    let (record, received_at) = waiting.await.expect("task finishes");
    let delay = received_at.duration_since(appended_at);
    assert!(
        record.is_some_and(|record| record.is_ok_and(|record| record.sequence.get() == 1))
            && delay <= Duration::from_millis(100),
        "delay {delay:?}"
    );
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

    let mut following = reader(&cluster)
        .follow_from(SequenceNumber::FIRST)
        .await
        .expect("follows");
    let committed = take(&mut following, 2).await;
    let uncommitted = timeout(Duration::from_secs(5), following.next()).await;
    assert!(
        committed == [1, 2] && uncommitted.is_err(),
        "the follower saw {uncommitted:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn follower_continues_across_a_roll() {
    let cluster = Cluster::start(4).await;
    let writer = open_writer(&cluster, "compute-1").await;
    cluster.place_standby(&[0, 1, 3]).await;
    let mut following = reader(&cluster)
        .follow_from(SequenceNumber::FIRST)
        .await
        .expect("follows");
    append_numbered(&writer, 3).await;
    cluster.nodes()[2].set_reachable(false);
    settle(WriterSettings::default().replica_failure_timeout + Duration::from_secs(1)).await;
    append_numbered(&writer, 3).await;

    assert_eq!(
        (
            take(&mut following, 6).await,
            cluster.stream().await.segments.len()
        ),
        (vec![1, 2, 3, 4, 5, 6], 2)
    );
}

#[tokio::test(start_paused = true)]
async fn follower_continues_across_a_writer_change() {
    let cluster = Cluster::start(3).await;
    let first_writer = open_writer(&cluster, "compute-1").await;
    let mut following = reader(&cluster)
        .follow_from(SequenceNumber::FIRST)
        .await
        .expect("follows");
    append_numbered(&first_writer, 3).await;
    drop(first_writer);
    cluster.place_standby(&[0, 1, 2]).await;
    let second_writer = open_writer(&cluster, "compute-2").await;
    append_numbered(&second_writer, 2).await;

    assert_eq!(take(&mut following, 5).await, [1, 2, 3, 4, 5]);
}

#[tokio::test(start_paused = true)]
async fn follower_switches_away_from_a_replica_cut_off_from_the_writer() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 2).await;
    settle(HEARTBEAT_SETTLE).await;
    let mut following = reader(&cluster)
        .follow_from(SequenceNumber::FIRST)
        .await
        .expect("follows");
    let caught_up = take(&mut following, 2).await;

    // The follower's replica stops hearing from the writer but still
    // answers reads, so it looks alive and merely idle.
    cluster.nodes()[0]
        .replica()
        .expect("the node is running")
        .drain()
        .await
        .expect("drains");
    append_numbered(&writer, 2).await;
    assert_eq!(
        (caught_up, take(&mut following, 2).await),
        (vec![1, 2], vec![3, 4])
    );
}

#[tokio::test(start_paused = true)]
async fn follower_rides_out_a_metadata_outage() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    let mut following = reader(&cluster)
        .follow_from(SequenceNumber::FIRST)
        .await
        .expect("follows");
    append_numbered(&writer, 2).await;
    let before_outage = take(&mut following, 2).await;

    cluster.key_value_store().set_unavailable(true);
    let during_outage = timeout(Duration::from_secs(3), following.next()).await;
    cluster.key_value_store().set_unavailable(false);
    append_numbered(&writer, 1).await;
    assert!(
        before_outage == [1, 2] && during_outage.is_err() && take(&mut following, 1).await == [3]
    );
}
