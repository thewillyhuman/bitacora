//! The write path and the failover protocol, end to end against real
//! replicas on in-memory disks. Time is virtual, so timeouts cost nothing.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use bitacora_types::replica_api::ReplicaApi;
use bitacora_types::{Epoch, SegmentId, SequenceNumber};
use bytes::Bytes;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};

use super::harness::{Cluster, STREAM, settle};
use crate::{AppendError, OpenError, StreamWriter, TakeoverPolicy, WriterSettings};

const HEARTBEAT_SETTLE: Duration = Duration::from_millis(200);

fn record(text: &str) -> Bytes {
    Bytes::copy_from_slice(text.as_bytes())
}

async fn open(cluster: &Cluster, writer_identity: &str) -> StreamWriter {
    cluster
        .client(writer_identity)
        .open_writer(STREAM, TakeoverPolicy::Force)
        .await
        .expect("the writer opens")
}

async fn append_many(writer: &StreamWriter, count: u64) {
    for number in 0..count {
        writer
            .append(record(&format!("record-{number}")))
            .await
            .expect("the record commits");
    }
}

/// Starts an append in its own task, so the test can go on while it waits.
fn append_in_background(writer: &Arc<StreamWriter>, text: &str) -> JoinHandle<Result<SequenceNumber, AppendError>> {
    let writer = Arc::clone(writer);
    let payload = record(text);
    tokio::spawn(async move { writer.append(payload).await })
}

/// Simulates a compute node that dies with an append in flight, and
/// returns once nothing of the writer is running any more.
async fn crash(writer: Arc<StreamWriter>, in_flight: JoinHandle<Result<SequenceNumber, AppendError>>) {
    in_flight.abort();
    drop(writer);
    // The aborted task held the last handle to the writer. Waiting for it
    // guarantees the writer has stopped before the test changes the network.
    let cancelled = in_flight.await;
    assert!(cancelled.is_err(), "the append was still in flight");
}

// ---- opening and appending ----

#[tokio::test(start_paused = true)]
async fn opening_a_fresh_stream_claims_the_first_epoch_and_segment() {
    let cluster = Cluster::start(3).await;
    let writer = open(&cluster, "compute-1").await;
    let stream = cluster.stream().await;
    assert_eq!(
        (
            writer.epoch(),
            stream.segments.len(),
            stream.segments[0].first_sequence,
            stream.writer.map(|lease| lease.writer_identity)
        ),
        (Epoch::new(1), 1, SequenceNumber::FIRST, Some("compute-1".to_owned()))
    );
}

#[tokio::test(start_paused = true)]
async fn opening_a_stream_that_does_not_exist_fails() {
    let cluster = Cluster::start(3).await;
    let opened = cluster
        .client("compute-1")
        .open_writer("no-such-stream", TakeoverPolicy::Force)
        .await;
    assert!(matches!(opened, Err(OpenError::Metadata(_))));
}

#[tokio::test(start_paused = true)]
async fn appends_get_consecutive_sequence_numbers_from_one() {
    let cluster = Cluster::start(3).await;
    let writer = open(&cluster, "compute-1").await;
    let mut sequences = Vec::new();
    for text in ["a", "b", "c"] {
        sequences.push(writer.append(record(text)).await.expect("commits").get());
    }
    assert_eq!(sequences, [1, 2, 3]);
}

#[tokio::test(start_paused = true)]
async fn durable_sequence_follows_the_commits() {
    let cluster = Cluster::start(3).await;
    let writer = open(&cluster, "compute-1").await;
    append_many(&writer, 4).await;
    assert_eq!(writer.durable_sequence(), SequenceNumber::new(4));
}

#[tokio::test(start_paused = true)]
async fn committed_records_reach_every_healthy_replica() {
    let cluster = Cluster::start(3).await;
    let writer = open(&cluster, "compute-1").await;
    append_many(&writer, 4).await;
    settle(HEARTBEAT_SETTLE).await;
    let segment = cluster.segment_address(1).await;
    for node in cluster.nodes() {
        assert_eq!(
            node.local_end(segment).await,
            Some(SequenceNumber::new(4)),
            "{}",
            node.replica_id()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn replicas_serve_committed_records_within_a_heartbeat() {
    let cluster = Cluster::start(3).await;
    let writer = open(&cluster, "compute-1").await;
    append_many(&writer, 3).await;
    settle(HEARTBEAT_SETTLE).await;
    let segment = cluster.segment_address(1).await;
    for node in cluster.nodes() {
        assert_eq!(node.readable_sequences(segment).await, [1, 2, 3]);
    }
}

#[tokio::test(start_paused = true)]
async fn concurrent_appends_each_get_their_own_sequence_number() {
    let cluster = Cluster::start(3).await;
    let writer = Arc::new(open(&cluster, "compute-1").await);
    let appends: Vec<_> = (0..50)
        .map(|number| append_in_background(&writer, &format!("record-{number}")))
        .collect();
    let mut sequences = BTreeSet::new();
    for append in appends {
        sequences.insert(append.await.expect("task finishes").expect("commits").get());
    }
    assert_eq!(sequences, (1..=50).collect());
}

#[tokio::test(start_paused = true)]
async fn batch_append_occupies_one_consecutive_range() {
    let cluster = Cluster::start(3).await;
    let writer = open(&cluster, "compute-1").await;
    writer.append(record("before")).await.expect("commits");
    let range = writer
        .append_batch(vec![record("a"), record("b"), record("c")])
        .await
        .expect("commits");
    assert_eq!(range.to_string(), "2..=4");
}

#[tokio::test(start_paused = true)]
async fn oversized_record_is_rejected_and_the_writer_keeps_working() {
    let cluster = Cluster::start(3).await;
    let writer = open(&cluster, "compute-1").await;
    let oversized = Bytes::from(vec![0; bitacora_types::MAX_RECORD_PAYLOAD_BYTES + 1]);
    let rejected = writer.append(oversized).await;
    let next = writer.append(record("fits")).await;
    assert!(
        matches!(rejected, Err(AppendError::Rejected(_))) && next == Ok(SequenceNumber::FIRST),
        "rejected: {rejected:?}, next: {next:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn appends_do_not_need_the_metadata_store() {
    let cluster = Cluster::start(3).await;
    let writer = open(&cluster, "compute-1").await;
    cluster.key_value_store().set_unavailable(true);
    assert_eq!(writer.append(record("a")).await, Ok(SequenceNumber::FIRST));
}

// ---- replica failures ----

#[tokio::test(start_paused = true)]
async fn append_commits_with_one_replica_down() {
    let cluster = Cluster::start(3).await;
    let writer = open(&cluster, "compute-1").await;
    cluster.nodes()[2].set_reachable(false);
    assert_eq!(writer.append(record("a")).await, Ok(SequenceNumber::FIRST));
}

#[tokio::test(start_paused = true)]
async fn append_waits_without_a_quorum_and_commits_when_it_returns() {
    let cluster = Cluster::start(3).await;
    let writer = Arc::new(open(&cluster, "compute-1").await);
    cluster.nodes()[1].set_reachable(false);
    cluster.nodes()[2].set_reachable(false);
    let mut pending = append_in_background(&writer, "a");
    let waited = timeout(Duration::from_secs(2), &mut pending).await;
    cluster.nodes()[1].set_reachable(true);
    let committed = pending.await.expect("task finishes");
    assert!(
        waited.is_err() && committed == Ok(SequenceNumber::FIRST),
        "committed: {committed:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn replica_that_missed_records_catches_up_when_it_returns() {
    let cluster = Cluster::start(3).await;
    let writer = open(&cluster, "compute-1").await;
    cluster.nodes()[2].set_reachable(false);
    append_many(&writer, 5).await;
    cluster.nodes()[2].set_reachable(true);
    settle(Duration::from_secs(1)).await;
    let segment = cluster.segment_address(1).await;
    assert_eq!(
        cluster.nodes()[2].local_end(segment).await,
        Some(SequenceNumber::new(5))
    );
}

#[tokio::test(start_paused = true)]
async fn appends_wait_while_too_much_is_unacknowledged() {
    let cluster = Cluster::start(3).await;
    let settings = WriterSettings {
        max_unacknowledged_bytes: 1024,
        ..WriterSettings::default()
    };
    let writer = Arc::new(
        cluster
            .client_with("compute-1", &settings)
            .open_writer(STREAM, TakeoverPolicy::Force)
            .await
            .expect("the writer opens"),
    );
    cluster.nodes()[1].set_reachable(false);
    cluster.nodes()[2].set_reachable(false);
    let first = append_in_background(&writer, &"x".repeat(2048));
    settle(HEARTBEAT_SETTLE).await;
    let second = append_in_background(&writer, "held back by flow control");
    settle(HEARTBEAT_SETTLE).await;
    let segment = cluster.segment_address(1).await;
    let sent_while_full = cluster.nodes()[0].local_end(segment).await;

    cluster.nodes()[1].set_reachable(true);
    let committed = (
        first.await.expect("task finishes"),
        second.await.expect("task finishes"),
    );
    assert_eq!(
        (sent_while_full, committed),
        (
            Some(SequenceNumber::new(1)),
            (Ok(SequenceNumber::new(1)), Ok(SequenceNumber::new(2)))
        )
    );
}

// ---- rolling ----

#[tokio::test(start_paused = true)]
async fn writer_rolls_away_from_a_replica_that_stays_down() {
    let cluster = Cluster::start(4).await;
    let writer = open(&cluster, "compute-1").await;
    cluster.place_standby(&[0, 1, 3]).await;
    append_many(&writer, 3).await;
    cluster.nodes()[2].set_reachable(false);
    settle(WriterSettings::default().replica_failure_timeout + Duration::from_secs(1)).await;

    let next = writer.append(record("after the roll")).await;
    let stream = cluster.stream().await;
    assert_eq!(
        (
            next,
            stream.segments.len(),
            stream.segments[0].sealed_end(),
            stream.segments[1].first_sequence
        ),
        (
            Ok(SequenceNumber::new(4)),
            2,
            Some(SequenceNumber::new(3)),
            SequenceNumber::new(4)
        )
    );
}

#[tokio::test(start_paused = true)]
async fn records_in_flight_during_a_roll_keep_their_sequence_numbers() {
    let cluster = Cluster::start(5).await;
    let writer = Arc::new(open(&cluster, "compute-1").await);
    cluster.place_standby(&[0, 3, 4]).await;
    append_many(&writer, 2).await;
    cluster.nodes()[1].set_reachable(false);
    cluster.nodes()[2].set_reachable(false);

    let in_flight = append_in_background(&writer, "uncommitted when the roll starts");
    let committed = in_flight.await.expect("task finishes");
    let stream = cluster.stream().await;
    assert_eq!(
        (
            committed,
            stream.segments[0].sealed_end(),
            stream.segments[1].first_sequence
        ),
        (
            Ok(SequenceNumber::new(3)),
            Some(SequenceNumber::new(2)),
            SequenceNumber::new(3)
        )
    );
}

#[tokio::test(start_paused = true)]
async fn sealed_segment_keeps_no_record_above_its_sealed_end() {
    let cluster = Cluster::start(5).await;
    let writer = Arc::new(open(&cluster, "compute-1").await);
    cluster.place_standby(&[0, 3, 4]).await;
    append_many(&writer, 2).await;
    cluster.nodes()[1].set_reachable(false);
    cluster.nodes()[2].set_reachable(false);
    append_in_background(&writer, "uncommitted when the roll starts")
        .await
        .expect("task finishes")
        .expect("commits on the new segment");
    settle(HEARTBEAT_SETTLE).await;
    let old_segment = cluster.segment_address(1).await;
    assert_eq!(
        cluster.nodes()[0].local_end(old_segment).await,
        Some(SequenceNumber::new(2))
    );
}

#[tokio::test(start_paused = true)]
async fn writer_rolls_at_once_when_a_replica_drains() {
    let cluster = Cluster::start(4).await;
    let writer = open(&cluster, "compute-1").await;
    cluster.place_standby(&[0, 1, 3]).await;
    append_many(&writer, 2).await;
    cluster.nodes()[2]
        .replica()
        .expect("the node is running")
        .drain()
        .await
        .expect("drains");
    settle(Duration::from_secs(1)).await;
    assert_eq!(cluster.stream().await.segments.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn writer_rolls_when_the_segment_reaches_its_target_size() {
    let cluster = Cluster::start(3).await;
    let settings = WriterSettings {
        target_segment_bytes: 2 * 1024,
        ..WriterSettings::default()
    };
    let writer = cluster
        .client_with("compute-1", &settings)
        .open_writer(STREAM, TakeoverPolicy::Force)
        .await
        .expect("the writer opens");
    cluster.place_standby(&[0, 1, 2]).await;
    writer.append(Bytes::from(vec![0xAB; 3 * 1024])).await.expect("commits");
    settle(Duration::from_secs(1)).await;
    let stream = cluster.stream().await;
    assert_eq!(
        (stream.segments.len(), stream.segments[0].sealed_end()),
        (2, Some(SequenceNumber::new(1)))
    );
}

#[tokio::test(start_paused = true)]
async fn without_a_standby_the_writer_keeps_committing_on_its_segment() {
    let cluster = Cluster::start(3).await;
    let writer = open(&cluster, "compute-1").await;
    cluster.nodes()[2].set_reachable(false);
    settle(WriterSettings::default().replica_failure_timeout + Duration::from_secs(5)).await;
    let committed = writer.append(record("a")).await;
    assert_eq!(
        (committed, cluster.stream().await.segments.len()),
        (Ok(SequenceNumber::FIRST), 1)
    );
}

// ---- failover ----

#[tokio::test(start_paused = true)]
async fn new_writer_seals_the_old_segment_and_continues_after_it() {
    let cluster = Cluster::start(3).await;
    let first_writer = open(&cluster, "compute-1").await;
    append_many(&first_writer, 5).await;
    drop(first_writer);
    cluster.place_standby(&[0, 1, 2]).await;

    let second_writer = open(&cluster, "compute-2").await;
    let next = second_writer.append(record("from the new writer")).await;
    let stream = cluster.stream().await;
    assert_eq!(
        (
            second_writer.epoch(),
            stream.segments[0].sealed_end(),
            stream.segments[1].epoch,
            next
        ),
        (
            Epoch::new(2),
            Some(SequenceNumber::new(5)),
            Epoch::new(2),
            Ok(SequenceNumber::new(6))
        )
    );
}

#[tokio::test(start_paused = true)]
async fn old_writer_is_fenced_after_a_takeover() {
    let cluster = Cluster::start(3).await;
    let first_writer = open(&cluster, "compute-1").await;
    append_many(&first_writer, 2).await;
    cluster.place_standby(&[0, 1, 2]).await;
    let _second_writer = open(&cluster, "compute-2").await;
    assert_eq!(first_writer.append(record("too late")).await, Err(AppendError::Fenced));
}

#[tokio::test(start_paused = true)]
async fn takeover_keeps_an_uncommitted_tail_found_on_a_responder() {
    let cluster = Cluster::start(3).await;
    let first_writer = Arc::new(open(&cluster, "compute-1").await);
    append_many(&first_writer, 3).await;
    cluster.nodes()[1].set_reachable(false);
    cluster.nodes()[2].set_reachable(false);
    let in_flight = append_in_background(&first_writer, "reached one replica only");
    settle(HEARTBEAT_SETTLE).await;
    crash(first_writer, in_flight).await;
    cluster.nodes()[1].set_reachable(true);
    cluster.nodes()[2].set_reachable(true);
    cluster.place_standby(&[0, 1, 2]).await;

    let second_writer = open(&cluster, "compute-2").await;
    let old_segment = cluster.segment_address(1).await;
    assert_eq!(
        (
            cluster.stream().await.segments[0].sealed_end(),
            cluster.nodes()[1].local_end(old_segment).await,
            second_writer.append(record("next")).await
        ),
        (
            Some(SequenceNumber::new(4)),
            Some(SequenceNumber::new(4)),
            Ok(SequenceNumber::new(5))
        )
    );
}

#[tokio::test(start_paused = true)]
async fn takeover_discards_a_tail_that_no_responder_holds() {
    let cluster = Cluster::start(3).await;
    let first_writer = Arc::new(open(&cluster, "compute-1").await);
    append_many(&first_writer, 3).await;
    cluster.nodes()[1].set_reachable(false);
    cluster.nodes()[2].set_reachable(false);
    let in_flight = append_in_background(&first_writer, "reached one replica only");
    settle(HEARTBEAT_SETTLE).await;
    crash(first_writer, in_flight).await;
    cluster.nodes()[0].set_reachable(false);
    cluster.nodes()[1].set_reachable(true);
    cluster.nodes()[2].set_reachable(true);
    cluster.place_standby(&[1, 2, 0]).await;

    let second_writer = open(&cluster, "compute-2").await;
    assert_eq!(
        (
            cluster.stream().await.segments[0].sealed_end(),
            second_writer.append(record("next")).await
        ),
        (Some(SequenceNumber::new(3)), Ok(SequenceNumber::new(4)))
    );
}

#[tokio::test(start_paused = true)]
async fn takeover_fails_without_a_recovery_quorum() {
    let cluster = Cluster::start(3).await;
    let first_writer = open(&cluster, "compute-1").await;
    append_many(&first_writer, 3).await;
    drop(first_writer);
    cluster.nodes()[1].set_reachable(false);
    cluster.nodes()[2].set_reachable(false);
    cluster.place_standby(&[0, 1, 2]).await;

    let opened = cluster
        .client("compute-2")
        .open_writer(STREAM, TakeoverPolicy::Force)
        .await;
    assert_eq!(
        opened.err(),
        Some(OpenError::RecoveryQuorumUnavailable {
            segment: cluster.segment_address(1).await,
            needed: 2,
            reached: 1,
        })
    );
}

#[tokio::test(start_paused = true)]
async fn takeover_waits_for_a_dead_writers_lease_to_lapse() {
    let cluster = Cluster::start(3).await;
    let first_writer = open(&cluster, "compute-1").await;
    drop(first_writer);
    cluster.place_standby(&[0, 1, 2]).await;

    let started_at = Instant::now();
    let opened = cluster
        .client("compute-2")
        .open_writer(STREAM, TakeoverPolicy::WaitForLeaseExpiry)
        .await;
    assert!(
        opened.is_ok() && started_at.elapsed() >= WriterSettings::default().lease_duration,
        "waited {:?}",
        started_at.elapsed()
    );
}

#[tokio::test(start_paused = true)]
async fn takeover_does_not_wait_out_a_live_writer() {
    let cluster = Cluster::start(3).await;
    let _live_writer = open(&cluster, "compute-1").await;
    let impatient = WriterSettings {
        open_timeout: Duration::from_secs(5),
        ..WriterSettings::default()
    };
    let opened = cluster
        .client_with("compute-2", &impatient)
        .open_writer(STREAM, TakeoverPolicy::WaitForLeaseExpiry)
        .await;
    assert!(
        matches!(&opened, Err(OpenError::LeaseHeld { writer_identity, .. }) if writer_identity == "compute-1"),
        "{:?}",
        opened.err()
    );
}

#[tokio::test(start_paused = true)]
async fn live_writer_keeps_its_lease_renewed() {
    let cluster = Cluster::start(3).await;
    let _live_writer = open(&cluster, "compute-1").await;
    let first_expiry = cluster.stream().await.writer.expect("lease").expires_at;
    settle(Duration::from_secs(20)).await;
    let later_expiry = cluster.stream().await.writer.expect("lease").expires_at;
    assert!(later_expiry.duration_since(first_expiry) >= Duration::from_secs(15));
}

#[tokio::test(start_paused = true)]
async fn takeover_waits_for_a_standby_segment_to_be_placed() {
    let cluster = Arc::new(Cluster::start(3).await);
    let first_writer = open(&cluster, "compute-1").await;
    append_many(&first_writer, 2).await;
    drop(first_writer);

    let mut opening = {
        let cluster = Arc::clone(&cluster);
        tokio::spawn(async move {
            cluster
                .client("compute-2")
                .open_writer(STREAM, TakeoverPolicy::Force)
                .await
                .map(|writer| writer.epoch())
        })
    };
    let waited = timeout(Duration::from_secs(2), &mut opening).await;
    cluster.place_standby(&[0, 1, 2]).await;
    let opened = opening.await.expect("task finishes");
    assert!(waited.is_err() && opened == Ok(Epoch::new(2)), "{opened:?}");
}

#[tokio::test(start_paused = true)]
async fn takeover_gives_up_when_no_standby_is_placed_in_time() {
    let cluster = Cluster::start(3).await;
    let first_writer = open(&cluster, "compute-1").await;
    drop(first_writer);
    let opened = cluster
        .client("compute-2")
        .open_writer(STREAM, TakeoverPolicy::Force)
        .await;
    assert_eq!(
        opened.err(),
        Some(OpenError::NoStandbySegment {
            stream: STREAM.to_owned()
        })
    );
}

#[tokio::test(start_paused = true)]
async fn replica_fenced_by_a_newer_writer_stops_an_older_takeover() {
    let cluster = Cluster::start(3).await;
    let first_writer = open(&cluster, "compute-1").await;
    append_many(&first_writer, 2).await;
    drop(first_writer);
    let segment = cluster.segment_address(1).await;
    for node in cluster.nodes() {
        node.replica()
            .expect("the node is running")
            .fence(bitacora_types::replica_api::FenceRequest {
                segment,
                epoch: Epoch::new(9),
            })
            .await
            .expect("fences");
    }
    let opened = cluster
        .client("compute-2")
        .open_writer(STREAM, TakeoverPolicy::Force)
        .await;
    assert_eq!(
        opened.err(),
        Some(OpenError::TakenByAnotherWriter {
            current_epoch: Epoch::new(9)
        })
    );
}

// ---- closing ----

#[tokio::test(start_paused = true)]
async fn closed_writer_leaves_a_sealed_segment_and_no_lease() {
    let cluster = Cluster::start(3).await;
    let writer = open(&cluster, "compute-1").await;
    append_many(&writer, 3).await;
    writer.close().await.expect("closes");
    let stream = cluster.stream().await;
    assert_eq!(
        (stream.segments[0].sealed_end(), stream.writer),
        (Some(SequenceNumber::new(3)), None)
    );
}

#[tokio::test(start_paused = true)]
async fn closed_writer_tells_the_replicas_the_sealed_end() {
    let cluster = Cluster::start(3).await;
    let writer = open(&cluster, "compute-1").await;
    append_many(&writer, 3).await;
    writer.close().await.expect("closes");
    let segment = cluster.segment_address(1).await;
    let sealed_ends: Vec<_> = cluster.nodes()[0]
        .replica()
        .expect("the node is running")
        .list_segments()
        .await
        .expect("lists")
        .into_iter()
        .filter(|status| status.address == segment)
        .map(|status| status.sealed_end)
        .collect();
    assert_eq!(sealed_ends, [Some(SequenceNumber::new(3))]);
}

#[tokio::test(start_paused = true)]
async fn writer_opened_after_a_clean_close_needs_no_recovery() {
    let cluster = Cluster::start(6).await;
    let first_writer = open(&cluster, "compute-1").await;
    append_many(&first_writer, 3).await;
    first_writer.close().await.expect("closes");
    for old_replica in &cluster.nodes()[..3] {
        old_replica.set_reachable(false);
    }
    cluster.place_standby(&[3, 4, 5]).await;

    let second_writer = cluster
        .client("compute-2")
        .open_writer(STREAM, TakeoverPolicy::WaitForLeaseExpiry)
        .await
        .expect("opens without touching the old replicas");
    assert_eq!(
        (
            second_writer.append(record("next")).await,
            cluster.stream().await.segments[1].segment_id
        ),
        (Ok(SequenceNumber::new(4)), SegmentId::new(2))
    );
}

#[tokio::test(start_paused = true)]
async fn close_reports_a_writer_that_was_fenced() {
    let cluster = Cluster::start(3).await;
    let first_writer = open(&cluster, "compute-1").await;
    append_many(&first_writer, 2).await;
    cluster.place_standby(&[0, 1, 2]).await;
    let _second_writer = open(&cluster, "compute-2").await;
    assert_eq!(first_writer.close().await, Err(AppendError::Fenced));
}
