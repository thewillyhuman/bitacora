//! Tests of the write path and the failover protocol, end to end.
//!
//! # What is covered
//!
//! - Opening a writer: the first epoch and segment, the lease, a stream that does not exist.
//! - Appending: sequence numbers, the durable sequence, batches, rejected records, concurrent appends, flow control,
//!   and that an append does not need the metadata store.
//! - Replica failures: committing with one replica down, waiting without a quorum, a replica that catches up.
//! - Rolling: away from a replica that stays down or drains, at the target segment size, with records in flight, and
//!   staying put when there is no standby segment.
//! - Failover: sealing the old writer's segment, fencing the old writer, a tail found or not found on a responder, the
//!   recovery quorum, the lease, the standby segment, and losing to a newer writer.
//! - Closing: the seal, the lease, the replicas, the next writer, and a writer that was fenced.
//!
//! # How the fixtures work
//!
//! `support::Cluster` is three or more real replicas on in-memory disks with one stream, `orders`. A test cuts a
//! replica off with `set_reachable(false)` and places the standby segment itself, because no controller runs. Time is
//! virtual, so waiting out a timeout costs nothing; `settle` lets it pass.
//!
//! Scenarios with two outcomes worth a test each (an append that waits and then commits, for example) are run by a
//! function that returns a small struct, and each test asserts one of its fields.

mod support;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use bitacora_client::{AppendError, OpenError, StreamWriter, TakeoverPolicy, WriterSettings};
use bitacora_types::replica_api::{FenceRequest, ReplicaApi};
use bitacora_types::{Epoch, MAX_RECORD_PAYLOAD_BYTES, SegmentId, SequenceNumber};
use bytes::Bytes;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};

use support::{Cluster, HEARTBEAT_SETTLE, STREAM, append_numbered, open_writer, settle};

/// An append running in its own task, and what it resolves to.
type BackgroundAppend = JoinHandle<Result<SequenceNumber, AppendError>>;

/// A record payload holding `text`.
fn payload(text: &str) -> Bytes {
    Bytes::copy_from_slice(text.as_bytes())
}

/// Starts an append of `text` in its own task, so that the test can go on while the append waits.
fn append_in_background(writer: &Arc<StreamWriter>, text: &str) -> BackgroundAppend {
    let writer = Arc::clone(writer);
    let record = payload(text);
    tokio::spawn(async move { writer.append(record).await })
}

/// Simulates a compute node that dies with an append in flight, and returns once nothing of the writer is running any
/// more.
async fn crash(writer: Arc<StreamWriter>, in_flight: BackgroundAppend) {
    in_flight.abort();
    drop(writer);
    // The aborted task held the last handle to the writer. Waiting for it guarantees the writer has stopped before the
    // test changes the network.
    in_flight
        .await
        .expect_err("the append was still in flight when the node died");
}

/// Opens a writer of [`STREAM`] with `settings`, taking the stream over at once.
async fn open_writer_with(cluster: &Cluster, writer_identity: &str, settings: &WriterSettings) -> StreamWriter {
    cluster
        .client_with(writer_identity, settings)
        .open_writer(STREAM, TakeoverPolicy::Force)
        .await
        .expect("the stream has a recovery quorum and a segment to open")
}

/// What became of an append made while two of the three replicas were cut off.
struct AppendWithoutQuorum {
    /// Whether the append was still waiting two seconds after it was made.
    was_still_waiting: bool,
    /// What the append resolved to after one of the two replicas returned.
    outcome: Result<SequenceNumber, AppendError>,
}

/// Appends one record while only one replica is reachable, waits two seconds, then lets a second replica return.
async fn append_without_a_quorum() -> AppendWithoutQuorum {
    let cluster = Cluster::start(3).await;
    let writer = Arc::new(open_writer(&cluster, "compute-1").await);
    cluster.nodes()[1].set_reachable(false);
    cluster.nodes()[2].set_reachable(false);
    let mut pending = append_in_background(&writer, "a");

    let waited = timeout(Duration::from_secs(2), &mut pending).await;
    cluster.nodes()[1].set_reachable(true);
    let outcome = pending.await.expect("the append's task is not cancelled");

    AppendWithoutQuorum {
        was_still_waiting: waited.is_err(),
        outcome,
    }
}

/// What became of two appends made while the first alone exceeded the writer's limit of unacknowledged bytes.
struct AppendsUnderFlowControl {
    /// The local end of the one reachable replica while the limit was exceeded: how much the writer had sent by then.
    sent_while_full: Option<SequenceNumber>,
    /// What the first append resolved to once a quorum returned.
    first_outcome: Result<SequenceNumber, AppendError>,
    /// What the second append, the one held back, resolved to once a quorum returned.
    second_outcome: Result<SequenceNumber, AppendError>,
}

/// Makes two appends on a writer that may hold 1 KiB unacknowledged while no quorum is reachable: the first is 2 KiB,
/// so the second must wait. Then lets a quorum return.
async fn two_appends_under_flow_control() -> AppendsUnderFlowControl {
    let cluster = Cluster::start(3).await;
    let settings = WriterSettings {
        max_unacknowledged_bytes: 1024,
        ..WriterSettings::default()
    };
    let writer = Arc::new(open_writer_with(&cluster, "compute-1", &settings).await);
    cluster.nodes()[1].set_reachable(false);
    cluster.nodes()[2].set_reachable(false);

    let first = append_in_background(&writer, &"x".repeat(2048));
    settle(HEARTBEAT_SETTLE).await;
    let second = append_in_background(&writer, "held back by flow control");
    settle(HEARTBEAT_SETTLE).await;
    let segment = cluster.segment_address(1).await;
    let sent_while_full = cluster.nodes()[0].local_end(segment).await;
    cluster.nodes()[1].set_reachable(true);

    AppendsUnderFlowControl {
        sent_while_full,
        first_outcome: first.await.expect("the first append's task is not cancelled"),
        second_outcome: second.await.expect("the second append's task is not cancelled"),
    }
}

/// A stream whose first writer committed five records and went away, taken over by a second writer.
struct TakenOverStream {
    /// The cluster the stream lives on.
    cluster: Cluster,
    /// The writer that took the stream over.
    second_writer: StreamWriter,
}

/// Lets a first writer commit five records and disappear without closing, then opens a second writer.
async fn take_over_after_five_records() -> TakenOverStream {
    let cluster = Cluster::start(3).await;
    let first_writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&first_writer, 5).await;
    drop(first_writer);
    cluster.place_standby(&[0, 1, 2]).await;

    let second_writer = open_writer(&cluster, "compute-2").await;

    TakenOverStream { cluster, second_writer }
}

/// A cluster whose first writer died with record 4 in flight: records 1 to 3 are committed, and record 4 reached
/// replica 0 only. Replicas 1 and 2 are still cut off, as they were when the writer died.
async fn cluster_with_a_tail_on_one_replica() -> Cluster {
    let cluster = Cluster::start(3).await;
    let first_writer = Arc::new(open_writer(&cluster, "compute-1").await);
    append_numbered(&first_writer, 3).await;
    cluster.nodes()[1].set_reachable(false);
    cluster.nodes()[2].set_reachable(false);
    let in_flight = append_in_background(&first_writer, "reached one replica only");
    settle(HEARTBEAT_SETTLE).await;
    crash(first_writer, in_flight).await;
    cluster
}

/// A cluster whose only writer committed three records and closed cleanly.
async fn cluster_with_a_closed_writer() -> Cluster {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&writer, 3).await;
    writer.close().await.expect("the records commit and the segment seals");
    cluster
}

#[tokio::test(start_paused = true)]
async fn writer_of_a_fresh_stream_holds_the_first_epoch() {
    let cluster = Cluster::start(3).await;

    let writer = open_writer(&cluster, "compute-1").await;

    assert_eq!(writer.epoch(), Epoch::new(1));
}

#[tokio::test(start_paused = true)]
async fn opening_a_fresh_stream_opens_one_segment_at_the_first_record() {
    let cluster = Cluster::start(3).await;

    let _writer = open_writer(&cluster, "compute-1").await;

    let stream = cluster.stream().await;
    assert_eq!(stream.segments.len(), 1);
    assert_eq!(stream.segments[0].first_sequence, SequenceNumber::FIRST);
}

#[tokio::test(start_paused = true)]
async fn open_writer_holds_the_lease_under_its_identity() {
    let cluster = Cluster::start(3).await;

    let _writer = open_writer(&cluster, "compute-1").await;

    let lease = cluster.stream().await.writer.expect("an open writer holds a lease");
    assert_eq!(lease.writer_identity, "compute-1");
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
    let writer = open_writer(&cluster, "compute-1").await;

    let mut sequences = Vec::new();
    for text in ["a", "b", "c"] {
        let sequence = writer.append(payload(text)).await.expect("a write quorum is reachable");
        sequences.push(sequence.get());
    }

    assert_eq!(sequences, [1, 2, 3]);
}

#[tokio::test(start_paused = true)]
async fn durable_sequence_follows_the_commits() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;

    append_numbered(&writer, 4).await;

    assert_eq!(writer.durable_sequence(), SequenceNumber::new(4));
}

#[tokio::test(start_paused = true)]
async fn committed_records_reach_every_healthy_replica() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;

    append_numbered(&writer, 4).await;
    settle(HEARTBEAT_SETTLE).await;

    let segment = cluster.segment_address(1).await;
    let mut local_ends = Vec::new();
    for node in cluster.nodes() {
        local_ends.push(node.local_end(segment).await);
    }
    assert_eq!(local_ends, [Some(SequenceNumber::new(4)); 3]);
}

#[tokio::test(start_paused = true)]
async fn replicas_serve_committed_records_within_a_heartbeat() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;

    append_numbered(&writer, 3).await;
    settle(HEARTBEAT_SETTLE).await;

    let segment = cluster.segment_address(1).await;
    let mut readable_per_replica = Vec::new();
    for node in cluster.nodes() {
        readable_per_replica.push(node.readable_sequences(segment).await);
    }
    assert_eq!(readable_per_replica, [[1, 2, 3], [1, 2, 3], [1, 2, 3]]);
}

#[tokio::test(start_paused = true)]
async fn concurrent_appends_each_get_their_own_sequence_number() {
    let cluster = Cluster::start(3).await;
    let writer = Arc::new(open_writer(&cluster, "compute-1").await);

    let mut appends = Vec::new();
    for number in 0..50 {
        appends.push(append_in_background(&writer, &format!("record-{number}")));
    }
    let mut sequences = BTreeSet::new();
    for append in appends {
        let sequence = append
            .await
            .expect("the append's task is not cancelled")
            .expect("a write quorum is reachable");
        sequences.insert(sequence.get());
    }

    let every_sequence: BTreeSet<u64> = (1..=50).collect();
    assert_eq!(sequences, every_sequence);
}

#[tokio::test(start_paused = true)]
async fn batch_append_occupies_one_consecutive_range() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    writer
        .append(payload("before"))
        .await
        .expect("a write quorum is reachable");

    let range = writer
        .append_batch(vec![payload("a"), payload("b"), payload("c")])
        .await
        .expect("a write quorum is reachable");

    assert_eq!(range.to_string(), "2..=4");
}

#[tokio::test(start_paused = true)]
async fn oversized_record_is_rejected() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    let oversized = Bytes::from(vec![0; MAX_RECORD_PAYLOAD_BYTES + 1]);

    let rejected = writer.append(oversized).await;

    assert!(matches!(rejected, Err(AppendError::Rejected(_))), "{rejected:?}");
}

#[tokio::test(start_paused = true)]
async fn writer_keeps_working_after_rejecting_a_record() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    let oversized = Bytes::from(vec![0; MAX_RECORD_PAYLOAD_BYTES + 1]);
    let _rejected = writer.append(oversized).await;

    let next = writer.append(payload("fits")).await;

    assert_eq!(next, Ok(SequenceNumber::FIRST));
}

#[tokio::test(start_paused = true)]
async fn appends_do_not_need_the_metadata_store() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    cluster.key_value_store().set_unavailable(true);

    let committed = writer.append(payload("a")).await;

    assert_eq!(committed, Ok(SequenceNumber::FIRST));
}

#[tokio::test(start_paused = true)]
async fn append_commits_with_one_replica_down() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    cluster.nodes()[2].set_reachable(false);

    let committed = writer.append(payload("a")).await;

    assert_eq!(committed, Ok(SequenceNumber::FIRST));
}

#[tokio::test(start_paused = true)]
async fn append_waits_while_no_write_quorum_is_reachable() {
    let append = append_without_a_quorum().await;

    assert!(append.was_still_waiting);
}

#[tokio::test(start_paused = true)]
async fn append_that_waited_for_a_quorum_commits_when_one_returns() {
    let append = append_without_a_quorum().await;

    assert_eq!(append.outcome, Ok(SequenceNumber::FIRST));
}

#[tokio::test(start_paused = true)]
async fn replica_that_missed_records_catches_up_when_it_returns() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    cluster.nodes()[2].set_reachable(false);
    append_numbered(&writer, 5).await;

    cluster.nodes()[2].set_reachable(true);
    settle(Duration::from_secs(1)).await;

    let segment = cluster.segment_address(1).await;
    assert_eq!(
        cluster.nodes()[2].local_end(segment).await,
        Some(SequenceNumber::new(5))
    );
}

#[tokio::test(start_paused = true)]
async fn append_is_not_sent_while_too_much_is_unacknowledged() {
    let appends = two_appends_under_flow_control().await;

    assert_eq!(appends.sent_while_full, Some(SequenceNumber::new(1)));
}

#[tokio::test(start_paused = true)]
async fn appends_held_back_by_flow_control_commit_in_order_once_a_quorum_returns() {
    let appends = two_appends_under_flow_control().await;

    assert_eq!(appends.first_outcome, Ok(SequenceNumber::new(1)));
    assert_eq!(appends.second_outcome, Ok(SequenceNumber::new(2)));
}

#[tokio::test(start_paused = true)]
async fn writer_rolls_away_from_a_replica_that_stays_down() {
    let cluster = Cluster::start(4).await;
    let writer = open_writer(&cluster, "compute-1").await;
    cluster.place_standby(&[0, 1, 3]).await;
    append_numbered(&writer, 3).await;

    cluster.nodes()[2].set_reachable(false);
    settle(WriterSettings::default().replica_failure_timeout + Duration::from_secs(1)).await;
    let next = writer.append(payload("after the roll")).await;

    let stream = cluster.stream().await;
    assert_eq!(stream.segments.len(), 2);
    assert_eq!(stream.segments[0].sealed_end(), Some(SequenceNumber::new(3)));
    assert_eq!(stream.segments[1].first_sequence, SequenceNumber::new(4));
    assert_eq!(next, Ok(SequenceNumber::new(4)));
}

#[tokio::test(start_paused = true)]
async fn records_in_flight_during_a_roll_keep_their_sequence_numbers() {
    let cluster = Cluster::start(5).await;
    let writer = Arc::new(open_writer(&cluster, "compute-1").await);
    cluster.place_standby(&[0, 3, 4]).await;
    append_numbered(&writer, 2).await;
    cluster.nodes()[1].set_reachable(false);
    cluster.nodes()[2].set_reachable(false);

    let in_flight = append_in_background(&writer, "uncommitted when the roll starts");
    let committed = in_flight.await.expect("the append's task is not cancelled");

    let stream = cluster.stream().await;
    assert_eq!(committed, Ok(SequenceNumber::new(3)));
    assert_eq!(stream.segments[0].sealed_end(), Some(SequenceNumber::new(2)));
    assert_eq!(stream.segments[1].first_sequence, SequenceNumber::new(3));
}

#[tokio::test(start_paused = true)]
async fn sealed_segment_keeps_no_record_above_its_sealed_end() {
    let cluster = Cluster::start(5).await;
    let writer = Arc::new(open_writer(&cluster, "compute-1").await);
    cluster.place_standby(&[0, 3, 4]).await;
    append_numbered(&writer, 2).await;
    cluster.nodes()[1].set_reachable(false);
    cluster.nodes()[2].set_reachable(false);

    append_in_background(&writer, "uncommitted when the roll starts")
        .await
        .expect("the append's task is not cancelled")
        .expect("the record commits on the new segment");
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
    let writer = open_writer(&cluster, "compute-1").await;
    cluster.place_standby(&[0, 1, 3]).await;
    append_numbered(&writer, 2).await;

    cluster.nodes()[2]
        .replica()
        .expect("the node is running")
        .drain()
        .await
        .expect("a running replica can be drained");
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
    let writer = open_writer_with(&cluster, "compute-1", &settings).await;
    cluster.place_standby(&[0, 1, 2]).await;

    writer
        .append(Bytes::from(vec![0xAB; 3 * 1024]))
        .await
        .expect("a write quorum is reachable");
    settle(Duration::from_secs(1)).await;

    let stream = cluster.stream().await;
    assert_eq!(stream.segments.len(), 2);
    assert_eq!(stream.segments[0].sealed_end(), Some(SequenceNumber::new(1)));
}

#[tokio::test(start_paused = true)]
async fn without_a_standby_the_writer_keeps_committing_on_its_segment() {
    let cluster = Cluster::start(3).await;
    let writer = open_writer(&cluster, "compute-1").await;
    cluster.nodes()[2].set_reachable(false);
    settle(WriterSettings::default().replica_failure_timeout + Duration::from_secs(5)).await;

    let committed = writer.append(payload("a")).await;

    assert_eq!(committed, Ok(SequenceNumber::FIRST));
    assert_eq!(cluster.stream().await.segments.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn new_writer_seals_the_old_segment_at_its_last_record() {
    let taken_over = take_over_after_five_records().await;

    let stream = taken_over.cluster.stream().await;

    assert_eq!(stream.segments[0].sealed_end(), Some(SequenceNumber::new(5)));
}

#[tokio::test(start_paused = true)]
async fn new_writer_continues_after_the_old_segment_under_the_next_epoch() {
    let taken_over = take_over_after_five_records().await;

    let next = taken_over.second_writer.append(payload("from the new writer")).await;

    let stream = taken_over.cluster.stream().await;
    assert_eq!(taken_over.second_writer.epoch(), Epoch::new(2));
    assert_eq!(stream.segments[1].epoch, Epoch::new(2));
    assert_eq!(next, Ok(SequenceNumber::new(6)));
}

#[tokio::test(start_paused = true)]
async fn old_writer_is_fenced_after_a_takeover() {
    let cluster = Cluster::start(3).await;
    let first_writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&first_writer, 2).await;
    cluster.place_standby(&[0, 1, 2]).await;
    let _second_writer = open_writer(&cluster, "compute-2").await;

    let too_late = first_writer.append(payload("too late")).await;

    assert_eq!(too_late, Err(AppendError::Fenced));
}

#[tokio::test(start_paused = true)]
async fn takeover_keeps_an_uncommitted_tail_found_on_a_responder() {
    let cluster = cluster_with_a_tail_on_one_replica().await;
    cluster.nodes()[1].set_reachable(true);
    cluster.nodes()[2].set_reachable(true);
    cluster.place_standby(&[0, 1, 2]).await;

    let second_writer = open_writer(&cluster, "compute-2").await;

    let old_segment = cluster.segment_address(1).await;
    assert_eq!(
        cluster.stream().await.segments[0].sealed_end(),
        Some(SequenceNumber::new(4))
    );
    assert_eq!(
        cluster.nodes()[1].local_end(old_segment).await,
        Some(SequenceNumber::new(4))
    );
    assert_eq!(second_writer.append(payload("next")).await, Ok(SequenceNumber::new(5)));
}

#[tokio::test(start_paused = true)]
async fn takeover_discards_a_tail_that_no_responder_holds() {
    let cluster = cluster_with_a_tail_on_one_replica().await;
    cluster.nodes()[0].set_reachable(false);
    cluster.nodes()[1].set_reachable(true);
    cluster.nodes()[2].set_reachable(true);
    cluster.place_standby(&[1, 2, 0]).await;

    let second_writer = open_writer(&cluster, "compute-2").await;

    assert_eq!(
        cluster.stream().await.segments[0].sealed_end(),
        Some(SequenceNumber::new(3))
    );
    assert_eq!(second_writer.append(payload("next")).await, Ok(SequenceNumber::new(4)));
}

#[tokio::test(start_paused = true)]
async fn takeover_fails_without_a_recovery_quorum() {
    let cluster = Cluster::start(3).await;
    let first_writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&first_writer, 3).await;
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
    let first_writer = open_writer(&cluster, "compute-1").await;
    drop(first_writer);
    cluster.place_standby(&[0, 1, 2]).await;
    let started_at = Instant::now();

    let opened = cluster
        .client("compute-2")
        .open_writer(STREAM, TakeoverPolicy::WaitForLeaseExpiry)
        .await;

    let waited = started_at.elapsed();
    assert!(opened.is_ok(), "{:?}", opened.err());
    assert!(waited >= WriterSettings::default().lease_duration, "waited {waited:?}");
}

#[tokio::test(start_paused = true)]
async fn takeover_does_not_wait_out_a_live_writer() {
    let cluster = Cluster::start(3).await;
    let _live_writer = open_writer(&cluster, "compute-1").await;
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
    let _live_writer = open_writer(&cluster, "compute-1").await;
    let first_expiry = cluster
        .stream()
        .await
        .writer
        .expect("a live writer holds a lease")
        .expires_at;

    settle(Duration::from_secs(20)).await;

    let later_expiry = cluster
        .stream()
        .await
        .writer
        .expect("a live writer holds a lease")
        .expires_at;
    assert!(later_expiry.duration_since(first_expiry) >= Duration::from_secs(15));
}

#[tokio::test(start_paused = true)]
async fn takeover_waits_for_a_standby_segment_to_be_placed() {
    let cluster = Arc::new(Cluster::start(3).await);
    let first_writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&first_writer, 2).await;
    drop(first_writer);
    let mut opening = {
        let cluster = Arc::clone(&cluster);
        tokio::spawn(async move {
            let opened = cluster
                .client("compute-2")
                .open_writer(STREAM, TakeoverPolicy::Force)
                .await;
            match opened {
                Ok(writer) => Ok(writer.epoch()),
                Err(open_error) => Err(open_error),
            }
        })
    };

    let waited = timeout(Duration::from_secs(2), &mut opening).await;
    cluster.place_standby(&[0, 1, 2]).await;
    let opened_epoch = opening.await.expect("the opening task is not cancelled");

    assert!(waited.is_err(), "the takeover did not wait for a standby segment");
    assert_eq!(opened_epoch, Ok(Epoch::new(2)));
}

#[tokio::test(start_paused = true)]
async fn takeover_gives_up_when_no_standby_is_placed_in_time() {
    let cluster = Cluster::start(3).await;
    let first_writer = open_writer(&cluster, "compute-1").await;
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
    let first_writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&first_writer, 2).await;
    drop(first_writer);
    let segment = cluster.segment_address(1).await;
    for node in cluster.nodes() {
        node.replica()
            .expect("the node is running")
            .fence(FenceRequest {
                segment,
                epoch: Epoch::new(9),
            })
            .await
            .expect("epoch 9 is above every epoch the replica has seen");
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

#[tokio::test(start_paused = true)]
async fn closed_writer_leaves_its_segment_sealed_at_the_last_record() {
    let cluster = cluster_with_a_closed_writer().await;

    let stream = cluster.stream().await;

    assert_eq!(stream.segments[0].sealed_end(), Some(SequenceNumber::new(3)));
}

#[tokio::test(start_paused = true)]
async fn closed_writer_leaves_no_lease() {
    let cluster = cluster_with_a_closed_writer().await;

    let stream = cluster.stream().await;

    assert_eq!(stream.writer, None);
}

#[tokio::test(start_paused = true)]
async fn closed_writer_tells_the_replicas_the_sealed_end() {
    let cluster = cluster_with_a_closed_writer().await;

    let segment_statuses = cluster.nodes()[0]
        .replica()
        .expect("the node is running")
        .list_segments()
        .await
        .expect("a running replica lists its segments");

    let segment = cluster.segment_address(1).await;
    let mut sealed_ends = Vec::new();
    for status in segment_statuses {
        if status.address == segment {
            sealed_ends.push(status.sealed_end);
        }
    }
    assert_eq!(sealed_ends, [Some(SequenceNumber::new(3))]);
}

#[tokio::test(start_paused = true)]
async fn writer_opened_after_a_clean_close_needs_no_recovery() {
    let cluster = Cluster::start(6).await;
    let first_writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&first_writer, 3).await;
    first_writer
        .close()
        .await
        .expect("the records commit and the segment seals");
    for old_replica in &cluster.nodes()[..3] {
        old_replica.set_reachable(false);
    }
    cluster.place_standby(&[3, 4, 5]).await;

    let second_writer = cluster
        .client("compute-2")
        .open_writer(STREAM, TakeoverPolicy::WaitForLeaseExpiry)
        .await
        .expect("a sealed segment is not recovered, so its replicas are not needed");

    assert_eq!(second_writer.append(payload("next")).await, Ok(SequenceNumber::new(4)));
    assert_eq!(cluster.stream().await.segments[1].segment_id, SegmentId::new(2));
}

#[tokio::test(start_paused = true)]
async fn close_reports_a_writer_that_was_fenced() {
    let cluster = Cluster::start(3).await;
    let first_writer = open_writer(&cluster, "compute-1").await;
    append_numbered(&first_writer, 2).await;
    cluster.place_standby(&[0, 1, 2]).await;
    let _second_writer = open_writer(&cluster, "compute-2").await;

    let closed = first_writer.close().await;

    assert_eq!(closed, Err(AppendError::Fenced));
}
