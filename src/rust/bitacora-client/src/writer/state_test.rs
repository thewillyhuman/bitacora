//! Tests of the writer's bookkeeping.
//!
//! # What is covered
//!
//! - Sequence assignment: dense numbering, and a rejected record using no number.
//! - Commit: the durable sequence as the second highest acknowledged end of three, appends resolving in order and only
//!   up to it, and acknowledgements that go backwards or run ahead.
//! - What to send: everything pending to a fresh replica, only the missing batches to one that acknowledged some, a
//!   heartbeat to one that has all, and nothing to a task of an older segment.
//! - Flow control and lag: capacity running out and returning, a replica abandoned past the lag limit.
//! - Roll triggers: a silent, refusing or regressing replica, and a segment that is full or old.
//! - Rolling: nothing commits meanwhile, an abandoned roll catches up, a completed roll resends for the new segment.
//! - Stopping and closing.
//!
//! # How the fixtures work
//!
//! [`new_state`] is a writer at the start of segment 3, whose first record is 100, with three replicas and a write
//! quorum of two. Every record is [`PAYLOAD_BYTES`] long, so byte limits can be written as multiples of it. The state
//! never reads a clock; tests that need time to pass run on a paused runtime and pass `Instant::now()`.

use std::time::Duration;

use bitacora_segment::format::decode_frames;
use bitacora_types::{Epoch, SegmentAddress, SegmentId, StreamId};
use tokio::sync::oneshot::error::TryRecvError;

use super::*;

/// The sequence number of the first record of the segment the tests start in.
const FIRST_SEQUENCE: u64 = 100;

/// The size of every record the tests append.
const PAYLOAD_BYTES: usize = 1_000;

/// A state in the middle of a roll, with the receivers of the appends made before it.
struct RollingState {
    /// The state, after `begin_roll` froze the durable sequence at 101.
    state: WriterState,
    /// The receivers of records 100 to 104, in order.
    receivers: Vec<oneshot::Receiver<CommitResult>>,
}

/// The descriptor of segment `segment_id` of stream 7, written in epoch 5, starting at `first_sequence`.
fn segment(segment_id: u64, first_sequence: u64) -> SegmentDescriptor {
    SegmentDescriptor {
        address: SegmentAddress::new(StreamId::new(7), SegmentId::new(segment_id)),
        epoch: Epoch::new(5),
        first_sequence: SequenceNumber::new(first_sequence),
    }
}

/// The replica identifier `name`.
fn replica_id(name: &str) -> ReplicaId {
    ReplicaId::new(name).expect("a valid identifier")
}

/// The replica set of the segment the tests start in.
fn first_replica_set() -> Vec<ReplicaId> {
    vec![
        replica_id("replica-a"),
        replica_id("replica-b"),
        replica_id("replica-c"),
    ]
}

/// The replica set of the segment the tests roll into.
fn next_replica_set() -> Vec<ReplicaId> {
    vec![
        replica_id("replica-d"),
        replica_id("replica-e"),
        replica_id("replica-f"),
    ]
}

/// A writer at the start of segment 3 with three replicas and a write quorum of two.
fn new_state() -> WriterState {
    WriterState::new(segment(3, FIRST_SEQUENCE), first_replica_set(), 2, Instant::now())
}

/// One record of [`PAYLOAD_BYTES`].
fn payload() -> Bytes {
    Bytes::from(vec![0xAB; PAYLOAD_BYTES])
}

/// Appends `count` single-record batches and returns their receivers, in order.
fn enqueue(state: &mut WriterState, count: usize) -> Vec<oneshot::Receiver<CommitResult>> {
    let mut receivers = Vec::new();
    for _record in 0..count {
        let (_range, receiver) = state.enqueue(&[payload()]).expect("the writer is running");
        receivers.push(receiver);
    }
    receivers
}

/// Appends one batch of `payloads` and returns the sequence numbers it got, or why it was refused.
fn enqueue_batch(state: &mut WriterState, payloads: &[Bytes]) -> Result<SequenceRange, AppendError> {
    let (range, _receiver) = state.enqueue(payloads)?;
    Ok(range)
}

/// Records that the replica at `replica_index` acknowledged everything up to `local_end`, under the default settings.
fn acknowledge(state: &mut WriterState, replica_index: usize, local_end: u64) -> bool {
    state.record_acknowledgement(
        state.generation(),
        replica_index,
        SequenceNumber::new(local_end),
        Instant::now(),
        &WriterSettings::default(),
    )
}

/// The next request for the replica at `replica_index`, without a size limit.
fn plan_everything(state: &mut WriterState, replica_index: usize) -> RequestPlan {
    state
        .plan_request(state.generation(), replica_index, usize::MAX)
        .expect("the generation is current")
}

/// The sequence numbers of the records in the next request for the replica at `replica_index`.
fn planned_sequences(state: &mut WriterState, replica_index: usize) -> Vec<u64> {
    let plan = plan_everything(state, replica_index);
    let batches = decode_frames(plan.segment.address, &plan.batch_frames).expect("valid frames");
    let mut sequences = Vec::new();
    for batch in &batches {
        for record in batch.records() {
            sequences.push(record.sequence.get());
        }
    }
    sequences
}

/// Enqueues records 100 to 104, commits 100 and 101, and starts a roll.
fn state_rolling_at_101() -> RollingState {
    let mut state = new_state();
    let receivers = enqueue(&mut state, 5);
    acknowledge(&mut state, 0, 104);
    acknowledge(&mut state, 1, 101);
    let sealed_end = state.begin_roll();
    assert_eq!(sealed_end, SequenceNumber::new(101), "the fixture rolls at 101");
    RollingState { state, receivers }
}

/// Completes the roll of `state` into segment 4, which starts at 102, on a new replica set.
fn complete_roll_into_segment_4(state: &mut WriterState) {
    state.complete_roll(segment(4, 102), next_replica_set(), Instant::now());
}

#[tokio::test]
async fn first_record_gets_the_first_sequence_of_the_segment() {
    let mut state = new_state();

    let first = enqueue_batch(&mut state, &[payload()]).expect("accepted");

    assert_eq!(first.to_string(), "100..=100");
}

#[tokio::test]
async fn records_are_numbered_densely_across_batches() {
    let mut state = new_state();
    enqueue_batch(&mut state, &[payload()]).expect("accepted");

    let second = enqueue_batch(&mut state, &[payload(), payload()]).expect("accepted");

    assert_eq!(second.to_string(), "101..=102");
}

#[tokio::test]
async fn oversized_record_is_rejected() {
    let mut state = new_state();
    let oversized = Bytes::from(vec![0; bitacora_types::MAX_RECORD_PAYLOAD_BYTES + 1]);

    let rejected = enqueue_batch(&mut state, &[oversized]);

    assert!(matches!(rejected, Err(AppendError::Rejected(_))), "{rejected:?}");
}

#[tokio::test]
async fn rejected_record_uses_no_sequence_number() {
    let mut state = new_state();
    let oversized = Bytes::from(vec![0; bitacora_types::MAX_RECORD_PAYLOAD_BYTES + 1]);
    let _rejected = enqueue_batch(&mut state, &[oversized]);

    let next = enqueue_batch(&mut state, &[payload()]).expect("accepted");

    assert_eq!(next.first(), SequenceNumber::new(FIRST_SEQUENCE));
}

#[tokio::test]
async fn nothing_commits_on_one_acknowledgement_of_three() {
    let mut state = new_state();
    let mut receivers = enqueue(&mut state, 1);

    acknowledge(&mut state, 0, 100);

    assert_eq!(receivers[0].try_recv(), Err(TryRecvError::Empty));
}

#[tokio::test]
async fn record_commits_on_the_second_acknowledgement() {
    let mut state = new_state();
    let mut receivers = enqueue(&mut state, 1);
    acknowledge(&mut state, 0, 100);

    acknowledge(&mut state, 2, 100);

    assert_eq!(receivers[0].try_recv(), Ok(Ok(())));
    assert_eq!(state.durable_sequence(), SequenceNumber::new(100));
}

#[tokio::test]
async fn durable_sequence_is_the_second_highest_acknowledged_end() {
    let mut state = new_state();
    let _receivers = enqueue(&mut state, 10);

    acknowledge(&mut state, 0, 109);
    acknowledge(&mut state, 1, 104);
    acknowledge(&mut state, 2, 101);

    assert_eq!(state.durable_sequence(), SequenceNumber::new(104));
}

#[tokio::test]
async fn appends_resolve_only_up_to_the_durable_sequence() {
    let mut state = new_state();
    let mut receivers = enqueue(&mut state, 3);

    acknowledge(&mut state, 0, 102);
    acknowledge(&mut state, 1, 101);

    let mut is_resolved = Vec::new();
    for receiver in &mut receivers {
        is_resolved.push(receiver.try_recv().is_ok());
    }
    assert_eq!(is_resolved, [true, true, false]);
}

#[tokio::test]
async fn durable_sequence_never_moves_backwards() {
    let mut state = new_state();
    let _receivers = enqueue(&mut state, 5);
    acknowledge(&mut state, 0, 104);
    acknowledge(&mut state, 1, 104);

    acknowledge(&mut state, 1, 102);

    assert_eq!(state.durable_sequence(), SequenceNumber::new(104));
}

#[tokio::test]
async fn acknowledgement_beyond_what_was_assigned_is_capped() {
    let mut state = new_state();
    let _receivers = enqueue(&mut state, 2);

    acknowledge(&mut state, 0, 500);
    acknowledge(&mut state, 1, 500);

    assert_eq!(state.durable_sequence(), SequenceNumber::new(101));
}

#[tokio::test]
async fn writer_with_uncommitted_records_is_not_drained() {
    let mut state = new_state();

    let _receivers = enqueue(&mut state, 2);

    assert!(!state.is_drained());
}

#[tokio::test]
async fn writer_is_drained_once_everything_accepted_is_durable() {
    let mut state = new_state();
    let _receivers = enqueue(&mut state, 2);

    acknowledge(&mut state, 0, 101);
    acknowledge(&mut state, 1, 101);

    assert!(state.is_drained());
}

#[tokio::test]
async fn first_acknowledgement_of_new_records_is_progress() {
    let mut state = new_state();
    let _receivers = enqueue(&mut state, 3);

    let has_progressed = acknowledge(&mut state, 0, 101);

    assert!(has_progressed);
}

#[tokio::test]
async fn repeated_acknowledgement_is_not_progress() {
    let mut state = new_state();
    let _receivers = enqueue(&mut state, 3);
    acknowledge(&mut state, 0, 101);

    let has_progressed = acknowledge(&mut state, 0, 101);

    assert!(!has_progressed);
}

#[tokio::test]
async fn acknowledgement_that_goes_backwards_is_not_progress() {
    let mut state = new_state();
    let _receivers = enqueue(&mut state, 3);
    acknowledge(&mut state, 0, 101);

    let has_progressed = acknowledge(&mut state, 0, 100);

    assert!(!has_progressed);
}

#[tokio::test]
async fn fresh_replica_is_sent_everything_pending() {
    let mut state = new_state();

    let _receivers = enqueue(&mut state, 3);

    assert_eq!(planned_sequences(&mut state, 0), [100, 101, 102]);
}

#[tokio::test]
async fn replica_is_sent_only_what_it_has_not_acknowledged() {
    let mut state = new_state();
    let _receivers = enqueue(&mut state, 3);

    acknowledge(&mut state, 0, 101);

    assert_eq!(planned_sequences(&mut state, 0), [102]);
}

#[tokio::test]
async fn caught_up_replica_gets_a_request_without_batches() {
    let mut state = new_state();
    let _receivers = enqueue(&mut state, 2);
    acknowledge(&mut state, 0, 101);
    acknowledge(&mut state, 1, 101);

    let plan = plan_everything(&mut state, 0);

    assert!(plan.batch_frames.is_empty());
}

#[tokio::test]
async fn heartbeat_carries_the_durable_sequence() {
    let mut state = new_state();
    let _receivers = enqueue(&mut state, 2);
    acknowledge(&mut state, 0, 101);
    acknowledge(&mut state, 1, 101);

    let plan = plan_everything(&mut state, 0);

    assert_eq!(plan.durable_sequence, SequenceNumber::new(101));
}

#[tokio::test]
async fn request_stops_after_the_batch_that_reaches_the_size_target() {
    let mut state = new_state();
    let _receivers = enqueue(&mut state, 5);

    let plan = state
        .plan_request(state.generation(), 0, PAYLOAD_BYTES + 100)
        .expect("the generation is current");

    let batches = decode_frames(plan.segment.address, &plan.batch_frames).expect("valid frames");
    assert_eq!(batches.len(), 2);
}

#[tokio::test]
async fn committed_batches_are_kept_for_a_replica_that_is_catching_up() {
    let mut state = new_state();
    let _receivers = enqueue(&mut state, 3);

    acknowledge(&mut state, 0, 102);
    acknowledge(&mut state, 1, 102);

    assert_eq!(planned_sequences(&mut state, 2), [100, 101, 102]);
}

#[tokio::test]
async fn replica_with_pending_batches_has_news() {
    let mut state = new_state();

    let _receivers = enqueue(&mut state, 1);

    assert!(state.has_batches_for(state.generation(), 0));
}

#[tokio::test]
async fn replica_that_acknowledged_everything_has_no_news() {
    let mut state = new_state();
    let _receivers = enqueue(&mut state, 1);

    acknowledge(&mut state, 0, 100);

    assert!(!state.has_batches_for(state.generation(), 0));
}

#[tokio::test]
async fn task_of_an_older_segment_gets_no_plan() {
    let mut state = new_state();
    let stale_generation = state.generation();
    state.begin_roll();

    state.complete_roll(segment(4, FIRST_SEQUENCE), next_replica_set(), Instant::now());

    assert_eq!(state.plan_request(stale_generation, 0, usize::MAX), None);
}

#[tokio::test]
async fn capacity_runs_out_at_the_limit() {
    let settings = WriterSettings {
        max_unacknowledged_bytes: 2 * PAYLOAD_BYTES,
        ..WriterSettings::default()
    };
    let mut state = new_state();

    let _receivers = enqueue(&mut state, 2);

    assert!(!state.has_capacity(&settings));
}

#[tokio::test]
async fn capacity_returns_when_records_commit() {
    let settings = WriterSettings {
        max_unacknowledged_bytes: 2 * PAYLOAD_BYTES,
        ..WriterSettings::default()
    };
    let mut state = new_state();
    let _receivers = enqueue(&mut state, 2);

    acknowledge(&mut state, 0, 101);
    acknowledge(&mut state, 1, 101);

    assert!(state.has_capacity(&settings));
}

/// Builds a state in which replicas a and b acknowledged records 100 to 104 one at a time and replica c never answered,
/// under a lag limit of four records.
fn state_with_a_replica_past_the_lag_limit(settings: &WriterSettings) -> WriterState {
    let mut state = new_state();
    let _receivers = enqueue(&mut state, 5);
    for acknowledged_end in FIRST_SEQUENCE..FIRST_SEQUENCE + 5 {
        for replica_index in [0, 1] {
            state.record_acknowledgement(
                state.generation(),
                replica_index,
                SequenceNumber::new(acknowledged_end),
                Instant::now(),
                settings,
            );
        }
    }
    state
}

#[tokio::test(start_paused = true)]
async fn replica_past_the_lag_limit_triggers_a_roll() {
    let settings = WriterSettings {
        max_replica_lag_bytes: 4 * PAYLOAD_BYTES,
        ..WriterSettings::default()
    };

    let state = state_with_a_replica_past_the_lag_limit(&settings);

    assert_eq!(
        state.roll_reason(Instant::now(), &settings),
        Some(RollReason::ReplicaLagging(replica_id("replica-c")))
    );
}

#[tokio::test(start_paused = true)]
async fn replica_past_the_lag_limit_is_sent_no_more_batches() {
    let settings = WriterSettings {
        max_replica_lag_bytes: 4 * PAYLOAD_BYTES,
        ..WriterSettings::default()
    };

    let mut state = state_with_a_replica_past_the_lag_limit(&settings);

    assert_eq!(planned_sequences(&mut state, 2), Vec::<u64>::new());
}

#[tokio::test(start_paused = true)]
async fn records_in_flight_to_everyone_do_not_count_as_lag() {
    let settings = WriterSettings {
        max_replica_lag_bytes: PAYLOAD_BYTES,
        ..WriterSettings::default()
    };
    let mut state = new_state();
    let _receivers = enqueue(&mut state, 5);

    state.record_acknowledgement(
        state.generation(),
        0,
        SequenceNumber::new(FIRST_SEQUENCE),
        Instant::now(),
        &settings,
    );

    assert_eq!(state.roll_reason(Instant::now(), &settings), None);
}

#[tokio::test(start_paused = true)]
async fn healthy_young_segment_does_not_roll() {
    let state = new_state();

    let roll_reason = state.roll_reason(Instant::now(), &WriterSettings::default());

    assert_eq!(roll_reason, None);
}

#[tokio::test(start_paused = true)]
async fn silent_replica_triggers_a_roll_after_the_failure_timeout() {
    let settings = WriterSettings::default();
    let mut state = new_state();
    tokio::time::advance(settings.replica_failure_timeout).await;

    for replica_index in [0, 1] {
        state.record_contact(state.generation(), replica_index, Instant::now());
    }

    assert_eq!(
        state.roll_reason(Instant::now(), &settings),
        Some(RollReason::ReplicaUnreachable(replica_id("replica-c")))
    );
}

#[tokio::test(start_paused = true)]
async fn replica_that_refuses_appends_triggers_a_roll_at_once() {
    let mut state = new_state();

    state.record_refusal(state.generation(), 1);

    assert_eq!(
        state.roll_reason(Instant::now(), &WriterSettings::default()),
        Some(RollReason::ReplicaRefused(replica_id("replica-b")))
    );
}

#[tokio::test(start_paused = true)]
async fn replica_that_lost_acknowledged_records_triggers_a_roll() {
    let mut state = new_state();
    let _receivers = enqueue(&mut state, 3);
    acknowledge(&mut state, 0, 102);

    acknowledge(&mut state, 0, 100);

    assert_eq!(
        state.roll_reason(Instant::now(), &WriterSettings::default()),
        Some(RollReason::ReplicaRefused(replica_id("replica-a")))
    );
}

#[tokio::test(start_paused = true)]
async fn segment_at_its_target_size_triggers_a_roll() {
    let settings = WriterSettings {
        target_segment_bytes: 2_000,
        ..WriterSettings::default()
    };
    let mut state = new_state();
    let _receivers = enqueue(&mut state, 2);

    acknowledge(&mut state, 0, 101);
    acknowledge(&mut state, 1, 101);

    assert_eq!(
        state.roll_reason(Instant::now(), &settings),
        Some(RollReason::SegmentFull)
    );
}

/// Rolling for size before the records commit would seal the segment without them and open the next one already full.
#[tokio::test(start_paused = true)]
async fn records_still_in_flight_do_not_count_towards_the_segment_size() {
    let settings = WriterSettings {
        target_segment_bytes: 2_000,
        ..WriterSettings::default()
    };
    let mut state = new_state();

    let _receivers = enqueue(&mut state, 2);

    assert_eq!(state.roll_reason(Instant::now(), &settings), None);
}

#[tokio::test(start_paused = true)]
async fn segment_opened_by_a_roll_starts_empty() {
    let settings = WriterSettings {
        target_segment_bytes: 2_000,
        ..WriterSettings::default()
    };
    let mut state = new_state();
    let _receivers = enqueue(&mut state, 4);
    acknowledge(&mut state, 0, 101);
    acknowledge(&mut state, 1, 101);
    state.begin_roll();

    complete_roll_into_segment_4(&mut state);

    assert_eq!(state.roll_reason(Instant::now(), &settings), None);
}

#[tokio::test(start_paused = true)]
async fn segment_at_its_target_age_triggers_a_roll() {
    let settings = WriterSettings::default();
    let mut state = new_state();
    tokio::time::advance(settings.target_segment_age).await;

    for replica_index in 0..3 {
        state.record_contact(state.generation(), replica_index, Instant::now());
    }

    assert_eq!(
        state.roll_reason(Instant::now(), &settings),
        Some(RollReason::SegmentOld)
    );
}

#[tokio::test]
async fn roll_in_flight_asks_for_no_second_roll() {
    let RollingState {
        mut state,
        receivers: _receivers,
    } = state_rolling_at_101();
    state.record_refusal(state.generation(), 0);

    let roll_reason = state.roll_reason(Instant::now(), &WriterSettings::default());

    assert_eq!(roll_reason, None);
}

#[tokio::test]
async fn durable_sequence_stands_still_while_a_roll_is_in_flight() {
    let RollingState {
        mut state,
        receivers: _receivers,
    } = state_rolling_at_101();

    acknowledge(&mut state, 1, 104);

    assert_eq!(state.durable_sequence(), SequenceNumber::new(101));
}

#[tokio::test]
async fn nothing_commits_while_a_roll_is_in_flight() {
    let RollingState {
        mut state,
        mut receivers,
    } = state_rolling_at_101();

    acknowledge(&mut state, 1, 104);

    assert_eq!(receivers[2].try_recv(), Err(TryRecvError::Empty));
}

#[tokio::test]
async fn abandoned_roll_commits_what_was_acknowledged_meanwhile() {
    let RollingState {
        mut state,
        mut receivers,
    } = state_rolling_at_101();
    acknowledge(&mut state, 1, 104);

    state.abandon_roll();

    assert_eq!(state.durable_sequence(), SequenceNumber::new(104));
    assert_eq!(receivers[4].try_recv(), Ok(Ok(())));
}

#[tokio::test]
async fn completed_roll_resends_everything_above_the_sealed_end() {
    let RollingState {
        mut state,
        receivers: _receivers,
    } = state_rolling_at_101();

    complete_roll_into_segment_4(&mut state);

    assert_eq!(planned_sequences(&mut state, 0), [102, 103, 104]);
}

#[tokio::test]
async fn completed_roll_raises_the_generation() {
    let RollingState {
        mut state,
        receivers: _receivers,
    } = state_rolling_at_101();
    let generation_before = state.generation();

    complete_roll_into_segment_4(&mut state);

    assert_eq!(state.generation(), generation_before + 1);
}

#[tokio::test]
async fn requests_after_a_roll_are_for_the_new_segment() {
    let RollingState {
        mut state,
        receivers: _receivers,
    } = state_rolling_at_101();

    complete_roll_into_segment_4(&mut state);

    assert_eq!(plan_everything(&mut state, 0).segment, segment(4, 102));
}

#[tokio::test]
async fn resent_batches_are_valid_for_the_new_segment() {
    let RollingState {
        mut state,
        receivers: _receivers,
    } = state_rolling_at_101();

    complete_roll_into_segment_4(&mut state);

    let plan = plan_everything(&mut state, 0);
    assert!(decode_frames(segment(4, 102).address, &plan.batch_frames).is_ok());
}

/// A batch checksum covers the segment's address, so a batch encoded for the old segment must not reach the new one.
#[tokio::test]
async fn resent_batches_are_not_valid_for_the_old_segment() {
    let RollingState {
        mut state,
        receivers: _receivers,
    } = state_rolling_at_101();

    complete_roll_into_segment_4(&mut state);

    let plan = plan_everything(&mut state, 0);
    assert!(decode_frames(segment(3, FIRST_SEQUENCE).address, &plan.batch_frames).is_err());
}

#[tokio::test]
async fn records_resent_after_a_roll_commit_on_the_new_replicas() {
    let RollingState {
        mut state,
        mut receivers,
    } = state_rolling_at_101();
    complete_roll_into_segment_4(&mut state);

    acknowledge(&mut state, 0, 104);
    acknowledge(&mut state, 1, 104);

    assert_eq!(receivers[4].try_recv(), Ok(Ok(())));
}

#[tokio::test]
async fn acknowledgement_from_the_old_segment_is_ignored_after_a_roll() {
    let RollingState {
        mut state,
        receivers: _receivers,
    } = state_rolling_at_101();
    let old_generation = state.generation();
    complete_roll_into_segment_4(&mut state);

    for replica_index in [0, 1] {
        state.record_acknowledgement(
            old_generation,
            replica_index,
            SequenceNumber::new(104),
            Instant::now(),
            &WriterSettings::default(),
        );
    }

    assert_eq!(state.durable_sequence(), SequenceNumber::new(101));
}

#[tokio::test]
async fn record_appended_during_a_roll_gets_the_next_sequence_number() {
    let RollingState {
        mut state,
        receivers: _receivers,
    } = state_rolling_at_101();

    let during_roll = enqueue_batch(&mut state, &[payload()]).expect("accepted");

    assert_eq!(during_roll.first(), SequenceNumber::new(105));
}

#[tokio::test]
async fn record_appended_during_a_roll_is_sent_to_the_new_segment() {
    let RollingState {
        mut state,
        receivers: _receivers,
    } = state_rolling_at_101();
    enqueue_batch(&mut state, &[payload()]).expect("accepted");

    complete_roll_into_segment_4(&mut state);

    assert_eq!(planned_sequences(&mut state, 0), [102, 103, 104, 105]);
}

#[tokio::test]
async fn stopped_writer_fails_its_pending_appends() {
    let mut state = new_state();
    let mut receivers = enqueue(&mut state, 1);

    state.stop(&AppendError::Fenced);

    assert_eq!(receivers[0].try_recv(), Ok(Err(AppendError::Fenced)));
}

#[tokio::test]
async fn stopped_writer_fails_later_appends_with_the_same_error() {
    let mut state = new_state();
    state.stop(&AppendError::Fenced);

    let later = enqueue_batch(&mut state, &[payload()]);

    assert_eq!(later, Err(AppendError::Fenced));
}

#[tokio::test]
async fn stopping_does_not_fail_what_already_committed() {
    let mut state = new_state();
    let mut receivers = enqueue(&mut state, 1);
    acknowledge(&mut state, 0, 100);
    acknowledge(&mut state, 1, 100);

    state.stop(&AppendError::Fenced);

    assert_eq!(receivers[0].try_recv(), Ok(Ok(())));
}

#[tokio::test]
async fn first_stop_reason_wins() {
    let mut state = new_state();
    state.stop(&AppendError::Fenced);

    state.stop(&AppendError::Closed);

    assert_eq!(state.status(), &Status::Stopped(AppendError::Fenced));
}

#[tokio::test]
async fn closing_writer_refuses_new_appends() {
    let mut state = new_state();
    state.begin_closing();

    let refused = enqueue_batch(&mut state, &[payload()]);

    assert_eq!(refused, Err(AppendError::Closed));
}

#[tokio::test]
async fn closing_writer_still_commits_accepted_appends() {
    let mut state = new_state();
    let mut receivers = enqueue(&mut state, 1);
    state.begin_closing();

    acknowledge(&mut state, 0, 100);
    acknowledge(&mut state, 1, 100);

    assert_eq!(receivers[0].try_recv(), Ok(Ok(())));
}

#[tokio::test]
async fn closing_does_not_revive_a_stopped_writer() {
    let mut state = new_state();
    state.stop(&AppendError::Fenced);

    state.begin_closing();

    assert_eq!(state.status(), &Status::Stopped(AppendError::Fenced));
}

#[tokio::test(start_paused = true)]
async fn stopped_writer_never_asks_to_roll() {
    let settings = WriterSettings::default();
    let mut state = new_state();
    state.stop(&AppendError::Fenced);

    tokio::time::advance(settings.replica_failure_timeout + Duration::from_secs(1)).await;

    assert_eq!(state.roll_reason(Instant::now(), &settings), None);
}
