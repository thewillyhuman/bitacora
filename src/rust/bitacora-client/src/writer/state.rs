//! The writer's bookkeeping, free of I/O (design, section 5).
//!
//! [`WriterState`] knows which records await commit, what each replica has
//! acknowledged, and therefore what is durable. The tasks around it only
//! move bytes: they ask it what to send and tell it what came back. Keeping
//! the decisions here makes them testable without a network or a clock.

use std::collections::VecDeque;

use bitacora_segment::format::{Batch, EncodeError};
use bitacora_types::replica_api::SegmentDescriptor;
use bitacora_types::{ReplicaId, SequenceNumber, SequenceRange};
use bytes::{Bytes, BytesMut};
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::error::AppendError;
use crate::settings::WriterSettings;

/// What an append resolves to once its fate is known.
pub(crate) type CommitResult = Result<(), AppendError>;

/// Whether the writer still accepts and commits records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Status {
    /// Appends are accepted and committed.
    Running,
    /// No new appends; the ones already accepted are being committed.
    Closing,
    /// The writer is done. Every append resolves to this error.
    Stopped(AppendError),
}

/// Why the writer wants to move to a new segment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RollReason {
    /// A replica has not answered for too long.
    ReplicaUnreachable(ReplicaId),
    /// A replica fell too far behind to catch up.
    ReplicaLagging(ReplicaId),
    /// A replica refused appends: it is draining, full or damaged.
    ReplicaRefused(ReplicaId),
    /// The segment reached its target size.
    SegmentFull,
    /// The segment reached its target age.
    SegmentOld,
}

/// One batch that is not yet acknowledged by every replica.
struct PendingBatch {
    range: SequenceRange,
    /// The batch, encoded for the open segment.
    batch: Batch,
    /// Resolves the append that produced the batch. Taken once it commits.
    waiter: Option<oneshot::Sender<CommitResult>>,
}

/// What the writer knows about one replica of the open segment.
#[derive(Clone, Debug)]
struct ReplicaProgress {
    replica_id: ReplicaId,
    /// The last sequence number the replica acknowledged as flushed.
    acknowledged_end: SequenceNumber,
    /// When the replica last answered anything.
    last_contact: Instant,
    /// Set when the replica refused appends and will keep refusing.
    has_refused: bool,
    /// Set when the replica fell so far behind that the batches it needs
    /// are no longer kept. It only receives heartbeats from then on.
    is_abandoned: bool,
}

/// What to send to one replica next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RequestPlan {
    /// The segment the request is for.
    pub segment: SegmentDescriptor,
    /// Back-to-back batch frames. Empty for a heartbeat.
    pub batch_frames: Bytes,
    /// The durable sequence to tell the replica.
    pub durable_sequence: SequenceNumber,
}

/// The writer's view of its stream.
pub(crate) struct WriterState {
    status: Status,
    /// Counts the segments this writer has opened. Tasks working for an
    /// older segment recognize themselves as stale by it.
    generation: u64,
    segment: SegmentDescriptor,
    segment_opened_at: Instant,
    /// How many bytes of committed batches the open segment holds. Batches
    /// still in flight do not count: rolling for size before they commit
    /// would seal the segment without them.
    committed_segment_bytes: u64,
    write_quorum: usize,
    replicas: Vec<ReplicaProgress>,
    /// The sequence number the next record gets.
    next_sequence: SequenceNumber,
    durable_sequence: SequenceNumber,
    /// While a roll's metadata write is in flight, nothing may commit: the
    /// segment is being sealed at the durable sequence it had when the roll
    /// began.
    is_rolling: bool,
    /// Batches above what every caught-up replica acknowledged, in order.
    pending: VecDeque<PendingBatch>,
    /// The bytes of pending batches above the durable sequence.
    unacknowledged_bytes: usize,
}

impl WriterState {
    /// Starts at the beginning of `segment`, which nobody has appended to.
    pub(crate) fn new(
        segment: SegmentDescriptor,
        replica_ids: Vec<ReplicaId>,
        write_quorum: usize,
        now: Instant,
    ) -> Self {
        let durable_sequence = segment.first_sequence.preceding();
        Self {
            status: Status::Running,
            generation: 0,
            segment,
            segment_opened_at: now,
            committed_segment_bytes: 0,
            write_quorum,
            replicas: fresh_progress(replica_ids, durable_sequence, now),
            next_sequence: segment.first_sequence,
            durable_sequence,
            is_rolling: false,
            pending: VecDeque::new(),
            unacknowledged_bytes: 0,
        }
    }

    pub(crate) const fn status(&self) -> &Status {
        &self.status
    }

    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) const fn segment(&self) -> SegmentDescriptor {
        self.segment
    }

    pub(crate) const fn durable_sequence(&self) -> SequenceNumber {
        self.durable_sequence
    }

    /// Whether another append fits under the flow-control limit.
    pub(crate) const fn has_capacity(&self, settings: &WriterSettings) -> bool {
        self.unacknowledged_bytes < settings.max_unacknowledged_bytes
    }

    /// Whether every accepted record is committed.
    pub(crate) fn is_drained(&self) -> bool {
        self.durable_sequence == self.next_sequence.preceding()
    }

    /// Accepts `payloads` as one atomic batch and assigns its sequence
    /// numbers. The returned receiver resolves when the batch commits or the
    /// writer stops.
    pub(crate) fn enqueue(
        &mut self,
        payloads: &[Bytes],
    ) -> Result<(SequenceRange, oneshot::Receiver<CommitResult>), AppendError> {
        match &self.status {
            Status::Running => {}
            Status::Closing => return Err(AppendError::Closed),
            Status::Stopped(error) => return Err(error.clone()),
        }
        let batch = Batch::encode(self.segment.address, self.next_sequence, payloads)?;
        let range = SequenceRange::new(batch.first_sequence(), batch.last_sequence())
            .expect("an encoded batch holds at least one record");
        let (waiter, commit_receiver) = oneshot::channel();
        self.next_sequence = range.last().next();
        self.unacknowledged_bytes += batch.frame().len();
        self.pending.push_back(PendingBatch {
            range,
            batch,
            waiter: Some(waiter),
        });
        Ok((range, commit_receiver))
    }

    /// Plans the next request to the replica at `replica_index`: the batches
    /// right after what it acknowledged, up to `max_request_bytes`, or a
    /// heartbeat if it has them all. Returns `None` if `generation` is no
    /// longer the open segment's.
    pub(crate) fn plan_request(
        &mut self,
        generation: u64,
        replica_index: usize,
        max_request_bytes: usize,
    ) -> Option<RequestPlan> {
        if generation != self.generation {
            return None;
        }
        let replica = &mut self.replicas[replica_index];
        let mut batch_frames = BytesMut::new();
        if !replica.is_abandoned {
            let mut expected = replica.acknowledged_end.next();
            for pending_batch in &self.pending {
                if pending_batch.range.last() <= replica.acknowledged_end {
                    continue;
                }
                if pending_batch.range.first() != expected {
                    // The batches this replica needs were dropped already.
                    replica.is_abandoned = true;
                    batch_frames.clear();
                    break;
                }
                batch_frames.extend_from_slice(pending_batch.batch.frame());
                expected = pending_batch.range.last().next();
                if batch_frames.len() >= max_request_bytes {
                    break;
                }
            }
        }
        Some(RequestPlan {
            segment: self.segment,
            batch_frames: batch_frames.freeze(),
            durable_sequence: self.durable_sequence,
        })
    }

    /// Whether the replica at `replica_index` is missing pending batches.
    pub(crate) fn has_batches_for(&self, generation: u64, replica_index: usize) -> bool {
        generation == self.generation
            && !self.replicas[replica_index].is_abandoned
            && self.next_sequence.preceding() > self.replicas[replica_index].acknowledged_end
    }

    /// Records that a replica acknowledged everything up to `local_end`,
    /// then commits whatever a write quorum now holds. Returns whether the
    /// replica acknowledged anything it had not acknowledged before.
    pub(crate) fn record_acknowledgement(
        &mut self,
        generation: u64,
        replica_index: usize,
        local_end: SequenceNumber,
        now: Instant,
        settings: &WriterSettings,
    ) -> bool {
        if generation != self.generation {
            return false;
        }
        let last_assigned = self.next_sequence.preceding();
        let replica = &mut self.replicas[replica_index];
        replica.last_contact = now;
        let previously_acknowledged = replica.acknowledged_end;
        if local_end < previously_acknowledged {
            // A replica must never lose what it acknowledged. If it did, its
            // copy cannot be trusted any more: roll away from it.
            replica.has_refused = true;
        } else {
            replica.acknowledged_end = local_end.min(last_assigned);
        }
        let has_progressed = replica.acknowledged_end > previously_acknowledged;
        self.advance_durable_sequence();
        self.abandon_lagging_replicas(settings);
        self.drop_fully_acknowledged_batches();
        has_progressed
    }

    /// Records that a replica answered, without acknowledging anything new.
    pub(crate) fn record_contact(&mut self, generation: u64, replica_index: usize, now: Instant) {
        if generation == self.generation {
            self.replicas[replica_index].last_contact = now;
        }
    }

    /// Records that a replica refuses appends for a reason that will last:
    /// it is draining, out of space, or its copy is damaged.
    pub(crate) fn record_refusal(&mut self, generation: u64, replica_index: usize) {
        if generation == self.generation {
            self.replicas[replica_index].has_refused = true;
        }
    }

    /// Tells whether the writer should roll to a new segment now, and why.
    pub(crate) fn roll_reason(&self, now: Instant, settings: &WriterSettings) -> Option<RollReason> {
        if self.is_rolling || matches!(self.status, Status::Stopped(_)) {
            return None;
        }
        for replica in &self.replicas {
            if replica.has_refused {
                return Some(RollReason::ReplicaRefused(replica.replica_id.clone()));
            }
            if replica.is_abandoned {
                return Some(RollReason::ReplicaLagging(replica.replica_id.clone()));
            }
            if now.duration_since(replica.last_contact) >= settings.replica_failure_timeout {
                return Some(RollReason::ReplicaUnreachable(replica.replica_id.clone()));
            }
        }
        if self.committed_segment_bytes >= settings.target_segment_bytes {
            return Some(RollReason::SegmentFull);
        }
        if now.duration_since(self.segment_opened_at) >= settings.target_segment_age {
            return Some(RollReason::SegmentOld);
        }
        None
    }

    /// Freezes the durable sequence and returns it: the position the open
    /// segment will be sealed at. Nothing commits until the roll completes
    /// or is abandoned.
    pub(crate) const fn begin_roll(&mut self) -> SequenceNumber {
        self.is_rolling = true;
        self.durable_sequence
    }

    /// Resumes committing on the open segment after a roll that did not
    /// happen, for example because no standby segment exists yet.
    pub(crate) fn abandon_roll(&mut self) {
        self.is_rolling = false;
        self.advance_durable_sequence();
        self.drop_fully_acknowledged_batches();
    }

    /// Moves to `new_segment`, which starts right after the durable
    /// sequence. Every record above it is re-encoded for the new segment and
    /// will be sent there with the same sequence number; this is safe
    /// because none of them was ever reported as committed (design, section
    /// 5, "A slow or failed replica", step 4).
    pub(crate) fn complete_roll(
        &mut self,
        new_segment: SegmentDescriptor,
        new_replica_ids: Vec<ReplicaId>,
        now: Instant,
    ) {
        debug_assert_eq!(new_segment.first_sequence, self.durable_sequence.next());
        self.pending
            .retain(|pending_batch| pending_batch.range.last() > self.durable_sequence);
        for pending_batch in &mut self.pending {
            let payloads: Vec<Bytes> = pending_batch.batch.records().map(|record| record.payload).collect();
            pending_batch.batch = Batch::encode(new_segment.address, pending_batch.range.first(), &payloads)
                .unwrap_or_else(|encode_error: EncodeError| {
                    unreachable!("a batch that was encoded once encodes again: {encode_error}")
                });
        }
        self.generation += 1;
        self.segment = new_segment;
        self.segment_opened_at = now;
        self.committed_segment_bytes = 0;
        self.replicas = fresh_progress(new_replica_ids, self.durable_sequence, now);
        self.is_rolling = false;
    }

    /// Stops accepting appends; the ones already accepted still commit.
    pub(crate) fn begin_closing(&mut self) {
        if self.status == Status::Running {
            self.status = Status::Closing;
        }
    }

    /// Stops the writer for good. Every append that has not committed
    /// resolves to `error`, and so does every later one.
    pub(crate) fn stop(&mut self, error: &AppendError) {
        if matches!(self.status, Status::Stopped(_)) {
            return;
        }
        self.status = Status::Stopped(error.clone());
        for pending_batch in &mut self.pending {
            if let Some(waiter) = pending_batch.waiter.take() {
                // The appender may have stopped waiting; that is its right.
                drop(waiter.send(Err(error.clone())));
            }
        }
        self.pending.clear();
        self.unacknowledged_bytes = 0;
    }

    /// Raises the durable sequence to the highest position a write quorum
    /// has acknowledged, and resolves the appends below it, in order.
    fn advance_durable_sequence(&mut self) {
        if self.is_rolling {
            return;
        }
        let mut acknowledged_ends: Vec<SequenceNumber> =
            self.replicas.iter().map(|replica| replica.acknowledged_end).collect();
        acknowledged_ends.sort_unstable_by(|left, right| right.cmp(left));
        let Some(quorum_end) = acknowledged_ends.get(self.write_quorum - 1).copied() else {
            return;
        };
        if quorum_end <= self.durable_sequence {
            return;
        }
        self.durable_sequence = quorum_end;
        for pending_batch in &mut self.pending {
            if pending_batch.range.last() > quorum_end {
                break;
            }
            if let Some(waiter) = pending_batch.waiter.take() {
                self.unacknowledged_bytes -= pending_batch.batch.frame().len();
                self.committed_segment_bytes += to_u64(pending_batch.batch.frame().len());
                // The appender may have stopped waiting; the record is
                // committed either way.
                drop(waiter.send(Ok(())));
            }
        }
    }

    /// Gives up on replicas that are missing more than the lag limit of
    /// committed records, so that the batches kept for them stay bounded.
    ///
    /// Lag is measured against the durable sequence, not against what was
    /// just accepted: records still in flight to everyone are not a sign
    /// that one replica is slow.
    fn abandon_lagging_replicas(&mut self, settings: &WriterSettings) {
        for replica in &mut self.replicas {
            let lag_bytes: usize = self
                .pending
                .iter()
                .filter(|pending_batch| {
                    pending_batch.range.last() > replica.acknowledged_end
                        && pending_batch.range.last() <= self.durable_sequence
                })
                .map(|pending_batch| pending_batch.batch.frame().len())
                .sum();
            if lag_bytes > settings.max_replica_lag_bytes {
                replica.is_abandoned = true;
            }
        }
    }

    /// Forgets committed batches that every caught-up replica holds.
    fn drop_fully_acknowledged_batches(&mut self) {
        let kept_from = self
            .replicas
            .iter()
            .filter(|replica| !replica.is_abandoned)
            .map(|replica| replica.acknowledged_end)
            .min()
            .unwrap_or(self.durable_sequence)
            .min(self.durable_sequence);
        while self
            .pending
            .front()
            .is_some_and(|pending_batch| pending_batch.range.last() <= kept_from)
        {
            self.pending.pop_front();
        }
    }
}

fn fresh_progress(replica_ids: Vec<ReplicaId>, acknowledged_end: SequenceNumber, now: Instant) -> Vec<ReplicaProgress> {
    replica_ids
        .into_iter()
        .map(|replica_id| ReplicaProgress {
            replica_id,
            acknowledged_end,
            last_contact: now,
            has_refused: false,
            is_abandoned: false,
        })
        .collect()
}

fn to_u64(length_bytes: usize) -> u64 {
    u64::try_from(length_bytes).expect("lengths fit in 64 bits")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bitacora_segment::format::decode_frames;
    use bitacora_types::{Epoch, SegmentAddress, SegmentId, StreamId};
    use tokio::sync::oneshot::error::TryRecvError;

    use super::*;

    const FIRST_SEQUENCE: u64 = 100;
    const PAYLOAD_BYTES: usize = 1_000;

    fn segment(segment_id: u64, first_sequence: u64) -> SegmentDescriptor {
        SegmentDescriptor {
            address: SegmentAddress::new(StreamId::new(7), SegmentId::new(segment_id)),
            epoch: Epoch::new(5),
            first_sequence: SequenceNumber::new(first_sequence),
        }
    }

    fn replica_ids(names: &[&str]) -> Vec<ReplicaId> {
        names
            .iter()
            .map(|name| ReplicaId::new(*name).expect("valid identifier"))
            .collect()
    }

    /// A writer at the start of segment 3 with three replicas and W = 2.
    fn new_state() -> WriterState {
        WriterState::new(
            segment(3, FIRST_SEQUENCE),
            replica_ids(&["replica-a", "replica-b", "replica-c"]),
            2,
            Instant::now(),
        )
    }

    fn payload() -> Bytes {
        Bytes::from(vec![0xAB; PAYLOAD_BYTES])
    }

    /// Appends `count` single-record batches and returns their receivers.
    fn enqueue(state: &mut WriterState, count: usize) -> Vec<oneshot::Receiver<CommitResult>> {
        (0..count)
            .map(|_| state.enqueue(&[payload()]).expect("the writer is running").1)
            .collect()
    }

    fn acknowledge(state: &mut WriterState, replica_index: usize, local_end: u64) -> bool {
        state.record_acknowledgement(
            state.generation(),
            replica_index,
            SequenceNumber::new(local_end),
            Instant::now(),
            &WriterSettings::default(),
        )
    }

    fn planned_sequences(state: &mut WriterState, replica_index: usize) -> Vec<u64> {
        let plan = state
            .plan_request(state.generation(), replica_index, usize::MAX)
            .expect("the generation is current");
        decode_frames(plan.segment.address, &plan.batch_frames)
            .expect("valid frames")
            .iter()
            .flat_map(Batch::records)
            .map(|record| record.sequence.get())
            .collect()
    }

    // ---- sequence assignment ----

    #[tokio::test]
    async fn records_are_numbered_densely_from_the_segment_start() {
        let mut state = new_state();
        let first = state.enqueue(&[payload()]).expect("accepted").0;
        let second = state.enqueue(&[payload(), payload()]).expect("accepted").0;
        assert_eq!(
            (first.to_string(), second.to_string()),
            ("100..=100".to_owned(), "101..=102".to_owned())
        );
    }

    #[tokio::test]
    async fn oversized_record_is_rejected_without_using_a_sequence_number() {
        let mut state = new_state();
        let oversized = Bytes::from(vec![0; bitacora_types::MAX_RECORD_PAYLOAD_BYTES + 1]);
        let rejected = state.enqueue(&[oversized]).map(|(range, _receiver)| range);
        let next = state.enqueue(&[payload()]).expect("accepted").0;
        assert!(
            matches!(rejected, Err(AppendError::Rejected(_))) && next.first() == SequenceNumber::new(FIRST_SEQUENCE)
        );
    }

    // ---- commit ----

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
        assert_eq!(
            (receivers[0].try_recv(), state.durable_sequence()),
            (Ok(Ok(())), SequenceNumber::new(100))
        );
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
        let resolved: Vec<bool> = receivers
            .iter_mut()
            .map(|receiver| receiver.try_recv().is_ok())
            .collect();
        assert_eq!(resolved, [true, true, false]);
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
    async fn writer_is_drained_once_everything_accepted_is_durable() {
        let mut state = new_state();
        let _receivers = enqueue(&mut state, 2);
        let was_drained_before = state.is_drained();
        acknowledge(&mut state, 0, 101);
        acknowledge(&mut state, 1, 101);
        assert_eq!((was_drained_before, state.is_drained()), (false, true));
    }

    #[tokio::test]
    async fn acknowledgement_reports_whether_the_replica_progressed() {
        let mut state = new_state();
        let _receivers = enqueue(&mut state, 3);
        let progressed_first = acknowledge(&mut state, 0, 101);
        let progressed_again = acknowledge(&mut state, 0, 101);
        let went_backwards = acknowledge(&mut state, 0, 100);
        assert_eq!(
            (progressed_first, progressed_again, went_backwards),
            (true, false, false)
        );
    }

    // ---- what to send ----

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
    async fn caught_up_replica_gets_a_heartbeat_with_the_durable_sequence() {
        let mut state = new_state();
        let _receivers = enqueue(&mut state, 2);
        acknowledge(&mut state, 0, 101);
        acknowledge(&mut state, 1, 101);
        let plan = state
            .plan_request(state.generation(), 0, usize::MAX)
            .expect("the generation is current");
        assert_eq!(
            (plan.batch_frames.len(), plan.durable_sequence),
            (0, SequenceNumber::new(101))
        );
    }

    #[tokio::test]
    async fn request_stops_after_the_batch_that_reaches_the_size_target() {
        let mut state = new_state();
        let _receivers = enqueue(&mut state, 5);
        let plan = state
            .plan_request(state.generation(), 0, PAYLOAD_BYTES + 100)
            .expect("the generation is current");
        let batch_count = decode_frames(plan.segment.address, &plan.batch_frames)
            .expect("valid frames")
            .len();
        assert_eq!(batch_count, 2);
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
    async fn task_of_an_older_segment_gets_no_plan() {
        let mut state = new_state();
        let stale_generation = state.generation();
        state.begin_roll();
        state.complete_roll(
            segment(4, FIRST_SEQUENCE),
            replica_ids(&["replica-d", "replica-e", "replica-f"]),
            Instant::now(),
        );
        assert_eq!(state.plan_request(stale_generation, 0, usize::MAX), None);
    }

    // ---- flow control and lag ----

    #[tokio::test]
    async fn capacity_runs_out_at_the_limit_and_returns_on_commit() {
        let settings = WriterSettings {
            max_unacknowledged_bytes: 2 * PAYLOAD_BYTES,
            ..WriterSettings::default()
        };
        let mut state = new_state();
        let _receivers = enqueue(&mut state, 2);
        let had_capacity_when_full = state.has_capacity(&settings);
        acknowledge(&mut state, 0, 101);
        acknowledge(&mut state, 1, 101);
        assert_eq!((had_capacity_when_full, state.has_capacity(&settings)), (false, true));
    }

    #[tokio::test(start_paused = true)]
    async fn replica_past_the_lag_limit_is_abandoned_and_triggers_a_roll() {
        let settings = WriterSettings {
            max_replica_lag_bytes: 4 * PAYLOAD_BYTES,
            ..WriterSettings::default()
        };
        let mut state = new_state();
        let _receivers = enqueue(&mut state, 5);
        // Replicas a and b keep up one record at a time; c never answers.
        for acknowledged_end in FIRST_SEQUENCE..FIRST_SEQUENCE + 5 {
            for replica_index in [0, 1] {
                state.record_acknowledgement(
                    state.generation(),
                    replica_index,
                    SequenceNumber::new(acknowledged_end),
                    Instant::now(),
                    &settings,
                );
            }
        }
        assert_eq!(
            (
                state.roll_reason(Instant::now(), &settings),
                planned_sequences(&mut state, 2)
            ),
            (
                Some(RollReason::ReplicaLagging(
                    ReplicaId::new("replica-c").expect("valid identifier")
                )),
                Vec::new()
            )
        );
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

    // ---- roll triggers ----

    #[tokio::test(start_paused = true)]
    async fn healthy_young_segment_does_not_roll() {
        let state = new_state();
        assert_eq!(state.roll_reason(Instant::now(), &WriterSettings::default()), None);
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
            Some(RollReason::ReplicaUnreachable(
                ReplicaId::new("replica-c").expect("valid identifier")
            ))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn replica_that_refuses_appends_triggers_a_roll_at_once() {
        let mut state = new_state();
        state.record_refusal(state.generation(), 1);
        assert_eq!(
            state.roll_reason(Instant::now(), &WriterSettings::default()),
            Some(RollReason::ReplicaRefused(
                ReplicaId::new("replica-b").expect("valid identifier")
            ))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn replica_that_lost_acknowledged_records_triggers_a_roll() {
        let mut state = new_state();
        let _receivers = enqueue(&mut state, 3);
        acknowledge(&mut state, 0, 102);
        acknowledge(&mut state, 0, 100);
        assert!(matches!(
            state.roll_reason(Instant::now(), &WriterSettings::default()),
            Some(RollReason::ReplicaRefused(_))
        ));
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

    /// Rolling for size before the records commit would seal the segment
    /// without them and open the next one already full.
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
        state.complete_roll(
            segment(4, 102),
            replica_ids(&["replica-d", "replica-e", "replica-f"]),
            Instant::now(),
        );
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

    // ---- rolling ----

    /// Enqueues five records, commits 100..=101, and starts a roll.
    fn state_rolling_at_101() -> (WriterState, Vec<oneshot::Receiver<CommitResult>>) {
        let mut state = new_state();
        let receivers = enqueue(&mut state, 5);
        acknowledge(&mut state, 0, 104);
        acknowledge(&mut state, 1, 101);
        assert_eq!(state.begin_roll(), SequenceNumber::new(101));
        (state, receivers)
    }

    #[tokio::test]
    async fn nothing_commits_while_a_roll_is_in_flight() {
        let (mut state, mut receivers) = state_rolling_at_101();
        acknowledge(&mut state, 1, 104);
        assert_eq!(
            (state.durable_sequence(), receivers[2].try_recv()),
            (SequenceNumber::new(101), Err(TryRecvError::Empty))
        );
    }

    #[tokio::test]
    async fn abandoned_roll_commits_what_was_acknowledged_meanwhile() {
        let (mut state, mut receivers) = state_rolling_at_101();
        acknowledge(&mut state, 1, 104);
        state.abandon_roll();
        assert_eq!(
            (state.durable_sequence(), receivers[4].try_recv()),
            (SequenceNumber::new(104), Ok(Ok(())))
        );
    }

    #[tokio::test]
    async fn completed_roll_resends_everything_above_the_sealed_end() {
        let (mut state, _receivers) = state_rolling_at_101();
        state.complete_roll(
            segment(4, 102),
            replica_ids(&["replica-d", "replica-e", "replica-f"]),
            Instant::now(),
        );
        assert_eq!(planned_sequences(&mut state, 0), [102, 103, 104]);
    }

    #[tokio::test]
    async fn resent_batches_are_encoded_for_the_new_segment() {
        let (mut state, _receivers) = state_rolling_at_101();
        let new_segment = segment(4, 102);
        state.complete_roll(
            new_segment,
            replica_ids(&["replica-d", "replica-e", "replica-f"]),
            Instant::now(),
        );
        let plan = state
            .plan_request(state.generation(), 0, usize::MAX)
            .expect("the generation is current");
        assert!(
            plan.segment == new_segment
                && decode_frames(new_segment.address, &plan.batch_frames).is_ok()
                && decode_frames(segment(3, FIRST_SEQUENCE).address, &plan.batch_frames).is_err()
        );
    }

    #[tokio::test]
    async fn records_resent_after_a_roll_commit_on_the_new_replicas() {
        let (mut state, mut receivers) = state_rolling_at_101();
        state.complete_roll(
            segment(4, 102),
            replica_ids(&["replica-d", "replica-e", "replica-f"]),
            Instant::now(),
        );
        acknowledge(&mut state, 0, 104);
        acknowledge(&mut state, 1, 104);
        assert_eq!(receivers[4].try_recv(), Ok(Ok(())));
    }

    #[tokio::test]
    async fn acknowledgement_from_the_old_segment_is_ignored_after_a_roll() {
        let (mut state, _receivers) = state_rolling_at_101();
        let old_generation = state.generation();
        state.complete_roll(
            segment(4, 102),
            replica_ids(&["replica-d", "replica-e", "replica-f"]),
            Instant::now(),
        );
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
    async fn appends_continue_across_a_roll_without_a_sequence_gap() {
        let (mut state, _receivers) = state_rolling_at_101();
        let during_roll = state.enqueue(&[payload()]).expect("accepted").0;
        state.complete_roll(
            segment(4, 102),
            replica_ids(&["replica-d", "replica-e", "replica-f"]),
            Instant::now(),
        );
        assert_eq!(
            (during_roll.first(), planned_sequences(&mut state, 0)),
            (SequenceNumber::new(105), vec![102, 103, 104, 105])
        );
    }

    // ---- stopping ----

    #[tokio::test]
    async fn stopped_writer_fails_pending_and_later_appends_alike() {
        let mut state = new_state();
        let mut receivers = enqueue(&mut state, 1);
        state.stop(&AppendError::Fenced);
        let later = state.enqueue(&[payload()]).map(|(range, _receiver)| range);
        assert_eq!(
            (receivers[0].try_recv(), later),
            (Ok(Err(AppendError::Fenced)), Err(AppendError::Fenced))
        );
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
    async fn closing_writer_refuses_new_appends_but_commits_accepted_ones() {
        let mut state = new_state();
        let mut receivers = enqueue(&mut state, 1);
        state.begin_closing();
        let refused = state.enqueue(&[payload()]).map(|(range, _receiver)| range);
        acknowledge(&mut state, 0, 100);
        acknowledge(&mut state, 1, 100);
        assert_eq!(
            (refused, receivers[0].try_recv()),
            (Err(AppendError::Closed), Ok(Ok(())))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stopped_writer_never_asks_to_roll() {
        let settings = WriterSettings::default();
        let mut state = new_state();
        state.stop(&AppendError::Fenced);
        tokio::time::advance(settings.replica_failure_timeout + Duration::from_secs(1)).await;
        assert_eq!(state.roll_reason(Instant::now(), &settings), None);
    }
}
