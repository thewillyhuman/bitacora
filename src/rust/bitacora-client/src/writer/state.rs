//! The writer's bookkeeping, free of I/O.
//!
//! # Why this file exists
//!
//! The write path is concurrent: callers append, one task per replica sends, and a driver renews the lease and rolls
//! segments. What they have to agree on is small and exact: which sequence number comes next, which batches still await
//! commit, what each replica has acknowledged, and therefore what is durable (design, section 5). If that knowledge
//! were spread over the tasks, every one of them would hold a piece of the commit rule. Here it is one value behind one
//! lock, and the rule is plain synchronous code that can be tested without a network, a clock or a runtime.
//!
//! # What it contains
//!
//! - [`WriterState`]: the writer's view of its stream, and every decision made from it.
//! - [`Status`]: whether the writer still accepts and commits records.
//! - [`RollReason`]: why the writer wants a new segment.
//! - [`RequestPlan`]: what to send to one replica next.
//! - [`CommitResult`]: what an append resolves to.
//!
//! # How it works
//!
//! **Appending.** `enqueue` encodes the caller's records as one batch frame for the open segment, gives them the next
//! sequence numbers and keeps the batch in `pending`, with a one-shot channel that will tell the caller its fate.
//!
//! **Sending.** A sender asks `plan_request` for its replica: the answer is every pending batch right after what that
//! replica acknowledged, up to a size target, or an empty request (a heartbeat) if it has them all. The sender reports
//! the replica's answer with `record_acknowledgement`.
//!
//! **Committing.** The durable sequence is the W-th highest acknowledged end. Each acknowledgement recomputes it,
//! resolves the appends at or below it, in order, and forgets the batches that every replica still in the race holds.
//!
//! **Rolling.** `roll_reason` says when to leave the segment. `begin_roll` freezes the durable sequence: the segment is
//! about to be sealed there, so nothing may commit on it any more. `complete_roll` re-encodes every batch above the
//! sealed end for the new segment, under the same sequence numbers, and starts counting acknowledgements afresh;
//! `abandon_roll` resumes on the old segment if the roll did not happen. Each segment the writer opens has a
//! *generation*; a task that still works for an older one is ignored.
//!
//! **Stopping.** `stop` fails every append that has not committed with one error, and every later one too.
//!
//! # Invariants
//!
//! - Sequence numbers are assigned densely and never reused, across rolls too.
//! - The durable sequence never decreases, and an append is reported as committed only at or below it.
//! - Appends resolve in sequence order.
//! - Nothing commits between `begin_roll` and the end of the roll.
//! - Nothing here awaits, sleeps or reads a clock: callers pass `now`. That is what keeps this file testable as pure
//!   code and the lock around it short.
//! - `enqueue`, `plan_request` and `record_acknowledgement` are the hot path of every append. They do one pass over the
//!   pending batches at most, encode a record once, and allocate only the batch frame and the request buffer. They are
//!   kept as single functions with explicit loops for that reason.
//!
//! # See also
//!
//! - `../writer.rs`: the handle whose appends end up here.
//! - `sender.rs` and `driver.rs`: the tasks that call this.
//! - `docs/protocol/replication.md`, "A writer": the rules in protocol terms.

use std::collections::VecDeque;

use bitacora_segment::format::Batch;
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
    /// The sequence numbers of the batch's records.
    range: SequenceRange,
    /// The batch, encoded for the open segment.
    batch: Batch,
    /// Resolves the append that produced the batch. Taken once it commits.
    waiter: Option<oneshot::Sender<CommitResult>>,
}

/// What the writer knows about one replica of the open segment.
#[derive(Clone, Debug)]
struct ReplicaProgress {
    /// Which replica this is, for the reason of a roll.
    replica_id: ReplicaId,
    /// The last sequence number the replica acknowledged as flushed.
    acknowledged_end: SequenceNumber,
    /// When the replica last answered anything.
    last_contact: Instant,
    /// Set when the replica refused appends and will keep refusing.
    has_refused: bool,
    /// Set when the replica fell so far behind that the batches it needs are no longer kept. It only receives
    /// heartbeats from then on.
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

/// The writer's view of its stream. See the file header for how the pieces work together.
pub(crate) struct WriterState {
    /// Whether appends are accepted, only drained, or failed.
    status: Status,
    /// Counts the segments this writer has opened. Tasks working for an older segment recognize themselves as stale by
    /// it.
    generation: u64,
    /// The open segment.
    segment: SegmentDescriptor,
    /// When the open segment was opened, for the age limit.
    segment_opened_at: Instant,
    /// How many bytes of committed batches the open segment holds. Batches still in flight do not count: rolling for
    /// size before they commit would seal the segment without them.
    committed_segment_bytes: u64,
    /// How many acknowledgements commit a record (W).
    write_quorum: usize,
    /// The replicas of the open segment, in the order of its replica set. Senders address them by index.
    replicas: Vec<ReplicaProgress>,
    /// The sequence number the next record gets.
    next_sequence: SequenceNumber,
    /// The highest sequence number such that every record at or below it is committed.
    durable_sequence: SequenceNumber,
    /// While a roll's metadata write is in flight, nothing may commit: the segment is being sealed at the durable
    /// sequence it had when the roll began.
    is_rolling: bool,
    /// Batches above what every caught-up replica acknowledged, in sequence order.
    pending: VecDeque<PendingBatch>,
    /// The bytes of pending batches above the durable sequence: what flow control limits.
    unacknowledged_bytes: usize,
}

impl WriterState {
    /// Starts at the beginning of `segment`, which nobody has appended to.
    ///
    /// # Arguments
    ///
    /// * `segment` - The segment the writer just opened.
    /// * `replica_ids` - The segment's replica set, in the order senders will index it.
    /// * `write_quorum` - How many acknowledgements commit a record. Must be at least 1.
    /// * `now` - The current instant, from which the segment's age and the replicas' silence are measured.
    ///
    /// # Returns
    ///
    /// A running state with nothing pending, whose durable sequence is the position before the segment's first record.
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

    /// Whether the writer is running, closing or stopped.
    ///
    /// # Returns
    ///
    /// The status. A stopped writer's status holds the error every pending and later append resolves to.
    pub(crate) const fn status(&self) -> &Status {
        &self.status
    }

    /// The generation of the open segment.
    ///
    /// # Returns
    ///
    /// 0 for the segment the writer opened with, and one more after every completed roll. A task compares it with the
    /// generation it was started for to learn that its segment is no longer the open one.
    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    /// The open segment.
    ///
    /// # Returns
    ///
    /// The descriptor of the segment records are being appended to: its address, epoch and first sequence number.
    pub(crate) const fn segment(&self) -> SegmentDescriptor {
        self.segment
    }

    /// The highest sequence number such that every record at or below it is committed.
    ///
    /// # Returns
    ///
    /// The durable sequence. The position before the segment's first record until something commits.
    pub(crate) const fn durable_sequence(&self) -> SequenceNumber {
        self.durable_sequence
    }

    /// Whether another append fits under the flow-control limit.
    ///
    /// # Arguments
    ///
    /// * `settings` - The writer's settings; `max_unacknowledged_bytes` is the limit.
    ///
    /// # Returns
    ///
    /// `true` while fewer bytes than the limit await commit. The append that crosses the limit is still accepted, so
    /// one record larger than the limit never blocks forever.
    pub(crate) const fn has_capacity(&self, settings: &WriterSettings) -> bool {
        self.unacknowledged_bytes < settings.max_unacknowledged_bytes
    }

    /// Whether every accepted record is committed.
    ///
    /// # Returns
    ///
    /// `true` if no sequence number was assigned above the durable sequence, which is when a closing writer may seal.
    pub(crate) fn is_drained(&self) -> bool {
        self.durable_sequence == self.next_sequence.preceding()
    }

    /// Accepts `payloads` as one atomic batch and assigns its sequence numbers.
    ///
    /// The records are encoded once, as one batch frame for the open segment, and wait in the pending queue until a
    /// write quorum has acknowledged them.
    ///
    /// # Arguments
    ///
    /// * `payloads` - The records of the batch, at least one.
    ///
    /// # Returns
    ///
    /// The sequence numbers the records got, and a receiver that resolves when the batch commits or the writer stops.
    ///
    /// # Errors
    ///
    /// * [`AppendError::Closed`] if the writer is closing.
    /// * The error the writer stopped with, if it is stopped.
    /// * [`AppendError::Rejected`] if there is no record or a size limit is exceeded. No sequence number is used.
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

    /// Plans the next request to one replica: the batches right after what it acknowledged, or a heartbeat if it has
    /// them all.
    ///
    /// A replica that needs a batch which was dropped already is marked abandoned here and gets heartbeats from then
    /// on; the driver rolls away from it.
    ///
    /// # Arguments
    ///
    /// * `generation` - The generation of the segment the calling sender works for.
    /// * `replica_index` - The replica's position in the segment's replica set.
    /// * `max_request_bytes` - The size target. The request ends after the batch that reaches it, so it always carries
    ///   at least one batch if the replica lacks any.
    ///
    /// # Returns
    ///
    /// The request to send, or `None` if `generation` is no longer the open segment's: the sender is stale and should
    /// end.
    ///
    /// # Panics
    ///
    /// Panics if `replica_index` is not an index into the replica set the state was given.
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

    /// Whether the replica at `replica_index` is missing pending batches that it can still be sent.
    ///
    /// # Arguments
    ///
    /// * `generation` - The generation of the segment the calling sender works for.
    /// * `replica_index` - The replica's position in the segment's replica set.
    ///
    /// # Returns
    ///
    /// `false` for a stale generation and for an abandoned replica, which only gets heartbeats.
    ///
    /// # Panics
    ///
    /// Panics if `replica_index` is not an index into the replica set the state was given.
    pub(crate) fn has_batches_for(&self, generation: u64, replica_index: usize) -> bool {
        generation == self.generation
            && !self.replicas[replica_index].is_abandoned
            && self.next_sequence.preceding() > self.replicas[replica_index].acknowledged_end
    }

    /// Records that a replica acknowledged everything up to `local_end`, then commits whatever a write quorum now
    /// holds.
    ///
    /// A replica that reports less than it acknowledged before has lost records it vouched for: its copy cannot be
    /// trusted any more, and it is marked as refusing so that the driver rolls away from it.
    ///
    /// # Arguments
    ///
    /// * `generation` - The generation of the segment the calling sender works for. A stale one is ignored.
    /// * `replica_index` - The replica's position in the segment's replica set.
    /// * `local_end` - The last sequence number the replica reports as flushed. Capped at the last one assigned.
    /// * `now` - The current instant: the replica answered, so its silence starts anew.
    /// * `settings` - The writer's settings; `max_replica_lag_bytes` decides which replicas are given up.
    ///
    /// # Returns
    ///
    /// Whether the replica acknowledged anything it had not acknowledged before.
    ///
    /// # Panics
    ///
    /// Panics if `replica_index` is not an index into the replica set the state was given.
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
            // A replica must never lose what it acknowledged. If it did, its copy cannot be trusted any more: roll
            // away from it.
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
    ///
    /// # Arguments
    ///
    /// * `generation` - The generation of the segment the calling sender works for. A stale one is ignored.
    /// * `replica_index` - The replica's position in the segment's replica set.
    /// * `now` - The current instant.
    ///
    /// # Panics
    ///
    /// Panics if `replica_index` is not an index into the replica set the state was given.
    pub(crate) fn record_contact(&mut self, generation: u64, replica_index: usize, now: Instant) {
        if generation == self.generation {
            self.replicas[replica_index].last_contact = now;
        }
    }

    /// Records that a replica refuses appends for a reason that will last: it is draining, out of space, or its copy is
    /// damaged.
    ///
    /// # Arguments
    ///
    /// * `generation` - The generation of the segment the calling sender works for. A stale one is ignored.
    /// * `replica_index` - The replica's position in the segment's replica set.
    ///
    /// # Panics
    ///
    /// Panics if `replica_index` is not an index into the replica set the state was given.
    pub(crate) fn record_refusal(&mut self, generation: u64, replica_index: usize) {
        if generation == self.generation {
            self.replicas[replica_index].has_refused = true;
        }
    }

    /// Tells whether the writer should roll to a new segment now, and why.
    ///
    /// Replicas are looked at first, in the order of the replica set: one that refuses, one that was abandoned for
    /// lagging, one that has been silent for the failure timeout. Then the segment itself: its committed size, its age.
    ///
    /// # Arguments
    ///
    /// * `now` - The current instant.
    /// * `settings` - The writer's settings: the failure timeout and the segment's target size and age.
    ///
    /// # Returns
    ///
    /// The first reason found, or `None` if the segment is fine, a roll is already in flight, or the writer is stopped.
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

    /// Freezes the durable sequence for a roll.
    ///
    /// Nothing commits until the roll completes or is abandoned: the open segment is about to be sealed at the position
    /// returned, and a record committed above it afterwards would be outside the sealed segment.
    ///
    /// # Returns
    ///
    /// The durable sequence: the position the open segment will be sealed at.
    pub(crate) const fn begin_roll(&mut self) -> SequenceNumber {
        self.is_rolling = true;
        self.durable_sequence
    }

    /// Whether a roll is under way: it began and has neither completed nor been abandoned.
    ///
    /// A sender asks before it reads a replica's "sealed" as the loss of the stream. During a roll the open segment is
    /// being sealed by the writer itself, and its replicas may learn that before the writer has moved on.
    pub(crate) const fn is_rolling(&self) -> bool {
        self.is_rolling
    }

    /// Resumes committing on the open segment after a roll that did not happen, for example because no standby segment
    /// exists yet. Whatever a write quorum acknowledged in the meantime commits at once.
    pub(crate) fn abandon_roll(&mut self) {
        self.is_rolling = false;
        self.advance_durable_sequence();
        self.drop_fully_acknowledged_batches();
    }

    /// Moves to `new_segment`, which starts right after the durable sequence.
    ///
    /// Every record above the durable sequence is re-encoded for the new segment and will be sent there with the same
    /// sequence number. This is safe because none of them was ever reported as committed (design, section 5, "A slow or
    /// failed replica", step 4). The generation goes up by one, so tasks of the old segment become stale.
    ///
    /// # Arguments
    ///
    /// * `new_segment` - The segment the roll opened. Its first sequence number must follow the durable sequence.
    /// * `new_replica_ids` - Its replica set, in the order the new senders will index it.
    /// * `now` - The current instant, from which the new segment's age and its replicas' silence are measured.
    ///
    /// # Panics
    ///
    /// Panics in debug builds if `new_segment` does not start right after the durable sequence.
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
                .expect("a batch that was encoded once encodes again");
        }
        self.generation += 1;
        self.segment = new_segment;
        self.segment_opened_at = now;
        self.committed_segment_bytes = 0;
        self.replicas = fresh_progress(new_replica_ids, self.durable_sequence, now);
        self.is_rolling = false;
    }

    /// Stops accepting appends; the ones already accepted still commit. Does nothing unless the writer is running.
    pub(crate) fn begin_closing(&mut self) {
        if self.status == Status::Running {
            self.status = Status::Closing;
        }
    }

    /// Stops the writer for good.
    ///
    /// Every append that has not committed resolves to `error`, and so does every later one. The first stop wins: a
    /// writer that is stopped already keeps its error.
    ///
    /// # Arguments
    ///
    /// * `error` - What the appends that never committed are told.
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

    /// Raises the durable sequence to the highest position a write quorum has acknowledged, and resolves the appends at
    /// or below it, in order. Does nothing while a roll is in flight.
    fn advance_durable_sequence(&mut self) {
        if self.is_rolling {
            return;
        }
        let Some(quorum_end) = self.quorum_acknowledged_end() else {
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
                // The appender may have stopped waiting; the record is committed either way.
                drop(waiter.send(Ok(())));
            }
        }
    }

    /// The W-th highest acknowledged end among the replicas: the furthest position that a write quorum holds. `None` if
    /// the segment has fewer replicas than the write quorum, in which case nothing can commit.
    fn quorum_acknowledged_end(&self) -> Option<SequenceNumber> {
        let mut acknowledged_ends: Vec<SequenceNumber> =
            self.replicas.iter().map(|replica| replica.acknowledged_end).collect();
        acknowledged_ends.sort_unstable_by(|left, right| right.cmp(left));
        acknowledged_ends.get(self.write_quorum - 1).copied()
    }

    /// Gives up on replicas that are missing more than the lag limit of committed records, so that the batches kept for
    /// them stay bounded.
    ///
    /// Lag is measured against the durable sequence, not against what was just accepted: records still in flight to
    /// everyone are not a sign that one replica is slow.
    fn abandon_lagging_replicas(&mut self, settings: &WriterSettings) {
        for replica in &mut self.replicas {
            let mut lag_bytes = 0;
            for pending_batch in &self.pending {
                let batch_end = pending_batch.range.last();
                let is_committed_and_missing =
                    batch_end > replica.acknowledged_end && batch_end <= self.durable_sequence;
                if is_committed_and_missing {
                    lag_bytes += pending_batch.batch.frame().len();
                }
            }
            if lag_bytes > settings.max_replica_lag_bytes {
                replica.is_abandoned = true;
            }
        }
    }

    /// Forgets committed batches that every replica still in the race holds. An abandoned replica keeps nothing alive:
    /// it will never be sent these batches.
    fn drop_fully_acknowledged_batches(&mut self) {
        let mut kept_from = self.durable_sequence;
        for replica in &self.replicas {
            if !replica.is_abandoned {
                kept_from = kept_from.min(replica.acknowledged_end);
            }
        }
        while let Some(oldest_batch) = self.pending.front() {
            if oldest_batch.range.last() > kept_from {
                break;
            }
            self.pending.pop_front();
        }
    }
}

/// The progress of the replicas of a segment nobody has sent anything to yet: each has acknowledged everything up to
/// `acknowledged_end`, the position before the segment's first record, and was last heard from `now`.
fn fresh_progress(replica_ids: Vec<ReplicaId>, acknowledged_end: SequenceNumber, now: Instant) -> Vec<ReplicaProgress> {
    let mut replicas = Vec::with_capacity(replica_ids.len());
    for replica_id in replica_ids {
        replicas.push(ReplicaProgress {
            replica_id,
            acknowledged_end,
            last_contact: now,
            has_refused: false,
            is_abandoned: false,
        });
    }
    replicas
}

/// Converts a length in memory to the 64-bit count that segment sizes use.
fn to_u64(length_bytes: usize) -> u64 {
    u64::try_from(length_bytes).expect("lengths fit in 64 bits")
}

#[cfg(test)]
#[path = "state_test.rs"]
mod tests;
