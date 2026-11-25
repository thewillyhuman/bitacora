//! The steps the modelled system can take.
//!
//! # Why this file exists
//!
//! A model checker needs two things from a system: which steps can happen in a state, and what each step
//! leads to. This file is both, for bitacora's append, roll and failover protocols. It is the executable
//! form of sections 5 and 9 of the design: every step below quotes the step of the design it stands for,
//! and a change to the protocol is a change to this file first.
//!
//! # What it contains
//!
//! - [`Action`]: one step of one actor.
//! - `enabled_actions`: every step that can happen in a state.
//! - `apply`: the state a step leads to.
//! - One private function per step, named after it, and one per actor that lists the actor's steps.
//!
//! # How it works
//!
//! The network is not a queue. A request a writer has sent can reach any replica at any later time or
//! never; an acknowledgement a replica has produced can reach the writer at any later time or never. So a
//! replica may persist any prefix of what a writer has sent beyond what it holds, and a writer may learn
//! any acknowledgement a replica has produced beyond what the writer knows. That covers message loss,
//! delay, reordering and duplication without modelling the messages themselves.
//!
//! A writer that crashes is a writer that takes no further step. There is no action for it: the checker
//! explores every order of steps, so every run in which a writer stops after any of its steps, with its
//! requests still on their way, is explored anyway. A replica that crashes and restarts keeps what it
//! flushed and forgets what it only held in memory.
//!
//! A conditional write to the metadata store is one step, because the store applies it atomically. The
//! condition is part of when the step is enabled: the first writer can only roll while the epoch is still
//! its own.
//!
//! After every step, whatever any replica would now serve to a reader is added to the observations, so
//! that the invariants can speak about every read that could have happened along the run.
//!
//! # Invariants
//!
//! - `apply` is only called with an action that `enabled_actions` listed for the same state. A step
//!   function may rely on its precondition.
//! - The order in which `enabled_actions` lists steps is fixed: first writer, then each replica in turn,
//!   then the recovering writer. The explorer visits states in that order, so the order decides which of
//!   several shortest counterexamples is reported. Reordering is not a behaviour change of the protocol,
//!   but it changes what the tests see.
//! - Both functions run for every state the explorer visits. Neither allocates: steps are appended to a
//!   vector the caller reuses, and a state is copied on the stack.
//!
//! # See also
//!
//! - `state.rs`: what a step changes.
//! - `rules.rs`: the rules and bounds consulted here.
//! - `invariants.rs`: what every state these steps produce must satisfy.
//! - `protocol_test.rs`: where the steps are tested, through whole runs of the model. This file has no test
//!   file of its own, because a step means nothing outside a run.

use crate::rules::{Bounds, RecoveryEnd, Rules};
use crate::state::{
    AbandonedSegment, Appender, Breach, Epoch, FIRST_EPOCH, REPLICA_COUNT, RecoveryPhase, SECOND_EPOCH, Sequence, State,
};

/// One step of the modelled system: something one writer or one replica does, or one message arriving.
///
/// A run of the model is a list of actions, and a counterexample is reported as one. The `Debug` form of
/// an action is how a trace is read.
///
/// # Examples
///
/// The first steps of an append that commits:
///
/// ```
/// use bitacora_model::{Action, Bounds, ProtocolModel};
///
/// let model = ProtocolModel::as_designed(Bounds::FAILOVER);
/// let trace = [
///     Action::FirstWriterAssigns,
///     Action::ReplicaPersists { replica: 0, segment: 0, through: 1 },
///     Action::ReplicaPersists { replica: 1, segment: 0, through: 1 },
///     Action::FirstWriterLearnsAcknowledgement { replica: 0, end: 1 },
///     Action::FirstWriterLearnsAcknowledgement { replica: 1, end: 1 },
/// ];
///
/// let state_after_the_commit = model.replay(&trace);
///
/// println!("{state_after_the_commit:#?}");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// The first writer assigns the next sequence number to a record and sends it to the replicas of its
    /// open segment (section 5, steps 1 and 2).
    FirstWriterAssigns,
    /// An acknowledgement reaches the first writer, which commits whatever a write quorum now holds
    /// (section 5, step 4).
    FirstWriterLearnsAcknowledgement {
        /// The replica that acknowledged.
        replica: usize,
        /// The local end it acknowledged.
        end: Sequence,
    },
    /// The first writer seals its segment at the durable sequence and opens the standby segment, in one
    /// conditional write (section 5, "A slow or failed replica").
    FirstWriterRolls,
    /// A replica receives records from the writer of a segment, persists them and acknowledges (section 5,
    /// step 3).
    ReplicaPersists {
        /// The replica that received the request.
        replica: usize,
        /// The segment the request is for.
        segment: usize,
        /// The last sequence number of the request.
        through: Sequence,
    },
    /// A replica flushes what it holds. Only a separate step when replicas are told to acknowledge before
    /// flushing.
    ReplicaFlushes {
        /// The replica that flushes.
        replica: usize,
        /// The segment whose copy it flushes.
        segment: usize,
    },
    /// A replica learns the sealed end of a segment, from a writer or from the controller, and discards
    /// what it holds above it.
    ReplicaLearnsSeal {
        /// The replica that learns it.
        replica: usize,
        /// The sealed segment.
        segment: usize,
    },
    /// A replica crashes and restarts: it keeps what it flushed and forgets the rest.
    ReplicaRestarts {
        /// The replica that restarts.
        replica: usize,
    },
    /// The recovering writer raises the epoch and reads the segment list (section 9, steps 1 and 2), then
    /// sends its fences (step 3).
    RecoveringWriterClaims,
    /// A fence reaches a replica, which persists it and answers with its local end.
    ReplicaIsFenced {
        /// The replica that is fenced.
        replica: usize,
        /// Whether its answer reaches the recovering writer in time to be counted.
        reply_reaches_writer: bool,
    },
    /// The recovering writer has enough answers and chooses the recovery end (section 9, steps 4 and 5).
    RecoveringWriterChoosesEnd,
    /// The recovering writer copies the missing tail to a responder (section 9, step 6).
    RecoveringWriterRepairs {
        /// The responder that is brought up to the recovery end.
        replica: usize,
    },
    /// The recovering writer seals the old segment with a conditional write (section 9, step 7).
    RecoveringWriterSeals,
    /// The recovering writer opens its own segment (section 9, step 8).
    RecoveringWriterOpens,
    /// The recovering writer assigns and sends a record of its own.
    RecoveringWriterAssigns,
    /// An acknowledgement reaches the recovering writer.
    RecoveringWriterLearnsAcknowledgement {
        /// The replica that acknowledged.
        replica: usize,
        /// The local end it acknowledged.
        end: Sequence,
    },
}

/// Which of the two writers of the model something belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Writer {
    /// The writer of epoch 1, which owns the stream when the model starts.
    First,
    /// The writer of epoch 2, which takes the stream over.
    Recovering,
}

/// Lists every step that can happen in a state.
///
/// The steps are appended in a fixed order: the first writer's, then each replica's in turn, then the
/// recovering writer's. Runs for every state the explorer visits and does not allocate beyond growing the
/// caller's vector.
///
/// # Arguments
///
/// * `state` - The state the steps are taken from.
/// * `rules` - The rules the actors follow. A broken rule can enable steps the design forbids.
/// * `bounds` - How much the run may still do. A bound that is used up disables the steps it limits.
/// * `actions` - Where the steps are appended. The caller clears it between states.
pub(crate) fn enabled_actions(state: &State, rules: &Rules, bounds: Bounds, actions: &mut Vec<Action>) {
    push_first_writer_actions(state, bounds, actions);
    push_replica_actions(state, rules, bounds, actions);
    push_recovering_writer_actions(state, rules, bounds, actions);
}

/// Appends the steps the first writer can take: assign a record while its bound allows, learn any
/// acknowledgement on its way, and roll once while the stream is still its own and a standby segment
/// exists.
fn push_first_writer_actions(state: &State, bounds: Bounds, actions: &mut Vec<Action>) {
    let writer = state.first_writer;
    if writer.appender.assigned_end < bounds.first_writer_records {
        actions.push(Action::FirstWriterAssigns);
    }
    push_acknowledgements_on_their_way(state, writer.appender, Writer::First, actions);
    // The roll is a conditional write: it only succeeds while the metadata store still shows the first
    // writer's epoch.
    let can_roll = bounds.first_writer_may_roll
        && writer.rolled_from.is_none()
        && state.metadata.epoch == FIRST_EPOCH
        && state.metadata.standby_segments > 0;
    if can_roll {
        actions.push(Action::FirstWriterRolls);
    }
}

/// Appends one step for every acknowledgement a writer has not seen yet: for every replica, every end
/// above what the writer knows, up to what the replica has acknowledged.
///
/// Every such end is a separate step because acknowledgements can be lost or overtaken: the writer may
/// learn of end 2 without ever having learned of end 1.
fn push_acknowledgements_on_their_way(
    state: &State,
    appender: Appender,
    learning_writer: Writer,
    actions: &mut Vec<Action>,
) {
    for replica in 0..REPLICA_COUNT {
        let known_end = appender.acknowledged_ends[replica];
        let acknowledged_end = state.copies[replica][appender.segment].acknowledged_to_owner;
        for end in known_end + 1..=acknowledged_end {
            actions.push(match learning_writer {
                Writer::First => Action::FirstWriterLearnsAcknowledgement { replica, end },
                Writer::Recovering => Action::RecoveringWriterLearnsAcknowledgement { replica, end },
            });
        }
    }
}

/// Appends the steps every replica can take: on each of its copies, then a restart, then the arrival of
/// a fence.
fn push_replica_actions(state: &State, rules: &Rules, bounds: Bounds, actions: &mut Vec<Action>) {
    let segment_count = state.segment_count();
    for replica in 0..REPLICA_COUNT {
        let mut would_lose_something_on_restart = false;
        for segment in 0..segment_count {
            would_lose_something_on_restart |= push_copy_actions(state, rules, replica, segment, actions);
        }
        // A restart that would change nothing is not a step: it would only add a loop to every state.
        if bounds.replicas_may_restart && would_lose_something_on_restart {
            actions.push(Action::ReplicaRestarts { replica });
        }
        push_fence_arrivals(state, replica, actions);
    }
}

/// Appends the steps one replica can take on its copy of one segment: persist any prefix of what the
/// segment's writer has sent beyond the copy, flush, and learn the seal.
///
/// # Returns
///
/// Whether a restart of the replica would change this copy: it holds records it has not flushed, or it
/// serves readers from a durable sequence it only keeps in memory.
fn push_copy_actions(state: &State, rules: &Rules, replica: usize, segment: usize, actions: &mut Vec<Action>) -> bool {
    let record = state.segment(segment);
    let copy = state.copies[replica][segment];
    let passes_the_fence = record.epoch >= copy.fenced_epoch || !rules.replicas_enforce_fencing;
    if !copy.knows_seal && passes_the_fence {
        for through in copy.local_end + 1..=state.offered_through(segment) {
            actions.push(Action::ReplicaPersists {
                replica,
                segment,
                through,
            });
        }
    }
    let holds_unflushed_records = copy.flushed_end < copy.local_end;
    if holds_unflushed_records {
        actions.push(Action::ReplicaFlushes { replica, segment });
    }
    if !copy.knows_seal && record.sealed_end.is_some() {
        actions.push(Action::ReplicaLearnsSeal { replica, segment });
    }
    let serves_readers_from_memory = !copy.knows_seal && copy.known_durable >= record.first_sequence;
    holds_unflushed_records || serves_readers_from_memory
}

/// Appends the arrival of the recovering writer's fence at one replica, if the fence is on its way: once
/// with the answer lost, and once with the answer counted while the writer is still collecting answers.
fn push_fence_arrivals(state: &State, replica: usize, actions: &mut Vec<Action>) {
    let Some(target_segment) = state.recovering_writer.target_segment else {
        return;
    };
    let is_fence_on_its_way = state.copies[replica][target_segment].fenced_epoch < SECOND_EPOCH;
    if !is_fence_on_its_way {
        return;
    }
    actions.push(Action::ReplicaIsFenced {
        replica,
        reply_reaches_writer: false,
    });
    let is_writer_collecting = matches!(state.recovering_writer.phase, RecoveryPhase::Fencing { .. });
    if is_writer_collecting {
        actions.push(Action::ReplicaIsFenced {
            replica,
            reply_reaches_writer: true,
        });
    }
}

/// Appends the steps the recovering writer can take, which depend on how far it has got in the failover
/// protocol.
fn push_recovering_writer_actions(state: &State, rules: &Rules, bounds: Bounds, actions: &mut Vec<Action>) {
    match state.recovering_writer.phase {
        RecoveryPhase::NotStarted => {
            // Claiming is a conditional write on the epoch the writer read.
            if bounds.recovering_writer_takes_over && state.metadata.epoch == FIRST_EPOCH {
                actions.push(Action::RecoveringWriterClaims);
            }
        }
        RecoveryPhase::Fencing { replies } => {
            let reply_count = count_replies(replies);
            // With no answer at all there is no end to choose, whatever a broken rule says.
            if reply_count >= rules.recovery_quorum && reply_count > 0 {
                actions.push(Action::RecoveringWriterChoosesEnd);
            }
        }
        RecoveryPhase::Repairing {
            recovery_end,
            responders,
        } => push_repair_actions(state, recovery_end, responders, actions),
        RecoveryPhase::Sealed { .. } => {
            if state.metadata.standby_segments > 0 {
                actions.push(Action::RecoveringWriterOpens);
            }
        }
        RecoveryPhase::Writing(appender) => {
            push_new_writer_append_actions(state, bounds, appender, actions);
        }
    }
}

/// Counts the fence answers that have reached the recovering writer.
fn count_replies(replies: [Option<Sequence>; REPLICA_COUNT]) -> usize {
    let mut reply_count = 0;
    for reply in replies {
        if reply.is_some() {
            reply_count += 1;
        }
    }
    reply_count
}

/// Appends the steps of a recovering writer that is repairing the tail: one repair for every responder
/// that is still short of the recovery end, or the seal once none is.
///
/// # Panics
///
/// Panics if the writer has no target segment, which a writer in this phase always has.
fn push_repair_actions(
    state: &State,
    recovery_end: Sequence,
    responders: [bool; REPLICA_COUNT],
    actions: &mut Vec<Action>,
) {
    let target_segment = state
        .recovering_writer
        .target_segment
        .expect("a writer that repairs has a target segment");
    let mut is_every_responder_complete = true;
    for (replica, is_responder) in responders.into_iter().enumerate() {
        let copy = state.copies[replica][target_segment];
        if is_responder && copy.local_end < recovery_end {
            is_every_responder_complete = false;
            actions.push(Action::RecoveringWriterRepairs { replica });
        }
    }
    if is_every_responder_complete {
        actions.push(Action::RecoveringWriterSeals);
    }
}

/// Appends the steps of a recovering writer that appends to its own segment: assign a record while its
/// bound allows, and learn any acknowledgement on its way.
fn push_new_writer_append_actions(state: &State, bounds: Bounds, appender: Appender, actions: &mut Vec<Action>) {
    let first_sequence = state.segment(appender.segment).first_sequence;
    let records_assigned = appender.assigned_end + 1 - first_sequence;
    if records_assigned < bounds.recovering_writer_records {
        actions.push(Action::RecoveringWriterAssigns);
    }
    push_acknowledgements_on_their_way(state, appender, Writer::Recovering, actions);
}

/// Computes the state a step leads to.
///
/// Copies the state, lets the step change the copy, and then adds whatever any replica would now serve to
/// a reader to the observations. Runs for every step the explorer tries and does not allocate.
///
/// # Arguments
///
/// * `state` - The state the step is taken from. It is not changed.
/// * `rules` - The rules the actors follow.
/// * `action` - The step. It must be one that [`enabled_actions`] lists for `state`.
///
/// # Returns
///
/// The state after the step.
///
/// # Panics
///
/// May panic if the step cannot happen in `state`, for example a seal learned for a segment that is not
/// sealed.
pub(crate) fn apply(state: &State, rules: &Rules, action: Action) -> State {
    let mut next_state = *state;
    match action {
        Action::FirstWriterAssigns => first_writer_assigns(&mut next_state),
        Action::FirstWriterLearnsAcknowledgement { replica, end } => {
            first_writer_learns_acknowledgement(&mut next_state, rules, replica, end);
        }
        Action::FirstWriterRolls => first_writer_rolls(&mut next_state),
        Action::ReplicaPersists {
            replica,
            segment,
            through,
        } => replica_persists(&mut next_state, rules, replica, segment, through),
        Action::ReplicaFlushes { replica, segment } => {
            replica_flushes(&mut next_state, replica, segment);
        }
        Action::ReplicaLearnsSeal { replica, segment } => {
            replica_learns_seal(&mut next_state, replica, segment);
        }
        Action::ReplicaRestarts { replica } => replica_restarts(&mut next_state, replica),
        Action::RecoveringWriterClaims => recovering_writer_claims(&mut next_state),
        Action::ReplicaIsFenced {
            replica,
            reply_reaches_writer,
        } => replica_is_fenced(&mut next_state, replica, reply_reaches_writer),
        Action::RecoveringWriterChoosesEnd => {
            recovering_writer_chooses_end(&mut next_state, rules);
        }
        Action::RecoveringWriterRepairs { replica } => {
            recovering_writer_repairs(&mut next_state, rules, replica);
        }
        Action::RecoveringWriterSeals => recovering_writer_seals(&mut next_state),
        Action::RecoveringWriterOpens => recovering_writer_opens(&mut next_state),
        Action::RecoveringWriterAssigns => recovering_writer_assigns(&mut next_state),
        Action::RecoveringWriterLearnsAcknowledgement { replica, end } => {
            recovering_writer_learns_acknowledgement(&mut next_state, rules, replica, end);
        }
    }
    next_state.absorb_readable_records();
    next_state
}

/// The first writer assigns the next sequence number and sends the record (section 5, steps 1 and 2).
///
/// If the recovering writer has already fenced a recovery quorum, the record is noted as assigned after
/// the takeover: invariant I4 says such a record must never be reported as committed.
fn first_writer_assigns(state: &mut State) {
    let is_after_takeover = state.has_fenced_recovery_quorum();
    let appender = &mut state.first_writer.appender;
    appender.assigned_end += 1;
    if is_after_takeover {
        state.observations.assigned_after_takeover[usize::from(appender.assigned_end)] = true;
    }
}

/// An acknowledgement reaches the first writer (section 5, step 4).
fn first_writer_learns_acknowledgement(state: &mut State, rules: &Rules, replica: usize, end: Sequence) {
    let appender = state.first_writer.appender;
    state.first_writer.appender = learn_acknowledgement(state, rules, FIRST_EPOCH, appender, replica, end);
}

/// An acknowledgement reaches the recovering writer, which by now appends to its own segment.
fn recovering_writer_learns_acknowledgement(state: &mut State, rules: &Rules, replica: usize, end: Sequence) {
    let RecoveryPhase::Writing(appender) = state.recovering_writer.phase else {
        return;
    };
    let appender = learn_acknowledgement(state, rules, SECOND_EPOCH, appender, replica, end);
    state.recovering_writer.phase = RecoveryPhase::Writing(appender);
}

/// A writer takes note of an acknowledgement and commits what a quorum now holds.
///
/// The writer raises its durable sequence to the highest position a commit quorum has acknowledged,
/// reports the records up to it as committed, and tells the replicas (section 5, steps 4 and 5).
///
/// # Arguments
///
/// * `state` - The state the step changes: the observations gain the commits, and the copies learn the
///   durable sequence.
/// * `rules` - The rules: how many acknowledgements commit, and whether replicas enforce fencing.
/// * `writer_epoch` - The epoch of the writer that learns the acknowledgement.
/// * `appender` - The writer's appender before the step.
/// * `replica` - The replica that acknowledged.
/// * `end` - The local end it acknowledged.
///
/// # Returns
///
/// The writer's appender after the step. The caller stores it back, because the two writers keep theirs in
/// different places.
fn learn_acknowledgement(
    state: &mut State,
    rules: &Rules,
    writer_epoch: Epoch,
    mut appender: Appender,
    replica: usize,
    end: Sequence,
) -> Appender {
    appender.acknowledged_ends[replica] = end;
    let quorum_end = end_acknowledged_by_quorum(appender.acknowledged_ends, rules.commit_quorum);
    if quorum_end <= appender.durable {
        return appender;
    }
    for sequence in appender.durable + 1..=quorum_end {
        state.observations.record_commit(sequence, writer_epoch);
    }
    appender.durable = quorum_end;
    publish_durable_sequence(state, rules, writer_epoch, appender.segment, quorum_end);
    appender
}

/// Returns the highest position that at least `commit_quorum` replicas have acknowledged: the
/// `commit_quorum`-th highest of the acknowledged ends.
fn end_acknowledged_by_quorum(acknowledged_ends: [Sequence; REPLICA_COUNT], commit_quorum: usize) -> Sequence {
    let mut highest_first = acknowledged_ends;
    highest_first.sort_unstable_by(|left_end, right_end| right_end.cmp(left_end));
    highest_first[commit_quorum - 1]
}

/// Tells the replicas of a segment the writer's new durable sequence.
///
/// The durable sequence travels with the next request or heartbeat. A replica that receives it serves
/// readers up to it; a replica fenced against this writer refuses it like any other request. The model
/// delivers it at once to every replica that would accept it: replicas can only serve more this way,
/// never less, so a reader disagreement cannot be missed.
fn publish_durable_sequence(state: &mut State, rules: &Rules, writer_epoch: Epoch, segment: usize, durable: Sequence) {
    for replica_copies in &mut state.copies {
        let copy = &mut replica_copies[segment];
        let passes_the_fence = writer_epoch >= copy.fenced_epoch || !rules.replicas_enforce_fencing;
        if passes_the_fence && !copy.knows_seal {
            copy.known_durable = copy.known_durable.max(durable);
        }
    }
}

/// The first writer rolls: it seals its segment at the durable sequence and opens the standby segment, in
/// one conditional write (section 5, "A slow or failed replica").
///
/// Every record above the sealed end is sent again to the new segment under the same sequence number.
/// That is safe because none of them was reported as committed. The requests already sent to the old
/// segment may still arrive there, which the state remembers as the abandoned segment.
fn first_writer_rolls(state: &mut State) {
    let old_appender = state.first_writer.appender;
    let sealed_end = old_appender.durable;
    let mut sealed_segment = state.segment(old_appender.segment);
    sealed_segment.sealed_end = Some(sealed_end);
    state.metadata.segments[old_appender.segment] = Some(sealed_segment);
    state.metadata.standby_segments -= 1;
    let new_segment = state.open_segment(FIRST_EPOCH, sealed_end + 1);
    state.first_writer.rolled_from = Some(AbandonedSegment {
        segment: old_appender.segment,
        offered_through: old_appender.assigned_end,
    });
    state.first_writer.appender = Appender {
        segment: new_segment,
        assigned_end: old_appender.assigned_end,
        acknowledged_ends: [sealed_end; REPLICA_COUNT],
        durable: sealed_end,
    };
}

/// A replica receives records up to `through` from the writer of a segment, persists them and
/// acknowledges (section 5, step 3).
///
/// Whether the replica flushes before it acknowledges is a rule. Persisting an append from an epoch the
/// replica is fenced against can only happen when replicas are told to ignore fencing, and is recorded as
/// a breach.
fn replica_persists(state: &mut State, rules: &Rules, replica: usize, segment: usize, through: Sequence) {
    let writer_epoch = state.segment(segment).epoch;
    let copy = &mut state.copies[replica][segment];
    if writer_epoch < copy.fenced_epoch {
        state
            .observations
            .breach
            .get_or_insert(Breach::FencedReplicaPersistedAnOlderEpoch);
    }
    copy.local_end = through;
    copy.acknowledged_to_owner = through;
    if rules.replicas_flush_before_acknowledging {
        copy.flushed_end = through;
    }
}

/// A replica flushes everything its copy of a segment holds.
fn replica_flushes(state: &mut State, replica: usize, segment: usize) {
    let copy = &mut state.copies[replica][segment];
    copy.flushed_end = copy.local_end;
}

/// A replica learns the sealed end of a segment and discards what it holds above it.
///
/// # Panics
///
/// Panics if the segment is not sealed in the metadata store: there is no seal to learn.
fn replica_learns_seal(state: &mut State, replica: usize, segment: usize) {
    let sealed_end = state
        .segment(segment)
        .sealed_end
        .expect("a replica can only learn a seal that was written");
    let copy = &mut state.copies[replica][segment];
    copy.local_end = copy.local_end.min(sealed_end);
    copy.flushed_end = copy.flushed_end.min(sealed_end);
    copy.knows_seal = true;
}

/// A replica crashes and restarts: every copy falls back to what was flushed, and the replica forgets the
/// durable sequences it only kept in memory.
///
/// Until the replica hears from a segment's writer again, it serves nothing from its open copy of that
/// segment. A copy whose seal the replica knows is not affected: the seal is flushed.
fn replica_restarts(state: &mut State, replica: usize) {
    for segment in 0..state.segment_count() {
        let record = state.segment(segment);
        let copy = &mut state.copies[replica][segment];
        copy.local_end = copy.flushed_end;
        if !copy.knows_seal {
            copy.known_durable = record.first_sequence - 1;
        }
    }
}

/// The recovering writer raises the epoch, reads the segment list and sends its fences (section 9, steps
/// 1 to 3).
///
/// The fences are for the last segment, the only one that can be open. From now on they may arrive at
/// any replica at any time.
fn recovering_writer_claims(state: &mut State) {
    state.metadata.epoch = SECOND_EPOCH;
    state.recovering_writer.target_segment = Some(state.last_segment_index());
    state.recovering_writer.phase = RecoveryPhase::Fencing {
        replies: [None; REPLICA_COUNT],
    };
}

/// A fence reaches a replica, which persists the new fenced epoch and answers with its local end (section
/// 9, step 3).
///
/// The answer is counted only if it reaches the writer while the writer is still collecting answers.
///
/// # Panics
///
/// Panics if the recovering writer has no target segment: no fence was ever sent.
fn replica_is_fenced(state: &mut State, replica: usize, reply_reaches_writer: bool) {
    let target_segment = state
        .recovering_writer
        .target_segment
        .expect("a fence is only sent for a target segment");
    let copy = &mut state.copies[replica][target_segment];
    copy.fenced_epoch = SECOND_EPOCH;
    if reply_reaches_writer && let RecoveryPhase::Fencing { replies } = &mut state.recovering_writer.phase {
        replies[replica] = Some(copy.local_end);
    }
}

/// The recovering writer chooses the recovery end among the answers it has (section 9, steps 4 and 5).
///
/// The design takes the highest reported local end; taking the lowest is a rule that is broken on
/// purpose. The replicas that answered become the responders that are repaired.
///
/// # Panics
///
/// Panics if no replica has answered: there is no end to choose.
fn recovering_writer_chooses_end(state: &mut State, rules: &Rules) {
    let RecoveryPhase::Fencing { replies } = state.recovering_writer.phase else {
        return;
    };
    let mut chosen_end: Option<Sequence> = None;
    for reply in replies {
        let Some(reported_end) = reply else {
            continue;
        };
        chosen_end = Some(match (chosen_end, rules.recovery_end) {
            (None, _) => reported_end,
            (Some(end_so_far), RecoveryEnd::LongestResponder) => end_so_far.max(reported_end),
            (Some(end_so_far), RecoveryEnd::ShortestResponder) => end_so_far.min(reported_end),
        });
    }
    let recovery_end = chosen_end.expect("the end is only chosen once a replica has answered");
    state.recovering_writer.phase = RecoveryPhase::Repairing {
        recovery_end,
        responders: replies.map(|reply| reply.is_some()),
    };
}

/// The recovering writer copies the missing tail to one responder, which then holds the segment up to
/// the recovery end (section 9, step 6).
///
/// # Panics
///
/// Panics if the recovering writer has no target segment, which a writer that repairs always has.
fn recovering_writer_repairs(state: &mut State, rules: &Rules, replica: usize) {
    let RecoveryPhase::Repairing { recovery_end, .. } = state.recovering_writer.phase else {
        return;
    };
    let target_segment = state
        .recovering_writer
        .target_segment
        .expect("a writer that repairs has a target segment");
    let copy = &mut state.copies[replica][target_segment];
    copy.local_end = recovery_end;
    if rules.replicas_flush_before_acknowledging {
        copy.flushed_end = recovery_end;
    }
}

/// The recovering writer seals the old segment at the recovery end with a conditional write (section 9,
/// step 7).
///
/// # Panics
///
/// Panics if the recovering writer has no target segment, which a writer that seals always has.
fn recovering_writer_seals(state: &mut State) {
    let RecoveryPhase::Repairing { recovery_end, .. } = state.recovering_writer.phase else {
        return;
    };
    let target_segment = state
        .recovering_writer
        .target_segment
        .expect("a writer that seals has a target segment");
    let mut sealed_segment = state.segment(target_segment);
    sealed_segment.sealed_end = Some(recovery_end);
    state.metadata.segments[target_segment] = Some(sealed_segment);
    state.recovering_writer.phase = RecoveryPhase::Sealed { recovery_end };
}

/// The recovering writer opens its own segment on the standby replica set, starting one past the recovery
/// end (section 9, step 8). From here on it is an ordinary writer.
fn recovering_writer_opens(state: &mut State) {
    let RecoveryPhase::Sealed { recovery_end } = state.recovering_writer.phase else {
        return;
    };
    state.metadata.standby_segments -= 1;
    let new_segment = state.open_segment(SECOND_EPOCH, recovery_end + 1);
    state.recovering_writer.phase = RecoveryPhase::Writing(Appender {
        segment: new_segment,
        assigned_end: recovery_end,
        acknowledged_ends: [recovery_end; REPLICA_COUNT],
        durable: recovery_end,
    });
}

/// The recovering writer assigns the next sequence number to a record of its own and sends it.
fn recovering_writer_assigns(state: &mut State) {
    if let RecoveryPhase::Writing(appender) = &mut state.recovering_writer.phase {
        appender.assigned_end += 1;
    }
}
