//! The steps the modelled system can take.
//!
//! The network is not a queue. A request a writer has sent can reach any
//! replica at any later time or never; an acknowledgement a replica has
//! produced can reach the writer at any later time or never. That covers
//! message loss, delay, reordering and duplication without modelling the
//! messages themselves.
//!
//! A writer that crashes is a writer that takes no further step. There is
//! no action for it: the checker explores every order of steps, so every
//! run in which a writer stops after any of its steps, with its requests
//! still on their way, is explored anyway. A replica that crashes and
//! restarts keeps what it flushed and forgets what it only held in memory.

use crate::rules::{Bounds, RecoveryEnd, Rules};
use crate::state::{
    Appender, Breach, Epoch, FIRST_EPOCH, REPLICA_COUNT, RecoveryPhase, SECOND_EPOCH, Sequence,
    State,
};

/// One step of the system.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// The first writer assigns the next sequence number to a record and
    /// sends it to the replicas of its open segment (section 5, steps 1
    /// and 2).
    FirstWriterAssigns,
    /// An acknowledgement reaches the first writer, which commits whatever
    /// a write quorum now holds (section 5, step 4).
    FirstWriterLearnsAcknowledgement {
        /// The replica that acknowledged.
        replica: usize,
        /// The local end it acknowledged.
        end: Sequence,
    },
    /// The first writer seals its segment at the durable sequence and opens
    /// the standby segment, in one conditional write (section 5, "A slow or
    /// failed replica").
    FirstWriterRolls,
    /// A replica receives records from the writer of a segment, persists
    /// them and acknowledges (section 5, step 3).
    ReplicaPersists {
        /// The replica that received the request.
        replica: usize,
        /// The segment the request is for.
        segment: usize,
        /// The last sequence number of the request.
        through: Sequence,
    },
    /// A replica flushes what it holds. Only a separate step when replicas
    /// are told to acknowledge before flushing.
    ReplicaFlushes {
        /// The replica that flushes.
        replica: usize,
        /// The segment whose copy it flushes.
        segment: usize,
    },
    /// A replica learns the sealed end of a segment, from a writer or from
    /// the controller, and discards what it holds above it.
    ReplicaLearnsSeal {
        /// The replica that learns it.
        replica: usize,
        /// The sealed segment.
        segment: usize,
    },
    /// A replica crashes and restarts: it keeps what it flushed and forgets
    /// the rest.
    ReplicaRestarts {
        /// The replica that restarts.
        replica: usize,
    },
    /// The recovering writer raises the epoch and reads the segment list
    /// (section 9, steps 1 and 2), then sends its fences (step 3).
    RecoveringWriterClaims,
    /// A fence reaches a replica, which persists it and answers with its
    /// local end.
    ReplicaIsFenced {
        /// The replica that is fenced.
        replica: usize,
        /// Whether its answer reaches the recovering writer in time to be
        /// counted.
        reply_reaches_writer: bool,
    },
    /// The recovering writer has enough answers and chooses the recovery
    /// end (section 9, steps 4 and 5).
    RecoveringWriterChoosesEnd,
    /// The recovering writer copies the missing tail to a responder
    /// (section 9, step 6).
    RecoveringWriterRepairs {
        /// The responder that is brought up to the recovery end.
        replica: usize,
    },
    /// The recovering writer seals the old segment with a conditional
    /// write (section 9, step 7).
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

/// Appends to `actions` every step that can happen in `state`.
pub(crate) fn enabled_actions(
    state: &State,
    rules: &Rules,
    bounds: Bounds,
    actions: &mut Vec<Action>,
) {
    first_writer_actions(state, bounds, actions);
    replica_actions(state, rules, bounds, actions);
    recovering_writer_actions(state, rules, bounds, actions);
}

fn first_writer_actions(state: &State, bounds: Bounds, actions: &mut Vec<Action>) {
    let writer = state.first_writer;
    if writer.appender.assigned_end < bounds.first_writer_records {
        actions.push(Action::FirstWriterAssigns);
    }
    for (replica, end) in acknowledgements_on_their_way(state, writer.appender) {
        actions.push(Action::FirstWriterLearnsAcknowledgement { replica, end });
    }
    let can_roll = bounds.first_writer_may_roll
        && writer.rolled_from.is_none()
        && state.metadata.epoch == FIRST_EPOCH
        && state.metadata.standby_segments > 0;
    if can_roll {
        actions.push(Action::FirstWriterRolls);
    }
}

/// The acknowledgements a writer has not seen yet: for every replica, every
/// end above what the writer knows, up to what the replica acknowledged.
fn acknowledgements_on_their_way(
    state: &State,
    appender: Appender,
) -> impl Iterator<Item = (usize, Sequence)> + '_ {
    (0..REPLICA_COUNT).flat_map(move |replica| {
        let known_end = appender.acknowledged_ends[replica];
        let acknowledged_end = state.copies[replica][appender.segment].acknowledged_to_owner;
        (known_end + 1..=acknowledged_end).map(move |end| (replica, end))
    })
}

fn replica_actions(state: &State, rules: &Rules, bounds: Bounds, actions: &mut Vec<Action>) {
    for replica in 0..REPLICA_COUNT {
        let mut would_lose_something_on_restart = false;
        for (segment, record) in state.segments() {
            let copy = state.copies[replica][segment];
            let passes_the_fence =
                record.epoch >= copy.fenced_epoch || !rules.replicas_enforce_fencing;
            if !copy.knows_seal && passes_the_fence {
                for through in copy.local_end + 1..=state.offered_through(segment) {
                    actions.push(Action::ReplicaPersists {
                        replica,
                        segment,
                        through,
                    });
                }
            }
            if copy.flushed_end < copy.local_end {
                actions.push(Action::ReplicaFlushes { replica, segment });
            }
            if !copy.knows_seal && record.sealed_end.is_some() {
                actions.push(Action::ReplicaLearnsSeal { replica, segment });
            }
            would_lose_something_on_restart |= copy.flushed_end < copy.local_end
                || (!copy.knows_seal && copy.known_durable >= record.first_sequence);
        }
        if bounds.replicas_may_restart && would_lose_something_on_restart {
            actions.push(Action::ReplicaRestarts { replica });
        }

        let is_fence_on_its_way = state
            .recovering_writer
            .target_segment
            .is_some_and(|target| state.copies[replica][target].fenced_epoch < SECOND_EPOCH);
        if is_fence_on_its_way {
            actions.push(Action::ReplicaIsFenced {
                replica,
                reply_reaches_writer: false,
            });
            let is_writer_collecting =
                matches!(state.recovering_writer.phase, RecoveryPhase::Fencing { .. });
            if is_writer_collecting {
                actions.push(Action::ReplicaIsFenced {
                    replica,
                    reply_reaches_writer: true,
                });
            }
        }
    }
}

fn recovering_writer_actions(
    state: &State,
    rules: &Rules,
    bounds: Bounds,
    actions: &mut Vec<Action>,
) {
    let writer = state.recovering_writer;
    match writer.phase {
        RecoveryPhase::NotStarted => {
            if bounds.recovering_writer_takes_over && state.metadata.epoch == FIRST_EPOCH {
                actions.push(Action::RecoveringWriterClaims);
            }
        }
        RecoveryPhase::Fencing { replies } => {
            let reply_count = replies.iter().flatten().count();
            if reply_count >= rules.recovery_quorum && reply_count > 0 {
                actions.push(Action::RecoveringWriterChoosesEnd);
            }
        }
        RecoveryPhase::Repairing {
            recovery_end,
            responders,
        } => {
            let target = writer
                .target_segment
                .expect("a writer that repairs has a target segment");
            let mut is_every_responder_complete = true;
            for (replica, is_responder) in responders.into_iter().enumerate() {
                let copy = state.copies[replica][target];
                if is_responder && copy.local_end < recovery_end {
                    is_every_responder_complete = false;
                    actions.push(Action::RecoveringWriterRepairs { replica });
                }
            }
            if is_every_responder_complete {
                actions.push(Action::RecoveringWriterSeals);
            }
        }
        RecoveryPhase::Sealed { .. } => {
            if state.metadata.standby_segments > 0 {
                actions.push(Action::RecoveringWriterOpens);
            }
        }
        RecoveryPhase::Writing(appender) => {
            let first_sequence = state.segment(appender.segment).first_sequence;
            let records_assigned = appender.assigned_end + 1 - first_sequence;
            if records_assigned < bounds.recovering_writer_records {
                actions.push(Action::RecoveringWriterAssigns);
            }
            for (replica, end) in acknowledgements_on_their_way(state, appender) {
                actions.push(Action::RecoveringWriterLearnsAcknowledgement { replica, end });
            }
        }
    }
}

/// Returns the state that `action` leads to from `state`.
pub(crate) fn apply(state: &State, rules: &Rules, action: Action) -> State {
    let mut next = *state;
    match action {
        Action::FirstWriterAssigns => first_writer_assigns(&mut next),
        Action::FirstWriterLearnsAcknowledgement { replica, end } => {
            next.first_writer.appender = learn_acknowledgement(
                &mut next,
                rules,
                FIRST_EPOCH,
                state.first_writer.appender,
                replica,
                end,
            );
        }
        Action::FirstWriterRolls => first_writer_rolls(&mut next),
        Action::ReplicaPersists {
            replica,
            segment,
            through,
        } => replica_persists(&mut next, rules, replica, segment, through),
        Action::ReplicaFlushes { replica, segment } => {
            let copy = &mut next.copies[replica][segment];
            copy.flushed_end = copy.local_end;
        }
        Action::ReplicaLearnsSeal { replica, segment } => {
            replica_learns_seal(&mut next, replica, segment);
        }
        Action::ReplicaRestarts { replica } => replica_restarts(&mut next, replica),
        Action::RecoveringWriterClaims => {
            next.metadata.epoch = SECOND_EPOCH;
            next.recovering_writer.target_segment = Some(next.last_segment_index());
            next.recovering_writer.phase = RecoveryPhase::Fencing {
                replies: [None; REPLICA_COUNT],
            };
        }
        Action::ReplicaIsFenced {
            replica,
            reply_reaches_writer,
        } => replica_is_fenced(&mut next, replica, reply_reaches_writer),
        Action::RecoveringWriterChoosesEnd => recovering_writer_chooses_end(&mut next, rules),
        Action::RecoveringWriterRepairs { replica } => {
            recovering_writer_repairs(&mut next, rules, replica);
        }
        Action::RecoveringWriterSeals => recovering_writer_seals(&mut next),
        Action::RecoveringWriterOpens => recovering_writer_opens(&mut next),
        Action::RecoveringWriterAssigns => {
            if let RecoveryPhase::Writing(appender) = &mut next.recovering_writer.phase {
                appender.assigned_end += 1;
            }
        }
        Action::RecoveringWriterLearnsAcknowledgement { replica, end } => {
            if let RecoveryPhase::Writing(appender) = state.recovering_writer.phase {
                let appender =
                    learn_acknowledgement(&mut next, rules, SECOND_EPOCH, appender, replica, end);
                next.recovering_writer.phase = RecoveryPhase::Writing(appender);
            }
        }
    }
    next.absorb_readable_records();
    next
}

fn first_writer_assigns(state: &mut State) {
    let is_after_takeover = state.has_fenced_recovery_quorum();
    let appender = &mut state.first_writer.appender;
    appender.assigned_end += 1;
    if is_after_takeover {
        state.observations.assigned_after_takeover[usize::from(appender.assigned_end)] = true;
    }
}

/// A writer takes note of an acknowledgement, raises its durable sequence
/// to the highest position a commit quorum has acknowledged, reports the
/// records up to it as committed, and tells the replicas. Returns the
/// writer's updated appender.
fn learn_acknowledgement(
    state: &mut State,
    rules: &Rules,
    writer_epoch: Epoch,
    mut appender: Appender,
    replica: usize,
    end: Sequence,
) -> Appender {
    appender.acknowledged_ends[replica] = end;
    let mut highest_first = appender.acknowledged_ends;
    highest_first.sort_unstable_by(|left, right| right.cmp(left));
    let quorum_end = highest_first[rules.commit_quorum - 1];
    if quorum_end <= appender.durable {
        return appender;
    }
    for sequence in appender.durable + 1..=quorum_end {
        state.observations.record_commit(sequence, writer_epoch);
    }
    appender.durable = quorum_end;
    // The durable sequence travels with the next request or heartbeat. A
    // replica that receives it serves readers up to it; a replica fenced
    // against this writer refuses it like any other request.
    for replica_copies in &mut state.copies {
        let copy = &mut replica_copies[appender.segment];
        let passes_the_fence = writer_epoch >= copy.fenced_epoch || !rules.replicas_enforce_fencing;
        if passes_the_fence && !copy.knows_seal {
            copy.known_durable = copy.known_durable.max(quorum_end);
        }
    }
    appender
}

fn first_writer_rolls(state: &mut State) {
    let old_appender = state.first_writer.appender;
    let sealed_end = old_appender.durable;
    let mut sealed_segment = state.segment(old_appender.segment);
    sealed_segment.sealed_end = Some(sealed_end);
    state.metadata.segments[old_appender.segment] = Some(sealed_segment);
    state.metadata.standby_segments -= 1;
    let new_segment = state.open_segment(FIRST_EPOCH, sealed_end + 1);
    // Every record above the sealed end is sent again to the new segment
    // with the same sequence number: none of them was reported committed.
    state.first_writer.rolled_from = Some((old_appender.segment, old_appender.assigned_end));
    state.first_writer.appender = Appender {
        segment: new_segment,
        assigned_end: old_appender.assigned_end,
        acknowledged_ends: [sealed_end; REPLICA_COUNT],
        durable: sealed_end,
    };
}

fn replica_persists(
    state: &mut State,
    rules: &Rules,
    replica: usize,
    segment: usize,
    through: Sequence,
) {
    let writer_epoch = state.segment(segment).epoch;
    let copy = &mut state.copies[replica][segment];
    if writer_epoch < copy.fenced_epoch {
        // Only reachable when replicas are told to ignore fencing.
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

fn replica_restarts(state: &mut State, replica: usize) {
    for (segment, record) in state.metadata.segments.into_iter().enumerate() {
        let Some(record) = record else {
            continue;
        };
        let copy = &mut state.copies[replica][segment];
        copy.local_end = copy.flushed_end;
        // The durable sequence is only kept in memory. Until the replica
        // hears from the writer again it serves nothing from an open copy.
        if !copy.knows_seal {
            copy.known_durable = record.first_sequence - 1;
        }
    }
}

fn replica_is_fenced(state: &mut State, replica: usize, reply_reaches_writer: bool) {
    let target = state
        .recovering_writer
        .target_segment
        .expect("a fence is only sent for a target segment");
    let copy = &mut state.copies[replica][target];
    copy.fenced_epoch = SECOND_EPOCH;
    if reply_reaches_writer
        && let RecoveryPhase::Fencing { replies } = &mut state.recovering_writer.phase
    {
        replies[replica] = Some(copy.local_end);
    }
}

fn recovering_writer_chooses_end(state: &mut State, rules: &Rules) {
    let RecoveryPhase::Fencing { replies } = state.recovering_writer.phase else {
        return;
    };
    let reported_ends = replies.iter().flatten().copied();
    let recovery_end = match rules.recovery_end {
        RecoveryEnd::LongestResponder => reported_ends.max(),
        RecoveryEnd::ShortestResponder => reported_ends.min(),
    }
    .expect("the end is only chosen once a replica has answered");
    state.recovering_writer.phase = RecoveryPhase::Repairing {
        recovery_end,
        responders: replies.map(|reply| reply.is_some()),
    };
}

fn recovering_writer_repairs(state: &mut State, rules: &Rules, replica: usize) {
    let RecoveryPhase::Repairing { recovery_end, .. } = state.recovering_writer.phase else {
        return;
    };
    let target = state
        .recovering_writer
        .target_segment
        .expect("a writer that repairs has a target segment");
    let copy = &mut state.copies[replica][target];
    copy.local_end = recovery_end;
    if rules.replicas_flush_before_acknowledging {
        copy.flushed_end = recovery_end;
    }
}

fn recovering_writer_seals(state: &mut State) {
    let RecoveryPhase::Repairing { recovery_end, .. } = state.recovering_writer.phase else {
        return;
    };
    let target = state
        .recovering_writer
        .target_segment
        .expect("a writer that seals has a target segment");
    let mut sealed_segment = state.segment(target);
    sealed_segment.sealed_end = Some(recovery_end);
    state.metadata.segments[target] = Some(sealed_segment);
    state.recovering_writer.phase = RecoveryPhase::Sealed { recovery_end };
}

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
