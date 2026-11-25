//! The state of the modelled system: one stream, its metadata, three replicas, two writers, and what
//! clients have observed so far.
//!
//! # Why this file exists
//!
//! A model checker explores a graph whose nodes are states. Everything the protocol's future can depend on
//! has to be in a state, and nothing else should be, because every extra distinction multiplies the
//! number of states to visit. This file decides what a state is for bitacora's append, roll and failover
//! protocols, and how two states are recognised as the same.
//!
//! # What it contains
//!
//! - [`State`]: one configuration of the whole system.
//! - Its parts: `MetadataStore` and `SegmentRecord`, `SegmentCopy`, `FirstWriter`, `RecoveringWriter` with
//!   its `RecoveryPhase`, `Appender`, and `Observations` with `Breach`.
//! - The sizes of the modelled system: [`REPLICA_COUNT`], [`WRITE_QUORUM`], [`RECOVERY_QUORUM`], and the
//!   two epochs [`FIRST_EPOCH`] and [`SECOND_EPOCH`].
//! - `State::fingerprint`: the bytes that identify a state to the explorer.
//!
//! # How it works
//!
//! Records are not stored. Within one segment there is only one possible record at each sequence number,
//! the one its only writer assigned (design, section 4, rule 1). So a copy of a segment is fully described
//! by how far it reaches, and a record is identified by its sequence number and the epoch of the segment it
//! lives in. That is what keeps a state down to about a hundred bytes of small numbers.
//!
//! A state is `Copy` and made of fixed-size arrays: taking a step copies the state and changes the copy,
//! with no allocation. Positions are `u8` because the model is tiny; sequence numbers index arrays
//! directly, slot 0 being the unused position before the first record.
//!
//! The network is not part of the state. What is on its way is derived: a writer has sent its records up
//! to `assigned_end`, and a replica has acknowledged up to `acknowledged_to_owner`, and any of those
//! messages may arrive at any later time or never (see `transitions.rs`).
//!
//! `Observations` is the one part no actor can read. It records what writers reported as committed and
//! what readers could have been served, only so that the invariants can be stated about a single state
//! instead of about a history.
//!
//! The three replicas are interchangeable. Two states that differ only in which replica is which have the
//! same future, so the fingerprint sorts the replicas' views before writing them, and the explorer visits
//! one of up to six such states.
//!
//! # Invariants
//!
//! - Segments fill the metadata store's slots in order, without gaps: the stream's segments are the slots
//!   `0..segment_count()`.
//! - At most the last segment is open.
//! - Two states have the same fingerprint exactly when they are equal up to a renaming of the replicas.
//!   Everything in a state is in its fingerprint.
//!
//! # See also
//!
//! - `transitions.rs`: the steps that lead from one state to the next.
//! - `invariants.rs`: what must hold in every state.
//! - `rules.rs`: the bounds that size the initial state.

use crate::rules::Bounds;

/// How many replicas hold each segment (N in the design).
pub const REPLICA_COUNT: usize = 3;

/// How many replicas must acknowledge a record to commit it (W in the design).
pub const WRITE_QUORUM: usize = 2;

/// How many replicas a recovering writer must hear from (N − W + 1), so that at least one of them holds
/// every committed record.
pub const RECOVERY_QUORUM: usize = REPLICA_COUNT - WRITE_QUORUM + 1;

/// The most segments a run of the model can create: the first one, one opened by a roll, and one opened
/// by the takeover.
pub(crate) const MAX_SEGMENTS: usize = 3;

/// One slot per sequence number the model can assign, plus the unused slot of the empty position, so that
/// a sequence number indexes an array directly.
pub(crate) const SEQUENCE_SLOTS: usize = 5;

/// A position in the stream. Records are numbered from 1; 0 is the position before the first record.
pub type Sequence = u8;

/// A writer generation. Whoever raised the stream's epoch last is its writer.
pub type Epoch = u8;

/// The epoch of the writer that owns the stream when the model starts.
pub const FIRST_EPOCH: Epoch = 1;

/// The epoch the recovering writer claims.
pub const SECOND_EPOCH: Epoch = 2;

/// The position before the first record of the stream: what an end is when nothing is there yet.
pub(crate) const EMPTY_POSITION: Sequence = 0;

/// The sequence number of the first record of the stream.
pub(crate) const FIRST_SEQUENCE: Sequence = 1;

/// The position of the stream's first segment in the metadata store.
pub(crate) const FIRST_SEGMENT: usize = 0;

/// What the metadata store knows about one segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SegmentRecord {
    /// The epoch of the only writer that authors records in the segment.
    pub epoch: Epoch,
    /// The sequence number of the segment's first record.
    pub first_sequence: Sequence,
    /// The segment's final sequence number, once it is sealed. One below `first_sequence` if it was
    /// sealed empty. `None` while the segment is open.
    pub sealed_end: Option<Sequence>,
}

impl SegmentRecord {
    /// Whether a sequence number belongs to this segment.
    ///
    /// # Arguments
    ///
    /// * `sequence` - The sequence number asked about.
    ///
    /// # Returns
    ///
    /// `true` if the segment starts at or before `sequence` and is either open, in which case it holds
    /// everything from its first sequence on, or sealed at or after `sequence`.
    pub(crate) fn contains(self, sequence: Sequence) -> bool {
        if sequence < self.first_sequence {
            return false;
        }
        match self.sealed_end {
            Some(sealed_end) => sequence <= sealed_end,
            None => true,
        }
    }
}

/// The metadata store. It only changes through conditional writes, which the model takes as single atomic
/// steps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MetadataStore {
    /// The stream's current epoch. A conditional write of a writer succeeds only if this is its epoch.
    pub epoch: Epoch,
    /// The stream's segments, oldest first. Slots are filled in order; unused slots are `None`.
    pub segments: [Option<SegmentRecord>; MAX_SEGMENTS],
    /// How many segments the controller has placed in advance and nobody has opened yet. A roll and a
    /// takeover each need one.
    pub standby_segments: u8,
}

/// One replica's copy of one segment.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SegmentCopy {
    /// The last sequence number the copy holds, flushed or not.
    pub local_end: Sequence,
    /// The last sequence number that survives a restart.
    pub flushed_end: Sequence,
    /// Appends from a writer below this epoch are refused.
    pub fenced_epoch: Epoch,
    /// Whether the replica has learned the segment's sealed end.
    pub knows_seal: bool,
    /// The latest durable sequence heard from the segment's writer: how far the replica serves readers
    /// while the segment is open. Kept in memory only, so a restart forgets it.
    pub known_durable: Sequence,
    /// The highest end the replica has acknowledged to the segment's own writer. The acknowledgement may
    /// still be on its way, or lost.
    pub acknowledged_to_owner: Sequence,
}

/// The part of a writer that appends to its open segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Appender {
    /// The position of the segment the writer appends to.
    pub segment: usize,
    /// The last sequence number the writer has assigned and sent.
    pub assigned_end: Sequence,
    /// What each replica has acknowledged, as far as the writer knows, by replica.
    pub acknowledged_ends: [Sequence; REPLICA_COUNT],
    /// The writer's durable sequence: the last record it has reported as committed.
    pub durable: Sequence,
}

/// A segment the first writer rolled away from, to which requests it sent earlier may still arrive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AbandonedSegment {
    /// The position of the segment the writer rolled away from.
    pub segment: usize,
    /// How far the writer had sent records to that segment before it rolled.
    pub offered_through: Sequence,
}

/// The writer that owns the stream when the model starts (epoch 1).
///
/// Neither writer has a "crashed" flag. A crashed writer is one that takes no further step, and the
/// checker already explores every run in which a writer stops after any of its steps, with its requests
/// still on their way. A flag would multiply the states without adding a behaviour.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FirstWriter {
    /// The writer's open segment and what it knows about it.
    pub appender: Appender,
    /// The segment the writer rolled away from, if it has rolled. The model allows one roll.
    pub rolled_from: Option<AbandonedSegment>,
}

/// Where the recovering writer is in the failover protocol (design, section 9).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RecoveryPhase {
    /// The writer has not claimed the epoch.
    NotStarted,
    /// Steps 3 and 4: fences are out and the writer collects the answers.
    Fencing {
        /// The local end reported by every replica whose answer has arrived so far, by replica.
        replies: [Option<Sequence>; REPLICA_COUNT],
    },
    /// Steps 5 and 6: the end is chosen and the responders are brought up to it.
    Repairing {
        /// The sequence number the old segment will be sealed at.
        recovery_end: Sequence,
        /// Which replicas answered the fence in time, by replica. Only those are repaired.
        responders: [bool; REPLICA_COUNT],
    },
    /// Step 7 is done: the old segment is sealed in the metadata store.
    Sealed {
        /// The sequence number the old segment was sealed at.
        recovery_end: Sequence,
    },
    /// Step 8 is done: the writer appends to its own segment.
    Writing(Appender),
}

/// The writer that takes the stream over (epoch 2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RecoveringWriter {
    /// The segment it found open when it claimed the epoch. Its fences for that segment may arrive at any
    /// time afterwards, even if the writer itself never takes another step.
    pub target_segment: Option<usize>,
    /// How far the writer has got in the failover protocol.
    pub phase: RecoveryPhase,
}

/// A rule broken by a single step, which a later state would not show.
///
/// Most violations are visible in the state they produce. These three are events: once they have
/// happened, the state may look innocent again, so the step records them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Breach {
    /// Two writers reported different records committed at one sequence number.
    SequenceCommittedTwice,
    /// Two reads returned different records at one sequence number.
    ReadersDisagree,
    /// A replica persisted an append from an epoch it was fenced against.
    FencedReplicaPersistedAnOlderEpoch,
}

/// What clients have observed, kept only to state the invariants. No actor of the model can read it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Observations {
    /// For each sequence number, the epoch of the record a writer reported as committed there.
    pub committed: [Option<Epoch>; SEQUENCE_SLOTS],
    /// For each sequence number, the epoch of the record some replica has served, or could have served,
    /// to a reader.
    pub ever_readable: [Option<Epoch>; SEQUENCE_SLOTS],
    /// The sequence numbers the first writer assigned after the recovering writer had fenced a recovery
    /// quorum.
    pub assigned_after_takeover: [bool; SEQUENCE_SLOTS],
    /// The first rule a single step broke, if any did.
    pub breach: Option<Breach>,
}

impl Observations {
    /// Records that a writer reported a record as committed.
    ///
    /// Notes [`Breach::SequenceCommittedTwice`] if a writer of another epoch already reported a record
    /// committed at the same sequence number. An earlier breach is kept.
    ///
    /// # Arguments
    ///
    /// * `sequence` - The sequence number of the committed record.
    /// * `epoch` - The epoch of the writer that reported it, which identifies the record.
    pub(crate) fn record_commit(&mut self, sequence: Sequence, epoch: Epoch) {
        let committed = &mut self.committed[usize::from(sequence)];
        match *committed {
            None => *committed = Some(epoch),
            Some(existing_epoch) if existing_epoch != epoch => {
                self.breach.get_or_insert(Breach::SequenceCommittedTwice);
            }
            Some(_same_epoch) => {}
        }
    }

    /// Records that a reader could be handed the record of `epoch` at `sequence`, and notes
    /// [`Breach::ReadersDisagree`] if a reader could already have been handed a record of another epoch
    /// there. An earlier breach is kept.
    fn record_read(&mut self, sequence: Sequence, epoch: Epoch) {
        let readable = &mut self.ever_readable[usize::from(sequence)];
        match *readable {
            None => *readable = Some(epoch),
            Some(existing_epoch) if existing_epoch != epoch => {
                self.breach.get_or_insert(Breach::ReadersDisagree);
            }
            Some(_same_epoch) => {}
        }
    }
}

/// One configuration of the whole modelled system.
///
/// A state holds the metadata store, every replica's copy of every segment, both writers, and what
/// clients have observed. Obtain one from [`crate::ProtocolModel::replay`], for example to print the state
/// a counterexample ends in; its `Debug` output shows every part.
///
/// # Examples
///
/// ```
/// use bitacora_model::{Action, Bounds, ProtocolModel};
///
/// let model = ProtocolModel::as_designed(Bounds::FAILOVER);
///
/// let state_after_one_append = model.replay(&[Action::FirstWriterAssigns]);
///
/// assert_ne!(state_after_one_append, model.replay(&[]));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct State {
    /// What the metadata store holds.
    pub(crate) metadata: MetadataStore,
    /// Every replica's copy of every segment, by replica then by segment.
    pub(crate) copies: [[SegmentCopy; MAX_SEGMENTS]; REPLICA_COUNT],
    /// The writer that owns the stream when the model starts.
    pub(crate) first_writer: FirstWriter,
    /// The writer that takes the stream over.
    pub(crate) recovering_writer: RecoveringWriter,
    /// What clients have observed so far.
    pub(crate) observations: Observations,
}

impl State {
    /// Builds the state the model starts in.
    ///
    /// The first writer holds epoch 1, and an empty first segment is open on all three replicas. The
    /// recovering writer has not started, and nothing has been observed.
    ///
    /// # Arguments
    ///
    /// * `bounds` - How large a system is explored. Only the number of standby segments enters the
    ///   state; the other bounds are checked to fit.
    ///
    /// # Panics
    ///
    /// Panics if `bounds` asks for more records or more standby segments than a state has room for.
    pub(crate) fn initial(bounds: Bounds) -> Self {
        assert_bounds_fit_a_state(bounds);
        let untouched_appender = Appender {
            segment: FIRST_SEGMENT,
            assigned_end: EMPTY_POSITION,
            acknowledged_ends: [EMPTY_POSITION; REPLICA_COUNT],
            durable: EMPTY_POSITION,
        };
        let mut state = Self {
            metadata: MetadataStore {
                epoch: FIRST_EPOCH,
                segments: [None; MAX_SEGMENTS],
                standby_segments: bounds.standby_segments,
            },
            copies: [[SegmentCopy::default(); MAX_SEGMENTS]; REPLICA_COUNT],
            first_writer: FirstWriter {
                appender: untouched_appender,
                rolled_from: None,
            },
            recovering_writer: RecoveringWriter {
                target_segment: None,
                phase: RecoveryPhase::NotStarted,
            },
            observations: Observations {
                committed: [None; SEQUENCE_SLOTS],
                ever_readable: [None; SEQUENCE_SLOTS],
                assigned_after_takeover: [false; SEQUENCE_SLOTS],
                breach: None,
            },
        };
        state.open_segment(FIRST_EPOCH, FIRST_SEQUENCE);
        state
    }

    /// Counts the stream's segments.
    ///
    /// # Returns
    ///
    /// How many segments the metadata store lists. They are at the positions `0..segment_count()`, oldest
    /// first, because slots are filled in order.
    pub(crate) fn segment_count(&self) -> usize {
        let mut segment_count = 0;
        for slot in &self.metadata.segments {
            if slot.is_some() {
                segment_count += 1;
            }
        }
        segment_count
    }

    /// Returns the record of the segment at a position.
    ///
    /// # Arguments
    ///
    /// * `segment` - The position of the segment, below [`State::segment_count`].
    ///
    /// # Panics
    ///
    /// Panics if no segment was created at that position.
    pub(crate) fn segment(&self, segment: usize) -> SegmentRecord {
        self.metadata.segments[segment].expect("the model only names segments that exist")
    }

    /// Returns the position of the most recent segment, the only one that can be open.
    ///
    /// # Panics
    ///
    /// Panics if the stream has no segment, which cannot happen: the initial state opens the first one
    /// and segments are never removed.
    pub(crate) fn last_segment_index(&self) -> usize {
        self.segment_count()
            .checked_sub(1)
            .expect("the stream always has its first segment")
    }

    /// Adds an open segment to the metadata store and gives every replica an empty copy of it.
    ///
    /// Each copy starts fenced at the segment's own epoch, so it accepts the segment's writer and nobody
    /// older, and ends one position before the segment's first record.
    ///
    /// # Arguments
    ///
    /// * `epoch` - The epoch of the writer that opens the segment.
    /// * `first_sequence` - The sequence number of the segment's first record, at least 1.
    ///
    /// # Returns
    ///
    /// The position of the new segment.
    ///
    /// # Panics
    ///
    /// Panics if the stream already has as many segments as a state has room for. The bounds checked by
    /// [`State::initial`] rule that out.
    pub(crate) fn open_segment(&mut self, epoch: Epoch, first_sequence: Sequence) -> usize {
        let new_segment = self.segment_count();
        self.metadata.segments[new_segment] = Some(SegmentRecord {
            epoch,
            first_sequence,
            sealed_end: None,
        });
        let empty_end = first_sequence - 1;
        for replica_copies in &mut self.copies {
            replica_copies[new_segment] = SegmentCopy {
                local_end: empty_end,
                flushed_end: empty_end,
                fenced_epoch: epoch,
                knows_seal: false,
                known_durable: empty_end,
                acknowledged_to_owner: empty_end,
            };
        }
        new_segment
    }

    /// Reports how far the writer of a segment has sent records to it.
    ///
    /// Any replica may receive any of those records at any later time, or never. This is how the model
    /// has a network without storing messages.
    ///
    /// # Arguments
    ///
    /// * `segment` - The position of the segment asked about.
    ///
    /// # Returns
    ///
    /// The last sequence number sent to the segment: by the first writer if it is its open segment or the
    /// one it rolled away from, by the recovering writer if it is the segment it opened. The empty
    /// position if no writer ever sent anything to the segment.
    pub(crate) fn offered_through(&self, segment: usize) -> Sequence {
        if self.first_writer.appender.segment == segment {
            return self.first_writer.appender.assigned_end;
        }
        if let Some(abandoned) = self.first_writer.rolled_from
            && abandoned.segment == segment
        {
            return abandoned.offered_through;
        }
        if let RecoveryPhase::Writing(appender) = self.recovering_writer.phase
            && appender.segment == segment
        {
            return appender.assigned_end;
        }
        EMPTY_POSITION
    }

    /// Reports the last sequence number a replica serves to readers from its copy of a segment.
    ///
    /// # Arguments
    ///
    /// * `replica` - The replica asked.
    /// * `segment` - The position of the segment read.
    ///
    /// # Returns
    ///
    /// The copy's local end once the replica knows the seal, because a sealed segment is committed up to
    /// its end. Before that, the lower of the local end and the durable sequence the replica has heard of:
    /// a replica never serves a record that may not be committed.
    pub(crate) fn readable_end(&self, replica: usize, segment: usize) -> Sequence {
        let copy = self.copies[replica][segment];
        if copy.knows_seal {
            copy.local_end
        } else {
            copy.local_end.min(copy.known_durable)
        }
    }

    /// Adds everything a reader could be handed in this state to what readers have observed.
    ///
    /// Called after every step, so that `ever_readable` covers every state the run went through. Notes
    /// [`Breach::ReadersDisagree`] if two replicas serve records of different epochs at one sequence
    /// number, now or compared with an earlier state.
    pub(crate) fn absorb_readable_records(&mut self) {
        let segment_count = self.segment_count();
        for replica in 0..REPLICA_COUNT {
            for segment in 0..segment_count {
                let record = self.segment(segment);
                for sequence in record.first_sequence..=self.readable_end(replica, segment) {
                    self.observations.record_read(sequence, record.epoch);
                }
            }
        }
    }

    /// Whether the recovering writer has fenced a recovery quorum of the segment it is taking over.
    ///
    /// From that moment the old writer can no longer gather a write quorum for anything new, whether or
    /// not the recovering writer knows it yet.
    ///
    /// # Returns
    ///
    /// `false` before the recovering writer has claimed the epoch.
    pub(crate) fn has_fenced_recovery_quorum(&self) -> bool {
        let Some(target_segment) = self.recovering_writer.target_segment else {
            return false;
        };
        let mut fenced_replicas = 0;
        for replica_copies in &self.copies {
            if replica_copies[target_segment].fenced_epoch >= SECOND_EPOCH {
                fenced_replicas += 1;
            }
        }
        fenced_replicas >= RECOVERY_QUORUM
    }

    /// Whether the stream still holds a given record durably.
    ///
    /// # Arguments
    ///
    /// * `sequence` - The sequence number of the record.
    /// * `author` - The epoch of the writer that authored it, which identifies the record.
    ///
    /// # Returns
    ///
    /// `true` if some segment of the metadata store that was written by `author` contains `sequence`, and
    /// at least one replica has flushed its copy of that segment up to `sequence`.
    pub(crate) fn stream_holds(&self, sequence: Sequence, author: Epoch) -> bool {
        for segment in 0..self.segment_count() {
            let record = self.segment(segment);
            if record.epoch != author || !record.contains(sequence) {
                continue;
            }
            for replica_copies in &self.copies {
                if replica_copies[segment].flushed_end >= sequence {
                    return true;
                }
            }
        }
        false
    }

    /// Writes the bytes that identify the state to the explorer.
    ///
    /// Everything in the state is written, in a fixed order, so two different states never share a
    /// fingerprint. The one exception is deliberate: the replicas are interchangeable, so each replica's
    /// part of the state is gathered into one view, and the views are sorted before they are written. Two
    /// states that only differ in which replica is which therefore have the same fingerprint and are
    /// explored once.
    ///
    /// This runs once for every step the explorer tries. It allocates the one vector it returns.
    ///
    /// # Returns
    ///
    /// The fingerprint, of the same length for every state of one phase of the recovering writer.
    pub(crate) fn fingerprint(&self) -> Vec<u8> {
        let mut fingerprint = Vec::with_capacity(FINGERPRINT_CAPACITY_BYTES);
        self.write_metadata(&mut fingerprint);
        self.write_first_writer(&mut fingerprint);
        self.write_recovering_writer(&mut fingerprint);
        self.write_observations(&mut fingerprint);
        self.write_replica_views(&mut fingerprint);
        fingerprint
    }

    /// Appends the metadata store to a fingerprint: the epoch, the standby count and three bytes per
    /// segment slot.
    fn write_metadata(&self, fingerprint: &mut Vec<u8>) {
        fingerprint.extend([self.metadata.epoch, self.metadata.standby_segments]);
        for slot in self.metadata.segments {
            match slot {
                Some(segment) => fingerprint.extend([
                    segment.epoch,
                    segment.first_sequence,
                    encode_optional(segment.sealed_end),
                ]),
                None => fingerprint.extend([ABSENT; 3]),
            }
        }
    }

    /// Appends the part of the first writer that does not belong to a replica: its segment, how far it
    /// assigned and committed, and the segment it rolled away from.
    fn write_first_writer(&self, fingerprint: &mut Vec<u8>) {
        let appender = self.first_writer.appender;
        fingerprint.extend([encode_index(appender.segment), appender.assigned_end, appender.durable]);
        match self.first_writer.rolled_from {
            Some(abandoned) => {
                fingerprint.extend([encode_index(abandoned.segment), abandoned.offered_through]);
            }
            None => fingerprint.extend([ABSENT; 2]),
        }
    }

    /// Appends the part of the recovering writer that does not belong to a replica: its target segment
    /// and its phase, each phase starting with its own tag.
    fn write_recovering_writer(&self, fingerprint: &mut Vec<u8>) {
        let target_segment = match self.recovering_writer.target_segment {
            Some(target_segment) => encode_index(target_segment),
            None => ABSENT,
        };
        fingerprint.push(target_segment);
        match self.recovering_writer.phase {
            RecoveryPhase::NotStarted => fingerprint.push(PHASE_TAG_NOT_STARTED),
            RecoveryPhase::Fencing { .. } => fingerprint.push(PHASE_TAG_FENCING),
            RecoveryPhase::Repairing { recovery_end, .. } => {
                fingerprint.extend([PHASE_TAG_REPAIRING, recovery_end]);
            }
            RecoveryPhase::Sealed { recovery_end } => {
                fingerprint.extend([PHASE_TAG_SEALED, recovery_end]);
            }
            RecoveryPhase::Writing(appender) => fingerprint.extend([
                PHASE_TAG_WRITING,
                encode_index(appender.segment),
                appender.assigned_end,
                appender.durable,
            ]),
        }
    }

    /// Appends what clients have observed: one byte per sequence slot for each of the three tables, and
    /// one byte for the breach.
    fn write_observations(&self, fingerprint: &mut Vec<u8>) {
        let observations = self.observations;
        fingerprint.extend(observations.committed.map(encode_optional));
        fingerprint.extend(observations.ever_readable.map(encode_optional));
        fingerprint.extend(observations.assigned_after_takeover.map(u8::from));
        fingerprint.push(match observations.breach {
            None => BREACH_TAG_NONE,
            Some(Breach::SequenceCommittedTwice) => BREACH_TAG_SEQUENCE_COMMITTED_TWICE,
            Some(Breach::ReadersDisagree) => BREACH_TAG_READERS_DISAGREE,
            Some(Breach::FencedReplicaPersistedAnOlderEpoch) => BREACH_TAG_FENCED_REPLICA_PERSISTED,
        });
    }

    /// Appends the views of the three replicas in sorted order, which is what makes the fingerprint blind
    /// to which replica is which.
    fn write_replica_views(&self, fingerprint: &mut Vec<u8>) {
        let mut replica_views: [ReplicaView; REPLICA_COUNT] = std::array::from_fn(|replica| self.replica_view(replica));
        replica_views.sort_unstable();
        for replica_view in replica_views {
            fingerprint.extend(replica_view);
        }
    }

    /// Gathers everything in the state that belongs to one replica: its copies of every segment, and what
    /// each of the two writers believes about it.
    ///
    /// Keeping a replica's copies together with the writers' beliefs about that same replica is what
    /// makes sorting the views sound: a state in which replica 0 holds a record and the writer knows it
    /// differs from one in which the writer believes it of replica 1.
    fn replica_view(&self, replica: usize) -> ReplicaView {
        let mut replica_view = [0; REPLICA_VIEW_BYTES];
        for (segment, copy) in self.copies[replica].into_iter().enumerate() {
            let copy_start = segment * COPY_BYTES;
            replica_view[copy_start..copy_start + COPY_BYTES].copy_from_slice(&[
                copy.local_end,
                copy.flushed_end,
                copy.fenced_epoch,
                u8::from(copy.knows_seal),
                copy.known_durable,
                copy.acknowledged_to_owner,
            ]);
        }
        replica_view[FIRST_WRITER_BELIEF_OFFSET] = self.first_writer.appender.acknowledged_ends[replica];
        replica_view[RECOVERING_WRITER_BELIEF_OFFSET] = match self.recovering_writer.phase {
            RecoveryPhase::NotStarted | RecoveryPhase::Sealed { .. } => 0,
            RecoveryPhase::Fencing { replies } => encode_optional(replies[replica]),
            RecoveryPhase::Repairing { responders, .. } => u8::from(responders[replica]),
            RecoveryPhase::Writing(appender) => appender.acknowledged_ends[replica],
        };
        replica_view
    }
}

/// Checks that a state has room for what the bounds allow a run to create.
///
/// # Panics
///
/// Panics if the two writers together may assign more records than there are sequence slots, or if there
/// are more standby segments than free segment slots.
fn assert_bounds_fit_a_state(bounds: Bounds) {
    let total_records = usize::from(bounds.first_writer_records) + usize::from(bounds.recovering_writer_records);
    assert!(
        total_records < SEQUENCE_SLOTS,
        "the model has room for {} records, not {total_records}",
        SEQUENCE_SLOTS - 1
    );
    assert!(
        usize::from(bounds.standby_segments) < MAX_SEGMENTS,
        "the model has room for {} standby segments, not {}",
        MAX_SEGMENTS - 1,
        bounds.standby_segments
    );
}

/// Enough bytes for the longest fingerprint, so that writing one never grows the vector.
const FINGERPRINT_CAPACITY_BYTES: usize = 128;

/// How many bytes describe one segment copy in a fingerprint: one per field of `SegmentCopy`.
const COPY_BYTES: usize = 6;

/// Where, in a replica's view, the byte for what the first writer believes about the replica is.
const FIRST_WRITER_BELIEF_OFFSET: usize = MAX_SEGMENTS * COPY_BYTES;

/// Where, in a replica's view, the byte for what the recovering writer believes about the replica is.
const RECOVERING_WRITER_BELIEF_OFFSET: usize = FIRST_WRITER_BELIEF_OFFSET + 1;

/// How many bytes describe one replica in a fingerprint: its copies, and one byte for what each of the two
/// writers believes about it.
const REPLICA_VIEW_BYTES: usize = RECOVERING_WRITER_BELIEF_OFFSET + 1;

/// Everything in a state that belongs to one replica, as bytes that can be sorted and compared.
type ReplicaView = [u8; REPLICA_VIEW_BYTES];

/// How an absent value is written in a fingerprint. No sequence number, epoch or segment position of the
/// model is this large.
const ABSENT: u8 = u8::MAX;

/// The byte that opens the fingerprint of a recovering writer that has not started.
const PHASE_TAG_NOT_STARTED: u8 = 0;

/// The byte that opens the fingerprint of a recovering writer that is collecting fence answers.
const PHASE_TAG_FENCING: u8 = 1;

/// The byte that opens the fingerprint of a recovering writer that is repairing the tail.
const PHASE_TAG_REPAIRING: u8 = 2;

/// The byte that opens the fingerprint of a recovering writer that has sealed the old segment.
const PHASE_TAG_SEALED: u8 = 3;

/// The byte that opens the fingerprint of a recovering writer that appends to its own segment.
const PHASE_TAG_WRITING: u8 = 4;

/// The fingerprint byte of a run in which no step broke a rule.
const BREACH_TAG_NONE: u8 = 0;

/// The fingerprint byte of [`Breach::SequenceCommittedTwice`].
const BREACH_TAG_SEQUENCE_COMMITTED_TWICE: u8 = 1;

/// The fingerprint byte of [`Breach::ReadersDisagree`].
const BREACH_TAG_READERS_DISAGREE: u8 = 2;

/// The fingerprint byte of [`Breach::FencedReplicaPersistedAnOlderEpoch`].
const BREACH_TAG_FENCED_REPLICA_PERSISTED: u8 = 3;

/// Writes an optional small number as one fingerprint byte: itself, or [`ABSENT`].
fn encode_optional(value: Option<u8>) -> u8 {
    value.unwrap_or(ABSENT)
}

/// Writes a segment position as one fingerprint byte.
///
/// # Panics
///
/// Panics if the position does not fit in a byte, which no position of the model does: there are three
/// segment slots.
fn encode_index(segment: usize) -> u8 {
    u8::try_from(segment).expect("segment positions are small")
}

#[cfg(test)]
#[path = "state_test.rs"]
mod tests;
