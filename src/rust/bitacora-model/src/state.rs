//! The state of the modelled system: one stream, its metadata, three
//! replicas, two writers, and what clients have observed so far.
//!
//! Records are not stored. Within one segment there is only one possible
//! record at each sequence number, the one its only writer assigned (design,
//! section 4, rule 1), so a copy of a segment is fully described by how far
//! it reaches, and a record is identified by its sequence number and the
//! epoch of the segment it lives in.

use crate::rules::Bounds;

/// How many replicas hold each segment (N).
pub const REPLICA_COUNT: usize = 3;

/// How many replicas must acknowledge a record to commit it (W).
pub const WRITE_QUORUM: usize = 2;

/// How many replicas a recovering writer must hear from (N − W + 1).
pub const RECOVERY_QUORUM: usize = REPLICA_COUNT - WRITE_QUORUM + 1;

/// The most segments a run of the model can create: the first one, one
/// opened by a roll, and one opened by the takeover.
pub(crate) const MAX_SEGMENTS: usize = 3;

/// One slot per sequence number the model can assign, plus the unused slot
/// of the empty position, so that a sequence number indexes directly.
pub(crate) const SEQUENCE_SLOTS: usize = 5;

/// A position in the stream. Records are numbered from 1; 0 is the position
/// before the first record.
pub type Sequence = u8;

/// A writer generation.
pub type Epoch = u8;

/// The epoch of the writer that owns the stream when the model starts.
pub const FIRST_EPOCH: Epoch = 1;

/// The epoch the recovering writer claims.
pub const SECOND_EPOCH: Epoch = 2;

/// What the metadata store knows about one segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SegmentRecord {
    /// The epoch of the only writer that authors records in it.
    pub epoch: Epoch,
    pub first_sequence: Sequence,
    pub sealed_end: Option<Sequence>,
}

impl SegmentRecord {
    /// Whether `sequence` belongs to this segment. An open segment holds
    /// everything from its first sequence on.
    pub(crate) fn contains(self, sequence: Sequence) -> bool {
        self.first_sequence <= sequence
            && self
                .sealed_end
                .is_none_or(|sealed_end| sequence <= sealed_end)
    }
}

/// The metadata store. It only changes through conditional writes, which
/// the model takes as single atomic steps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MetadataStore {
    pub epoch: Epoch,
    /// The stream's segments in order; unused slots are `None`.
    pub segments: [Option<SegmentRecord>; MAX_SEGMENTS],
    /// How many segments the controller has placed in advance.
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
    /// The latest durable sequence heard from the segment's writer: how far
    /// the replica serves readers while the segment is open. In memory only.
    pub known_durable: Sequence,
    /// The highest end the replica has acknowledged to the segment's own
    /// writer. The acknowledgement may still be on its way, or lost.
    pub acknowledged_to_owner: Sequence,
}

/// The part of a writer that appends to its open segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Appender {
    pub segment: usize,
    /// The last sequence number the writer has assigned and sent.
    pub assigned_end: Sequence,
    /// What each replica has acknowledged, as far as the writer knows.
    pub acknowledged_ends: [Sequence; REPLICA_COUNT],
    pub durable: Sequence,
}

/// The writer that owns the stream when the model starts (epoch 1).
///
/// Neither writer has a "crashed" flag. A crashed writer is one that takes
/// no further step, and the checker already explores every run in which a
/// writer stops after any of its steps, with its requests still on their
/// way. A flag would multiply the states without adding a behaviour.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FirstWriter {
    pub appender: Appender,
    /// The segment the writer rolled away from, and how far it had sent
    /// records to it. Those requests may still arrive there.
    pub rolled_from: Option<(usize, Sequence)>,
}

/// Where the recovering writer is in the failover protocol (design, section
/// 9).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RecoveryPhase {
    NotStarted,
    /// Steps 3 and 4: fences are out; the local end of every replica that
    /// answered so far.
    Fencing {
        replies: [Option<Sequence>; REPLICA_COUNT],
    },
    /// Steps 5 and 6: the end is chosen and the responders are brought up
    /// to it.
    Repairing {
        recovery_end: Sequence,
        responders: [bool; REPLICA_COUNT],
    },
    /// Step 7 is done: the old segment is sealed in the metadata store.
    Sealed {
        recovery_end: Sequence,
    },
    /// Step 8 is done: the writer appends to its own segment.
    Writing(Appender),
}

/// The writer that takes the stream over (epoch 2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RecoveringWriter {
    /// The segment it found open when it claimed the epoch. Its fences for
    /// that segment may arrive at any time afterwards, even if the writer
    /// itself never takes another step.
    pub target_segment: Option<usize>,
    pub phase: RecoveryPhase,
}

/// A rule broken by a single step, which a later state would not show.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Breach {
    /// Two writers reported different records committed at one sequence.
    SequenceCommittedTwice,
    /// Two reads returned different records at one sequence.
    ReadersDisagree,
    /// A replica persisted an append from an epoch it was fenced against.
    FencedReplicaPersistedAnOlderEpoch,
}

/// What clients have observed, kept only to state the invariants. No actor
/// of the model can read it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Observations {
    /// For each sequence number, the epoch of the record a writer reported
    /// as committed there.
    pub committed: [Option<Epoch>; SEQUENCE_SLOTS],
    /// For each sequence number, the epoch of the record some replica has
    /// served, or could have served, to a reader.
    pub ever_readable: [Option<Epoch>; SEQUENCE_SLOTS],
    /// The sequence numbers the first writer assigned after the recovering
    /// writer had fenced a recovery quorum.
    pub assigned_after_takeover: [bool; SEQUENCE_SLOTS],
    pub breach: Option<Breach>,
}

impl Observations {
    /// Records that the writer of `epoch` reported `sequence` as committed.
    pub(crate) fn record_commit(&mut self, sequence: Sequence, epoch: Epoch) {
        let committed = &mut self.committed[usize::from(sequence)];
        match *committed {
            None => *committed = Some(epoch),
            Some(existing_epoch) if existing_epoch != epoch => {
                self.breach.get_or_insert(Breach::SequenceCommittedTwice);
            }
            Some(_) => {}
        }
    }

    /// Records that a reader could be handed the record of `epoch` at
    /// `sequence`.
    fn record_read(&mut self, sequence: Sequence, epoch: Epoch) {
        let readable = &mut self.ever_readable[usize::from(sequence)];
        match *readable {
            None => *readable = Some(epoch),
            Some(existing_epoch) if existing_epoch != epoch => {
                self.breach.get_or_insert(Breach::ReadersDisagree);
            }
            Some(_) => {}
        }
    }
}

/// One configuration of the whole system.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct State {
    pub(crate) metadata: MetadataStore,
    /// Every replica's copy of every segment, by replica then by segment.
    pub(crate) copies: [[SegmentCopy; MAX_SEGMENTS]; REPLICA_COUNT],
    pub(crate) first_writer: FirstWriter,
    pub(crate) recovering_writer: RecoveringWriter,
    pub(crate) observations: Observations,
}

impl State {
    /// The state the model starts in: the first writer holds epoch 1 and an
    /// empty first segment is open on all three replicas.
    ///
    /// # Panics
    ///
    /// Panics if `bounds` asks for more records or segments than a state
    /// has room for.
    pub(crate) fn initial(bounds: Bounds) -> Self {
        let total_records = usize::from(bounds.first_writer_records)
            + usize::from(bounds.recovering_writer_records);
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
        let mut state = Self {
            metadata: MetadataStore {
                epoch: FIRST_EPOCH,
                segments: [None; MAX_SEGMENTS],
                standby_segments: bounds.standby_segments,
            },
            copies: [[SegmentCopy::default(); MAX_SEGMENTS]; REPLICA_COUNT],
            first_writer: FirstWriter {
                appender: Appender {
                    segment: 0,
                    assigned_end: 0,
                    acknowledged_ends: [0; REPLICA_COUNT],
                    durable: 0,
                },
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
        state.open_segment(FIRST_EPOCH, 1);
        state
    }

    /// The stream's segments with their positions, oldest first.
    pub(crate) fn segments(&self) -> impl Iterator<Item = (usize, SegmentRecord)> + '_ {
        self.metadata
            .segments
            .iter()
            .enumerate()
            .filter_map(|(index, segment)| segment.map(|segment| (index, segment)))
    }

    /// The segment at `index`.
    ///
    /// # Panics
    ///
    /// Panics if no segment was created at that position.
    pub(crate) fn segment(&self, index: usize) -> SegmentRecord {
        self.metadata.segments[index].expect("the model only names segments that exist")
    }

    /// The position of the most recent segment.
    pub(crate) fn last_segment_index(&self) -> usize {
        self.segments()
            .last()
            .map(|(index, _segment)| index)
            .expect("the stream always has its first segment")
    }

    /// Adds an open segment to the metadata store and gives every replica
    /// an empty copy of it that accepts `epoch`. Returns its position.
    pub(crate) fn open_segment(&mut self, epoch: Epoch, first_sequence: Sequence) -> usize {
        let index = self.segments().count();
        self.metadata.segments[index] = Some(SegmentRecord {
            epoch,
            first_sequence,
            sealed_end: None,
        });
        let empty_end = first_sequence - 1;
        for replica_copies in &mut self.copies {
            replica_copies[index] = SegmentCopy {
                local_end: empty_end,
                flushed_end: empty_end,
                fenced_epoch: epoch,
                knows_seal: false,
                known_durable: empty_end,
                acknowledged_to_owner: empty_end,
            };
        }
        index
    }

    /// How far the writer of `segment` has sent records to it. Any replica
    /// may receive any of them at any later time, or never.
    pub(crate) fn offered_through(&self, segment: usize) -> Sequence {
        if self.first_writer.appender.segment == segment {
            return self.first_writer.appender.assigned_end;
        }
        if let Some((rolled_from, offered_through)) = self.first_writer.rolled_from
            && rolled_from == segment
        {
            return offered_through;
        }
        if let RecoveryPhase::Writing(appender) = self.recovering_writer.phase
            && appender.segment == segment
        {
            return appender.assigned_end;
        }
        0
    }

    /// The last sequence number `replica` serves to readers from its copy
    /// of `segment`: up to the sealed end once it knows the seal, and up to
    /// the durable sequence it has heard of before that.
    pub(crate) fn readable_end(&self, replica: usize, segment: usize) -> Sequence {
        let copy = self.copies[replica][segment];
        if copy.knows_seal {
            copy.local_end
        } else {
            copy.local_end.min(copy.known_durable)
        }
    }

    /// Adds everything a reader could be handed in this state to what
    /// readers have observed, noting a disagreement if there is one.
    pub(crate) fn absorb_readable_records(&mut self) {
        for replica in 0..REPLICA_COUNT {
            for (index, segment) in self.metadata.segments.into_iter().enumerate() {
                let Some(segment) = segment else {
                    continue;
                };
                for sequence in segment.first_sequence..=self.readable_end(replica, index) {
                    self.observations.record_read(sequence, segment.epoch);
                }
            }
        }
    }

    /// Whether the recovering writer has fenced a recovery quorum of the
    /// segment it is taking over.
    pub(crate) fn has_fenced_recovery_quorum(&self) -> bool {
        let Some(target_segment) = self.recovering_writer.target_segment else {
            return false;
        };
        let fenced_replicas = self
            .copies
            .iter()
            .filter(|replica_copies| replica_copies[target_segment].fenced_epoch >= SECOND_EPOCH)
            .count();
        fenced_replicas >= RECOVERY_QUORUM
    }

    /// Whether the stream, as the metadata store defines it, holds the
    /// record of `author` at `sequence` durably on at least one replica.
    pub(crate) fn stream_holds(&self, sequence: Sequence, author: Epoch) -> bool {
        self.segments().any(|(index, segment)| {
            segment.contains(sequence)
                && segment.epoch == author
                && self
                    .copies
                    .iter()
                    .any(|replica_copies| replica_copies[index].flushed_end >= sequence)
        })
    }

    /// Identifies the state up to the order of the replicas, which are
    /// interchangeable: two states that only differ in which replica is
    /// which have the same future, and are explored once.
    pub(crate) fn fingerprint(&self) -> Vec<u8> {
        let mut fingerprint = Vec::with_capacity(128);
        fingerprint.extend([self.metadata.epoch, self.metadata.standby_segments]);
        for segment in self.metadata.segments {
            match segment {
                Some(segment) => fingerprint.extend([
                    segment.epoch,
                    segment.first_sequence,
                    encode_optional(segment.sealed_end),
                ]),
                None => fingerprint.extend([ABSENT; 3]),
            }
        }

        let first_writer = self.first_writer;
        fingerprint.extend([
            encode_index(first_writer.appender.segment),
            first_writer.appender.assigned_end,
            first_writer.appender.durable,
        ]);
        match first_writer.rolled_from {
            Some((segment, offered_through)) => {
                fingerprint.extend([encode_index(segment), offered_through]);
            }
            None => fingerprint.extend([ABSENT; 2]),
        }

        let recovering_writer = self.recovering_writer;
        fingerprint.push(
            recovering_writer
                .target_segment
                .map_or(ABSENT, encode_index),
        );
        match recovering_writer.phase {
            RecoveryPhase::NotStarted => fingerprint.push(0),
            RecoveryPhase::Fencing { .. } => fingerprint.push(1),
            RecoveryPhase::Repairing { recovery_end, .. } => {
                fingerprint.extend([2, recovery_end]);
            }
            RecoveryPhase::Sealed { recovery_end } => fingerprint.extend([3, recovery_end]),
            RecoveryPhase::Writing(appender) => fingerprint.extend([
                4,
                encode_index(appender.segment),
                appender.assigned_end,
                appender.durable,
            ]),
        }

        let observations = self.observations;
        fingerprint.extend(observations.committed.map(encode_optional));
        fingerprint.extend(observations.ever_readable.map(encode_optional));
        fingerprint.extend(observations.assigned_after_takeover.map(u8::from));
        fingerprint.push(match observations.breach {
            None => 0,
            Some(Breach::SequenceCommittedTwice) => 1,
            Some(Breach::ReadersDisagree) => 2,
            Some(Breach::FencedReplicaPersistedAnOlderEpoch) => 3,
        });

        let mut replica_views: [ReplicaView; REPLICA_COUNT] =
            std::array::from_fn(|replica| self.replica_view(replica));
        replica_views.sort_unstable();
        for replica_view in replica_views {
            fingerprint.extend(replica_view);
        }
        fingerprint
    }

    /// Everything in the state that belongs to one replica: its copies, and
    /// what each writer believes about it.
    fn replica_view(&self, replica: usize) -> ReplicaView {
        let mut view = [0; REPLICA_VIEW_BYTES];
        for (segment, copy) in self.copies[replica].into_iter().enumerate() {
            view[segment * COPY_BYTES..(segment + 1) * COPY_BYTES].copy_from_slice(&[
                copy.local_end,
                copy.flushed_end,
                copy.fenced_epoch,
                u8::from(copy.knows_seal),
                copy.known_durable,
                copy.acknowledged_to_owner,
            ]);
        }
        view[MAX_SEGMENTS * COPY_BYTES] = self.first_writer.appender.acknowledged_ends[replica];
        view[MAX_SEGMENTS * COPY_BYTES + 1] = match self.recovering_writer.phase {
            RecoveryPhase::NotStarted | RecoveryPhase::Sealed { .. } => 0,
            RecoveryPhase::Fencing { replies } => encode_optional(replies[replica]),
            RecoveryPhase::Repairing { responders, .. } => u8::from(responders[replica]),
            RecoveryPhase::Writing(appender) => appender.acknowledged_ends[replica],
        };
        view
    }
}

/// How many bytes describe one segment copy in a fingerprint.
const COPY_BYTES: usize = 6;

/// How many bytes describe one replica in a fingerprint: its copies, and
/// one byte for what each of the two writers believes about it.
const REPLICA_VIEW_BYTES: usize = MAX_SEGMENTS * COPY_BYTES + 2;

type ReplicaView = [u8; REPLICA_VIEW_BYTES];

/// How an absent value is written in a fingerprint. No sequence number,
/// epoch or segment position of the model is this large.
const ABSENT: u8 = u8::MAX;

fn encode_optional(value: Option<u8>) -> u8 {
    value.unwrap_or(ABSENT)
}

fn encode_index(index: usize) -> u8 {
    u8::try_from(index).expect("segment positions are small")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn initial() -> State {
        State::initial(Bounds::ROLL_RACING_FAILOVER)
    }

    #[test]
    fn model_starts_with_one_open_empty_segment_owned_by_the_first_epoch() {
        let state = initial();
        assert_eq!(
            state.segments().collect::<Vec<_>>(),
            [(
                0,
                SegmentRecord {
                    epoch: FIRST_EPOCH,
                    first_sequence: 1,
                    sealed_end: None,
                }
            )]
        );
    }

    #[test]
    fn open_segment_contains_everything_from_its_start() {
        let open = SegmentRecord {
            epoch: FIRST_EPOCH,
            first_sequence: 2,
            sealed_end: None,
        };
        assert!(!open.contains(1) && open.contains(2) && open.contains(4));
    }

    #[test]
    fn sealed_segment_contains_exactly_its_range() {
        let sealed = SegmentRecord {
            epoch: FIRST_EPOCH,
            first_sequence: 2,
            sealed_end: Some(3),
        };
        let contained: Vec<bool> = (1..=4).map(|sequence| sealed.contains(sequence)).collect();
        assert_eq!(contained, [false, true, true, false]);
    }

    #[test]
    fn replica_serves_an_open_segment_only_up_to_the_durable_sequence_it_knows() {
        let mut state = initial();
        state.copies[0][0].local_end = 2;
        state.copies[0][0].known_durable = 1;
        assert_eq!(state.readable_end(0, 0), 1);
    }

    #[test]
    fn replica_that_knows_the_seal_serves_its_whole_copy() {
        let mut state = initial();
        state.copies[0][0].local_end = 2;
        state.copies[0][0].knows_seal = true;
        assert_eq!(state.readable_end(0, 0), 2);
    }

    #[test]
    fn readers_that_are_served_different_records_are_noticed() {
        let mut state = initial();
        state.metadata.segments[0] = Some(SegmentRecord {
            sealed_end: Some(0),
            ..state.segment(0)
        });
        let second_segment = state.open_segment(SECOND_EPOCH, 1);
        // One replica still serves the first writer's record at sequence 1,
        // another serves the second writer's record at the same sequence.
        state.copies[0][0].local_end = 1;
        state.copies[0][0].known_durable = 1;
        state.copies[1][second_segment].local_end = 1;
        state.copies[1][second_segment].known_durable = 1;
        state.absorb_readable_records();
        assert_eq!(state.observations.breach, Some(Breach::ReadersDisagree));
    }

    #[test]
    fn stream_holds_a_record_that_a_replica_has_flushed_in_its_segment() {
        let mut state = initial();
        state.copies[2][0].flushed_end = 1;
        assert!(state.stream_holds(1, FIRST_EPOCH));
    }

    #[test]
    fn stream_does_not_hold_a_record_cut_off_by_a_seal() {
        let mut state = initial();
        state.copies[2][0].flushed_end = 1;
        state.metadata.segments[0] = Some(SegmentRecord {
            sealed_end: Some(0),
            ..state.segment(0)
        });
        assert!(!state.stream_holds(1, FIRST_EPOCH));
    }

    #[test]
    fn states_that_differ_only_in_replica_order_have_one_fingerprint() {
        let mut one_order = initial();
        one_order.copies[0][0].local_end = 2;
        one_order.first_writer.appender.acknowledged_ends[0] = 1;
        let mut other_order = initial();
        other_order.copies[2][0].local_end = 2;
        other_order.first_writer.appender.acknowledged_ends[2] = 1;
        assert_eq!(one_order.fingerprint(), other_order.fingerprint());
    }

    #[test]
    fn states_that_differ_in_what_a_replica_holds_have_different_fingerprints() {
        let mut longer_copy = initial();
        longer_copy.copies[0][0].local_end = 2;
        assert_ne!(initial().fingerprint(), longer_copy.fingerprint());
    }

    #[test]
    fn states_that_pair_copies_and_acknowledgements_differently_differ() {
        let mut same_replica = initial();
        same_replica.copies[0][0].local_end = 2;
        same_replica.first_writer.appender.acknowledged_ends[0] = 1;
        let mut different_replicas = initial();
        different_replicas.copies[0][0].local_end = 2;
        different_replicas.first_writer.appender.acknowledged_ends[1] = 1;
        assert_ne!(same_replica.fingerprint(), different_replicas.fingerprint());
    }
}
