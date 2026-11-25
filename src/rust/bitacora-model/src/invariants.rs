//! The invariants of the design (section 15), as predicates on a state of
//! the model.

use std::fmt;

use crate::state::{Breach, FIRST_EPOCH, SEQUENCE_SLOTS, Sequence, State};

/// The invariants the protocol model is checked against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Invariant {
    /// I1: a record reported as committed appears in every later read of
    /// the stream.
    Durability,
    /// I2: no two readers ever observe different records at the same
    /// sequence number.
    Uniqueness,
    /// I3: every reader observes a gapless prefix, in order, and what it
    /// could read is never taken back.
    Prefix,
    /// I4: at most one epoch can commit records at any moment.
    SingleWriter,
}

impl fmt::Display for Invariant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Durability => "I1 Durability",
            Self::Uniqueness => "I2 Uniqueness",
            Self::Prefix => "I3 Prefix",
            Self::SingleWriter => "I4 Single writer",
        })
    }
}

/// Returns the first invariant `state` violates, or `None` if it satisfies
/// them all.
pub(crate) fn violated_invariant(state: &State) -> Option<Invariant> {
    if !holds_single_writer(state) {
        return Some(Invariant::SingleWriter);
    }
    if !holds_durability(state) {
        return Some(Invariant::Durability);
    }
    if !holds_uniqueness(state) {
        return Some(Invariant::Uniqueness);
    }
    if !holds_prefix(state) {
        return Some(Invariant::Prefix);
    }
    None
}

/// Every sequence number the model can assign.
fn sequences() -> impl Iterator<Item = Sequence> {
    (1..SEQUENCE_SLOTS).map(|slot| Sequence::try_from(slot).expect("slots are few"))
}

/// I1: every record a writer reported as committed is still part of the
/// stream, stored durably, and no reader was ever handed another record in
/// its place.
fn holds_durability(state: &State) -> bool {
    sequences().all(|sequence| {
        let Some(author) = state.observations.committed[usize::from(sequence)] else {
            return true;
        };
        let was_read_as_another_record = state.observations.ever_readable[usize::from(sequence)]
            .is_some_and(|read_author| read_author != author);
        state.stream_holds(sequence, author) && !was_read_as_another_record
    })
}

/// I2: no two reads, by any reader at any time, returned different records
/// at the same sequence number.
fn holds_uniqueness(state: &State) -> bool {
    state.observations.breach != Some(Breach::ReadersDisagree)
}

/// I3: a reader that reads in order never meets a gap and is never taken
/// back. Segments tile the sequence space, so moving from one segment to
/// the next skips nothing; a replica serves a segment from its first record
/// without holes, by construction of a copy; and every record that was once
/// readable is still part of the stream.
fn holds_prefix(state: &State) -> bool {
    let segments: Vec<_> = state.segments().map(|(_index, segment)| segment).collect();
    let do_segments_tile = segments.windows(2).all(|neighbours| {
        neighbours[0]
            .sealed_end
            .is_some_and(|sealed_end| neighbours[1].first_sequence == sealed_end + 1)
    });
    let is_nothing_taken_back = sequences().all(|sequence| {
        state.observations.ever_readable[usize::from(sequence)]
            .is_none_or(|author| state.stream_holds(sequence, author))
    });
    do_segments_tile && is_nothing_taken_back
}

/// I4: two epochs never both commit. No sequence number is reported
/// committed with two different records; a replica never persists an append
/// from an epoch it was fenced against; and a record the first writer only
/// assigned after the recovering writer had fenced a recovery quorum is
/// never reported as committed.
///
/// The last clause is deliberately about records assigned after the
/// takeover. An acknowledgement that was already on its way when the fence
/// arrived may still reach the old writer, which then reports a record as
/// committed after the fence. That is allowed: the record was on a write
/// quorum before the fence, so the recovery found it.
fn holds_single_writer(state: &State) -> bool {
    let has_breach = matches!(
        state.observations.breach,
        Some(Breach::SequenceCommittedTwice | Breach::FencedReplicaPersistedAnOlderEpoch)
    );
    let did_old_epoch_commit_a_late_record = sequences().any(|sequence| {
        state.observations.assigned_after_takeover[usize::from(sequence)]
            && state.observations.committed[usize::from(sequence)] == Some(FIRST_EPOCH)
    });
    !has_breach && !did_old_epoch_commit_a_late_record
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::Bounds;
    use crate::state::{SECOND_EPOCH, SegmentRecord};

    fn initial() -> State {
        State::initial(Bounds::ROLL_RACING_FAILOVER)
    }

    /// A state in which the first writer committed sequence 1 on replicas 0
    /// and 1.
    fn state_with_a_committed_record() -> State {
        let mut state = initial();
        for replica in [0, 1] {
            state.copies[replica][0].local_end = 1;
            state.copies[replica][0].flushed_end = 1;
        }
        state.observations.record_commit(1, FIRST_EPOCH);
        state
    }

    fn seal_first_segment(state: &mut State, sealed_end: Sequence) {
        state.metadata.segments[0] = Some(SegmentRecord {
            sealed_end: Some(sealed_end),
            ..state.segment(0)
        });
    }

    #[test]
    fn initial_state_satisfies_every_invariant() {
        assert_eq!(violated_invariant(&initial()), None);
    }

    #[test]
    fn committed_record_on_a_write_quorum_satisfies_every_invariant() {
        assert_eq!(violated_invariant(&state_with_a_committed_record()), None);
    }

    #[test]
    fn committed_record_cut_off_by_a_seal_violates_durability() {
        let mut state = state_with_a_committed_record();
        seal_first_segment(&mut state, 0);
        assert_eq!(violated_invariant(&state), Some(Invariant::Durability));
    }

    #[test]
    fn committed_record_no_replica_has_flushed_violates_durability() {
        let mut state = state_with_a_committed_record();
        for replica in [0, 1] {
            state.copies[replica][0].flushed_end = 0;
        }
        assert_eq!(violated_invariant(&state), Some(Invariant::Durability));
    }

    #[test]
    fn readers_served_different_records_violate_uniqueness() {
        let mut state = initial();
        state.observations.breach = Some(Breach::ReadersDisagree);
        assert_eq!(violated_invariant(&state), Some(Invariant::Uniqueness));
    }

    #[test]
    fn readable_record_cut_off_by_a_seal_violates_the_prefix_property() {
        let mut state = initial();
        state.copies[0][0].local_end = 1;
        state.copies[0][0].flushed_end = 1;
        state.copies[0][0].known_durable = 1;
        state.absorb_readable_records();
        seal_first_segment(&mut state, 0);
        assert_eq!(violated_invariant(&state), Some(Invariant::Prefix));
    }

    #[test]
    fn segments_that_leave_a_gap_violate_the_prefix_property() {
        let mut state = initial();
        seal_first_segment(&mut state, 1);
        state.open_segment(SECOND_EPOCH, 3);
        assert_eq!(violated_invariant(&state), Some(Invariant::Prefix));
    }

    #[test]
    fn two_epochs_committing_one_sequence_violate_the_single_writer_property() {
        let mut state = state_with_a_committed_record();
        state.observations.record_commit(1, SECOND_EPOCH);
        assert_eq!(violated_invariant(&state), Some(Invariant::SingleWriter));
    }

    #[test]
    fn old_epoch_committing_a_record_assigned_after_the_takeover_violates_single_writer() {
        let mut state = state_with_a_committed_record();
        state.observations.assigned_after_takeover[1] = true;
        assert_eq!(violated_invariant(&state), Some(Invariant::SingleWriter));
    }

    #[test]
    fn fenced_replica_persisting_an_older_epoch_violates_single_writer() {
        let mut state = initial();
        state.observations.breach = Some(Breach::FencedReplicaPersistedAnOlderEpoch);
        assert_eq!(violated_invariant(&state), Some(Invariant::SingleWriter));
    }

    #[test]
    fn invariants_are_named_as_in_the_design() {
        let names: Vec<String> = [
            Invariant::Durability,
            Invariant::Uniqueness,
            Invariant::Prefix,
            Invariant::SingleWriter,
        ]
        .iter()
        .map(ToString::to_string)
        .collect();
        assert_eq!(
            names,
            [
                "I1 Durability",
                "I2 Uniqueness",
                "I3 Prefix",
                "I4 Single writer"
            ]
        );
    }
}
