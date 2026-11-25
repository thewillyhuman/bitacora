//! The invariants of the design, as predicates on one state of the model.
//!
//! # Why this file exists
//!
//! "Correct" has to be something a program can decide. The design names the properties people rely on
//! when they trust bitacora with their only copy of committed data (section 15): a committed record is
//! never lost, readers never disagree, a reader sees a gapless prefix, and only one writer commits at a
//! time. This file states each of them precisely enough to be evaluated in every state the explorer
//! reaches. If one of them is wrong here, the model proves the wrong thing, so each is written to be read
//! next to the design's sentence.
//!
//! # What it contains
//!
//! - [`Invariant`]: the four invariants the model checks, I1 to I4, with the design's names.
//! - `violated_invariant`: the first invariant a state violates, if any.
//! - One private predicate per invariant.
//!
//! # How it works
//!
//! The design states its invariants about histories: "appears in every later read", "never observe". The
//! model states them about single states, which is what an explorer can check. The bridge is the
//! `Observations` part of a state: every step records what writers reported as committed, what any replica
//! could have served to a reader, and the few events that leave no trace in the state itself. A predicate
//! then compares those observations with what the stream holds now.
//!
//! I5 (safe truncation) and I6 (determinism of materializers) are outside the model: it has no truncation
//! and no materializer.
//!
//! The predicates run once for every state the explorer discovers. They do not allocate.
//!
//! # Invariants
//!
//! - A predicate reads the state and nothing else. It has no memory between states.
//! - `violated_invariant` checks the single-writer property first, then durability, uniqueness and the
//!   prefix property. A state that violates several is reported under the first of them in that order,
//!   which the tests of the broken rules rely on.
//!
//! # See also
//!
//! - `state.rs`: the observations these predicates read, and `stream_holds`.
//! - `transitions.rs`: where the observations are recorded.
//! - `docs/decisions/0001-protocol-model.md`: why I4 is stated about records assigned after the takeover.

use std::fmt;

use crate::state::{Breach, FIRST_EPOCH, SEQUENCE_SLOTS, Sequence, State};

/// The slot of the first sequence number. Slot 0 is the position before the first record and holds no
/// record.
const FIRST_SEQUENCE_SLOT: usize = 1;

/// An invariant the protocol model is checked against.
///
/// The names and numbers are the design's (section 15). `Display` prints them as the design does, so a
/// counterexample can be matched with the text.
///
/// # Examples
///
/// ```
/// use bitacora_model::Invariant;
///
/// assert_eq!(Invariant::Durability.to_string(), "I1 Durability");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Invariant {
    /// I1: a record reported as committed appears in every later read of the stream.
    Durability,
    /// I2: no two readers ever observe different records at the same sequence number.
    Uniqueness,
    /// I3: every reader observes a gapless prefix, in order, and what it could read is never taken back.
    Prefix,
    /// I4: at most one epoch can commit records at any moment.
    SingleWriter,
}

impl fmt::Display for Invariant {
    /// Writes the invariant as the design names it, number first.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Durability => "I1 Durability",
            Self::Uniqueness => "I2 Uniqueness",
            Self::Prefix => "I3 Prefix",
            Self::SingleWriter => "I4 Single writer",
        })
    }
}

/// Finds the first invariant a state violates.
///
/// Checks the single-writer property, then durability, uniqueness and the prefix property, and stops at
/// the first that fails. Runs for every state the explorer discovers.
///
/// # Arguments
///
/// * `state` - The state to check.
///
/// # Returns
///
/// The violated invariant, or `None` if the state satisfies all four.
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

/// Every sequence number the model can assign, in order.
fn sequences() -> impl Iterator<Item = Sequence> {
    (FIRST_SEQUENCE_SLOT..SEQUENCE_SLOTS)
        .map(|slot| Sequence::try_from(slot).expect("slots are few"))
}

/// I1: every record a writer reported as committed is still part of the stream, stored durably, and no
/// reader was ever handed another record in its place.
fn holds_durability(state: &State) -> bool {
    for sequence in sequences() {
        let Some(author) = state.observations.committed[usize::from(sequence)] else {
            continue;
        };
        let readable_author = state.observations.ever_readable[usize::from(sequence)];
        let was_read_as_another_record =
            readable_author.is_some_and(|read_author| read_author != author);
        if !state.stream_holds(sequence, author) || was_read_as_another_record {
            return false;
        }
    }
    true
}

/// I2: no two reads, by any reader at any time, returned different records at the same sequence number.
///
/// A disagreement is an event, so the step that makes it possible records it as a breach, and this
/// predicate only looks for the mark.
fn holds_uniqueness(state: &State) -> bool {
    state.observations.breach != Some(Breach::ReadersDisagree)
}

/// I3: a reader that reads in order never meets a gap and is never taken back.
///
/// Segments tile the sequence space, so moving from one segment to the next skips nothing; a replica
/// serves a segment from its first record without holes, by construction of a copy; and every record that
/// was once readable is still part of the stream.
fn holds_prefix(state: &State) -> bool {
    do_segments_tile(state) && is_nothing_taken_back(state)
}

/// Whether every segment but the last is sealed, and each segment starts right after the one before it
/// ends.
fn do_segments_tile(state: &State) -> bool {
    for later_segment in 1..state.segment_count() {
        let earlier_record = state.segment(later_segment - 1);
        let later_record = state.segment(later_segment);
        let Some(sealed_end) = earlier_record.sealed_end else {
            return false;
        };
        if later_record.first_sequence != sealed_end + 1 {
            return false;
        }
    }
    true
}

/// Whether every record that some replica could once serve to a reader is still held by the stream.
fn is_nothing_taken_back(state: &State) -> bool {
    for sequence in sequences() {
        let Some(author) = state.observations.ever_readable[usize::from(sequence)] else {
            continue;
        };
        if !state.stream_holds(sequence, author) {
            return false;
        }
    }
    true
}

/// I4: two epochs never both commit.
///
/// No sequence number is reported committed with two different records; a replica never persists an
/// append from an epoch it was fenced against; and a record the first writer only assigned after the
/// recovering writer had fenced a recovery quorum is never reported as committed.
///
/// The last clause is deliberately about records *assigned* after the takeover. An acknowledgement that
/// was already on its way when the fence arrived may still reach the old writer, which then reports a
/// record as committed after the fence. That is allowed: the record was on a write quorum before the
/// fence, so the recovery found it.
fn holds_single_writer(state: &State) -> bool {
    let has_breach = matches!(
        state.observations.breach,
        Some(Breach::SequenceCommittedTwice | Breach::FencedReplicaPersistedAnOlderEpoch)
    );
    !has_breach && !did_old_epoch_commit_a_late_record(state)
}

/// Whether the first writer reported as committed a record it assigned after the recovering writer had
/// fenced a recovery quorum.
fn did_old_epoch_commit_a_late_record(state: &State) -> bool {
    for sequence in sequences() {
        let was_assigned_after_takeover =
            state.observations.assigned_after_takeover[usize::from(sequence)];
        let committed_author = state.observations.committed[usize::from(sequence)];
        if was_assigned_after_takeover && committed_author == Some(FIRST_EPOCH) {
            return true;
        }
    }
    false
}

#[cfg(test)]
#[path = "invariants_test.rs"]
mod tests;
