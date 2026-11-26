//! I3 Prefix: every reader observes a gapless prefix of the stream, in order.
//!
//! # Why this file exists
//!
//! A log is useful because a reader can replay it: start at a position and be handed every record from there on, each
//! once, in order. A reader that is handed record 7 after record 5 has lost record 6 without knowing, and whatever it
//! builds from the log is wrong from then on. The reader of the client crosses segments, replicas and the object store
//! to keep that promise, under faults, and this rule is what checks that it does.
//!
//! # What it contains
//!
//! - [`ReadPositions`]: for each reading session, the sequence number it must be handed next.
//!
//! # How it works
//!
//! A session begins at the position it asked for. Every record it is handed must be exactly the one it is waiting for;
//! after a record, it waits for the following one. A record out of turn is a violation, and the session then waits for
//! the record after the one it was handed, so one gap is reported once and not for every record that follows it.
//!
//! # Invariants
//!
//! - The rule is about one session at a time: sessions do not have to start at the same place or read equally far.
//! - A record read in a session that was never recorded as started is expected at the first sequence number.
//!
//! # See also
//!
//! - `checker.rs`: applies the rule to every record read, in the order of the history.
//! - `uniqueness.rs`: the rule between sessions.

use std::collections::BTreeMap;

use bitacora_types::SequenceNumber;

use super::Violation;

/// Where each reading session of a history stands.
#[derive(Default)]
pub(super) struct ReadPositions {
    /// The sequence number each session must be handed next, by session identifier.
    next_of_session: BTreeMap<u64, SequenceNumber>,
}

impl ReadPositions {
    /// Notes that `session` begins at `from_sequence`.
    ///
    /// # Arguments
    ///
    /// * `session` - The identifier of the session.
    /// * `from_sequence` - The first sequence number the session asked for, and so the first it must be handed.
    pub(super) fn start_session(&mut self, session: u64, from_sequence: SequenceNumber) {
        self.next_of_session.insert(session, from_sequence);
    }

    /// Requires that the record `session` was handed is the one it was waiting for, and moves the session past it.
    ///
    /// # Arguments
    ///
    /// * `session` - The identifier of the session.
    /// * `sequence` - The sequence number of the record it was handed.
    ///
    /// # Returns
    ///
    /// [`Violation::ReaderOutOfOrder`] if the session was waiting for another sequence number; `None` if the record is
    /// in order.
    pub(super) fn require_next_in_order(&mut self, session: u64, sequence: SequenceNumber) -> Option<Violation> {
        let expected = match self.next_of_session.get(&session) {
            Some(next_sequence) => *next_sequence,
            None => SequenceNumber::FIRST,
        };
        self.next_of_session.insert(session, sequence.next());
        if sequence == expected {
            return None;
        }
        Some(Violation::ReaderOutOfOrder {
            session,
            expected,
            found: sequence,
        })
    }
}

#[cfg(test)]
#[path = "prefix_test.rs"]
mod tests;
