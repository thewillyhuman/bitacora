//! I2 Uniqueness: a sequence number holds one record, the same for every reader, for ever.
//!
//! # Why this file exists
//!
//! Two readers of one log must see one log. If one reader is handed record `a` at sequence number 9 and another is
//! handed `b`, then two replicas served different histories, and whatever the readers build from the log diverges. The
//! protocol prevents that with epochs and with the rule that a new writer first recovers the tail of its predecessor; a
//! mistake there shows as exactly this disagreement, and nowhere else as plainly.
//!
//! # What it contains
//!
//! - [`require_agreement`]: holds one record that was read against what was read at its sequence number before.
//!
//! # How it works
//!
//! The checker remembers, for every sequence number, the record of the first session that read it. Every later read of
//! that sequence number, by any session, at any time, must be the same record. The first read is never replaced, so
//! every reader that differs from it is reported, each against the first.
//!
//! # Invariants
//!
//! - The rule holds for fillers too: a filler may appear at many sequence numbers, but at each of them every reader
//!   must find it.
//! - Agreement is on the payload. Records are told apart by nothing else.
//!
//! # See also
//!
//! - `checker.rs`: applies the rule to every record read, in the order of the history.
//! - `observations.rs`: the map of first reads, which this rule fills.
//! - `durability.rs`: holds the same map against what the writers were told.

use std::collections::BTreeMap;

use bitacora_types::SequenceNumber;
use bytes::Bytes;

use super::Violation;
use super::observations::FirstRead;

/// Requires that the record `session` read at `sequence` is the record that was read there before, and remembers it if
/// it is the first.
///
/// # Arguments
///
/// * `stream` - What was first read at each sequence number so far. The read is added if the sequence number is new.
/// * `session` - The session that read the record.
/// * `sequence` - The sequence number of the record.
/// * `payload` - The payload the session was handed.
///
/// # Returns
///
/// [`Violation::ReadersDisagree`] if an earlier read of `sequence` found another payload; `None` if this is the first
/// read of `sequence` or it agrees with the first.
pub(super) fn require_agreement(
    stream: &mut BTreeMap<SequenceNumber, FirstRead>,
    session: u64,
    sequence: SequenceNumber,
    payload: &Bytes,
) -> Option<Violation> {
    let Some(first_read) = stream.get(&sequence) else {
        stream.insert(
            sequence,
            FirstRead {
                payload: payload.clone(),
                session,
            },
        );
        return None;
    };
    if first_read.payload == *payload {
        return None;
    }
    Some(Violation::ReadersDisagree {
        sequence,
        first_session: first_read.session,
        second_session: session,
    })
}

#[cfg(test)]
#[path = "uniqueness_test.rs"]
mod tests;
