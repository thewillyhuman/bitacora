//! I4 Single writer: at any moment at most one writer can commit.
//!
//! # Why this file exists
//!
//! A stream has one writer at a time, and the sequence numbers are that writer's to give out. Two writers committing at
//! once is the failure the whole epoch and fencing protocol exists to prevent (design, section 9): it is how two
//! callers are both told "your record is number 12". An observer cannot see who held the lease, but it can see the two
//! consequences such an overlap must have, and this file checks for both.
//!
//! # What it contains
//!
//! - [`Commits`]: the appends that were reported as committed, by sequence number.
//! - [`Commits::note_commit`]: no two appends may be reported committed at one sequence number.
//! - [`require_epochs_in_order`]: along the stream, the epoch of the committing writer never decreases.
//!
//! # How it works
//!
//! Every append that was reported as committed is entered under its sequence number. A second append for a sequence
//! number that is taken is a violation; the first keeps its place.
//!
//! Then the commits are walked in the order of the stream. A new writer takes over at a higher epoch and appends after
//! everything its predecessor committed, so the epochs along the stream can only stay or rise. A commit at a lower
//! epoch than one before it in the stream means the older writer was still able to commit after the newer one had: both
//! were writers at once. The walk remembers the newest epoch it has seen and holds every later commit against it, so a
//! run of commits by the stale writer is reported commit by commit.
//!
//! # Invariants
//!
//! - Only appends reported as committed are considered. What a fenced writer was told is "unknown", and proves nothing.
//! - The two operations of a [`Violation::SequenceCommittedTwice`] are named smaller identifier first, whichever order
//!   they were met in.
//!
//! # See also
//!
//! - `checker.rs`: enters every committed append and then asks for the walk.
//! - `durability.rs`: what a commit promises about the stream.

use std::collections::BTreeMap;

use bitacora_types::{Epoch, SequenceNumber};

use super::Violation;

/// One append that was reported as committed.
struct Commit {
    /// The identifier of the call.
    operation: u64,
    /// The epoch of the writer that reported it.
    epoch: Epoch,
}

/// The appends of a history that were reported as committed, by the sequence number they were reported at.
#[derive(Default)]
pub(super) struct Commits {
    /// The first append reported committed at each sequence number.
    by_sequence: BTreeMap<SequenceNumber, Commit>,
}

impl Commits {
    /// Enters an append that was reported committed at `sequence`, unless the sequence number is taken.
    ///
    /// # Arguments
    ///
    /// * `sequence` - Where the append was reported committed.
    /// * `operation` - The identifier of the call.
    /// * `epoch` - The epoch of the writer that reported it.
    ///
    /// # Returns
    ///
    /// [`Violation::SequenceCommittedTwice`] if another append was already entered at `sequence`; that append keeps its
    /// place. `None` if the sequence number was free.
    pub(super) fn note_commit(&mut self, sequence: SequenceNumber, operation: u64, epoch: Epoch) -> Option<Violation> {
        if let Some(first_commit) = self.by_sequence.get(&sequence) {
            return Some(Violation::SequenceCommittedTwice {
                sequence,
                first_operation: first_commit.operation.min(operation),
                second_operation: first_commit.operation.max(operation),
            });
        }
        self.by_sequence.insert(sequence, Commit { operation, epoch });
        None
    }
}

/// Requires that, along the stream, the committing epoch never decreases.
///
/// # Arguments
///
/// * `commits` - The committed appends of a history.
/// * `violations` - Where a [`Violation::EpochWentBackwards`] is added for every commit at a lower epoch than a commit
///   before it in the stream.
pub(super) fn require_epochs_in_order(commits: &Commits, violations: &mut Vec<Violation>) {
    let mut newest: Option<(SequenceNumber, Epoch)> = None;
    for (sequence, commit) in &commits.by_sequence {
        match newest {
            Some((earlier_sequence, earlier_epoch)) if commit.epoch < earlier_epoch => {
                violations.push(Violation::EpochWentBackwards {
                    earlier_sequence,
                    earlier_epoch,
                    later_sequence: *sequence,
                    later_epoch: commit.epoch,
                });
            }
            _ => newest = Some((*sequence, commit.epoch)),
        }
    }
}

#[cfg(test)]
#[path = "single_writer_test.rs"]
mod tests;
