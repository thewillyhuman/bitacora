//! What one pass over a history collects for the rules of the checker.
//!
//! # Why this file exists
//!
//! Most of the checker's rules compare two things that are far apart in a history: what a writer was told about an
//! append, and what readers later found in the stream. The history holds them as separate events, in the order they
//! happened. Before the rules can be applied, the events have to be brought together: each append with its outcome,
//! each sequence number with the record that was read there. This file is where they are brought together, so that
//! every rule works on the same picture.
//!
//! # What it contains
//!
//! - [`Observations`]: the appends by payload, the stream by sequence number, and the declared fillers.
//! - [`Append`]: what is known about one append call.
//! - [`FirstRead`]: what was first read at one sequence number, and by whom.
//!
//! # How it works
//!
//! Appends are keyed by their payload, because the payload is what a reader hands back: every append of a simulation
//! writes a payload of its own, so a record that is read names the append it came from. A second map, from operation
//! identifier to payload, connects the event that finishes an append to the event that began it.
//!
//! The stream is what readers found: for each sequence number, the record of the first session that read it. Later
//! sessions are compared with it (`uniqueness.rs`) and never replace it.
//!
//! # Invariants
//!
//! - The maps are ordered, so every rule that walks one finds violations in a deterministic order.
//! - A payload is kept as `Bytes`, which shares the bytes of the event: collecting copies no payload.
//! - The outcome of an append is `None` until its finishing event is seen, and stays `None` for an append whose client
//!   crashed.
//!
//! # See also
//!
//! - `checker.rs`: the pass that fills this in.
//! - `provenance.rs`, `durability.rs`, `single_writer.rs`: the rules that read it.

use std::collections::{BTreeMap, BTreeSet};

use bitacora_types::SequenceNumber;
use bytes::Bytes;

use crate::history::AppendOutcome;

/// What is known about one append call.
pub(super) struct Append {
    /// The identifier of the call in the history.
    pub(super) operation: u64,
    /// What the caller was told, or `None` if the call never returned.
    pub(super) outcome: Option<AppendOutcome>,
}

/// The record that was first read at one sequence number.
pub(super) struct FirstRead {
    /// The payload the first session to read the sequence number was handed.
    pub(super) payload: Bytes,
    /// That session.
    pub(super) session: u64,
}

/// What the readers of a history saw, and what its writers were told.
#[derive(Default)]
pub(super) struct Observations {
    /// Every append of the history, by the payload it wrote.
    pub(super) appends_by_payload: BTreeMap<Bytes, Append>,
    /// What the stream holds at each sequence number, and who read it first.
    pub(super) stream: BTreeMap<SequenceNumber, FirstRead>,
    /// The payloads declared to be fillers: nobody has to have appended them, and they may repeat.
    pub(super) fillers: BTreeSet<Bytes>,
    /// The payload of every append, by the identifier of the call, to find the append an outcome belongs to.
    payload_by_operation: BTreeMap<u64, Bytes>,
}

impl Observations {
    /// Notes that the append `operation` of `payload` began.
    ///
    /// # Arguments
    ///
    /// * `operation` - The identifier of the call.
    /// * `payload` - The payload of the record. A second append of the same payload replaces the first.
    pub(super) fn note_append_invoked(&mut self, operation: u64, payload: &Bytes) {
        self.appends_by_payload.insert(
            payload.clone(),
            Append {
                operation,
                outcome: None,
            },
        );
        self.payload_by_operation.insert(operation, payload.clone());
    }

    /// Notes how the append `operation` ended. An outcome for a call that never began is ignored.
    ///
    /// # Arguments
    ///
    /// * `operation` - The identifier of the call.
    /// * `outcome` - What the caller was told.
    pub(super) fn note_append_finished(&mut self, operation: u64, outcome: &AppendOutcome) {
        let Some(payload) = self.payload_by_operation.get(&operation) else {
            return;
        };
        if let Some(append) = self.appends_by_payload.get_mut(payload) {
            append.outcome = Some(outcome.clone());
        }
    }

    /// Notes that records with `payload` are fillers.
    ///
    /// # Arguments
    ///
    /// * `payload` - The payload every such record has.
    pub(super) fn note_filler(&mut self, payload: &Bytes) {
        self.fillers.insert(payload.clone());
    }
}

#[cfg(test)]
#[path = "observations_test.rs"]
mod tests;
