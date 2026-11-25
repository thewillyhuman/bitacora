//! Identifiers, positions, records and errors shared by every bitacora crate.
//!
//! This crate is the vocabulary of the system. It holds no behaviour beyond
//! validation and arithmetic on positions, and it depends on nothing else in
//! the workspace, so every other crate can depend on it.

mod identifiers;

pub use identifiers::{Epoch, InvalidReplicaId, ReplicaId, SegmentId, SequenceNumber, StreamId};
