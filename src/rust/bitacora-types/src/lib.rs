//! Identifiers, positions, records and errors shared by every bitacora crate.
//!
//! This crate is the vocabulary of the system. It holds no behaviour beyond
//! validation and arithmetic on positions, and it depends on nothing else in
//! the workspace, so every other crate can depend on it.

mod identifiers;
mod random;
mod records;
pub mod replica_api;
mod replication;
mod time;

pub use identifiers::{
    Epoch, InvalidReplicaId, ReplicaId, SegmentAddress, SegmentId, SequenceNumber, StreamId,
};
pub use random::DeterministicRandom;
pub use records::{InvalidSequenceRange, LogRecord, MAX_RECORD_PAYLOAD_BYTES, SequenceRange};
pub use replication::{InvalidReplicationSettings, ReplicationSettings};
pub use time::{Clock, ManualClock, SystemClock, Timestamp};
