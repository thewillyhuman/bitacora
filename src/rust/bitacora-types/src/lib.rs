//! The vocabulary of bitacora: identifiers, positions, records, time, randomness and the replica contract.
//!
//! # Why this crate exists
//!
//! Every crate of the workspace talks about streams, segments, epochs, sequence numbers and replicas, and
//! most of them talk to replicas. Those notions must mean exactly the same thing in the segment format,
//! the replica, the client, the controller and the simulator. They live here, in the one crate that
//! depends on nothing else in the workspace, so that every other crate can depend on it and none of them
//! has to define a sequence number of its own.
//!
//! It also holds the three things a deterministic simulation has to control, as interfaces or in-tree
//! implementations: the clock, the random source, and the way to reach a replica (design, section 15).
//!
//! # What it contains
//!
//! - Identifiers and positions: [`StreamId`], [`SegmentId`], [`SegmentAddress`], [`Epoch`],
//!   [`SequenceNumber`], [`ReplicaId`].
//! - Records: [`LogRecord`], [`SequenceRange`], [`MAX_RECORD_PAYLOAD_BYTES`].
//! - Replication: [`ReplicationSettings`], which holds the quorum arithmetic.
//! - Time: [`Timestamp`], the [`Clock`] interface, [`SystemClock`] and [`ManualClock`].
//! - Randomness: [`DeterministicRandom`], the only random source in the workspace.
//! - [`replica_api`]: the operations a replica offers, their requests, answers and errors, and how a
//!   handle to a replica is obtained.
//!
//! # How it works
//!
//! The crate is types and small pure functions. It holds no behaviour beyond validation and arithmetic on
//! positions, does no I/O, and starts no task. Values that have rules (a replica identifier, a range, the
//! replication settings) can only be built through a function that checks them, also when they are
//! deserialized, so the rest of the workspace receives valid values and does not check again.
//!
//! # Invariants
//!
//! - The crate depends on no other bitacora crate, and must stay that way.
//! - The serialized form of every type here that derives `serde` traits is part of the metadata schema; the
//!   golden tests in `bitacora-meta` hold it.
//! - No I/O: the one exception is [`SystemClock`], which reads the system clock and is the only code in the
//!   workspace allowed to.
//!
//! # See also
//!
//! - `identifiers.rs`, `records.rs`, `replication.rs`, `time.rs`, `random.rs`, `replica_api.rs`: one file
//!   per group of the list above.
//! - `docs/protocol/README.md`: the same vocabulary in prose.

#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#![warn(rustdoc::broken_intra_doc_links)]

mod identifiers;
mod random;
mod records;
pub mod replica_api;
mod replication;
mod time;

pub use identifiers::{Epoch, InvalidReplicaId, ReplicaId, SegmentAddress, SegmentId, SequenceNumber, StreamId};
pub use random::DeterministicRandom;
pub use records::{InvalidSequenceRange, LogRecord, MAX_RECORD_PAYLOAD_BYTES, SequenceRange};
pub use replication::{InvalidReplicationSettings, ReplicationSettings};
pub use time::{Clock, ManualClock, SystemClock, Timestamp};
