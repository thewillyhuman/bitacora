//! The bitacora metadata store.
//!
//! # Why this crate exists
//!
//! The append path of bitacora needs no consensus: one writer sends records to a fixed set of replicas and counts
//! acknowledgements. That only works because the hard questions are settled somewhere else, before and after: who is
//! the writer, which replicas hold the segment, where a sealed segment ends, how far each materializer got. Those
//! answers must be the same for everybody and must change atomically, which is consensus. This crate is the one place
//! where bitacora uses it (design, section 8). It is touched only when a segment rolls, a writer changes, a checkpoint
//! advances or the controller does its housekeeping: never on an append or a read.
//!
//! # What it contains
//!
//! - [`MetadataStore`]: the operations of the protocol, each one a conditional write.
//! - [`MetaError`]: how those operations fail.
//! - [`StreamRecord`], [`SegmentRecord`], [`CheckpointRecord`], [`ReplicaRecord`], [`LeaderRecord`] and the smaller
//!   records they are made of: what is stored.
//! - [`KeyValueStore`]: the small interface a backend implements, with [`MemoryKeyValueStore`] for tests and
//!   simulation.
//! - [`choose_replica_set`] and [`choose_additional_replica`]: placement, a pure function of the replica registry.
//! - [`MetadataSnapshot`]: the whole store as one object, for backup.
//!
//! # How it works
//!
//! The crate has two layers. [`KeyValueStore`] asks a backend for very little: linearizable reads and a transaction
//! that applies some writes only if some keys are unchanged. Everything bitacora-specific is built once on top of it,
//! in [`MetadataStore`], so that every backend behaves the same: a writer's change is valid only while the stream's
//! epoch is still the writer's, a materializer's only while its fencing token is current, and both are enforced by
//! making the write conditional on the record that the decision was read from.
//!
//! # Invariants
//!
//! - Every change to a record that a decision depends on is a transaction conditional on the revision at which the
//!   record was read. This is what makes a paused or partitioned process harmless when it wakes up.
//! - Epochs, low-water marks, checkpoints and segment identifiers never decrease.
//! - The crate depends on `bitacora-types` only. It knows nothing of networks, disks or clocks: callers pass the time.
//!
//! # See also
//!
//! - `key_value.rs`, `memory_store.rs`: the backend interface and the in-memory backend.
//! - `records.rs`, `keys.rs`: what is stored, and where.
//! - `store.rs` and the files under `store/`: the operations.
//! - `placement.rs`: choosing replicas. `snapshot.rs`: the backup format. `error.rs`: failures.
//! - `docs/protocol/metadata.md`: the keys, records and transitions for readers who do not read Rust.

#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#![warn(rustdoc::broken_intra_doc_links)]

mod error;
mod key_value;
mod keys;
mod memory_store;
mod placement;
mod records;
mod snapshot;
mod store;

pub use error::MetaError;
pub use key_value::{
    Condition, KeyValueStore, Mutation, Revision, StoreUnavailable, TransactionOutcome, VersionedValue,
};
pub use keys::MAX_NAME_LENGTH_BYTES;
pub use memory_store::MemoryKeyValueStore;
pub use placement::{PlacementError, PlacementPolicy, choose_additional_replica, choose_replica_set, spread_of};
pub use records::{
    CheckpointRecord, CopyRecord, CopyState, DeletedStreamRecord, LeaderRecord, RECORD_SCHEMA_VERSION, ReplicaRecord,
    RetentionPolicy, SealRecord, SegmentRecord, StreamRecord, TieredRecord, WriterLease,
};
pub use snapshot::{
    MetadataSnapshot, SNAPSHOT_FORMAT_VERSION, SNAPSHOT_KEY_PREFIX, SNAPSHOT_MAGIC, SnapshotEntry, SnapshotError,
    snapshot_object_key, snapshot_time_of,
};
pub use store::{MetadataStore, RestoreMode, RestoreOutcome};
