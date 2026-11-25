//! The bitacora metadata store.
//!
//! This is the only place where bitacora needs consensus (design, section
//! 8). It holds epochs, segment lists, checkpoints and the replica registry,
//! and it is touched only when a segment rolls, a writer changes or a
//! checkpoint advances: never on an append or a read.
//!
//! The crate has two layers. [`KeyValueStore`] is the small interface a
//! backend implements: linearizable reads and conditional transactions.
//! Everything bitacora-specific is built once on top of it, so every backend
//! behaves the same.

mod key_value;
mod memory_store;
mod placement;
mod records;
mod store;

pub use key_value::{
    Condition, KeyValueStore, Mutation, Revision, StoreUnavailable, TransactionOutcome,
    VersionedValue,
};
pub use memory_store::MemoryKeyValueStore;
pub use placement::{
    PlacementError, PlacementPolicy, choose_additional_replica, choose_replica_set,
};
pub use records::{
    CheckpointRecord, CopyRecord, CopyState, LeaderRecord, RECORD_SCHEMA_VERSION, ReplicaRecord,
    RetentionPolicy, SealRecord, SegmentRecord, StreamRecord, TieredRecord, WriterLease,
};
pub use store::{MAX_NAME_LENGTH_BYTES, MetaError, MetadataStore};
