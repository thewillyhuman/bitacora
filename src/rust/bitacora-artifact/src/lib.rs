//! The artifact store: immutable objects addressed by key.
//!
//! Tiered log segments and the files materializers write (layer files,
//! index segments, checkpoints) all live behind one small interface,
//! [`ArtifactStore`], so that the same code runs against a directory in a
//! test and against an object store in production (design, section 11,
//! "Artifact store").
//!
//! - [`MemoryArtifactStore`] is for unit tests and simulation.
//! - [`LocalArtifactStore`] keeps objects in a local directory, for
//!   development clusters.
//! - [`contract::check`] is the behaviour every backend must have; each
//!   backend runs it in its own tests.

pub mod contract;
mod local_store;
mod memory_store;
mod store;

pub use local_store::LocalArtifactStore;
pub use memory_store::MemoryArtifactStore;
pub use store::{ArtifactError, ArtifactStore, ObjectInfo, validate_key};
