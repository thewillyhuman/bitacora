//! The artifact store: immutable objects addressed by key.
//!
//! # Why this crate exists
//!
//! Tiered log segments and the files materializers write (layer files, index segments, checkpoints) are
//! all immutable objects in an object store (design, sections 10 and 11). The code that writes and reads
//! them must run unchanged in three places: inside the deterministic simulator, on a developer's machine,
//! and against a real object store. This crate is the one small interface that makes that possible, the
//! two backends that need no network, and the executable definition of what any backend must do.
//!
//! # What it contains
//!
//! - [`ArtifactStore`]: the interface, with [`ObjectUpload`], [`ArtifactError`], [`ObjectInfo`] and
//!   [`validate_key`].
//! - [`MemoryArtifactStore`]: objects in memory, with switches to inject faults, for tests and simulation.
//! - [`LocalArtifactStore`]: objects in a directory of the local file system, for development clusters.
//! - [`contract::check`]: the behaviour every backend must have, as one function a backend runs against
//!   itself in its tests.
//!
//! # How it works
//!
//! Callers hold an `Arc<dyn ArtifactStore>` and never know which backend is behind it. A backend that
//! needs a network client lives in a crate of its own (the S3 backend), so that everything that only needs
//! the interface or the in-memory store does not compile an HTTP stack.
//!
//! # Invariants
//!
//! - The crate depends on no other bitacora crate. It knows nothing about segments or checkpoints: keys
//!   and contents are opaque to it.
//! - Every backend passes [`contract::check`]. A difference between backends is a bug in one of them.
//!
//! # See also
//!
//! - `store.rs`: the interface and the rule for keys.
//! - `contract.rs`: the shared check.
//! - `memory_store.rs` and `local_store.rs`: the two backends.

#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#![warn(rustdoc::broken_intra_doc_links)]

pub mod contract;
mod local_store;
mod memory_store;
mod store;

pub use local_store::LocalArtifactStore;
pub use memory_store::MemoryArtifactStore;
pub use store::{ArtifactError, ArtifactStore, ObjectInfo, ObjectUpload, validate_key};
