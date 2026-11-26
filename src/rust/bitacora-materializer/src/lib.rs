//! The bitacora materializer framework.
//!
//! # Why this crate exists
//!
//! The log only stores records. Something has to turn them into state that can be queried: pages of a
//! database, index segments, a key-value store. A materializer is that something: a deterministic
//! function from a prefix of the log to queryable state (design, section 11). The function differs per
//! workload. Everything around it does not: tailing the log, checkpointing, letting the log be truncated,
//! storing artifacts, making sure only one instance commits. This crate is everything around it.
//!
//! # What it contains
//!
//! - The contract a workload implements: [`Materializer`], [`CheckpointManifest`], [`MaterializerError`].
//! - What a materializer writes through: [`ArtifactNamespace`].
//! - What runs it: [`MaterializerRunner`], with [`RunnerDependencies`], [`RunnerSettings`],
//!   [`RunnerProgress`], [`Progress`], [`RunnerError`] and [`RunnerStopped`].
//! - What cleans up after it: [`sweep_orphans`], with [`SweepReport`] and [`SweepError`].
//! - The adapter for engines with pages and redo records: [`PageMaterializer`], [`PageStore`],
//!   [`PageReader`], with [`LayeringPolicy`] and [`PageReadError`].
//! - The adapter for append-only data that compacts into files: [`ArtifactBuilder`],
//!   [`ArtifactCatalog`], [`CatalogView`], with [`CatalogEntry`], [`ArtifactDescriptor`],
//!   [`CompactionPolicy`] and [`decode_catalog`].
//! - The stored form of a manifest: [`encode_manifest`] and [`decode_manifest`].
//! - What a process that is not the runner reads the committed state with:
//!   [`read_committed_checkpoint`], with [`CommittedCheckpoint`] and [`CommittedCheckpointError`].
//!
//! # How it fits together
//!
//! ```text
//! log --follow--> MaterializerRunner --apply_records--> Materializer
//!                    |        |                             |
//!                    |        +--write_checkpoint---------->+--put_artifact--> ArtifactNamespace
//!                    |                                                             |
//!                    +--commit_checkpoint--> metadata store                   artifact store
//! ```
//!
//! The committed checkpoint in the metadata store is what lets the controller truncate the log: never
//! above it (invariant I5).
//!
//! # See also
//!
//! The `README.md` of this crate for the design across files, and `docs/decisions/` for the choices the
//! design document leaves open.
#![warn(clippy::missing_docs_in_private_items)]

mod artifact_catalog;
mod committed;
mod contract;
mod layer;
mod manifest;
mod namespace;
mod page_store;
mod runner;
mod sweeper;

#[cfg(test)]
mod test_harness;

pub use artifact_catalog::{
    ArtifactBuilder, ArtifactCatalog, ArtifactDescriptor, CatalogEntry, CatalogView, CompactionPolicy, StoredCatalog,
    decode_catalog,
};
pub use committed::{CommittedCheckpoint, CommittedCheckpointError, read_committed_checkpoint};
pub use contract::{CheckpointManifest, Materializer, MaterializerError};
pub use manifest::{decode_manifest, encode_manifest};
pub use namespace::{ArtifactNamespace, namespace_prefix, token_of_artifact};
pub use page_store::{LayeringPolicy, PageMaterializer, PageReadError, PageReader, PageStore};
pub use runner::{
    MaterializerRunner, Progress, RunnerDependencies, RunnerError, RunnerProgress, RunnerSettings, RunnerStopped,
};
pub use sweeper::{SweepError, SweepReport, sweep_orphans};
