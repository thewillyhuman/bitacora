//! The materializer of the metrics store, as the framework's runner runs it.
//!
//! # Why this file exists
//!
//! The metrics store writes no materializer of its own. The framework's `ArtifactCatalog` turns a builder
//! into one: it keeps the catalog of blocks, has the builder seal the blocks that are full, save the rest
//! of its head at checkpoints, and merge small blocks.
//! This file names that combination, and the name it is registered under on a stream, so that the store,
//! the reader and the tests all mean the same thing.
//!
//! # What it contains
//!
//! - [`MetricsMaterializer`]: the framework's catalog over the block builder.
//! - [`METRICS_MATERIALIZER`]: the name the materializer is registered under on a stream.
//!
//! # How it works
//!
//! [`MetricsMaterializer`] is a type alias, not a type: `ArtifactCatalog` over [`BlockBuilder`]. The runner
//! restores it, applies records to it and has it write checkpoints, like any materializer. Everything that is
//! particular to metrics happens in the builder the catalog calls.
//!
//! The name is a constant because three programs must agree on it: the store that registers and runs the
//! materializer, a reader that looks its committed checkpoint up, and an operator who removes it.
//!
//! # Invariants
//!
//! - The name never changes for an existing stream: the checkpoint record and the object keys of every block
//!   carry it.
//!
//! # See also
//!
//! - `builder.rs`: the builder, which is all of the materializer that this crate writes.
//! - `store.rs`: where the materializer is started and its head and catalog are handed to queries.

use bitacora_materializer::ArtifactCatalog;

use crate::builder::{BlockBuilder, StandardBlocks};

/// The name the materializer of a metrics store is registered under on its stream.
///
/// # Examples
///
/// ```
/// use bitacora_materializer::namespace_prefix;
/// use bitacora_metrics::METRICS_MATERIALIZER;
/// use bitacora_types::StreamId;
///
/// // Where the blocks of the metrics store on stream 7 live in the object store.
/// assert_eq!(namespace_prefix(StreamId::new(7), METRICS_MATERIALIZER), "materializers/7/bitacora-metrics/");
/// ```
pub const METRICS_MATERIALIZER: &str = "bitacora-metrics";

/// The materializer of a metrics store: the framework's catalog of artifacts over [`BlockBuilder`].
///
/// The framework's runner owns it. It is not used directly; see [`MetricsStore`](crate::MetricsStore).
pub type MetricsMaterializer<Policy = StandardBlocks> = ArtifactCatalog<BlockBuilder<Policy>>;
