//! The materializer of the metrics store, as the framework's runner runs it.
//!
//! # Why this file exists
//!
//! The metrics store writes no materializer of its own. The framework's `ArtifactCatalog` turns a builder
//! into one: it keeps the catalog of blocks, has the builder seal at checkpoints and merge small blocks.
//! This file names that combination, and the name it is registered under on a stream, so that the store,
//! the reader and the tests all mean the same thing.
//!
//! # What it contains
//!
//! - [`MetricsMaterializer`]: the framework's catalog over the block builder.
//! - [`METRICS_MATERIALIZER`]: the name the materializer is registered under on a stream.
//!
//! # See also
//!
//! - `builder.rs`: the builder, which is all of the materializer that this crate writes.
//! - `store.rs`: where the materializer is started and its head and catalog are handed to queries.

use bitacora_materializer::ArtifactCatalog;

use crate::builder::{BlockBuilder, StandardBlocks};

/// The name the materializer of a metrics store is registered under on its stream.
pub const METRICS_MATERIALIZER: &str = "bitacora-metrics";

/// The materializer of a metrics store: the framework's catalog of artifacts over [`BlockBuilder`].
///
/// The framework's runner owns it. It is not used directly; see [`MetricsStore`](crate::MetricsStore).
pub type MetricsMaterializer<P = StandardBlocks> = ArtifactCatalog<BlockBuilder<P>>;
