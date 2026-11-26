//! bitacora-metrics: a small time-series store that cuts immutable blocks from one stream.
//!
//! # Why this crate exists
//!
//! bitacora-kv, the first materializer, was written together with the materializer framework. That
//! proves the framework can do what its author had in mind, and nothing more. This crate is the second
//! materializer (design, section 18, milestone M6): a different kind of workload, written afterwards,
//! against the framework's public interface only. Whatever it could not do through that interface is a
//! finding, and the findings are the point; they are recorded in
//! `docs/decisions/0015-metrics-materializer.md`.
//!
//! The workload is the one the design sketches for Prometheus (section 19): samples arrive in batches,
//! one batch per log record; the newest are served from memory; older ones are cut into immutable blocks
//! in the object store, which are merged into larger blocks over time. It uses the framework's adapter
//! for immutable artifacts, the one bitacora-kv does not exercise.
//!
//! It is not a replacement for a real time-series database. Its blocks are its own format, not
//! Prometheus's, and it has no query language: a query selects series by their labels and returns their
//! samples in a span of time.
//!
//! # What it contains
//!
//! - [`MetricsStore`]: the store. `append` and `query`, with [`QueryPosition`] for read-your-writes.
//! - [`BlockReader`]: queries over the committed blocks, from a process that is not the store.
//! - [`Labels`], [`Label`], [`Sample`], [`SeriesSamples`], [`TimeRange`]: what is stored and returned.
//! - [`Selector`], [`Matcher`]: which series a query wants.
//! - [`encode_record`], [`decode_record`]: the log records.
//! - [`BlockBuilder`], [`BlockPolicy`], [`StandardBlocks`], [`MetricsMaterializer`]: the materializer.
//! - [`BlockSummary`], [`QueryResult`], and the error types.
//!
//! # How it fits together
//!
//! ```text
//! append --> record --> StreamWriter --> log
//!                                         |
//!                         MaterializerRunner<MetricsMaterializer>
//!                            |                         |
//!                          head (memory)        blocks (object store)
//!                            |                         |
//! query -------------------- + ------------------------+
//! ```
//!
//! # See also
//!
//! The `README.md` of this crate for the formats, and `docs/decisions/0015-metrics-materializer.md` for
//! what building it showed about the framework.

#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#![warn(rustdoc::broken_intra_doc_links)]

mod block;
mod builder;
mod chunk;
mod head;
mod materializer;
mod query;
mod reader;
mod record;
mod sample;
mod selector;
mod store;
#[cfg(test)]
mod test_support;

pub use block::{BlockSummary, DamagedBlock};
pub use builder::{BlockBuilder, BlockPolicy, StandardBlocks};
pub use chunk::DamagedChunk;
pub use materializer::{METRICS_MATERIALIZER, MetricsMaterializer};
pub use query::QueryResult;
pub use reader::{BlockReader, BlockReaderError};
pub use record::{InvalidBatch, MetricsRecordError, decode_record, encode_record};
pub use sample::{
    InvalidLabels, InvalidTimeRange, Label, Labels, MAX_LABEL_TEXT_BYTES, MAX_LABELS_PER_SERIES, Sample, SeriesSamples,
    TimeRange,
};
pub use selector::{Matcher, Selector};
pub use store::{MetricsDependencies, MetricsError, MetricsSettings, MetricsStore, QueryPosition};
