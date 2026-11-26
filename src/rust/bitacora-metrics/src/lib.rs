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
//! # How it works
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
//! A store is the writer of its stream. An append is one record. A materializer follows the stream and keeps the
//! samples of the newest records in memory, the head; when the head holds as much as one block may, it writes the
//! samples as a block to the object store and drops them from memory. A query merges what the blocks and the head
//! hold of the series it selects, and says up to which record its answer is exact.
//!
//! # Invariants
//!
//! - An answer with `as_of` = N holds exactly what the records up to N make of the selected series.
//! - An append reported as done is in the log once, at the returned sequence number.
//! - The same records give the same blocks, byte for byte, whenever checkpoints happen and whichever instance
//!   cuts them (invariant I6).
//! - Nothing is returned from a block before the checksum that covers it was verified.
//! - The crate uses the materializer framework through its public interface only.
//!
//! # See also
//!
//! - `README.md` in this crate: the design, and which file holds what.
//! - `docs/protocol/metrics.md`: the formats of records, blocks and chunks.
//! - `docs/decisions/0015-metrics-materializer.md`: what building this crate showed about the framework.

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
