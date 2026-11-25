//! The bitacora client library.
//!
//! This is where the write protocol lives (design, section 13): the library
//! is linked into the compute node, assigns sequence numbers, replicates
//! batches, tracks the durable sequence and runs failover. Its API is small:
//!
//! - [`StreamWriter`]: `append`, `append_batch`, `durable_sequence`, `close`.
//! - [`StreamReader`]: `read_range`, `follow_from`, `low_water_mark`.
//!
//! Everything it needs from the outside world is injected through
//! [`BitacoraClient::new`], so the same code runs against a real cluster and
//! inside the deterministic simulator.

mod client;
mod context;
mod error;
mod reader;
mod settings;
mod writer;

#[cfg(test)]
mod tests;

pub use client::{BitacoraClient, ClientSettings};
pub use error::{AppendError, OpenError, ReadError};
pub use reader::{RecordStream, StreamReader};
pub use settings::{ReaderSettings, TakeoverPolicy, WriterSettings};
pub use writer::StreamWriter;
