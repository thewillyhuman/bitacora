//! The bitacora client library.
//!
//! # Why this crate exists
//!
//! bitacora has no server on the append path. The write protocol runs inside the application that writes the log: the
//! library is linked into the compute node, assigns sequence numbers, sends batches to the replicas, decides what is
//! committed and takes over a stream after a failure (design, section 13). Putting the protocol in a library is what
//! makes an append cost one network round trip, and it means that this crate is where the protocol's safety rules are
//! implemented. Readers and administration live here too, so that an application needs one dependency and one set of
//! injected interfaces.
//!
//! # What it contains
//!
//! - [`BitacoraClient`]: the entry point, from which the three handles below are obtained. [`ClientSettings`] tunes it.
//! - [`StreamWriter`]: `append`, `append_batch`, `durable_sequence`, `epoch`, `close`. Opening one runs the takeover
//!   protocol; [`TakeoverPolicy`] says how politely, and [`WriterSettings`] holds its timeouts and limits.
//! - [`StreamReader`]: `read_range`, `follow_from`, `low_water_mark`. Following yields a [`RecordStream`].
//!   [`ReaderSettings`] holds its timeouts and limits.
//! - [`StreamAdmin`]: `create_stream`, `describe_stream`, `list_streams`, `truncate_before`, `delete_stream`,
//!   `register_materializer`, `unregister_materializer`, with [`StreamOptions`] and [`StreamDescription`].
//! - [`AppendError`], [`OpenError`], [`ReadError`]: how each of them fails.
//!
//! # How it works
//!
//! Everything the library needs from the outside world is injected through [`BitacoraClient::new`]: the metadata store,
//! a connector that turns a replica's address into a handle, and a clock. The same code therefore runs against a real
//! cluster over gRPC and inside the deterministic simulator, where all three are in memory.
//!
//! A writer keeps its bookkeeping in one plain data structure behind a mutex and runs one task per replica plus one for
//! the lease and segment rolls. A reader is a cursor that asks one replica at a time and falls back to the object store
//! for segments that were tiered. Administration is a thin layer over the metadata store.
//!
//! # Invariants
//!
//! - A record is reported as committed only after a write quorum of replicas has flushed it, and a committed record is
//!   never lost or reordered by a segment roll or a takeover.
//! - A stream has one writer at a time. A writer that lost the stream cannot commit: replicas refuse it and its
//!   metadata writes fail on the epoch.
//! - A reader returns only committed records, in order, and verifies every batch checksum itself.
//! - The metadata store is never touched on an append, nor on a read that the reader's replica can serve.
//! - The crate reads no clock, opens no socket and touches no file of its own: all three come through the injected
//!   interfaces.
//!
//! # See also
//!
//! - `client.rs`: the entry point. `context.rs`: the injected interfaces, shared by the handles.
//! - `writer.rs` and `writer/`: the write path and the takeover protocol.
//! - `reader.rs`, `reader/` and `tiered.rs`: the read path.
//! - `admin.rs`, `settings.rs`, `error.rs`: administration, tunables, failures.
//! - `docs/protocol/replication.md`: the same protocol for readers who do not read Rust.
//! - `bitacora-model` and `bitacora-sim`: the model that checks the protocol and the simulator that checks this code.

#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#![warn(rustdoc::broken_intra_doc_links)]

mod admin;
mod client;
mod context;
mod error;
mod reader;
mod settings;
mod tiered;
mod writer;

pub use admin::{StreamAdmin, StreamDescription, StreamOptions};
pub use client::{BitacoraClient, ClientSettings};
pub use error::{AppendError, OpenError, ReadError};
pub use reader::{RecordStream, StreamReader};
pub use settings::{ReaderSettings, TakeoverPolicy, WriterSettings};
pub use writer::StreamWriter;
