//! The bitacora replica: one storage node of the log.
//!
//! # Why this crate exists
//!
//! A stream's records live on replicas, and a record is committed when enough of them hold it on disk.
//! A replica is deliberately simple (design, section 7): it appends batches to segment files, flushes
//! them, and serves them back. It makes no decisions about ordering, membership or leadership. The writer
//! assigns sequence numbers, the metadata store holds epochs and segment lists, and the replica enforces
//! the handful of local rules that make those decisions safe: accept only batches that continue its copy,
//! refuse writers that were fenced, never serve a record before the writer declares it durable, and never
//! forget a fence or a seal.
//!
//! This crate is the replica's core, free of any transport and of any process. The `bita-replica` daemon
//! wires it to a local disk and a gRPC server; the test kit and the simulator wire it to an in-memory
//! disk and in-process links, and run the same code.
//!
//! # What it contains
//!
//! - [`Replica`]: the node. It implements `bitacora_types::replica_api::ReplicaApi`, which is how every
//!   other program calls it.
//! - [`ReplicaSettings`]: how a replica uses its disk, its peers and the object store.
//! - [`WriteFailure`]: what the replica tells its host when the disk refuses a write, after which the
//!   host must stop it.
//!
//! # How it works
//!
//! A replica holds one copy of every segment placed on it. Each copy is owned by a task of its own, which
//! applies the requests for that segment one at a time and to completion; the replica routes a request to
//! the task of the segment it is about. A copy is a records file plus a small state file, both read back
//! and verified when the replica is opened.
//!
//! Everything the replica needs from outside is handed to it: the disk, the way to reach its peers, and
//! the object store. It reads no clock but the runtime's and opens no file or socket itself, which is
//! what lets the simulator crash it, cut it off and corrupt its disk at will.
//!
//! # Invariants
//!
//! - An append is acknowledged only after its flush returned.
//! - Requests for one segment are applied in order, one at a time, and a request that started finishes
//!   even if its caller is gone.
//! - A copy that failed a checksum answers nothing but its status, and waits to be replaced.
//! - After the disk refused a write the replica must be stopped; it says so through [`WriteFailure`].
//! - The crate depends on `bitacora-types`, `bitacora-segment` and `bitacora-artifact` only. It knows
//!   nothing of the metadata store, the wire protocol or the controller.
//!
//! # See also
//!
//! - `README.md` in this crate: the design, and which file holds what.
//! - `docs/protocol/replication.md`, "A replica": the rules in protocol terms.
//! - `docs/protocol/disk-format.md`: the data directory.

#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#![warn(rustdoc::broken_intra_doc_links)]

mod actor;
mod copy;
mod paths;
mod replica;
mod settings;
#[cfg(test)]
mod test_support;
mod upload;
mod write_failure;

pub use replica::Replica;
pub use settings::ReplicaSettings;
pub use write_failure::WriteFailure;
