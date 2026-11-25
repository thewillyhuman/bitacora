//! The bitacora replica: one storage node of the log.
//!
//! A replica is deliberately simple (design, section 7). It appends batches
//! to segment files, flushes them, and serves them back. It makes no
//! decisions about ordering, membership or leadership: the writer assigns
//! sequence numbers, the metadata store holds epochs and segment lists, and
//! the replica enforces the handful of local rules that make those decisions
//! safe: accept only contiguous batches, refuse fenced epochs, never serve
//! past the durable sequence.
//!
//! This crate is the replica's core, free of any transport. The `bita-replica`
//! binary wires it to a local disk and a gRPC server.

mod actor;
mod copy;
mod replica;
mod upload;

pub use actor::WriteFailureHandler;
pub use replica::{Replica, ReplicaSettings};
