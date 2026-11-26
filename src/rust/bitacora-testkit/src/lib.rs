//! An in-process bitacora cluster with switchable faults.
//!
//! # Why this crate exists
//!
//! Every crate above the replica has to be tested against a cluster: a writer needs replicas to append to, the
//! controller needs copies to repair, a reader needs segments to read. Real processes on real disks and sockets would
//! make those tests slow, and would make the interesting ones impossible: a test cannot ask a kernel to lose exactly
//! the bytes that were not flushed, or a network to deliver a request and lose its answer.
//!
//! This crate is the cluster those tests use instead. It runs the real replica code (`bitacora-replica`) on in-memory
//! disks, registers the replicas in an in-memory metadata store, shares an in-memory object store between them, and
//! joins everything with links that can be cut, slowed or made to lose answers. Nothing in it is a mock of the code
//! under test; only the disk, the network, the stores and the clock are replaced, and those are exactly the interfaces
//! the production code is handed too.
//!
//! The crate injects no fault on its own. A test flips the switches by hand, at the point it wants; the simulator
//! (`bitacora-sim`) flips the same switches from a seed. Because wall-clock time follows the runtime's clock, a test
//! under a paused runtime sees leases, heartbeats and timers move together, runs in no real time, and gives the same
//! result every time.
//!
//! # What it contains
//!
//! - [`Cluster`]: the storage nodes, the metadata store they register in, the object store they upload to and the clock
//!   they share.
//! - [`FailureDomainLayout`]: where the nodes of a cluster are, in terms of zones and hosts.
//! - [`Node`]: one storage node: a replica process, its disk, and the link that reaches it. It can crash, lose its disk
//!   and restart.
//! - [`Link`] and [`LinkFault`]: the network path to a node and what is wrong with it.
//! - [`VirtualClock`]: wall-clock time that advances with the runtime's clock.
//!
//! # How it works
//!
//! [`Cluster::start`] builds the stores, starts the nodes one after the other and registers them. Callers reach a node
//! in one of two ways: through its [`Link`], which is the network and applies the link's fault, or through
//! [`Node::replica`], which bypasses the network and is for looking at what the replica holds. Clients and the
//! controller are given [`Cluster::connector`], which resolves an address to the link registered under it, so every
//! request they send crosses a link.
//!
//! # Invariants
//!
//! - The crate never injects a fault by itself and holds no random state: what happens in a cluster is decided by the
//!   caller alone.
//! - A fault is reported the way a real network or disk would report it. Nothing tells the code under test that it runs
//!   in a test.
//! - A node that lost its disk returns under a new identity (decision record
//!   `docs/decisions/0010-replica-identity.md`).
//! - The crate depends on `bitacora-types`, `bitacora-segment`, `bitacora-artifact`, `bitacora-replica` and
//!   `bitacora-meta` only. It knows nothing of writers, readers or the controller, so that their crates can use it in
//!   their tests.
//!
//! # See also
//!
//! - `README.md` in this crate: the design, and which file holds what.
//! - `bitacora-sim`: the simulator that drives a cluster from a seed.
//! - `bitacora-segment`: `MemoryDisk` and `CrashMode`, the disk a node lives on and what a crash leaves on it.

#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#![warn(rustdoc::broken_intra_doc_links)]

mod clock;
mod cluster;
mod connector;
mod layout;
mod link;
mod node;
#[cfg(test)]
mod test_support;

pub use clock::VirtualClock;
pub use cluster::Cluster;
pub use layout::FailureDomainLayout;
pub use link::{Link, LinkFault};
pub use node::Node;
