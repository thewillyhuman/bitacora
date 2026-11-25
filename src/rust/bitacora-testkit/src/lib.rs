//! An in-process bitacora cluster with switchable faults.
//!
//! Every crate above the replica tests against the same thing: real replicas
//! on in-memory disks, an in-memory metadata store, and network links that
//! can be cut, slowed or made lossy. This crate is that thing. It injects no
//! faults on its own; tests flip the switches by hand and the simulator
//! flips them from a seed.
//!
//! Wall-clock time follows Tokio's clock, so under a paused test runtime
//! leases, heartbeats and timers all move together and a run takes no real
//! time.

mod clock;
mod cluster;
mod link;

pub use clock::VirtualClock;
pub use cluster::{Cluster, Node};
pub use link::{Link, LinkFault};
