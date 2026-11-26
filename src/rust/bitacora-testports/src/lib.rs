//! Loopback ports for tests that start real servers.
//!
//! # Why this crate exists
//!
//! Some tests start programs that open their own sockets: etcd, an S3 server, the bitacora daemons. Such a
//! program must be told which port to listen on, so the test has to name a port that is free. The usual way
//! is to bind port zero, read the port the operating system chose, close the socket and pass the number on.
//! Between the close and the program's own bind, the operating system may give the same port to anyone else
//! who asks for "any port", and tests that run in parallel all ask at once. One test of this repository
//! failed that way in 6 of 30 parallel runs.
//!
//! This crate gives tests ports that the operating system never hands out by itself, in blocks that one test
//! owns for as long as it needs them, so that two tests are never given the same port.
//!
//! # What it contains
//!
//! - [`PortBlock`]: fifty neighbouring ports that belong to whoever claimed them, until the block is dropped.
//! - [`take_shared_port`]: the next port of one block that a whole test binary shares.
//!
//! # How it works
//!
//! ```
//! use bitacora_testports::PortBlock;
//!
//! // A test that starts a cluster claims a block and keeps it as long as the cluster runs.
//! let mut ports = PortBlock::claim();
//! let client_port = ports.take();
//! let peer_port = ports.take();
//!
//! assert_ne!(client_port, peer_port);
//! ```
//!
//! A test binary whose tests each need a port or two shares one block instead, with [`take_shared_port`]:
//! there are only thirty-seven blocks for all the tests that run on a machine at one moment.
//!
//! # Invariants
//!
//! - While a [`PortBlock`] lives, no other [`PortBlock`] in any process hands out one of its ports.
//! - The crate depends on nothing but the standard library, so the test of any crate can use it.
//! - Nothing here is used by production code.
//!
//! # See also
//!
//! - `port_block.rs`: how a block is claimed, and the ranges the other test suites of the repository use.
//! - `bitacora-clusterkit`: the cluster of real daemons, which takes its ports from a block.

#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#![warn(rustdoc::broken_intra_doc_links)]

mod port_block;

pub use port_block::{PortBlock, take_shared_port};
