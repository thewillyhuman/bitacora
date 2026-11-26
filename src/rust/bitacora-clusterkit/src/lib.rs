//! A bitacora cluster of real processes on one machine, for tests.
//!
//! # Why this crate exists
//!
//! `bitacora-testkit` builds a cluster inside one process, on in-memory disks and a virtual clock; that
//! is where the protocol is tested, exhaustively and deterministically. It cannot test what an operator
//! deploys: the daemons as programs, with configuration files, sockets, signals and exit codes, against
//! a real etcd. This crate is the fixture for that last layer of the design's testing plan (section 15,
//! "Layer 4: real clusters"). It is deliberately small: the tests that use it show that the pieces work
//! together as processes, not that the protocol is correct.
//!
//! # What it contains
//!
//! - [`Binaries`]: where the programs are, or the decision to skip.
//! - [`RealCluster`]: etcd, replica daemons and a controller daemon, started, ready, and stoppable.
//! - [`ChildProcess`], [`free_local_ports`]: the building blocks, for tests that need a process of
//!   their own.
//!
//! # How it works
//!
//! ```no_run
//! # async fn example() {
//! use bitacora_clusterkit::{Binaries, RealCluster};
//!
//! // `None` under Cargo without the programs: the test passes without running.
//! let Some(binaries) = Binaries::from_environment() else {
//!     return;
//! };
//! let mut cluster = RealCluster::start(&binaries, 3).await;
//!
//! // Use the cluster as an application does.
//! let client = bitacora::connect(cluster.cluster_config(), bitacora::ClientSettings::default())
//!     .await
//!     .expect("the configuration is valid");
//!
//! cluster.kill_replica(1);
//! cluster.restart_replica(1).await;
//!
//! // Every daemon is asked to stop with SIGTERM; the result says how each exited.
//! let exited_daemons = cluster.stop().await;
//! # drop((client, exited_daemons));
//! # }
//! ```
//!
//! # Invariants
//!
//! - No process outlives the value that owns it, also when a test panics.
//! - Everything a cluster writes is below one temporary directory, removed after its processes.
//! - Nothing here is used by production code.
//!
//! # See also
//!
//! - `bitacora-testkit`: the in-process cluster, for everything that does not need real processes.
//! - `tests/real_cluster_test.rs`: the test of a cluster of real processes.

#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#![warn(rustdoc::broken_intra_doc_links)]

mod binaries;
mod cluster;
mod process;

pub use binaries::{Binaries, CONTROLLER_BINARY_VARIABLE, ETCD_BINARY_VARIABLE, REPLICA_BINARY_VARIABLE};
pub use cluster::{ExitedDaemon, RealCluster};
pub use process::{ChildProcess, free_local_ports};
