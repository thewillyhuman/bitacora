//! The bitacora controller.
//!
//! # Why this crate exists
//!
//! A writer appends to a fixed set of replicas and never asks anybody's permission to do so, which is what keeps the
//! append path short. Somebody still has to do the slow, cluster-wide work that makes this safe over months: place the
//! replicas of the next segment before a writer needs them, notice that a copy of a sealed segment is short, damaged
//! or gone and have it rebuilt, move old segments to the object store, delete what no reader may ask for any more, and
//! keep a backup of the metadata store. That somebody is the controller (design, section 8). It is a separate,
//! leader-elected process that reads and writes the metadata store and talks to the replicas. It is never on the
//! append path: while it is down, streams keep appending and reading, and rolls continue until the standby segments
//! run out.
//!
//! # What it contains
//!
//! - [`Controller`]: the controller itself. `run` loops forever, `run_round` does one round of that loop, `run_pass`
//!   does one pass over the cluster.
//! - [`ControllerSettings`]: how the controller paces itself and judges replicas.
//! - [`PassReport`] and [`Round`]: what one pass and one round did, for the process that hosts the controller.
//!
//! # How it works
//!
//! Everything the controller does is a pass: read the metadata store, ask the replicas what they hold, and make
//! conditional writes that bring the two closer to what they should be. The controller keeps no state between passes
//! that a pass depends on. `controller.rs` holds the pass and the order of its steps; each step is a file of its own
//! under `controller/`.
//!
//! # Invariants
//!
//! - Every write to the metadata store is conditional on what the pass saw. A pass that is interrupted, repeated, or
//!   run by two controllers at once therefore does no harm.
//! - The controller never touches an open segment. Only the segment's writer does.
//! - The crate knows nothing of networks, disks or clocks: it reaches replicas, the object store and the time through
//!   the interfaces it is given, which is what lets it run under deterministic simulation.
//!
//! # See also
//!
//! - `controller.rs`: the controller and the pass.
//! - `docs/protocol/replication.md`, "The controller": the same passes for readers who do not read Rust.
//! - `bita-controller`: the daemon that hosts a controller.

#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#![warn(rustdoc::broken_intra_doc_links)]

mod controller;

pub use controller::{Controller, ControllerSettings, PassReport, Round};
