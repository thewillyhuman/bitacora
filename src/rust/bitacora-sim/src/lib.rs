//! Deterministic simulation of a bitacora cluster.
//!
//! # Why this crate exists
//!
//! The bugs that lose data in a replicated log are not in any one function. They are in what happens when a writer is
//! fenced while a replica restarts behind a slow link, with a torn write on its disk, in the second the metadata store
//! was unreachable. Unit tests do not reach those states, tests against real processes reach them rarely and cannot
//! reach them twice, and nobody can write the scenarios down by hand, because the ones that find bugs are the ones
//! nobody thought of.
//!
//! This crate runs the whole system (replicas, the controller, compute nodes serving a key-value store, readers) in one
//! thread, on a simulated clock, network and disk, and lets a random number generator decide what goes wrong and when
//! (design, section 15). Everything a run does follows from its seed, so a seed that finds a bug finds it again on
//! every machine, as often as it takes to understand it. Afterwards the run is judged from the outside, by what its
//! clients saw, against the invariants the design promises.
//!
//! It is how the project knows that its protocol holds, and how a change to the replica, the client or the controller
//! is shown to keep it holding.
//!
//! # What it contains
//!
//! - [`run`]: executes one seeded run, described by a [`SimulationConfig`] and its [`FaultMix`], and returns a
//!   [`SimulationReport`].
//! - [`History`]: the record of what clients did and saw, made of [`TimedEvent`]s: [`Event`], [`AppendOutcome`],
//!   [`KvCall`], [`KvOutcome`].
//! - [`check`] and [`check_audit`]: judge a history against the invariants of the log, as [`Violation`]s.
//! - [`check_linearizability`]: judges the calls made to the key-value store.
//! - [`InjectedBug`]: mistakes made on purpose, to show that the checks catch them.
//! - [`print_log_lines_to_stderr`]: shows what the cluster logged while a seed is replayed.
//!
//! # How it works
//!
//! The cluster is the one of `bitacora-testkit`: real replicas on in-memory disks behind links that can fail. Around it
//! this crate runs what a deployment has (a controller, registration, a supervisor, scrubbing), a workload (compute
//! nodes that take turns being the writer and serve a key-value store, and readers that follow the stream), and a
//! nemesis that injects faults from the seed. A run is a minute of that, then healing, then an audit that reads
//! everything back. The history is then checked.
//!
//! # Invariants
//!
//! - The same configuration produces the same report, on any machine. Nothing in a run reads the system clock, an
//!   unseeded random source, or anything else outside the simulation.
//! - The checks see only what clients saw. They never ask the system under test what happened.
//! - A run fails if an invariant is broken at any time, and if the cluster has not recovered once every fault is gone.
//!   It never fails for being slow or unavailable while faults are being injected.
//! - Every seed that finds a bug is kept as a regression test in `tests/fault_matrix_test.rs`.
//! - The order in which a run draws random numbers, spawns tasks and awaits, and the names inside the history's events,
//!   are part of what a seed means. A change to either changes every run; the fingerprint of a run
//!   ([`SimulationReport::fingerprint`]) shows whether one did.
//!
//! # See also
//!
//! - `README.md` in this crate: the design, which file holds what, and how to run more seeds, replay one, dump its
//!   history and print the cluster's log lines.
//! - `docs/decisions/0008-deterministic-simulation.md`: why the project tests this way.
//! - `tests/fault_matrix_test.rs`: the seeds that run on every change.

#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#![warn(rustdoc::broken_intra_doc_links)]

mod audit;
mod background;
mod checker;
mod config;
mod diagnosis;
mod history;
mod kv_workload;
mod linearizability;
mod nemesis;
mod reader_workload;
mod report;
mod simulation;
#[cfg(test)]
mod test_support;
mod trace;
mod world;

pub use checker::{Violation, check, check_audit};
pub use config::{FaultMix, SimulationConfig};
pub use history::{AppendOutcome, Event, History, KvCall, KvOutcome, TimedEvent};
pub use kv_workload::InjectedBug;
pub use linearizability::check_linearizability;
pub use report::SimulationReport;
pub use simulation::run;
pub use trace::print_log_lines_to_stderr;
