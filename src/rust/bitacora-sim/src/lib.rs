//! Deterministic simulation of a bitacora cluster.
//!
//! The whole system (replicas, controller, writers, readers) runs in one
//! thread on a simulated clock, network and disk, driven by a random seed,
//! and a failing seed reproduces the failure exactly (design, section 15).
//!
//! - [`run`] executes one seeded run and returns a [`SimulationReport`].
//! - [`History`] records what clients did and saw.
//! - [`check`] judges a history against the invariants of the design.
//!
//! Every seed that finds a bug is kept as a regression test in
//! `tests/fault_matrix.rs`.

mod checker;
mod history;
mod simulation;

pub use checker::{Violation, check, check_audit};
pub use history::{AppendOutcome, Event, History, TimedEvent};
pub use simulation::{FaultMix, SimulationConfig, SimulationReport, run};
