//! Judging what the clients of a stream saw, and keeping the evidence when the verdict is bad.
//!
//! # Why this file exists
//!
//! The simulator decides whether a run kept the promises of the log by handing the history of the run to a checker
//! that knows nothing about the cluster (design, section 15). A soak must reach its verdict the same way, with the
//! same checker: if the two judged differently, a history that passes in simulation could fail on a real cluster for
//! no other reason than who looked at it. This file hands a stream's history to that checker, and, when the checker
//! finds something, writes the history down so that a person can look.
//!
//! # What it contains
//!
//! - [`violations_of`]: everything the simulator's checker finds in one stream's history.
//! - [`write_dump`]: the history as a text file, in the format the simulator prints with `BITA_SIM_DUMP`.
//!
//! # How it works
//!
//! A history is judged twice, as in the simulator: `bitacora_sim::check` looks at what writers were told and readers
//! were handed during the round (I2, I3, I4, and I1 for records that were read), and `bitacora_sim::check_audit`
//! compares every append reported as committed with the read of the whole stream after healing (I1 for the rest).
//!
//! A dump has one line per event: the time since the history began, then the event as the checker saw it.
//!
//! # Invariants
//!
//! - The verdict depends on the events alone, and on nothing the cluster says about itself.
//! - A dump holds exactly the events that were judged, in their order.
//! - This file is the only one of the tool that writes a file.
//!
//! # See also
//!
//! - `bitacora-sim/src/checker.rs`: the rules.
//! - `audit.rs`: makes the read the second check judges.
//! - `round.rs`: calls both functions.

// This file writes the tool's own evidence to a directory the operator names. It is not the cluster's disk, which
// only the replicas touch through the injected `Disk`.
#![allow(clippy::disallowed_methods)]

use std::fmt::Write as _;
use std::io;
use std::path::{Path, PathBuf};

use bitacora_sim::{TimedEvent, Violation, check, check_audit};

/// Lists everything the simulator's checker finds in the history of one stream.
///
/// # Arguments
///
/// * `events` - The history of one stream, in the order it was recorded.
/// * `audit_session` - The reading session of the audit's read of the whole stream.
///
/// # Returns
///
/// The violations of the round's reads and writes, then those the audit reveals. Empty if the stream kept every
/// promise the checker knows.
pub(crate) fn violations_of(events: &[TimedEvent], audit_session: u64) -> Vec<Violation> {
    let mut violations = check(events);
    violations.extend(check_audit(events, audit_session));
    violations
}

/// Renders `events` as text, one event per line, as the simulator dumps a history.
pub(crate) fn dump_text(events: &[TimedEvent]) -> String {
    let mut text = String::new();
    for timed in events {
        let _ = writeln!(text, "{:>10.3?} {:?}", timed.at, timed.event);
    }
    text
}

/// Writes `events` to the file `file_name` in `directory`.
///
/// # Arguments
///
/// * `directory` - Where dumps go. It must exist.
/// * `file_name` - The name of the file, without a directory.
/// * `events` - The history to keep.
///
/// # Returns
///
/// The path of the file that was written.
///
/// # Errors
///
/// Fails if the file cannot be written.
pub(crate) fn write_dump(directory: &Path, file_name: &str, events: &[TimedEvent]) -> io::Result<PathBuf> {
    let path = directory.join(file_name);
    std::fs::write(&path, dump_text(events))?;
    Ok(path)
}

#[cfg(test)]
#[path = "judge_test.rs"]
mod tests;
