//! Printing the cluster's own log lines during a simulated run.
//!
//! # Why this file exists
//!
//! The history a run records is what clients did and saw. When a seed fails, that is often not enough to
//! see why: the cause is something the controller, a replica or a writer decided, and those say what they
//! do through `tracing`. This file makes those lines visible for one replayed seed, in the order they
//! happened and stamped with the run's virtual time, so that they can be read next to the history.
//!
//! # What it contains
//!
//! - [`print_log_lines_to_stderr`]: turns the printing on for the rest of the process.
//!
//! # How it works
//!
//! A minimal `tracing` subscriber formats every event at `DEBUG` or above as one line on standard error:
//! virtual time, level, the module that logged, then the fields. Spans are accepted and ignored; the code
//! under simulation logs events, not spans.
//!
//! # Invariants
//!
//! - Printing changes nothing about a run: the subscriber only reads events, so a seed replays the same
//!   history with it as without it.
//!
//! # See also
//!
//! - `tests/fault_matrix.rs`: the replay test that turns this on when `BITA_SIM_TRACE` is set.

use std::fmt::Write as _;
use std::sync::OnceLock;

use tokio::time::Instant;

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};

/// The identifier handed out for every span. Spans are not tracked, so one is enough.
const THE_ONLY_SPAN_ID: u64 = 1;

/// Prints every log line of the code under simulation to standard error, from now until the process ends.
///
/// Call it once, before the run. A second call does nothing: the first subscriber stays.
pub fn print_log_lines_to_stderr() {
    // Fails only if a subscriber is already installed, which is what the caller wants anyway.
    let _already_installed = tracing::subscriber::set_global_default(StderrPrinter);
}

/// The virtual time of the first log line. Lines are stamped with the time since then, which is the time
/// since the start of the run, because every run logs as it starts.
static FIRST_LINE_AT: OnceLock<Instant> = OnceLock::new();

/// A subscriber that prints events and ignores spans.
struct StderrPrinter;

impl Subscriber for StderrPrinter {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        *metadata.level() <= Level::DEBUG
    }

    fn new_span(&self, _attributes: &Attributes<'_>) -> Id {
        Id::from_u64(THE_ONLY_SPAN_ID)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut fields = FieldsAsText::default();
        event.record(&mut fields);
        let metadata = event.metadata();
        let since_the_first_line = FIRST_LINE_AT.get_or_init(Instant::now).elapsed();
        eprintln!(
            "{since_the_first_line:>10.3?} log {:>5} {}:{}",
            metadata.level(),
            metadata.target(),
            fields.text
        );
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

/// Collects the fields of one event as `name=value` pairs, the message first and without its name.
#[derive(Default)]
struct FieldsAsText {
    /// The line built so far.
    text: String,
}

impl Visit for FieldsAsText {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.text, " {value:?}");
        } else {
            let _ = write!(self.text, " {}={value:?}", field.name());
        }
    }
}
