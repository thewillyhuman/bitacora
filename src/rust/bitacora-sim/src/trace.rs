//! Printing the cluster's own log lines during a simulated run.
//!
//! # Why this file exists
//!
//! The history a run records is what clients did and saw. When a seed fails, that is often not enough to see why: the
//! cause is something the controller, a replica or a writer decided, and those say what they do through `tracing`. This
//! file makes those lines visible for one replayed seed, in the order they happened and stamped with the run's virtual
//! time, so that they can be read next to the history.
//!
//! # What it contains
//!
//! - [`print_log_lines_to_stderr`]: turns the printing on for the rest of the process.
//! - `StderrPrinter`: the `tracing` subscriber that does the printing.
//! - `FieldsAsText`: the fields of one event, rendered as one line.
//!
//! # How it works
//!
//! A minimal `tracing` subscriber formats every event at `DEBUG` or above as one line on standard error: virtual time,
//! level, the module that logged, then the fields. Spans are accepted and ignored; the code under simulation logs
//! events, not spans.
//!
//! # Invariants
//!
//! - Printing changes nothing about a run: the subscriber only reads events, so a seed replays the same history with it
//!   as without it.
//! - The subscriber is installed for the whole process and cannot be removed; a test binary that turns it on prints for
//!   every test that runs after.
//!
//! # See also
//!
//! - `tests/fault_matrix_test.rs`: the replay test that turns this on when `BITA_SIM_TRACE` is set.
//! - `README.md` in this crate: how to replay a seed with its log lines.
//! - This file has no unit tests: installing a process-wide subscriber in the crate's test binary would print for every
//!   other test. The replay test exercises it.

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
/// Installs a process-wide `tracing` subscriber. Call it once, before the run. A second call does nothing: the first
/// subscriber stays, and so does a subscriber somebody else installed earlier.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use bitacora_sim::{FaultMix, SimulationConfig, print_log_lines_to_stderr, run};
///
/// let short_and_calm = SimulationConfig {
///     faults: FaultMix::NONE,
///     chaos_duration: Duration::from_secs(1),
///     ..SimulationConfig::with_seed(1)
/// };
///
/// print_log_lines_to_stderr();
/// let report = run(&short_and_calm);
///
/// assert!(report.is_clean(), "{}", report.failure_summary());
/// ```
pub fn print_log_lines_to_stderr() {
    // Fails only if a subscriber is already installed, which is what the caller wants anyway.
    let _already_installed = tracing::subscriber::set_global_default(StderrPrinter);
}

/// The virtual time of the first log line. Lines are stamped with the time since then, which is the time since the
/// start of the run, because every run logs as it starts.
static FIRST_LINE_AT: OnceLock<Instant> = OnceLock::new();

/// A subscriber that prints events and ignores spans.
struct StderrPrinter;

impl Subscriber for StderrPrinter {
    /// Accepts events at `DEBUG` and above; `TRACE` would drown the rest.
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        *metadata.level() <= Level::DEBUG
    }

    /// Hands out the same identifier for every span: spans are not tracked.
    fn new_span(&self, _attributes: &Attributes<'_>) -> Id {
        Id::from_u64(THE_ONLY_SPAN_ID)
    }

    /// Ignores values recorded on a span.
    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    /// Ignores relations between spans.
    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    /// Prints the event as one line on standard error: virtual time, level, module, fields.
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

    /// Ignores the entering of a span.
    fn enter(&self, _span: &Id) {}

    /// Ignores the leaving of a span.
    fn exit(&self, _span: &Id) {}
}

/// Collects the fields of one event as `name=value` pairs, the message first and without its name.
#[derive(Default)]
struct FieldsAsText {
    /// The line built so far.
    text: String,
}

impl Visit for FieldsAsText {
    /// Adds one field to the line: the message as it is, any other field as `name=value`.
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.text, " {value:?}");
        } else {
            let _ = write!(self.text, " {}={value:?}", field.name());
        }
    }
}
