//! The log output of a daemon.
//!
//! # Why this file exists
//!
//! Every bitacora process writes structured JSON logs (design, section 14, "Observability"): one event
//! per line, with its fields as JSON members, so that a log pipeline can filter on `stream` or `segment`
//! without parsing sentences. The library crates only emit events through `tracing`; somebody has to
//! decide where they go and in which form. That is this file, once for all daemons, so that their logs
//! look the same.
//!
//! # What it contains
//!
//! - [`LogSettings`], [`LogLevel`], [`LogFormat`]: the `[log]` table of a daemon's configuration file.
//! - [`init_logging`]: installs the log output for the whole process.
//! - [`LoggingError`]: why it could not be installed.
//!
//! # How it works
//!
//! Events go to standard output, which is where a process supervisor and a container runtime collect
//! them from. In the JSON format an event is one line such as
//!
//! ```text
//! {"timestamp":"2026-11-25T22:03:44.123456Z","level":"INFO","message":"replica opened",
//!  "replica_id":"replica-a","segment_count":12,"target":"bitacora_replica::replica"}
//! ```
//!
//! with the event's fields next to `message`, not nested. The text format is for a person reading a
//! terminal during development; nothing should parse it.
//!
//! Events below the configured level are dropped before they are formatted.
//!
//! # Invariants
//!
//! - The log output is installed once per process and never replaced.
//! - Nothing is written to standard error, so that the two streams do not interleave.
//!
//! # See also
//!
//! - `config_file.rs`: reads the configuration that holds the [`LogSettings`].

use serde::Deserialize;
use thiserror::Error;
use tracing::Subscriber;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::fmt::MakeWriter;

/// The `[log]` table of a daemon's configuration file.
///
/// # Examples
///
/// ```toml
/// [log]
/// level = "info"
/// format = "json"
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogSettings {
    /// The least severe events that are written. Defaults to `info`.
    #[serde(default)]
    pub level: LogLevel,
    /// The form events are written in. Defaults to `json`.
    #[serde(default)]
    pub format: LogFormat,
}

/// How severe an event must be to be written.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    /// Only failures the process could not handle.
    Error,
    /// Also trouble the process worked around, such as a request that will be retried.
    Warn,
    /// Also the normal milestones: a segment sealed, a copy repaired.
    #[default]
    Info,
    /// Also the detail needed to follow one request.
    Debug,
    /// Everything.
    Trace,
}

/// The form log events are written in.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    /// One JSON object per line, for machines.
    #[default]
    Json,
    /// One readable line per event, for a person at a terminal.
    Text,
}

/// Why the log output could not be installed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum LoggingError {
    /// The process already has a log output. It is installed once, at start.
    #[error("the log output of this process is already installed")]
    AlreadyInstalled,
}

/// Installs the log output of the process: every `tracing` event at or above the configured level is
/// written to standard output in the configured format.
///
/// Call it once, right after the configuration is loaded. Events emitted before are lost.
///
/// # Arguments
///
/// * `settings` - The level and the format.
///
/// # Errors
///
/// Returns [`LoggingError::AlreadyInstalled`] if a log output was installed before.
///
/// # Examples
///
/// ```
/// use bitacora_process::{LogFormat, LogLevel, LogSettings, init_logging};
///
/// let settings = LogSettings {
///     level: LogLevel::Info,
///     format: LogFormat::Json,
/// };
/// init_logging(settings).expect("nothing installed a log output before");
/// tracing::info!(replica_id = "replica-a", "replica opened");
/// ```
pub fn init_logging(settings: LogSettings) -> Result<(), LoggingError> {
    let log_output = build_log_output(settings, std::io::stdout);
    match tracing::subscriber::set_global_default(log_output) {
        Ok(()) => Ok(()),
        Err(_already_set) => Err(LoggingError::AlreadyInstalled),
    }
}

/// Builds the log output that writes to `writer`, without installing it. Tests use it to capture what
/// would be written.
fn build_log_output<Writer>(settings: LogSettings, writer: Writer) -> Box<dyn Subscriber + Send + Sync>
where
    Writer: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
{
    let least_severe_level = match settings.level {
        LogLevel::Error => LevelFilter::ERROR,
        LogLevel::Warn => LevelFilter::WARN,
        LogLevel::Info => LevelFilter::INFO,
        LogLevel::Debug => LevelFilter::DEBUG,
        LogLevel::Trace => LevelFilter::TRACE,
    };
    let builder = tracing_subscriber::fmt()
        .with_max_level(least_severe_level)
        .with_writer(writer);
    match settings.format {
        // Fields next to the message, and no span context: the events carry what a reader needs.
        LogFormat::Json => Box::new(
            builder
                .json()
                .flatten_event(true)
                .with_current_span(false)
                .with_span_list(false)
                .finish(),
        ),
        LogFormat::Text => Box::new(builder.finish()),
    }
}

#[cfg(test)]
#[path = "logging_test.rs"]
mod tests;
