//! Tests of the daemons' log output.
//!
//! # What is covered
//!
//! - The JSON format: one object per line, with the level, the message and the event's fields at the top
//!   level.
//! - The level: events below it are dropped, events at it are written.
//! - The text format writes the message readably.
//! - How the `[log]` table is read: defaults, each value, and refusal of unknown values and fields.
//!
//! # How the fixtures work
//!
//! `logged_by` builds the log output with a writer that keeps everything in memory, makes it the output
//! of the current thread only while the test emits its events, and returns what was written. The
//! process-wide output is never installed here, so the tests do not disturb each other.

use std::io;
use std::sync::{Arc, Mutex, PoisonError};

use super::*;

/// A writer that appends to a buffer shared with the test.
#[derive(Clone, Default)]
struct CapturedOutput {
    /// Everything written so far.
    written_bytes: Arc<Mutex<Vec<u8>>>,
}

impl io::Write for CapturedOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.written_bytes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl CapturedOutput {
    /// Everything written so far, as text.
    fn text(&self) -> String {
        let written_bytes = self
            .written_bytes
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        String::from_utf8(written_bytes.clone()).expect("log output is text")
    }
}

/// What kind of event a test emits.
#[derive(Clone, Copy, Debug)]
enum TestEvent {
    /// `info!(replica_id = "replica-a", segment_count = 12, "replica opened")`
    ReplicaOpened,
    /// `debug!("read failed, trying another replica")`
    DebugDetail,
}

/// Returns what a log output with `settings` writes for `events`.
fn logged_by(settings: LogSettings, events: &[TestEvent]) -> String {
    let captured_output = CapturedOutput::default();
    let writer = captured_output.clone();
    let log_output = build_log_output(settings, move || writer.clone());
    tracing::subscriber::with_default(log_output, || {
        for event in events {
            match event {
                TestEvent::ReplicaOpened => {
                    tracing::info!(
                        replica_id = "replica-a",
                        segment_count = 12,
                        "replica opened"
                    );
                }
                TestEvent::DebugDetail => {
                    tracing::debug!("read failed, trying another replica");
                }
            }
        }
    });
    captured_output.text()
}

/// Settings for the JSON format at the given level.
fn json_at(level: LogLevel) -> LogSettings {
    LogSettings {
        level,
        format: LogFormat::Json,
    }
}

/// The single JSON line a log output wrote, parsed.
fn single_json_line(logged: &str) -> serde_json::Value {
    let mut lines = logged.lines();
    let line = lines.next().expect("one line was written");
    assert_eq!(lines.next(), None, "more than one line: {logged}");
    serde_json::from_str(line).expect("the line is JSON")
}

/// Parses a `[log]` table from JSON, which reads the same `serde` attributes as TOML does.
fn parse_settings(json: &str) -> Result<LogSettings, serde_json::Error> {
    serde_json::from_str(json)
}

#[test]
fn json_event_is_one_line() {
    let logged = logged_by(json_at(LogLevel::Info), &[TestEvent::ReplicaOpened]);

    assert_eq!(logged.lines().count(), 1, "{logged}");
}

#[test]
fn json_event_carries_its_message() {
    let logged = logged_by(json_at(LogLevel::Info), &[TestEvent::ReplicaOpened]);

    assert_eq!(single_json_line(&logged)["message"], "replica opened");
}

#[test]
fn json_event_carries_its_level() {
    let logged = logged_by(json_at(LogLevel::Info), &[TestEvent::ReplicaOpened]);

    assert_eq!(single_json_line(&logged)["level"], "INFO");
}

#[test]
fn json_event_carries_its_text_fields_at_the_top_level() {
    let logged = logged_by(json_at(LogLevel::Info), &[TestEvent::ReplicaOpened]);

    assert_eq!(single_json_line(&logged)["replica_id"], "replica-a");
}

#[test]
fn json_event_carries_its_numeric_fields_as_numbers() {
    let logged = logged_by(json_at(LogLevel::Info), &[TestEvent::ReplicaOpened]);

    assert_eq!(single_json_line(&logged)["segment_count"], 12);
}

#[test]
fn json_event_carries_a_timestamp() {
    let logged = logged_by(json_at(LogLevel::Info), &[TestEvent::ReplicaOpened]);

    assert!(
        single_json_line(&logged)["timestamp"].is_string(),
        "{logged}"
    );
}

#[test]
fn event_below_the_configured_level_is_dropped() {
    let logged = logged_by(json_at(LogLevel::Info), &[TestEvent::DebugDetail]);

    assert_eq!(logged, "");
}

#[test]
fn event_at_the_configured_level_is_written() {
    let logged = logged_by(json_at(LogLevel::Debug), &[TestEvent::DebugDetail]);

    assert_eq!(single_json_line(&logged)["level"], "DEBUG");
}

#[test]
fn error_level_drops_informational_events() {
    let logged = logged_by(json_at(LogLevel::Error), &[TestEvent::ReplicaOpened]);

    assert_eq!(logged, "");
}

#[test]
fn every_event_gets_its_own_line() {
    let logged = logged_by(
        json_at(LogLevel::Trace),
        &[TestEvent::ReplicaOpened, TestEvent::DebugDetail],
    );

    assert_eq!(logged.lines().count(), 2, "{logged}");
}

#[test]
fn text_format_writes_the_message_readably() {
    let text_settings = LogSettings {
        level: LogLevel::Info,
        format: LogFormat::Text,
    };

    let logged = logged_by(text_settings, &[TestEvent::ReplicaOpened]);

    assert!(logged.contains("replica opened"), "{logged}");
}

#[test]
fn text_format_is_not_json() {
    let text_settings = LogSettings {
        level: LogLevel::Info,
        format: LogFormat::Text,
    };

    let logged = logged_by(text_settings, &[TestEvent::ReplicaOpened]);

    assert!(!logged.starts_with('{'), "{logged}");
}

#[test]
fn empty_log_table_means_json_at_info() {
    let settings = parse_settings("{}");

    assert_eq!(
        settings.ok(),
        Some(LogSettings {
            level: LogLevel::Info,
            format: LogFormat::Json,
        })
    );
}

#[test]
fn log_table_can_set_the_level_and_the_format() {
    let settings = parse_settings(r#"{"level": "debug", "format": "text"}"#);

    assert_eq!(
        settings.ok(),
        Some(LogSettings {
            level: LogLevel::Debug,
            format: LogFormat::Text,
        })
    );
}

#[test]
fn unknown_level_is_refused() {
    let settings = parse_settings(r#"{"level": "chatty"}"#);

    assert!(settings.is_err(), "{settings:?}");
}

#[test]
fn unknown_format_is_refused() {
    let settings = parse_settings(r#"{"format": "xml"}"#);

    assert!(settings.is_err(), "{settings:?}");
}

#[test]
fn misspelled_log_setting_is_refused() {
    let settings = parse_settings(r#"{"levle": "debug"}"#);

    assert!(settings.is_err(), "{settings:?}");
}
