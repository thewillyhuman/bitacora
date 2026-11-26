//! Tests of the command line of `bita-bench`.
//!
//! # What is covered
//!
//! The definition is consistent, each workload parses with its defaults and with every option, the two
//! limits exclude each other, and the limit a workload runs with follows from them.

use clap::CommandFactory;
use clap::error::ErrorKind;

use super::*;

/// Parses a command line written as one string of words separated by single spaces.
fn parse(command_line: &str) -> Result<Arguments, clap::Error> {
    Arguments::try_parse_from(command_line.split(' '))
}

#[test]
fn command_line_definition_is_consistent() {
    Arguments::command().debug_assert();
}

#[test]
fn append_workload_has_its_defaults() {
    let arguments = parse("bita-bench append orders").expect("a valid command line");

    assert_eq!(
        arguments.workload,
        WorkloadCommand::Append {
            stream: "orders".to_owned(),
            record_bytes: 1024,
            records_per_append: 1,
            appends_in_flight: 16,
            request_bytes: 262_144,
            seconds: None,
            records: None,
            rate: None,
            streams: 1,
            create: false,
            force: false,
        }
    );
}

/// Without the option, a benchmark sends what an application with default settings sends.
#[test]
fn default_request_size_is_the_writers_default() {
    assert_eq!(
        DEFAULT_REQUEST_BYTES,
        bitacora::WriterSettings::default().max_request_bytes
    );
}

#[test]
fn append_workload_takes_every_option() {
    let arguments = parse(
        "bita-bench append orders --record-bytes 4096 --records-per-append 8 --appends-in-flight 64 \
         --request-bytes 1048576 --seconds 30 --rate 50000 --streams 4 --create --force",
    )
    .expect("a valid command line");

    assert_eq!(
        arguments.workload,
        WorkloadCommand::Append {
            stream: "orders".to_owned(),
            record_bytes: 4096,
            records_per_append: 8,
            appends_in_flight: 64,
            request_bytes: 1_048_576,
            seconds: Some(30),
            records: None,
            rate: Some(50_000),
            streams: 4,
            create: true,
            force: true,
        }
    );
}

#[test]
fn read_workload_takes_a_start_and_a_record_limit() {
    let arguments = parse("bita-bench read orders --from 100 --records 5000").expect("a valid command line");

    assert_eq!(
        arguments.workload,
        WorkloadCommand::Read {
            stream: "orders".to_owned(),
            from: Some(100),
            seconds: None,
            records: Some(5_000),
        }
    );
}

#[test]
fn output_defaults_to_text() {
    let arguments = parse("bita-bench append orders").expect("a valid command line");

    assert_eq!(arguments.output, OutputFormat::Text);
}

#[test]
fn cluster_options_are_accepted_after_the_workload() {
    let arguments = parse(
        "bita-bench append orders --etcd-endpoint http://a:2379 --etcd-key-prefix /staging/ \
         --output json",
    )
    .expect("a valid command line");

    assert_eq!(arguments.etcd_endpoints, ["http://a:2379"]);
    assert_eq!(arguments.etcd_key_prefix, Some("/staging/".to_owned()));
    assert_eq!(arguments.output, OutputFormat::Json);
}

#[test]
fn seconds_and_records_together_are_a_usage_error() {
    let refused = parse("bita-bench append orders --seconds 5 --records 100").expect_err("an invalid command line");

    assert_eq!(refused.kind(), ErrorKind::ArgumentConflict);
}

#[test]
fn workload_without_a_stream_is_a_usage_error() {
    let refused = parse("bita-bench append").expect_err("an invalid command line");

    assert_eq!(refused.kind(), ErrorKind::MissingRequiredArgument);
}

#[test]
fn record_limit_is_used_when_given() {
    assert_eq!(workload_limit(None, Some(500)), WorkloadLimit::Records(500));
}

#[test]
fn duration_limit_is_used_when_given() {
    assert_eq!(
        workload_limit(Some(30), None),
        WorkloadLimit::Duration(Duration::from_secs(30))
    );
}

#[test]
fn workload_without_a_limit_runs_for_the_default_duration() {
    assert_eq!(
        workload_limit(None, None),
        WorkloadLimit::Duration(Duration::from_secs(DEFAULT_SECONDS))
    );
}
