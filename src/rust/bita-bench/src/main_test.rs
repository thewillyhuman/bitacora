//! Tests of how the tool turns a command line into the settings of its client.
//!
//! # What is covered
//!
//! - The request size of an append workload becomes the writer's, and a scan keeps the writer's default.
//! - The tool's writers carry its name on their leases.
//!
//! The workloads themselves are tested in `workload_test.rs`, the command line in `arguments_test.rs`.
//!
//! # How the fixtures work
//!
//! `parsed` parses a command line as `main` does, so each test starts from what a person would type.

use super::*;

/// The workload of `command_line`, which is split at single spaces.
fn parsed(command_line: &str) -> WorkloadCommand {
    Arguments::try_parse_from(command_line.split(' '))
        .expect("a valid command line")
        .workload
}

#[test]
fn request_size_of_an_append_becomes_the_writers() {
    let workload = parsed("bita-bench append orders --request-bytes 1048576");

    let settings = client_settings(&workload);

    assert_eq!(settings.writer.max_request_bytes, 1_048_576);
}

#[test]
fn scan_keeps_the_default_request_size_of_a_writer() {
    let workload = parsed("bita-bench read orders");

    let settings = client_settings(&workload);

    assert_eq!(
        settings.writer.max_request_bytes,
        WriterSettings::default().max_request_bytes
    );
}

#[test]
fn writers_of_the_tool_carry_its_name() {
    let workload = parsed("bita-bench append orders");

    let settings = client_settings(&workload);

    assert_eq!(settings.writer.writer_identity, "bita-bench");
}
