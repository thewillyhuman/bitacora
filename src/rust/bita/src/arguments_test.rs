//! Tests of the command line.
//!
//! # What is covered
//!
//! - The definition is consistent, by `clap`'s own check.
//! - Each family of commands parses into the expected value, with its defaults.
//! - Global options are accepted before and after the command.
//! - Mistakes are usage errors: an unknown command, a missing argument, a duration or a replica
//!   identifier that is not one.
//!
//! # How the fixtures work
//!
//! `parse` runs the real parser on a command line written as one string, split at spaces. Nothing is
//! connected: parsing has no side effect.

use clap::CommandFactory;
use clap::error::ErrorKind;

use super::*;

/// Parses a command line written as one string of words separated by single spaces.
fn parse(command_line: &str) -> Result<Arguments, clap::Error> {
    Arguments::try_parse_from(command_line.split(' '))
}

/// Parses a command line that is expected to be valid, and returns its command.
fn command_of(command_line: &str) -> Command {
    parse(command_line).expect("a valid command line").command
}

/// The kind of usage error a command line is refused with.
fn refusal_of(command_line: &str) -> ErrorKind {
    parse(command_line)
        .expect_err("an invalid command line")
        .kind()
}

#[test]
fn command_line_definition_is_consistent() {
    Arguments::command().debug_assert();
}

#[test]
fn output_defaults_to_a_table() {
    let arguments = parse("bita stream list").expect("a valid command line");

    assert_eq!(arguments.output, OutputFormat::Table);
}

#[test]
fn request_timeout_defaults_to_five_seconds() {
    let arguments = parse("bita stream list").expect("a valid command line");

    assert_eq!(arguments.request_timeout, Duration::from_secs(5));
}

#[test]
fn global_options_are_accepted_after_the_command() {
    let arguments =
        parse("bita stream list --output json --request-timeout 250ms").expect("a valid line");

    assert_eq!(arguments.output, OutputFormat::Json);
    assert_eq!(arguments.request_timeout, Duration::from_millis(250));
}

#[test]
fn global_options_are_accepted_before_the_command() {
    let arguments = parse("bita --output json stream list").expect("a valid command line");

    assert_eq!(arguments.output, OutputFormat::Json);
}

#[test]
fn etcd_endpoint_can_be_repeated() {
    let arguments =
        parse("bita --etcd-endpoint http://a:2379 --etcd-endpoint http://b:2379 stream list")
            .expect("a valid command line");

    assert_eq!(
        arguments.cluster.etcd_endpoints,
        ["http://a:2379", "http://b:2379"]
    );
}

#[test]
fn config_file_and_key_prefix_are_read() {
    let arguments = parse("bita --config /etc/bita.toml --etcd-key-prefix /staging/ stream list")
        .expect("a valid command line");

    assert_eq!(
        arguments.cluster.config,
        Some(PathBuf::from("/etc/bita.toml"))
    );
    assert_eq!(
        arguments.cluster.etcd_key_prefix,
        Some("/staging/".to_owned())
    );
}

#[test]
fn stream_create_has_the_design_defaults() {
    assert_eq!(
        command_of("bita stream create orders"),
        Command::Stream(StreamCommand::Create {
            name: "orders".to_owned(),
            replicas: 3,
            write_quorum: 2,
            keep_sealed_for: None,
        })
    );
}

#[test]
fn stream_create_takes_replication_and_retention() {
    assert_eq!(
        command_of("bita stream create orders --replicas 5 --write-quorum 3 --keep-sealed-for 7d"),
        Command::Stream(StreamCommand::Create {
            name: "orders".to_owned(),
            replicas: 5,
            write_quorum: 3,
            keep_sealed_for: Some(Duration::from_hours(7 * 24)),
        })
    );
}

#[test]
fn stream_delete_is_not_confirmed_by_default() {
    assert_eq!(
        command_of("bita stream delete orders"),
        Command::Stream(StreamCommand::Delete {
            name: "orders".to_owned(),
            yes: false,
        })
    );
}

#[test]
fn stream_truncate_before_takes_a_sequence_number() {
    assert_eq!(
        command_of("bita stream truncate-before orders 1500"),
        Command::Stream(StreamCommand::TruncateBefore {
            name: "orders".to_owned(),
            sequence: 1_500,
        })
    );
}

#[test]
fn stream_register_materializer_takes_stream_and_materializer() {
    assert_eq!(
        command_of("bita stream register-materializer orders pages"),
        Command::Stream(StreamCommand::RegisterMaterializer {
            name: "orders".to_owned(),
            materializer: "pages".to_owned(),
        })
    );
}

#[test]
fn stream_append_takes_records_as_arguments() {
    assert_eq!(
        command_of("bita stream append orders first second --force"),
        Command::Stream(StreamCommand::Append {
            name: "orders".to_owned(),
            records: vec!["first".to_owned(), "second".to_owned()],
            force: true,
        })
    );
}

#[test]
fn stream_read_reads_a_hundred_records_by_default() {
    assert_eq!(
        command_of("bita stream read orders"),
        Command::Stream(StreamCommand::Read {
            name: "orders".to_owned(),
            from: None,
            count: 100,
        })
    );
}

#[test]
fn stream_tail_follows_without_a_limit_by_default() {
    assert_eq!(
        command_of("bita stream tail orders --from 7"),
        Command::Stream(StreamCommand::Tail {
            name: "orders".to_owned(),
            from: Some(7),
            count: None,
        })
    );
}

#[test]
fn replica_drain_takes_a_wait() {
    assert_eq!(
        command_of("bita replica drain replica-0 --wait 2m"),
        Command::Replica(ReplicaCommand::Drain {
            replica_id: ReplicaId::new("replica-0").expect("a valid identifier"),
            wait: Some(Duration::from_mins(2)),
        })
    );
}

#[test]
fn replica_remove_is_not_forced_by_default() {
    assert_eq!(
        command_of("bita replica remove replica-0"),
        Command::Replica(ReplicaCommand::Remove {
            replica_id: ReplicaId::new("replica-0").expect("a valid identifier"),
            force: false,
        })
    );
}

#[test]
fn segment_list_takes_a_stream() {
    assert_eq!(
        command_of("bita segment list orders"),
        Command::Segment(SegmentCommand::List {
            stream: "orders".to_owned(),
        })
    );
}

#[test]
fn controller_status_parses() {
    assert_eq!(
        command_of("bita controller status"),
        Command::Controller(ControllerCommand::Status)
    );
}

#[test]
fn unknown_command_is_a_usage_error() {
    assert_eq!(
        refusal_of("bita stream explode"),
        ErrorKind::InvalidSubcommand
    );
}

#[test]
fn missing_argument_is_a_usage_error() {
    assert_eq!(
        refusal_of("bita stream describe"),
        ErrorKind::MissingRequiredArgument
    );
}

#[test]
fn duration_without_a_unit_is_a_usage_error() {
    assert_eq!(
        refusal_of("bita stream list --request-timeout 5"),
        ErrorKind::ValueValidation
    );
}

#[test]
fn text_that_is_no_replica_identifier_is_a_usage_error() {
    assert_eq!(
        refusal_of("bita replica status not/a/replica"),
        ErrorKind::ValueValidation
    );
}

#[test]
fn sequence_that_is_no_number_is_a_usage_error() {
    assert_eq!(
        refusal_of("bita stream truncate-before orders soon"),
        ErrorKind::ValueValidation
    );
}

#[test]
fn unknown_output_format_is_a_usage_error() {
    assert_eq!(
        refusal_of("bita stream list --output yaml"),
        ErrorKind::InvalidValue
    );
}

#[test]
fn help_is_not_a_mistake() {
    let help = parse("bita --help").expect_err("help ends parsing");

    assert!(!help.use_stderr());
}

/// The user's reference of the tool. It is compiled into the tests so that it cannot describe a
/// different tool than the one that is built.
const REFERENCE: &str = include_str!("../../../../docs/cli.md");

/// What a section of the reference that documents one command starts with.
const COMMAND_HEADING_START: &str = "### `bita ";

/// Every command of the tool, as it is typed: `bita stream create`, `bita metadata snapshot list`, ...
fn commands_of_the_tool() -> Vec<String> {
    let mut commands = Vec::new();
    collect_commands("bita", &Arguments::command(), &mut commands);
    commands
}

/// Adds the commands below `command`, which is typed as `typed_so_far`, to `commands`. A command without
/// subcommands is one the operator can run; one with subcommands only groups them.
fn collect_commands(typed_so_far: &str, command: &clap::Command, commands: &mut Vec<String>) {
    let mut has_subcommands = false;
    for subcommand in command.get_subcommands() {
        has_subcommands = true;
        let typed = format!("{typed_so_far} {}", subcommand.get_name());
        collect_commands(&typed, subcommand, commands);
    }
    if !has_subcommands {
        commands.push(typed_so_far.to_owned());
    }
}

/// Every command the reference has a section for, as it is typed.
fn commands_of_the_reference() -> Vec<String> {
    let mut commands = Vec::new();
    for line in REFERENCE.lines() {
        if let Some(rest) = line.strip_prefix(COMMAND_HEADING_START) {
            let command = rest.trim_end_matches('`');
            commands.push(format!("bita {command}"));
        }
    }
    commands
}

#[test]
fn every_command_has_a_section_in_the_reference() {
    let documented = commands_of_the_reference();

    for command in commands_of_the_tool() {
        assert!(
            documented.contains(&command),
            "docs/cli.md has no section for `{command}`"
        );
    }
}

#[test]
fn reference_documents_no_command_that_does_not_exist() {
    let existing = commands_of_the_tool();

    for command in commands_of_the_reference() {
        assert!(
            existing.contains(&command),
            "docs/cli.md documents `{command}`, which the tool does not have"
        );
    }
}
