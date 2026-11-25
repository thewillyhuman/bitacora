//! Tests of the daemons' command line.
//!
//! # What is covered
//!
//! - Both spellings of `--config`.
//! - `--help` and `--version`, alone and next to other arguments.
//! - Every way the arguments can be wrong.
//!
//! # How the fixtures work
//!
//! `parse` takes the arguments as string slices, which reads better than owned strings in a test.

use super::*;

/// Parses `arguments` as a daemon would.
fn parse(arguments: &[&str]) -> Result<CommandLine, UsageError> {
    let mut owned_arguments = Vec::new();
    for argument in arguments {
        owned_arguments.push((*argument).to_owned());
    }
    parse_command_line(owned_arguments)
}

/// The command line that runs with `/etc/bitacora/replica.toml`.
fn run_with_the_replica_config() -> CommandLine {
    CommandLine::Run {
        config_path: PathBuf::from("/etc/bitacora/replica.toml"),
    }
}

#[test]
fn config_path_follows_the_option() {
    let parsed = parse(&["--config", "/etc/bitacora/replica.toml"]);

    assert_eq!(parsed, Ok(run_with_the_replica_config()));
}

#[test]
fn config_path_can_be_joined_to_the_option() {
    let parsed = parse(&["--config=/etc/bitacora/replica.toml"]);

    assert_eq!(parsed, Ok(run_with_the_replica_config()));
}

#[test]
fn help_is_understood() {
    assert_eq!(parse(&["--help"]), Ok(CommandLine::PrintHelp));
}

#[test]
fn short_help_is_understood() {
    assert_eq!(parse(&["-h"]), Ok(CommandLine::PrintHelp));
}

#[test]
fn version_is_understood() {
    assert_eq!(parse(&["--version"]), Ok(CommandLine::PrintVersion));
}

#[test]
fn short_version_is_understood() {
    assert_eq!(parse(&["-V"]), Ok(CommandLine::PrintVersion));
}

#[test]
fn help_wins_over_arguments_that_would_be_refused() {
    let parsed = parse(&["--no-such-option", "--help"]);

    assert_eq!(parsed, Ok(CommandLine::PrintHelp));
}

#[test]
fn help_wins_over_version() {
    let parsed = parse(&["--version", "--help"]);

    assert_eq!(parsed, Ok(CommandLine::PrintHelp));
}

#[test]
fn no_arguments_are_refused() {
    assert_eq!(parse(&[]), Err(UsageError::MissingConfigPath));
}

#[test]
fn config_option_without_a_path_is_refused() {
    assert_eq!(parse(&["--config"]), Err(UsageError::MissingConfigValue));
}

#[test]
fn config_option_with_an_empty_path_is_refused() {
    assert_eq!(parse(&["--config="]), Err(UsageError::MissingConfigValue));
}

#[test]
fn config_option_given_twice_is_refused() {
    let parsed = parse(&["--config", "a.toml", "--config", "b.toml"]);

    assert_eq!(parsed, Err(UsageError::RepeatedConfigPath));
}

#[test]
fn unknown_argument_is_refused_by_name() {
    let parsed = parse(&["--config", "a.toml", "--verbose"]);

    assert_eq!(
        parsed,
        Err(UsageError::UnknownArgument {
            argument: "--verbose".to_owned()
        })
    );
}

#[test]
fn path_without_the_option_is_refused() {
    let parsed = parse(&["/etc/bitacora/replica.toml"]);

    assert_eq!(
        parsed,
        Err(UsageError::UnknownArgument {
            argument: "/etc/bitacora/replica.toml".to_owned()
        })
    );
}

#[test]
fn usage_names_the_daemon() {
    let text = usage("bita-replica");

    assert!(text.starts_with("Usage: bita-replica --config <path>\n"), "{text}");
}

#[test]
fn usage_lists_the_config_option() {
    let text = usage("bita-replica");

    assert!(text.contains("--config <path>  The configuration file"), "{text}");
}
