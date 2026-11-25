//! The command line of a daemon.
//!
//! # Why this file exists
//!
//! A daemon takes everything it needs to know from one configuration file, so its command line has one
//! job: saying where that file is, and whether to run with it or only to check it. Both daemons must
//! agree on how, so that unit files and container images look the same for each. This file is that
//! agreement.
//!
//! # What it contains
//!
//! - [`parse_command_line`]: turns the arguments into a [`CommandLine`].
//! - [`CommandLine`]: what the daemon was asked to do.
//! - [`UsageError`]: why the arguments make no sense.
//! - [`usage`]: the text a daemon prints for `--help` and after a [`UsageError`].
//!
//! # How it works
//!
//! The grammar is small enough to parse by hand, which keeps an argument parsing library out of the
//! daemons:
//!
//! ```text
//! <daemon> --config <path>
//! <daemon> --config=<path>
//! <daemon> --config <path> --check-config
//! <daemon> --help
//! <daemon> --version
//! ```
//!
//! `--help` and `--version` win over everything else, so that they work even next to arguments that
//! would be refused.
//!
//! `--check-config` exists so that a configuration can be judged before anything depends on it: by a
//! supervisor before it starts or restarts the daemon, by whatever renders the file for a deployment,
//! and by the tests of this repository's own deployment files. The daemon reads and validates the file
//! exactly as it would at start, opens nothing, and exits.
//!
//! # Invariants
//!
//! - Parsing has no side effects: it prints nothing and reads no file.
//!
//! # See also
//!
//! - `config_file.rs`: reads the file the command line names.

use std::path::PathBuf;

use thiserror::Error;

/// What a daemon was asked to do on its command line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandLine {
    /// Run with the configuration file at this path.
    Run {
        /// The configuration file, as given.
        config_path: PathBuf,
    },
    /// Read and validate the configuration file at this path, say whether it is valid, and exit without
    /// starting anything.
    CheckConfig {
        /// The configuration file, as given.
        config_path: PathBuf,
    },
    /// Print the usage and exit successfully.
    PrintHelp,
    /// Print the version and exit successfully.
    PrintVersion,
}

/// Why a command line cannot be understood.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum UsageError {
    /// No configuration file was named.
    #[error("no configuration file was given: pass --config <path>")]
    MissingConfigPath,
    /// `--config` was the last argument, or its value was empty.
    #[error("--config needs the path of the configuration file")]
    MissingConfigValue,
    /// `--config` was given more than once.
    #[error("--config was given more than once")]
    RepeatedConfigPath,
    /// An argument this daemon does not know.
    #[error("unknown argument {argument:?}")]
    UnknownArgument {
        /// The argument as it was typed.
        argument: String,
    },
}

/// Understands the arguments of a daemon.
///
/// # Arguments
///
/// * `arguments` - The arguments after the program name, in order: `std::env::args().skip(1)`.
///
/// # Returns
///
/// What the daemon was asked to do.
///
/// # Errors
///
/// Returns [`UsageError`] if no configuration file is named, if `--config` has no value or appears
/// twice, or if an argument is unknown. `--check-config` needs a configuration file like a run does.
///
/// # Examples
///
/// ```
/// use std::path::PathBuf;
///
/// use bitacora_process::{CommandLine, parse_command_line};
///
/// let arguments = ["--config".to_owned(), "/etc/bitacora/replica.toml".to_owned()];
///
/// assert_eq!(
///     parse_command_line(arguments),
///     Ok(CommandLine::Run {
///         config_path: PathBuf::from("/etc/bitacora/replica.toml")
///     })
/// );
/// ```
pub fn parse_command_line(arguments: impl IntoIterator<Item = String>) -> Result<CommandLine, UsageError> {
    let arguments: Vec<String> = arguments.into_iter().collect();
    for argument in &arguments {
        if argument == "--help" || argument == "-h" {
            return Ok(CommandLine::PrintHelp);
        }
    }
    for argument in &arguments {
        if argument == "--version" || argument == "-V" {
            return Ok(CommandLine::PrintVersion);
        }
    }

    let mut config_path: Option<PathBuf> = None;
    let mut is_check_only = false;
    let mut remaining_arguments = arguments.into_iter();
    while let Some(argument) = remaining_arguments.next() {
        if argument == "--check-config" {
            is_check_only = true;
            continue;
        }
        let config_value = if argument == "--config" {
            remaining_arguments.next()
        } else if let Some(inline_value) = argument.strip_prefix("--config=") {
            Some(inline_value.to_owned())
        } else {
            return Err(UsageError::UnknownArgument { argument });
        };
        let Some(config_value) = config_value.filter(|value| !value.is_empty()) else {
            return Err(UsageError::MissingConfigValue);
        };
        if config_path.is_some() {
            return Err(UsageError::RepeatedConfigPath);
        }
        config_path = Some(PathBuf::from(config_value));
    }
    match config_path {
        Some(config_path) if is_check_only => Ok(CommandLine::CheckConfig { config_path }),
        Some(config_path) => Ok(CommandLine::Run { config_path }),
        None => Err(UsageError::MissingConfigPath),
    }
}

/// The usage text of a daemon.
///
/// # Arguments
///
/// * `daemon_name` - The name the daemon is started under, for example `bita-replica`.
///
/// # Returns
///
/// A few lines that list the accepted arguments, ending with a newline.
#[must_use]
pub fn usage(daemon_name: &str) -> String {
    format!(
        "Usage: {daemon_name} --config <path> [--check-config]\n\
         \n\
         Options:\n  \
           --config <path>  The configuration file (TOML)\n  \
           --check-config   Check the configuration file and exit: 0 if it is valid, 2 if not\n  \
           --help           Print this text\n  \
           --version        Print the version\n"
    )
}

#[cfg(test)]
#[path = "command_line_test.rs"]
mod tests;
