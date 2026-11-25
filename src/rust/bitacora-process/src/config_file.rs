//! Reading a daemon's configuration file.
//!
//! # Why this file exists
//!
//! Each daemon has one configuration file (design, section 14, "Deployment targets"). A daemon that
//! starts with a configuration it only half understood is worse than one that does not start: a
//! misspelled setting silently keeps its default, and nobody notices until the default hurts. This file
//! is the one place where configuration files are read, so that every daemon refuses a bad one the same
//! way and with an error an operator can act on.
//!
//! # What it contains
//!
//! - [`load_config_file`]: reads a TOML file into a configuration type.
//! - [`ConfigFileError`]: why a file could not be used.
//!
//! # How it works
//!
//! The file is read whole, its references to environment variables are replaced, and the result is
//! parsed as TOML into the type the daemon asks for. The parser's error already says which line and
//! which field is wrong and what was expected there; this file adds the path of the file.
//!
//! A double-quoted string may take all or part of its value from the environment of the process:
//! `replica_id = "${POD_NAME}"`. That is how one file serves many processes that differ in a few
//! settings, as the pods of one set on Kubernetes do. A reference to a variable that is not set is an
//! error, never an empty value. `config_environment.rs` has the exact rules.
//!
//! Unknown fields are refused by the configuration types themselves: every one of them carries
//! `#[serde(deny_unknown_fields)]`, and the tests of each daemon check it. This function cannot enforce
//! it for a type that leaves the attribute out.
//!
//! Values that are well-formed TOML but make no sense together (an address without a port, a threshold
//! above 100) are not this file's business: each daemon validates its own configuration after loading.
//!
//! # Invariants
//!
//! - A configuration is either loaded completely or not at all.
//! - Every error names the file.
//!
//! # See also
//!
//! - `command_line.rs`: where the path of the file comes from.
//! - `config_environment.rs`: how references to environment variables are replaced.

// This module is the adapter that reads the configuration from the file system at start, before any of
// the injected interfaces exist.
#![allow(clippy::disallowed_methods)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::config_environment::expand_environment;

/// Why a configuration file could not be used.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ConfigFileError {
    /// The file could not be read: it does not exist, is not readable, or is not text.
    #[error("cannot read configuration file {}: {reason}", path.display())]
    Unreadable {
        /// The file that was asked for.
        path: PathBuf,
        /// What the operating system reported.
        reason: String,
    },
    /// The file was read but is not a valid configuration: it refers to an environment variable that
    /// is not set, it is not TOML, a field is unknown or missing, or a value has the wrong type.
    #[error("configuration file {} is invalid: {reason}", path.display())]
    Invalid {
        /// The file that was read.
        path: PathBuf,
        /// What is wrong, with the line and the field.
        reason: String,
    },
}

/// Reads the TOML file at `path` into a configuration.
///
/// Reads from the file system, synchronously: it is meant to be called once, at start. A `${NAME}`
/// inside a double-quoted string of the file is replaced by the value of the environment variable
/// `NAME` of this process before the file is parsed.
///
/// # Arguments
///
/// * `path` - The configuration file.
///
/// # Returns
///
/// The configuration, with every field the file left out at its default.
///
/// # Errors
///
/// * [`ConfigFileError::Unreadable`] if the file cannot be read.
/// * [`ConfigFileError::Invalid`] if it refers to an environment variable that is not set, or if its
///   content is not a valid configuration of type `Config`.
///
/// # Examples
///
/// ```
/// use bitacora_process::load_config_file;
/// use serde::Deserialize;
///
/// #[derive(Debug, Deserialize)]
/// #[serde(deny_unknown_fields)]
/// struct ExampleConfig {
///     listen_address: String,
/// }
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let directory = tempfile::tempdir()?;
/// let path = directory.path().join("example.toml");
/// std::fs::write(&path, "listen_address = \"0.0.0.0:7000\"\n")?;
///
/// let config: ExampleConfig = load_config_file(&path)?;
///
/// assert_eq!(config.listen_address, "0.0.0.0:7000");
/// # Ok(())
/// # }
/// ```
pub fn load_config_file<Config: DeserializeOwned>(path: &Path) -> Result<Config, ConfigFileError> {
    load_config_file_with(path, &process_environment())
}

/// Reads the TOML file at `path` into a configuration, taking the values of the environment variables it
/// refers to from `variables`.
///
/// # Errors
///
/// As [`load_config_file`].
fn load_config_file_with<Config: DeserializeOwned>(
    path: &Path,
    variables: &BTreeMap<String, String>,
) -> Result<Config, ConfigFileError> {
    let file_text = match std::fs::read_to_string(path) {
        Ok(file_text) => file_text,
        Err(io_error) => {
            return Err(ConfigFileError::Unreadable {
                path: path.to_owned(),
                reason: io_error.to_string(),
            });
        }
    };
    let expanded_text = match expand_environment(&file_text, variables) {
        Ok(expanded_text) => expanded_text,
        Err(expansion_error) => {
            return Err(ConfigFileError::Invalid {
                path: path.to_owned(),
                reason: expansion_error.to_string(),
            });
        }
    };
    match toml::from_str(&expanded_text) {
        Ok(config) => Ok(config),
        Err(toml_error) => Err(ConfigFileError::Invalid {
            path: path.to_owned(),
            reason: toml_error.to_string(),
        }),
    }
}

/// The environment variables of this process whose name and value are both text.
///
/// A variable that is not valid UTF-8 is left out, so a reference to it is reported as not set: a
/// configuration cannot hold such a value anyway.
fn process_environment() -> BTreeMap<String, String> {
    let mut variables = BTreeMap::new();
    for (name, value) in std::env::vars_os() {
        if let (Ok(name), Ok(value)) = (name.into_string(), value.into_string()) {
            variables.insert(name, value);
        }
    }
    variables
}

#[cfg(test)]
#[path = "config_file_test.rs"]
mod tests;
