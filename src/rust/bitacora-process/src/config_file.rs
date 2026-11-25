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
//! The file is read whole and parsed as TOML into the type the daemon asks for. The parser's error
//! already says which line and which field is wrong and what was expected there; this file adds the path
//! of the file.
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

// This module is the adapter that reads the configuration from the file system at start, before any of
// the injected interfaces exist.
#![allow(clippy::disallowed_methods)]

use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use thiserror::Error;

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
    /// The file was read but is not a valid configuration: it is not TOML, a field is unknown or
    /// missing, or a value has the wrong type.
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
/// Reads from the file system, synchronously: it is meant to be called once, at start.
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
/// * [`ConfigFileError::Invalid`] if its content is not a valid configuration of type `Config`.
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
    let file_text = match std::fs::read_to_string(path) {
        Ok(file_text) => file_text,
        Err(io_error) => {
            return Err(ConfigFileError::Unreadable {
                path: path.to_owned(),
                reason: io_error.to_string(),
            });
        }
    };
    match toml::from_str(&file_text) {
        Ok(config) => Ok(config),
        Err(toml_error) => Err(ConfigFileError::Invalid {
            path: path.to_owned(),
            reason: toml_error.to_string(),
        }),
    }
}

#[cfg(test)]
#[path = "config_file_test.rs"]
mod tests;
