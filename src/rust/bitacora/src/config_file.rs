//! Working out which cluster a tool is to use, from a configuration file and from options.
//!
//! # Why this file exists
//!
//! A tool that drives a real cluster has to be told which one. Every tool takes the same configuration file as
//! the daemons of a deployment, or the etcd endpoints directly, and each used to read the file and apply the
//! options with code of its own. The file is also the only way to name the object store, which a tool needs as
//! soon as segments are tiered. This file is that step, once, for the tools that take a file and options.
//!
//! # What it contains
//!
//! - [`resolve_cluster_config`]: from a named file and the cluster options to a [`ClusterConfig`].
//! - [`ClusterConfigError`]: why no usable configuration was found.
//!
//! # How it works
//!
//! The configuration file, if one is named, is TOML holding a [`ClusterConfig`]. Endpoints and a key prefix given
//! as options replace the file's; the object store can only come from the file. No environment variable is read
//! here: a tool that takes values from the environment, as `bita` does, looks them up itself.
//!
//! A configuration without any etcd endpoint is refused: a tool never guesses a cluster.
//!
//! # Invariants
//!
//! - Nothing but the named file is read, and nothing is connected.
//!
//! # See also
//!
//! - `cluster.rs`: the configuration structure and the connecting.
//! - `bita-bench` and `bita-soak`: the tools that call this. `bita` still answers the same question with code of
//!   its own, because it also reads the environment.

use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::cluster::{ClusterConfig, DEFAULT_ETCD_KEY_PREFIX, ObjectStoreConfig};

/// Why no usable cluster configuration was found.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ClusterConfigError {
    /// Neither a file nor an endpoint was given.
    #[error("no cluster given: use --config or --etcd-endpoint")]
    NoClusterGiven,
    /// The configuration file cannot be read.
    #[error("cannot read the configuration file {}: {reason}", path.display())]
    UnreadableFile {
        /// The file that was named.
        path: PathBuf,
        /// What the operating system reported.
        reason: String,
    },
    /// The configuration file is not a cluster configuration.
    #[error("the configuration file {} is not valid: {reason}", path.display())]
    InvalidFile {
        /// The file that was named.
        path: PathBuf,
        /// What is wrong with its content.
        reason: String,
    },
}

/// Works out which cluster a tool is to use.
///
/// Reads the named file, if there is one, and lets the options replace what it says. Nothing is connected.
///
/// # Arguments
///
/// * `config_path` - The configuration file the tool was given, if any.
/// * `etcd_endpoints` - The endpoints the tool was given as options. If there are any, they replace the file's.
/// * `etcd_key_prefix` - The key prefix the tool was given as an option, which replaces the file's.
///
/// # Returns
///
/// The cluster configuration, with at least one etcd endpoint. Its object store is the file's, or none without a
/// file.
///
/// # Errors
///
/// * [`ClusterConfigError::UnreadableFile`] if the named file cannot be read.
/// * [`ClusterConfigError::InvalidFile`] if it is not TOML, misses a required value or has an unknown one.
/// * [`ClusterConfigError::NoClusterGiven`] if no etcd endpoint is known in the end.
///
/// # Examples
///
/// ```
/// use bitacora::{ClusterConfigError, ObjectStoreConfig, resolve_cluster_config};
///
/// let endpoints = ["http://10.0.0.1:2379".to_owned()];
///
/// let from_options = resolve_cluster_config(None, &endpoints, None)?;
/// let from_nothing = resolve_cluster_config(None, &[], None);
///
/// assert_eq!(from_options.etcd_endpoints, endpoints);
/// assert_eq!(from_options.object_store, ObjectStoreConfig::None);
/// assert_eq!(from_nothing, Err(ClusterConfigError::NoClusterGiven));
/// # Ok::<(), ClusterConfigError>(())
/// ```
pub fn resolve_cluster_config(
    config_path: Option<&Path>,
    etcd_endpoints: &[String],
    etcd_key_prefix: Option<&str>,
) -> Result<ClusterConfig, ClusterConfigError> {
    let mut config = match config_path {
        Some(path) => read_config_file(path)?,
        None => ClusterConfig {
            etcd_endpoints: Vec::new(),
            etcd_key_prefix: DEFAULT_ETCD_KEY_PREFIX.to_owned(),
            object_store: ObjectStoreConfig::None,
        },
    };
    if !etcd_endpoints.is_empty() {
        config.etcd_endpoints = etcd_endpoints.to_vec();
    }
    if let Some(key_prefix) = etcd_key_prefix {
        key_prefix.clone_into(&mut config.etcd_key_prefix);
    }
    if config.etcd_endpoints.is_empty() {
        return Err(ClusterConfigError::NoClusterGiven);
    }
    Ok(config)
}

/// Reads and parses the configuration file at `path`.
fn read_config_file(path: &Path) -> Result<ClusterConfig, ClusterConfigError> {
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(io_error) => {
            return Err(ClusterConfigError::UnreadableFile {
                path: path.to_owned(),
                reason: io_error.to_string(),
            });
        }
    };
    match toml::from_str::<ClusterConfig>(&content) {
        Ok(config) => Ok(config),
        Err(toml_error) => Err(ClusterConfigError::InvalidFile {
            path: path.to_owned(),
            reason: toml_error.to_string(),
        }),
    }
}

#[cfg(test)]
#[path = "config_file_test.rs"]
mod tests;
