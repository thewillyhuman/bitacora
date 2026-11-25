//! Finding the cluster to benchmark.
//!
//! # Why this file exists
//!
//! The benchmark drives a real cluster, so it has to be told which one. It takes the same configuration
//! file as every other process of a deployment, or the etcd endpoints directly. This file reads the one
//! and applies the other.
//!
//! # What it contains
//!
//! - [`resolve_cluster_config`]: from the cluster options to a cluster configuration.
//! - [`ClusterConfigError`]: why no usable configuration was found.
//!
//! # How it works
//!
//! The configuration file, if one is named, is TOML holding a `bitacora::ClusterConfig`. Endpoints and a
//! key prefix given as options replace the file's. Unlike `bita`, the tool reads no environment
//! variables: the command line of a benchmark should say everything about the run.
//!
//! A configuration without any etcd endpoint is refused: the tool never guesses a cluster to load.
//!
//! # Invariants
//!
//! - Nothing but the named file is read, and nothing is connected.
//!
//! # See also
//!
//! - `bitacora/src/cluster.rs`: the configuration structure and the connecting.
//! - `bita/src/cluster.rs`: the same question answered for the operator's tool.

use std::path::{Path, PathBuf};

use bitacora::{ClusterConfig, DEFAULT_ETCD_KEY_PREFIX, ObjectStoreConfig};
use thiserror::Error;

/// Why no usable cluster configuration was found.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub(crate) enum ClusterConfigError {
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

/// Works out which cluster to benchmark.
///
/// # Arguments
///
/// * `config_path` - The configuration file named with `--config`, if any.
/// * `etcd_endpoints` - The endpoints given with `--etcd-endpoint`. If there are any, they replace the
///   file's.
/// * `etcd_key_prefix` - The prefix given with `--etcd-key-prefix`, which replaces the file's.
///
/// # Returns
///
/// The cluster configuration, with at least one etcd endpoint.
///
/// # Errors
///
/// * [`ClusterConfigError::UnreadableFile`] if the named file cannot be read.
/// * [`ClusterConfigError::InvalidFile`] if it is not TOML, misses a required value or has an unknown one.
/// * [`ClusterConfigError::NoClusterGiven`] if no etcd endpoint is known in the end.
pub(crate) fn resolve_cluster_config(
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
#[path = "cluster_test.rs"]
mod tests;
