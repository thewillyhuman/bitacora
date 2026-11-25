//! Finding the cluster and holding the ways to reach it.
//!
//! # Why this file exists
//!
//! A command needs the cluster's metadata store, a way to reach replicas, a clock and, for some commands,
//! the object store. Where those come from is the same question for every command: a configuration file,
//! options or environment variables. Answering it here keeps the commands free of it, and lets the tests
//! hand the same commands an in-process cluster instead.
//!
//! # What it contains
//!
//! - [`resolve_cluster_config`]: from options, environment and an optional file to a cluster
//!   configuration.
//! - [`Cluster`]: the interfaces every command works through.
//! - [`ClusterConfigError`]: why no usable configuration was found.
//!
//! # How it works
//!
//! The configuration file is TOML holding a `bitacora::ClusterConfig`. It is named by `--config` or, if
//! that is absent, by `BITA_CONFIG`. The etcd endpoints and the key prefix can also be given directly,
//! by options or by `BITA_ETCD_ENDPOINTS` and `BITA_ETCD_KEY_PREFIX`. For each value an option wins over
//! the environment, and both win over the file. An object store can only be named in the file.
//!
//! A configuration without any etcd endpoint is refused: the tool never guesses a cluster to operate on.
//!
//! [`Cluster::connect`] then opens the real interfaces through the `bitacora` crate, exactly as every
//! other process of a deployment does.
//!
//! # Invariants
//!
//! - Resolving reads the named file and nothing else; the environment is passed in, so the function can
//!   be tested without touching the process's environment.
//! - Connecting sends nothing: an etcd that is down shows up on the first request.
//!
//! # See also
//!
//! - `arguments.rs`: the options this file reads.
//! - `bitacora/src/cluster.rs`: the configuration structure and the real connections.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bitacora::{
    BitacoraClient, ClientSettings, ClusterConfig, ConnectError, DEFAULT_ETCD_KEY_PREFIX, ObjectStoreConfig,
    WriterSettings,
};
use bitacora_artifact::ArtifactStore;
use bitacora_meta::MetadataStore;
use bitacora_types::replica_api::ReplicaConnector;
use bitacora_types::{Clock, SystemClock};
use thiserror::Error;

use crate::arguments::ClusterArguments;

/// The environment variable that names the configuration file.
pub(crate) const CONFIG_VARIABLE: &str = "BITA_CONFIG";
/// The environment variable that lists the etcd endpoints, separated by commas.
pub(crate) const ETCD_ENDPOINTS_VARIABLE: &str = "BITA_ETCD_ENDPOINTS";
/// The environment variable that holds the etcd key prefix.
pub(crate) const ETCD_KEY_PREFIX_VARIABLE: &str = "BITA_ETCD_KEY_PREFIX";

/// Why no usable cluster configuration was found.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub(crate) enum ClusterConfigError {
    /// Neither a file nor an endpoint was given.
    #[error(
        "no cluster given: use --config or --etcd-endpoint, or set {CONFIG_VARIABLE} or \
         {ETCD_ENDPOINTS_VARIABLE}"
    )]
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

/// Works out which cluster the tool is to operate on.
///
/// See the file header for the order in which the sources are consulted.
///
/// # Arguments
///
/// * `arguments` - The cluster options of the command line.
/// * `environment` - The environment variables of the process, by name.
///
/// # Returns
///
/// The cluster configuration, with at least one etcd endpoint.
///
/// # Errors
///
/// * [`ClusterConfigError::UnreadableFile`] if the named file cannot be read.
/// * [`ClusterConfigError::InvalidFile`] if it is not TOML, misses a required value or has an unknown one.
/// * [`ClusterConfigError::NoClusterGiven`] if, after all sources, no etcd endpoint is known.
pub(crate) fn resolve_cluster_config(
    arguments: &ClusterArguments,
    environment: &BTreeMap<String, String>,
) -> Result<ClusterConfig, ClusterConfigError> {
    let config_path = match &arguments.config {
        Some(path) => Some(path.clone()),
        None => environment.get(CONFIG_VARIABLE).map(PathBuf::from),
    };
    let mut config = match config_path {
        Some(path) => read_config_file(path)?,
        None => ClusterConfig {
            etcd_endpoints: Vec::new(),
            etcd_key_prefix: DEFAULT_ETCD_KEY_PREFIX.to_owned(),
            object_store: ObjectStoreConfig::None,
        },
    };

    if !arguments.etcd_endpoints.is_empty() {
        config.etcd_endpoints.clone_from(&arguments.etcd_endpoints);
    } else if let Some(listed_endpoints) = environment.get(ETCD_ENDPOINTS_VARIABLE) {
        config.etcd_endpoints = split_endpoints(listed_endpoints);
    }
    if let Some(key_prefix) = &arguments.etcd_key_prefix {
        config.etcd_key_prefix.clone_from(key_prefix);
    } else if let Some(key_prefix) = environment.get(ETCD_KEY_PREFIX_VARIABLE) {
        config.etcd_key_prefix.clone_from(key_prefix);
    }

    if config.etcd_endpoints.is_empty() {
        return Err(ClusterConfigError::NoClusterGiven);
    }
    Ok(config)
}

/// Reads and parses the configuration file at `path`.
fn read_config_file(path: PathBuf) -> Result<ClusterConfig, ClusterConfigError> {
    let content = match std::fs::read_to_string(&path) {
        Ok(content) => content,
        Err(io_error) => {
            return Err(ClusterConfigError::UnreadableFile {
                path,
                reason: io_error.to_string(),
            });
        }
    };
    match toml::from_str::<ClusterConfig>(&content) {
        Ok(config) => Ok(config),
        Err(toml_error) => Err(ClusterConfigError::InvalidFile {
            path,
            reason: toml_error.to_string(),
        }),
    }
}

/// Splits a comma-separated list of endpoints, ignoring blanks around and between them.
fn split_endpoints(listed_endpoints: &str) -> Vec<String> {
    let mut endpoints = Vec::new();
    for endpoint in listed_endpoints.split(',') {
        let endpoint = endpoint.trim();
        if !endpoint.is_empty() {
            endpoints.push(endpoint.to_owned());
        }
    }
    endpoints
}

/// The interfaces every command works through.
///
/// A real run gets them from [`Cluster::connect`]; a test builds the structure from an in-process
/// cluster. Commands never know which.
#[derive(Clone)]
pub(crate) struct Cluster {
    /// The cluster's metadata store.
    pub(crate) metadata: MetadataStore,
    /// How replicas are reached at the addresses they registered.
    pub(crate) connector: Arc<dyn ReplicaConnector>,
    /// The clock that leases and heartbeats are compared with.
    pub(crate) clock: Arc<dyn Clock>,
    /// The object store sealed segments are tiered to, if the cluster has one.
    pub(crate) artifact_store: Option<Arc<dyn ArtifactStore>>,
    /// How long one request to a replica may take before the command gives up on it.
    pub(crate) request_timeout: Duration,
}

impl Cluster {
    /// Opens the real interfaces of the cluster that `config` describes.
    ///
    /// # Arguments
    ///
    /// * `config` - Where the cluster is.
    /// * `request_timeout` - How long one request to a replica may take.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError`] if the configuration can never work or the object store cannot be opened.
    pub(crate) async fn connect(config: &ClusterConfig, request_timeout: Duration) -> Result<Self, ConnectError> {
        Ok(Self {
            metadata: bitacora::connect_metadata(config).await?,
            connector: bitacora::replica_connector(),
            clock: Arc::new(SystemClock),
            artifact_store: bitacora::open_object_store(&config.object_store).await?,
            request_timeout,
        })
    }

    /// Builds a client over the same interfaces, for the commands that write or read records.
    ///
    /// # Arguments
    ///
    /// * `writer_identity` - The name a writer opened by this client records with its lease, so that an
    ///   operator can tell who holds a stream.
    pub(crate) fn client(&self, writer_identity: &str) -> BitacoraClient {
        let settings = ClientSettings {
            writer: WriterSettings {
                writer_identity: writer_identity.to_owned(),
                ..WriterSettings::default()
            },
            ..ClientSettings::default()
        };
        let client = BitacoraClient::new(
            self.metadata.clone(),
            Arc::clone(&self.connector),
            Arc::clone(&self.clock),
            settings,
        );
        match &self.artifact_store {
            Some(artifact_store) => client.with_artifact_store(Arc::clone(artifact_store)),
            None => client,
        }
    }
}

#[cfg(test)]
#[path = "cluster_test.rs"]
mod tests;
