//! Connecting to a real bitacora cluster.
//!
//! # Why this file exists
//!
//! The client library takes its metadata store, its way to reach replicas, its clock and its object store
//! as injected interfaces, which is what lets it run inside the simulator. An application, a daemon or a
//! command-line tool does not want to assemble those by hand: it has a configuration that says where etcd
//! is and where the object store is, and wants a client. This file turns the one into the other, in one
//! place, so that every process of a deployment reads the same configuration the same way.
//!
//! # What it contains
//!
//! - [`ClusterConfig`]: where the cluster's metadata store and object store are.
//! - [`ObjectStoreConfig`]: which object store sealed segments are tiered to, if any.
//! - [`connect`]: builds a [`BitacoraClient`] for the cluster.
//! - [`connect_metadata`], [`open_object_store`], [`replica_connector`]: the three parts, for processes
//!   that are not clients (replicas, the controller, operator tools).
//! - [`ConnectError`]: why a configuration cannot be used.
//!
//! # How it works
//!
//! A cluster is found through its metadata store: etcd holds the stream records and the registry of
//! replicas with their addresses. So the configuration names the etcd members and nothing else about the
//! cluster's shape. Replicas are reached over gRPC at the addresses they registered, and time comes from
//! the system clock.
//!
//! Connecting sends nothing. A wrong or unreachable endpoint shows up on the first request, as an
//! unavailable metadata store; only a configuration that can never work is refused here.
//!
//! [`ClusterConfig`] implements `serde::Deserialize`, so a process embeds it in its own configuration
//! file. Unknown fields are refused: a misspelled setting must not be silently ignored.
//!
//! # Invariants
//!
//! - Every process of one deployment must be given the same etcd key prefix and the same object store:
//!   the controller refuses to record an upload it cannot see itself, but nothing can detect two clusters
//!   that share a prefix by mistake.
//!
//! # See also
//!
//! - `bitacora-meta-etcd`: the etcd backend of the metadata store.
//! - `bitacora-proto`: the gRPC transport to replicas.
//! - `bitacora-artifact`: the object store interface and its local backend.

use std::path::PathBuf;
use std::sync::Arc;

use bitacora_artifact::{ArtifactError, ArtifactStore, LocalArtifactStore};
use bitacora_client::{BitacoraClient, ClientSettings};
use bitacora_meta::MetadataStore;
use bitacora_meta_etcd::{EtcdKeyValueStore, EtcdSettings, InvalidEtcdSettings};
use bitacora_proto::{GrpcConnectorSettings, GrpcReplicaConnector};
use bitacora_types::SystemClock;
use bitacora_types::replica_api::ReplicaConnector;
use serde::Deserialize;
use thiserror::Error;

/// The etcd key prefix used when the configuration names none.
pub const DEFAULT_ETCD_KEY_PREFIX: &str = "/bitacora/";

/// Where a bitacora cluster is.
///
/// # Examples
///
/// As it appears in a TOML configuration file:
///
/// ```toml
/// etcd_endpoints = ["http://10.0.0.1:2379", "http://10.0.0.2:2379", "http://10.0.0.3:2379"]
///
/// [object_store]
/// kind = "local_directory"
/// path = "/var/lib/bitacora/objects"
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    /// The client URLs of the etcd members that hold the cluster's metadata, for example
    /// `http://10.0.0.1:2379`. At least one is required; list all of them, so that the loss of one member
    /// is survived.
    pub etcd_endpoints: Vec<String>,
    /// The text in front of every key of this cluster in etcd. Two clusters share one etcd by using two
    /// prefixes. Defaults to [`DEFAULT_ETCD_KEY_PREFIX`].
    #[serde(default = "default_etcd_key_prefix")]
    pub etcd_key_prefix: String,
    /// The object store sealed segments are tiered to. Defaults to none, which means no tiering.
    #[serde(default)]
    pub object_store: ObjectStoreConfig,
}

/// The default of [`ClusterConfig::etcd_key_prefix`], as a function because that is what serde calls.
fn default_etcd_key_prefix() -> String {
    DEFAULT_ETCD_KEY_PREFIX.to_owned()
}

/// Which object store a cluster tiers sealed segments to.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ObjectStoreConfig {
    /// No object store: segments stay on the replicas until they are truncated.
    #[default]
    None,
    /// A directory of the local file system. Every process must see the same directory, so this is for
    /// a cluster on one machine or on a shared file system: development, not production.
    LocalDirectory {
        /// The directory that holds the objects. It is created if it does not exist.
        path: PathBuf,
    },
}

/// Why a cluster configuration cannot be used.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ConnectError {
    /// The etcd settings can never work, for example because no endpoint is listed.
    #[error("cannot use the metadata store configuration: {0}")]
    InvalidMetadataStore(#[from] InvalidEtcdSettings),
    /// The object store could not be opened.
    #[error("cannot open the object store: {0}")]
    ObjectStore(#[from] ArtifactError),
}

/// Builds a client for the cluster that `config` describes.
///
/// The client reaches the metadata store in etcd, replicas over gRPC, and the object store that the
/// configuration names, if any. Nothing is sent yet: see the file header.
///
/// # Arguments
///
/// * `config` - Where the cluster is.
/// * `settings` - How the writers and readers opened by the client behave.
///
/// # Returns
///
/// A client to open writers, readers and the stream administration from.
///
/// # Errors
///
/// * [`ConnectError::InvalidMetadataStore`] if the etcd settings can never work.
/// * [`ConnectError::ObjectStore`] if the object store cannot be opened.
///
/// # Examples
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use bitacora::{ClientSettings, ClusterConfig, ObjectStoreConfig, TakeoverPolicy};
///
/// let config = ClusterConfig {
///     etcd_endpoints: vec!["http://127.0.0.1:2379".to_owned()],
///     etcd_key_prefix: "/bitacora/".to_owned(),
///     object_store: ObjectStoreConfig::None,
/// };
/// let cluster = bitacora::connect(&config, ClientSettings::default()).await?;
/// let writer = cluster
///     .open_writer("orders-database", TakeoverPolicy::WaitForLeaseExpiry)
///     .await?;
/// let commit_sequence = writer.append("first record".into()).await?;
/// println!("committed at {commit_sequence}");
/// # Ok(())
/// # }
/// ```
pub async fn connect(
    config: &ClusterConfig,
    settings: ClientSettings,
) -> Result<BitacoraClient, ConnectError> {
    let metadata = connect_metadata(config).await?;
    let client = BitacoraClient::new(
        metadata,
        replica_connector(),
        Arc::new(SystemClock),
        settings,
    );
    match open_object_store(&config.object_store).await? {
        Some(object_store) => Ok(client.with_artifact_store(object_store)),
        None => Ok(client),
    }
}

/// Opens the cluster's metadata store in etcd.
///
/// # Arguments
///
/// * `config` - Where the cluster is. Only the etcd fields are used.
///
/// # Errors
///
/// Returns [`ConnectError::InvalidMetadataStore`] if the etcd settings can never work. An etcd that is
/// down is not an error here; requests to the store fail until it is back.
pub async fn connect_metadata(config: &ClusterConfig) -> Result<MetadataStore, ConnectError> {
    let mut etcd_settings = EtcdSettings::new(config.etcd_endpoints.clone());
    etcd_settings.key_prefix.clone_from(&config.etcd_key_prefix);
    let key_value_store = EtcdKeyValueStore::connect(etcd_settings).await?;
    Ok(MetadataStore::new(Arc::new(key_value_store)))
}

/// Opens the object store that `config` names.
///
/// # Returns
///
/// The store, or `None` if the cluster has no object store.
///
/// # Errors
///
/// Returns [`ConnectError::ObjectStore`] if the store cannot be opened, for example because its directory
/// cannot be created.
pub async fn open_object_store(
    config: &ObjectStoreConfig,
) -> Result<Option<Arc<dyn ArtifactStore>>, ConnectError> {
    match config {
        ObjectStoreConfig::None => Ok(None),
        ObjectStoreConfig::LocalDirectory { path } => {
            let local_store = LocalArtifactStore::open(path.clone()).await?;
            Ok(Some(Arc::new(local_store)))
        }
    }
}

/// The way every process of a deployment reaches replicas: gRPC, at the addresses the replicas registered
/// in the metadata store, with one lazily opened connection per replica.
#[must_use]
pub fn replica_connector() -> Arc<dyn ReplicaConnector> {
    Arc::new(GrpcReplicaConnector::new(GrpcConnectorSettings::default()))
}

#[cfg(test)]
#[path = "cluster_test.rs"]
mod tests;
