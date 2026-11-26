//! The bitacora client for applications.
//!
//! # Why this crate exists
//!
//! `bitacora-client` holds the write and read protocol, and takes everything it needs from the outside
//! world as injected interfaces, so that the same code runs in the deterministic simulator. That makes it
//! the wrong thing to hand to an application, which would have to know about etcd backends and gRPC
//! connectors to use it. This crate is what an application, a daemon or a tool depends on instead: the
//! client's API, plus one function that connects it to a real cluster.
//!
//! # What it contains
//!
//! - [`connect`] and [`ClusterConfig`]: from a configuration to a [`BitacoraClient`].
//! - The client's API, re-exported: [`BitacoraClient`], [`StreamWriter`], [`StreamReader`],
//!   [`StreamAdmin`], their settings and errors, and the value types they use.
//! - [`connect_metadata`], [`connect_key_value_store`], [`open_object_store`], [`replica_connector`]:
//!   the same wiring in parts, for processes that are not clients.
//!
//! # How it works
//!
//! See `cluster.rs`. In short: the cluster is found through etcd, replicas are reached over gRPC at the
//! addresses they registered, and tiered segments are read from the configured object store.
//!
//! The design writes `BitacoraClient::connect(cluster_config)`. Here it is the free function
//! [`connect`], because the client type lives in a crate that must not depend on etcd or gRPC.
//!
//! # Invariants
//!
//! - This crate adds no behaviour to the protocol. Everything it does is choosing implementations.
//! - It is the only library crate that depends on both a metadata backend and a transport.
//!
//! # See also
//!
//! - `cluster.rs`: the configuration and the connecting.
//! - `bitacora-client`: the client library itself.

#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#![warn(rustdoc::broken_intra_doc_links)]

mod cluster;
mod config_file;

pub use bitacora_client::{
    AppendError, BitacoraClient, ClientSettings, OpenError, ReadError, ReaderSettings, RecordStream, StreamAdmin,
    StreamDescription, StreamOptions, StreamReader, StreamWriter, TakeoverPolicy, WriterSettings,
};
pub use bitacora_types::{LogRecord, SequenceNumber, SequenceRange};
pub use cluster::{
    ClusterConfig, ConnectError, DEFAULT_ETCD_KEY_PREFIX, ObjectStoreConfig, connect, connect_key_value_store,
    connect_metadata, open_object_store, replica_connector,
};
pub use config_file::{ClusterConfigError, resolve_cluster_config};
