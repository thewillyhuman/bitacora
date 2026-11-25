//! The etcd backend of the bitacora metadata store.
//!
//! # Why this crate exists
//!
//! The metadata store is the one place where bitacora needs consensus
//! (design, section 8), and version 0 gets it from etcd. `bitacora-meta`
//! defines the small [`bitacora_meta::KeyValueStore`] interface and builds
//! everything bitacora-specific on top of it; this crate implements that
//! interface on an etcd cluster.
//!
//! It is a crate of its own, and not a module of `bitacora-meta`, so that
//! the simulator, the test kit and every other user of the in-memory store
//! do not have to compile a gRPC client (decision record
//! `docs/decisions/0003-etcd-backend-crate.md`).
//!
//! # What it contains
//!
//! - [`EtcdKeyValueStore`]: the store. Hand it to
//!   `bitacora_meta::MetadataStore::new`.
//! - [`EtcdSettings`]: where etcd is and how long to wait for it.
//! - [`InvalidEtcdSettings`]: why a set of settings was refused.
//!
//! # How it works
//!
//! A binary builds [`EtcdSettings`] from its configuration, calls
//! [`EtcdKeyValueStore::connect`] once at start and shares the store for
//! the life of the process. Each method of the interface is one etcd
//! request with a timeout, and every failure comes back as
//! `bitacora_meta::StoreUnavailable`; `store.rs` has the details.
//!
//! # Invariants
//!
//! - The crate depends on `bitacora-meta` and never the other way round.
//! - It holds no bitacora logic: records, epochs and leases belong to
//!   `bitacora-meta`, so both backends behave the same.
//!
//! # See also
//!
//! - `settings.rs`: the settings and their defaults.
//! - `store.rs`: the store and how each call maps to etcd.
//! - `tests/etcd/`: the tests, which run against a real etcd.

mod settings;
mod store;

pub use settings::{EtcdSettings, InvalidEtcdSettings};
pub use store::EtcdKeyValueStore;
