//! The S3 backend of the bitacora artifact store.
//!
//! # Why this crate exists
//!
//! Tiered log segments and the files materializers write live in an object store (design, section 11,
//! "Artifact store"). `bitacora-artifact` defines the small `bitacora_artifact::ArtifactStore` interface,
//! with an in-memory backend for the simulator and a directory backend for development. This crate is the
//! backend a real cluster runs on: any S3-compatible service, through the `object_store` crate.
//!
//! It is a crate of its own, and not a module of `bitacora-artifact`, so that the simulator, the test kit
//! and every other user of the in-memory store do not have to compile an HTTP client and a TLS stack
//! (decision record `docs/decisions/0005-s3-backend.md`).
//!
//! # What it contains
//!
//! - [`S3ArtifactStore`]: the store. Hand it to whatever takes an `Arc<dyn ArtifactStore>`.
//! - [`S3Settings`]: which bucket, where the service is and how long to wait for it.
//! - [`S3Credentials`]: a key pair, for the rare program that does not take its credentials from the
//!   environment.
//! - [`InvalidS3Settings`]: why a set of settings was refused.
//!
//! # How it works
//!
//! A binary builds [`S3Settings`] from its configuration, calls [`S3ArtifactStore::open`] once at start
//! and shares the store for the life of the process. Each method of the interface is one operation with a
//! deadline; `store.rs` explains how each one maps to S3, and `s3.rs` what is retried and where the
//! credentials come from.
//!
//! # Invariants
//!
//! - The crate depends on `bitacora-artifact` and never the other way round.
//! - It behaves like every other artifact store: it passes `bitacora_artifact::contract::check`, against
//!   an in-memory service and against a real S3 server.
//! - No secret is ever part of a printable value.
//!
//! # See also
//!
//! - `settings.rs`: the settings and their defaults.
//! - `s3.rs`: opening the store on an S3 service.
//! - `store.rs`: the store and how each call maps to the service.
//! - `tests/s3/`: the tests that run against a real S3 server.

mod s3;
mod settings;
mod store;

pub use settings::{InvalidS3Settings, S3Credentials, S3Settings};
pub use store::S3ArtifactStore;
