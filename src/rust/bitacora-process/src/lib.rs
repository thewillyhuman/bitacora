//! What every bitacora daemon shares.
//!
//! # Why this crate exists
//!
//! A bitacora cluster is a few kinds of long-running process (design, section 14). Each of them has to
//! do the same unglamorous things before and around its real work: find and read its configuration file,
//! write logs, notice that it was asked to stop, and tell monitoring how it is doing. Written once per
//! daemon, those things drift apart, and an operator ends up with two log formats and two sets of metric
//! names. This crate writes them once.
//!
//! It is also where observability is attached to the rest of the system. The library crates are free of
//! a metrics library on purpose, so that they run unchanged in the deterministic simulator. They reach
//! the outside world through injected interfaces, and this crate provides a decorator for each of them
//! that measures what passes through.
//!
//! # What it contains
//!
//! - [`parse_command_line`], [`CommandLine`], [`usage`]: `--config <path>`.
//! - [`load_config_file`]: a TOML file into a configuration type.
//! - [`init_logging`], [`LogSettings`]: structured logs on standard output.
//! - [`ShutdownListener`]: SIGTERM and SIGINT.
//! - [`MetricsRegistry`], [`Counter`], [`Gauge`], [`DurationHistogram`]: the metrics of a process.
//! - [`undocumented_metrics`]: the check that keeps `docs/operations/metrics.md` true.
//! - [`InstrumentedDisk`], [`InstrumentedReplica`], [`InstrumentedKeyValueStore`],
//!   [`InstrumentedArtifactStore`]: the decorators.
//! - [`serve_observability`], [`Readiness`]: `/metrics`, `/healthz` and `/readyz`.
//! - [`http_get`]: a minimal client for that endpoint.
//!
//! # How it works
//!
//! A daemon's `main` is a short script over this crate:
//!
//! 1. [`parse_command_line`] finds the configuration file, [`load_config_file`] reads it.
//! 2. [`init_logging`] installs the log output.
//! 3. [`ShutdownListener::install`] takes over SIGTERM and SIGINT.
//! 4. The daemon creates a [`MetricsRegistry`], wraps its disk, metadata store and object store in the
//!    decorators, and builds its own component on top of them.
//! 5. [`serve_observability`] exposes the registry and a [`Readiness`] flag, which the daemon sets once
//!    it can do its work and clears when the shutdown signal arrives.
//!
//! Metric names follow one scheme, described in `metrics.rs` and listed in
//! `docs/operations/metrics.md`.
//!
//! # Invariants
//!
//! - No decorator changes, delays or retries a call: with or without them the system behaves the same.
//! - This crate is the only one that depends on the metrics, logging and HTTP server libraries.
//! - It holds no bitacora logic: nothing here knows what a segment or an epoch is.
//!
//! # See also
//!
//! - `docs/decisions/0006-daemons-and-observability.md`: why observability is built this way.
//! - `bita-replica` and `bita-controller`: the daemons that use this crate.

#![warn(clippy::missing_docs_in_private_items)]

mod command_line;
mod config_file;
mod http_endpoint;
mod instrumented_artifact_store;
mod instrumented_disk;
mod instrumented_key_value_store;
mod instrumented_replica;
mod logging;
mod metrics;
mod metrics_reference;
mod probe;
mod shutdown;

pub use command_line::{CommandLine, UsageError, parse_command_line, usage};
pub use config_file::{ConfigFileError, load_config_file};
pub use http_endpoint::{Readiness, serve_observability};
pub use instrumented_artifact_store::InstrumentedArtifactStore;
pub use instrumented_disk::InstrumentedDisk;
pub use instrumented_key_value_store::InstrumentedKeyValueStore;
pub use instrumented_replica::InstrumentedReplica;
pub use logging::{LogFormat, LogLevel, LogSettings, LoggingError, init_logging};
pub use metrics::{Counter, DurationHistogram, Gauge, MetricsRegistry};
pub use metrics_reference::undocumented_metrics;
pub use probe::{HttpAnswer, ProbeError, http_get};
pub use shutdown::{ShutdownListener, ShutdownSignal};
