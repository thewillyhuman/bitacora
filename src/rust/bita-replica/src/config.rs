//! The configuration of the replica daemon.
//!
//! # Why this file exists
//!
//! A replica is configured by one file (design, section 14). Two kinds of mistake can be in it. A
//! misspelled or mistyped setting is caught when the file is read. A setting that is well-formed but
//! cannot work (an advertised address without a port, a threshold above 100) is not, and a replica that
//! starts with one fails later and in a confusing place: it registers an address nobody can reach, or
//! never accepts a segment. This file catches the second kind at start.
//!
//! # What it contains
//!
//! - [`ReplicaConfigFile`]: the file as an operator writes it. `example.toml` shows every setting.
//! - [`StorageConfig`]: its `[storage]` table.
//! - [`ReplicaDaemonSettings`]: the same settings, checked and in the types the daemon works with.
//! - [`InvalidConfig`]: which setting cannot work and why.
//!
//! # How it works
//!
//! `bitacora_process::load_config_file` reads the file into a [`ReplicaConfigFile`]; unknown fields are
//! refused there. [`ReplicaConfigFile::validate`] then checks every setting and builds the
//! [`ReplicaDaemonSettings`], or names the first setting that is wrong. Nothing else in the daemon reads
//! the raw file, so nothing can use a setting that was not checked.
//!
//! Quantities carry their unit in the setting's name (`_seconds`, `_bytes`, `_percent`), as the metrics
//! do.
//!
//! # Invariants
//!
//! - A [`ReplicaDaemonSettings`] exists only for a configuration that passed every check.
//! - Validation reads no file and opens no socket: whether an address is free or a directory writable
//!   is found out when the daemon uses it.
//!
//! # See also
//!
//! - `example.toml`: a complete configuration, which a test keeps valid.
//! - `daemon.rs`: what the settings are used for.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use bitacora::ClusterConfig;
use bitacora_process::LogSettings;
use bitacora_replica::ReplicaSettings;
use bitacora_types::ReplicaId;
use serde::Deserialize;
use thiserror::Error;

/// The smallest segment preallocation accepted: one page. Anything smaller would make a segment file
/// grow on nearly every append.
const SMALLEST_PREALLOCATION_BYTES: u64 = 4096;

/// A share of the disk cannot exceed the whole disk.
const WHOLE_DISK_PERCENT: u8 = 100;

/// The configuration file of a replica, as written. See `example.toml`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplicaConfigFile {
    /// The identity a new, empty data directory gets.
    pub replica_id: String,
    /// Where segment files live.
    pub data_directory: PathBuf,
    /// Where the replica accepts gRPC connections, as `ip:port`.
    pub listen_address: String,
    /// The address the replica registers under, as `host:port`.
    pub advertised_address: String,
    /// Where the replica is, widest failure domain first. Defaults to none.
    #[serde(default)]
    pub failure_domains: Vec<String>,
    /// Where `/metrics`, `/healthz` and `/readyz` are served, as `ip:port`.
    pub metrics_listen_address: String,
    /// How often the registration is renewed. Defaults to five seconds.
    #[serde(default = "default_registration_interval_seconds")]
    pub registration_interval_seconds: u64,
    /// How often sealed segments are scrubbed. Defaults to one week.
    #[serde(default = "default_scrub_interval_seconds")]
    pub scrub_interval_seconds: u64,
    /// Where the cluster is.
    pub cluster: ClusterConfig,
    /// The log output. Defaults to JSON at `info`.
    #[serde(default)]
    pub log: LogSettings,
    /// How the replica uses its disk. Defaults to the replica's own defaults.
    #[serde(default)]
    pub storage: StorageConfig,
}

/// The `[storage]` table: the settings of the replica an operator tunes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StorageConfig {
    /// How much disk a new segment file reserves, and grows by when full.
    #[serde(default = "default_preallocated_segment_bytes")]
    pub preallocated_segment_bytes: u64,
    /// From this share of used disk on, the replica accepts no new segments.
    #[serde(default = "default_refuse_new_segments_at_used_percent")]
    pub refuse_new_segments_at_used_percent: u8,
    /// From this share of used disk on, the replica refuses every append.
    #[serde(default = "default_refuse_appends_at_used_percent")]
    pub refuse_appends_at_used_percent: u8,
    /// How many bytes per second one repair may copy. Zero means no limit.
    #[serde(default = "default_repair_bytes_per_second")]
    pub repair_bytes_per_second: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            preallocated_segment_bytes: default_preallocated_segment_bytes(),
            refuse_new_segments_at_used_percent: default_refuse_new_segments_at_used_percent(),
            refuse_appends_at_used_percent: default_refuse_appends_at_used_percent(),
            repair_bytes_per_second: default_repair_bytes_per_second(),
        }
    }
}

/// The default of `registration_interval_seconds`: a third of the controller's default liveness window,
/// so that two renewals may be lost before a replica is presumed dead.
fn default_registration_interval_seconds() -> u64 {
    5
}

/// The default of `scrub_interval_seconds`: one week (design, section 7, "Integrity").
fn default_scrub_interval_seconds() -> u64 {
    7 * 24 * 60 * 60
}

/// The default of `preallocated_segment_bytes`: the replica's own.
fn default_preallocated_segment_bytes() -> u64 {
    ReplicaSettings::default().preallocated_segment_bytes
}

/// The default of `refuse_new_segments_at_used_percent`: the replica's own.
fn default_refuse_new_segments_at_used_percent() -> u8 {
    ReplicaSettings::default().refuse_new_segments_at_used_percent
}

/// The default of `refuse_appends_at_used_percent`: the replica's own.
fn default_refuse_appends_at_used_percent() -> u8 {
    ReplicaSettings::default().refuse_appends_at_used_percent
}

/// The default of `repair_bytes_per_second`: the replica's own.
fn default_repair_bytes_per_second() -> u64 {
    ReplicaSettings::default().repair_bytes_per_second
}

/// A setting that is well-formed but cannot work.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("setting `{setting}` is invalid: {reason}")]
pub(crate) struct InvalidConfig {
    /// The setting, as it is written in the file, for example `storage.refuse_appends_at_used_percent`.
    pub setting: &'static str,
    /// What is wrong with its value, for the operator who has to fix it.
    pub reason: String,
}

impl InvalidConfig {
    /// Builds the error for `setting` from the reason its value was refused.
    fn because(setting: &'static str, reason: impl Into<String>) -> Self {
        Self {
            setting,
            reason: reason.into(),
        }
    }
}

/// The settings of a replica daemon, checked and ready to use.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReplicaDaemonSettings {
    /// The identity a new, empty data directory gets.
    pub proposed_replica_id: ReplicaId,
    /// Where segment files live.
    pub data_directory: PathBuf,
    /// Where the replica accepts gRPC connections.
    pub listen_address: SocketAddr,
    /// The `host:port` the replica registers under.
    pub advertised_address: String,
    /// Where the replica is, widest failure domain first.
    pub failure_domains: Vec<String>,
    /// Where the observability endpoint is served.
    pub metrics_listen_address: SocketAddr,
    /// How long the replica waits between two renewals of its registration.
    pub registration_interval: Duration,
    /// How long the replica waits between two scrubs of its sealed segments.
    pub scrub_interval: Duration,
    /// Where the cluster is.
    pub cluster: ClusterConfig,
    /// The log output.
    pub log: LogSettings,
    /// How the replica uses its disk.
    pub replica: ReplicaSettings,
}

impl ReplicaConfigFile {
    /// Checks every setting and converts the file into the settings the daemon works with.
    ///
    /// # Returns
    ///
    /// The settings, if every one of them can work.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidConfig`] naming the first setting that cannot work:
    ///
    /// * `replica_id` that is not a valid replica identifier;
    /// * `data_directory` that is empty;
    /// * `listen_address` or `metrics_listen_address` that is not `ip:port`, or both the same;
    /// * `advertised_address` that is not `host:port` with a port above zero;
    /// * a failure domain that is empty or contains whitespace;
    /// * an interval of zero seconds;
    /// * a preallocation below one page, a threshold above 100 percent, or an append threshold below
    ///   the threshold for new segments.
    pub(crate) fn validate(self) -> Result<ReplicaDaemonSettings, InvalidConfig> {
        let proposed_replica_id = parse_replica_id(self.replica_id)?;
        check_data_directory(&self.data_directory)?;
        let listen_address = parse_listen_address("listen_address", &self.listen_address)?;
        let metrics_listen_address = parse_listen_address("metrics_listen_address", &self.metrics_listen_address)?;
        check_listen_addresses_differ(listen_address, metrics_listen_address)?;
        check_advertised_address(&self.advertised_address)?;
        for failure_domain in &self.failure_domains {
            check_failure_domain(failure_domain)?;
        }
        let registration_interval = whole_seconds("registration_interval_seconds", self.registration_interval_seconds)?;
        let scrub_interval = whole_seconds("scrub_interval_seconds", self.scrub_interval_seconds)?;
        let replica = self.storage.validate()?;
        Ok(ReplicaDaemonSettings {
            proposed_replica_id,
            data_directory: self.data_directory,
            listen_address,
            advertised_address: self.advertised_address,
            failure_domains: self.failure_domains,
            metrics_listen_address,
            registration_interval,
            scrub_interval,
            cluster: self.cluster,
            log: self.log,
            replica,
        })
    }
}

impl StorageConfig {
    /// Checks the `[storage]` table and lays it over the replica's default settings.
    fn validate(self) -> Result<ReplicaSettings, InvalidConfig> {
        let new_segments_percent = self.refuse_new_segments_at_used_percent;
        let appends_percent = self.refuse_appends_at_used_percent;
        check_preallocation(self.preallocated_segment_bytes)?;
        check_percent("storage.refuse_new_segments_at_used_percent", new_segments_percent)?;
        check_percent("storage.refuse_appends_at_used_percent", appends_percent)?;
        check_thresholds_are_in_order(new_segments_percent, appends_percent)?;
        Ok(ReplicaSettings {
            preallocated_segment_bytes: self.preallocated_segment_bytes,
            refuse_new_segments_at_used_percent: self.refuse_new_segments_at_used_percent,
            refuse_appends_at_used_percent: self.refuse_appends_at_used_percent,
            repair_bytes_per_second: self.repair_bytes_per_second,
            ..ReplicaSettings::default()
        })
    }
}

/// Parses the identity the configuration proposes for an empty data directory.
fn parse_replica_id(replica_id: String) -> Result<ReplicaId, InvalidConfig> {
    match ReplicaId::new(replica_id) {
        Ok(replica_id) => Ok(replica_id),
        Err(invalid) => Err(InvalidConfig::because("replica_id", invalid.to_string())),
    }
}

/// Checks that a data directory is named. Whether it exists and can be written is found out when the
/// daemon opens it.
fn check_data_directory(data_directory: &Path) -> Result<(), InvalidConfig> {
    if data_directory.as_os_str().is_empty() {
        return Err(InvalidConfig::because(
            "data_directory",
            "it is empty; name the directory that holds the segment files",
        ));
    }
    Ok(())
}

/// Checks that gRPC and metrics are not served on the same address: each needs a port of its own.
fn check_listen_addresses_differ(
    listen_address: SocketAddr,
    metrics_listen_address: SocketAddr,
) -> Result<(), InvalidConfig> {
    if listen_address == metrics_listen_address {
        return Err(InvalidConfig::because(
            "metrics_listen_address",
            "it is the same as listen_address; gRPC and metrics need a port each",
        ));
    }
    Ok(())
}

/// Checks that segment files are preallocated at least one page at a time.
fn check_preallocation(preallocated_segment_bytes: u64) -> Result<(), InvalidConfig> {
    if preallocated_segment_bytes < SMALLEST_PREALLOCATION_BYTES {
        return Err(InvalidConfig::because(
            "storage.preallocated_segment_bytes",
            format!(
                "{preallocated_segment_bytes} is less than {SMALLEST_PREALLOCATION_BYTES}, the smallest that is \
                 accepted"
            ),
        ));
    }
    Ok(())
}

/// Checks that the replica stops taking new segments no later than it stops taking appends, so that a
/// disk that fills up first stops growing in segments and only then refuses the writers it has.
fn check_thresholds_are_in_order(new_segments_percent: u8, appends_percent: u8) -> Result<(), InvalidConfig> {
    if appends_percent < new_segments_percent {
        return Err(InvalidConfig::because(
            "storage.refuse_appends_at_used_percent",
            format!(
                "{appends_percent} is below refuse_new_segments_at_used_percent ({new_segments_percent}); a replica \
                 must stop taking new segments before it stops taking appends"
            ),
        ));
    }
    Ok(())
}

/// Parses an address to listen on, which must be an IP address and a port.
fn parse_listen_address(setting: &'static str, address: &str) -> Result<SocketAddr, InvalidConfig> {
    match address.parse::<SocketAddr>() {
        Ok(socket_address) => Ok(socket_address),
        Err(_not_an_address) => Err(InvalidConfig::because(
            setting,
            format!("{address:?} is not `ip:port`, for example `0.0.0.0:7000`"),
        )),
    }
}

/// Checks that the advertised address is `host:port`, which is what the transport connects to.
fn check_advertised_address(address: &str) -> Result<(), InvalidConfig> {
    let refuse = |reason: &str| {
        Err(InvalidConfig::because(
            "advertised_address",
            format!("{address:?} {reason}; write `host:port`, for example `replica-a.example.org:7000`"),
        ))
    };
    let Some((host, port_text)) = address.rsplit_once(':') else {
        return refuse("has no port");
    };
    if host.is_empty() {
        return refuse("has no host");
    }
    if host.contains("//") || host.chars().any(char::is_whitespace) {
        return refuse("is not a bare host name or IP address");
    }
    match port_text.parse::<u16>() {
        Ok(0) => refuse("has port 0, which nobody can connect to"),
        Ok(_port) => Ok(()),
        Err(_not_a_port) => refuse("has a port that is not a number between 1 and 65535"),
    }
}

/// Checks one failure-domain label: placement compares labels as text, so an empty one or one with
/// stray whitespace would silently never match.
fn check_failure_domain(failure_domain: &str) -> Result<(), InvalidConfig> {
    if failure_domain.is_empty() {
        return Err(InvalidConfig::because("failure_domains", "a label is empty"));
    }
    if failure_domain.chars().any(char::is_whitespace) {
        return Err(InvalidConfig::because(
            "failure_domains",
            format!("label {failure_domain:?} contains whitespace"),
        ));
    }
    Ok(())
}

/// Converts an interval in seconds, which must be at least one.
fn whole_seconds(setting: &'static str, seconds: u64) -> Result<Duration, InvalidConfig> {
    if seconds == 0 {
        return Err(InvalidConfig::because(
            setting,
            "it is 0; an interval is at least one second",
        ));
    }
    Ok(Duration::from_secs(seconds))
}

/// Checks that a share of the disk is between 1 and 100 percent.
fn check_percent(setting: &'static str, percent: u8) -> Result<(), InvalidConfig> {
    if percent == 0 || percent > WHOLE_DISK_PERCENT {
        return Err(InvalidConfig::because(
            setting,
            format!("{percent} is not a share between 1 and {WHOLE_DISK_PERCENT} percent"),
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "config_test.rs"]
mod tests;
