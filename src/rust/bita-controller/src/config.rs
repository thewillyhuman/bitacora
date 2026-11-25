//! The configuration of the controller daemon.
//!
//! # Why this file exists
//!
//! A controller is configured by one file (design, section 14). A misspelled or mistyped setting is
//! caught when the file is read. A setting that is well-formed but cannot work is not: a leadership that
//! lapses between two passes makes two controllers take turns forever, and a controller that gives up a
//! replica before it even counts as dead re-replicates the cluster on every restart. This file catches
//! those at start.
//!
//! # What it contains
//!
//! - [`ControllerConfigFile`]: the file as an operator writes it. `example.toml` shows every setting.
//! - [`PlacementConfig`]: its `[placement]` table.
//! - [`ControllerDaemonSettings`]: the same settings, checked and in the types the daemon works with.
//! - [`InvalidConfig`]: which setting cannot work and why.
//!
//! # How it works
//!
//! `bitacora_process::load_config_file` reads the file into a [`ControllerConfigFile`]; unknown fields
//! are refused there. [`ControllerConfigFile::validate`] then checks every setting and builds the
//! [`ControllerDaemonSettings`], or names the first setting that is wrong.
//!
//! Only the settings an operator has a reason to change are in the file. The rest of
//! `bitacora_controller::ControllerSettings` (request and repair timeouts, how much one pass repairs
//! and uploads) keeps its default.
//!
//! Quantities carry their unit in the setting's name (`_seconds`, `_milliseconds`, `_percent`).
//!
//! # Invariants
//!
//! - A [`ControllerDaemonSettings`] exists only for a configuration that passed every check.
//! - The leadership lasts at least two pass intervals, so a leader that is merely resting keeps it.
//! - A replica is given up for lost only after it has counted as dead for a while.
//!
//! # See also
//!
//! - `example.toml`: a complete configuration, which a test keeps valid.
//! - `daemon.rs`: what the settings are used for.

use std::net::SocketAddr;
use std::time::Duration;

use bitacora::ClusterConfig;
use bitacora_controller::ControllerSettings;
use bitacora_meta::PlacementPolicy;
use bitacora_process::LogSettings;
use serde::Deserialize;
use thiserror::Error;

/// A share of the disk cannot exceed the whole disk.
const WHOLE_DISK_PERCENT: u8 = 100;

/// How many pass intervals the leadership must last at least: one to rest and one to spare.
const FEWEST_PASS_INTERVALS_PER_LEADERSHIP: u32 = 2;

/// The configuration file of a controller, as written. See `example.toml`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ControllerConfigFile {
    /// The name of this instance, recorded with the leadership.
    pub identity: String,
    /// Where `/metrics`, `/healthz` and `/readyz` are served, as `ip:port`.
    pub metrics_listen_address: String,
    /// How long the controller rests between two passes. Defaults to one second.
    #[serde(default = "default_pass_interval_milliseconds")]
    pub pass_interval_milliseconds: u64,
    /// How long leadership lasts without renewal. Defaults to ten seconds.
    #[serde(default = "default_leadership_duration_seconds")]
    pub leadership_duration_seconds: u64,
    /// How long a replica may be silent before its copies are given up. Defaults to five minutes.
    #[serde(default = "default_replica_lost_after_seconds")]
    pub replica_lost_after_seconds: u64,
    /// How long after its seal a fully replicated segment is uploaded. Defaults to fifteen minutes.
    #[serde(default = "default_tiering_delay_seconds")]
    pub tiering_delay_seconds: u64,
    /// How long after the upload the replicas keep their copies. Defaults to one day.
    #[serde(default = "default_hot_read_window_seconds")]
    pub hot_read_window_seconds: u64,
    /// Which replicas may receive new segments. Defaults to the controller's own defaults.
    #[serde(default)]
    pub placement: PlacementConfig,
    /// Where the cluster is.
    pub cluster: ClusterConfig,
    /// The log output. Defaults to JSON at `info`.
    #[serde(default)]
    pub log: LogSettings,
}

/// The `[placement]` table: which replicas may receive new segments.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PlacementConfig {
    /// A replica whose registration is older than this is presumed dead.
    #[serde(default = "default_liveness_window_seconds")]
    pub liveness_window_seconds: u64,
    /// A replica whose disk is at least this full receives no new segments.
    #[serde(default = "default_refuse_at_used_percent")]
    pub refuse_at_used_percent: u8,
}

impl Default for PlacementConfig {
    fn default() -> Self {
        Self {
            liveness_window_seconds: default_liveness_window_seconds(),
            refuse_at_used_percent: default_refuse_at_used_percent(),
        }
    }
}

/// The default of `pass_interval_milliseconds`: the controller's own.
fn default_pass_interval_milliseconds() -> u64 {
    whole_milliseconds(ControllerSettings::default().pass_interval)
}

/// The default of `leadership_duration_seconds`: the controller's own.
fn default_leadership_duration_seconds() -> u64 {
    ControllerSettings::default().leadership_duration.as_secs()
}

/// The default of `replica_lost_after_seconds`: the controller's own.
fn default_replica_lost_after_seconds() -> u64 {
    ControllerSettings::default().replica_lost_after.as_secs()
}

/// The default of `tiering_delay_seconds`: the controller's own.
fn default_tiering_delay_seconds() -> u64 {
    ControllerSettings::default().tiering_delay.as_secs()
}

/// The default of `hot_read_window_seconds`: the controller's own.
fn default_hot_read_window_seconds() -> u64 {
    ControllerSettings::default().hot_read_window.as_secs()
}

/// The default of `placement.liveness_window_seconds`: the placement policy's own.
fn default_liveness_window_seconds() -> u64 {
    PlacementPolicy::default().liveness_window.as_secs()
}

/// The default of `placement.refuse_at_used_percent`: the placement policy's own.
fn default_refuse_at_used_percent() -> u8 {
    PlacementPolicy::default().refuse_at_used_percent
}

/// A duration in whole milliseconds. The defaults this is used for are far below what fits.
fn whole_milliseconds(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// A setting that is well-formed but cannot work.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("setting `{setting}` is invalid: {reason}")]
pub(crate) struct InvalidConfig {
    /// The setting, as it is written in the file, for example `placement.liveness_window_seconds`.
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

/// The settings of a controller daemon, checked and ready to use.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ControllerDaemonSettings {
    /// Where the observability endpoint is served.
    pub metrics_listen_address: SocketAddr,
    /// Where the cluster is.
    pub cluster: ClusterConfig,
    /// The log output.
    pub log: LogSettings,
    /// How the controller paces itself and judges replicas, the instance's identity included.
    pub controller: ControllerSettings,
}

impl ControllerConfigFile {
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
    /// * `identity` that is empty or contains whitespace;
    /// * `metrics_listen_address` that is not `ip:port`;
    /// * `pass_interval_milliseconds` of zero;
    /// * `leadership_duration_seconds` shorter than two pass intervals;
    /// * `placement.liveness_window_seconds` of zero;
    /// * `placement.refuse_at_used_percent` that is not between 1 and 100;
    /// * `replica_lost_after_seconds` that is not longer than the liveness window.
    pub(crate) fn validate(self) -> Result<ControllerDaemonSettings, InvalidConfig> {
        if self.identity.is_empty() || self.identity.chars().any(char::is_whitespace) {
            return Err(InvalidConfig::because(
                "identity",
                format!(
                    "{:?} is not a name; use one word per instance, for example `controller-a`",
                    self.identity
                ),
            ));
        }
        let Ok(metrics_listen_address) = self.metrics_listen_address.parse::<SocketAddr>() else {
            return Err(InvalidConfig::because(
                "metrics_listen_address",
                format!(
                    "{:?} is not `ip:port`, for example `0.0.0.0:9101`",
                    self.metrics_listen_address
                ),
            ));
        };
        if self.pass_interval_milliseconds == 0 {
            return Err(InvalidConfig::because(
                "pass_interval_milliseconds",
                "it is 0; a controller that never rests would flood the metadata store",
            ));
        }
        let pass_interval = Duration::from_millis(self.pass_interval_milliseconds);
        let leadership_duration = Duration::from_secs(self.leadership_duration_seconds);
        if leadership_duration < pass_interval * FEWEST_PASS_INTERVALS_PER_LEADERSHIP {
            return Err(InvalidConfig::because(
                "leadership_duration_seconds",
                format!(
                    "{leadership_duration:?} is shorter than {FEWEST_PASS_INTERVALS_PER_LEADERSHIP} \
                     pass intervals of {pass_interval:?}; a leader that is resting between two \
                     passes would lose the leadership"
                ),
            ));
        }
        let placement = self.placement.validate()?;
        let replica_lost_after = Duration::from_secs(self.replica_lost_after_seconds);
        if replica_lost_after <= placement.liveness_window {
            return Err(InvalidConfig::because(
                "replica_lost_after_seconds",
                format!(
                    "{replica_lost_after:?} is not longer than the liveness window of {:?}; a \
                     replica that restarts would have its copies re-created elsewhere",
                    placement.liveness_window
                ),
            ));
        }
        Ok(ControllerDaemonSettings {
            metrics_listen_address,
            cluster: self.cluster,
            log: self.log,
            controller: ControllerSettings {
                identity: self.identity,
                pass_interval,
                leadership_duration,
                placement,
                replica_lost_after,
                tiering_delay: Duration::from_secs(self.tiering_delay_seconds),
                hot_read_window: Duration::from_secs(self.hot_read_window_seconds),
                ..ControllerSettings::default()
            },
        })
    }
}

impl PlacementConfig {
    /// Checks the `[placement]` table and converts it into the controller's placement policy.
    fn validate(self) -> Result<PlacementPolicy, InvalidConfig> {
        if self.liveness_window_seconds == 0 {
            return Err(InvalidConfig::because(
                "placement.liveness_window_seconds",
                "it is 0; every replica would count as dead",
            ));
        }
        if self.refuse_at_used_percent == 0 || self.refuse_at_used_percent > WHOLE_DISK_PERCENT {
            return Err(InvalidConfig::because(
                "placement.refuse_at_used_percent",
                format!(
                    "{} is not a share between 1 and {WHOLE_DISK_PERCENT} percent",
                    self.refuse_at_used_percent
                ),
            ));
        }
        Ok(PlacementPolicy {
            liveness_window: Duration::from_secs(self.liveness_window_seconds),
            refuse_at_used_percent: self.refuse_at_used_percent,
        })
    }
}

#[cfg(test)]
#[path = "config_test.rs"]
mod tests;
