//! The `bita controller` commands.
//!
//! # Why this file exists
//!
//! Two or three controllers run and one of them is active. When repair, tiering or deletion seem stuck,
//! the first question is whether any controller leads at all, and which one to look at. This file answers
//! it.
//!
//! # What it contains
//!
//! - [`status`]: who leads, since when, and whether the leadership is still valid.
//! - Its result: [`ControllerStatus`], [`LeaderDetails`].
//!
//! # How it works
//!
//! Controllers take turns through one record in the metadata store, which names the holder and when its
//! leadership lapses unless renewed. The record stays after a lapse, so the command compares the lapse
//! with the clock and says whether somebody leads now or merely led last.
//!
//! # Invariants
//!
//! - Read-only: the command never takes or releases the leadership.
//!
//! # See also
//!
//! - `bitacora-meta/src/store/leadership.rs`: `try_lead`, which writes the record.

use serde::Serialize;

use super::CommandError;
use crate::cluster::Cluster;
use crate::output::{format_age, format_timestamp, key_value_lines};

/// The controller instance named by the leadership record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct LeaderDetails {
    /// The name of the controller instance.
    pub(crate) holder: String,
    /// Whether its leadership had not lapsed when the command ran.
    pub(crate) is_current: bool,
    /// When its leadership began, in UTC. Absent only for a record written by an older version.
    pub(crate) leading_since: Option<String>,
    /// For how long it has led, in seconds, if the leadership is current and its start is known.
    pub(crate) leading_for_seconds: Option<u64>,
    /// When its leadership lapses unless renewed, in UTC.
    pub(crate) expires_at: String,
}

/// Who leads the controllers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ControllerStatus {
    /// The instance the leadership record names; `None` if no controller ever led this cluster.
    pub(crate) leader: Option<LeaderDetails>,
}

impl ControllerStatus {
    /// Renders the status as named values, or as one line if no controller ever led.
    pub(crate) fn render_text(&self) -> String {
        let Some(leader) = &self.leader else {
            return "no controller has ever led this cluster\n".to_owned();
        };
        let state = if leader.is_current {
            "leading"
        } else {
            "lapsed: no controller leads now"
        };
        let since = match (&leader.leading_since, leader.leading_for_seconds) {
            (Some(since), Some(seconds)) => {
                let age = format_age(std::time::Duration::from_secs(seconds));
                format!("{since} ({age})")
            }
            (Some(since), None) => since.clone(),
            (None, _) => "unknown".to_owned(),
        };
        key_value_lines(&[
            ("leader", leader.holder.clone()),
            ("state", state.to_owned()),
            ("since", since),
            ("valid until", leader.expires_at.clone()),
        ])
    }
}

/// Reads which controller instance leads.
///
/// # Arguments
///
/// * `cluster` - The interfaces to the cluster.
///
/// # Errors
///
/// Returns [`CommandError::Metadata`] if the metadata store is unavailable.
pub(crate) async fn status(cluster: &Cluster) -> Result<ControllerStatus, CommandError> {
    let Some(leader) = cluster.metadata.get_leader().await? else {
        return Ok(ControllerStatus { leader: None });
    };
    let now = cluster.clock.now();
    let is_current = leader.expires_at > now;
    let leading_for_seconds = match leader.leading_since {
        Some(since) if is_current => Some(now.duration_since(since).as_secs()),
        _lapsed_or_unknown => None,
    };
    Ok(ControllerStatus {
        leader: Some(LeaderDetails {
            holder: leader.holder,
            is_current,
            leading_since: leader.leading_since.map(format_timestamp),
            leading_for_seconds,
            expires_at: format_timestamp(leader.expires_at),
        }),
    })
}

#[cfg(test)]
#[path = "controller_test.rs"]
mod tests;
