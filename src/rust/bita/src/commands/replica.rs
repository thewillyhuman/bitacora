//! The `bita replica` commands.
//!
//! # Why this file exists
//!
//! Replicas are the part of a cluster that wears out: they are restarted, upgraded, filled up and lost.
//! The design's routine procedures (section 14) are all about them: drain one before a restart, remove a
//! failed one so that repair re-creates its copies. This file is those procedures as commands, plus the
//! two views an operator needs to decide what to do.
//!
//! # What it contains
//!
//! - [`list`]: the registry, with liveness, disk usage, draining and failure domains.
//! - [`status`]: what one replica answers about itself.
//! - [`drain`]: makes a replica refuse appends and shows which open segments still name it.
//! - [`remove`]: takes a replica out of the registry.
//! - Their results: [`ReplicaList`], [`ReplicaSummary`], [`ReplicaReport`], [`DrainReport`].
//!
//! # How it works
//!
//! `list` reads the registry in the metadata store and never contacts a replica, so it works while
//! replicas are down, which is when it is needed. `status` and `drain` ask the replica itself, at the
//! address it registered, with the request timeout.
//!
//! A replica counts as alive if it renewed its registration within the placement policy's liveness
//! window: the same rule placement uses to decide whether it may receive new segments.
//!
//! Draining is the replica's own state: once told, it refuses appends, and each writer that hits the
//! refusal rolls to its standby segment. `drain` therefore cannot finish the job itself. It reports the
//! open segments, and the standby sets, that still name the replica, and with a wait it keeps looking
//! until there are none.
//!
//! # Invariants
//!
//! - `remove` refuses a replica that is alive unless forced: removing one that still serves makes repair
//!   copy data that was never lost.
//! - Nothing here deletes data from a replica. Removing a replica from the registry only changes what the
//!   controller believes.
//!
//! # See also
//!
//! - `bitacora-meta/src/placement.rs`: the liveness window.
//! - `bitacora-controller`: what happens to the copies of a removed replica.

use std::time::Duration;

use bitacora_meta::{PlacementPolicy, ReplicaRecord, StreamRecord};
use bitacora_types::ReplicaId;
use serde::Serialize;
use tokio::time::{Instant, sleep, timeout};

use super::{Acknowledgement, CommandError};
use crate::cluster::Cluster;
use crate::output::{Table, format_age, format_bytes, format_timestamp, key_value_lines};

/// How long `drain` rests between two looks at the metadata store while it waits.
const DRAIN_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// One registered replica.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ReplicaSummary {
    /// The identity of the replica's data directory.
    pub(crate) replica_id: String,
    /// Where writers and readers reach the replica.
    pub(crate) address: String,
    /// Whether the replica renewed its registration within the liveness window.
    pub(crate) is_alive: bool,
    /// When the replica last renewed its registration, in UTC.
    pub(crate) last_heartbeat_at: String,
    /// How long ago that was, in seconds.
    pub(crate) heartbeat_age_seconds: u64,
    /// The share of the replica's disk in use, in percent.
    pub(crate) used_percent: u8,
    /// The total size of the replica's disk.
    pub(crate) capacity_bytes: u64,
    /// How much of the disk is unused.
    pub(crate) free_bytes: u64,
    /// Whether the replica registered itself as draining.
    pub(crate) is_draining: bool,
    /// The replica's failure domains, widest first.
    pub(crate) failure_domains: Vec<String>,
}

/// The registered replicas.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ReplicaList {
    /// The replicas, sorted by identifier.
    pub(crate) replicas: Vec<ReplicaSummary>,
}

impl ReplicaList {
    /// Renders the replicas as a table, one row each.
    pub(crate) fn render_text(&self) -> String {
        let mut table = Table::new(&[
            "REPLICA",
            "ADDRESS",
            "ALIVE",
            "HEARTBEAT",
            "USED",
            "CAPACITY",
            "DRAINING",
            "DOMAINS",
        ]);
        for replica in &self.replicas {
            table.push_row(vec![
                replica.replica_id.clone(),
                replica.address.clone(),
                yes_or_no(replica.is_alive),
                format!("{} ago", format_age(Duration::from_secs(replica.heartbeat_age_seconds))),
                format!("{}%", replica.used_percent),
                format_bytes(replica.capacity_bytes),
                yes_or_no(replica.is_draining),
                replica.failure_domains.join("/"),
            ]);
        }
        table.render()
    }
}

/// Renders a flag as the word an operator reads.
fn yes_or_no(flag: bool) -> String {
    if flag { "yes" } else { "no" }.to_owned()
}

/// What one replica reports about itself.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ReplicaReport {
    /// The identity the replica answered with.
    pub(crate) replica_id: String,
    /// The address the replica was reached at.
    pub(crate) address: String,
    /// The total size of its disk.
    pub(crate) capacity_bytes: u64,
    /// How much of the disk is unused.
    pub(crate) free_bytes: u64,
    /// Whether the replica is draining.
    pub(crate) is_draining: bool,
    /// How many segment copies it holds.
    pub(crate) segment_count: u64,
}

impl ReplicaReport {
    /// Renders the report as named values.
    pub(crate) fn render_text(&self) -> String {
        key_value_lines(&[
            ("replica", self.replica_id.clone()),
            ("address", self.address.clone()),
            ("capacity", format_bytes(self.capacity_bytes)),
            ("free", format_bytes(self.free_bytes)),
            ("draining", yes_or_no(self.is_draining)),
            ("segment copies", self.segment_count.to_string()),
        ])
    }
}

/// An open segment that still names a draining replica.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct OpenSegmentReference {
    /// The stream the segment belongs to.
    pub(crate) stream: String,
    /// The segment's identifier within the stream.
    pub(crate) segment_id: u64,
}

/// How far a drain has got.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct DrainReport {
    /// The replica that was told to drain.
    pub(crate) replica_id: String,
    /// Whether no open segment and no standby set names the replica any more: it can be stopped without
    /// making any writer roll.
    pub(crate) is_drained: bool,
    /// The open segments that still name the replica.
    pub(crate) open_segments: Vec<OpenSegmentReference>,
    /// The streams whose standby set, the replicas of their next segment, still names the replica.
    pub(crate) standby_streams: Vec<String>,
}

impl DrainReport {
    /// Renders the report: one line if the replica is drained, otherwise what still names it.
    pub(crate) fn render_text(&self) -> String {
        if self.is_drained {
            return format!(
                "replica {} is draining and no open segment names it: it can be stopped\n",
                self.replica_id
            );
        }
        let mut rendered = format!(
            "replica {} is draining; writers roll away from it as they append\n",
            self.replica_id
        );
        if !self.open_segments.is_empty() {
            let mut table = Table::new(&["STREAM", "OPEN SEGMENT"]);
            for segment in &self.open_segments {
                table.push_row(vec![segment.stream.clone(), segment.segment_id.to_string()]);
            }
            rendered.push('\n');
            rendered.push_str(&table.render());
        }
        if !self.standby_streams.is_empty() {
            rendered.push('\n');
            rendered.push_str("streams whose standby set still names it: ");
            rendered.push_str(&self.standby_streams.join(" "));
            rendered.push('\n');
        }
        rendered
    }
}

/// Lists the registered replicas.
///
/// Reads the registry only; no replica is contacted.
///
/// # Errors
///
/// Returns [`CommandError::Metadata`] if the metadata store is unavailable.
pub(crate) async fn list(cluster: &Cluster) -> Result<ReplicaList, CommandError> {
    let registered_replicas = cluster.metadata.list_replicas().await?;
    let now = cluster.clock.now();
    let liveness_window = PlacementPolicy::default().liveness_window;
    let mut replicas = Vec::with_capacity(registered_replicas.len());
    for replica in &registered_replicas {
        replicas.push(ReplicaSummary {
            replica_id: replica.replica_id.to_string(),
            address: replica.address.clone(),
            is_alive: replica.is_alive(now, liveness_window),
            last_heartbeat_at: format_timestamp(replica.heartbeat_at),
            heartbeat_age_seconds: now.duration_since(replica.heartbeat_at).as_secs(),
            used_percent: replica.used_percent(),
            capacity_bytes: replica.capacity_bytes,
            free_bytes: replica.free_bytes,
            is_draining: replica.is_draining,
            failure_domains: replica.failure_domains.clone(),
        });
    }
    Ok(ReplicaList { replicas })
}

/// Asks one replica what it reports about itself.
///
/// # Errors
///
/// * [`CommandError::UnknownReplica`] if no replica with this identifier is registered.
/// * [`CommandError::Replica`] or [`CommandError::ReplicaTimedOut`] if the replica does not answer.
pub(crate) async fn status(cluster: &Cluster, replica_id: &ReplicaId) -> Result<ReplicaReport, CommandError> {
    let registration = registered_replica(cluster, replica_id).await?;
    let client = cluster
        .connector
        .connect(&registration.replica_id, &registration.address);
    let answer = timeout(cluster.request_timeout, client.status()).await;
    let reported = match answer {
        Ok(Ok(reported)) => reported,
        Ok(Err(reason)) => {
            return Err(CommandError::Replica {
                replica_id: replica_id.clone(),
                reason,
            });
        }
        Err(_elapsed) => {
            return Err(CommandError::ReplicaTimedOut {
                replica_id: replica_id.clone(),
            });
        }
    };
    Ok(ReplicaReport {
        replica_id: reported.replica_id.to_string(),
        address: registration.address,
        capacity_bytes: reported.capacity_bytes,
        free_bytes: reported.free_bytes,
        is_draining: reported.is_draining,
        segment_count: reported.segment_count,
    })
}

/// Tells a replica to drain and reports which open segments still name it.
///
/// # Arguments
///
/// * `cluster` - The interfaces to the cluster.
/// * `replica_id` - The replica to drain.
/// * `wait` - How long to keep looking until no open segment and no standby set names the replica;
///   `None` looks once.
///
/// # Returns
///
/// What still names the replica when the command stopped looking. `is_drained` says whether the wait
/// ended because nothing does.
///
/// # Errors
///
/// * [`CommandError::UnknownReplica`] if no replica with this identifier is registered.
/// * [`CommandError::Replica`] or [`CommandError::ReplicaTimedOut`] if the replica does not take the
///   order.
/// * [`CommandError::Metadata`] if the metadata store is unavailable.
pub(crate) async fn drain(
    cluster: &Cluster,
    replica_id: &ReplicaId,
    wait: Option<Duration>,
) -> Result<DrainReport, CommandError> {
    let registration = registered_replica(cluster, replica_id).await?;
    let client = cluster
        .connector
        .connect(&registration.replica_id, &registration.address);
    match timeout(cluster.request_timeout, client.drain()).await {
        Ok(Ok(())) => {}
        Ok(Err(reason)) => {
            return Err(CommandError::Replica {
                replica_id: replica_id.clone(),
                reason,
            });
        }
        Err(_elapsed) => {
            return Err(CommandError::ReplicaTimedOut {
                replica_id: replica_id.clone(),
            });
        }
    }

    let give_up_at = Instant::now() + wait.unwrap_or_default();
    loop {
        let report = drain_progress(cluster, replica_id).await?;
        if report.is_drained || Instant::now() >= give_up_at {
            return Ok(report);
        }
        sleep(DRAIN_POLL_INTERVAL).await;
    }
}

/// Looks at every stream and collects what still names `replica_id`.
async fn drain_progress(cluster: &Cluster, replica_id: &ReplicaId) -> Result<DrainReport, CommandError> {
    let streams = cluster.metadata.list_streams().await?;
    let mut open_segments = Vec::new();
    let mut standby_streams = Vec::new();
    for stream in &streams {
        if let Some(segment_id) = open_segment_naming(stream, replica_id) {
            open_segments.push(OpenSegmentReference {
                stream: stream.name.clone(),
                segment_id,
            });
        }
        let is_in_standby = stream
            .standby_replicas
            .as_ref()
            .is_some_and(|standby| standby.contains(replica_id));
        if is_in_standby {
            standby_streams.push(stream.name.clone());
        }
    }
    Ok(DrainReport {
        replica_id: replica_id.to_string(),
        is_drained: open_segments.is_empty() && standby_streams.is_empty(),
        open_segments,
        standby_streams,
    })
}

/// The identifier of the stream's open segment, if that segment has a copy on `replica_id`.
fn open_segment_naming(stream: &StreamRecord, replica_id: &ReplicaId) -> Option<u64> {
    let open_segment = stream.open_segment()?;
    for copy in &open_segment.copies {
        if &copy.replica_id == replica_id {
            return Some(open_segment.segment_id.get());
        }
    }
    None
}

/// Removes a replica from the registry, after which the controller re-creates its copies elsewhere.
///
/// # Arguments
///
/// * `cluster` - The interfaces to the cluster.
/// * `replica_id` - The replica to remove.
/// * `is_forced` - Remove the replica even though it is alive.
///
/// # Errors
///
/// * [`CommandError::UnknownReplica`] if no replica with this identifier is registered.
/// * [`CommandError::Refused`] if the replica is alive and the removal is not forced.
/// * [`CommandError::Metadata`] if the metadata store is unavailable.
pub(crate) async fn remove(
    cluster: &Cluster,
    replica_id: &ReplicaId,
    is_forced: bool,
) -> Result<Acknowledgement, CommandError> {
    let registration = registered_replica(cluster, replica_id).await?;
    let liveness_window = PlacementPolicy::default().liveness_window;
    let is_alive = registration.is_alive(cluster.clock.now(), liveness_window);
    if is_alive && !is_forced {
        return Err(CommandError::Refused {
            reason: format!(
                "replica {replica_id} renewed its registration less than {} ago and looks alive; \
                 removing it makes the controller re-create copies that are not lost. \
                 Stop it first, or pass --force",
                format_age(liveness_window)
            ),
        });
    }
    cluster.metadata.remove_replica(replica_id).await?;
    Ok(Acknowledgement {
        message: format!("replica {replica_id} removed from the registry; the controller re-creates its copies"),
    })
}

/// Looks a replica up in the registry.
async fn registered_replica(cluster: &Cluster, replica_id: &ReplicaId) -> Result<ReplicaRecord, CommandError> {
    let registered_replicas = cluster.metadata.list_replicas().await?;
    for replica in registered_replicas {
        if &replica.replica_id == replica_id {
            return Ok(replica);
        }
    }
    Err(CommandError::UnknownReplica {
        replica_id: replica_id.clone(),
    })
}

#[cfg(test)]
#[path = "replica_test.rs"]
mod tests;
