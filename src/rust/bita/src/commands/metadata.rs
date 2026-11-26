//! The `bita metadata snapshot` and `bita metadata restore` commands.
//!
//! # Why this file exists
//!
//! The controller copies the metadata store to the object store every few minutes (design, section 14).
//! Those copies are worth something only if an operator can see them and put one back after the metadata
//! store was lost. This file is that half of disaster recovery; `rebuild.rs` is the other half, for
//! what no snapshot holds.
//!
//! # What it contains
//!
//! - [`list_snapshots`]: the snapshots in the object store.
//! - [`restore`]: writes one snapshot back into the metadata store.
//! - Their results: [`SnapshotList`], [`SnapshotRow`], [`RestoreReport`].
//! - The steps of a restore: `resolve_snapshot_key`, `fetch_snapshot`, `count_keys_missing_from` and
//!   `restore_mode_for`.
//!
//! # How it works
//!
//! Snapshots are objects under `metadata-snapshots/`, named after the moment they were taken, so a
//! listing gives them oldest first. `restore` fetches one, verifies its checksum, and writes every key it
//! holds into the metadata store with the bytes it had.
//!
//! A restore into a store that already holds keys would mix two states, so it is refused unless forced;
//! forced, it makes the store exactly what the snapshot was, deleting every other key. `--dry-run` does
//! everything except the write, and reports what the write would do.
//!
//! [`restore`] reads in a fixed order and writes last: the snapshot's key (a listing, for `latest`), the
//! snapshot object, then the whole metadata store as it is. Only then does it decide whether to write.
//!
//! A snapshot is up to one snapshot interval older than the cluster. Streams that rolled or changed
//! writer since then are behind in the restored store, which is why the procedure is restore first, then
//! `bita metadata rebuild`.
//!
//! # Invariants
//!
//! - A snapshot that fails its checksum is never restored, not even in part.
//! - A dry run writes nothing.
//! - Both commands need the cluster's object store and say so if the configuration names none.
//!
//! # See also
//!
//! - `bitacora-meta/src/snapshot.rs`: the snapshot format.
//! - `bitacora-controller/src/controller/snapshot.rs`: who writes the snapshots.
//! - `commands/rebuild.rs`: recovery from the replicas.

use std::collections::BTreeSet;
use std::sync::Arc;

use bitacora_artifact::ArtifactStore;
use bitacora_meta::{MetadataSnapshot, RestoreMode, SNAPSHOT_KEY_PREFIX, snapshot_time_of};
use bytes::Bytes;
use serde::Serialize;
use tokio::time::timeout;

use super::CommandError;
use crate::cluster::Cluster;
use crate::output::{Table, format_age, format_bytes, format_timestamp, key_value_lines};

/// What the operator writes instead of an object key to mean the newest snapshot.
const LATEST_SNAPSHOT: &str = "latest";

/// One snapshot of the metadata store.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct SnapshotRow {
    /// The key of the snapshot object; what `bita metadata restore` takes.
    pub(crate) object_key: String,
    /// When the snapshot was taken, in UTC.
    pub(crate) taken_at: String,
    /// How long ago that was, in seconds.
    pub(crate) age_seconds: u64,
    /// The size of the snapshot object.
    pub(crate) size_bytes: u64,
}

/// The snapshots of the metadata store in the object store.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct SnapshotList {
    /// The snapshots, oldest first.
    pub(crate) snapshots: Vec<SnapshotRow>,
}

impl SnapshotList {
    /// Renders the snapshots as a table, one row each.
    pub(crate) fn render_text(&self) -> String {
        let mut table = Table::new(&["SNAPSHOT", "TAKEN AT", "AGE", "SIZE"]);
        for snapshot in &self.snapshots {
            table.push_row(vec![
                snapshot.object_key.clone(),
                snapshot.taken_at.clone(),
                format_age(std::time::Duration::from_secs(snapshot.age_seconds)),
                format_bytes(snapshot.size_bytes),
            ]);
        }
        table.render()
    }
}

/// What a restore did, or would do.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct RestoreReport {
    /// The key of the snapshot object that was read.
    pub(crate) snapshot: String,
    /// When the snapshot was taken, in UTC.
    pub(crate) taken_at: String,
    /// Whether nothing was written because the command was a dry run.
    pub(crate) is_dry_run: bool,
    /// How many keys the metadata store held before the restore.
    pub(crate) keys_in_store_before: usize,
    /// How many keys were, or would be, written from the snapshot.
    pub(crate) keys_written: usize,
    /// How many keys the snapshot does not hold were, or would be, deleted.
    pub(crate) keys_deleted: usize,
}

impl RestoreReport {
    /// Renders the report as named values, followed by what to do next.
    pub(crate) fn render_text(&self) -> String {
        let outcome = if self.is_dry_run {
            "dry run: nothing was written"
        } else {
            "restored"
        };
        let mut rendered = key_value_lines(&[
            ("snapshot", self.snapshot.clone()),
            ("taken at", self.taken_at.clone()),
            ("outcome", outcome.to_owned()),
            ("keys in the store before", self.keys_in_store_before.to_string()),
            ("keys written", self.keys_written.to_string()),
            ("keys deleted", self.keys_deleted.to_string()),
        ]);
        if self.is_dry_run && self.keys_in_store_before > 0 {
            rendered.push_str("the metadata store is not empty: the restore needs --force and replaces it\n");
        }
        if !self.is_dry_run {
            rendered.push_str("next: run `bita metadata rebuild` to pick up what changed after the snapshot\n");
        }
        rendered
    }
}

/// The cluster's object store, or the error that tells the operator how to name one.
fn object_store_of(cluster: &Cluster) -> Result<Arc<dyn ArtifactStore>, CommandError> {
    match &cluster.artifact_store {
        Some(artifact_store) => Ok(Arc::clone(artifact_store)),
        None => Err(CommandError::NoObjectStore),
    }
}

/// Lists the metadata snapshots in the object store, oldest first.
///
/// # Arguments
///
/// * `cluster` - The interfaces to the cluster. The metadata store is not touched: this works while it
///   is lost.
///
/// # Errors
///
/// * [`CommandError::NoObjectStore`] if the configuration names no object store.
/// * [`CommandError::ObjectStore`] or [`CommandError::ObjectStoreTimedOut`] if it cannot be listed.
pub(crate) async fn list_snapshots(cluster: &Cluster) -> Result<SnapshotList, CommandError> {
    let artifact_store = object_store_of(cluster)?;
    let listing = timeout(cluster.request_timeout, artifact_store.list_prefix(SNAPSHOT_KEY_PREFIX)).await;
    let objects = match listing {
        Ok(Ok(objects)) => objects,
        Ok(Err(reason)) => return Err(CommandError::ObjectStore { reason }),
        Err(_elapsed) => return Err(CommandError::ObjectStoreTimedOut),
    };
    let now = cluster.clock.now();
    let mut snapshots = Vec::with_capacity(objects.len());
    for object in objects {
        // Objects under the prefix that are not named like a snapshot are somebody else's.
        let Some(taken_at) = snapshot_time_of(&object.object_key) else {
            continue;
        };
        snapshots.push(SnapshotRow {
            object_key: object.object_key,
            taken_at: format_timestamp(taken_at),
            age_seconds: now.duration_since(taken_at).as_secs(),
            size_bytes: object.size_bytes,
        });
    }
    Ok(SnapshotList { snapshots })
}

/// Writes a snapshot back into the metadata store.
///
/// # Arguments
///
/// * `cluster` - The interfaces to the cluster.
/// * `snapshot_name` - The object key of the snapshot, or `latest` for the newest one.
/// * `is_forced` - Restore although the metadata store holds keys: it ends up exactly as the snapshot
///   was.
/// * `is_dry_run` - Read and verify the snapshot, compare it with the store, and write nothing.
///
/// # Returns
///
/// What was, or would be, written and deleted.
///
/// # Errors
///
/// * [`CommandError::NoObjectStore`] if the configuration names no object store.
/// * [`CommandError::NothingToDo`] if `latest` was asked for and there is no snapshot.
/// * [`CommandError::ObjectStore`] or [`CommandError::ObjectStoreTimedOut`] if the snapshot cannot be
///   fetched.
/// * [`CommandError::UnreadableSnapshot`] if the object is not an intact snapshot.
/// * [`CommandError::Refused`] if the metadata store holds keys and the restore is not forced.
/// * [`CommandError::Metadata`] if the metadata store is unavailable or changed during the restore.
pub(crate) async fn restore(
    cluster: &Cluster,
    snapshot_name: &str,
    is_forced: bool,
    is_dry_run: bool,
) -> Result<RestoreReport, CommandError> {
    let artifact_store = object_store_of(cluster)?;
    let object_key = resolve_snapshot_key(cluster, snapshot_name).await?;
    let snapshot = fetch_snapshot(cluster, artifact_store.as_ref(), &object_key).await?;
    let store_before = cluster.metadata.snapshot(cluster.clock.now()).await?;
    let mut report = RestoreReport {
        snapshot: object_key,
        taken_at: format_timestamp(snapshot.taken_at),
        is_dry_run,
        keys_in_store_before: store_before.entries.len(),
        keys_written: snapshot.entries.len(),
        keys_deleted: count_keys_missing_from(&snapshot, &store_before),
    };
    if is_dry_run {
        return Ok(report);
    }
    let mode = restore_mode_for(store_before.entries.len(), is_forced)?;
    let outcome = cluster.metadata.restore(&snapshot, mode).await?;
    report.keys_written = outcome.keys_written;
    report.keys_deleted = outcome.keys_deleted;
    Ok(report)
}

/// The object key of the snapshot the operator named: the name itself, or the key of the newest snapshot
/// in the object store for `latest`.
///
/// # Errors
///
/// * [`CommandError::NothingToDo`] if `latest` was asked for and there is no snapshot.
/// * What [`list_snapshots`] returns if the snapshots cannot be listed.
async fn resolve_snapshot_key(cluster: &Cluster, snapshot_name: &str) -> Result<String, CommandError> {
    if snapshot_name != LATEST_SNAPSHOT {
        return Ok(snapshot_name.to_owned());
    }
    let listed = list_snapshots(cluster).await?;
    match listed.snapshots.last() {
        Some(newest) => Ok(newest.object_key.clone()),
        None => Err(CommandError::NothingToDo {
            reason: "the object store holds no metadata snapshot".to_owned(),
        }),
    }
}

/// Fetches the snapshot stored under `object_key` and verifies it.
///
/// # Errors
///
/// * [`CommandError::ObjectStore`] or [`CommandError::ObjectStoreTimedOut`] if the object cannot be
///   fetched.
/// * [`CommandError::UnreadableSnapshot`] if the object is not an intact snapshot.
async fn fetch_snapshot(
    cluster: &Cluster,
    artifact_store: &dyn ArtifactStore,
    object_key: &str,
) -> Result<MetadataSnapshot, CommandError> {
    let stored_object = fetch_object(cluster, artifact_store, object_key).await?;
    match MetadataSnapshot::decode(&stored_object) {
        Ok(snapshot) => Ok(snapshot),
        Err(reason) => Err(CommandError::UnreadableSnapshot {
            object_key: object_key.to_owned(),
            reason,
        }),
    }
}

/// How many keys of `store_before` the snapshot does not hold: the keys a forced restore deletes.
fn count_keys_missing_from(snapshot: &MetadataSnapshot, store_before: &MetadataSnapshot) -> usize {
    let mut snapshot_keys = BTreeSet::new();
    for entry in &snapshot.entries {
        snapshot_keys.insert(entry.key.as_str());
    }
    let mut missing_key_count = 0;
    for existing in &store_before.entries {
        if !snapshot_keys.contains(existing.key.as_str()) {
            missing_key_count += 1;
        }
    }
    missing_key_count
}

/// How to write a snapshot into a store that holds `keys_in_store` keys.
///
/// # Errors
///
/// Returns [`CommandError::Refused`] if the store holds keys and the restore is not forced: it would
/// replace all of them.
fn restore_mode_for(keys_in_store: usize, is_forced: bool) -> Result<RestoreMode, CommandError> {
    if keys_in_store > 0 && !is_forced {
        return Err(CommandError::Refused {
            reason: format!(
                "the metadata store holds {keys_in_store} keys; restoring replaces all of them with the snapshot. \
                 Look at it with --dry-run, then pass --force"
            ),
        });
    }
    if is_forced {
        Ok(RestoreMode::ReplaceEverything)
    } else {
        Ok(RestoreMode::IntoEmptyStore)
    }
}

/// Fetches one whole object, waiting no longer than the request timeout.
async fn fetch_object(
    cluster: &Cluster,
    artifact_store: &dyn ArtifactStore,
    object_key: &str,
) -> Result<Bytes, CommandError> {
    let fetched = timeout(cluster.request_timeout, artifact_store.get_object(object_key)).await;
    match fetched {
        Ok(Ok(stored_object)) => Ok(stored_object),
        Ok(Err(reason)) => Err(CommandError::ObjectStore { reason }),
        Err(_elapsed) => Err(CommandError::ObjectStoreTimedOut),
    }
}

#[cfg(test)]
#[path = "metadata_test.rs"]
mod tests;
