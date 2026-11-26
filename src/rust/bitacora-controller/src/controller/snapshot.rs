//! The controller's metadata snapshots: copying the metadata store to the object store.
//!
//! # Why this file exists
//!
//! The metadata store is the one thing a cluster cannot rebuild cheaply: it says which segments make up a
//! stream and where they end. The design therefore keeps a copy of it in the object store, refreshed every
//! five minutes (section 14, "Routine procedures"), so that the loss of every etcd member is recovered
//! from by restoring a snapshot instead of scanning every replica. Somebody has to write those snapshots
//! and throw old ones away. The controller already runs on a timer and already has the object store, so
//! it does both, at the end of its pass.
//!
//! # What it contains
//!
//! - `Controller::snapshot_metadata`: writes a snapshot when one is due, and prunes old ones.
//! - `Controller::newest_snapshot_time`, `is_snapshot_due`, `write_snapshot`, `prune_snapshots`: the steps
//!   below, one function each.
//! - `StoredSnapshot`: one snapshot object as the object store lists it.
//! - `SnapshotFailure`: why a snapshot could not be listed or written.
//!
//! # How it works
//!
//! 1. **Is one due?** A snapshot is due when the newest one in the object store is older than the
//!    snapshot interval, or there is none. The controller remembers when the newest was taken; an
//!    instance that does not know yet, because it just started, lists the object store once to find out.
//! 2. **Read.** The whole metadata store is read in one listing, which both backends answer from one
//!    consistent view.
//! 3. **Write.** The snapshot is encoded with its checksum and stored under a key that names the moment
//!    it was taken, so that keys sort by time.
//! 4. **Prune.** All but the newest `metadata_snapshots_kept` snapshots are deleted.
//!
//! Nothing here can fail a pass. A snapshot that cannot be read, written or pruned is logged, and the next
//! pass tries again, because nothing was remembered as done.
//!
//! # Invariants
//!
//! - Without an object store, no snapshot is taken and nothing is listed.
//! - The newest snapshot is never deleted, whatever `metadata_snapshots_kept` says.
//! - Two controllers that both believe they lead write at most one snapshot too many each. Snapshots are
//!   whole and independent, so an extra one is only an extra object.
//!
//! # See also
//!
//! - `bitacora-meta/src/snapshot.rs`: the snapshot format and the naming of the objects.
//! - `../controller.rs`: the pass this step ends.
//! - `bita metadata restore`: what reads the snapshots back.

use std::fmt;
use std::sync::PoisonError;

use bitacora_artifact::ArtifactStore;
use bitacora_meta::{MetaError, SNAPSHOT_KEY_PREFIX, snapshot_object_key, snapshot_time_of};
use bitacora_types::Timestamp;

use super::{Controller, ObjectStoreFailure, PassReport, object_store_answer_within};

/// Why a metadata snapshot could not be listed or written.
#[derive(Debug)]
enum SnapshotFailure {
    /// The metadata store could not be read.
    MetadataStore(MetaError),
    /// The object store failed or did not answer in time.
    ObjectStore(ObjectStoreFailure),
}

impl fmt::Display for SnapshotFailure {
    /// Writes the error of the store that failed.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MetadataStore(meta_error) => write!(formatter, "{meta_error}"),
            Self::ObjectStore(failure) => write!(formatter, "{failure}"),
        }
    }
}

/// One snapshot object in the object store.
struct StoredSnapshot {
    /// The object's key.
    object_key: String,
    /// When the snapshot was taken, as its key says.
    taken_at: Timestamp,
}

impl Controller {
    /// Writes a snapshot of the metadata store to the object store if one is due, then deletes the
    /// snapshots that are no longer kept.
    ///
    /// See the file header for the steps. Failures are logged, never returned: a pass that did its work
    /// must not fail because a backup could not be written.
    ///
    /// # Arguments
    ///
    /// * `now` - The time of this pass. The snapshot is named after it.
    /// * `report` - Counts the snapshot if one was written.
    pub(super) async fn snapshot_metadata(&self, now: Timestamp, report: &mut PassReport) {
        let Some(artifact_store) = self.artifact_store.clone() else {
            return;
        };
        let newest_snapshot_at = match self.newest_snapshot_time(artifact_store.as_ref()).await {
            Ok(newest_snapshot_at) => newest_snapshot_at,
            Err(reason) => {
                tracing::warn!(%reason, "cannot list metadata snapshots; none is taken this pass");
                return;
            }
        };
        if !self.is_snapshot_due(newest_snapshot_at, now) {
            self.remember_newest_snapshot(newest_snapshot_at);
            return;
        }

        if let Err(reason) = self.write_snapshot(artifact_store.as_ref(), now).await {
            tracing::warn!(%reason, "cannot write a metadata snapshot; the next pass tries again");
            return;
        }
        self.remember_newest_snapshot(Some(now));
        report.metadata_snapshots_written += 1;
        self.prune_snapshots(artifact_store.as_ref()).await;
    }

    /// Whether a snapshot should be taken now: there is none yet, or the newest is at least one snapshot
    /// interval old.
    fn is_snapshot_due(&self, newest_snapshot_at: Option<Timestamp>, now: Timestamp) -> bool {
        match newest_snapshot_at {
            Some(newest) => now.duration_since(newest) >= self.settings.metadata_snapshot_interval,
            None => true,
        }
    }

    /// When the newest snapshot was taken, if this instance already knows.
    fn remembered_newest_snapshot(&self) -> Option<Timestamp> {
        *self
            .newest_metadata_snapshot_at
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Remembers when the newest snapshot was taken, so that the next pass need not list the object
    /// store to learn it.
    fn remember_newest_snapshot(&self, taken_at: Option<Timestamp>) {
        *self
            .newest_metadata_snapshot_at
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = taken_at;
    }

    /// When the newest snapshot was taken: what this instance remembers, or else what the object store lists.
    /// `None` if the object store holds no snapshot.
    ///
    /// # Errors
    ///
    /// Returns why the object store could not be listed.
    async fn newest_snapshot_time(
        &self,
        artifact_store: &dyn ArtifactStore,
    ) -> Result<Option<Timestamp>, SnapshotFailure> {
        if let Some(remembered) = self.remembered_newest_snapshot() {
            return Ok(Some(remembered));
        }
        let stored_snapshots = self.list_snapshots(artifact_store).await?;
        Ok(stored_snapshots.last().map(|newest| newest.taken_at))
    }

    /// Lists the snapshot objects with the time each was taken, oldest first. Objects under the snapshot
    /// prefix that are not named like a snapshot are left out, and left alone.
    ///
    /// # Errors
    ///
    /// Returns why the object store could not be listed.
    async fn list_snapshots(&self, artifact_store: &dyn ArtifactStore) -> Result<Vec<StoredSnapshot>, SnapshotFailure> {
        let listing = object_store_answer_within(
            self.settings.request_timeout,
            artifact_store.list_prefix(SNAPSHOT_KEY_PREFIX),
        );
        let objects = listing.await.map_err(SnapshotFailure::ObjectStore)?;
        let mut stored_snapshots = Vec::with_capacity(objects.len());
        for object in objects {
            if let Some(taken_at) = snapshot_time_of(&object.object_key) {
                stored_snapshots.push(StoredSnapshot {
                    object_key: object.object_key,
                    taken_at,
                });
            }
        }
        Ok(stored_snapshots)
    }

    /// Reads the whole metadata store and stores it as the snapshot taken at `now`.
    ///
    /// # Errors
    ///
    /// Returns why the metadata store could not be read or the object could not be stored.
    async fn write_snapshot(&self, artifact_store: &dyn ArtifactStore, now: Timestamp) -> Result<(), SnapshotFailure> {
        let snapshot = self
            .metadata
            .snapshot(now)
            .await
            .map_err(SnapshotFailure::MetadataStore)?;
        let entry_count = snapshot.entries.len();
        let object_key = snapshot_object_key(now);
        let storing = object_store_answer_within(
            self.settings.request_timeout,
            artifact_store.put_object(&object_key, snapshot.encode()),
        );
        storing.await.map_err(SnapshotFailure::ObjectStore)?;
        tracing::info!(%object_key, entry_count, "wrote a metadata snapshot");
        Ok(())
    }

    /// Deletes every snapshot but the newest `metadata_snapshots_kept`. A snapshot that cannot be
    /// deleted is logged and tried again after the next snapshot.
    async fn prune_snapshots(&self, artifact_store: &dyn ArtifactStore) {
        let stored_snapshots = match self.list_snapshots(artifact_store).await {
            Ok(stored_snapshots) => stored_snapshots,
            Err(reason) => {
                tracing::warn!(%reason, "cannot list metadata snapshots to prune them");
                return;
            }
        };
        // Never delete the newest snapshot, whatever the setting says.
        let kept_count = self.settings.metadata_snapshots_kept.max(1);
        let outdated_count = stored_snapshots.len().saturating_sub(kept_count);
        for outdated in &stored_snapshots[..outdated_count] {
            let object_key = &outdated.object_key;
            let deletion =
                object_store_answer_within(self.settings.request_timeout, artifact_store.delete_object(object_key));
            match deletion.await {
                Ok(()) => tracing::debug!(%object_key, "deleted an old metadata snapshot"),
                Err(reason) => {
                    tracing::warn!(%object_key, %reason, "cannot delete an old metadata snapshot");
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "snapshot_test.rs"]
mod tests;
