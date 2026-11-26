//! Refusing to start an empty data directory under an identity the cluster already knows.
//!
//! # Why this file exists
//!
//! A replica's identity belongs to its data directory. Writers and the controller reason about what
//! "replica X" holds: a record is committed once W replicas acknowledged it, and a writer that takes a
//! stream over trusts that one of any N − W + 1 replicas it hears from holds every committed record. That
//! only works if a replica that answers as X is the replica that made X's acknowledgements.
//!
//! A data directory that lost its contents made none of them any more. If the daemon started on it under
//! the old name, it would answer "I hold nothing" in the name of a replica that once held committed
//! records, and a recovery could seal a segment below them (decision record
//! `docs/decisions/0010-replica-identity.md`). The identity is taken from the configuration only when the
//! directory is empty, so the configuration of a machine whose disk was replaced proposes exactly the
//! name that must not be used again. This file is the check that refuses it.
//!
//! # What it contains
//!
//! - [`refuse_a_known_identity_on_an_empty_directory`]: the check, run once before the replica is opened.
//!
//! # How it works
//!
//! 1. A directory that already has its identity file is not empty: it keeps that identity, whatever the
//!    configuration proposes, and there is nothing to check.
//! 2. For an empty directory, the proposed identity must be unknown to the cluster: not in the registry
//!    of replicas, and not named by any stream as the holder of a copy or as a standby. A stream can name
//!    a replica that is no longer registered, which is why both are looked at.
//! 3. If the metadata store cannot be asked, an empty directory does not start. The supervisor starts
//!    the daemon again, and it starts once the question can be answered. A directory that has its
//!    identity never waits for the metadata store.
//!
//! # Invariants
//!
//! - An empty data directory never starts under an identity the cluster knows.
//! - A data directory that has an identity starts whether or not the metadata store answers.
//! - The check reads; it never writes to the disk or to the metadata store.
//!
//! # See also
//!
//! - `daemon.rs`: where the check runs, between opening the stores and opening the replica.
//! - `docs/protocol/disk-format.md`: the identity file of a data directory.
//! - `bitacora-replica`: `Replica::open`, which stores the proposed identity on an empty directory.

use bitacora_meta::{MetadataStore, StreamRecord};
use bitacora_segment::Disk;
use bitacora_types::ReplicaId;

use crate::daemon::DaemonError;

/// The file in which a data directory keeps its identity. The name is part of the layout of a data
/// directory (`docs/protocol/disk-format.md`); the replica crate writes the file.
const IDENTITY_FILE_PATH: &str = "replica.id";

/// Refuses to go on if `disk` is an empty data directory and the cluster already knows
/// `proposed_replica_id`.
///
/// Reads the identity file of the directory and, only if there is none, the registry of replicas and
/// every stream record. See the file header for why.
///
/// # Arguments
///
/// * `disk` - The data directory the replica is about to be opened on.
/// * `proposed_replica_id` - The identity the configuration proposes for an empty directory.
/// * `metadata` - The cluster's metadata store.
///
/// # Errors
///
/// * [`DaemonError::IdentityAlreadyKnown`] if the directory is empty and the registry or a stream names
///   the proposed identity. Starting again cannot help: the configuration must propose a new identity.
/// * [`DaemonError::CannotStart`] if the identity file cannot be read, or if the directory is empty and
///   the metadata store cannot be asked. Starting again later can help.
pub(crate) async fn refuse_a_known_identity_on_an_empty_directory(
    disk: &dyn Disk,
    proposed_replica_id: &ReplicaId,
    metadata: &MetadataStore,
) -> Result<(), DaemonError> {
    let stored_identity = match disk.read_file(IDENTITY_FILE_PATH).await {
        Ok(stored_identity) => stored_identity,
        Err(io_error) => {
            return Err(DaemonError::cannot_start(
                "read the identity of the data directory",
                &io_error,
            ));
        }
    };
    if stored_identity.is_some() {
        return Ok(());
    }

    let check = format!("check that the identity {proposed_replica_id} is new to the cluster");
    let registered_replicas = match metadata.list_replicas().await {
        Ok(registered_replicas) => registered_replicas,
        Err(meta_error) => return Err(DaemonError::cannot_start(check, &meta_error)),
    };
    for registered_replica in &registered_replicas {
        if &registered_replica.replica_id == proposed_replica_id {
            return Err(DaemonError::IdentityAlreadyKnown {
                replica_id: proposed_replica_id.clone(),
                known_from: format!("it is registered at {}", registered_replica.address),
            });
        }
    }
    let streams = match metadata.list_streams().await {
        Ok(streams) => streams,
        Err(meta_error) => return Err(DaemonError::cannot_start(check, &meta_error)),
    };
    for stream in &streams {
        if let Some(known_from) = how_a_stream_names(stream, proposed_replica_id) {
            return Err(DaemonError::IdentityAlreadyKnown {
                replica_id: proposed_replica_id.clone(),
                known_from,
            });
        }
    }
    Ok(())
}

/// Says how `stream` names `replica_id`, if it does: as the holder of a copy of one of its segments, or
/// as a member of its standby replica set.
fn how_a_stream_names(stream: &StreamRecord, replica_id: &ReplicaId) -> Option<String> {
    for segment in &stream.segments {
        for copy in &segment.copies {
            if &copy.replica_id == replica_id {
                return Some(format!(
                    "stream {} lists it as a holder of segment {}",
                    stream.name, segment.segment_id
                ));
            }
        }
    }
    let standby_replicas = stream.standby_replicas.as_deref().unwrap_or_default();
    if standby_replicas.contains(replica_id) {
        return Some(format!("stream {} lists it in its standby replica set", stream.name));
    }
    None
}

#[cfg(test)]
#[path = "identity_test.rs"]
mod tests;
