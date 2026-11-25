//! The replica's registration in the metadata store, and its renewal.
//!
//! # Why this file exists
//!
//! Nobody is told where the replicas of a cluster are. Each replica registers itself in the metadata
//! store with its address, its failure domains and its free space, and renews that registration on a
//! timer (design, section 7, "Capacity and admission"). Writers and readers find replicas there, the
//! controller places new segments on the ones that renewed recently and have room, and it gives up the
//! copies of one that has been silent for long. The replica library knows nothing of the metadata
//! store; registering is the daemon's job, and this file does it.
//!
//! # What it contains
//!
//! - [`Registration`]: what a replica registers, and the loop that keeps it registered.
//! - [`RegistrationError`]: why one renewal failed.
//!
//! # How it works
//!
//! One renewal asks the replica for its status (identity, disk space, drain state) and for the copies
//! it holds, updates the daemon's gauges from the answers, and writes the registration record with the
//! current time as its heartbeat. The record is written whole each time, so a renewal is also how a
//! changed address or a drain reaches the metadata store.
//!
//! [`Registration::run`] renews at once and then after every interval, forever. A renewal that fails is
//! counted, logged and tried again after the next interval: the metadata store being away must not stop
//! a replica from serving what it has. The first renewal that succeeds marks the daemon ready.
//!
//! The registration is never removed here, not even on a clean shutdown. A replica that restarts for an
//! upgrade comes back with the same identity and the same data; removing its registration would tell
//! the controller that its copies are gone and start re-replicating them. A replica that is gone for
//! good is removed by an operator, or given up by the controller after it has been silent for long.
//!
//! # Invariants
//!
//! - The registered identity is the one the replica reports, which is the identity of its data
//!   directory, never the one the configuration proposes.
//! - A failed renewal changes nothing in the metadata store.
//!
//! # See also
//!
//! - `daemon.rs`: starts the loop and stops it at shutdown.
//! - `bitacora-meta/src/placement.rs`: how the controller reads the registry.

use std::sync::Arc;
use std::time::Duration;

use bitacora_meta::{MetaError, MetadataStore, RECORD_SCHEMA_VERSION, ReplicaRecord};
use bitacora_process::Readiness;
use bitacora_types::Clock;
use bitacora_types::replica_api::{ReplicaApi, ReplicaError};
use thiserror::Error;
use tokio::time::sleep;

use crate::metrics::ReplicaDaemonMetrics;

/// Why one renewal of the registration failed.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub(crate) enum RegistrationError {
    /// The replica could not say what to register, for example because it is stopping.
    #[error("the replica cannot report its status: {0}")]
    Replica(#[from] ReplicaError),
    /// The record could not be written.
    #[error("the metadata store did not take the registration: {0}")]
    Metadata(#[from] MetaError),
}

/// What a replica registers about itself, and how.
pub(crate) struct Registration {
    /// Where the registration is written.
    pub metadata: MetadataStore,
    /// The replica that is registered. It is asked directly, not through the instrumented interface,
    /// so that the daemon's own questions are not counted as requests.
    pub replica: Arc<dyn ReplicaApi>,
    /// Where the heartbeat's time comes from.
    pub clock: Arc<dyn Clock>,
    /// The `host:port` others reach the replica at.
    pub advertised_address: String,
    /// Where the replica is, widest failure domain first.
    pub failure_domains: Vec<String>,
    /// The daemon's gauges and counters, which each renewal updates.
    pub metrics: ReplicaDaemonMetrics,
}

impl Registration {
    /// Writes the replica's registration once, with the current time as its heartbeat.
    ///
    /// Asks the replica for its status and its copies, updates the daemon's gauges, then writes the
    /// record. The gauges are updated even if the write fails.
    ///
    /// # Returns
    ///
    /// The record that was written.
    ///
    /// # Errors
    ///
    /// * [`RegistrationError::Replica`] if the replica cannot report its status.
    /// * [`RegistrationError::Metadata`] if the metadata store is unavailable. Nothing was changed.
    pub(crate) async fn renew(&self) -> Result<ReplicaRecord, RegistrationError> {
        let renewed = self.write_registration().await;
        match &renewed {
            Ok(_record) => self.metrics.registrations.add(1),
            Err(_registration_error) => self.metrics.registration_failures.add(1),
        }
        renewed
    }

    /// Does the work of [`Registration::renew`], without counting its outcome.
    async fn write_registration(&self) -> Result<ReplicaRecord, RegistrationError> {
        let status = self.replica.status().await?;
        let held_copies = self.replica.list_segments().await?;
        let mut damaged_copies = 0_u64;
        for copy in &held_copies {
            if copy.is_damaged {
                damaged_copies += 1;
            }
        }
        self.metrics.segment_copies.set_unsigned(held_copies.len() as u64);
        self.metrics.damaged_segment_copies.set_unsigned(damaged_copies);
        self.metrics.draining.set_flag(status.is_draining);

        let record = ReplicaRecord {
            schema_version: RECORD_SCHEMA_VERSION,
            replica_id: status.replica_id,
            address: self.advertised_address.clone(),
            failure_domains: self.failure_domains.clone(),
            capacity_bytes: status.capacity_bytes,
            free_bytes: status.free_bytes,
            is_draining: status.is_draining,
            heartbeat_at: self.clock.now(),
        };
        self.metadata.put_replica(&record).await?;
        Ok(record)
    }

    /// Keeps the replica registered: renews at once, then after every `interval`, until the future is
    /// dropped.
    ///
    /// # Arguments
    ///
    /// * `interval` - How long to wait between two renewals, whether the last one succeeded or not.
    /// * `readiness` - Set to ready after the first renewal that succeeds, and left alone afterwards:
    ///   a replica that loses the metadata store later still serves what it has.
    pub(crate) async fn run(&self, interval: Duration, readiness: &Readiness) {
        loop {
            match self.renew().await {
                Ok(record) => {
                    if !readiness.is_ready() {
                        tracing::info!(
                            replica_id = %record.replica_id,
                            address = %record.address,
                            "replica is registered"
                        );
                        readiness.set_ready(true);
                    }
                }
                Err(registration_error) => {
                    tracing::warn!(%registration_error, "registration was not renewed");
                }
            }
            sleep(interval).await;
        }
    }
}

#[cfg(test)]
#[path = "registration_test.rs"]
mod tests;
