//! Standby placement: keeping a replica set ready for every stream's next segment.
//!
//! # Why this file exists
//!
//! A writer that has to roll, because a replica failed or its segment is full, must not wait for somebody to choose
//! where the next segment goes: appends are paused for as long as that takes. So the choice is made in advance. Every
//! stream has a **standby**, a replica set recorded in its metadata that the next segment will use, and a roll is one
//! conditional write that consumes it (design, section 5). This file is the step that puts a standby there and keeps
//! it usable.
//!
//! # What it contains
//!
//! - `Controller::ensure_standby`: the step, for one stream, and `Controller::choose_standby`, which asks placement.
//! - `is_standby_usable`, `can_take_segments`: the rule that decides whether the standby a stream has may stay.
//!
//! # How it works
//!
//! A standby may stay if it names as many replicas as the stream's replication asks for, and each of them could
//! take a new segment right now: registered, alive within the liveness window, not draining, and not too full.
//! Otherwise the step asks placement for a fresh set and writes it, on the condition that the standby in the record
//! is still the one the step looked at. If a writer consumed the standby in between, or another controller placed
//! one, the write changes nothing, and the next pass looks again.
//!
//! When too few replicas are eligible, no standby is placed. That is not an error of the pass: it is logged, the
//! stream keeps the standby it has or none, and the next pass tries again.
//!
//! # Invariants
//!
//! - A standby is only ever replaced as a whole, and only if it is still the one that was judged.
//! - Placement is a pure function of the registry as surveyed; this step adds no randomness of its own.
//!
//! # See also
//!
//! - `bitacora-meta/src/placement.rs`: how a replica set is chosen.
//! - `bitacora-meta/src/store/writers.rs`: `open_segment` and `roll_segment`, which consume the standby.
//! - `survey.rs`: where the registry comes from.

use std::collections::BTreeSet;

use bitacora_meta::{MetaError, PlacementPolicy, ReplicaRecord, StreamRecord, choose_replica_set};
use bitacora_types::{ReplicaId, Timestamp};

use super::{Controller, PassReport, Survey};

impl Controller {
    /// Keeps one standby replica set placed for `stream`, so that a roll never waits on placement.
    ///
    /// Does nothing if the stream's standby is usable. Otherwise chooses a new one from the surveyed registry and
    /// writes it, unless the standby changed since the pass listed the stream.
    ///
    /// # Arguments
    ///
    /// * `stream` - The stream's record as the pass listed it.
    /// * `survey` - The registry of replicas as surveyed.
    /// * `report` - Counts the stream if a standby was placed.
    ///
    /// # Errors
    ///
    /// Fails if the metadata store cannot be written. Too few eligible replicas is not an error; see the file
    /// header.
    pub(super) async fn ensure_standby(
        &self,
        stream: &StreamRecord,
        survey: &Survey,
        report: &mut PassReport,
    ) -> Result<(), MetaError> {
        if is_standby_usable(stream, survey, self.settings.placement) {
            return Ok(());
        }
        let Some(chosen) = self.choose_standby(stream, survey) else {
            return Ok(());
        };
        let (updated, ()) = self
            .metadata
            .update_stream(&stream.name, |record| {
                // Only replace the standby this pass looked at: a writer may have consumed it meanwhile and another
                // pass placed a new one.
                if record.standby_replicas == stream.standby_replicas {
                    record.standby_replicas = Some(chosen.clone());
                }
                Ok(())
            })
            .await?;
        if updated.standby_replicas.as_ref() == Some(&chosen) {
            tracing::info!(stream = %stream.name, standby = ?chosen, "placed a standby segment");
            report.standbys_placed += 1;
        }
        Ok(())
    }

    /// Asks placement for a replica set for the next segment of `stream`, from the surveyed registry.
    ///
    /// # Returns
    ///
    /// The chosen replicas, or `None` if too few replicas are eligible, which is logged.
    fn choose_standby(&self, stream: &StreamRecord, survey: &Survey) -> Option<Vec<ReplicaId>> {
        let no_replica_excluded = BTreeSet::new();
        let placed = choose_replica_set(
            &survey.registered_replicas,
            stream.replication.replica_count(),
            survey.now,
            self.settings.placement,
            &no_replica_excluded,
        );
        match placed {
            Ok(chosen) => Some(chosen),
            Err(placement_error) => {
                tracing::warn!(stream = %stream.name, %placement_error, "cannot place a standby");
                None
            }
        }
    }
}

/// Whether the standby that `stream` has may stay: it is complete, and every replica in it could take a new segment
/// now.
///
/// # Arguments
///
/// * `stream` - The stream's record, with its standby if it has one.
/// * `survey` - The registry of replicas as surveyed, and the time of the pass.
/// * `policy` - Which replicas may receive new segments.
///
/// # Returns
///
/// `false` if the stream has no standby, if the standby names another number of replicas than the stream's
/// replication asks for, or if one of them is unregistered, not alive, draining or too full.
fn is_standby_usable(stream: &StreamRecord, survey: &Survey, policy: PlacementPolicy) -> bool {
    let Some(standby) = &stream.standby_replicas else {
        return false;
    };
    if standby.len() != stream.replication.replica_count() {
        return false;
    }
    standby
        .iter()
        .all(|replica_id| can_take_segments(survey.registration(replica_id), survey.now, policy))
}

/// Whether a replica could be given a new segment right now.
///
/// # Arguments
///
/// * `registration` - The replica's registration, or `None` if it is not registered.
/// * `now` - The time of the pass.
/// * `policy` - How recently a replica must have registered, and how full it may be.
///
/// # Returns
///
/// `true` if the replica is registered, alive, not draining and below the fullness at which new segments are
/// refused.
fn can_take_segments(registration: Option<&ReplicaRecord>, now: Timestamp, policy: PlacementPolicy) -> bool {
    let Some(replica) = registration else {
        return false;
    };
    replica.is_alive(now, policy.liveness_window)
        && !replica.is_draining
        && replica.used_percent() < policy.refuse_at_used_percent
}

#[cfg(test)]
#[path = "standby_test.rs"]
mod tests;
