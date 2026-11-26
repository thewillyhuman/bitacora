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
//! - `is_standby_usable`, `can_take_segments`: the rule that decides whether the standby a stream has can be used.
//! - `is_spread_wider`: the rule that decides whether a set chosen now would be better than the standby.
//!
//! # How it works
//!
//! A standby can be used if it names as many replicas as the stream's replication asks for, and each of them could
//! take a new segment right now: registered, alive within the liveness window, not draining, and not too full.
//!
//! A standby that can be used may still not be the one to keep. A set is chosen from the replicas that are eligible
//! at that moment, so one placed while a zone was away covers the zones that were left, and two of its replicas
//! share a zone. When the zone returns, nothing about that standby is wrong, and the stream's next segment would have
//! two of its three copies in one zone: a write quorum that one zone can take away. So every pass asks placement
//! what it would choose now, and replaces a standby that is spread over fewer failure domains than that. A standby
//! that is spread as widely stays, whichever replicas placement would name: only a wider spread is a reason to
//! change it.
//!
//! Placement is asked with the key of the stream's next segment, the one the standby is for. Replicas that are
//! equally spread and equally full are ordered by that key, so the standbys of different streams, and the successive
//! standbys of one stream, go to different replicas of a zone and no replica gets them all.
//!
//! In both cases the step writes the fresh set on the condition that the standby in the record is still the one the
//! step looked at. If a writer consumed the standby in between, or another controller placed one, the write changes
//! nothing, and the next pass looks again.
//!
//! When too few replicas are eligible, no standby is placed. That is not an error of the pass: it is logged, the
//! stream keeps the standby it has or none, and the next pass tries again.
//!
//! # Invariants
//!
//! - A standby is only ever replaced as a whole, and only if it is still the one that was judged.
//! - Placement is a pure function of the registry as surveyed; this step adds no randomness of its own.
//! - A standby is never replaced by a set of the same spread, so two passes over the same registry agree and the
//!   standby does not move back and forth. That holds also when placement would now name other replicas, because
//!   the key, the room or the registry changed: other replicas of the same spread are no reason.
//! - Placement is asked once per stream and pass. That is a few comparisons per registered replica; its cost on a
//!   cluster with many thousands of streams was not measured.
//!
//! # See also
//!
//! - `bitacora-meta/src/placement.rs`: how a replica set is chosen.
//! - `bitacora-meta/src/store/writers.rs`: `open_segment` and `roll_segment`, which consume the standby.
//! - `survey.rs`: where the registry comes from.

use std::collections::BTreeSet;

use bitacora_meta::{
    MetaError, PlacementPolicy, ReplicaRecord, StreamRecord, choose_replica_set, placement_key_of_segment, spread_of,
};
use bitacora_types::{ReplicaId, Timestamp};

use super::{Controller, PassReport, Survey};

impl Controller {
    /// Keeps one standby replica set placed for `stream`, so that a roll never waits on placement.
    ///
    /// Does nothing if the stream's standby can be used and is spread as widely as a set chosen now would be.
    /// Otherwise writes the set placement chooses from the surveyed registry, unless the standby changed since the
    /// pass listed the stream.
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
        let is_usable = is_standby_usable(stream, survey, self.settings.placement);
        let Some(chosen) = self.choose_standby(stream, survey, is_usable) else {
            return Ok(());
        };
        if is_usable && !is_spread_wider(&chosen, stream.standby_replicas.as_deref().unwrap_or_default(), survey) {
            return Ok(());
        }
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

    /// Asks placement for a replica set for the next segment of `stream`, from the surveyed registry, with the key of
    /// that segment.
    ///
    /// # Arguments
    ///
    /// * `stream` - The stream to place a segment for.
    /// * `survey` - The registry of replicas as surveyed, and the time of the pass.
    /// * `has_usable_standby` - Whether the stream has a standby it can roll to. A stream that has one loses nothing
    ///   when placement fails, so nothing is logged for it.
    ///
    /// # Returns
    ///
    /// The chosen replicas, or `None` if too few replicas are eligible.
    fn choose_standby(
        &self,
        stream: &StreamRecord,
        survey: &Survey,
        has_usable_standby: bool,
    ) -> Option<Vec<ReplicaId>> {
        let no_replica_excluded = BTreeSet::new();
        let placed = choose_replica_set(
            &survey.registered_replicas,
            stream.replication.replica_count(),
            survey.now,
            self.settings.placement,
            &no_replica_excluded,
            placement_key_of_segment(stream.stream_id, stream.next_segment_id),
        );
        match placed {
            Ok(chosen) => Some(chosen),
            Err(placement_error) => {
                if !has_usable_standby {
                    tracing::warn!(stream = %stream.name, %placement_error, "cannot place a standby");
                }
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

/// Whether the replica set `chosen` is spread over more failure domains than the set `standby`.
///
/// # Arguments
///
/// * `chosen` - What placement chooses from the surveyed registry.
/// * `standby` - The standby a stream has.
/// * `survey` - The registry both sets are looked up in. A replica that is not registered counts for nothing.
///
/// # Returns
///
/// `true` only if `chosen` covers more domains at some level and no fewer at every wider one; sets of the same
/// spread are not better than each other.
fn is_spread_wider(chosen: &[ReplicaId], standby: &[ReplicaId], survey: &Survey) -> bool {
    spread_of(&registrations_of(chosen, survey)) > spread_of(&registrations_of(standby, survey))
}

/// The registrations of the replicas of a set that are registered.
fn registrations_of<'survey>(replica_ids: &[ReplicaId], survey: &'survey Survey) -> Vec<&'survey ReplicaRecord> {
    let mut registrations = Vec::with_capacity(replica_ids.len());
    for replica_id in replica_ids {
        if let Some(registration) = survey.registration(replica_id) {
            registrations.push(registration);
        }
    }
    registrations
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
