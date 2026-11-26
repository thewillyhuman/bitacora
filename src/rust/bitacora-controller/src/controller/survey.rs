//! The survey: what a pass learns about the replicas before it decides anything.
//!
//! # Why this file exists
//!
//! Almost every step of a pass needs to know two things about a replica: is it registered and alive, and which
//! segment copies does it hold, in what condition. Asking each replica again in each step would be slow, and worse,
//! two steps of one pass could then act on two different pictures of the same replica. So a pass asks once, at its
//! start, and every step works from that one picture. This file takes the picture and answers the questions the
//! steps put to it.
//!
//! # What it contains
//!
//! - [`Survey`]: the registry as read, the time it was read at, and what every replica that answered holds.
//! - `Controller::survey`: takes one.
//! - [`Survey::registration`], [`Survey::has_answered`], [`Survey::copies_held_by`],
//!   [`Survey::holder_of_a_good_copy`]: the questions.
//!
//! # How it works
//!
//! The registry is read from the metadata store. Every replica whose registration is within the placement policy's
//! liveness window is asked to list its segment copies, one after the other, each with the request time limit. A
//! replica that does not answer, or answers with an error, is simply absent from the answers: the steps tell a
//! replica that **is registered but did not answer** from one that **answered that it holds nothing**, because the
//! two mean very different things for the copies it is supposed to hold.
//!
//! A replica whose registration has lapsed is not asked at all. It may still be running, cut off from the metadata
//! store; whether its copies are given up is decided by reconciliation, from how long it has been silent.
//!
//! # Invariants
//!
//! - A survey is never refreshed. A replica that comes back in the middle of a pass is seen by the next pass.
//! - The time of the survey is the time of the pass: every step that compares against the clock uses `Survey::now`,
//!   so a pass that takes long does not judge its first stream and its last by different times.
//!
//! # See also
//!
//! - `../controller.rs`: the pass, which takes the survey first.
//! - `reconciliation.rs`, `repair.rs`, `tiering.rs`, `leftovers.rs`: the steps that read it.

use std::collections::BTreeMap;

use bitacora_meta::{CopyState, MetaError, ReplicaRecord, SegmentRecord};
use bitacora_types::replica_api::SegmentStatus;
use bitacora_types::{ReplicaId, SegmentAddress, Timestamp};

use super::{Controller, replica_answer_within};

/// What the controller learned about the replicas at the start of a pass.
pub(super) struct Survey {
    /// When the survey was taken. It is the time of the whole pass.
    pub(super) now: Timestamp,
    /// Every registration in the metadata store, alive or not.
    pub(super) registered_replicas: Vec<ReplicaRecord>,
    /// The segment copies of every replica that answered, by replica and then by segment. A replica that is
    /// registered but missing here did not answer, or was not asked because its registration had lapsed.
    pub(super) copies_by_replica: BTreeMap<ReplicaId, BTreeMap<SegmentAddress, SegmentStatus>>,
}

impl Survey {
    /// Looks up the registration of a replica.
    ///
    /// # Arguments
    ///
    /// * `replica_id` - The replica to look up.
    ///
    /// # Returns
    ///
    /// The registration, or `None` if no replica is registered under this identity: an operator removed it, or it
    /// never existed.
    pub(super) fn registration(&self, replica_id: &ReplicaId) -> Option<&ReplicaRecord> {
        self.registered_replicas
            .iter()
            .find(|replica| &replica.replica_id == replica_id)
    }

    /// Whether a replica answered the survey, and can therefore be asked for more in this pass.
    ///
    /// # Arguments
    ///
    /// * `replica_id` - The replica in question.
    ///
    /// # Returns
    ///
    /// `true` if the replica listed its copies when the survey was taken.
    pub(super) fn has_answered(&self, replica_id: &ReplicaId) -> bool {
        self.copies_by_replica.contains_key(replica_id)
    }

    /// Returns what a replica said it holds.
    ///
    /// # Arguments
    ///
    /// * `replica_id` - The replica in question.
    ///
    /// # Returns
    ///
    /// Its segment copies by segment, possibly none, or `None` if the replica did not answer the survey. The two
    /// are not the same: an empty answer means its copies are gone, no answer means nothing is known.
    pub(super) fn copies_held_by(&self, replica_id: &ReplicaId) -> Option<&BTreeMap<SegmentAddress, SegmentStatus>> {
        self.copies_by_replica.get(replica_id)
    }

    /// Picks a replica to read a sealed segment from: the first one in the segment's replica set whose copy is good
    /// and that answered this survey.
    ///
    /// Repair uses it as the source of a rebuild, tiering as the replica that uploads.
    ///
    /// # Arguments
    ///
    /// * `segment` - The segment's record, with the states of its copies.
    ///
    /// # Returns
    ///
    /// The replica's registration, or `None` if no good copy is on a replica that can be asked right now.
    pub(super) fn holder_of_a_good_copy(&self, segment: &SegmentRecord) -> Option<&ReplicaRecord> {
        for copy in &segment.copies {
            if copy.state != CopyState::Good || !self.has_answered(&copy.replica_id) {
                continue;
            }
            if let Some(registration) = self.registration(&copy.replica_id) {
                return Some(registration);
            }
        }
        None
    }
}

impl Controller {
    /// Reads the replica registry and asks every live replica what it holds.
    ///
    /// Makes one request per live replica, one after the other, each bounded by the request time limit. A replica
    /// that fails to answer is logged at debug level and left out of the answers.
    ///
    /// # Returns
    ///
    /// The survey, stamped with the time it was started at.
    ///
    /// # Errors
    ///
    /// Fails if the replica registry cannot be read.
    pub(super) async fn survey(&self) -> Result<Survey, MetaError> {
        let now = self.clock.now();
        let registered_replicas = self.metadata.list_replicas().await?;
        let mut copies_by_replica = BTreeMap::new();
        for replica in &registered_replicas {
            if !replica.is_alive(now, self.settings.placement.liveness_window) {
                continue;
            }
            let client = self.connect_to(replica);
            match replica_answer_within(self.settings.request_timeout, client.list_segments()).await {
                Ok(statuses) => {
                    copies_by_replica.insert(replica.replica_id.clone(), copies_by_segment(statuses));
                }
                Err(failure) => {
                    tracing::debug!(replica = %replica.replica_id, reason = %failure, "survey failed");
                }
            }
        }
        Ok(Survey {
            now,
            registered_replicas,
            copies_by_replica,
        })
    }
}

/// Indexes what one replica listed by the segment each copy belongs to.
fn copies_by_segment(statuses: Vec<SegmentStatus>) -> BTreeMap<SegmentAddress, SegmentStatus> {
    let mut copies = BTreeMap::new();
    for status in statuses {
        copies.insert(status.address, status);
    }
    copies
}

#[cfg(test)]
#[path = "survey_test.rs"]
mod tests;
