//! Repair: rebuilding the bad copies of sealed segments from a good one.
//!
//! # Why this file exists
//!
//! A sealed segment is meant to have as many good copies as its stream's replication asks for. Disks fail, nodes are
//! replaced, a replica misses the last records before a seal: reconciliation finds such copies and marks them bad,
//! and from that moment the segment survives one failure fewer than it should. This file is the step that restores
//! it, by having a replica rebuild the copy from a good one (design, section 10, "Repair"). It runs only on sealed
//! segments, which never change, so it never competes with appends.
//!
//! # What it contains
//!
//! - `Controller::repair_bad_copies`: the step, for one stream.
//! - `Controller::repair_target`: which replica rebuilds a bad copy.
//! - `Controller::record_repaired_copy`: the write that puts the rebuilt copy into the segment's replica set.
//! - `BadCopy`: the copy a repair is about.
//!
//! # How it works
//!
//! For every sealed segment with a bad copy:
//!
//! 1. A **source** is chosen: the first replica of the segment whose copy is good and that answered the survey.
//!    Without one, nothing can be done in this pass, and it is logged as an error: it means the segment has no
//!    reachable good copy.
//! 2. For each bad copy a **target** is chosen. If the replica that holds the bad copy still answers and is not
//!    draining, it rebuilds the copy where it is. Otherwise placement picks a new replica that keeps the segment
//!    spread across failure domains together with the copies that stay.
//! 3. The target is asked to repair. It pulls what it lacks from the source, verifying every batch, and answers once
//!    its copy is complete, flushed and sealed. That can take long, so it has its own time limit.
//! 4. On success the copy is recorded as good on the target, if the copy in the record is still the bad one the pass
//!    looked at. A copy that moved leaves its old file behind; the leftover sweep removes it.
//!
//! A pass rebuilds at most `max_repairs_per_pass` copies, so that it stays short and standby placement keeps up
//! while a failed node is re-replicated. The rest waits for the next pass.
//!
//! # Invariants
//!
//! - A copy is recorded as good only after its replica answered that the repair is complete.
//! - Repair never removes a copy. A target that rebuilds in place replaces its own file only once the new one is
//!   whole (that is the replica's rule), and a copy that moved is removed by the sweep, later.
//! - A failed repair changes nothing: the copy stays bad and the next pass tries again.
//!
//! # See also
//!
//! - `reconciliation.rs`: where copies are marked bad.
//! - `leftovers.rs`: removes the copy a repair moved away from.
//! - `bitacora-replica`: what a replica does when it is asked to repair a copy.
//! - `bitacora-meta/src/placement.rs`: `choose_additional_replica`.

use bitacora_meta::{
    CopyRecord, CopyState, MetaError, ReplicaRecord, SegmentRecord, StreamRecord, choose_additional_replica,
};
use bitacora_types::replica_api::RepairRequest;
use bitacora_types::{ReplicaId, SequenceNumber};

use super::{Controller, PassReport, Survey, replica_answer_within, unchanged_copy_mut};

/// One bad copy of a sealed segment, as the pass read it.
struct BadCopy<'pass> {
    /// The stream the segment belongs to.
    stream: &'pass StreamRecord,
    /// The sealed segment.
    segment: &'pass SegmentRecord,
    /// Where the segment ends.
    sealed_end: SequenceNumber,
    /// The bad copy's position in the segment's replica set.
    replica_index: usize,
}

impl Controller {
    /// Rebuilds bad copies of the stream's sealed segments from a good one (design, section 10, "Repair").
    ///
    /// A copy on a replica that still answers is rebuilt where it is; a copy on a lost replica moves to a replica
    /// that preserves the segment's spread across failure domains. Stops once the pass has rebuilt
    /// `max_repairs_per_pass` copies.
    ///
    /// # Arguments
    ///
    /// * `stream_name` - The stream. Its record is read again, because reconciliation before this may have changed
    ///   which copies are good.
    /// * `survey` - Which replicas answered and what is registered.
    /// * `report` - Counts the copies rebuilt, over the whole pass.
    ///
    /// # Errors
    ///
    /// Fails if the metadata store cannot be read or written. A repair that a replica refuses or does not finish in
    /// time is logged and retried on the next pass.
    pub(super) async fn repair_bad_copies(
        &self,
        stream_name: &str,
        survey: &Survey,
        report: &mut PassReport,
    ) -> Result<(), MetaError> {
        let stream = self.metadata.get_stream(stream_name).await?;
        for segment in &stream.segments {
            let Some(sealed_end) = segment.sealed_end() else {
                continue;
            };
            let bad_replica_indexes = indexes_of_bad_copies(segment);
            if bad_replica_indexes.is_empty() {
                continue;
            }
            let Some(source) = repair_source(&stream, segment, survey) else {
                continue;
            };
            for replica_index in bad_replica_indexes {
                if report.copies_repaired >= self.settings.max_repairs_per_pass {
                    return Ok(());
                }
                let bad_copy = BadCopy {
                    stream: &stream,
                    segment,
                    sealed_end,
                    replica_index,
                };
                self.repair_copy(&bad_copy, source, survey, report).await?;
            }
        }
        Ok(())
    }

    /// Has one bad copy rebuilt from `source` and records the result.
    ///
    /// # Arguments
    ///
    /// * `bad_copy` - The copy to rebuild.
    /// * `source` - A replica that holds a good copy of the segment.
    /// * `survey` - Which replicas answered and what is registered, to choose the target from.
    /// * `report` - Counts the copy if it was rebuilt.
    ///
    /// # Errors
    ///
    /// Fails if the metadata store cannot be written. No target, a refused repair and a repair that timed out are
    /// logged and leave the copy bad.
    async fn repair_copy(
        &self,
        bad_copy: &BadCopy<'_>,
        source: &ReplicaRecord,
        survey: &Survey,
        report: &mut PassReport,
    ) -> Result<(), MetaError> {
        let segment = bad_copy.segment;
        let Some(target) = self.repair_target(segment, bad_copy.replica_index, survey) else {
            return Ok(());
        };
        let request = RepairRequest {
            segment: segment.descriptor(bad_copy.stream.stream_id),
            sealed_end: bad_copy.sealed_end,
            source_replica_id: source.replica_id.clone(),
            source_address: source.address.clone(),
        };
        let client = self.connect_to(&target);
        let repaired = replica_answer_within(self.settings.repair_timeout, client.repair_segment(request)).await;
        if let Err(failure) = repaired {
            tracing::warn!(replica = %target.replica_id, reason = %failure, "repair failed");
            return Ok(());
        }
        self.record_repaired_copy(bad_copy, &target).await?;
        report.copies_repaired += 1;
        Ok(())
    }

    /// Picks the replica that rebuilds the bad copy at `replica_index`.
    ///
    /// # Arguments
    ///
    /// * `segment` - The segment with the bad copy.
    /// * `replica_index` - The bad copy's position in the segment's replica set.
    /// * `survey` - Which replicas answered and what is registered.
    ///
    /// # Returns
    ///
    /// The replica that holds the bad copy, if it answered the survey and is not draining: it rebuilds in place.
    /// Otherwise a new replica that keeps the segment spread across failure domains. `None` if placement finds no
    /// such replica, which is logged.
    fn repair_target(&self, segment: &SegmentRecord, replica_index: usize, survey: &Survey) -> Option<ReplicaRecord> {
        let bad_copy = &segment.copies[replica_index];
        if let Some(holder) = survey.registration(&bad_copy.replica_id)
            && survey.has_answered(&holder.replica_id)
            && !holder.is_draining
        {
            return Some(holder.clone());
        }
        let kept_replicas = replicas_of_the_other_copies(segment, replica_index);
        let placed = choose_additional_replica(
            &survey.registered_replicas,
            &kept_replicas,
            survey.now,
            self.settings.placement,
        );
        match placed {
            Ok(replica_id) => survey.registration(&replica_id).cloned(),
            Err(placement_error) => {
                tracing::warn!(segment = %segment.segment_id, %placement_error, "no replica to repair onto");
                None
            }
        }
    }

    /// Records the rebuilt copy as good on `target` in the segment's replica set (design, section 10, "Repair",
    /// step 4), unless the copy at that position changed since the pass read it.
    ///
    /// # Arguments
    ///
    /// * `bad_copy` - The copy that was rebuilt, as the pass read it.
    /// * `target` - The replica that now holds the complete copy.
    ///
    /// # Errors
    ///
    /// Fails if the metadata store cannot be written.
    async fn record_repaired_copy(&self, bad_copy: &BadCopy<'_>, target: &ReplicaRecord) -> Result<(), MetaError> {
        let segment_id = bad_copy.segment.segment_id;
        let seen_copy = &bad_copy.segment.copies[bad_copy.replica_index];
        self.metadata
            .update_stream(&bad_copy.stream.name, |record| {
                let recorded_copy = unchanged_copy_mut(record, segment_id, bad_copy.replica_index, seen_copy);
                if let Some(recorded_copy) = recorded_copy {
                    *recorded_copy = CopyRecord {
                        replica_id: target.replica_id.clone(),
                        state: CopyState::Good,
                    };
                }
                Ok(())
            })
            .await?;
        tracing::info!(
            stream = bad_copy.stream.name.as_str(),
            segment = %segment_id,
            replica = %target.replica_id,
            "rebuilt a copy of a sealed segment"
        );
        Ok(())
    }
}

/// Picks the replica that the bad copies of `segment` are rebuilt from: one that holds a good copy and answered the
/// survey. `None`, which is logged as an error, means the segment cannot be repaired in this pass.
fn repair_source<'survey>(
    stream: &StreamRecord,
    segment: &SegmentRecord,
    survey: &'survey Survey,
) -> Option<&'survey ReplicaRecord> {
    let source = survey.holder_of_a_good_copy(segment);
    if source.is_none() {
        tracing::error!(
            stream = stream.name.as_str(),
            segment = %segment.segment_id,
            "no reachable good copy to repair from"
        );
    }
    source
}

/// Lists the positions, in the segment's replica set, of the copies that are marked bad.
fn indexes_of_bad_copies(segment: &SegmentRecord) -> Vec<usize> {
    let mut bad_replica_indexes = Vec::new();
    for (replica_index, copy) in segment.copies.iter().enumerate() {
        if copy.state == CopyState::Bad {
            bad_replica_indexes.push(replica_index);
        }
    }
    bad_replica_indexes
}

/// Lists the replicas of every copy of `segment` except the one at `replaced_index`: the copies a new replica has
/// to be spread away from.
fn replicas_of_the_other_copies(segment: &SegmentRecord, replaced_index: usize) -> Vec<ReplicaId> {
    let mut kept_replicas = Vec::new();
    for (replica_index, copy) in segment.copies.iter().enumerate() {
        if replica_index != replaced_index {
            kept_replicas.push(copy.replica_id.clone());
        }
    }
    kept_replicas
}

#[cfg(test)]
#[path = "repair_test.rs"]
mod tests;
