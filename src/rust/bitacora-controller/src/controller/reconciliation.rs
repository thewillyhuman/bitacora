//! Reconciliation: comparing every copy of a sealed segment with what the metadata store says it should hold.
//!
//! # Why this file exists
//!
//! When a segment is sealed, the metadata store records where it ends, and that record is the truth. The replicas
//! may not agree yet: one was down when the writer announced the seal, one acknowledged fewer records than were
//! committed, one found a bad checksum when it scrubbed, one lost its disk. Nothing else in the system looks at
//! sealed copies again, so without this step a short or damaged copy would be counted as a full one for ever, and a
//! segment could end up with fewer real copies than anybody believes (design, section 8, "Reconciliation"). This
//! file is the step that looks, and writes down what it found.
//!
//! # What it contains
//!
//! - `Controller::reconcile_sealed_copies`: the step, for one stream.
//! - `Controller::judge_copy`: what one copy's state should be, from the survey and, if needed, from telling its
//!   replica the seal.
//! - `state_of_a_copy`: the rule itself, as a pure function.
//! - `Verdict`, `SealAnswer`: what the two halves of the step hand to each other.
//!
//! # How it works
//!
//! A copy of a sealed segment is in one of three states in the metadata store: unverified, good or bad. For every
//! copy the step reaches one of four conclusions:
//!
//! - **Its replica is not registered.** An operator removed it. The copy is bad.
//! - **Its replica did not answer the survey.** If the replica has been silent for longer than
//!   `replica_lost_after`, the copy is given up as bad. Otherwise nothing is concluded: a restart must not trigger
//!   repair.
//! - **Its replica answered and already knows the seal.** The copy is good if it ends exactly at the sealed end and
//!   is not damaged, bad otherwise.
//! - **Its replica answered and does not know the seal.** The step tells it. The replica's answer says where its copy
//!   ends once it has cut off anything above the sealed end, and the same rule applies. If the replica cannot be
//!   reached for this, nothing is concluded; if it refuses the seal, the copy is bad.
//!
//! A segment that was sealed empty holds no records, so a replica that never created a file for it is as complete as
//! one that did.
//!
//! The conclusions that differ from the recorded state are written in one conditional update. Each is applied only
//! if the copy at that position is still the one that was judged.
//!
//! # Invariants
//!
//! - A copy becomes good only on evidence from its own replica: an answer in this pass that it holds the segment up
//!   to the sealed end.
//! - A copy is never judged from another replica's word, and an open segment is never judged at all.
//! - Reconciliation never deletes or rebuilds anything. It only records; `repair.rs` acts on what it recorded.
//!
//! # See also
//!
//! - `survey.rs`: where "answered", "holds" and "registered" come from.
//! - `repair.rs`: what happens to a copy marked bad.
//! - `docs/protocol/replication.md`, "The controller".

use bitacora_meta::{CopyRecord, CopyState, MetaError, ReplicaRecord, SegmentRecord, StreamRecord};
use bitacora_types::replica_api::{ReplicaError, SealRequest};
use bitacora_types::{SegmentAddress, SegmentId, SequenceNumber};

use super::{Controller, PassReport, ReplicaFailure, Survey, replica_answer_within, unchanged_copy_mut};

/// A conclusion about one copy of a sealed segment: the state it should be recorded in, which is not the state it
/// is recorded in now.
struct Verdict {
    /// The segment the copy belongs to.
    segment_id: SegmentId,
    /// The copy's position in the segment's replica set.
    replica_index: usize,
    /// The copy as it was when it was judged. The verdict applies to this copy and to no other.
    seen_copy: CopyRecord,
    /// The state the copy should be recorded in.
    state: CopyState,
}

/// What came of telling a replica that a segment is sealed.
enum SealAnswer {
    /// The replica recorded the seal. Its copy now ends here; `None` if it holds no records of the segment.
    Sealed(Option<SequenceNumber>),
    /// The replica could not be reached, so nothing is known about its copy.
    Unknown,
    /// The replica answered and refused: its copy cannot be the segment that was sealed.
    Refused,
}

impl Controller {
    /// Checks every copy of every sealed segment of `stream` against what its replica holds, and records the copies
    /// whose state turns out to be different (design, section 8, "Reconciliation").
    ///
    /// A copy that is complete becomes good. One that is short, damaged, or on a replica given up for lost becomes
    /// bad, and is later replaced by repair. Replicas that missed a seal are told it on the way.
    ///
    /// # Arguments
    ///
    /// * `stream` - The stream's record as the pass listed it.
    /// * `survey` - What the replicas hold, as surveyed.
    /// * `report` - Counts the copies confirmed good and the copies marked bad.
    ///
    /// # Errors
    ///
    /// Fails if the metadata store cannot be written. A replica that cannot be reached is not an error: its copies
    /// are judged by a later pass.
    pub(super) async fn reconcile_sealed_copies(
        &self,
        stream: &StreamRecord,
        survey: &Survey,
        report: &mut PassReport,
    ) -> Result<(), MetaError> {
        let verdicts = self.judge_sealed_copies(stream, survey).await;
        if verdicts.is_empty() {
            return Ok(());
        }
        self.metadata
            .update_stream(&stream.name, |record| {
                for verdict in &verdicts {
                    // Apply the verdict only to the copy it was reached on.
                    let judged_copy =
                        unchanged_copy_mut(record, verdict.segment_id, verdict.replica_index, &verdict.seen_copy);
                    if let Some(judged_copy) = judged_copy {
                        judged_copy.state = verdict.state;
                    }
                }
                Ok(())
            })
            .await?;
        for verdict in &verdicts {
            match verdict.state {
                CopyState::Good => report.copies_confirmed += 1,
                CopyState::Bad => report.copies_marked_bad += 1,
                CopyState::Unverified => {}
            }
        }
        Ok(())
    }

    /// Judges every copy of every sealed segment of `stream`, in the order of the record, and returns the verdicts
    /// that differ from what is recorded.
    async fn judge_sealed_copies(&self, stream: &StreamRecord, survey: &Survey) -> Vec<Verdict> {
        let mut verdicts = Vec::new();
        for segment in &stream.segments {
            let Some(sealed_end) = segment.sealed_end() else {
                continue;
            };
            for (replica_index, copy) in segment.copies.iter().enumerate() {
                let judged = self.judge_copy(stream, segment, sealed_end, copy, survey).await;
                if let Some(state) = judged
                    && state != copy.state
                {
                    verdicts.push(Verdict {
                        segment_id: segment.segment_id,
                        replica_index,
                        seen_copy: copy.clone(),
                        state,
                    });
                }
            }
        }
        verdicts
    }

    /// Decides what state a copy of a sealed segment should be in.
    ///
    /// # Arguments
    ///
    /// * `stream` - The stream the segment belongs to.
    /// * `segment` - The sealed segment.
    /// * `sealed_end` - Where the metadata store says the segment ends.
    /// * `copy` - The copy to judge: the replica that holds it and its recorded state.
    /// * `survey` - What the replicas hold, as surveyed.
    ///
    /// # Returns
    ///
    /// The state the copy should be in, or `None` if that cannot be known right now: its replica did not answer and
    /// has not been silent for long enough to be given up.
    async fn judge_copy(
        &self,
        stream: &StreamRecord,
        segment: &SegmentRecord,
        sealed_end: SequenceNumber,
        copy: &CopyRecord,
        survey: &Survey,
    ) -> Option<CopyState> {
        let Some(registration) = survey.registration(&copy.replica_id) else {
            // The operator removed the replica: its copies are gone.
            return Some(CopyState::Bad);
        };
        let Some(held_copies) = survey.copies_held_by(&copy.replica_id) else {
            // The replica did not answer. Give its copies up only after it has been silent for long; a restart must
            // not trigger repair.
            let is_lost = !registration.is_alive(survey.now, self.settings.replica_lost_after);
            return if is_lost { Some(CopyState::Bad) } else { None };
        };

        let address = segment.address(stream.stream_id);
        let held = held_copies.get(&address);
        let knows_the_seal = held.is_some_and(|status| status.sealed_end == Some(sealed_end));
        let local_end = if knows_the_seal {
            held.and_then(|status| status.local_end)
        } else {
            // Tell the replica; its answer says what it holds afterwards.
            match self.tell_the_seal(registration, address, sealed_end).await {
                SealAnswer::Sealed(local_end) => local_end,
                SealAnswer::Unknown => return None,
                SealAnswer::Refused => return Some(CopyState::Bad),
            }
        };
        let is_damaged = held.is_some_and(|status| status.is_damaged);
        Some(state_of_a_copy(segment, sealed_end, local_end, is_damaged))
    }

    /// Tells a replica that a segment is sealed, and reports what its copy holds afterwards.
    ///
    /// # Arguments
    ///
    /// * `replica` - The registration of the replica to tell.
    /// * `address` - The segment.
    /// * `sealed_end` - Where the metadata store says the segment ends.
    ///
    /// # Returns
    ///
    /// [`SealAnswer::Sealed`] with the end of the replica's copy if it recorded the seal; [`SealAnswer::Unknown`] if
    /// it was unavailable or did not answer in time; [`SealAnswer::Refused`] if it answered with any other error,
    /// which is logged.
    async fn tell_the_seal(
        &self,
        replica: &ReplicaRecord,
        address: SegmentAddress,
        sealed_end: SequenceNumber,
    ) -> SealAnswer {
        let client = self.connect_to(replica);
        let request = SealRequest {
            segment: address,
            sealed_end,
        };
        match replica_answer_within(self.settings.request_timeout, client.seal(request)).await {
            Ok(sealed) => SealAnswer::Sealed(sealed.local_end),
            Err(ReplicaFailure::Refused(ReplicaError::Unavailable { .. }) | ReplicaFailure::TimedOut) => {
                SealAnswer::Unknown
            }
            Err(ReplicaFailure::Refused(replica_error)) => {
                tracing::warn!(
                    segment = %address,
                    replica = %replica.replica_id,
                    %replica_error,
                    "copy refuses its seal"
                );
                SealAnswer::Refused
            }
        }
    }
}

/// The state of a copy whose replica knows the seal: good if it holds the whole segment undamaged, bad otherwise.
///
/// # Arguments
///
/// * `segment` - The sealed segment the copy belongs to.
/// * `sealed_end` - Where the segment ends.
/// * `local_end` - Where the replica's copy ends, or `None` if the replica holds no records of the segment.
/// * `is_damaged` - Whether the replica reported the copy as damaged.
///
/// # Returns
///
/// [`CopyState::Good`] if the copy ends exactly at the sealed end and is not damaged, [`CopyState::Bad`] otherwise.
/// A replica without records counts as ending just before the segment's first record, so it holds all of a segment
/// that was sealed empty.
fn state_of_a_copy(
    segment: &SegmentRecord,
    sealed_end: SequenceNumber,
    local_end: Option<SequenceNumber>,
    is_damaged: bool,
) -> CopyState {
    // A segment sealed empty holds nothing, so a replica that never created a file for it is as complete as one
    // that did.
    let end_of_an_empty_copy = segment.first_sequence.preceding();
    let is_complete = local_end.unwrap_or(end_of_an_empty_copy) == sealed_end;
    if is_complete && !is_damaged {
        CopyState::Good
    } else {
        CopyState::Bad
    }
}

#[cfg(test)]
#[path = "reconciliation_test.rs"]
mod tests;
