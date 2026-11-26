//! The leftover sweep: removing segment copies that no record refers to any more.
//!
//! # Why this file exists
//!
//! The other steps of a pass change what the metadata store says and leave the replicas for later. Truncation
//! removes a segment from its stream's record; repair moves a copy to another replica; releasing a tiered segment
//! empties its replica set; deleting a stream removes the record altogether. In each case files stay behind on
//! replicas, and a replica that was away when it happened still has them when it returns. Without this step they
//! would fill the disks for ever. This file finds those copies and has their replicas delete them, and it is also
//! what finally forgets a deleted stream.
//!
//! # What it contains
//!
//! - `Controller::remove_leftover_copies`: the sweep, once per pass, over every replica that answered the survey.
//! - `is_leftover_copy`: the rule that decides whether a copy of a known stream may be deleted, as a pure function.
//! - `KnownStreams`: the streams the metadata store knows, and the ones it knows were deleted.
//!
//! # How it works
//!
//! The sweep lists the streams and the deleted streams after the survey was taken, so every copy the survey saw
//! belongs to a segment these records already know, or have already dropped. For every copy on every replica that
//! answered:
//!
//! - **Its stream was deleted**: the copy is a leftover.
//! - **Its stream is unknown**, neither listed nor recorded as deleted: the copy is left alone. It is what a metadata
//!   store restored from an older snapshot would be rebuilt from.
//! - **Its segment is listed**: the copy is a leftover if the segment is sealed and the replica is not in its replica
//!   set, which is what repair and release leave behind. A copy of an open segment is never touched.
//! - **Its segment is not listed**: the copy is a leftover if its segment is older than every listed one (retention
//!   deleted it), or if nothing the copy holds can be read any more because all of it is below the low-water mark.
//!   The second case finds the copies of a deleted segment while an older, tiered segment is still listed because
//!   its object could not be deleted yet. A copy of an unlisted segment that is newer than what is listed, and holds
//!   readable records, is left alone: a metadata store restored from an older snapshot must not cost the records
//!   written since.
//!
//! A deleted stream is forgotten, and its note removed, once every replica that could still hold a copy of it has
//! answered and no copy is left, and once its objects are gone from the object store. A replica that has been silent
//! for longer than `replica_lost_after` does not hold the forgetting up.
//!
//! # Invariants
//!
//! - A copy is deleted only if the metadata store positively knows it is not needed. Not knowing a stream or a
//!   segment is never a reason to delete.
//! - The sweep never deletes a copy of an open segment, and never a copy that a sealed segment's record lists.
//! - A replica that fails to delete is asked again by the next pass; nothing is recorded about the attempt.
//!
//! # See also
//!
//! - `truncation.rs`, `repair.rs`, `tiering.rs`: the steps that leave copies behind.
//! - `tiering.rs`: `delete_objects_of_stream`, which empties the object store of a deleted stream.
//! - `survey.rs`: which copies each replica holds.

use std::collections::{BTreeMap, BTreeSet};

use bitacora_meta::{DeletedStreamRecord, MetaError, ReplicaRecord, StreamRecord};
use bitacora_types::replica_api::SegmentStatus;
use bitacora_types::{ReplicaId, SegmentAddress, StreamId};

use super::{Controller, PassReport, Survey, replica_answer_within};

/// What the metadata store knows about streams when the sweep runs.
struct KnownStreams {
    /// The record of every stream that exists, by identifier.
    streams_by_id: BTreeMap<StreamId, StreamRecord>,
    /// The identifiers of the streams that were deleted and not yet forgotten.
    deleted_stream_ids: BTreeSet<StreamId>,
}

impl KnownStreams {
    /// Whether the copy `held` by `replica_id` may be deleted; see the file header for the cases.
    fn is_leftover(&self, replica_id: &ReplicaId, held: &SegmentStatus) -> bool {
        let stream_id = held.address.stream_id;
        if self.deleted_stream_ids.contains(&stream_id) {
            return true;
        }
        match self.streams_by_id.get(&stream_id) {
            Some(stream) => is_leftover_copy(stream, replica_id, held),
            // Not a stream the metadata store knows anything about: leave its copies alone.
            None => false,
        }
    }
}

impl Controller {
    /// Removes the copies that no segment record refers to any more, and forgets the deleted streams that are
    /// cleaned up.
    ///
    /// That is: copies of segments that retention deleted, copies that repair or release left behind, and
    /// everything of streams that were deleted. Copies of a stream the metadata store knows nothing about are left
    /// alone.
    ///
    /// # Arguments
    ///
    /// * `survey` - What every replica that answered holds.
    /// * `report` - Counts the copies removed.
    ///
    /// # Errors
    ///
    /// Fails if the streams or the deleted streams cannot be listed, or a deleted stream's note cannot be removed.
    /// A replica that fails to delete a copy is not an error; the next pass asks again.
    pub(super) async fn remove_leftover_copies(
        &self,
        survey: &Survey,
        report: &mut PassReport,
    ) -> Result<(), MetaError> {
        // Listed after the survey, so every copy the survey saw belongs to a segment these records already know or
        // have already dropped.
        let streams = self.metadata.list_streams().await?;
        let deleted_streams = self.metadata.list_deleted_streams().await?;
        let known_streams = known_streams_of(streams, &deleted_streams);

        let streams_with_copies_left = self.sweep_replicas(survey, &known_streams, report).await;
        self.forget_cleaned_streams(survey, deleted_streams, &streams_with_copies_left)
            .await
    }

    /// Asks every replica that answered the survey to delete its leftover copies, replica by replica and copy by
    /// copy.
    ///
    /// # Returns
    ///
    /// The streams of which a leftover copy could not be deleted, and so still exists.
    async fn sweep_replicas(
        &self,
        survey: &Survey,
        known_streams: &KnownStreams,
        report: &mut PassReport,
    ) -> BTreeSet<StreamId> {
        let mut streams_with_copies_left = BTreeSet::new();
        for (replica_id, held_copies) in &survey.copies_by_replica {
            let Some(replica) = survey.registration(replica_id) else {
                continue;
            };
            for (address, held) in held_copies {
                if !known_streams.is_leftover(replica_id, held) {
                    continue;
                }
                if self.delete_copy(replica, *address).await {
                    report.copies_removed += 1;
                } else {
                    streams_with_copies_left.insert(address.stream_id);
                }
            }
        }
        streams_with_copies_left
    }

    /// Forgets every deleted stream of which nothing is left: no copy on a replica that could hold one, and no
    /// object in the object store.
    ///
    /// # Arguments
    ///
    /// * `survey` - Which replicas answered, to know whether every one that matters was looked at.
    /// * `deleted_streams` - The notes of the deleted streams.
    /// * `streams_with_copies_left` - The streams of which the sweep could not delete a copy.
    ///
    /// # Errors
    ///
    /// Fails if a note cannot be removed from the metadata store.
    async fn forget_cleaned_streams(
        &self,
        survey: &Survey,
        deleted_streams: Vec<DeletedStreamRecord>,
        streams_with_copies_left: &BTreeSet<StreamId>,
    ) -> Result<(), MetaError> {
        let has_every_replica_answered = self.has_every_replica_that_matters_answered(survey);
        for deleted in deleted_streams {
            if has_every_replica_answered
                && !streams_with_copies_left.contains(&deleted.stream_id)
                && self.delete_objects_of_stream(deleted.stream_id).await
            {
                self.metadata.forget_deleted_stream(deleted.stream_id).await?;
                tracing::info!(stream = %deleted.name, "deleted stream is cleaned up");
            }
        }
        Ok(())
    }

    /// Whether every replica that could still hold a copy was looked at by the survey: each registered replica
    /// either answered, or has been silent for longer than `replica_lost_after` and is given up.
    fn has_every_replica_that_matters_answered(&self, survey: &Survey) -> bool {
        survey.registered_replicas.iter().all(|replica| {
            survey.has_answered(&replica.replica_id) || !replica.is_alive(survey.now, self.settings.replica_lost_after)
        })
    }

    /// Asks a replica to delete its copy of a segment.
    ///
    /// # Returns
    ///
    /// Whether the replica answered that the copy is gone. A refusal and a missing answer are both `false`; neither
    /// is logged, because the next pass simply asks again.
    async fn delete_copy(&self, replica: &ReplicaRecord, address: SegmentAddress) -> bool {
        let client = self.connect_to(replica);
        replica_answer_within(self.settings.request_timeout, client.delete_segment(address))
            .await
            .is_ok()
    }
}

/// Builds what the sweep knows about streams from the two listings of the metadata store.
fn known_streams_of(streams: Vec<StreamRecord>, deleted_streams: &[DeletedStreamRecord]) -> KnownStreams {
    let mut streams_by_id = BTreeMap::new();
    for stream in streams {
        streams_by_id.insert(stream.stream_id, stream);
    }
    let mut deleted_stream_ids = BTreeSet::new();
    for deleted in deleted_streams {
        deleted_stream_ids.insert(deleted.stream_id);
    }
    KnownStreams {
        streams_by_id,
        deleted_stream_ids,
    }
}

/// Whether a copy held by `replica_id` belongs to nothing in `stream` any more, and may be deleted.
///
/// # Arguments
///
/// * `stream` - The record of the stream the copy belongs to.
/// * `replica_id` - The replica that holds the copy.
/// * `held` - What the replica reported about the copy.
///
/// # Returns
///
/// For a copy of a listed segment: `true` if the segment is sealed and the replica is not in its replica set. For a
/// copy of a segment that is not listed: `true` if the segment is older than every listed one, or if everything the
/// copy holds is below the low-water mark. See the file header for why each case is safe.
fn is_leftover_copy(stream: &StreamRecord, replica_id: &ReplicaId, held: &SegmentStatus) -> bool {
    let listed_segment = stream
        .segments
        .iter()
        .find(|segment| segment.segment_id == held.address.segment_id);
    if let Some(segment) = listed_segment {
        // A sealed segment's copy on a replica outside its replica set was replaced by repair. An open segment is
        // never touched.
        return segment.sealed.is_some() && segment.copies.iter().all(|copy| &copy.replica_id != replica_id);
    }
    is_older_than_every_listed_segment(stream, held) || holds_only_truncated_records(stream, held)
}

/// Whether the copy's segment comes before the oldest segment the stream lists: retention deleted it.
///
/// Anything newer than what is listed is left alone; a metadata store restored from an older snapshot must not cost
/// the records written since. A stream that lists no segment has no oldest one, so this is `false` for it.
fn is_older_than_every_listed_segment(stream: &StreamRecord, held: &SegmentStatus) -> bool {
    match stream.segments.first() {
        Some(oldest_listed) => held.address.segment_id < oldest_listed.segment_id,
        None => false,
    }
}

/// Whether nothing the copy holds can be read any more: it ends below the stream's low-water mark.
///
/// This holds wherever the copy's segment stood in the list. It is what finds the copies of a deleted segment while
/// an older, tiered segment is still listed because its object could not be deleted yet. A copy that reports neither
/// a sealed end nor any records says nothing about what it holds, and is kept.
fn holds_only_truncated_records(stream: &StreamRecord, held: &SegmentStatus) -> bool {
    match held.sealed_end.or(held.local_end) {
        Some(end_of_the_copy) => end_of_the_copy < stream.low_water_mark,
        None => false,
    }
}

#[cfg(test)]
#[path = "leftovers_test.rs"]
mod tests;
