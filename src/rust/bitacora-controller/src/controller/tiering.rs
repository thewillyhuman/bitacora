//! The controller's tiering passes: moving sealed segments to the object store and removing them from it.
//!
//! # Why this file exists
//!
//! Local disks are for recent data. A sealed segment is uploaded to the object store, which then serves as
//! the cold read path and as the backup: a cluster that loses every replica node keeps all data older than
//! the tiering delay (design, section 10, "Tiering"). Somebody has to decide when a segment is uploaded,
//! when its local copies may go, and when its object is deleted. That is the controller, and this file is
//! that part of its pass.
//!
//! # What it contains
//!
//! - `Controller::tier_sealed_segments`: has a replica upload each segment that is ready, and records
//!   where it went.
//! - `Controller::release_tiered_segments`: after the hot-read window, leaves a segment to the object
//!   store alone.
//! - `Controller::delete_tiered_segments_below_the_mark`: deletes the objects of truncated segments, then
//!   their records.
//! - `Controller::delete_objects_of_stream`: deletes what a deleted stream left in the object store.
//! - [`tiered_object_key`]: where a segment's object lives.
//!
//! # How it works
//!
//! A segment's life in the object store has three steps, each one conditional write to the metadata store
//! after the work it records is done and verified:
//!
//! 1. **Upload.** Once a segment has been sealed for the tiering delay and every copy is good, a replica
//!    that holds a good copy uploads it and reads the object back. The controller then checks that it sees
//!    the object itself, with the size the replica reported, and records the object in the segment's
//!    metadata. From then on readers may use it.
//! 2. **Release.** Once the hot-read window has passed since the upload, the controller checks that the
//!    object is still there and empties the segment's replica set. The leftover sweep at the end of the
//!    pass then removes the copies from the replicas.
//! 3. **Deletion.** When the low-water mark passes a tiered segment, the controller deletes the object
//!    first and removes the segment's record second. A controller that stops in between finds the record
//!    on its next pass and deletes again, which is harmless. No object is ever orphaned, and no listing of
//!    the object store is needed to find garbage.
//!
//! # Invariants
//!
//! - A segment's metadata never points at an object the controller has not seen in the object store.
//! - Local copies are released only while the object is present with its recorded size.
//! - An object is deleted only once the stream's low-water mark is past its segment, or its stream is
//!   deleted.
//! - Every step can be repeated, interrupted, or run by two controllers at once.
//!
//! # See also
//!
//! - `../controller.rs`: the pass these steps are part of, and truncation, which advances the mark.
//! - `bitacora-replica/src/upload.rs`: what a replica does when asked to upload.
//! - `bitacora-client/src/tiered.rs`: how readers use the object.

use std::time::Duration;

use bitacora_artifact::ArtifactStore;
use bitacora_meta::{CopyState, MetaError, ReplicaRecord, SegmentRecord, StreamRecord, TieredRecord};
use bitacora_types::replica_api::UploadRequest;
use bitacora_types::{SegmentId, SequenceNumber, StreamId, Timestamp};
use tokio::time::timeout;

use super::{Controller, PassReport, Survey};

/// The key of the object that holds a tiered segment.
///
/// The segment identifier is padded so that the objects of a stream list in segment order and so that no
/// key is a prefix of another.
///
/// # Arguments
///
/// * `stream_id` - The stream the segment belongs to.
/// * `segment_id` - The segment's position within the stream.
///
/// # Returns
///
/// The key, for example `streams/7/segments/00000000000000000003`.
pub(super) fn tiered_object_key(stream_id: StreamId, segment_id: SegmentId) -> String {
    format!("{}{:020}", tiered_objects_prefix(stream_id), segment_id.get())
}

/// The key prefix of every tiered segment of a stream.
fn tiered_objects_prefix(stream_id: StreamId) -> String {
    format!("streams/{stream_id}/segments/")
}

/// The key prefix of everything the materializers of a stream wrote: their layers, artifacts and
/// manifests. The materializer framework owns the layout below it (`docs/protocol/object-store.md`); the
/// controller only needs the prefix, to remove what a deleted stream leaves behind.
fn materializer_objects_prefix(stream_id: StreamId) -> String {
    format!("materializers/{stream_id}/")
}

/// Whether a segment should be uploaded now.
///
/// A segment is uploaded once it is sealed, holds records, is still above the low-water mark, has a full
/// set of good copies, and has been sealed for at least `tiering_delay`.
fn is_ready_to_tier(stream: &StreamRecord, segment: &SegmentRecord, now: Timestamp, tiering_delay: Duration) -> bool {
    let Some(seal) = segment.sealed else {
        return false;
    };
    if segment.tiered.is_some() {
        return false;
    }
    let holds_records = seal.end >= segment.first_sequence;
    let is_above_the_mark = seal.end >= stream.low_water_mark;
    let mut good_copies = 0;
    for copy in &segment.copies {
        if copy.state == CopyState::Good {
            good_copies += 1;
        }
    }
    let is_fully_replicated = good_copies == stream.replication.replica_count();
    let has_waited = now.duration_since(seal.at) >= tiering_delay;
    holds_records && is_above_the_mark && is_fully_replicated && has_waited
}

impl Controller {
    /// Uploads every segment of the stream that is ready for the object store (step 1 of the file header).
    ///
    /// Does nothing without an object store. Stops once the pass has uploaded
    /// `max_uploads_per_pass` segments; the rest wait for the next pass.
    ///
    /// # Errors
    ///
    /// Fails if the metadata store cannot be read or written. A failed upload is logged and retried on
    /// the next pass.
    pub(super) async fn tier_sealed_segments(
        &self,
        stream_name: &str,
        survey: &Survey,
        report: &mut PassReport,
    ) -> Result<(), MetaError> {
        let Some(artifact_store) = self.artifact_store.clone() else {
            return Ok(());
        };
        // Read again: reconciliation and repair before this may have changed which copies are good.
        let stream = self.metadata.get_stream(stream_name).await?;
        for segment in &stream.segments {
            if report.segments_tiered >= self.settings.max_uploads_per_pass {
                break;
            }
            if !is_ready_to_tier(&stream, segment, survey.now, self.settings.tiering_delay) {
                continue;
            }
            let Some(source_replica) = source_replica_of(segment, survey) else {
                continue;
            };
            let uploaded = self
                .upload_segment(&stream, segment, source_replica, artifact_store.as_ref())
                .await;
            let Some(tiered) = uploaded else {
                continue;
            };
            self.record_tiered_segment(stream_name, segment.segment_id, &tiered)
                .await?;
            tracing::info!(
                stream = stream_name,
                segment = %segment.segment_id,
                object_key = %tiered.object_key,
                size_bytes = tiered.size_bytes,
                "tiered a segment to the object store"
            );
            report.segments_tiered += 1;
        }
        Ok(())
    }

    /// Has `source_replica` upload its copy of `segment` and confirms that the object is in the store.
    ///
    /// # Returns
    ///
    /// What to record about the object, or `None` if the upload failed or the object is not where the
    /// replica said it put it. Both are logged.
    async fn upload_segment(
        &self,
        stream: &StreamRecord,
        segment: &SegmentRecord,
        source_replica: &ReplicaRecord,
        artifact_store: &dyn ArtifactStore,
    ) -> Option<TieredRecord> {
        let sealed_end = segment.sealed_end()?;
        let object_key = tiered_object_key(stream.stream_id, segment.segment_id);
        let request = UploadRequest {
            segment: segment.descriptor(stream.stream_id),
            sealed_end,
            object_key: object_key.clone(),
        };
        let client = self
            .connector
            .connect(&source_replica.replica_id, &source_replica.address);
        let answer = timeout(self.settings.upload_timeout, client.upload_segment(request)).await;
        let uploaded = match answer {
            Ok(Ok(uploaded)) => uploaded,
            Ok(Err(replica_error)) => {
                tracing::warn!(replica = %source_replica.replica_id, %object_key, %replica_error, "upload failed");
                return None;
            }
            Err(_elapsed) => {
                tracing::warn!(replica = %source_replica.replica_id, %object_key, "upload timed out");
                return None;
            }
        };
        // The replica verified the object against its own view of the store. Make sure this controller,
        // which will later have to delete the object, sees the same store.
        if !self
            .is_object_stored(artifact_store, &object_key, uploaded.size_bytes)
            .await
        {
            tracing::error!(
                replica = %source_replica.replica_id,
                %object_key,
                "the replica reports an upload that this controller cannot find; \
                 they may be configured with different object stores"
            );
            return None;
        }
        Some(TieredRecord {
            object_key,
            size_bytes: uploaded.size_bytes,
            uploaded_at: self.clock.now(),
            are_local_copies_dropped: false,
        })
    }

    /// Records in the metadata store that `segment_id` now also lives in the object store, unless the
    /// segment was deleted or recorded meanwhile.
    async fn record_tiered_segment(
        &self,
        stream_name: &str,
        segment_id: SegmentId,
        tiered: &TieredRecord,
    ) -> Result<(), MetaError> {
        self.metadata
            .update_stream(stream_name, |record| {
                for listed_segment in &mut record.segments {
                    if listed_segment.segment_id == segment_id && listed_segment.tiered.is_none() {
                        listed_segment.tiered = Some(tiered.clone());
                    }
                }
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Leaves every tiered segment whose hot-read window is over to the object store alone (step 2 of the
    /// file header).
    ///
    /// The segment's replica set is emptied in the metadata store; the leftover sweep at the end of the
    /// pass removes the copies from the replicas. A segment whose object is missing or has another size
    /// keeps its copies, and the problem is logged.
    ///
    /// # Errors
    ///
    /// Fails if the metadata store cannot be read or written.
    pub(super) async fn release_tiered_segments(
        &self,
        stream_name: &str,
        now: Timestamp,
        report: &mut PassReport,
    ) -> Result<(), MetaError> {
        let Some(artifact_store) = self.artifact_store.clone() else {
            return Ok(());
        };
        let stream = self.metadata.get_stream(stream_name).await?;
        for segment in &stream.segments {
            let Some(tiered) = &segment.tiered else {
                continue;
            };
            let is_window_over = now.duration_since(tiered.uploaded_at) >= self.settings.hot_read_window;
            if tiered.are_local_copies_dropped || !is_window_over {
                continue;
            }
            if !self
                .is_object_stored(artifact_store.as_ref(), &tiered.object_key, tiered.size_bytes)
                .await
            {
                tracing::error!(
                    stream = stream_name,
                    segment = %segment.segment_id,
                    object_key = %tiered.object_key,
                    "cannot confirm the object of a tiered segment; its local copies are kept"
                );
                continue;
            }
            let seen_tiered = tiered.clone();
            self.metadata
                .update_stream(stream_name, |record| {
                    for listed_segment in &mut record.segments {
                        // Release only the upload that was just confirmed.
                        if listed_segment.segment_id == segment.segment_id
                            && listed_segment.tiered.as_ref() == Some(&seen_tiered)
                        {
                            listed_segment.copies.clear();
                            listed_segment.tiered = Some(TieredRecord {
                                are_local_copies_dropped: true,
                                ..seen_tiered.clone()
                            });
                        }
                    }
                    Ok(())
                })
                .await?;
            tracing::info!(
                stream = stream_name,
                segment = %segment.segment_id,
                "released the local copies of a tiered segment"
            );
            report.segments_released += 1;
        }
        Ok(())
    }

    /// Deletes every tiered segment that lies entirely below the low-water mark: its object first, its
    /// record second (step 3 of the file header).
    ///
    /// Truncation leaves such segments listed for this step. Without an object store nothing can be
    /// deleted; the segments stay listed and the problem is logged.
    ///
    /// # Errors
    ///
    /// Fails if the metadata store cannot be read or written. An object that cannot be deleted is logged
    /// and retried on the next pass.
    pub(super) async fn delete_tiered_segments_below_the_mark(
        &self,
        stream_name: &str,
        report: &mut PassReport,
    ) -> Result<(), MetaError> {
        let stream = self.metadata.get_stream(stream_name).await?;
        for segment in &stream.segments {
            let Some(tiered) = &segment.tiered else {
                continue;
            };
            if !is_below_the_mark(segment, stream.low_water_mark) {
                continue;
            }
            let Some(artifact_store) = &self.artifact_store else {
                tracing::error!(
                    stream = stream_name,
                    segment = %segment.segment_id,
                    "a tiered segment is due for deletion, but this controller has no object store"
                );
                continue;
            };
            if !self.delete_object(artifact_store.as_ref(), &tiered.object_key).await {
                continue;
            }
            let deleted_segment_id = segment.segment_id;
            self.metadata
                .update_stream(stream_name, |record| {
                    let low_water_mark = record.low_water_mark;
                    record.segments.retain(|listed_segment| {
                        listed_segment.segment_id != deleted_segment_id
                            || !is_below_the_mark(listed_segment, low_water_mark)
                    });
                    Ok(())
                })
                .await?;
            tracing::info!(stream = stream_name, segment = %segment.segment_id, "deleted a tiered segment");
            report.segments_deleted += 1;
        }
        Ok(())
    }

    /// Deletes everything a deleted stream left in the object store: its tiered segments and what its
    /// materializers wrote.
    ///
    /// # Returns
    ///
    /// Whether the object store holds nothing of the stream any more. `true` without an object store:
    /// then this controller never tiered anything. `false` if the store could not be listed or an object
    /// could not be deleted; the stream must then be remembered and cleaned up on a later pass.
    pub(super) async fn delete_objects_of_stream(&self, stream_id: StreamId) -> bool {
        let Some(artifact_store) = &self.artifact_store else {
            return true;
        };
        let mut is_everything_deleted = true;
        for key_prefix in [tiered_objects_prefix(stream_id), materializer_objects_prefix(stream_id)] {
            if !self.delete_objects_under(artifact_store.as_ref(), &key_prefix).await {
                is_everything_deleted = false;
            }
        }
        is_everything_deleted
    }

    /// Deletes every object whose key starts with `key_prefix`. Returns whether none is left; `false` if
    /// the store could not be listed or an object could not be deleted, both of which are logged.
    async fn delete_objects_under(&self, artifact_store: &dyn ArtifactStore, key_prefix: &str) -> bool {
        let listing = timeout(self.settings.request_timeout, artifact_store.list_prefix(key_prefix)).await;
        let Ok(Ok(objects)) = listing else {
            tracing::warn!(%key_prefix, "cannot list the objects of a deleted stream");
            return false;
        };
        let mut is_everything_deleted = true;
        for object in &objects {
            if !self.delete_object(artifact_store, &object.object_key).await {
                is_everything_deleted = false;
            }
        }
        is_everything_deleted
    }

    /// Whether the object store holds `object_key` with exactly `size_bytes`. `false` also if the store
    /// could not be asked in time.
    async fn is_object_stored(&self, artifact_store: &dyn ArtifactStore, object_key: &str, size_bytes: u64) -> bool {
        let listing = timeout(self.settings.request_timeout, artifact_store.list_prefix(object_key)).await;
        let Ok(Ok(objects)) = listing else {
            return false;
        };
        for object in &objects {
            if object.object_key == object_key && object.size_bytes == size_bytes {
                return true;
            }
        }
        false
    }

    /// Deletes one object. Returns whether it is gone; a failure is logged.
    async fn delete_object(&self, artifact_store: &dyn ArtifactStore, object_key: &str) -> bool {
        let deleted = timeout(self.settings.request_timeout, artifact_store.delete_object(object_key)).await;
        match deleted {
            Ok(Ok(())) => true,
            Ok(Err(artifact_error)) => {
                tracing::warn!(%object_key, %artifact_error, "cannot delete an object");
                false
            }
            Err(_elapsed) => {
                tracing::warn!(%object_key, "deleting an object timed out");
                false
            }
        }
    }
}

/// Whether a sealed segment lies entirely below `low_water_mark`.
fn is_below_the_mark(segment: &SegmentRecord, low_water_mark: SequenceNumber) -> bool {
    match segment.sealed_end() {
        Some(sealed_end) => sealed_end < low_water_mark,
        None => false,
    }
}

/// Picks the replica that uploads `segment`: the first one that holds a good copy and answered this
/// pass's survey. `None` if no such replica is registered.
fn source_replica_of<'survey>(segment: &SegmentRecord, survey: &'survey Survey) -> Option<&'survey ReplicaRecord> {
    for copy in &segment.copies {
        let answered_the_survey = survey.copies_by_replica.contains_key(&copy.replica_id);
        if copy.state != CopyState::Good || !answered_the_survey {
            continue;
        }
        if let Some(registration) = survey.registration(&copy.replica_id) {
            return Some(registration);
        }
    }
    None
}

#[cfg(test)]
#[path = "tiering_test.rs"]
mod tests;
