//! The controller's passes over the cluster (design, section 8).
//!
//! The controller holds no state of its own and is never on the append
//! path. Everything it does is a pass: read the metadata store, look at the
//! replicas, and make conditional writes that bring the two closer to what
//! they should be. A pass that is interrupted, repeated, or run by two
//! controllers at once does no harm, because every write is conditional on
//! what the pass saw.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use bitacora_artifact::ArtifactStore;
use bitacora_meta::{
    CheckpointRecord, CopyRecord, CopyState, MetaError, MetadataStore, PlacementPolicy,
    ReplicaRecord, SegmentRecord, StreamRecord, choose_additional_replica, choose_replica_set,
};
use bitacora_types::replica_api::{
    RepairRequest, ReplicaConnector, ReplicaError, SealRequest, SegmentStatus,
};
use bitacora_types::{
    Clock, ReplicaId, SegmentAddress, SegmentId, SequenceNumber, StreamId, Timestamp,
};
use tokio::time::{sleep, timeout};

mod tiering;

/// How the controller paces itself and judges replicas.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControllerSettings {
    /// A name for this controller instance, recorded with its leadership.
    pub identity: String,
    /// How long the controller rests between two passes.
    pub pass_interval: Duration,
    /// How long leadership lasts without renewal.
    pub leadership_duration: Duration,
    /// Which replicas may receive new segments.
    pub placement: PlacementPolicy,
    /// How long one request to a replica may take.
    pub request_timeout: Duration,
    /// How long a replica may be silent before its copies are given up and
    /// re-created elsewhere. Much longer than the liveness window, so that
    /// a restart or an upgrade does not trigger re-replication.
    pub replica_lost_after: Duration,
    /// How long rebuilding one copy of a sealed segment may take.
    pub repair_timeout: Duration,
    /// How many copies one pass rebuilds at most, so that a pass stays
    /// short and placement keeps up while a failed node is re-replicated.
    pub max_repairs_per_pass: usize,
    /// How long after its seal a fully replicated segment is uploaded to
    /// the object store.
    pub tiering_delay: Duration,
    /// How long after the upload the replicas keep their copies, so that
    /// recent data is read from local disks.
    pub hot_read_window: Duration,
    /// How long uploading one segment may take.
    pub upload_timeout: Duration,
    /// How many segments one pass uploads at most, so that a pass stays
    /// short.
    pub max_uploads_per_pass: usize,
}

impl Default for ControllerSettings {
    fn default() -> Self {
        Self {
            identity: "bitacora-controller".to_owned(),
            pass_interval: Duration::from_secs(1),
            leadership_duration: Duration::from_secs(10),
            placement: PlacementPolicy::default(),
            request_timeout: Duration::from_secs(5),
            replica_lost_after: Duration::from_mins(5),
            repair_timeout: Duration::from_mins(30),
            max_repairs_per_pass: 8,
            tiering_delay: Duration::from_mins(15),
            hot_read_window: Duration::from_hours(24),
            upload_timeout: Duration::from_mins(30),
            max_uploads_per_pass: 4,
        }
    }
}

/// What one pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PassReport {
    /// How many streams got a standby segment placed.
    pub standbys_placed: usize,
    /// How many copies of sealed segments were confirmed complete.
    pub copies_confirmed: usize,
    /// How many copies of sealed segments were found short, damaged or
    /// lost, and marked for replacement.
    pub copies_marked_bad: usize,
    /// How many bad copies of sealed segments were rebuilt from a good one.
    pub copies_repaired: usize,
    /// How many sealed segments were uploaded to the object store.
    pub segments_tiered: usize,
    /// How many tiered segments were left to the object store alone, their
    /// hot-read window being over.
    pub segments_released: usize,
    /// How many segments below the low-water mark were deleted.
    pub segments_deleted: usize,
    /// How many segment copies were removed from replicas because their
    /// segment was deleted, moved by repair, or their stream was deleted.
    pub copies_removed: usize,
    /// How many streams could not be fully handled; they are retried on the
    /// next pass.
    pub streams_with_errors: usize,
}

/// What the controller learned about the replicas at the start of a pass.
struct Survey {
    now: Timestamp,
    registered_replicas: Vec<ReplicaRecord>,
    /// The segment copies of every replica that answered, by replica.
    copies_by_replica: BTreeMap<ReplicaId, BTreeMap<SegmentAddress, SegmentStatus>>,
}

impl Survey {
    fn registration(&self, replica_id: &ReplicaId) -> Option<&ReplicaRecord> {
        self.registered_replicas
            .iter()
            .find(|replica| &replica.replica_id == replica_id)
    }
}

/// A verdict on one copy of a sealed segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Verdict {
    segment_id: SegmentId,
    replica_index: usize,
    state: CopyState,
}

/// The controller.
pub struct Controller {
    metadata: MetadataStore,
    connector: Arc<dyn ReplicaConnector>,
    clock: Arc<dyn Clock>,
    settings: ControllerSettings,
    /// Where sealed segments are tiered to. A controller without one tiers
    /// nothing and does everything else.
    artifact_store: Option<Arc<dyn ArtifactStore>>,
}

impl Controller {
    /// Creates a controller over the given interfaces.
    #[must_use]
    pub fn new(
        metadata: MetadataStore,
        connector: Arc<dyn ReplicaConnector>,
        clock: Arc<dyn Clock>,
        settings: ControllerSettings,
    ) -> Self {
        Self {
            metadata,
            connector,
            clock,
            settings,
            artifact_store: None,
        }
    }

    /// Gives the controller the object store that sealed segments are
    /// tiered to (design, section 10, "Tiering"). It must be the store the
    /// replicas upload to. Without one, the controller tiers nothing and
    /// does everything else.
    #[must_use]
    pub fn with_artifact_store(mut self, artifact_store: Arc<dyn ArtifactStore>) -> Self {
        self.artifact_store = Some(artifact_store);
        self
    }

    /// Runs passes forever, as long as this instance holds or can take the
    /// leadership. Stop it by dropping the future.
    ///
    /// Leadership only avoids duplicate work: two controllers that both
    /// believe they lead cannot corrupt anything, because every write of a
    /// pass is conditional.
    pub async fn run(&self) {
        loop {
            let now = self.clock.now();
            let leads = self
                .metadata
                .try_lead(
                    &self.settings.identity,
                    now,
                    now.plus(self.settings.leadership_duration),
                )
                .await;
            match leads {
                Ok(true) => {
                    if let Err(meta_error) = self.run_pass().await {
                        tracing::warn!(%meta_error, "controller pass failed");
                    }
                }
                Ok(false) => {}
                Err(meta_error) => tracing::warn!(%meta_error, "cannot check leadership"),
            }
            sleep(self.settings.pass_interval).await;
        }
    }

    /// Runs one pass over every stream.
    ///
    /// # Errors
    ///
    /// Fails if the metadata store cannot be listed. Trouble with a single
    /// stream or replica is counted in the report, not returned.
    pub async fn run_pass(&self) -> Result<PassReport, MetaError> {
        let survey = self.survey().await?;
        let mut report = PassReport::default();
        for stream in self.metadata.list_streams().await? {
            let handled = self.handle_stream(&stream, &survey, &mut report).await;
            if let Err(meta_error) = handled {
                tracing::warn!(stream = %stream.name, %meta_error, "stream not fully handled");
                report.streams_with_errors += 1;
            }
        }
        self.remove_leftover_copies(&survey, &mut report).await?;
        Ok(report)
    }

    async fn handle_stream(
        &self,
        stream: &StreamRecord,
        survey: &Survey,
        report: &mut PassReport,
    ) -> Result<(), MetaError> {
        self.ensure_standby(stream, survey, report).await?;
        self.reconcile_sealed_copies(stream, survey, report).await?;
        self.repair_bad_copies(&stream.name, survey, report).await?;
        self.tier_sealed_segments(&stream.name, survey, report)
            .await?;
        self.release_tiered_segments(&stream.name, survey.now, report)
            .await?;
        self.truncate(&stream.name, survey.now, report).await?;
        self.delete_tiered_segments_below_the_mark(&stream.name, report)
            .await
    }

    /// Reads the registry and asks every live replica what it holds.
    async fn survey(&self) -> Result<Survey, MetaError> {
        let now = self.clock.now();
        let registered_replicas = self.metadata.list_replicas().await?;
        let mut copies_by_replica = BTreeMap::new();
        for replica in &registered_replicas {
            if !replica.is_alive(now, self.settings.placement.liveness_window) {
                continue;
            }
            let client = self
                .connector
                .connect(&replica.replica_id, &replica.address);
            match timeout(self.settings.request_timeout, client.list_segments()).await {
                Ok(Ok(statuses)) => {
                    copies_by_replica.insert(
                        replica.replica_id.clone(),
                        statuses
                            .into_iter()
                            .map(|status| (status.address, status))
                            .collect(),
                    );
                }
                Ok(Err(replica_error)) => {
                    tracing::debug!(replica = %replica.replica_id, %replica_error, "survey failed");
                }
                Err(_elapsed) => {
                    tracing::debug!(replica = %replica.replica_id, "survey timed out");
                }
            }
        }
        Ok(Survey {
            now,
            registered_replicas,
            copies_by_replica,
        })
    }

    /// Keeps one standby segment placed, so that a roll never waits on
    /// placement. A standby naming a replica that can no longer take new
    /// segments is replaced.
    async fn ensure_standby(
        &self,
        stream: &StreamRecord,
        survey: &Survey,
        report: &mut PassReport,
    ) -> Result<(), MetaError> {
        let policy = self.settings.placement;
        let can_take_segments = |replica_id: &ReplicaId| {
            survey.registration(replica_id).is_some_and(|replica| {
                replica.is_alive(survey.now, policy.liveness_window)
                    && !replica.is_draining
                    && replica.used_percent() < policy.refuse_at_used_percent
            })
        };
        let is_standby_usable = stream.standby_replicas.as_ref().is_some_and(|standby| {
            standby.len() == stream.replication.replica_count()
                && standby.iter().all(can_take_segments)
        });
        if is_standby_usable {
            return Ok(());
        }
        let chosen = match choose_replica_set(
            &survey.registered_replicas,
            stream.replication.replica_count(),
            survey.now,
            policy,
            &BTreeSet::new(),
        ) {
            Ok(chosen) => chosen,
            Err(placement_error) => {
                tracing::warn!(stream = %stream.name, %placement_error, "cannot place a standby");
                return Ok(());
            }
        };
        let seen_standby = stream.standby_replicas.clone();
        let (updated, ()) = self
            .metadata
            .update_stream(&stream.name, |record| {
                // Only replace the standby this pass looked at: a writer may
                // have consumed it meanwhile and another pass placed a new one.
                if record.standby_replicas == seen_standby {
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

    /// Checks every copy of every sealed segment against what its replica
    /// holds, telling replicas about seals they missed (design, section 8,
    /// "Reconciliation"). A copy that is complete becomes good; one that is
    /// short, damaged, or on a replica given up for lost becomes bad and is
    /// later replaced by repair.
    async fn reconcile_sealed_copies(
        &self,
        stream: &StreamRecord,
        survey: &Survey,
        report: &mut PassReport,
    ) -> Result<(), MetaError> {
        let mut verdicts = Vec::new();
        for segment in &stream.segments {
            let Some(sealed_end) = segment.sealed_end() else {
                continue;
            };
            for (replica_index, copy) in segment.copies.iter().enumerate() {
                let judged = self
                    .judge_copy(stream, segment, sealed_end, &copy.replica_id, survey)
                    .await;
                if let Some(state) = judged
                    && state != copy.state
                {
                    verdicts.push(Verdict {
                        segment_id: segment.segment_id,
                        replica_index,
                        state,
                    });
                }
            }
        }
        if verdicts.is_empty() {
            return Ok(());
        }

        let seen_segments = stream.segments.clone();
        self.metadata
            .update_stream(&stream.name, |record| {
                for verdict in &verdicts {
                    let seen_copy = seen_segments
                        .iter()
                        .find(|segment| segment.segment_id == verdict.segment_id)
                        .map(|segment| &segment.copies[verdict.replica_index]);
                    let current_copy = record
                        .segments
                        .iter_mut()
                        .find(|segment| segment.segment_id == verdict.segment_id)
                        .and_then(|segment| segment.copies.get_mut(verdict.replica_index));
                    // Apply the verdict only to the copy it was reached on.
                    if let Some(current_copy) = current_copy
                        && Some(&*current_copy) == seen_copy
                    {
                        current_copy.state = verdict.state;
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

    /// Rebuilds bad copies of sealed segments from a good one (design,
    /// section 10, "Repair"). A copy on a replica that still answers is
    /// rebuilt where it is; a copy on a lost replica moves to a replica that
    /// preserves the segment's spread across failure domains.
    async fn repair_bad_copies(
        &self,
        stream_name: &str,
        survey: &Survey,
        report: &mut PassReport,
    ) -> Result<(), MetaError> {
        // Read again: the reconciliation before this may have changed
        // which copies are good.
        let stream = self.metadata.get_stream(stream_name).await?;
        for segment in &stream.segments {
            let Some(sealed_end) = segment.sealed_end() else {
                continue;
            };
            let bad_copies: Vec<usize> = (0..segment.copies.len())
                .filter(|replica_index| segment.copies[*replica_index].state == CopyState::Bad)
                .collect();
            if bad_copies.is_empty() {
                continue;
            }
            let good_source = segment
                .copies
                .iter()
                .filter(|copy| copy.state == CopyState::Good)
                .filter(|copy| survey.copies_by_replica.contains_key(&copy.replica_id))
                .find_map(|copy| survey.registration(&copy.replica_id));
            let Some(source) = good_source else {
                tracing::error!(
                    stream = stream_name,
                    segment = %segment.segment_id,
                    "no reachable good copy to repair from"
                );
                continue;
            };
            for replica_index in bad_copies {
                if report.copies_repaired >= self.settings.max_repairs_per_pass {
                    return Ok(());
                }
                let Some(target) = self.repair_target(segment, replica_index, survey) else {
                    continue;
                };
                let request = RepairRequest {
                    segment: segment.descriptor(stream.stream_id),
                    sealed_end,
                    source_replica_id: source.replica_id.clone(),
                    source_address: source.address.clone(),
                };
                let client = self.connector.connect(&target.replica_id, &target.address);
                let repaired =
                    timeout(self.settings.repair_timeout, client.repair_segment(request)).await;
                match repaired {
                    Ok(Ok(_complete)) => {
                        self.record_repaired_copy(&stream, segment, replica_index, &target)
                            .await?;
                        tracing::info!(
                            stream = stream_name,
                            segment = %segment.segment_id,
                            replica = %target.replica_id,
                            "rebuilt a copy of a sealed segment"
                        );
                        report.copies_repaired += 1;
                    }
                    Ok(Err(replica_error)) => {
                        tracing::warn!(replica = %target.replica_id, %replica_error, "repair failed");
                    }
                    Err(_elapsed) => {
                        tracing::warn!(replica = %target.replica_id, "repair timed out");
                    }
                }
            }
        }
        Ok(())
    }

    /// Picks the replica that rebuilds the bad copy at `replica_index`: the
    /// one that holds it, if it still answers and takes data; otherwise a
    /// new one that keeps the segment spread across failure domains.
    fn repair_target(
        &self,
        segment: &SegmentRecord,
        replica_index: usize,
        survey: &Survey,
    ) -> Option<ReplicaRecord> {
        let bad_copy = &segment.copies[replica_index];
        let holder = survey
            .registration(&bad_copy.replica_id)
            .filter(|replica| survey.copies_by_replica.contains_key(&replica.replica_id))
            .filter(|replica| !replica.is_draining);
        if let Some(holder) = holder {
            return Some(holder.clone());
        }
        let kept_replicas: Vec<ReplicaId> = segment
            .copies
            .iter()
            .enumerate()
            .filter(|(index, _copy)| *index != replica_index)
            .map(|(_index, copy)| copy.replica_id.clone())
            .collect();
        match choose_additional_replica(
            &survey.registered_replicas,
            &kept_replicas,
            survey.now,
            self.settings.placement,
        ) {
            Ok(replica_id) => survey.registration(&replica_id).cloned(),
            Err(placement_error) => {
                tracing::warn!(segment = %segment.segment_id, %placement_error, "no replica to repair onto");
                None
            }
        }
    }

    /// Swaps the repaired copy into the segment's replica set (design,
    /// section 10, "Repair", step 4), unless the set changed meanwhile.
    async fn record_repaired_copy(
        &self,
        stream: &StreamRecord,
        segment: &SegmentRecord,
        replica_index: usize,
        target: &ReplicaRecord,
    ) -> Result<(), MetaError> {
        let seen_copy = segment.copies[replica_index].clone();
        self.metadata
            .update_stream(&stream.name, |record| {
                let current_copy = record
                    .segments
                    .iter_mut()
                    .find(|candidate| candidate.segment_id == segment.segment_id)
                    .and_then(|candidate| candidate.copies.get_mut(replica_index));
                if let Some(current_copy) = current_copy
                    && *current_copy == seen_copy
                {
                    *current_copy = CopyRecord {
                        replica_id: target.replica_id.clone(),
                        state: CopyState::Good,
                    };
                }
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Advances the stream's low-water mark as far as its materializers,
    /// its owner and its retention policy allow, and deletes the segments
    /// that lie entirely below it (design, section 10, "Truncation and
    /// retention"). Deletion is whole-segment only.
    async fn truncate(
        &self,
        stream_name: &str,
        now: Timestamp,
        report: &mut PassReport,
    ) -> Result<(), MetaError> {
        let stream = self.metadata.get_stream(stream_name).await?;
        let checkpoints = self.metadata.list_checkpoints(stream.stream_id).await?;
        let Some(allowed_mark) = allowed_low_water_mark(&stream, &checkpoints) else {
            return Ok(());
        };
        let seen_materializers = stream.materializers.clone();
        let (_updated, deleted_segments) = self
            .metadata
            .update_stream(stream_name, |record| {
                // A materializer registered since the checkpoints were read
                // starts below the mark this pass would set. Decide again on
                // the next pass, with its checkpoint in hand.
                if record.materializers != seen_materializers {
                    return Ok(Vec::new());
                }
                let mark = hold_back_for_retention(record, allowed_mark, now);
                record.low_water_mark = record.low_water_mark.max(mark);
                let low_water_mark = record.low_water_mark;
                let (deleted, kept): (Vec<SegmentRecord>, Vec<SegmentRecord>) =
                    std::mem::take(&mut record.segments)
                        .into_iter()
                        .partition(|segment| {
                            // A tiered segment stays listed until its
                            // object is deleted, so that a controller that
                            // stops in between still knows about the object.
                            segment.tiered.is_none()
                                && segment
                                    .sealed_end()
                                    .is_some_and(|sealed_end| sealed_end < low_water_mark)
                        });
                record.segments = kept;
                Ok(deleted)
            })
            .await?;

        // The metadata store no longer lists the segments, so readers are
        // told `Truncated`. Their copies are removed by the leftover sweep
        // at the end of this pass, or of a later one for a replica that is
        // away now.
        for segment in &deleted_segments {
            tracing::info!(stream = stream_name, segment = %segment.segment_id, "deleted a segment");
        }
        report.segments_deleted += deleted_segments.len();
        Ok(())
    }

    /// Removes copies that no segment record refers to any more: copies of
    /// segments that retention deleted while their replica was away, copies
    /// that repair replaced, and everything of streams that were deleted.
    ///
    /// Copies of a stream the metadata store knows nothing about are left
    /// alone. They are what a metadata store restored from an older
    /// snapshot would be rebuilt from.
    async fn remove_leftover_copies(
        &self,
        survey: &Survey,
        report: &mut PassReport,
    ) -> Result<(), MetaError> {
        // Listed after the survey, so every copy the survey saw belongs to
        // a segment these records already know or have already dropped.
        let streams_by_id: BTreeMap<StreamId, StreamRecord> = self
            .metadata
            .list_streams()
            .await?
            .into_iter()
            .map(|stream| (stream.stream_id, stream))
            .collect();
        let deleted_streams = self.metadata.list_deleted_streams().await?;
        let deleted_stream_ids: BTreeSet<StreamId> = deleted_streams
            .iter()
            .map(|deleted| deleted.stream_id)
            .collect();

        let mut streams_with_copies_left: BTreeSet<StreamId> = BTreeSet::new();
        for (replica_id, held_copies) in &survey.copies_by_replica {
            let Some(replica) = survey.registration(replica_id) else {
                continue;
            };
            for (address, held) in held_copies {
                let is_leftover = if deleted_stream_ids.contains(&address.stream_id) {
                    true
                } else {
                    streams_by_id
                        .get(&address.stream_id)
                        .is_some_and(|stream| is_leftover_copy(stream, replica_id, held))
                };
                if !is_leftover {
                    continue;
                }
                if self.delete_copy(replica, *address).await {
                    report.copies_removed += 1;
                } else {
                    streams_with_copies_left.insert(address.stream_id);
                }
            }
        }

        // A deleted stream is forgotten once every replica that could still
        // hold a copy of it has been looked at and holds none.
        let has_every_replica_answered = survey.registered_replicas.iter().all(|replica| {
            survey.copies_by_replica.contains_key(&replica.replica_id)
                || !replica.is_alive(survey.now, self.settings.replica_lost_after)
        });
        for deleted in deleted_streams {
            if has_every_replica_answered
                && !streams_with_copies_left.contains(&deleted.stream_id)
                && self.delete_objects_of_stream(deleted.stream_id).await
            {
                self.metadata
                    .forget_deleted_stream(deleted.stream_id)
                    .await?;
                tracing::info!(stream = %deleted.name, "deleted stream is cleaned up");
            }
        }
        Ok(())
    }

    /// Asks a replica to delete its copy of a segment. Returns whether it
    /// did.
    async fn delete_copy(&self, replica: &ReplicaRecord, address: SegmentAddress) -> bool {
        let client = self
            .connector
            .connect(&replica.replica_id, &replica.address);
        let deleted = timeout(
            self.settings.request_timeout,
            client.delete_segment(address),
        )
        .await;
        matches!(deleted, Ok(Ok(())))
    }

    /// Decides what state a copy of a sealed segment should be in, or
    /// `None` if that cannot be known right now.
    async fn judge_copy(
        &self,
        stream: &StreamRecord,
        segment: &SegmentRecord,
        sealed_end: SequenceNumber,
        replica_id: &ReplicaId,
        survey: &Survey,
    ) -> Option<CopyState> {
        let address = segment.address(stream.stream_id);
        let Some(registration) = survey.registration(replica_id) else {
            // The operator removed the replica: its copies are gone.
            return Some(CopyState::Bad);
        };
        let Some(held_copies) = survey.copies_by_replica.get(replica_id) else {
            // The replica did not answer. Give its copies up only after it
            // has been silent for long; a restart must not trigger repair.
            let is_lost = !registration.is_alive(survey.now, self.settings.replica_lost_after);
            return is_lost.then_some(CopyState::Bad);
        };

        let held = held_copies.get(&address);
        let knows_the_seal = held.is_some_and(|status| status.sealed_end == Some(sealed_end));
        let local_end = if knows_the_seal {
            held.and_then(|status| status.local_end)
        } else {
            // Tell the replica; its answer says what it holds afterwards.
            let client = self
                .connector
                .connect(&registration.replica_id, &registration.address);
            let request = SealRequest {
                segment: address,
                sealed_end,
            };
            match timeout(self.settings.request_timeout, client.seal(request)).await {
                Ok(Ok(sealed)) => sealed.local_end,
                Ok(Err(ReplicaError::Unavailable { .. })) | Err(_) => return None,
                Ok(Err(replica_error)) => {
                    tracing::warn!(segment = %address, replica = %replica_id, %replica_error, "copy refuses its seal");
                    return Some(CopyState::Bad);
                }
            }
        };
        let is_damaged = held.is_some_and(|status| status.is_damaged);
        // A segment sealed empty holds nothing, so a replica that never
        // created a file for it is as complete as one that did.
        let end_of_an_empty_copy = segment.first_sequence.preceding();
        let is_complete = local_end.unwrap_or(end_of_an_empty_copy) == sealed_end;
        Some(if is_complete && !is_damaged {
            CopyState::Good
        } else {
            CopyState::Bad
        })
    }
}

/// The mark the stream's materializers and owner allow: every record below
/// it is in the artifacts of every registered materializer and below the
/// position the owner asked to truncate before. `None` if neither says
/// anything, in which case nothing may be deleted.
fn allowed_low_water_mark(
    stream: &StreamRecord,
    checkpoints: &[CheckpointRecord],
) -> Option<SequenceNumber> {
    // A registered materializer without a checkpoint record pins the log at
    // its start; that cannot happen through the metadata store's own
    // operations, and assuming the worst is the safe reading if it does.
    let covered_by_all_materializers = stream
        .materializers
        .iter()
        .map(|materializer| {
            checkpoints
                .iter()
                .find(|checkpoint| &checkpoint.materializer == materializer)
                .map_or(SequenceNumber::FIRST, |checkpoint| {
                    checkpoint.checkpoint.next()
                })
        })
        .min();
    match (covered_by_all_materializers, stream.truncate_before) {
        (Some(covered), Some(requested)) => Some(covered.min(requested)),
        (Some(mark), None) | (None, Some(mark)) => Some(mark),
        (None, None) => None,
    }
}

/// Holds the mark back to the start of the first segment that must stay:
/// the open one, or one sealed more recently than the retention period.
fn hold_back_for_retention(
    stream: &StreamRecord,
    allowed_mark: SequenceNumber,
    now: Timestamp,
) -> SequenceNumber {
    let keep_sealed_for = stream.retention.keep_sealed_for();
    let first_kept_segment = stream.segments.iter().find(|segment| {
        segment.sealed.is_none_or(|seal| {
            keep_sealed_for.is_some_and(|keep_for| now.duration_since(seal.at) < keep_for)
        })
    });
    let retention_limit = first_kept_segment.map_or_else(
        || stream.next_segment_first_sequence(),
        |segment| segment.first_sequence,
    );
    allowed_mark.min(retention_limit)
}

/// Whether a copy held by `replica_id` belongs to nothing in `stream` any
/// more.
fn is_leftover_copy(stream: &StreamRecord, replica_id: &ReplicaId, held: &SegmentStatus) -> bool {
    let listed = stream
        .segments
        .iter()
        .find(|segment| segment.segment_id == held.address.segment_id);
    match (listed, stream.segments.first()) {
        // A sealed segment's copy on a replica outside its replica set was
        // replaced by repair. An open segment is never touched.
        (Some(segment), _) => {
            segment.sealed.is_some()
                && segment
                    .copies
                    .iter()
                    .all(|copy| &copy.replica_id != replica_id)
        }
        // Older than everything listed: retention deleted it. Anything
        // newer than what is listed is left alone; a metadata store restored
        // from an older snapshot must not cost the records written since.
        (None, Some(oldest_listed)) => held.address.segment_id < oldest_listed.segment_id,
        (None, None) => held
            .sealed_end
            .or(held.local_end)
            .is_some_and(|end| end < stream.low_water_mark),
    }
}

#[cfg(test)]
mod tests {
    use bitacora_meta::{RECORD_SCHEMA_VERSION, RetentionPolicy, SealRecord};
    use bitacora_types::{Epoch, ReplicationSettings};

    use super::*;

    const NOW: Timestamp = Timestamp::from_unix_millis(10_000_000);

    fn segment(segment_id: u64, first_sequence: u64, sealed: Option<(u64, u64)>) -> SegmentRecord {
        SegmentRecord {
            segment_id: SegmentId::new(segment_id),
            epoch: Epoch::new(1),
            first_sequence: SequenceNumber::new(first_sequence),
            sealed: sealed.map(|(end, sealed_at_millis)| SealRecord {
                end: SequenceNumber::new(end),
                at: Timestamp::from_unix_millis(sealed_at_millis),
            }),
            copies: vec![CopyRecord {
                replica_id: ReplicaId::new("replica-a").expect("valid identifier"),
                state: CopyState::Good,
            }],
            tiered: None,
        }
    }

    /// A stream with segments 1..=9 and 10..=19 sealed long ago and an open
    /// segment from 20.
    fn stream() -> StreamRecord {
        StreamRecord {
            schema_version: RECORD_SCHEMA_VERSION,
            stream_id: StreamId::new(7),
            name: "orders".to_owned(),
            replication: ReplicationSettings::DEFAULT,
            retention: RetentionPolicy::default(),
            epoch: Epoch::new(1),
            writer: None,
            segments: vec![
                segment(1, 1, Some((9, 1_000))),
                segment(2, 10, Some((19, 2_000))),
                segment(3, 20, None),
            ],
            next_segment_id: SegmentId::new(4),
            standby_replicas: None,
            truncate_before: None,
            low_water_mark: SequenceNumber::FIRST,
            materializers: BTreeSet::new(),
        }
    }

    fn checkpoint(materializer: &str, checkpoint: u64) -> CheckpointRecord {
        CheckpointRecord {
            schema_version: RECORD_SCHEMA_VERSION,
            materializer: materializer.to_owned(),
            checkpoint: SequenceNumber::new(checkpoint),
            fencing_token: 1,
            manifest: None,
        }
    }

    fn with_materializers(names: &[&str]) -> StreamRecord {
        let mut stream = stream();
        stream.materializers = names.iter().map(|name| (*name).to_owned()).collect();
        stream
    }

    fn held(segment_id: u64, sealed_end: Option<u64>) -> SegmentStatus {
        SegmentStatus {
            address: SegmentAddress::new(StreamId::new(7), SegmentId::new(segment_id)),
            local_end: sealed_end.map(SequenceNumber::new),
            fenced_epoch: Epoch::new(1),
            sealed_end: sealed_end.map(SequenceNumber::new),
            size_bytes: 0,
            is_damaged: false,
        }
    }

    fn replica(name: &str) -> ReplicaId {
        ReplicaId::new(name).expect("valid identifier")
    }

    // ---- what may be deleted ----

    #[test]
    fn nothing_may_be_deleted_without_a_materializer_or_a_request() {
        assert_eq!(allowed_low_water_mark(&stream(), &[]), None);
    }

    #[test]
    fn mark_follows_the_slowest_materializer() {
        let stream = with_materializers(&["blocks", "pages"]);
        let checkpoints = [checkpoint("blocks", 15), checkpoint("pages", 9)];
        assert_eq!(
            allowed_low_water_mark(&stream, &checkpoints),
            Some(SequenceNumber::new(10))
        );
    }

    #[test]
    fn materializer_that_never_checkpointed_pins_the_log() {
        let stream = with_materializers(&["pages"]);
        assert_eq!(
            allowed_low_water_mark(&stream, &[checkpoint("pages", 0)]),
            Some(SequenceNumber::FIRST)
        );
    }

    #[test]
    fn registered_materializer_without_a_checkpoint_record_pins_the_log() {
        let stream = with_materializers(&["blocks", "pages"]);
        assert_eq!(
            allowed_low_water_mark(&stream, &[checkpoint("blocks", 15)]),
            Some(SequenceNumber::FIRST)
        );
    }

    #[test]
    fn owner_request_alone_sets_the_mark() {
        let mut stream = stream();
        stream.truncate_before = Some(SequenceNumber::new(12));
        assert_eq!(
            allowed_low_water_mark(&stream, &[]),
            Some(SequenceNumber::new(12))
        );
    }

    #[test]
    fn owner_request_cannot_pass_a_materializers_checkpoint() {
        let mut stream = with_materializers(&["pages"]);
        stream.truncate_before = Some(SequenceNumber::new(18));
        assert_eq!(
            allowed_low_water_mark(&stream, &[checkpoint("pages", 9)]),
            Some(SequenceNumber::new(10))
        );
    }

    #[test]
    fn checkpoint_of_an_unregistered_materializer_is_ignored() {
        let stream = with_materializers(&["pages"]);
        let checkpoints = [checkpoint("gone", 2), checkpoint("pages", 15)];
        assert_eq!(
            allowed_low_water_mark(&stream, &checkpoints),
            Some(SequenceNumber::new(16))
        );
    }

    // ---- retention ----

    #[test]
    fn mark_never_enters_the_open_segment() {
        assert_eq!(
            hold_back_for_retention(&stream(), SequenceNumber::new(500), NOW),
            SequenceNumber::new(20)
        );
    }

    #[test]
    fn mark_below_the_retention_limit_is_kept_as_it_is() {
        assert_eq!(
            hold_back_for_retention(&stream(), SequenceNumber::new(12), NOW),
            SequenceNumber::new(12)
        );
    }

    #[test]
    fn retention_period_holds_the_mark_at_a_recently_sealed_segment() {
        let mut stream = stream();
        stream.retention = RetentionPolicy::keep_for(Duration::from_hours(1));
        let shortly_after_the_second_seal = Timestamp::from_unix_millis(2_000 + 60_000);
        assert_eq!(
            hold_back_for_retention(
                &stream,
                SequenceNumber::new(500),
                shortly_after_the_second_seal
            ),
            SequenceNumber::new(1)
        );
    }

    #[test]
    fn retention_period_releases_segments_sealed_long_ago() {
        let mut stream = stream();
        stream.retention = RetentionPolicy::keep_for(Duration::from_hours(1));
        assert_eq!(
            hold_back_for_retention(&stream, SequenceNumber::new(500), NOW),
            SequenceNumber::new(20)
        );
    }

    #[test]
    fn stream_with_only_sealed_segments_can_be_truncated_to_its_end() {
        let mut stream = stream();
        stream.segments.pop();
        assert_eq!(
            hold_back_for_retention(&stream, SequenceNumber::new(500), NOW),
            SequenceNumber::new(20)
        );
    }

    // ---- leftover copies ----

    #[test]
    fn copy_of_a_listed_segment_on_its_replica_is_kept() {
        assert!(!is_leftover_copy(
            &stream(),
            &replica("replica-a"),
            &held(1, Some(9))
        ));
    }

    #[test]
    fn copy_of_a_sealed_segment_on_a_replica_outside_its_set_is_leftover() {
        assert!(is_leftover_copy(
            &stream(),
            &replica("replica-z"),
            &held(1, Some(9))
        ));
    }

    #[test]
    fn copy_of_the_open_segment_is_never_leftover() {
        assert!(!is_leftover_copy(
            &stream(),
            &replica("replica-z"),
            &held(3, None)
        ));
    }

    #[test]
    fn copy_older_than_every_listed_segment_is_leftover() {
        let mut stream = stream();
        stream.segments.remove(0);
        assert!(is_leftover_copy(
            &stream,
            &replica("replica-a"),
            &held(1, Some(9))
        ));
    }

    #[test]
    fn copy_newer_than_every_listed_segment_is_kept() {
        assert!(!is_leftover_copy(
            &stream(),
            &replica("replica-a"),
            &held(4, Some(30))
        ));
    }

    #[test]
    fn copy_below_the_mark_of_a_stream_without_segments_is_leftover() {
        let mut stream = stream();
        stream.segments.clear();
        stream.low_water_mark = SequenceNumber::new(20);
        assert!(is_leftover_copy(
            &stream,
            &replica("replica-a"),
            &held(2, Some(19))
        ));
    }

    #[test]
    fn copy_at_or_above_the_mark_of_a_stream_without_segments_is_kept() {
        let mut stream = stream();
        stream.segments.clear();
        stream.low_water_mark = SequenceNumber::new(20);
        assert!(!is_leftover_copy(
            &stream,
            &replica("replica-a"),
            &held(3, Some(25))
        ));
    }
}
