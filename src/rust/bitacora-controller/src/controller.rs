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

use bitacora_meta::{
    CopyState, MetaError, MetadataStore, PlacementPolicy, ReplicaRecord, SegmentRecord,
    StreamRecord, choose_replica_set,
};
use bitacora_types::replica_api::{ReplicaConnector, ReplicaError, SealRequest, SegmentStatus};
use bitacora_types::{Clock, ReplicaId, SegmentAddress, SegmentId, SequenceNumber, Timestamp};
use tokio::time::{sleep, timeout};

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
        }
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
        Ok(report)
    }

    async fn handle_stream(
        &self,
        stream: &StreamRecord,
        survey: &Survey,
        report: &mut PassReport,
    ) -> Result<(), MetaError> {
        self.ensure_standby(stream, survey, report).await?;
        self.reconcile_sealed_copies(stream, survey, report).await
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
            let client = self.connector.connect(&replica.address);
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
            let client = self.connector.connect(&registration.address);
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
