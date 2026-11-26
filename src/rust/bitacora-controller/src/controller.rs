//! The controller and one pass of it over the cluster.
//!
//! # Why this file exists
//!
//! The controller's work is a list of independent steps: place standbys, check copies, repair, tier, truncate, sweep,
//! back up. Each step is simple on its own, and each lives in its own file. What is left to say in one place is what
//! the controller is made of, in which order a pass takes the steps, and the few pieces of plumbing every step uses to
//! talk to a replica or to the object store. This file is that place.
//!
//! # What it contains
//!
//! - [`Controller`]: the handle a process holds, built with [`Controller::new`] and
//!   [`Controller::with_artifact_store`].
//! - [`Controller::run_pass`]: one pass over every stream, and `Controller::handle_stream`, the steps for one stream
//!   in their order.
//! - `Controller::connect_to`, `replica_answer_within`, `object_store_answer_within`: asking a replica or the object
//!   store for something, with a time limit, and `ReplicaFailure` and `ObjectStoreFailure`: why it did not happen.
//! - `unchanged_copy_mut`: "apply this only to the copy the pass looked at", the condition every write about a copy
//!   carries.
//!
//! # How it works
//!
//! A pass starts with a **survey**: the registry of replicas and what every live replica says it holds. Everything
//! the pass decides about replicas, it decides from that one picture.
//!
//! Then, for every stream, in this order:
//!
//! 1. **Standby**: make sure a replica set is placed for the stream's next segment.
//! 2. **Reconciliation**: compare every copy of every sealed segment with the sealed end, and mark it good or bad.
//! 3. **Repair**: have bad copies rebuilt from a good one.
//! 4. **Tiering**: upload segments that are ready to the object store.
//! 5. **Release**: leave segments whose hot-read window is over to the object store alone.
//! 6. **Truncation**: advance the low-water mark and delete the segments below it.
//! 7. **Deletion of tiered segments** below the mark: the object first, the record second.
//!
//! The order matters in two places. Repair comes after reconciliation, because reconciliation is what marks copies
//! bad; and tiering comes after repair, because a segment is uploaded only once all its copies are good. A stream
//! that cannot be handled, because the metadata store failed in the middle, is counted and left for the next pass;
//! the other streams are still handled.
//!
//! After the streams, once per pass: the **leftover sweep** removes segment copies that no record refers to any more,
//! and a **metadata snapshot** is written to the object store if one is due.
//!
//! Every step reads the stream's record again before it decides, and writes with
//! `MetadataStore::update_stream`, whose change is applied only if the part of the record the step looked at is still
//! what it saw. That is the whole concurrency story: there is no lock, and no step trusts what an earlier step read.
//!
//! # Invariants
//!
//! - The controller is never on the append path, and nothing here is a hot path: every function is written to be
//!   read, not to be fast.
//! - Every request to a replica or to the object store has a time limit.
//! - A failure of a replica or of the object store never fails a pass. It is logged, and the next pass finds the same
//!   work to do. Only a metadata store that cannot be listed fails a pass.
//! - The order of the steps, and of the requests within a step, is part of the behaviour the simulator replays. A
//!   change to it changes every seed's history.
//!
//! # See also
//!
//! - `controller/settings.rs`, `controller/report.rs`: the settings, and what a pass reports.
//! - `controller/round.rs`: leadership and the loop around the pass.
//! - `controller/survey.rs`: the picture of the replicas a pass works from.
//! - `controller/standby.rs`, `reconciliation.rs`, `repair.rs`, `tiering.rs`, `truncation.rs`, `leftovers.rs`,
//!   `snapshot.rs`: the steps.
//! - `controller/test_support.rs`: what the tests of the steps share.

use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bitacora_artifact::{ArtifactError, ArtifactStore};
use bitacora_meta::{CopyRecord, MetaError, MetadataStore, ReplicaRecord, StreamRecord};
use bitacora_types::replica_api::{ReplicaApi, ReplicaConnector, ReplicaError};
use bitacora_types::{Clock, SegmentId, Timestamp};
use tokio::time::timeout;

mod leftovers;
mod reconciliation;
mod repair;
mod report;
mod round;
mod settings;
mod snapshot;
mod standby;
mod survey;
mod tiering;
mod truncation;

#[cfg(test)]
mod test_support;

pub use report::{PassReport, Round};
pub use settings::ControllerSettings;

use report::segments_missing_copies;
use survey::Survey;

/// The controller: placement, reconciliation, repair, tiering, retention and backup for one cluster.
///
/// It is built over the interfaces it needs (the metadata store, the way to reach replicas, the clock, and optionally
/// the object store) and holds nothing else that matters: all it knows, it reads again on every pass. Two or three
/// run in a cluster, and the one that holds the leadership works; see [`Controller::run`].
///
/// # Examples
///
/// One pass over a cluster of three replicas places the standby of a new stream:
///
/// ```
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() -> Result<(), bitacora_meta::MetaError> {
/// use bitacora_controller::{Controller, ControllerSettings};
/// use bitacora_meta::RetentionPolicy;
/// use bitacora_testkit::Cluster;
/// use bitacora_types::ReplicationSettings;
///
/// let cluster = Cluster::start(3).await;
/// cluster
///     .metadata()
///     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
///     .await?;
/// let controller = Controller::new(
///     cluster.metadata().clone(),
///     cluster.connector(),
///     cluster.clock(),
///     ControllerSettings::default(),
/// );
///
/// let report = controller.run_pass().await?;
///
/// assert_eq!(report.standbys_placed, 1);
/// # Ok(())
/// # }
/// ```
pub struct Controller {
    /// The metadata store the controller reads its work from and records its results in.
    metadata: MetadataStore,
    /// How the controller reaches a replica, given its identity and its registered address.
    connector: Arc<dyn ReplicaConnector>,
    /// The time, for liveness, leadership, tiering delays and retention.
    clock: Arc<dyn Clock>,
    /// How the controller paces itself and judges replicas.
    settings: ControllerSettings,
    /// Where sealed segments are tiered to. A controller without one tiers nothing, takes no metadata snapshot, and
    /// does everything else.
    artifact_store: Option<Arc<dyn ArtifactStore>>,
    /// When the newest metadata snapshot in the object store was taken, as far as this instance knows. It only saves
    /// a listing of the object store on every pass; `None` means "look". A mutex because a pass takes `&self`.
    newest_metadata_snapshot_at: Mutex<Option<Timestamp>>,
}

impl Controller {
    /// Creates a controller over the given interfaces.
    ///
    /// Nothing is read or written yet. The controller has no object store until
    /// [`Controller::with_artifact_store`] gives it one.
    ///
    /// # Arguments
    ///
    /// * `metadata` - The cluster's metadata store.
    /// * `connector` - How to reach a replica. Production passes the gRPC connector, tests and the simulator an
    ///   in-process one.
    /// * `clock` - The time. Production passes the system clock.
    /// * `settings` - How the controller paces itself and judges replicas.
    ///
    /// # Returns
    ///
    /// The controller, ready to run.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use std::time::Duration;
    ///
    /// use bitacora_controller::{Controller, ControllerSettings};
    /// use bitacora_testkit::Cluster;
    ///
    /// let cluster = Cluster::start(3).await;
    /// let settings = ControllerSettings {
    ///     identity: "controller-a".to_owned(),
    ///     pass_interval: Duration::from_millis(500),
    ///     ..ControllerSettings::default()
    /// };
    ///
    /// let _controller = Controller::new(cluster.metadata().clone(), cluster.connector(), cluster.clock(), settings);
    /// # }
    /// ```
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
            newest_metadata_snapshot_at: Mutex::new(None),
        }
    }

    /// Gives the controller the object store that sealed segments are tiered to (design, section 10, "Tiering").
    ///
    /// It must be the store the replicas upload to: the controller records an upload only once it sees the object
    /// itself. Without an object store the controller tiers nothing, takes no metadata snapshot, and does everything
    /// else.
    ///
    /// # Arguments
    ///
    /// * `artifact_store` - The cluster's object store.
    ///
    /// # Returns
    ///
    /// The controller, now with an object store.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use std::sync::Arc;
    ///
    /// use bitacora_controller::{Controller, ControllerSettings};
    /// use bitacora_testkit::Cluster;
    ///
    /// let cluster = Cluster::start(3).await;
    ///
    /// let _controller = Controller::new(
    ///     cluster.metadata().clone(),
    ///     cluster.connector(),
    ///     cluster.clock(),
    ///     ControllerSettings::default(),
    /// )
    /// .with_artifact_store(Arc::new(cluster.artifact_store().clone()));
    /// # }
    /// ```
    #[must_use]
    pub fn with_artifact_store(mut self, artifact_store: Arc<dyn ArtifactStore>) -> Self {
        self.artifact_store = Some(artifact_store);
        self
    }

    /// Runs one pass over the cluster: every stream, then the leftover sweep and the metadata snapshot.
    ///
    /// The file header lists the steps. The pass does not check who leads; [`Controller::run_round`] does that
    /// before it calls this. Calling it directly is safe at any time, because every write of a pass is conditional
    /// on what the pass saw.
    ///
    /// # Returns
    ///
    /// What the pass did, counted. Trouble with a single stream or replica is counted in the report, not returned.
    ///
    /// # Errors
    ///
    /// Fails if the replica registry, the streams or the deleted streams cannot be listed. What the pass did before
    /// that stays done.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// use bitacora_controller::{Controller, ControllerSettings, PassReport};
    /// use bitacora_testkit::Cluster;
    ///
    /// let cluster = Cluster::start(3).await;
    /// let controller = Controller::new(
    ///     cluster.metadata().clone(),
    ///     cluster.connector(),
    ///     cluster.clock(),
    ///     ControllerSettings::default(),
    /// );
    ///
    /// // A cluster without streams gives a pass nothing to do.
    /// let report = controller.run_pass().await?;
    ///
    /// assert_eq!(report, PassReport::default());
    /// # Ok(())
    /// # }
    /// ```
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
        self.snapshot_metadata(survey.now, &mut report).await;
        Ok(report)
    }

    /// Takes the steps of a pass for one stream, in the order the file header gives, and then counts the stream's
    /// segments that are short of good copies.
    ///
    /// # Arguments
    ///
    /// * `stream` - The stream's record as the pass listed it. Steps after the first two read it again.
    /// * `survey` - What the pass learned about the replicas.
    /// * `report` - Where the steps count what they did.
    ///
    /// # Errors
    ///
    /// Fails if the metadata store fails a read or a write. The steps taken before the failure stay done; the pass
    /// counts the stream as not fully handled and goes on with the next one.
    async fn handle_stream(
        &self,
        stream: &StreamRecord,
        survey: &Survey,
        report: &mut PassReport,
    ) -> Result<(), MetaError> {
        self.ensure_standby(stream, survey, report).await?;
        self.reconcile_sealed_copies(stream, survey, report).await?;
        self.repair_bad_copies(&stream.name, survey, report).await?;
        self.tier_sealed_segments(&stream.name, survey, report).await?;
        self.release_tiered_segments(&stream.name, survey.now, report).await?;
        self.truncate(&stream.name, survey.now, report).await?;
        self.delete_tiered_segments_below_the_mark(&stream.name, report).await?;
        let handled_stream = self.metadata.get_stream(&stream.name).await?;
        report.segments_missing_copies += segments_missing_copies(&handled_stream);
        Ok(())
    }

    /// Returns a handle to the replica that `replica` registers.
    ///
    /// The handle is addressed to that identity: a node that was rebuilt at the same address under a new identity
    /// refuses its requests (decision record 0010).
    ///
    /// # Arguments
    ///
    /// * `replica` - The registration of the replica to reach.
    ///
    /// # Returns
    ///
    /// The handle. Getting one sends nothing.
    fn connect_to(&self, replica: &ReplicaRecord) -> Arc<dyn ReplicaApi> {
        self.connector.connect(&replica.replica_id, &replica.address)
    }
}

/// Why a replica did not do what the controller asked of it.
#[derive(Debug)]
enum ReplicaFailure {
    /// The replica answered, and refused or failed.
    Refused(ReplicaError),
    /// No answer came within the time the controller allows for the request.
    TimedOut,
}

impl fmt::Display for ReplicaFailure {
    /// Writes the replica's own error, or that it did not answer in time.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused(replica_error) => write!(formatter, "{replica_error}"),
            Self::TimedOut => formatter.write_str("the replica did not answer in time"),
        }
    }
}

/// Waits for a replica's answer to `request`, for at most `time_limit`.
///
/// # Arguments
///
/// * `time_limit` - How long the answer may take.
/// * `request` - The request, not yet started: for example `client.list_segments()`.
///
/// # Returns
///
/// The replica's answer.
///
/// # Errors
///
/// * [`ReplicaFailure::Refused`] with the replica's own error if it answered with one.
/// * [`ReplicaFailure::TimedOut`] if no answer came in time. The request may still have been applied.
async fn replica_answer_within<Answer>(
    time_limit: Duration,
    request: impl Future<Output = Result<Answer, ReplicaError>>,
) -> Result<Answer, ReplicaFailure> {
    match timeout(time_limit, request).await {
        Ok(Ok(answer)) => Ok(answer),
        Ok(Err(replica_error)) => Err(ReplicaFailure::Refused(replica_error)),
        Err(_elapsed) => Err(ReplicaFailure::TimedOut),
    }
}

/// Why the object store did not do what the controller asked of it.
#[derive(Debug)]
enum ObjectStoreFailure {
    /// The object store answered with an error.
    Failed(ArtifactError),
    /// No answer came within the time the controller allows for the request.
    TimedOut,
}

impl fmt::Display for ObjectStoreFailure {
    /// Writes the object store's own error, or that it did not answer in time.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed(artifact_error) => write!(formatter, "{artifact_error}"),
            Self::TimedOut => formatter.write_str("the object store did not answer in time"),
        }
    }
}

/// Waits for the object store's answer to `request`, for at most `time_limit`.
///
/// # Arguments
///
/// * `time_limit` - How long the answer may take.
/// * `request` - The request, not yet started: for example `artifact_store.list_prefix(key_prefix)`.
///
/// # Returns
///
/// The object store's answer.
///
/// # Errors
///
/// * [`ObjectStoreFailure::Failed`] with the store's own error if it answered with one.
/// * [`ObjectStoreFailure::TimedOut`] if no answer came in time. The request may still have been applied.
async fn object_store_answer_within<Answer>(
    time_limit: Duration,
    request: impl Future<Output = Result<Answer, ArtifactError>>,
) -> Result<Answer, ObjectStoreFailure> {
    match timeout(time_limit, request).await {
        Ok(Ok(answer)) => Ok(answer),
        Ok(Err(artifact_error)) => Err(ObjectStoreFailure::Failed(artifact_error)),
        Err(_elapsed) => Err(ObjectStoreFailure::TimedOut),
    }
}

/// Returns the copy at `replica_index` of segment `segment_id` in `record`, if it is still the copy a pass looked at.
///
/// A pass reaches a conclusion about a copy from what it read earlier. Between that read and the write, repair may
/// have replaced the copy, another controller may have judged it, or retention may have deleted the segment. The
/// conclusion then no longer applies, and the write must leave the record alone. Every write about a copy goes
/// through this function for that reason.
///
/// # Arguments
///
/// * `record` - The stream's record as it is now, inside `MetadataStore::update_stream`.
/// * `segment_id` - The segment the copy belongs to.
/// * `replica_index` - The copy's position in the segment's replica set.
/// * `seen_copy` - The copy as the pass saw it: the replica that held it and its state.
///
/// # Returns
///
/// The copy, to be changed, or `None` if the segment is gone, the replica set is shorter, or the copy at that
/// position is no longer `seen_copy`.
fn unchanged_copy_mut<'record>(
    record: &'record mut StreamRecord,
    segment_id: SegmentId,
    replica_index: usize,
    seen_copy: &CopyRecord,
) -> Option<&'record mut CopyRecord> {
    let segment = record
        .segments
        .iter_mut()
        .find(|segment| segment.segment_id == segment_id)?;
    let current_copy = segment.copies.get_mut(replica_index)?;
    if *current_copy == *seen_copy {
        Some(current_copy)
    } else {
        None
    }
}

#[cfg(test)]
#[path = "controller_test.rs"]
mod tests;
