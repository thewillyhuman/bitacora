//! The stream writer: the write path of the design (section 5).
//!
//! An append costs one network round trip to the replicas and one disk flush
//! on each; the metadata store is never touched. The writer assigns sequence
//! numbers, sends batches to every replica of the open segment in parallel,
//! and commits a record once a write quorum has flushed it.
//!
//! Three kinds of task cooperate through [`state::WriterState`]:
//!
//! - one *sender* per replica, which sends whatever that replica is missing
//!   and reports what it acknowledged;
//! - one *driver*, which renews the lease and rolls to a new segment when a
//!   replica fails or the segment is full;
//! - the callers of [`StreamWriter::append`], which wait for their records
//!   to commit.

mod state;
pub(crate) mod takeover;

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bitacora_meta::MetaError;
use bitacora_types::replica_api::{AppendRequest, ReplicaApi, ReplicaError, SegmentDescriptor};
use bitacora_types::{Epoch, SequenceNumber, SequenceRange, StreamId};
use bytes::Bytes;
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, sleep, timeout, timeout_at};

use crate::context::{ClientContext, ConnectedReplica};
use crate::error::{AppendError, OpenError};
use crate::settings::{TakeoverPolicy, WriterSettings};
use state::{RollReason, Status, WriterState};
use takeover::{OpenedStream, announce_seal, open_stream};

/// How often the driver looks for conditions that only time brings about:
/// a replica silent for too long, a segment grown old.
const DRIVER_TICK: Duration = Duration::from_millis(250);

/// What the writer's tasks share.
struct Shared {
    context: Arc<ClientContext>,
    settings: WriterSettings,
    stream_name: String,
    stream_id: StreamId,
    epoch: Epoch,
    state: Mutex<WriterState>,
    /// Bumped on every change of `state` that someone may be waiting for.
    changes: watch::Sender<u64>,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, WriterState> {
        // The state is only changed through methods that leave it
        // consistent, so a panic in one task does not corrupt it for the
        // others.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn announce_change(&self) {
        self.changes.send_modify(|version| *version += 1);
    }

    /// Stops the writer and wakes everyone so they notice.
    fn stop(&self, error: &AppendError) {
        self.lock().stop(error);
        self.announce_change();
    }

    /// Stops the writer on behalf of a task working for `generation`,
    /// unless the writer has moved on to a newer segment since.
    fn stop_if_current(&self, generation: u64, error: &AppendError) {
        let mut state = self.lock();
        if state.generation() == generation {
            state.stop(error);
            drop(state);
            self.announce_change();
        }
    }
}

/// The single writer of a stream.
///
/// Obtained from [`crate::BitacoraClient::open_writer`], which runs the
/// takeover protocol first. Dropping the writer without closing it leaves
/// its segment open, to be recovered by the next writer; appends still in
/// flight then resolve as [`AppendError::Indeterminate`].
pub struct StreamWriter {
    shared: Arc<Shared>,
    driver: JoinHandle<()>,
}

impl StreamWriter {
    /// Takes over `stream_name` and starts the writer's tasks.
    pub(crate) async fn open(
        context: Arc<ClientContext>,
        settings: WriterSettings,
        stream_name: &str,
        policy: TakeoverPolicy,
    ) -> Result<Self, OpenError> {
        let OpenedStream {
            stream,
            epoch,
            segment,
            replicas,
        } = open_stream(&context, &settings, stream_name, policy).await?;

        let state = WriterState::new(
            segment.descriptor(stream.stream_id),
            replicas
                .iter()
                .map(|replica| replica.replica_id.clone())
                .collect(),
            stream.replication.write_quorum(),
            Instant::now(),
        );
        let (changes, _first_subscriber) = watch::channel(0);
        let shared = Arc::new(Shared {
            context,
            settings,
            stream_name: stream_name.to_owned(),
            stream_id: stream.stream_id,
            epoch,
            state: Mutex::new(state),
            changes,
        });
        let driver = tokio::spawn(run_driver(Arc::clone(&shared), replicas));
        Ok(Self { shared, driver })
    }

    /// Appends one record and waits until it is committed: stored on a write
    /// quorum and visible to every reader. Returns its sequence number.
    ///
    /// Waits first if too many records are still awaiting commit. Once the
    /// record has a sequence number it will be sent even if the returned
    /// future is dropped.
    ///
    /// # Errors
    ///
    /// Returns [`AppendError`]; see its variants for whether the record may
    /// have been committed.
    pub async fn append(&self, payload: Bytes) -> Result<SequenceNumber, AppendError> {
        let range = self.append_batch(vec![payload]).await?;
        Ok(range.first())
    }

    /// Appends several records as one unit: they get consecutive sequence
    /// numbers and are committed together or not at all. Returns the range
    /// they occupy.
    ///
    /// # Errors
    ///
    /// Returns [`AppendError`]; see [`StreamWriter::append`].
    pub async fn append_batch(&self, payloads: Vec<Bytes>) -> Result<SequenceRange, AppendError> {
        let mut changes = self.shared.changes.subscribe();
        let (range, committed) = loop {
            {
                let mut state = self.shared.lock();
                let can_enqueue_or_must_fail =
                    state.has_capacity(&self.shared.settings) || *state.status() != Status::Running;
                if can_enqueue_or_must_fail {
                    break state.enqueue(&payloads)?;
                }
            }
            if changes.changed().await.is_err() {
                return Err(writer_gone());
            }
        };
        self.shared.announce_change();
        match committed.await {
            Ok(commit_result) => commit_result.map(|()| range),
            Err(_writer_dropped) => Err(writer_gone()),
        }
    }

    /// The highest sequence number such that every record at or below it is
    /// committed.
    #[must_use]
    pub fn durable_sequence(&self) -> SequenceNumber {
        self.shared.lock().durable_sequence()
    }

    /// The epoch this writer holds.
    #[must_use]
    pub fn epoch(&self) -> Epoch {
        self.shared.epoch
    }

    /// Stops accepting appends, waits for the accepted ones to commit, then
    /// seals the segment and releases the lease, so that the next writer can
    /// start without recovery.
    ///
    /// # Errors
    ///
    /// Returns [`AppendError::Fenced`] if another writer took the stream, or
    /// [`AppendError::Indeterminate`] if the accepted records could not be
    /// committed or the segment could not be sealed in time. The next writer
    /// then recovers the segment as after a crash.
    pub async fn close(self) -> Result<(), AppendError> {
        self.shared.lock().begin_closing();
        self.shared.announce_change();

        let drained = timeout(
            self.shared.settings.request_timeout,
            self.wait_until_drained(),
        )
        .await
        .unwrap_or_else(|_elapsed| {
            Err(AppendError::Indeterminate {
                reason: "accepted records did not commit before the writer closed".to_owned(),
            })
        });
        let closed = match drained {
            Ok(()) => self.seal_and_release().await,
            Err(error) => Err(error),
        };
        self.shared
            .stop(closed.as_ref().err().unwrap_or(&AppendError::Closed));
        closed
    }

    async fn wait_until_drained(&self) -> Result<(), AppendError> {
        let mut changes = self.shared.changes.subscribe();
        loop {
            {
                let state = self.shared.lock();
                if let Status::Stopped(error) = state.status() {
                    return Err(error.clone());
                }
                if state.is_drained() {
                    return Ok(());
                }
            }
            if changes.changed().await.is_err() {
                return Err(writer_gone());
            }
        }
    }

    /// Seals the open segment at the durable sequence and gives up the
    /// lease.
    async fn seal_and_release(&self) -> Result<(), AppendError> {
        let (segment, sealed_end) = {
            let state = self.shared.lock();
            (state.segment(), state.durable_sequence())
        };
        let shared = &self.shared;
        let metadata = &shared.context.metadata;
        metadata
            .seal_segment(
                &shared.stream_name,
                shared.epoch,
                segment.address.segment_id,
                sealed_end,
                shared.context.clock.now(),
            )
            .await
            .map_err(|meta_error| close_error(&meta_error))?;
        metadata
            .release_lease(&shared.stream_name, shared.epoch)
            .await
            .map_err(|meta_error| close_error(&meta_error))?;

        let stream = metadata
            .get_stream(&shared.stream_name)
            .await
            .map_err(|meta_error| close_error(&meta_error))?;
        if let Some(sealed_segment) = stream
            .segments
            .iter()
            .find(|candidate| candidate.segment_id == segment.address.segment_id)
            && let Ok(replicas) = shared.context.connect_copies(&sealed_segment.copies).await
        {
            announce_seal(&replicas, segment.address, sealed_end, &shared.settings).await;
        }
        tracing::info!(stream = %shared.stream_name, %sealed_end, "writer closed");
        Ok(())
    }
}

impl Drop for StreamWriter {
    fn drop(&mut self) {
        // Stopping the driver stops the senders it owns. Appends still
        // waiting see their channel close and report an unknown outcome.
        self.driver.abort();
        self.shared.stop(&writer_gone());
    }
}

fn writer_gone() -> AppendError {
    AppendError::Indeterminate {
        reason: "the writer was dropped before the outcome was known".to_owned(),
    }
}

fn close_error(meta_error: &MetaError) -> AppendError {
    match meta_error {
        MetaError::Fenced { .. } => AppendError::Fenced,
        other => AppendError::Indeterminate {
            reason: format!("the segment could not be sealed: {other}"),
        },
    }
}

/// Renews the lease and rolls to a new segment when needed. Owns the sender
/// tasks: when the driver stops, they stop.
async fn run_driver(shared: Arc<Shared>, first_replicas: Vec<ConnectedReplica>) {
    let mut tasks = JoinSet::new();
    spawn_senders(&shared, &mut tasks, 0, &first_replicas);

    let mut changes = shared.changes.subscribe();
    let mut renew_lease_at = Instant::now() + shared.settings.lease_renewal_interval;
    let mut may_roll_at = Instant::now();
    loop {
        let wake_at = renew_lease_at.min(Instant::now() + DRIVER_TICK);
        // Either a change or the deadline leads to the same checks below.
        let _woken_by = timeout_at(wake_at, changes.changed()).await;

        if matches!(shared.lock().status(), Status::Stopped(_)) {
            return;
        }
        if Instant::now() >= renew_lease_at {
            renew_lease(&shared).await;
            renew_lease_at = Instant::now() + shared.settings.lease_renewal_interval;
        }
        let roll_reason = shared.lock().roll_reason(Instant::now(), &shared.settings);
        if let Some(roll_reason) = roll_reason
            && Instant::now() >= may_roll_at
        {
            match roll(&shared, &roll_reason).await {
                Some(new_replicas) => {
                    let generation = shared.lock().generation();
                    spawn_senders(&shared, &mut tasks, generation, &new_replicas);
                }
                None => may_roll_at = Instant::now() + shared.settings.retry_interval,
            }
        }
        // Reap finished senders so the set does not grow with every roll.
        while tasks.try_join_next().is_some() {}
    }
}

fn spawn_senders(
    shared: &Arc<Shared>,
    tasks: &mut JoinSet<()>,
    generation: u64,
    replicas: &[ConnectedReplica],
) {
    for (replica_index, replica) in replicas.iter().enumerate() {
        tasks.spawn(run_sender(
            Arc::clone(shared),
            generation,
            replica_index,
            Arc::clone(&replica.client),
        ));
    }
}

async fn renew_lease(shared: &Shared) {
    let expires_at = shared
        .context
        .clock
        .now()
        .plus(shared.settings.lease_duration);
    match shared
        .context
        .metadata
        .renew_lease(&shared.stream_name, shared.epoch, expires_at)
        .await
    {
        Ok(()) => {}
        Err(MetaError::Fenced { current_epoch, .. }) => {
            tracing::warn!(stream = %shared.stream_name, %current_epoch, "lost the stream");
            shared.stop(&AppendError::Fenced);
        }
        // The lease is a courtesy. If it cannot be renewed the writer keeps
        // going; only the epoch decides who may write.
        Err(meta_error) => {
            tracing::warn!(stream = %shared.stream_name, %meta_error, "lease renewal failed");
        }
    }
}

/// Seals the open segment at the durable sequence and moves to the standby
/// segment (design, section 5, "A slow or failed replica"). Returns the new
/// replica set, or `None` if the roll did not happen.
async fn roll(shared: &Arc<Shared>, roll_reason: &RollReason) -> Option<Vec<ConnectedReplica>> {
    let metadata = &shared.context.metadata;
    // Read first, while still committing: if the store is down this fails
    // before anything is frozen, and appends continue on the old segment.
    let stream_before = match metadata.get_stream(&shared.stream_name).await {
        Ok(stream) => stream,
        Err(meta_error) => {
            tracing::warn!(stream = %shared.stream_name, %meta_error, "cannot roll yet");
            return None;
        }
    };
    if stream_before.standby_replicas.is_none() && stream_before.epoch == shared.epoch {
        tracing::warn!(stream = %shared.stream_name, ?roll_reason, "no standby segment to roll into");
        return None;
    }

    // Step 1: stop committing on the current segment.
    let (old_segment, sealed_end) = {
        let mut state = shared.lock();
        (state.segment(), state.begin_roll())
    };
    tracing::info!(
        stream = %shared.stream_name,
        segment = %old_segment.address,
        %sealed_end,
        ?roll_reason,
        "rolling to a new segment"
    );

    // Step 2: one conditional write seals this segment and opens the next.
    // The write is repeated until its outcome is known, because until then
    // nothing may commit on either segment.
    let rolled = loop {
        let outcome = metadata
            .roll_segment(
                &shared.stream_name,
                shared.epoch,
                old_segment.address.segment_id,
                sealed_end,
                shared.context.clock.now(),
            )
            .await;
        match outcome {
            Err(MetaError::Unavailable(store_unavailable)) => {
                tracing::warn!(stream = %shared.stream_name, %store_unavailable, "roll outcome unknown");
                sleep(shared.settings.retry_interval).await;
            }
            known_outcome => break known_outcome,
        }
    };
    let (stream, new_segment) = match rolled {
        Ok(rolled) => rolled,
        Err(MetaError::Fenced { current_epoch, .. }) => {
            tracing::warn!(stream = %shared.stream_name, %current_epoch, "lost the stream");
            shared.stop(&AppendError::Fenced);
            return None;
        }
        Err(meta_error) => {
            tracing::warn!(stream = %shared.stream_name, %meta_error, "roll did not happen");
            shared.lock().abandon_roll();
            shared.announce_change();
            return None;
        }
    };

    // The new replica set must be reachable before anything can commit, so
    // its addresses are looked up until the registry answers.
    let new_replicas = loop {
        match shared.context.connect_copies(&new_segment.copies).await {
            Ok(replicas) => break replicas,
            Err(meta_error) => {
                tracing::warn!(stream = %shared.stream_name, %meta_error, "cannot resolve replicas");
                sleep(shared.settings.retry_interval).await;
            }
        }
    };

    // Step 4: resend every record above the sealed end to the new segment.
    shared.lock().complete_roll(
        new_segment.descriptor(shared.stream_id),
        new_replicas
            .iter()
            .map(|replica| replica.replica_id.clone())
            .collect(),
        Instant::now(),
    );
    shared.announce_change();

    // Step 3: tell the old replicas the sealed end. Whoever misses it is
    // told by the controller.
    if let Some(sealed_segment) = stream
        .segments
        .iter()
        .find(|segment| segment.segment_id == old_segment.address.segment_id)
        && let Ok(old_replicas) = shared.context.connect_copies(&sealed_segment.copies).await
    {
        announce_seal(
            &old_replicas,
            old_segment.address,
            sealed_end,
            &shared.settings,
        )
        .await;
    }
    Some(new_replicas)
}

/// Keeps one replica of the segment opened at `generation` up to date.
async fn run_sender(
    shared: Arc<Shared>,
    generation: u64,
    replica_index: usize,
    replica: Arc<dyn ReplicaApi>,
) {
    let mut changes = shared.changes.subscribe();
    let mut last_durable_sent: Option<SequenceNumber> = None;
    let mut heartbeat_due_at = Instant::now();
    loop {
        let has_news = {
            let state = shared.lock();
            if matches!(state.status(), Status::Stopped(_)) || state.generation() != generation {
                return;
            }
            state.has_batches_for(generation, replica_index)
                || Some(state.durable_sequence()) != last_durable_sent
        };
        if !has_news && Instant::now() < heartbeat_due_at {
            // Wait for news or for the heartbeat to fall due.
            let _woken_by = timeout_at(heartbeat_due_at, changes.changed()).await;
            continue;
        }

        let Some(plan) = shared.lock().plan_request(
            generation,
            replica_index,
            shared.settings.max_request_bytes,
        ) else {
            return;
        };
        let durable_sent = plan.durable_sequence;
        let segment: SegmentDescriptor = plan.segment;
        let answer = timeout(
            shared.settings.request_timeout,
            replica.append(AppendRequest {
                segment,
                writer_epoch: shared.epoch,
                batch_frames: plan.batch_frames,
                durable_sequence: durable_sent,
            }),
        )
        .await;
        heartbeat_due_at = Instant::now() + shared.settings.heartbeat_interval;

        match answer {
            Ok(Ok(acknowledged)) => {
                last_durable_sent = Some(durable_sent);
                shared.lock().record_acknowledgement(
                    generation,
                    replica_index,
                    acknowledged.local_end,
                    Instant::now(),
                    &shared.settings,
                );
                shared.announce_change();
            }
            Ok(Err(ReplicaError::OutOfOrder { local_end, .. })) => {
                // The replica's copy ends elsewhere than assumed, typically
                // after a request whose answer was lost. Resume from there.
                shared.lock().record_acknowledgement(
                    generation,
                    replica_index,
                    local_end,
                    Instant::now(),
                    &shared.settings,
                );
                shared.announce_change();
            }
            // A fence or a seal on the open segment can only come from a
            // newer writer's takeover.
            Ok(Err(ReplicaError::Fenced { .. } | ReplicaError::Sealed { .. })) => {
                tracing::warn!(stream = %shared.stream_name, "lost the stream");
                shared.stop_if_current(generation, &AppendError::Fenced);
                return;
            }
            Ok(Err(ReplicaError::Unavailable { .. })) | Err(_) => {
                sleep(shared.settings.retry_interval).await;
            }
            Ok(Err(lasting_refusal)) => {
                tracing::warn!(stream = %shared.stream_name, %lasting_refusal, "replica refuses appends");
                {
                    let mut state = shared.lock();
                    state.record_contact(generation, replica_index, Instant::now());
                    state.record_refusal(generation, replica_index);
                }
                shared.announce_change();
                sleep(shared.settings.retry_interval).await;
            }
        }
    }
}
