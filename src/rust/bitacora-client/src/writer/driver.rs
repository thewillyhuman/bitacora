//! The writer's driver task: lease renewal and segment rolls.
//!
//! # Why this file exists
//!
//! Two things a writer must do are caused by time and by failures, not by an append: keep its lease from lapsing, and
//! leave a segment that is full, old, or held back by a replica that stopped answering (design, section 5, "A slow or
//! failed replica"). Neither may sit on the append path, where the metadata store is never touched. One background task
//! per writer does both, and this file is that task.
//!
//! # What it contains
//!
//! - `run_driver`: the task. It also owns the sender tasks, so that they stop when it stops.
//! - `renew_lease`: one renewal, and what losing the stream there means.
//! - `roll` and its steps (`has_standby_segment`, `stop_committing`, `record_roll`, `connect_new_replicas`): moving to
//!   the standby segment.
//! - `DRIVER_TICK`: how often the task looks even if nothing wakes it.
//!
//! # How it works
//!
//! The driver sleeps until the state changes, the lease is due, or a tick passes. Then it renews the lease if due, and
//! asks the state whether a roll is needed.
//!
//! A roll has four steps:
//!
//! 1. Stop committing on the current segment. The state freezes the durable sequence; that is where the segment ends.
//! 2. One conditional metadata write seals the segment there and opens the next one on the standby replica set. It is
//!    repeated until its outcome is known, because until then nothing may commit on either segment.
//! 3. Tell the old replicas the sealed end. Best effort: the controller tells whoever misses it.
//! 4. Resend every record above the sealed end to the new segment, and start one sender per new replica.
//!
//! Step 4 is done before step 3 in the code: appenders are waiting on it, and nobody waits on step 3.
//!
//! # Invariants
//!
//! - The stream's record is read before anything is frozen. If the metadata store is down, the roll fails there and
//!   appends continue on the old segment.
//! - A roll that finds no standby segment does not freeze anything either: the writer keeps the segment it has until
//!   the controller places one.
//! - After a failed attempt the next one waits one retry interval, so a metadata outage is not hammered.
//! - The lease is a courtesy. If it cannot be renewed the writer keeps going; only the epoch decides who may write.
//! - Nothing here runs per append. Lease renewal and rolls are rare, so this code favours being followed step by step
//!   over being fast.
//!
//! # See also
//!
//! - `state.rs`: `roll_reason`, `begin_roll`, `complete_roll`, `abandon_roll`, which decide and record what this file
//!   carries out.
//! - `sender.rs`: the tasks this one spawns.
//! - `bitacora-meta/src/store/writers.rs`: `roll_segment` and `renew_lease`.
//! - `docs/protocol/replication.md`, "Rolling".

use std::sync::Arc;
use std::time::Duration;

use bitacora_meta::{MetaError, SegmentRecord, StreamRecord};
use bitacora_types::SequenceNumber;
use bitacora_types::replica_api::SegmentDescriptor;
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep, timeout_at};

use super::sender::run_sender;
use super::state::{RollReason, Status};
use super::takeover::announce_seal_of_listed_segment;
use super::{Shared, replica_ids_of};
use crate::context::ConnectedReplica;
use crate::error::AppendError;

/// How often the driver looks for conditions that only time brings about: a replica silent for too long, a segment
/// grown old.
const DRIVER_TICK: Duration = Duration::from_millis(250);

/// Renews the lease and rolls to a new segment when needed, until the writer stops.
///
/// Owns the sender tasks: when the driver is aborted or returns, they are aborted with it.
///
/// # Arguments
///
/// * `shared` - What the writer's tasks share.
/// * `first_replicas` - The replicas of the segment the takeover opened, in the order of its replica set.
pub(super) async fn run_driver(shared: Arc<Shared>, first_replicas: Vec<ConnectedReplica>) {
    let mut senders = JoinSet::new();
    spawn_senders(&shared, &mut senders, 0, &first_replicas);

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
                    spawn_senders(&shared, &mut senders, generation, &new_replicas);
                }
                None => may_roll_at = Instant::now() + shared.settings.retry_interval,
            }
        }
        // Reap finished senders so the set does not grow with every roll.
        while senders.try_join_next().is_some() {}
    }
}

/// Starts one sender per replica of the segment opened at `generation`, in the order of the replica set.
fn spawn_senders(shared: &Arc<Shared>, senders: &mut JoinSet<()>, generation: u64, replicas: &[ConnectedReplica]) {
    for (replica_index, replica) in replicas.iter().enumerate() {
        senders.spawn(run_sender(
            Arc::clone(shared),
            generation,
            replica_index,
            Arc::clone(&replica.client),
        ));
    }
}

/// Extends the writer's lease by one lease duration from now.
///
/// A renewal refused because the epoch moved on means another writer took the stream: the writer stops. Any other
/// failure is only logged, because the lease is a courtesy and only the epoch decides who may write.
async fn renew_lease(shared: &Shared) {
    let expires_at = shared.context.clock.now().plus(shared.settings.lease_duration);
    let renewed = shared
        .context
        .metadata
        .renew_lease(&shared.stream_name, shared.epoch, expires_at)
        .await;
    match renewed {
        Ok(()) => {}
        Err(MetaError::Fenced { current_epoch, .. }) => {
            tracing::warn!(stream = %shared.stream_name, %current_epoch, "lost the stream");
            shared.stop(&AppendError::Fenced);
        }
        Err(meta_error) => {
            tracing::warn!(stream = %shared.stream_name, %meta_error, "lease renewal failed");
        }
    }
}

/// Seals the open segment at the durable sequence and moves to the standby segment.
///
/// Returns the new segment's replicas, connected, or `None` if the roll did not happen: the metadata store could not be
/// read, there is no standby segment, the writer lost the stream, or the conditional write was refused.
async fn roll(shared: &Arc<Shared>, roll_reason: &RollReason) -> Option<Vec<ConnectedReplica>> {
    // Read first, while still committing: if the store is down this fails before anything is frozen, and appends
    // continue on the old segment.
    if !has_standby_segment(shared, roll_reason).await {
        return None;
    }

    // Step 1: stop committing on the current segment.
    let (old_segment, sealed_end) = stop_committing(shared, roll_reason);

    // Step 2: one conditional write seals this segment and opens the next.
    let (stream, new_segment) = record_roll(shared, old_segment, sealed_end).await?;
    let new_replicas = connect_new_replicas(shared, &new_segment).await;

    // Step 4: resend every record above the sealed end to the new segment.
    shared.lock().complete_roll(
        new_segment.descriptor(shared.stream_id),
        replica_ids_of(&new_replicas),
        Instant::now(),
    );
    shared.announce_change();

    // Step 3: tell the old replicas the sealed end. Whoever misses it is told by the controller.
    announce_seal_of_listed_segment(
        &shared.context,
        &stream,
        old_segment.address,
        sealed_end,
        &shared.settings,
    )
    .await;
    Some(new_replicas)
}

/// Reads the stream's record and says whether there is a segment to roll into.
///
/// Says no, after logging why, if the store cannot be read or no standby segment is placed. A record whose epoch is no
/// longer this writer's passes: the conditional write that follows is what tells the writer it lost the stream.
async fn has_standby_segment(shared: &Shared, roll_reason: &RollReason) -> bool {
    let stream_before = match shared.context.metadata.get_stream(&shared.stream_name).await {
        Ok(stream) => stream,
        Err(meta_error) => {
            tracing::warn!(stream = %shared.stream_name, %meta_error, "cannot roll yet");
            return false;
        }
    };
    if stream_before.standby_replicas.is_none() && stream_before.epoch == shared.epoch {
        tracing::warn!(stream = %shared.stream_name, ?roll_reason, "no standby segment to roll into");
        return false;
    }
    true
}

/// Step 1 of a roll: freezes the durable sequence. Returns the segment being left and the position it is sealed at.
fn stop_committing(shared: &Shared, roll_reason: &RollReason) -> (SegmentDescriptor, SequenceNumber) {
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
    (old_segment, sealed_end)
}

/// Step 2 of a roll: the conditional write that seals `old_segment` at `sealed_end` and opens the next segment.
///
/// The write is repeated until its outcome is known. Returns the stream's record after the write and the new segment,
/// or `None` if the roll did not happen: a writer that lost the stream stops, and any other refusal resumes committing
/// on the old segment.
async fn record_roll(
    shared: &Shared,
    old_segment: SegmentDescriptor,
    sealed_end: SequenceNumber,
) -> Option<(StreamRecord, SegmentRecord)> {
    match write_roll_until_known(shared, old_segment, sealed_end).await {
        Ok(rolled) => Some(rolled),
        Err(MetaError::Fenced { current_epoch, .. }) => {
            tracing::warn!(stream = %shared.stream_name, %current_epoch, "lost the stream");
            shared.stop(&AppendError::Fenced);
            None
        }
        Err(meta_error) => {
            tracing::warn!(stream = %shared.stream_name, %meta_error, "roll did not happen");
            shared.lock().abandon_roll();
            shared.announce_change();
            None
        }
    }
}

/// Repeats the roll's conditional write, one retry interval apart, for as long as the store cannot say whether it
/// happened. Until its outcome is known nothing may commit on either segment.
async fn write_roll_until_known(
    shared: &Shared,
    old_segment: SegmentDescriptor,
    sealed_end: SequenceNumber,
) -> Result<(StreamRecord, SegmentRecord), MetaError> {
    loop {
        let outcome = shared
            .context
            .metadata
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
            known_outcome => return known_outcome,
        }
    }
}

/// Connects to the replicas of the segment a roll opened.
///
/// The new replica set must be reachable before anything can commit, so its addresses are looked up, one retry interval
/// apart, until the registry answers.
async fn connect_new_replicas(shared: &Shared, new_segment: &SegmentRecord) -> Vec<ConnectedReplica> {
    loop {
        match shared.context.connect_copies(&new_segment.copies).await {
            Ok(replicas) => return replicas,
            Err(meta_error) => {
                tracing::warn!(stream = %shared.stream_name, %meta_error, "cannot resolve replicas");
                sleep(shared.settings.retry_interval).await;
            }
        }
    }
}
