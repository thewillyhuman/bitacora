//! After the chaos: healing the cluster and auditing what is left of the run.
//!
//! # Why this file exists
//!
//! While faults are being injected a cluster is allowed to refuse work: safety is required always, liveness only once
//! the faults are gone (design, section 15). So a run cannot end when the chaos does. Two things are still unknown
//! then. One is whether the cluster recovers at all: whether, with every fault removed, it can take a write and serve
//! the whole stream back. The other is which of the appends and writes whose callers never learned their outcome took
//! effect, which the checkers need to know to judge the reads of the run. This file removes the faults and then finds
//! both out.
//!
//! # What it contains
//!
//! - [`heal`]: removes every fault and restarts every storage node.
//! - [`audit`]: commits a final record through a new key-value store, reads every key, reads the whole stream back,
//!   checks that truncation stayed behind the store's checkpoint (I5), and waits for every sealed segment to be whole
//!   again.
//! - `is_where_it_belongs`: whether a segment needs nothing more from the controller.
//!
//! # How it works
//!
//! Healing stops the clients first, so that nothing but the audit writes from then on, then makes both stores
//! reachable, heals every link and restarts every node, running or not: a restart is what clears a disk that refuses
//! flushes, and a node that the nemesis left down must come back.
//!
//! The audit is a sequence of steps, each retried every half second until it works. The simulation bounds the whole
//! audit with a timeout; a cluster that does not get through it in time is reported as stuck, with the step it was at
//! (`diagnosis.rs`). The key-value store the audit opens stays open to the end, because its writer's heartbeats are
//! what keeps the durable sequence of the stream current while the stream is read.
//!
//! The read of the whole stream starts at the low-water mark: whatever was truncated can no longer be read, and whether
//! it was right to truncate it is a question of its own, which the audit answers by comparing the mark with the store's
//! committed checkpoint.
//!
//! # Invariants
//!
//! - Healing awaits in a fixed order: the clients end, then each node is healed and restarted in order of its number.
//!   That order is part of what a seed means, and so is the order of the audit's steps.
//! - The audit's session is recorded only for a read that reached the final record, so a history never holds half an
//!   audit.
//! - The audit returns only when every sealed segment is where it belongs; the caller bounds the wait.
//! - [`audit`] is one function of some thirty lines, because the store and the reader it opens must stay alive across
//!   its steps and its awaits must stay in this order.
//!
//! # See also
//!
//! - `simulation.rs`: calls these between the chaos and the verdict.
//! - `kv_workload.rs`: the final record and the read of every key.
//! - `diagnosis.rs`: what is reported when the audit does not finish.
//! - `checker.rs`: `check_audit`, which judges the session this file returns.

use std::time::Duration;

use bitacora_client::{StreamReader, TakeoverPolicy};
use bitacora_kv::{KV_MATERIALIZER, KvStore};
use bitacora_meta::{CopyState, SegmentRecord, StreamRecord};
use bitacora_testkit::LinkFault;
use bitacora_types::{SequenceNumber, SequenceRange};
use tokio::task::JoinSet;
use tokio::time::sleep;

use crate::kv_workload::{commit_final_record, read_every_key, store_settings};
use crate::world::{AuditStep, STREAM, World};

/// How long the audit waits before it tries a step again.
const RETRY_INTERVAL: Duration = Duration::from_millis(500);

/// The identity the audit's clients and stores use.
const AUDITOR: &str = "auditor";

/// The final record of a run, and the store that committed it.
struct FinalRecord {
    /// Where the final record was committed. The audit reads the stream up to here.
    sequence: SequenceNumber,
    /// The store that committed it. Kept open until the audit is done, so that its writer's heartbeats keep the durable
    /// sequence of the stream current.
    store: KvStore,
}

/// One successful read of the whole stream.
struct WholeStreamRead {
    /// The reading session the records were recorded under.
    session: u64,
    /// The low-water mark the read started at.
    low_water_mark: SequenceNumber,
}

/// Removes every fault and makes every process run again.
///
/// Stops the clients and waits until they are gone, makes the metadata store and the object store reachable, then, for
/// each node in order, heals its link, kills its replica and starts it again. Records one entry in the history when it
/// is done.
///
/// # Arguments
///
/// * `world` - The run to heal.
/// * `clients` - The compute nodes and readers of the run. They are aborted and joined.
pub(crate) async fn heal(world: &World, clients: &mut JoinSet<()>) {
    clients.abort_all();
    while clients.join_next().await.is_some() {}
    world.cluster.key_value_store().set_unavailable(false);
    world.cluster.artifact_store().set_unavailable(false);
    for node in world.cluster.nodes() {
        node.link().set_fault(LinkFault::default());
        node.crash();
        node.restart().await;
    }
    world.history.fault("healed: every fault removed, every node restarted");
}

/// Audits a healed cluster: a fresh key-value store commits one last record, every key is read through the store, and
/// the whole stream is read back.
///
/// Each step is retried until it succeeds, so the function returns only if the cluster recovers; the caller bounds it
/// with a timeout. The step in progress is noted in the world, for the report of a run that does not get through.
///
/// # Arguments
///
/// * `world` - The healed run. The audit records its calls and its reads in the world's history.
///
/// # Returns
///
/// The reading session under which the whole stream was recorded, for `check_audit`.
///
/// # Errors
///
/// Returns a description of the failure if the metadata store cannot be asked for the stream or for the store's
/// checkpoint when truncation is checked.
pub(crate) async fn audit(world: &World) -> Result<u64, String> {
    world.now_waiting_for(AuditStep::CommittingFinalRecord);
    let final_record = commit_final_record_through_a_new_store(world).await;

    world.now_waiting_for(AuditStep::ReadingEveryKey);
    while !read_every_key(&final_record.store, &world.history).await {
        sleep(RETRY_INTERVAL).await;
    }

    world.now_waiting_for(AuditStep::ReadingWholeStream);
    let reader = world.client(AUDITOR).open_reader(STREAM);
    let whole_stream = read_whole_stream_back(world, &reader, final_record.sequence).await;
    check_truncation(world, whole_stream.low_water_mark).await?;

    world.now_waiting_for(AuditStep::WaitingForFullReplication);
    wait_for_full_replication(world).await;
    Ok(whole_stream.session)
}

/// Opens key-value stores, one after the other, until one of them commits the final record of the run.
///
/// Each attempt takes the stream over by force and writes a value of its own. An attempt whose store does not open, or
/// does not commit in time, is abandoned and its store closed before the next one starts.
async fn commit_final_record_through_a_new_store(world: &World) -> FinalRecord {
    let mut attempt = 0_u64;
    loop {
        attempt += 1;
        let opened = KvStore::open(
            world.store_dependencies(AUDITOR),
            STREAM,
            store_settings(TakeoverPolicy::Force),
        )
        .await;
        if let Ok(store) = opened
            && let Some(sequence) = commit_final_record(&store, &world.history, attempt).await
        {
            return FinalRecord { sequence, store };
        }
        sleep(RETRY_INTERVAL).await;
    }
}

/// Reads the stream from its low-water mark to `final_sequence`, again and again until one read returns all of it, and
/// records that read in the history as one session.
///
/// Whatever was truncated can no longer be read; the audit covers the rest.
async fn read_whole_stream_back(
    world: &World,
    reader: &StreamReader,
    final_sequence: SequenceNumber,
) -> WholeStreamRead {
    loop {
        if let Ok(low_water_mark) = reader.low_water_mark().await
            && let Ok(whole_stream) = SequenceRange::new(low_water_mark, final_sequence)
            && let Ok(records) = reader.read_range(whole_stream).await
            && records.last().map(|record| record.sequence) == Some(final_sequence)
        {
            let session = world.history.reader_started(low_water_mark);
            for record in records {
                world.history.record_read(session, record.sequence, record.payload);
            }
            return WholeStreamRead {
                session,
                low_water_mark,
            };
        }
        sleep(RETRY_INTERVAL).await;
    }
}

/// Checks safe truncation (I5): the low-water mark must not have passed the key-value store's committed checkpoint. If
/// it has, the fact is recorded in the history, where the checker finds it.
///
/// # Errors
///
/// Returns the metadata store's error, as text, if the stream or the checkpoint cannot be read.
async fn check_truncation(world: &World, low_water_mark: SequenceNumber) -> Result<(), String> {
    let metadata = world.cluster.metadata();
    let stream = match metadata.get_stream(STREAM).await {
        Ok(stream) => stream,
        Err(meta_error) => return Err(meta_error.to_string()),
    };
    let checkpoint = match metadata.get_checkpoint(stream.stream_id, KV_MATERIALIZER).await {
        Ok(checkpoint_record) => checkpoint_record.checkpoint,
        Err(meta_error) => return Err(meta_error.to_string()),
    };
    if low_water_mark > checkpoint.next() {
        world.history.truncated_past_checkpoint(checkpoint, low_water_mark);
    }
    Ok(())
}

/// Waits until the controller has brought every sealed segment back to where it belongs (design, section 18, the exit
/// gate of M3): a full set of good copies, or the object store alone once its hot-read window is over. It returns only
/// then; the caller bounds the wait.
async fn wait_for_full_replication(world: &World) {
    loop {
        if let Ok(stream) = world.cluster.metadata().get_stream(STREAM).await
            && is_every_segment_where_it_belongs(&stream)
        {
            return;
        }
        sleep(RETRY_INTERVAL).await;
    }
}

/// Says whether no segment of `stream` needs anything more from the controller.
fn is_every_segment_where_it_belongs(stream: &StreamRecord) -> bool {
    for segment in &stream.segments {
        if !is_where_it_belongs(stream, segment) {
            return false;
        }
    }
    true
}

/// Says whether `segment` needs nothing more from the controller.
///
/// That is the case for an open segment, which is the writer's business; for a segment below the low-water mark, of
/// which only the deletion is pending; for a segment that lives in the object store alone; and for a sealed segment
/// with a full set of good copies.
fn is_where_it_belongs(stream: &StreamRecord, segment: &SegmentRecord) -> bool {
    let Some(sealed_end) = segment.sealed_end() else {
        return true;
    };
    if sealed_end < stream.low_water_mark {
        return true;
    }
    let is_released = match &segment.tiered {
        Some(tiered) => segment.copies.is_empty() && tiered.are_local_copies_dropped,
        None => false,
    };
    is_released || has_every_good_copy(stream, segment)
}

/// Says whether `segment` has as many copies as the stream's replication asks for, all of them good.
fn has_every_good_copy(stream: &StreamRecord, segment: &SegmentRecord) -> bool {
    if segment.copies.len() != stream.replication.replica_count() {
        return false;
    }
    for copy in &segment.copies {
        if copy.state != CopyState::Good {
            return false;
        }
    }
    true
}

#[cfg(test)]
#[path = "audit_test.rs"]
mod tests;
