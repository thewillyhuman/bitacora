//! Taking over a stream: the failover protocol.
//!
//! # Why this file exists
//!
//! A stream has one writer at a time, and writers die. The next one must continue the stream without losing a record
//! that was reported as committed, and without letting the old writer, which may only be slow, commit anything more
//! (design, section 9). bitacora does not choose who the next writer is; it guarantees that whoever asks last wins
//! cleanly. This file is what "asking" does: it is run by every writer when it is opened.
//!
//! # What it contains
//!
//! - [`open_stream`]: the whole protocol, from waiting for the lease to an open segment.
//! - [`OpenedStream`]: what the protocol hands to the writer.
//! - [`announce_seal`], [`announce_seal_of_listed_segment`]: telling replicas where a segment ended, used by takeover,
//!   by rolls and by a clean close.
//! - The steps as private functions (`wait_for_lease`, `recover_segment`, `fence_replicas`, `repair_tail`,
//!   `open_next_segment`) and the decisions inside them as pure ones (`live_lease`, `recovery_quorum`,
//!   `choose_recovery_end`).
//!
//! # How it works
//!
//! The steps are the eight of the design:
//!
//! 1. **Claim the epoch.** A conditional write raises the stream's epoch. If it fails, another candidate is ahead.
//! 2. **Read the segment list.** The record returned by the claim reflects every roll the old writer completed before
//!    it lost the epoch. If its last segment is sealed, or there is none, skip to step 8.
//! 3. **Fence.** Ask every replica of the open segment to refuse writers below the new epoch.
//! 4. **Wait for N − W + 1 answers.** Any committed record is on at least W replicas, so at least one responder holds
//!    it. After the quorum is reached, the rest get a short grace: hearing from all of them avoids repairs later.
//! 5. **Choose the end.** The highest local end among the responders, unless one of them already knows the segment's
//!    sealed end from an earlier, interrupted recovery.
//! 6. **Repair the tail.** Copy what the shorter responders lack from the longest one, under the new epoch.
//! 7. **Seal.** One conditional write records the end. Then the replicas are told.
//! 8. **Open.** A conditional write opens the next segment on the standby replica set the controller placed.
//!
//! Steps 3 to 6 are repeated until they succeed or the open timeout passes. Before step 1 the candidate waits for the
//! previous writer's lease to lapse, unless told to force the takeover.
//!
//! # Invariants
//!
//! - No committed record is lost: the recovery quorum overlaps every write quorum, and the end is the maximum.
//! - The old writer cannot commit after the fence: it can collect at most W − 1 acknowledgements, and its metadata
//!   writes fail on the epoch.
//! - Every step can be repeated. A recovery that stops halfway is started again by the next candidate under a higher
//!   epoch; two attempts may compute different ends, but only one seal is written.
//! - Nothing here is on the append path. The code favours being readable over being fast.
//!
//! # See also
//!
//! - `../writer.rs`: the caller, and what happens once the segment is open.
//! - `bitacora-meta/src/store/writers.rs`: the conditional writes of steps 1, 7 and 8.
//! - `docs/protocol/replication.md`, "Taking over a stream": the same protocol for readers who do not read Rust.
//! - `bitacora-model`: the executable model that checks it exhaustively.

use std::sync::Arc;

use bitacora_meta::{MetaError, SegmentRecord, StreamRecord, WriterLease};
use bitacora_types::replica_api::{
    AppendRequest, FenceRequest, FenceResponse, ReadLimit, ReadRequest, ReplicaApi, ReplicaError, SealRequest,
    SegmentDescriptor,
};
use bitacora_types::{Epoch, SegmentAddress, SequenceNumber, Timestamp};
use bytes::Bytes;
use tokio::task::JoinSet;
use tokio::time::error::Elapsed;
use tokio::time::{Instant, sleep, timeout, timeout_at};

use crate::context::{ClientContext, ConnectedReplica};
use crate::error::OpenError;
use crate::settings::{TakeoverPolicy, WriterSettings};

/// A stream this writer now owns, with its open segment.
pub(crate) struct OpenedStream {
    /// The stream's record as of the write that opened the segment.
    pub stream: StreamRecord,
    /// The epoch the writer claimed.
    pub epoch: Epoch,
    /// The segment only this writer appends to.
    pub segment: SegmentRecord,
    /// The segment's replicas, connected, in the order of its replica set.
    pub replicas: Vec<ConnectedReplica>,
}

/// One replica's answer to a fence request.
struct Responder {
    /// The replica that answered.
    replica: ConnectedReplica,
    /// The end of the replica's copy; the position before the segment's first record if it has no copy.
    local_end: SequenceNumber,
    /// The segment's sealed end, if the replica already knows it from an earlier, interrupted recovery.
    sealed_end: Option<SequenceNumber>,
}

/// What a replica answered to a fence, with the replica that answered: the result of one task of step 3.
type FenceAnswer = (ConnectedReplica, Result<Result<FenceResponse, ReplicaError>, Elapsed>);

/// What recovering a segment needs to know about it.
struct Recovery<'takeover> {
    /// The writer's settings: timeouts, the grace for stragglers, the retry interval.
    settings: &'takeover WriterSettings,
    /// The segment's cluster-wide address.
    address: SegmentAddress,
    /// What a replica needs to create a copy of the segment.
    descriptor: SegmentDescriptor,
    /// The epoch the recovering writer claimed.
    epoch: Epoch,
    /// How many replicas must answer the fence (N − W + 1).
    needed: usize,
    /// The end of a copy that holds nothing: the position before the segment's first record.
    empty_end: SequenceNumber,
}

/// Runs the takeover protocol and returns the stream with a fresh open segment that only this writer appends to.
///
/// See the file header for the steps. The call takes as long as recovery needs, bounded by the settings'
/// `open_timeout`.
///
/// # Arguments
///
/// * `context` - The injected interfaces of the client.
/// * `settings` - The writer's settings.
/// * `stream_name` - The stream to take over.
/// * `policy` - Whether to wait for the previous writer's lease to lapse first.
///
/// # Returns
///
/// The stream, the claimed epoch, and the new open segment with its replicas connected.
///
/// # Errors
///
/// * [`OpenError::Metadata`] if the stream does not exist or the metadata store fails.
/// * [`OpenError::LeaseHeld`] if a live writer's lease outlasts the open timeout and `policy` says to wait.
/// * [`OpenError::TakenByAnotherWriter`] if another candidate claimed the stream, before or during recovery.
/// * [`OpenError::RecoveryQuorumUnavailable`] if too few replicas of the previous writer's segment answered in time.
/// * [`OpenError::NoStandbySegment`] if no standby segment was placed in time.
pub(crate) async fn open_stream(
    context: &ClientContext,
    settings: &WriterSettings,
    stream_name: &str,
    policy: TakeoverPolicy,
) -> Result<OpenedStream, OpenError> {
    let deadline = Instant::now() + settings.open_timeout;
    let stream_before = wait_for_lease(context, stream_name, policy, deadline).await?;

    // Step 1: claim the epoch. If the swap fails, someone else is ahead.
    let lease_expires_at = context.clock.now().plus(settings.lease_duration);
    let stream = context
        .metadata
        .claim_epoch(
            stream_name,
            stream_before.epoch,
            &settings.writer_identity,
            lease_expires_at,
        )
        .await
        .map_err(lost_race_or_metadata)?;
    let epoch = stream.epoch;
    tracing::info!(stream = stream_name, %epoch, "claimed the stream");

    // Step 2: the record returned by the claim reflects every roll the old writer completed before losing the epoch.
    // Steps 3 to 7 apply only if its last segment was left open.
    if let Some(open_segment) = stream.open_segment() {
        recover_segment(context, settings, &stream, open_segment, epoch, deadline).await?;
    }
    open_next_segment(context, settings, stream_name, epoch, deadline).await
}

/// Waits for the previous writer's lease to lapse, unless the caller forces the takeover.
///
/// The lease is a courtesy: correctness depends only on the epoch. Returns the stream's record as last read, whose
/// epoch the claim is conditional on.
async fn wait_for_lease(
    context: &ClientContext,
    stream_name: &str,
    policy: TakeoverPolicy,
    deadline: Instant,
) -> Result<StreamRecord, OpenError> {
    loop {
        let stream = context.metadata.get_stream(stream_name).await?;
        let now = context.clock.now();
        let Some(lease) = live_lease(stream.writer.as_ref(), policy, now) else {
            return Ok(stream);
        };
        let lapses_at = Instant::now() + lease.expires_at.duration_since(now);
        if lapses_at > deadline {
            return Err(OpenError::LeaseHeld {
                writer_identity: lease.writer_identity.clone(),
                expires_at: lease.expires_at,
            });
        }
        tokio::time::sleep_until(lapses_at).await;
    }
}

/// The lease a candidate has to wait for: `lease`, if the policy says to respect leases and it has not lapsed at `now`.
/// `None` means the candidate may claim the stream at once.
fn live_lease(lease: Option<&WriterLease>, policy: TakeoverPolicy, now: Timestamp) -> Option<&WriterLease> {
    let lease = lease?;
    let is_respected = policy == TakeoverPolicy::WaitForLeaseExpiry;
    if is_respected && lease.expires_at > now {
        Some(lease)
    } else {
        None
    }
}

/// How many replicas recovery must hear from so that at least one of them holds every committed record: N − W + 1.
fn recovery_quorum(replica_count: usize, write_quorum: usize) -> usize {
    replica_count.saturating_sub(write_quorum) + 1
}

/// Steps 3 to 7: fences the previous writer's segment, finds its end, repairs the tail, and seals it.
async fn recover_segment(
    context: &ClientContext,
    settings: &WriterSettings,
    stream: &StreamRecord,
    segment: &SegmentRecord,
    epoch: Epoch,
    deadline: Instant,
) -> Result<(), OpenError> {
    let replicas = context.connect_copies(&segment.copies).await?;
    let recovery = Recovery {
        settings,
        address: segment.address(stream.stream_id),
        descriptor: segment.descriptor(stream.stream_id),
        epoch,
        needed: recovery_quorum(replicas.len(), stream.replication.write_quorum()),
        empty_end: segment.first_sequence.preceding(),
    };
    let recovery_end = find_and_repair_the_end(&recovery, &replicas, deadline).await?;

    // Step 7: seal with one conditional write, then tell the replicas.
    context
        .metadata
        .seal_segment(
            &stream.name,
            epoch,
            segment.segment_id,
            recovery_end,
            context.clock.now(),
        )
        .await
        .map_err(lost_race_or_metadata)?;
    tracing::info!(segment = %recovery.address, %recovery_end, "sealed the previous writer's segment");
    announce_seal(&replicas, recovery.address, recovery_end, settings).await;
    Ok(())
}

/// Steps 3 to 6, repeated until they succeed or `deadline` passes: fences the replicas, chooses the recovery end from a
/// quorum of answers, and makes every responder hold the segment up to it. Returns the recovery end.
async fn find_and_repair_the_end(
    recovery: &Recovery<'_>,
    replicas: &[ConnectedReplica],
    deadline: Instant,
) -> Result<SequenceNumber, OpenError> {
    loop {
        // Steps 3 and 4: fence, and wait for N - W + 1 replies. Any committed record is on at least W replicas, so at
        // least one responder holds it.
        let responders = fence_replicas(recovery, replicas).await?;
        if responders.len() >= recovery.needed {
            // Step 5.
            let recovery_end = choose_recovery_end(&responders, recovery.empty_end);
            // Step 6: every responder must hold the segment up to the recovery end before it is sealed there.
            match repair_tail(recovery, &responders, recovery_end).await {
                Ok(()) => return Ok(recovery_end),
                Err(repair_error) => {
                    tracing::warn!(segment = %recovery.address, %repair_error, "tail repair failed");
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(OpenError::RecoveryQuorumUnavailable {
                segment: recovery.address,
                needed: recovery.needed,
                reached: responders.len(),
            });
        }
        sleep(recovery.settings.retry_interval).await;
    }
}

/// Step 5: the position the segment is sealed at.
///
/// It is the highest local end among the responders, unless one of them already knows the segment's sealed end from an
/// earlier, interrupted recovery: a seal that was written is never changed. Without responders it is `empty_end`.
fn choose_recovery_end(responders: &[Responder], empty_end: SequenceNumber) -> SequenceNumber {
    let mut highest_local_end: Option<SequenceNumber> = None;
    for responder in responders {
        if let Some(sealed_end) = responder.sealed_end {
            return sealed_end;
        }
        highest_local_end = highest_local_end.max(Some(responder.local_end));
    }
    highest_local_end.unwrap_or(empty_end)
}

/// Steps 3 and 4: sends the fence to every replica at once and collects the answers until the recovery quorum has
/// arrived, plus a short grace for the rest.
///
/// Fails at once if a replica is already fenced at a higher epoch: a newer writer is taking the stream.
async fn fence_replicas(recovery: &Recovery<'_>, replicas: &[ConnectedReplica]) -> Result<Vec<Responder>, OpenError> {
    let mut fence_tasks = send_fences(recovery, replicas);
    let mut responders = Vec::new();
    let mut grace_ends_at: Option<Instant> = None;
    loop {
        let next_answer = match grace_ends_at {
            Some(grace_ends_at) => match timeout_at(grace_ends_at, fence_tasks.join_next()).await {
                Ok(next_answer) => next_answer,
                Err(_grace_elapsed) => break,
            },
            None => fence_tasks.join_next().await,
        };
        let Some(joined) = next_answer else {
            break;
        };
        let Ok((replica, answer)) = joined else {
            continue;
        };
        if let Some(responder) = responder_from(replica, answer, recovery.empty_end)? {
            responders.push(responder);
            if responders.len() >= recovery.needed && grace_ends_at.is_none() {
                grace_ends_at = Some(Instant::now() + recovery.settings.straggler_grace);
            }
        }
    }
    Ok(responders)
}

/// Starts one task per replica that sends it the fence, each bounded by the request timeout.
fn send_fences(recovery: &Recovery<'_>, replicas: &[ConnectedReplica]) -> JoinSet<FenceAnswer> {
    let mut fence_tasks = JoinSet::new();
    for replica in replicas {
        let replica = replica.clone();
        let request_timeout = recovery.settings.request_timeout;
        let request = FenceRequest {
            segment: recovery.address,
            epoch: recovery.epoch,
        };
        fence_tasks.spawn(async move {
            let answer = timeout(request_timeout, replica.client.fence(request)).await;
            (replica, answer)
        });
    }
    fence_tasks
}

/// Turns one replica's answer to the fence into a responder.
///
/// Returns `None`, after logging why, for a replica that failed or did not answer in time: it simply does not count.
/// Fails if the replica is fenced at a higher epoch, which ends this takeover.
fn responder_from(
    replica: ConnectedReplica,
    answer: Result<Result<FenceResponse, ReplicaError>, Elapsed>,
    empty_end: SequenceNumber,
) -> Result<Option<Responder>, OpenError> {
    match answer {
        Ok(Ok(fence_answer)) => Ok(Some(Responder {
            replica,
            local_end: fence_answer.local_end.unwrap_or(empty_end),
            sealed_end: fence_answer.sealed_end,
        })),
        Ok(Err(ReplicaError::Fenced { fenced_epoch, .. })) => Err(OpenError::TakenByAnotherWriter {
            current_epoch: fenced_epoch,
        }),
        Ok(Err(replica_error)) => {
            tracing::warn!(replica = %replica.replica_id, %replica_error, "fence failed");
            Ok(None)
        }
        Err(_timed_out) => {
            tracing::warn!(replica = %replica.replica_id, "fence timed out");
            Ok(None)
        }
    }
}

/// Step 6: copies missing records from the longest responder to the others, until every responder holds the segment up
/// to `recovery_end`.
async fn repair_tail(
    recovery: &Recovery<'_>,
    responders: &[Responder],
    recovery_end: SequenceNumber,
) -> Result<(), ReplicaError> {
    let Some(longest) = longest_responder(responders, recovery_end) else {
        // Only possible when the end came from a replica that knows the seal but lost the tail: the seal already
        // happened, and repair of sealed segments is the controller's job.
        return Ok(());
    };
    for short in responders {
        if short.local_end < recovery_end {
            copy_tail(recovery, longest, short, recovery_end).await?;
        }
    }
    Ok(())
}

/// The first responder whose copy reaches `recovery_end`: the one the others are repaired from.
fn longest_responder(responders: &[Responder], recovery_end: SequenceNumber) -> Option<&Responder> {
    responders.iter().find(|responder| responder.local_end >= recovery_end)
}

/// Copies the records after `short`'s local end from `longest` to `short`, request by request, up to `recovery_end`.
async fn copy_tail(
    recovery: &Recovery<'_>,
    longest: &Responder,
    short: &Responder,
    recovery_end: SequenceNumber,
) -> Result<(), ReplicaError> {
    let mut copied_end = short.local_end;
    while copied_end < recovery_end {
        let batch_frames = read_batches_after(recovery, longest, copied_end).await?;
        copied_end = append_batches_after(recovery, short, batch_frames, copied_end).await?;
    }
    Ok(())
}

/// Reads from `longest` the batches that follow `copied_end`, as many as fit in one request.
///
/// The read asks for the copy's local end, not its durable sequence, which only a writer that fenced the copy at its
/// own epoch may do. Fails if the replica returns nothing: it no longer holds what it said it held.
async fn read_batches_after(
    recovery: &Recovery<'_>,
    longest: &Responder,
    copied_end: SequenceNumber,
) -> Result<Bytes, ReplicaError> {
    let request = ReadRequest {
        segment: recovery.address,
        from_sequence: copied_end.next(),
        max_bytes: recovery.settings.max_request_bytes,
        wait_for_records: None,
        limit: ReadLimit::LocalEnd {
            recovery_epoch: recovery.epoch,
        },
    };
    let read = with_timeout(recovery.settings, longest.replica.client.read(request)).await?;
    if read.batch_frames.is_empty() {
        return Err(ReplicaError::Unavailable {
            reason: format!(
                "replica {} no longer holds records after {copied_end}",
                longest.replica.replica_id
            ),
        });
    }
    Ok(read.batch_frames)
}

/// Appends `batch_frames`, which follow `copied_end`, to the copy of `short`, under the recovering writer's epoch.
///
/// Returns the copy's new local end. Fails if it did not move past `copied_end`, because repeating the request would
/// not end.
async fn append_batches_after(
    recovery: &Recovery<'_>,
    short: &Responder,
    batch_frames: Bytes,
    copied_end: SequenceNumber,
) -> Result<SequenceNumber, ReplicaError> {
    let request = AppendRequest {
        segment: recovery.descriptor,
        writer_epoch: recovery.epoch,
        batch_frames,
        // Unknown to the recovering writer; the seal that follows tells the replica what it may serve.
        durable_sequence: SequenceNumber::ZERO,
    };
    let appended = with_timeout(recovery.settings, short.replica.client.append(request)).await?;
    if appended.local_end <= copied_end {
        return Err(ReplicaError::Unavailable {
            reason: format!(
                "replica {} made no progress past {copied_end}",
                short.replica.replica_id
            ),
        });
    }
    Ok(appended.local_end)
}

/// Step 8: opens the next segment on the standby replica set, waiting until `deadline` for the controller to place one
/// if there is none.
async fn open_next_segment(
    context: &ClientContext,
    settings: &WriterSettings,
    stream_name: &str,
    epoch: Epoch,
    deadline: Instant,
) -> Result<OpenedStream, OpenError> {
    loop {
        match context.metadata.open_segment(stream_name, epoch).await {
            Ok((stream, segment)) => {
                let replicas = context.connect_copies(&segment.copies).await?;
                tracing::info!(
                    stream = stream_name,
                    %epoch,
                    segment = %segment.segment_id,
                    first_sequence = %segment.first_sequence,
                    "opened a segment"
                );
                return Ok(OpenedStream {
                    stream,
                    epoch,
                    segment,
                    replicas,
                });
            }
            Err(MetaError::NoStandbySegment { .. }) if Instant::now() < deadline => {
                sleep(settings.retry_interval).await;
            }
            Err(MetaError::NoStandbySegment { name }) => {
                return Err(OpenError::NoStandbySegment { stream: name });
            }
            Err(meta_error) => return Err(lost_race_or_metadata(meta_error)),
        }
    }
}

/// Tells every replica of a segment its sealed end, waiting only briefly.
///
/// One request per replica is sent at once. The call returns when all have answered or the straggler grace has passed,
/// whichever is first; requests still in flight are dropped. A replica that misses the announcement learns the sealed
/// end from the controller's reconciliation.
///
/// # Arguments
///
/// * `replicas` - The segment's replicas, connected.
/// * `address` - The sealed segment.
/// * `sealed_end` - Its final sequence number, as written to the metadata store.
/// * `settings` - The writer's settings; `straggler_grace` bounds the wait.
pub(crate) async fn announce_seal(
    replicas: &[ConnectedReplica],
    address: SegmentAddress,
    sealed_end: SequenceNumber,
    settings: &WriterSettings,
) {
    let mut announcements = JoinSet::new();
    for replica in replicas {
        let client: Arc<dyn ReplicaApi> = Arc::clone(&replica.client);
        announcements.spawn(async move {
            client
                .seal(SealRequest {
                    segment: address,
                    sealed_end,
                })
                .await
        });
    }
    let grace_ends_at = Instant::now() + settings.straggler_grace;
    while let Ok(Some(_announced)) = timeout_at(grace_ends_at, announcements.join_next()).await {}
}

/// Tells the replicas of a segment that `stream` lists its sealed end.
///
/// Looks the segment up in `stream`, connects to its replicas and calls [`announce_seal`]. Used after a roll and after
/// a clean close, where the replica set is taken from the record the seal was written to. Best effort throughout: if
/// the segment is not listed or the registry cannot be read, nothing is announced, and the controller does it later.
///
/// # Arguments
///
/// * `context` - The injected interfaces of the client.
/// * `stream` - The stream's record after the seal.
/// * `address` - The sealed segment.
/// * `sealed_end` - Its final sequence number.
/// * `settings` - The writer's settings; `straggler_grace` bounds the wait.
pub(crate) async fn announce_seal_of_listed_segment(
    context: &ClientContext,
    stream: &StreamRecord,
    address: SegmentAddress,
    sealed_end: SequenceNumber,
    settings: &WriterSettings,
) {
    let listed_segment = stream
        .segments
        .iter()
        .find(|segment| segment.segment_id == address.segment_id);
    let Some(sealed_segment) = listed_segment else {
        return;
    };
    let Ok(replicas) = context.connect_copies(&sealed_segment.copies).await else {
        return;
    };
    announce_seal(&replicas, address, sealed_end, settings).await;
}

/// Awaits `request`, failing it as unavailable if it takes longer than the request timeout.
async fn with_timeout<Response>(
    settings: &WriterSettings,
    request: impl Future<Output = Result<Response, ReplicaError>>,
) -> Result<Response, ReplicaError> {
    match timeout(settings.request_timeout, request).await {
        Ok(answer) => answer,
        Err(_elapsed) => Err(ReplicaError::Unavailable {
            reason: "request timed out".to_owned(),
        }),
    }
}

/// Maps a lost epoch race to its own error and passes everything else on.
fn lost_race_or_metadata(meta_error: MetaError) -> OpenError {
    match meta_error {
        MetaError::Fenced { current_epoch, .. } => OpenError::TakenByAnotherWriter { current_epoch },
        other => OpenError::Metadata(other),
    }
}

#[cfg(test)]
#[path = "takeover_test.rs"]
mod tests;
