//! Taking over a stream: the failover protocol (design, section 9).
//!
//! A new writer raises the epoch, fences the previous writer's segment,
//! seals it at the longest copy it can find, and opens a new segment. No
//! committed record can be lost, because the replicas it hears from overlap
//! every write quorum, and the old writer cannot commit anything after the
//! fence. Every step is idempotent, so a recovery that crashes is simply
//! started again by the next candidate under a higher epoch.

use std::sync::Arc;

use bitacora_meta::{MetaError, SegmentRecord, StreamRecord};
use bitacora_types::replica_api::{
    AppendRequest, FenceRequest, ReadLimit, ReadRequest, ReplicaApi, ReplicaError, SealRequest,
};
use bitacora_types::{Epoch, SegmentAddress, SequenceNumber};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep, timeout, timeout_at};

use crate::context::{ClientContext, ConnectedReplica};
use crate::error::OpenError;
use crate::settings::{TakeoverPolicy, WriterSettings};

/// A stream this writer now owns, with its open segment.
pub(crate) struct OpenedStream {
    pub stream: StreamRecord,
    pub epoch: Epoch,
    pub segment: SegmentRecord,
    pub replicas: Vec<ConnectedReplica>,
}

/// One replica's answer to a fence request.
struct Responder {
    replica: ConnectedReplica,
    /// The end of the replica's copy; the position before the segment's
    /// first record if it has no copy.
    local_end: SequenceNumber,
    sealed_end: Option<SequenceNumber>,
}

/// Runs the takeover protocol and returns the stream with a fresh open
/// segment that only this writer appends to.
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

    // Step 2: the record returned by the claim reflects every roll the old
    // writer completed before losing the epoch. Steps 3 to 7 apply only if
    // its last segment was left open.
    if let Some(open_segment) = stream.open_segment() {
        recover_segment(context, settings, &stream, open_segment, epoch, deadline).await?;
    }

    // Step 8: open the next segment on the standby replica set.
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

/// Waits for the previous writer's lease to lapse, unless the caller forces
/// the takeover. The lease is a courtesy: correctness depends only on the
/// epoch.
async fn wait_for_lease(
    context: &ClientContext,
    stream_name: &str,
    policy: TakeoverPolicy,
    deadline: Instant,
) -> Result<StreamRecord, OpenError> {
    loop {
        let stream = context.metadata.get_stream(stream_name).await?;
        let now = context.clock.now();
        let live_lease = stream
            .writer
            .as_ref()
            .filter(|lease| policy == TakeoverPolicy::WaitForLeaseExpiry && lease.expires_at > now);
        let Some(lease) = live_lease else {
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

/// Steps 3 to 7: fence the previous writer's segment, find its end, repair
/// the tail, and seal it.
async fn recover_segment(
    context: &ClientContext,
    settings: &WriterSettings,
    stream: &StreamRecord,
    segment: &SegmentRecord,
    epoch: Epoch,
    deadline: Instant,
) -> Result<(), OpenError> {
    let address = segment.address(stream.stream_id);
    let replicas = context.connect_copies(&segment.copies).await?;
    let needed = replicas
        .len()
        .saturating_sub(stream.replication.write_quorum())
        + 1;
    let empty_end = segment.first_sequence.preceding();

    let recovery_end = loop {
        // Steps 3 and 4: fence, and wait for N - W + 1 replies. Any
        // committed record is on at least W replicas, so at least one
        // responder holds it.
        let responders =
            fence_replicas(settings, &replicas, address, epoch, needed, empty_end).await?;
        if responders.len() >= needed {
            // Step 5: the recovery end is the highest local end among the
            // responders, unless one of them already knows the segment's
            // sealed end from an earlier, interrupted recovery.
            let recovery_end = responders
                .iter()
                .find_map(|responder| responder.sealed_end)
                .or_else(|| responders.iter().map(|responder| responder.local_end).max())
                .unwrap_or(empty_end);
            // Step 6: every responder must hold the segment up to the
            // recovery end before it is sealed there.
            match repair_tail(settings, stream, segment, &responders, epoch, recovery_end).await {
                Ok(()) => break recovery_end,
                Err(repair_error) => {
                    tracing::warn!(segment = %address, %repair_error, "tail repair failed");
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(OpenError::RecoveryQuorumUnavailable {
                segment: address,
                needed,
                reached: responders.len(),
            });
        }
        sleep(settings.retry_interval).await;
    };

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
    tracing::info!(segment = %address, %recovery_end, "sealed the previous writer's segment");
    announce_seal(&replicas, address, recovery_end, settings).await;
    Ok(())
}

/// Sends the fence to every replica at once and collects the answers until
/// `needed` have arrived plus a short grace for the rest.
async fn fence_replicas(
    settings: &WriterSettings,
    replicas: &[ConnectedReplica],
    address: SegmentAddress,
    epoch: Epoch,
    needed: usize,
    empty_end: SequenceNumber,
) -> Result<Vec<Responder>, OpenError> {
    let mut fence_tasks = JoinSet::new();
    for replica in replicas {
        let replica = replica.clone();
        let request_timeout = settings.request_timeout;
        fence_tasks.spawn(async move {
            let request = FenceRequest {
                segment: address,
                epoch,
            };
            let answer = timeout(request_timeout, replica.client.fence(request)).await;
            (replica, answer)
        });
    }

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
        match answer {
            Ok(Ok(fence_answer)) => {
                responders.push(Responder {
                    replica,
                    local_end: fence_answer.local_end.unwrap_or(empty_end),
                    sealed_end: fence_answer.sealed_end,
                });
                if responders.len() >= needed && grace_ends_at.is_none() {
                    grace_ends_at = Some(Instant::now() + settings.straggler_grace);
                }
            }
            Ok(Err(ReplicaError::Fenced { fenced_epoch, .. })) => {
                return Err(OpenError::TakenByAnotherWriter {
                    current_epoch: fenced_epoch,
                });
            }
            Ok(Err(replica_error)) => {
                tracing::warn!(replica = %replica.replica_id, %replica_error, "fence failed");
            }
            Err(_timed_out) => {
                tracing::warn!(replica = %replica.replica_id, "fence timed out");
            }
        }
    }
    Ok(responders)
}

/// Copies missing records from the longest responder to the others, until
/// every responder holds the segment up to `recovery_end`.
async fn repair_tail(
    settings: &WriterSettings,
    stream: &StreamRecord,
    segment: &SegmentRecord,
    responders: &[Responder],
    epoch: Epoch,
    recovery_end: SequenceNumber,
) -> Result<(), ReplicaError> {
    let Some(longest) = responders
        .iter()
        .find(|responder| responder.local_end >= recovery_end)
    else {
        // Only possible when the end came from a replica that knows the
        // seal but lost the tail: the seal already happened, and repair of
        // sealed segments is the controller's job.
        return Ok(());
    };
    let address = segment.address(stream.stream_id);
    for short in responders
        .iter()
        .filter(|responder| responder.local_end < recovery_end)
    {
        let mut copied_end = short.local_end;
        while copied_end < recovery_end {
            let read = with_timeout(
                settings,
                longest.replica.client.read(ReadRequest {
                    segment: address,
                    from_sequence: copied_end.next(),
                    max_bytes: settings.max_request_bytes,
                    wait_for_records: None,
                    limit: ReadLimit::LocalEnd {
                        recovery_epoch: epoch,
                    },
                }),
            )
            .await?;
            if read.batch_frames.is_empty() {
                return Err(ReplicaError::Unavailable {
                    reason: format!(
                        "replica {} no longer holds records after {copied_end}",
                        longest.replica.replica_id
                    ),
                });
            }
            let appended = with_timeout(
                settings,
                short.replica.client.append(AppendRequest {
                    segment: segment.descriptor(stream.stream_id),
                    writer_epoch: epoch,
                    batch_frames: read.batch_frames,
                    // Unknown to the recovering writer; the seal that
                    // follows tells the replica what it may serve.
                    durable_sequence: SequenceNumber::ZERO,
                }),
            )
            .await?;
            if appended.local_end <= copied_end {
                return Err(ReplicaError::Unavailable {
                    reason: format!(
                        "replica {} made no progress past {copied_end}",
                        short.replica.replica_id
                    ),
                });
            }
            copied_end = appended.local_end;
        }
    }
    Ok(())
}

/// Tells every replica of a segment its sealed end, waiting only briefly.
/// A replica that misses it learns it from the controller's reconciliation.
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

async fn with_timeout<Response>(
    settings: &WriterSettings,
    request: impl Future<Output = Result<Response, ReplicaError>>,
) -> Result<Response, ReplicaError> {
    timeout(settings.request_timeout, request)
        .await
        .unwrap_or_else(|_elapsed| {
            Err(ReplicaError::Unavailable {
                reason: "request timed out".to_owned(),
            })
        })
}

/// Maps a lost epoch race to its own error and passes everything else on.
fn lost_race_or_metadata(meta_error: MetaError) -> OpenError {
    match meta_error {
        MetaError::Fenced { current_epoch, .. } => {
            OpenError::TakenByAnotherWriter { current_epoch }
        }
        other => OpenError::Metadata(other),
    }
}
