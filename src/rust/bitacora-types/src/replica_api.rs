//! The contract between a replica and everyone who talks to it.
//!
//! [`ReplicaApi`] is the injected network interface of the design (section
//! 15): writers, readers and the controller only ever hold a
//! `dyn ReplicaApi`. In production it is a gRPC client; in simulation it is
//! the replica itself behind a fault-injecting link. Because the replica
//! implements the same trait, the protocol is tested without a socket.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

use crate::{Epoch, ReplicaId, SegmentAddress, SequenceNumber};

/// Everything a replica needs to create its copy of a segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentDescriptor {
    /// Which segment this is.
    pub address: SegmentAddress,
    /// The epoch of the only writer that authors records in it.
    pub epoch: Epoch,
    /// The sequence number of its first record.
    pub first_sequence: SequenceNumber,
}

/// Why a replica refused or failed a request.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ReplicaError {
    /// A newer writer has fenced the segment. The caller has lost the stream
    /// and must stop.
    #[error("epoch {request_epoch} is fenced: the replica accepts epoch {fenced_epoch} or later")]
    Fenced {
        /// The epoch the request carried.
        request_epoch: Epoch,
        /// The lowest epoch the replica still accepts.
        fenced_epoch: Epoch,
    },
    /// The segment is sealed and accepts no more appends.
    #[error("segment is sealed at {sealed_end}")]
    Sealed {
        /// The segment's final sequence number.
        sealed_end: SequenceNumber,
    },
    /// The batches leave a gap after the replica's copy. The caller should
    /// resend from `local_end + 1`.
    #[error("batches start at {first_sequence} but the copy ends at {local_end}")]
    OutOfOrder {
        /// The first sequence number of the rejected batches.
        first_sequence: SequenceNumber,
        /// The last sequence number the replica holds.
        local_end: SequenceNumber,
    },
    /// The replica already knows a different sealed end for the segment,
    /// which can only happen if the metadata store was bypassed.
    #[error("segment is already sealed at {existing_sealed_end}, not {requested_sealed_end}")]
    ConflictingSeal {
        /// The sealed end the replica has recorded.
        existing_sealed_end: SequenceNumber,
        /// The sealed end the request asked for.
        requested_sealed_end: SequenceNumber,
    },
    /// The replica has no copy of the segment.
    #[error("replica has no copy of segment {segment}")]
    SegmentNotFound {
        /// The segment that was asked for.
        segment: SegmentAddress,
    },
    /// The request itself is wrong: malformed frames, a failed checksum, or
    /// arguments that contradict the replica's copy. Retrying cannot help.
    #[error("invalid request: {reason}")]
    InvalidRequest {
        /// What is wrong with it.
        reason: String,
    },
    /// The replica's disk is too full for this request. Writers roll to
    /// another replica set.
    #[error("replica is out of space: disk is {used_percent}% full")]
    NoCapacity {
        /// How full the replica's disk is.
        used_percent: u8,
    },
    /// The replica is being drained for maintenance. Writers roll away.
    #[error("replica is draining")]
    Draining,
    /// The replica's copy is damaged or its disk failed. The copy must be
    /// treated as suspect and replaced.
    #[error("replica storage failed: {reason}")]
    Storage {
        /// What the disk reported.
        reason: String,
    },
    /// The replica could not be reached or did not answer in time. The
    /// request may or may not have been applied.
    #[error("replica is unavailable: {reason}")]
    Unavailable {
        /// What went wrong on the way.
        reason: String,
    },
}

impl ReplicaError {
    /// Whether sending the same request again later can succeed.
    #[must_use]
    pub const fn is_retriable(&self) -> bool {
        matches!(self, Self::Unavailable { .. })
    }
}

/// Appends batches to the open segment, or carries only the durable sequence
/// when there is nothing to append (the heartbeat of an idle stream).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendRequest {
    /// The segment to append to. A replica without a copy creates one.
    pub segment: SegmentDescriptor,
    /// The epoch of the writer sending the request.
    pub writer_epoch: Epoch,
    /// Back-to-back batch frames, in sequence order. May be empty.
    pub batch_frames: Bytes,
    /// The writer's current durable sequence: what replicas may serve.
    pub durable_sequence: SequenceNumber,
}

/// The replica's answer to an [`AppendRequest`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendResponse {
    /// The last sequence number flushed in the replica's copy.
    pub local_end: SequenceNumber,
}

/// How far a read may go into the replica's copy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadLimit {
    /// Up to the durable sequence the replica knows. What every reader uses.
    DurableSequence,
    /// Up to the end of the copy, committed or not. Only the writer that
    /// fenced the segment at `recovery_epoch` may ask for this, to repair the
    /// tail during failover.
    LocalEnd {
        /// The epoch the caller fenced the segment with.
        recovery_epoch: Epoch,
    },
}

/// Reads batches from a segment copy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadRequest {
    /// The segment to read.
    pub segment: SegmentAddress,
    /// The first sequence number wanted.
    pub from_sequence: SequenceNumber,
    /// A soft cap on the size of the answer. At least one batch is returned
    /// when one is available.
    pub max_bytes: usize,
    /// How long the replica may hold the request when nothing is readable
    /// yet. `None` answers at once.
    pub wait_for_records: Option<Duration>,
    /// How far into the copy the read may go.
    pub limit: ReadLimit,
}

/// The replica's answer to a [`ReadRequest`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadResponse {
    /// Back-to-back batch frames. The first one holds `from_sequence` but may
    /// start before it. Empty if nothing was readable.
    pub batch_frames: Bytes,
    /// The last sequence number the replica was allowed to serve.
    pub readable_end: SequenceNumber,
    /// The segment's final sequence number, if the replica knows it is
    /// sealed. A reader that has consumed it moves to the next segment.
    pub sealed_end: Option<SequenceNumber>,
}

/// Raises the fenced epoch of a segment copy (failover, step 3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FenceRequest {
    /// The segment to fence.
    pub segment: SegmentAddress,
    /// The epoch of the writer taking over.
    pub epoch: Epoch,
}

/// The replica's answer to a [`FenceRequest`], sent only after the new
/// fenced epoch is durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FenceResponse {
    /// The last sequence number in the replica's copy, or `None` if it has
    /// no copy of the segment at all.
    pub local_end: Option<SequenceNumber>,
    /// The segment's final sequence number, if the replica knows it is
    /// already sealed.
    pub sealed_end: Option<SequenceNumber>,
}

/// Tells a replica the final sequence number of a segment, as recorded in
/// the metadata store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SealRequest {
    /// The segment that was sealed.
    pub segment: SegmentAddress,
    /// Its final sequence number. The replica discards anything above it.
    pub sealed_end: SequenceNumber,
}

/// The replica's answer to a [`SealRequest`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SealResponse {
    /// The last sequence number in the replica's copy after sealing, or
    /// `None` if it has no copy. Below the sealed end, the copy is incomplete
    /// and needs repair.
    pub local_end: Option<SequenceNumber>,
}

/// Asks a replica to make its copy of a sealed segment complete by pulling
/// what it lacks from another replica (design, section 10, "Repair").
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepairRequest {
    /// The sealed segment to copy. A replica without a copy creates one.
    pub segment: SegmentDescriptor,
    /// The segment's final sequence number, as recorded in the metadata
    /// store.
    pub sealed_end: SequenceNumber,
    /// The registered address of a replica that holds a good copy.
    pub source_address: String,
}

/// The replica's answer to a [`RepairRequest`], sent once its copy is
/// complete, flushed and sealed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RepairResponse {
    /// The last sequence number in the repaired copy: the sealed end.
    pub local_end: SequenceNumber,
}

/// What a replica knows about one of its segment copies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentStatus {
    /// Which segment the copy belongs to.
    pub address: SegmentAddress,
    /// The last sequence number in the copy, or `None` if the replica only
    /// holds fencing or sealing state for the segment.
    pub local_end: Option<SequenceNumber>,
    /// The lowest epoch the replica accepts for the segment.
    pub fenced_epoch: Epoch,
    /// The segment's final sequence number, if sealed.
    pub sealed_end: Option<SequenceNumber>,
    /// How many bytes the copy occupies.
    pub size_bytes: u64,
    /// Whether the copy failed a checksum or a flush and must be replaced.
    pub is_damaged: bool,
}

/// What a replica reports about itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaStatus {
    /// The identity of the replica's data directory.
    pub replica_id: ReplicaId,
    /// The total size of its disk.
    pub capacity_bytes: u64,
    /// How much of the disk is unused.
    pub free_bytes: u64,
    /// Whether the replica is being drained for maintenance.
    pub is_draining: bool,
    /// How many segment copies it holds.
    pub segment_count: u64,
}

/// The operations a replica offers.
///
/// Every operation is safe to retry: appends of batches the replica already
/// holds are acknowledged again, and fence and seal are idempotent.
#[async_trait]
pub trait ReplicaApi: Send + Sync {
    /// Appends batches and flushes them before answering.
    async fn append(&self, request: AppendRequest) -> Result<AppendResponse, ReplicaError>;

    /// Reads batches, optionally waiting until some are readable.
    async fn read(&self, request: ReadRequest) -> Result<ReadResponse, ReplicaError>;

    /// Raises the fenced epoch and reports the end of the copy.
    async fn fence(&self, request: FenceRequest) -> Result<FenceResponse, ReplicaError>;

    /// Records the sealed end and discards anything above it.
    async fn seal(&self, request: SealRequest) -> Result<SealResponse, ReplicaError>;

    /// Completes the replica's copy of a sealed segment from another
    /// replica, verifying every batch on the way.
    async fn repair_segment(&self, request: RepairRequest) -> Result<RepairResponse, ReplicaError>;

    /// Deletes the replica's copy of a segment. Deleting an absent copy
    /// succeeds.
    async fn delete_segment(&self, segment: SegmentAddress) -> Result<(), ReplicaError>;

    /// Lists every segment copy the replica holds, sorted by address.
    async fn list_segments(&self) -> Result<Vec<SegmentStatus>, ReplicaError>;

    /// Reports the replica's identity, space and drain state.
    async fn status(&self) -> Result<ReplicaStatus, ReplicaError>;

    /// Starts draining: the replica refuses appends so writers roll away,
    /// and keeps serving reads.
    async fn drain(&self) -> Result<(), ReplicaError>;
}

/// Turns a replica's registered address into a handle to talk to it.
///
/// Connecting is lazy and never fails: an unreachable replica shows up as
/// [`ReplicaError::Unavailable`] on the first request.
pub trait ReplicaConnector: Send + Sync {
    /// Returns a handle to the replica listening at `address`.
    fn connect(&self, address: &str) -> Arc<dyn ReplicaApi>;
}

/// A connector over a fixed set of replica handles, keyed by address.
///
/// It serves in-process clusters: tests, the simulator and single-binary
/// deployments. An address it does not know behaves like a replica that is
/// down.
#[derive(Clone, Default)]
pub struct StaticReplicaConnector {
    replicas_by_address: BTreeMap<String, Arc<dyn ReplicaApi>>,
}

impl StaticReplicaConnector {
    /// Creates a connector that knows no replica.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Makes `replica` reachable at `address`, replacing any previous one.
    pub fn register(&mut self, address: impl Into<String>, replica: Arc<dyn ReplicaApi>) {
        self.replicas_by_address.insert(address.into(), replica);
    }
}

impl ReplicaConnector for StaticReplicaConnector {
    fn connect(&self, address: &str) -> Arc<dyn ReplicaApi> {
        match self.replicas_by_address.get(address) {
            Some(replica) => Arc::clone(replica),
            None => Arc::new(UnreachableReplica::new(format!(
                "no replica is reachable at {address}"
            ))),
        }
    }
}

/// Stands in for a replica that cannot be reached, for example one that is
/// no longer in the registry: every request fails as
/// [`ReplicaError::Unavailable`].
#[derive(Clone, Debug)]
pub struct UnreachableReplica {
    reason: String,
}

impl UnreachableReplica {
    /// Creates a handle whose every request fails with `reason`.
    #[must_use]
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }

    fn unavailable<Response>(&self) -> Result<Response, ReplicaError> {
        Err(ReplicaError::Unavailable {
            reason: self.reason.clone(),
        })
    }
}

#[async_trait]
impl ReplicaApi for UnreachableReplica {
    async fn append(&self, _request: AppendRequest) -> Result<AppendResponse, ReplicaError> {
        self.unavailable()
    }

    async fn read(&self, _request: ReadRequest) -> Result<ReadResponse, ReplicaError> {
        self.unavailable()
    }

    async fn fence(&self, _request: FenceRequest) -> Result<FenceResponse, ReplicaError> {
        self.unavailable()
    }

    async fn seal(&self, _request: SealRequest) -> Result<SealResponse, ReplicaError> {
        self.unavailable()
    }

    async fn repair_segment(
        &self,
        _request: RepairRequest,
    ) -> Result<RepairResponse, ReplicaError> {
        self.unavailable()
    }

    async fn delete_segment(&self, _segment: SegmentAddress) -> Result<(), ReplicaError> {
        self.unavailable()
    }

    async fn list_segments(&self) -> Result<Vec<SegmentStatus>, ReplicaError> {
        self.unavailable()
    }

    async fn status(&self) -> Result<ReplicaStatus, ReplicaError> {
        self.unavailable()
    }

    async fn drain(&self) -> Result<(), ReplicaError> {
        self.unavailable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unknown_address_behaves_like_a_replica_that_is_down() {
        let connector = StaticReplicaConnector::new();
        let status = connector.connect("replica-a:7000").status().await;
        assert_eq!(
            status,
            Err(ReplicaError::Unavailable {
                reason: "no replica is reachable at replica-a:7000".to_owned()
            })
        );
    }

    #[tokio::test]
    async fn registered_address_reaches_its_replica() {
        let mut connector = StaticReplicaConnector::new();
        connector.register(
            "replica-a:7000",
            Arc::new(UnreachableReplica::new("the registered handle answered")),
        );
        let status = connector.connect("replica-a:7000").status().await;
        assert_eq!(
            status,
            Err(ReplicaError::Unavailable {
                reason: "the registered handle answered".to_owned()
            })
        );
    }

    #[test]
    fn only_unavailability_is_worth_retrying() {
        let unavailable = ReplicaError::Unavailable {
            reason: "connection reset".to_owned(),
        };
        assert!(unavailable.is_retriable() && !ReplicaError::Draining.is_retriable());
    }

    #[test]
    fn fenced_error_names_both_epochs() {
        let fenced = ReplicaError::Fenced {
            request_epoch: Epoch::new(4),
            fenced_epoch: Epoch::new(5),
        };
        assert_eq!(
            fenced.to_string(),
            "epoch 4 is fenced: the replica accepts epoch 5 or later"
        );
    }
}
