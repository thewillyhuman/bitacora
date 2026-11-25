//! The contract between a replica and everyone who talks to it.
//!
//! # Why this file exists
//!
//! Writers, readers, the controller and repairing replicas all send requests to replicas. In production
//! those travel over gRPC; in the simulator and in most tests they must not, because a socket cannot be
//! made to lose exactly the third answer or to delay a request by 700 simulated milliseconds (design,
//! section 15). So the network is an injected interface. This file defines it: the operations a replica
//! offers, the data each one carries, and the errors it can answer with. Everything that talks to a replica
//! holds a `dyn ReplicaApi` and cannot tell whether a socket, a fault-injecting link or the replica itself
//! is behind it. Because the replica implements the same trait, the whole protocol is tested in process.
//!
//! # What it contains
//!
//! - [`ReplicaApi`]: the operations, one method each.
//! - The request and answer of every operation: [`AppendRequest`] and [`AppendResponse`], [`ReadRequest`]
//!   with [`ReadLimit`] and [`ReadResponse`], [`FenceRequest`] and [`FenceResponse`], [`SealRequest`] and
//!   [`SealResponse`], [`RepairRequest`] and [`RepairResponse`], [`UploadRequest`] and [`UploadResponse`],
//!   [`SegmentStatus`], [`ReplicaStatus`], and [`SegmentDescriptor`], which several of them share.
//! - [`ReplicaError`]: why a replica refused or failed a request.
//! - [`ReplicaConnector`]: how a registered replica becomes a handle to talk to it.
//! - [`StaticReplicaConnector`] and [`UnreachableReplica`]: the two implementations that need nothing
//!   else, for tests and for replicas that are known to be gone.
//!
//! # How it works
//!
//! Every operation is a request and one answer. Batches travel as the bytes of their frames, exactly as
//! they are stored (`bitacora-segment` defines the frame), so a checksum computed once by the writer is
//! verified by the replica and again by every reader.
//!
//! A refusal is an answer: a replica that is fenced, sealed or full says so with a [`ReplicaError`]
//! variant that names what the caller needs to react (the fenced epoch, the copy's end). The one variant
//! that is not a refusal is [`ReplicaError::Unavailable`]: no answer arrived, and the request may or may
//! not have been applied.
//!
//! Nobody connects to an address alone. [`ReplicaConnector::connect`] takes the identity of the replica
//! the caller means together with its address, and a handle only ever reaches that replica (decision
//! record `docs/decisions/0010-replica-identity.md`).
//!
//! # Invariants
//!
//! - Every operation is safe to send again: appends of batches the replica already holds are acknowledged
//!   again, and the other operations are idempotent.
//! - An answer through a handle comes from the replica the handle was connected to, or the request fails as
//!   [`ReplicaError::Unavailable`].
//! - This file names no transport. `bitacora-proto` maps it to gRPC, message by message.
//!
//! # See also
//!
//! - `docs/protocol/replication.md`: what a replica must do for each operation.
//! - `src/proto/bitacora/v1/replica.proto`: the same contract on the wire.
//! - `bitacora-replica`: the implementation. `bitacora-testkit/src/link.rs`: the fault-injecting one.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

use crate::{Epoch, ReplicaId, SegmentAddress, SequenceNumber};

/// Everything a replica needs to create its copy of a segment.
///
/// These three values are what the header of a segment file holds. A request that may create a copy
/// carries them, so that a replica which never heard of the segment can start one.
///
/// # Examples
///
/// ```
/// use bitacora_types::replica_api::SegmentDescriptor;
/// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
///
/// let descriptor = SegmentDescriptor {
///     address: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
///     epoch: Epoch::new(5),
///     first_sequence: SequenceNumber::new(100),
/// };
///
/// assert_eq!(descriptor.address.to_string(), "7/3");
/// ```
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
///
/// Each variant is a situation the caller reacts to differently: a fenced writer stops, a writer told of a
/// gap resends, a writer refused for space or draining rolls to other replicas, and only unavailability is
/// worth retrying as it is.
///
/// # Examples
///
/// ```
/// use bitacora_types::Epoch;
/// use bitacora_types::replica_api::ReplicaError;
///
/// let fenced = ReplicaError::Fenced {
///     request_epoch: Epoch::new(4),
///     fenced_epoch: Epoch::new(5),
/// };
///
/// assert!(!fenced.is_retriable());
/// assert_eq!(
///     fenced.to_string(),
///     "epoch 4 is fenced: the replica accepts epoch 5 or later"
/// );
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ReplicaError {
    /// A newer writer has fenced the segment. The caller has lost the stream and must stop.
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
    /// The batches leave a gap after the replica's copy. The caller should resend from `local_end + 1`.
    #[error("batches start at {first_sequence} but the copy ends at {local_end}")]
    OutOfOrder {
        /// The first sequence number of the rejected batches.
        first_sequence: SequenceNumber,
        /// The last sequence number the replica holds.
        local_end: SequenceNumber,
    },
    /// The replica already knows a different sealed end for the segment, which can only happen if the
    /// metadata store was bypassed.
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
    /// The request itself is wrong: malformed frames, a failed checksum, or arguments that contradict the
    /// replica's copy. Retrying cannot help.
    #[error("invalid request: {reason}")]
    InvalidRequest {
        /// What is wrong with it.
        reason: String,
    },
    /// The replica's disk is too full for this request. Writers roll to another replica set.
    #[error("replica is out of space: disk is {used_percent}% full")]
    NoCapacity {
        /// How full the replica's disk is, in percent.
        used_percent: u8,
    },
    /// The replica is being drained for maintenance. Writers roll away.
    #[error("replica is draining")]
    Draining,
    /// The replica's copy is damaged or its disk failed. The copy must be treated as suspect and replaced.
    #[error("replica storage failed: {reason}")]
    Storage {
        /// What the disk or the checksum reported.
        reason: String,
    },
    /// The replica could not be reached or did not answer in time. The request may or may not have been
    /// applied.
    #[error("replica is unavailable: {reason}")]
    Unavailable {
        /// What went wrong on the way.
        reason: String,
    },
}

impl ReplicaError {
    /// Tells whether sending the same request again later can succeed.
    ///
    /// # Returns
    ///
    /// `true` only for [`ReplicaError::Unavailable`]. Every other variant is the replica's considered
    /// answer, and the same request would get it again.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::replica_api::ReplicaError;
    ///
    /// let no_answer = ReplicaError::Unavailable {
    ///     reason: "connection reset".to_owned(),
    /// };
    ///
    /// assert!(no_answer.is_retriable());
    /// assert!(!ReplicaError::Draining.is_retriable());
    /// ```
    #[must_use]
    pub const fn is_retriable(&self) -> bool {
        matches!(self, Self::Unavailable { .. })
    }
}

/// Appends batches to the open segment, or carries only the durable sequence when there is nothing to
/// append: the heartbeat of an idle stream.
///
/// # Examples
///
/// A heartbeat that tells the replica records up to 104 are committed:
///
/// ```
/// use bitacora_types::replica_api::{AppendRequest, SegmentDescriptor};
/// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
/// use bytes::Bytes;
///
/// let heartbeat = AppendRequest {
///     segment: SegmentDescriptor {
///         address: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
///         epoch: Epoch::new(5),
///         first_sequence: SequenceNumber::new(100),
///     },
///     writer_epoch: Epoch::new(5),
///     batch_frames: Bytes::new(),
///     durable_sequence: SequenceNumber::new(104),
/// };
///
/// assert!(heartbeat.batch_frames.is_empty());
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendRequest {
    /// The segment to append to. A replica without a copy creates one.
    pub segment: SegmentDescriptor,
    /// The epoch of the writer sending the request. Normally the segment's own; higher when a recovering
    /// writer repairs the tail of its predecessor's segment.
    pub writer_epoch: Epoch,
    /// Back-to-back batch frames, in sequence order. May be empty.
    pub batch_frames: Bytes,
    /// The writer's current durable sequence: what replicas may serve to readers.
    pub durable_sequence: SequenceNumber,
}

/// The replica's answer to an [`AppendRequest`], sent only after the batches are flushed.
///
/// # Examples
///
/// ```
/// use bitacora_types::SequenceNumber;
/// use bitacora_types::replica_api::AppendResponse;
///
/// let acknowledgement = AppendResponse {
///     local_end: SequenceNumber::new(104),
/// };
///
/// assert_eq!(acknowledgement.local_end.get(), 104);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendResponse {
    /// The last sequence number flushed in the replica's copy. The writer commits at the W-th highest of
    /// these.
    pub local_end: SequenceNumber,
}

/// How far a read may go into the replica's copy.
///
/// # Examples
///
/// ```
/// use bitacora_types::Epoch;
/// use bitacora_types::replica_api::ReadLimit;
///
/// let for_any_reader = ReadLimit::DurableSequence;
/// let for_the_recovering_writer = ReadLimit::LocalEnd {
///     recovery_epoch: Epoch::new(6),
/// };
///
/// assert_ne!(for_any_reader, for_the_recovering_writer);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadLimit {
    /// Up to the durable sequence the replica knows. What every reader uses.
    DurableSequence,
    /// Up to the end of the copy, committed or not. Only the writer that fenced the segment at
    /// `recovery_epoch` may ask for this, to repair the tail during failover.
    LocalEnd {
        /// The epoch the caller fenced the segment with.
        recovery_epoch: Epoch,
    },
}

/// Reads batches from a segment copy.
///
/// # Examples
///
/// A follower that lets the replica hold the request for up to a second when nothing is new:
///
/// ```
/// use std::time::Duration;
///
/// use bitacora_types::replica_api::{ReadLimit, ReadRequest};
/// use bitacora_types::{SegmentAddress, SegmentId, SequenceNumber, StreamId};
///
/// let tail_read = ReadRequest {
///     segment: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
///     from_sequence: SequenceNumber::new(105),
///     max_bytes: 1024 * 1024,
///     wait_for_records: Some(Duration::from_secs(1)),
///     limit: ReadLimit::DurableSequence,
/// };
///
/// assert_eq!(tail_read.from_sequence.get(), 105);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadRequest {
    /// The segment to read.
    pub segment: SegmentAddress,
    /// The first sequence number wanted.
    pub from_sequence: SequenceNumber,
    /// A soft cap on the size of the answer, in bytes. At least one batch is returned when one is
    /// available.
    pub max_bytes: usize,
    /// How long the replica may hold the request when nothing is readable yet. `None` answers at once.
    pub wait_for_records: Option<Duration>,
    /// How far into the copy the read may go.
    pub limit: ReadLimit,
}

/// The replica's answer to a [`ReadRequest`].
///
/// # Examples
///
/// ```
/// use bitacora_types::SequenceNumber;
/// use bitacora_types::replica_api::ReadResponse;
/// use bytes::Bytes;
///
/// let nothing_new = ReadResponse {
///     batch_frames: Bytes::new(),
///     readable_end: SequenceNumber::new(104),
///     sealed_end: None,
/// };
///
/// assert!(nothing_new.batch_frames.is_empty());
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadResponse {
    /// Back-to-back batch frames. The first one holds `from_sequence` but may start before it. Empty if
    /// nothing was readable.
    pub batch_frames: Bytes,
    /// The last sequence number the replica was allowed to serve.
    pub readable_end: SequenceNumber,
    /// The segment's final sequence number, if the replica knows it is sealed. A reader that has consumed
    /// it moves to the next segment.
    pub sealed_end: Option<SequenceNumber>,
}

/// Raises the fenced epoch of a segment copy (failover, step 3).
///
/// # Examples
///
/// ```
/// use bitacora_types::replica_api::FenceRequest;
/// use bitacora_types::{Epoch, SegmentAddress, SegmentId, StreamId};
///
/// let fence = FenceRequest {
///     segment: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
///     epoch: Epoch::new(6),
/// };
///
/// assert_eq!(fence.epoch, Epoch::new(6));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FenceRequest {
    /// The segment to fence.
    pub segment: SegmentAddress,
    /// The epoch of the writer taking over. Afterwards the replica refuses any writer below it.
    pub epoch: Epoch,
}

/// The replica's answer to a [`FenceRequest`], sent only after the new fenced epoch is durable.
///
/// # Examples
///
/// ```
/// use bitacora_types::SequenceNumber;
/// use bitacora_types::replica_api::FenceResponse;
///
/// let replica_without_a_copy = FenceResponse {
///     local_end: None,
///     sealed_end: None,
/// };
/// let replica_with_records = FenceResponse {
///     local_end: Some(SequenceNumber::new(104)),
///     sealed_end: None,
/// };
///
/// assert!(replica_with_records.local_end > replica_without_a_copy.local_end);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FenceResponse {
    /// The last sequence number in the replica's copy, or `None` if it has no copy of the segment at all.
    pub local_end: Option<SequenceNumber>,
    /// The segment's final sequence number, if the replica knows it is already sealed.
    pub sealed_end: Option<SequenceNumber>,
}

/// Tells a replica the final sequence number of a segment, as recorded in the metadata store.
///
/// # Examples
///
/// ```
/// use bitacora_types::replica_api::SealRequest;
/// use bitacora_types::{SegmentAddress, SegmentId, SequenceNumber, StreamId};
///
/// let seal = SealRequest {
///     segment: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
///     sealed_end: SequenceNumber::new(104),
/// };
///
/// assert_eq!(seal.sealed_end.get(), 104);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SealRequest {
    /// The segment that was sealed.
    pub segment: SegmentAddress,
    /// Its final sequence number. The replica discards anything above it.
    pub sealed_end: SequenceNumber,
}

/// The replica's answer to a [`SealRequest`].
///
/// # Examples
///
/// ```
/// use bitacora_types::SequenceNumber;
/// use bitacora_types::replica_api::SealResponse;
///
/// let sealed_end = SequenceNumber::new(104);
/// let answer = SealResponse {
///     local_end: Some(SequenceNumber::new(99)),
/// };
///
/// let needs_repair = answer.local_end != Some(sealed_end);
/// assert!(needs_repair);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SealResponse {
    /// The last sequence number in the replica's copy after sealing, or `None` if it has no copy. Below
    /// the sealed end, the copy is incomplete and needs repair.
    pub local_end: Option<SequenceNumber>,
}

/// Asks a replica to make its copy of a sealed segment complete by pulling what it lacks from another
/// replica (design, section 10, "Repair").
///
/// # Examples
///
/// ```
/// use bitacora_types::replica_api::{RepairRequest, SegmentDescriptor};
/// use bitacora_types::{Epoch, ReplicaId, SegmentAddress, SegmentId, SequenceNumber, StreamId};
///
/// let repair = RepairRequest {
///     segment: SegmentDescriptor {
///         address: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
///         epoch: Epoch::new(5),
///         first_sequence: SequenceNumber::new(100),
///     },
///     sealed_end: SequenceNumber::new(104),
///     source_replica_id: ReplicaId::new("replica-b")?,
///     source_address: "replica-b.example.org:7000".to_owned(),
/// };
///
/// assert_eq!(repair.source_replica_id.as_str(), "replica-b");
/// # Ok::<(), bitacora_types::InvalidReplicaId>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepairRequest {
    /// The sealed segment to copy. A replica without a copy creates one.
    pub segment: SegmentDescriptor,
    /// The segment's final sequence number, as recorded in the metadata store.
    pub sealed_end: SequenceNumber,
    /// The identity of a replica that holds a good copy.
    pub source_replica_id: ReplicaId,
    /// The registered address of that replica, as `host:port`.
    pub source_address: String,
}

/// The replica's answer to a [`RepairRequest`], sent once its copy is complete, flushed and sealed.
///
/// # Examples
///
/// ```
/// use bitacora_types::SequenceNumber;
/// use bitacora_types::replica_api::RepairResponse;
///
/// let repaired = RepairResponse {
///     local_end: SequenceNumber::new(104),
/// };
///
/// assert_eq!(repaired.local_end.get(), 104);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RepairResponse {
    /// The last sequence number in the repaired copy: the sealed end.
    pub local_end: SequenceNumber,
}

/// Asks a replica to upload its copy of a sealed segment to the object store as one tiered object (design,
/// section 10, "Tiering").
///
/// # Examples
///
/// ```
/// use bitacora_types::replica_api::{SegmentDescriptor, UploadRequest};
/// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
///
/// let upload = UploadRequest {
///     segment: SegmentDescriptor {
///         address: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
///         epoch: Epoch::new(5),
///         first_sequence: SequenceNumber::new(100),
///     },
///     sealed_end: SequenceNumber::new(104),
///     object_key: "streams/7/segments/00000000000000000003".to_owned(),
/// };
///
/// assert!(upload.object_key.starts_with("streams/7/"));
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadRequest {
    /// The sealed segment to upload.
    pub segment: SegmentDescriptor,
    /// The segment's final sequence number, as recorded in the metadata store. A copy that is not sealed
    /// exactly there is not uploaded.
    pub sealed_end: SequenceNumber,
    /// The key to store the object under. An object already there is replaced.
    pub object_key: String,
}

/// The replica's answer to an [`UploadRequest`], sent once the object is stored and was read back intact.
///
/// # Examples
///
/// ```
/// use bitacora_types::replica_api::UploadResponse;
///
/// let uploaded = UploadResponse { size_bytes: 4_096 };
///
/// assert_eq!(uploaded.size_bytes, 4_096);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UploadResponse {
    /// The size of the stored object in bytes, which a reader needs to find its footer.
    pub size_bytes: u64,
}

/// What a replica knows about one of its segment copies.
///
/// # Examples
///
/// A copy of a segment sealed at 104 that stops at 99 is incomplete:
///
/// ```
/// use bitacora_types::replica_api::SegmentStatus;
/// use bitacora_types::{Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
///
/// let status = SegmentStatus {
///     address: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
///     local_end: Some(SequenceNumber::new(99)),
///     fenced_epoch: Epoch::new(5),
///     sealed_end: Some(SequenceNumber::new(104)),
///     size_bytes: 8_192,
///     is_damaged: false,
///     descriptor: None,
/// };
///
/// let is_complete = status.local_end == status.sealed_end;
/// assert!(!is_complete);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentStatus {
    /// Which segment the copy belongs to.
    pub address: SegmentAddress,
    /// The last sequence number in the copy, or `None` if the replica only holds fencing or sealing state
    /// for the segment.
    pub local_end: Option<SequenceNumber>,
    /// The lowest epoch the replica accepts for the segment.
    pub fenced_epoch: Epoch,
    /// The segment's final sequence number, if sealed.
    pub sealed_end: Option<SequenceNumber>,
    /// How many bytes the copy occupies.
    pub size_bytes: u64,
    /// Whether the copy failed a checksum or a flush and must be replaced.
    pub is_damaged: bool,
    /// What the header of the replica's segment file says: the segment's epoch and first sequence number,
    /// with the address repeated. `None` if the replica holds no file for the segment, only fencing or
    /// sealing state. It is what lets the segment list of a stream be reconstructed from the replicas if
    /// the metadata store is lost.
    pub descriptor: Option<SegmentDescriptor>,
}

/// What a replica reports about itself.
///
/// # Examples
///
/// ```
/// use bitacora_types::ReplicaId;
/// use bitacora_types::replica_api::ReplicaStatus;
///
/// let status = ReplicaStatus {
///     replica_id: ReplicaId::new("replica-a")?,
///     capacity_bytes: 1_000,
///     free_bytes: 250,
///     is_draining: false,
///     segment_count: 12,
/// };
///
/// let used_bytes = status.capacity_bytes - status.free_bytes;
/// assert_eq!(used_bytes, 750);
/// # Ok::<(), bitacora_types::InvalidReplicaId>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaStatus {
    /// The identity of the replica's data directory.
    pub replica_id: ReplicaId,
    /// The total size of its disk, in bytes.
    pub capacity_bytes: u64,
    /// How much of the disk is unused, in bytes.
    pub free_bytes: u64,
    /// Whether the replica is being drained for maintenance.
    pub is_draining: bool,
    /// How many segment copies it holds.
    pub segment_count: u64,
}

/// The operations a replica offers.
///
/// Every operation is safe to retry: appends of batches the replica already holds are acknowledged again,
/// and fence and seal are idempotent. What a replica must do for each one is specified in
/// `docs/protocol/replication.md`.
///
/// Callers hold an `Arc<dyn ReplicaApi>` obtained from a [`ReplicaConnector`]. Every method can fail with
/// [`ReplicaError::Unavailable`] when no answer arrives; the other errors each method lists are the
/// replica's own refusals.
///
/// # Examples
///
/// Code written against the trait runs against anything that implements it, here a replica that is known
/// to be gone:
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// use bitacora_types::replica_api::{ReplicaApi, ReplicaError, UnreachableReplica};
///
/// async fn is_reachable(replica: &dyn ReplicaApi) -> bool {
///     !matches!(replica.status().await, Err(ReplicaError::Unavailable { .. }))
/// }
///
/// let removed_replica = UnreachableReplica::new("replica-a is not registered");
/// assert!(!is_reachable(&removed_replica).await);
/// # }
/// ```
#[async_trait]
pub trait ReplicaApi: Send + Sync {
    /// Appends batches and flushes them before answering.
    ///
    /// # Errors
    ///
    /// * [`ReplicaError::Fenced`] if the writer's epoch is below the fenced epoch.
    /// * [`ReplicaError::Sealed`] if the segment is sealed.
    /// * [`ReplicaError::OutOfOrder`] if the batches leave a gap after the copy.
    /// * [`ReplicaError::InvalidRequest`] if a frame is malformed or contradicts the copy.
    /// * [`ReplicaError::NoCapacity`] or [`ReplicaError::Draining`] if the replica takes no appends.
    /// * [`ReplicaError::Storage`] if the disk failed or the copy is damaged.
    /// * [`ReplicaError::Unavailable`] if no answer arrived.
    async fn append(&self, request: AppendRequest) -> Result<AppendResponse, ReplicaError>;

    /// Reads batches, optionally waiting until some are readable.
    ///
    /// # Errors
    ///
    /// * [`ReplicaError::SegmentNotFound`] if the replica has no copy of the segment.
    /// * [`ReplicaError::Fenced`] or [`ReplicaError::InvalidRequest`] if a read to the local end comes from
    ///   anyone but the writer that fenced the copy.
    /// * [`ReplicaError::Storage`] if the copy is damaged.
    /// * [`ReplicaError::Unavailable`] if no answer arrived.
    async fn read(&self, request: ReadRequest) -> Result<ReadResponse, ReplicaError>;

    /// Raises the fenced epoch and reports the end of the copy.
    ///
    /// # Errors
    ///
    /// * [`ReplicaError::Fenced`] if the copy is already fenced at a higher epoch.
    /// * [`ReplicaError::Storage`] if the fence cannot be stored or the copy is damaged.
    /// * [`ReplicaError::Unavailable`] if no answer arrived.
    async fn fence(&self, request: FenceRequest) -> Result<FenceResponse, ReplicaError>;

    /// Records the sealed end and discards anything above it.
    ///
    /// # Errors
    ///
    /// * [`ReplicaError::ConflictingSeal`] if the replica knows another sealed end for the segment.
    /// * [`ReplicaError::Storage`] if the seal cannot be stored or the copy is damaged.
    /// * [`ReplicaError::Unavailable`] if no answer arrived.
    async fn seal(&self, request: SealRequest) -> Result<SealResponse, ReplicaError>;

    /// Completes the replica's copy of a sealed segment from another replica, verifying every batch on the
    /// way.
    ///
    /// # Errors
    ///
    /// * [`ReplicaError::NoCapacity`] or [`ReplicaError::Draining`] if the replica takes no new data.
    /// * [`ReplicaError::Storage`] if the disk failed.
    /// * [`ReplicaError::Unavailable`] if the source could not provide the records, or no answer arrived.
    async fn repair_segment(&self, request: RepairRequest) -> Result<RepairResponse, ReplicaError>;

    /// Uploads the replica's copy of a sealed segment to the object store and reads it back to verify what
    /// was stored.
    ///
    /// # Errors
    ///
    /// * [`ReplicaError::SegmentNotFound`] if the replica has no copy of the segment.
    /// * [`ReplicaError::InvalidRequest`] if the copy is not sealed at the requested end, or the replica
    ///   has no object store.
    /// * [`ReplicaError::Storage`] if the copy is damaged or incomplete.
    /// * [`ReplicaError::Unavailable`] if the object store failed, or no answer arrived.
    async fn upload_segment(&self, request: UploadRequest) -> Result<UploadResponse, ReplicaError>;

    /// Deletes the replica's copy of a segment. Deleting an absent copy succeeds.
    ///
    /// # Errors
    ///
    /// * [`ReplicaError::Storage`] if the files cannot be removed.
    /// * [`ReplicaError::Unavailable`] if no answer arrived.
    async fn delete_segment(&self, segment: SegmentAddress) -> Result<(), ReplicaError>;

    /// Lists every segment copy the replica holds, sorted by address.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaError::Unavailable`] if no answer arrived.
    async fn list_segments(&self) -> Result<Vec<SegmentStatus>, ReplicaError>;

    /// Reports the replica's identity, space and drain state.
    ///
    /// # Errors
    ///
    /// * [`ReplicaError::Storage`] if the disk cannot be measured.
    /// * [`ReplicaError::Unavailable`] if no answer arrived.
    async fn status(&self) -> Result<ReplicaStatus, ReplicaError>;

    /// Starts draining: the replica refuses appends so writers roll away, and keeps serving reads.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaError::Unavailable`] if no answer arrived.
    async fn drain(&self) -> Result<(), ReplicaError>;
}

/// Turns a replica's registration into a handle to talk to it.
///
/// Connecting is lazy and never fails: an unreachable replica shows up as [`ReplicaError::Unavailable`] on
/// the first request.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_types::InvalidReplicaId> {
/// use bitacora_types::ReplicaId;
/// use bitacora_types::replica_api::{ReplicaConnector, ReplicaError, StaticReplicaConnector};
///
/// let connector = StaticReplicaConnector::new();
/// let replica = connector.connect(&ReplicaId::new("replica-a")?, "replica-a:7000");
///
/// // Nothing is registered at that address, so the handle behaves like a replica that is down.
/// assert!(matches!(replica.status().await, Err(ReplicaError::Unavailable { .. })));
/// # Ok(())
/// # }
/// ```
pub trait ReplicaConnector: Send + Sync {
    /// Returns a handle to one replica.
    ///
    /// The handle must only ever reach that replica. An address can come to be served by another one: a
    /// node whose disk was replaced returns at the same address under a new identity, holding none of the
    /// copies the old identity held. If it answered in the old one's name, a recovering writer would take
    /// its empty copies for real ones. Every request through the handle must therefore fail as
    /// [`ReplicaError::Unavailable`] unless the replica that answers is `replica_id`.
    ///
    /// # Arguments
    ///
    /// * `replica_id` - The identity of the replica the caller means, as registered in the metadata store.
    /// * `address` - Where that replica listens, as registered.
    ///
    /// # Returns
    ///
    /// A handle whose requests reach `replica_id` or fail. Obtaining it sends nothing.
    fn connect(&self, replica_id: &ReplicaId, address: &str) -> Arc<dyn ReplicaApi>;
}

/// One entry of a [`StaticReplicaConnector`]: who answers at an address.
#[derive(Clone)]
struct RegisteredReplica {
    /// The identity of the replica registered at the address.
    replica_id: ReplicaId,
    /// The handle that reaches it.
    handle: Arc<dyn ReplicaApi>,
}

/// A connector over a fixed set of replica handles.
///
/// It serves in-process setups where the replicas never change: unit tests and single-binary deployments.
/// A replica it does not know, by identity and address together, behaves like one that is down.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_types::InvalidReplicaId> {
/// use std::sync::Arc;
///
/// use bitacora_types::ReplicaId;
/// use bitacora_types::replica_api::{
///     ReplicaConnector, ReplicaError, StaticReplicaConnector, UnreachableReplica,
/// };
///
/// let mut connector = StaticReplicaConnector::new();
/// connector.register(
///     ReplicaId::new("replica-a")?,
///     "replica-a:7000",
///     Arc::new(UnreachableReplica::new("this stands in for a real replica")),
/// );
///
/// // The address is now served by another identity than the caller means: the node was rebuilt.
/// let rebuilt_node = connector.connect(&ReplicaId::new("replica-a-disk-1")?, "replica-a:7000");
///
/// assert_eq!(
///     rebuilt_node.status().await,
///     Err(ReplicaError::Unavailable {
///         reason: "replica-a:7000 is replica replica-a, not replica-a-disk-1".to_owned()
///     })
/// );
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Default)]
pub struct StaticReplicaConnector {
    /// The registered replicas, by the address each one is reachable at.
    replicas_by_address: BTreeMap<String, RegisteredReplica>,
}

impl StaticReplicaConnector {
    /// Creates a connector that knows no replica.
    ///
    /// # Returns
    ///
    /// A connector whose every handle is unavailable until replicas are registered.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::replica_api::StaticReplicaConnector;
    ///
    /// let connector = StaticReplicaConnector::new();
    /// # drop(connector);
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Makes a replica reachable at an address, replacing whatever was registered there.
    ///
    /// # Arguments
    ///
    /// * `replica_id` - The identity of the replica. A caller must name it to reach the replica.
    /// * `address` - The address the replica is registered under.
    /// * `replica` - The handle requests are passed to.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use bitacora_types::ReplicaId;
    /// use bitacora_types::replica_api::{StaticReplicaConnector, UnreachableReplica};
    ///
    /// let mut connector = StaticReplicaConnector::new();
    /// connector.register(
    ///     ReplicaId::new("replica-a")?,
    ///     "replica-a:7000",
    ///     Arc::new(UnreachableReplica::new("this stands in for a real replica")),
    /// );
    /// # Ok::<(), bitacora_types::InvalidReplicaId>(())
    /// ```
    pub fn register(&mut self, replica_id: ReplicaId, address: impl Into<String>, replica: Arc<dyn ReplicaApi>) {
        let registered = RegisteredReplica {
            replica_id,
            handle: replica,
        };
        self.replicas_by_address.insert(address.into(), registered);
    }
}

impl ReplicaConnector for StaticReplicaConnector {
    /// Returns the handle registered at `address` if it belongs to `replica_id`, and otherwise a handle
    /// that is always unavailable and says why.
    fn connect(&self, replica_id: &ReplicaId, address: &str) -> Arc<dyn ReplicaApi> {
        let Some(registered) = self.replicas_by_address.get(address) else {
            return Arc::new(UnreachableReplica::new(format!("no replica is reachable at {address}")));
        };
        if &registered.replica_id != replica_id {
            return Arc::new(UnreachableReplica::new(format!(
                "{address} is replica {}, not {replica_id}",
                registered.replica_id
            )));
        }
        Arc::clone(&registered.handle)
    }
}

/// Stands in for a replica that cannot be reached, for example one that is no longer in the registry:
/// every request fails as [`ReplicaError::Unavailable`].
///
/// Callers that walk a replica set use it for a member they cannot connect to, so that the rest of their
/// code treats it like any other replica that is down.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// use bitacora_types::replica_api::{ReplicaApi, ReplicaError, UnreachableReplica};
///
/// let removed_replica = UnreachableReplica::new("replica-a is not registered");
///
/// assert_eq!(
///     removed_replica.drain().await,
///     Err(ReplicaError::Unavailable {
///         reason: "replica-a is not registered".to_owned()
///     })
/// );
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct UnreachableReplica {
    /// Why the replica cannot be reached. Every request fails with it.
    reason: String,
}

impl UnreachableReplica {
    /// Creates a handle whose every request fails with a given reason.
    ///
    /// # Arguments
    ///
    /// * `reason` - Why the replica cannot be reached, for the log of whoever asks.
    ///
    /// # Returns
    ///
    /// The handle.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::replica_api::UnreachableReplica;
    ///
    /// let removed_replica = UnreachableReplica::new("replica-a is not registered");
    /// # drop(removed_replica);
    /// ```
    #[must_use]
    pub fn new(reason: impl Into<String>) -> Self {
        Self { reason: reason.into() }
    }

    /// The answer to every request: unavailable, with this handle's reason.
    fn unavailable<Response>(&self) -> Result<Response, ReplicaError> {
        Err(ReplicaError::Unavailable {
            reason: self.reason.clone(),
        })
    }
}

#[async_trait]
impl ReplicaApi for UnreachableReplica {
    /// Fails as unavailable, like every operation of this handle.
    async fn append(&self, _request: AppendRequest) -> Result<AppendResponse, ReplicaError> {
        self.unavailable()
    }

    /// Fails as unavailable, like every operation of this handle.
    async fn read(&self, _request: ReadRequest) -> Result<ReadResponse, ReplicaError> {
        self.unavailable()
    }

    /// Fails as unavailable, like every operation of this handle.
    async fn fence(&self, _request: FenceRequest) -> Result<FenceResponse, ReplicaError> {
        self.unavailable()
    }

    /// Fails as unavailable, like every operation of this handle.
    async fn seal(&self, _request: SealRequest) -> Result<SealResponse, ReplicaError> {
        self.unavailable()
    }

    /// Fails as unavailable, like every operation of this handle.
    async fn repair_segment(&self, _request: RepairRequest) -> Result<RepairResponse, ReplicaError> {
        self.unavailable()
    }

    /// Fails as unavailable, like every operation of this handle.
    async fn upload_segment(&self, _request: UploadRequest) -> Result<UploadResponse, ReplicaError> {
        self.unavailable()
    }

    /// Fails as unavailable, like every operation of this handle.
    async fn delete_segment(&self, _segment: SegmentAddress) -> Result<(), ReplicaError> {
        self.unavailable()
    }

    /// Fails as unavailable, like every operation of this handle.
    async fn list_segments(&self) -> Result<Vec<SegmentStatus>, ReplicaError> {
        self.unavailable()
    }

    /// Fails as unavailable, like every operation of this handle.
    async fn status(&self) -> Result<ReplicaStatus, ReplicaError> {
        self.unavailable()
    }

    /// Fails as unavailable, like every operation of this handle.
    async fn drain(&self) -> Result<(), ReplicaError> {
        self.unavailable()
    }
}

#[cfg(test)]
#[path = "replica_api_test.rs"]
mod tests;
