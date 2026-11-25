//! The request of every call, to and from its message.
//!
//! # Why this file exists
//!
//! A client holds a domain request and must send a message; a server
//! receives a message and must hand the replica a domain request it can
//! trust. This file is both translations for every call that takes
//! arguments, next to each other so that a field added to one direction is
//! hard to forget in the other.
//!
//! # What it contains
//!
//! For each of `Append`, `Read`, `Fence`, `Seal`, `RepairSegment`,
//! `UploadSegment` and `DeleteSegment`:
//!
//! - `From<domain request> for wire request`, used by the client.
//! - `TryFrom<wire request> for domain request`, used by the server.
//!
//! `Status`, `Drain` and `ListSegments` take no arguments; their request
//! messages are empty and need no conversion.
//!
//! # How it works
//!
//! Most fields are copied. The exceptions:
//!
//! - **Required messages** (`segment`) are checked with `required_field`.
//! - **Record positions** (`from_sequence`, a descriptor's
//!   `first_sequence`) must be at least 1.
//! - **`ReadRequest.max_bytes`** is a `usize` in the domain and a `u64` in
//!   the message. Each side saturates, which keeps the meaning of a soft
//!   cap: "as much as you can" stays "as much as you can".
//! - **`ReadRequest.wait_for_records`** travels as whole nanoseconds, the
//!   resolution of the domain type, so that nothing is rounded. A wait
//!   longer than five centuries saturates.
//! - **`ReadRequest.limit`** travels as the optional `recovery_epoch`:
//!   absent means "up to the durable sequence", which is the restricted
//!   read. A client that forgets the field therefore gets the safe one.
//!
//! # Invariants
//!
//! - `domain -> message -> domain` is the identity for every request a
//!   64-bit machine can express.
//! - A message that fails a check here never reaches the replica.
//!
//! # See also
//!
//! - `conversions.rs` for the shared checks and [`MalformedMessage`].
//! - `server.rs`, which answers a malformed request with
//!   `ReplicaError::InvalidRequest`.

use std::time::Duration;

use bitacora_types::replica_api as domain;
use bitacora_types::{Epoch, SegmentAddress, SequenceNumber};

use crate::conversions::{MalformedMessage, record_sequence, replica_id_from_wire, required_field};
use crate::v1 as wire;

impl From<domain::AppendRequest> for wire::AppendRequest {
    /// Writes an append as its message. The batch frames are shared with
    /// the domain request, not copied.
    fn from(request: domain::AppendRequest) -> Self {
        Self {
            segment: Some(request.segment.into()),
            writer_epoch: request.writer_epoch.get(),
            batch_frames: request.batch_frames,
            durable_sequence: request.durable_sequence.get(),
        }
    }
}

impl TryFrom<wire::AppendRequest> for domain::AppendRequest {
    type Error = MalformedMessage;

    /// Reads an append. The batch frames are not looked at: the replica
    /// verifies them.
    ///
    /// # Errors
    ///
    /// Returns [`MalformedMessage::MissingField`] without a segment or
    /// without its address, and [`MalformedMessage::ZeroRecordSequence`] if
    /// the segment's first sequence is 0.
    fn try_from(request: wire::AppendRequest) -> Result<Self, Self::Error> {
        let segment = required_field(request.segment, "AppendRequest.segment")?;
        Ok(Self {
            segment: segment.try_into()?,
            writer_epoch: Epoch::new(request.writer_epoch),
            batch_frames: request.batch_frames,
            durable_sequence: SequenceNumber::new(request.durable_sequence),
        })
    }
}

impl From<domain::ReadRequest> for wire::ReadRequest {
    /// Writes a read as its message.
    fn from(request: domain::ReadRequest) -> Self {
        Self {
            segment: Some(request.segment.into()),
            from_sequence: request.from_sequence.get(),
            max_bytes: u64::try_from(request.max_bytes).unwrap_or(u64::MAX),
            wait_for_records_nanoseconds: request.wait_for_records.map(whole_nanoseconds),
            recovery_epoch: recovery_epoch_of(request.limit),
        }
    }
}

impl TryFrom<wire::ReadRequest> for domain::ReadRequest {
    type Error = MalformedMessage;

    /// Reads a read request.
    ///
    /// # Errors
    ///
    /// Returns [`MalformedMessage::MissingField`] without a segment, and
    /// [`MalformedMessage::ZeroRecordSequence`] if `from_sequence` is 0.
    fn try_from(request: wire::ReadRequest) -> Result<Self, Self::Error> {
        let segment = required_field(request.segment, "ReadRequest.segment")?;
        let from_sequence = record_sequence(request.from_sequence, "ReadRequest.from_sequence")?;
        Ok(Self {
            segment: segment.into(),
            from_sequence,
            max_bytes: usize::try_from(request.max_bytes).unwrap_or(usize::MAX),
            wait_for_records: request.wait_for_records_nanoseconds.map(Duration::from_nanos),
            limit: read_limit_of(request.recovery_epoch),
        })
    }
}

/// Returns `duration` in whole nanoseconds, or the largest number the
/// message can hold for a duration beyond it (about 584 years).
fn whole_nanoseconds(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Returns what `ReadRequest.recovery_epoch` holds for `limit`: nothing for
/// a read up to the durable sequence, the caller's epoch for a read up to
/// the end of the copy.
fn recovery_epoch_of(limit: domain::ReadLimit) -> Option<u64> {
    match limit {
        domain::ReadLimit::DurableSequence => None,
        domain::ReadLimit::LocalEnd { recovery_epoch } => Some(recovery_epoch.get()),
    }
}

/// Returns the read limit that `ReadRequest.recovery_epoch` stands for; the
/// inverse of [`recovery_epoch_of`].
fn read_limit_of(recovery_epoch: Option<u64>) -> domain::ReadLimit {
    match recovery_epoch {
        None => domain::ReadLimit::DurableSequence,
        Some(raw_epoch) => domain::ReadLimit::LocalEnd {
            recovery_epoch: Epoch::new(raw_epoch),
        },
    }
}

impl From<domain::FenceRequest> for wire::FenceRequest {
    /// Writes a fence as its message.
    fn from(request: domain::FenceRequest) -> Self {
        Self {
            segment: Some(request.segment.into()),
            epoch: request.epoch.get(),
        }
    }
}

impl TryFrom<wire::FenceRequest> for domain::FenceRequest {
    type Error = MalformedMessage;

    /// Reads a fence.
    ///
    /// # Errors
    ///
    /// Returns [`MalformedMessage::MissingField`] without a segment.
    fn try_from(request: wire::FenceRequest) -> Result<Self, Self::Error> {
        let segment = required_field(request.segment, "FenceRequest.segment")?;
        Ok(Self {
            segment: segment.into(),
            epoch: Epoch::new(request.epoch),
        })
    }
}

impl From<domain::SealRequest> for wire::SealRequest {
    /// Writes a seal as its message.
    fn from(request: domain::SealRequest) -> Self {
        Self {
            segment: Some(request.segment.into()),
            sealed_end: request.sealed_end.get(),
        }
    }
}

impl TryFrom<wire::SealRequest> for domain::SealRequest {
    type Error = MalformedMessage;

    /// Reads a seal. A sealed end of 0 is valid: a segment can be sealed
    /// empty.
    ///
    /// # Errors
    ///
    /// Returns [`MalformedMessage::MissingField`] without a segment.
    fn try_from(request: wire::SealRequest) -> Result<Self, Self::Error> {
        let segment = required_field(request.segment, "SealRequest.segment")?;
        Ok(Self {
            segment: segment.into(),
            sealed_end: SequenceNumber::new(request.sealed_end),
        })
    }
}

impl From<domain::RepairRequest> for wire::RepairSegmentRequest {
    /// Writes a repair as its message.
    fn from(request: domain::RepairRequest) -> Self {
        Self {
            segment: Some(request.segment.into()),
            sealed_end: request.sealed_end.get(),
            source_replica_id: request.source_replica_id.into(),
            source_address: request.source_address,
        }
    }
}

impl TryFrom<wire::RepairSegmentRequest> for domain::RepairRequest {
    type Error = MalformedMessage;

    /// Reads a repair. The source address is not looked at: connecting to
    /// it is what tells whether it is any good.
    ///
    /// # Errors
    ///
    /// Returns [`MalformedMessage::MissingField`] without a segment or
    /// without its address, [`MalformedMessage::ZeroRecordSequence`] if the
    /// segment's first sequence is 0, and
    /// [`MalformedMessage::InvalidReplicaId`] if the source is not a
    /// replica identifier.
    fn try_from(request: wire::RepairSegmentRequest) -> Result<Self, Self::Error> {
        let segment = required_field(request.segment, "RepairSegmentRequest.segment")?;
        let source_replica_id =
            replica_id_from_wire(request.source_replica_id, "RepairSegmentRequest.source_replica_id")?;
        Ok(Self {
            segment: segment.try_into()?,
            sealed_end: SequenceNumber::new(request.sealed_end),
            source_replica_id,
            source_address: request.source_address,
        })
    }
}

impl From<domain::UploadRequest> for wire::UploadSegmentRequest {
    /// Writes an upload as its message.
    fn from(request: domain::UploadRequest) -> Self {
        Self {
            segment: Some(request.segment.into()),
            sealed_end: request.sealed_end.get(),
            object_key: request.object_key,
        }
    }
}

impl TryFrom<wire::UploadSegmentRequest> for domain::UploadRequest {
    type Error = MalformedMessage;

    /// Reads an upload. The object key is not looked at: the object store
    /// is what tells whether it is any good.
    ///
    /// # Errors
    ///
    /// Returns [`MalformedMessage::MissingField`] without a segment or
    /// without its address, and [`MalformedMessage::ZeroRecordSequence`] if
    /// the segment's first sequence is 0.
    fn try_from(request: wire::UploadSegmentRequest) -> Result<Self, Self::Error> {
        let segment = required_field(request.segment, "UploadSegmentRequest.segment")?;
        Ok(Self {
            segment: segment.try_into()?,
            sealed_end: SequenceNumber::new(request.sealed_end),
            object_key: request.object_key,
        })
    }
}

impl From<SegmentAddress> for wire::DeleteSegmentRequest {
    /// Writes the deletion of a segment copy as its message. The domain
    /// call takes the address alone, so the address is the whole request.
    fn from(segment: SegmentAddress) -> Self {
        Self {
            segment: Some(segment.into()),
        }
    }
}

impl TryFrom<wire::DeleteSegmentRequest> for SegmentAddress {
    type Error = MalformedMessage;

    /// Reads which segment copy to delete.
    ///
    /// # Errors
    ///
    /// Returns [`MalformedMessage::MissingField`] without a segment.
    fn try_from(request: wire::DeleteSegmentRequest) -> Result<Self, Self::Error> {
        let segment = required_field(request.segment, "DeleteSegmentRequest.segment")?;
        Ok(segment.into())
    }
}

#[cfg(test)]
#[path = "request_conversions_test.rs"]
mod tests;
