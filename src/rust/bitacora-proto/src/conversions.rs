//! Conversions of the values that several messages share, and the error for
//! a message that cannot be converted.
//!
//! # Why this file exists
//!
//! The generated messages are plain data: every number is a raw `u64`, every
//! nested message is optional, and nothing is validated. The domain types
//! say more: a segment descriptor always has an address, a replica
//! identifier has a restricted alphabet, a record's sequence number is never
//! zero. Something has to check a message from the network before the rest
//! of the system trusts it. The checks that several calls need live here, so
//! each is written once.
//!
//! # What it contains
//!
//! - [`MalformedMessage`]: why a message cannot be converted.
//! - [`required_field`], [`record_sequence`], [`replica_id_from_wire`]: the
//!   checks themselves.
//! - Conversions of `SegmentAddress`, `SegmentDescriptor` and
//!   `SegmentStatus` in both directions.
//!
//! # How it works
//!
//! Domain to message is a [`From`] and cannot fail: a domain value is valid
//! by construction. Message to domain is a [`TryFrom`] whose error is
//! [`MalformedMessage`], except where every message is valid, where it is a
//! [`From`] as well.
//!
//! The error names the field as `Message.field`, as written in the protocol
//! file, so that the author of a client in another language can find it.
//!
//! # Invariants
//!
//! - No conversion panics, whatever the message holds.
//! - `domain -> message -> domain` is the identity.
//!
//! # See also
//!
//! - `error_conversions.rs`, `request_conversions.rs` and
//!   `response_conversions.rs`, which build on these.

use bitacora_types::replica_api as domain;
use bitacora_types::{
    Epoch, InvalidReplicaId, ReplicaId, SegmentAddress, SegmentId, SequenceNumber, StreamId,
};
use thiserror::Error;

use crate::v1 as wire;

/// Why a message from the network cannot be turned into a domain value.
///
/// It is the error of every `TryFrom` conversion from a message in
/// [`v1`](crate::v1) to a type of `bitacora-types`. A server answers a
/// malformed request with `ReplicaError::InvalidRequest` and this error's
/// text as the reason. A client reads a malformed answer as
/// `ReplicaError::Unavailable`.
///
/// # Examples
///
/// ```
/// use bitacora_proto::{MalformedMessage, v1};
/// use bitacora_types::replica_api::FenceRequest;
///
/// let request_without_a_segment = v1::FenceRequest {
///     segment: None,
///     epoch: 5,
/// };
///
/// assert_eq!(
///     FenceRequest::try_from(request_without_a_segment),
///     Err(MalformedMessage::MissingField {
///         field: "FenceRequest.segment"
///     })
/// );
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum MalformedMessage {
    /// A field that must be set is absent.
    #[error("field `{field}` is missing")]
    MissingField {
        /// The absent field, as `Message.field`.
        field: &'static str,
    },
    /// A field that holds the sequence number of a record is zero. Records
    /// are numbered from 1; zero is only valid as the end of something
    /// empty.
    #[error("field `{field}` is 0, but records are numbered from 1")]
    ZeroRecordSequence {
        /// The zero field, as `Message.field`.
        field: &'static str,
    },
    /// A field that holds a replica identifier holds text that is not one.
    #[error("field `{field}` is not a replica identifier: {reason}")]
    InvalidReplicaId {
        /// The rejected field, as `Message.field`.
        field: &'static str,
        /// What is wrong with the text.
        reason: InvalidReplicaId,
    },
    /// A number is larger than the domain type it converts to can hold.
    #[error("field `{field}` is {value}, but the largest value it can hold is {largest}")]
    NumberTooLarge {
        /// The rejected field, as `Message.field`.
        field: &'static str,
        /// The value found in the message.
        value: u64,
        /// The largest value the field accepts.
        largest: u64,
    },
}

/// Unwraps a field that the protocol requires.
///
/// Protocol Buffers makes every nested message optional on the wire, so a
/// required one has to be checked by hand.
///
/// # Arguments
///
/// * `value` - The field as generated: `None` when the sender left it out.
/// * `field` - The field's name as `Message.field`, for the error.
///
/// # Errors
///
/// Returns [`MalformedMessage::MissingField`] if `value` is `None`.
pub(crate) fn required_field<Field>(
    value: Option<Field>,
    field: &'static str,
) -> Result<Field, MalformedMessage> {
    value.ok_or(MalformedMessage::MissingField { field })
}

/// Reads a field that holds the sequence number of a record.
///
/// Use it only where zero can never be right: the first record of a segment,
/// or the first record a reader wants. Ends and durable sequences may be
/// zero and are read with [`SequenceNumber::new`].
///
/// # Arguments
///
/// * `raw_sequence` - The number found in the message.
/// * `field` - The field's name as `Message.field`, for the error.
///
/// # Errors
///
/// Returns [`MalformedMessage::ZeroRecordSequence`] if `raw_sequence` is 0.
pub(crate) fn record_sequence(
    raw_sequence: u64,
    field: &'static str,
) -> Result<SequenceNumber, MalformedMessage> {
    if raw_sequence == 0 {
        return Err(MalformedMessage::ZeroRecordSequence { field });
    }
    Ok(SequenceNumber::new(raw_sequence))
}

/// Reads a field that holds a replica identifier.
///
/// # Arguments
///
/// * `identifier` - The text found in the message.
/// * `field` - The field's name as `Message.field`, for the error.
///
/// # Errors
///
/// Returns [`MalformedMessage::InvalidReplicaId`] if `identifier` is empty,
/// longer than 128 bytes, or holds anything but ASCII letters, digits, `.`,
/// `_` and `-`.
pub(crate) fn replica_id_from_wire(
    identifier: String,
    field: &'static str,
) -> Result<ReplicaId, MalformedMessage> {
    ReplicaId::new(identifier)
        .map_err(|reason| MalformedMessage::InvalidReplicaId { field, reason })
}

/// Reads an optional end of a copy or of a segment: absent stays absent.
pub(crate) fn optional_sequence(raw_sequence: Option<u64>) -> Option<SequenceNumber> {
    raw_sequence.map(SequenceNumber::new)
}

impl From<SegmentAddress> for wire::SegmentAddress {
    /// Writes a segment address as its message.
    fn from(address: SegmentAddress) -> Self {
        Self {
            stream_id: address.stream_id.get(),
            segment_id: address.segment_id.get(),
        }
    }
}

impl From<wire::SegmentAddress> for SegmentAddress {
    /// Reads a segment address. Every pair of numbers is an address, so this
    /// cannot fail; whether the segment exists is for the replica to say.
    fn from(address: wire::SegmentAddress) -> Self {
        Self::new(
            StreamId::new(address.stream_id),
            SegmentId::new(address.segment_id),
        )
    }
}

impl From<domain::SegmentDescriptor> for wire::SegmentDescriptor {
    /// Writes a segment descriptor as its message.
    fn from(descriptor: domain::SegmentDescriptor) -> Self {
        Self {
            address: Some(descriptor.address.into()),
            epoch: descriptor.epoch.get(),
            first_sequence: descriptor.first_sequence.get(),
        }
    }
}

impl TryFrom<wire::SegmentDescriptor> for domain::SegmentDescriptor {
    type Error = MalformedMessage;

    /// Reads a segment descriptor.
    ///
    /// # Errors
    ///
    /// Returns [`MalformedMessage::MissingField`] without an address, and
    /// [`MalformedMessage::ZeroRecordSequence`] if the first sequence is 0.
    fn try_from(descriptor: wire::SegmentDescriptor) -> Result<Self, Self::Error> {
        let address = required_field(descriptor.address, "SegmentDescriptor.address")?;
        let first_sequence = record_sequence(
            descriptor.first_sequence,
            "SegmentDescriptor.first_sequence",
        )?;
        Ok(Self {
            address: address.into(),
            epoch: Epoch::new(descriptor.epoch),
            first_sequence,
        })
    }
}

impl From<domain::SegmentStatus> for wire::SegmentStatus {
    /// Writes what a replica knows about a segment copy as its message.
    fn from(status: domain::SegmentStatus) -> Self {
        Self {
            address: Some(status.address.into()),
            local_end: status.local_end.map(SequenceNumber::get),
            fenced_epoch: status.fenced_epoch.get(),
            sealed_end: status.sealed_end.map(SequenceNumber::get),
            size_bytes: status.size_bytes,
            is_damaged: status.is_damaged,
        }
    }
}

impl TryFrom<wire::SegmentStatus> for domain::SegmentStatus {
    type Error = MalformedMessage;

    /// Reads what a replica knows about a segment copy.
    ///
    /// # Errors
    ///
    /// Returns [`MalformedMessage::MissingField`] without an address.
    fn try_from(status: wire::SegmentStatus) -> Result<Self, Self::Error> {
        let address = required_field(status.address, "SegmentStatus.address")?;
        Ok(Self {
            address: address.into(),
            local_end: optional_sequence(status.local_end),
            fenced_epoch: Epoch::new(status.fenced_epoch),
            sealed_end: optional_sequence(status.sealed_end),
            size_bytes: status.size_bytes,
            is_damaged: status.is_damaged,
        })
    }
}

#[cfg(test)]
#[path = "conversions_test.rs"]
mod tests;
