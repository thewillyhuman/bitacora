//! `ReplicaError` to and from its message.
//!
//! # Why this file exists
//!
//! Callers act on the exact error a replica returns: a writer that is told
//! `Fenced` stops for good, one that is told `OutOfOrder` sends again from
//! the position in the error, one that is told `NoCapacity` rolls to other
//! replicas. An error that lost its variant or its fields on the way would
//! make them take the wrong decision. So the error travels as a message of
//! its own, variant by variant and field by field, and is never rebuilt
//! from a text.
//!
//! # What it contains
//!
//! - `From<ReplicaError> for wire::ReplicaError`: what a server sends.
//! - `TryFrom<wire::ReplicaError> for ReplicaError`: what a client reads.
//!
//! # How it works
//!
//! Each variant of the domain enum maps to the message of the same name
//! inside `ReplicaError.kind`. The two `match` expressions are exhaustive,
//! so adding a variant to the domain enum does not compile until it has a
//! message.
//!
//! Reading is strict. A client can receive an error it cannot read: a kind
//! added by a newer server, which arrives with no kind set, or a field out
//! of range. Reading reports that as a `MalformedMessage` and leaves the
//! decision to the caller. The protocol says what a client then does:
//! treat it as unavailability, the one error that promises nothing about
//! what the replica did.
//!
//! # Invariants
//!
//! - `ReplicaError -> message -> ReplicaError` is the identity, for every
//!   variant and every field value.
//! - An error that cannot be read is never read as another error.
//!
//! # See also
//!
//! - `src/proto/bitacora/v1/replica.proto`, message `ReplicaError`.

use bitacora_types::replica_api::ReplicaError;
use bitacora_types::{Epoch, SequenceNumber};

use crate::conversions::{MalformedMessage, required_field};
use crate::v1 as wire;
use crate::v1::replica_error as wire_error;

impl From<ReplicaError> for wire::ReplicaError {
    /// Writes a replica's error as its message, keeping every field.
    fn from(error: ReplicaError) -> Self {
        let kind = match error {
            ReplicaError::Fenced {
                request_epoch,
                fenced_epoch,
            } => wire_error::Kind::Fenced(wire_error::Fenced {
                request_epoch: request_epoch.get(),
                fenced_epoch: fenced_epoch.get(),
            }),
            ReplicaError::Sealed { sealed_end } => wire_error::Kind::Sealed(wire_error::Sealed {
                sealed_end: sealed_end.get(),
            }),
            ReplicaError::OutOfOrder {
                first_sequence,
                local_end,
            } => wire_error::Kind::OutOfOrder(wire_error::OutOfOrder {
                first_sequence: first_sequence.get(),
                local_end: local_end.get(),
            }),
            ReplicaError::ConflictingSeal {
                existing_sealed_end,
                requested_sealed_end,
            } => wire_error::Kind::ConflictingSeal(wire_error::ConflictingSeal {
                existing_sealed_end: existing_sealed_end.get(),
                requested_sealed_end: requested_sealed_end.get(),
            }),
            ReplicaError::SegmentNotFound { segment } => {
                wire_error::Kind::SegmentNotFound(wire_error::SegmentNotFound {
                    segment: Some(segment.into()),
                })
            }
            ReplicaError::InvalidRequest { reason } => {
                wire_error::Kind::InvalidRequest(wire_error::InvalidRequest { reason })
            }
            ReplicaError::NoCapacity { used_percent } => wire_error::Kind::NoCapacity(wire_error::NoCapacity {
                used_percent: u32::from(used_percent),
            }),
            ReplicaError::Draining => wire_error::Kind::Draining(wire_error::Draining {}),
            ReplicaError::Storage { reason } => wire_error::Kind::Storage(wire_error::Storage { reason }),
            ReplicaError::Unavailable { reason } => wire_error::Kind::Unavailable(wire_error::Unavailable { reason }),
        };
        Self { kind: Some(kind) }
    }
}

impl TryFrom<wire::ReplicaError> for ReplicaError {
    type Error = MalformedMessage;

    /// Reads a replica's error, keeping every field.
    ///
    /// # Errors
    ///
    /// Returns [`MalformedMessage::MissingField`] if no kind is set, which
    /// is also how a kind this version does not know shows up, or if
    /// `segment_not_found` names no segment. Returns
    /// [`MalformedMessage::NumberTooLarge`] if `no_capacity` reports a
    /// percentage that does not fit the domain type.
    fn try_from(error: wire::ReplicaError) -> Result<Self, Self::Error> {
        let kind = required_field(error.kind, "ReplicaError.kind")?;
        let error = match kind {
            wire_error::Kind::Fenced(fenced) => Self::Fenced {
                request_epoch: Epoch::new(fenced.request_epoch),
                fenced_epoch: Epoch::new(fenced.fenced_epoch),
            },
            wire_error::Kind::Sealed(sealed) => Self::Sealed {
                sealed_end: SequenceNumber::new(sealed.sealed_end),
            },
            wire_error::Kind::OutOfOrder(out_of_order) => Self::OutOfOrder {
                first_sequence: SequenceNumber::new(out_of_order.first_sequence),
                local_end: SequenceNumber::new(out_of_order.local_end),
            },
            wire_error::Kind::ConflictingSeal(conflicting_seal) => Self::ConflictingSeal {
                existing_sealed_end: SequenceNumber::new(conflicting_seal.existing_sealed_end),
                requested_sealed_end: SequenceNumber::new(conflicting_seal.requested_sealed_end),
            },
            wire_error::Kind::SegmentNotFound(segment_not_found) => {
                let segment = required_field(segment_not_found.segment, "ReplicaError.SegmentNotFound.segment")?;
                Self::SegmentNotFound {
                    segment: segment.into(),
                }
            }
            wire_error::Kind::InvalidRequest(invalid_request) => Self::InvalidRequest {
                reason: invalid_request.reason,
            },
            wire_error::Kind::NoCapacity(no_capacity) => Self::NoCapacity {
                used_percent: used_percent_from_wire(no_capacity.used_percent)?,
            },
            wire_error::Kind::Draining(wire_error::Draining {}) => Self::Draining,
            wire_error::Kind::Storage(storage) => Self::Storage { reason: storage.reason },
            wire_error::Kind::Unavailable(unavailable) => Self::Unavailable {
                reason: unavailable.reason,
            },
        };
        Ok(error)
    }
}

/// Reads how full a disk is. The message holds a 32-bit number, the
/// smallest Protocol Buffers has; the domain type holds 8 bits.
///
/// # Errors
///
/// Returns [`MalformedMessage::NumberTooLarge`] above 255.
fn used_percent_from_wire(used_percent: u32) -> Result<u8, MalformedMessage> {
    u8::try_from(used_percent).map_err(|_| MalformedMessage::NumberTooLarge {
        field: "ReplicaError.NoCapacity.used_percent",
        value: u64::from(used_percent),
        largest: u64::from(u8::MAX),
    })
}

#[cfg(test)]
#[path = "error_conversions_test.rs"]
mod tests;
