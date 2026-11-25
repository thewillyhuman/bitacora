//! Tests of the `ReplicaError` conversions in `error_conversions.rs`.
//!
//! [`every_replica_error`] lists one error per variant, with a different
//! value in every field so that two swapped fields cannot go unnoticed. The
//! round-trip tests walk that list; the remaining tests cover each way an
//! error message can be unreadable.

use bitacora_types::{SegmentAddress, SegmentId, StreamId};

use super::*;

/// One error per variant of [`ReplicaError`]. The `match` below does not
/// compile when a variant is added, which is the reminder to add it here.
fn every_replica_error() -> Vec<ReplicaError> {
    let errors = vec![
        ReplicaError::Fenced {
            request_epoch: Epoch::new(3),
            fenced_epoch: Epoch::new(5),
        },
        ReplicaError::Sealed {
            sealed_end: SequenceNumber::new(60),
        },
        ReplicaError::OutOfOrder {
            first_sequence: SequenceNumber::new(12),
            local_end: SequenceNumber::new(9),
        },
        ReplicaError::ConflictingSeal {
            existing_sealed_end: SequenceNumber::new(60),
            requested_sealed_end: SequenceNumber::new(58),
        },
        ReplicaError::SegmentNotFound {
            segment: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
        },
        ReplicaError::InvalidRequest {
            reason: "a batch failed its checksum".to_owned(),
        },
        ReplicaError::NoCapacity { used_percent: 96 },
        ReplicaError::Draining,
        ReplicaError::Storage {
            reason: "flush failed: input/output error".to_owned(),
        },
        ReplicaError::Unavailable {
            reason: "the replica is shutting down".to_owned(),
        },
    ];
    for error in &errors {
        match error {
            ReplicaError::Fenced { .. }
            | ReplicaError::Sealed { .. }
            | ReplicaError::OutOfOrder { .. }
            | ReplicaError::ConflictingSeal { .. }
            | ReplicaError::SegmentNotFound { .. }
            | ReplicaError::InvalidRequest { .. }
            | ReplicaError::NoCapacity { .. }
            | ReplicaError::Draining
            | ReplicaError::Storage { .. }
            | ReplicaError::Unavailable { .. } => {}
        }
    }
    errors
}

/// An error message of the given kind.
fn error_message(kind: wire_error::Kind) -> wire::ReplicaError {
    wire::ReplicaError { kind: Some(kind) }
}

#[test]
fn every_error_variant_survives_the_round_trip() {
    for error in every_replica_error() {
        let message = wire::ReplicaError::from(error.clone());

        assert_eq!(ReplicaError::try_from(message), Ok(error));
    }
}

#[test]
fn fullest_disk_the_domain_can_express_survives_the_round_trip() {
    let error = ReplicaError::NoCapacity {
        used_percent: u8::MAX,
    };

    let message = wire::ReplicaError::from(error.clone());

    assert_eq!(ReplicaError::try_from(message), Ok(error));
}

#[test]
fn error_without_a_kind_is_malformed() {
    let message = wire::ReplicaError { kind: None };

    assert_eq!(
        ReplicaError::try_from(message),
        Err(MalformedMessage::MissingField {
            field: "ReplicaError.kind"
        })
    );
}

#[test]
fn segment_not_found_without_a_segment_is_malformed() {
    let message = error_message(wire_error::Kind::SegmentNotFound(
        wire_error::SegmentNotFound { segment: None },
    ));

    assert_eq!(
        ReplicaError::try_from(message),
        Err(MalformedMessage::MissingField {
            field: "ReplicaError.SegmentNotFound.segment"
        })
    );
}

#[test]
fn used_percent_that_does_not_fit_is_malformed() {
    let message = error_message(wire_error::Kind::NoCapacity(wire_error::NoCapacity {
        used_percent: 256,
    }));

    assert_eq!(
        ReplicaError::try_from(message),
        Err(MalformedMessage::NumberTooLarge {
            field: "ReplicaError.NoCapacity.used_percent",
            value: 256,
            largest: 255,
        })
    );
}
