//! Tests of the response conversions in `response_conversions.rs`.
//!
//! The rule for unreadable answers comes first. Then each call is checked
//! three ways: a success survives the trip to its message and back, a
//! refusal survives it, and a message without an outcome is read as
//! unavailability. Every variant of `ReplicaError` is covered in
//! `error_conversions_test.rs`; here one refusal per call is enough to show
//! that the call's `error` arm is wired to it.

use bitacora_types::{Epoch, InvalidReplicaId, ReplicaId, SegmentAddress, SegmentId, StreamId};
use bytes::Bytes;

use super::*;

/// The refusal used to exercise the `error` arm of every response.
fn refusal() -> ReplicaError {
    ReplicaError::Fenced {
        request_epoch: Epoch::new(3),
        fenced_epoch: Epoch::new(5),
    }
}

/// The error a response must be read as when its required `field` is
/// absent.
fn unreadable_without(field: &'static str) -> ReplicaError {
    unreadable_answer(&MalformedMessage::MissingField { field })
}

/// The status of one segment copy, of segment `segment_id` of stream 7.
fn segment_status(segment_id: u64) -> domain::SegmentStatus {
    domain::SegmentStatus {
        address: SegmentAddress::new(StreamId::new(7), SegmentId::new(segment_id)),
        local_end: Some(SequenceNumber::new(60)),
        fenced_epoch: Epoch::new(2),
        sealed_end: None,
        size_bytes: 4096,
        is_damaged: false,
        descriptor: None,
    }
}

/// What a healthy replica reports about itself.
fn replica_status() -> domain::ReplicaStatus {
    domain::ReplicaStatus {
        replica_id: ReplicaId::new("replica-a").expect("a valid identifier"),
        capacity_bytes: 1_000_000,
        free_bytes: 400_000,
        is_draining: true,
        segment_count: 12,
    }
}

#[test]
fn unreadable_answer_names_the_field_at_fault() {
    let malformed_message = MalformedMessage::MissingField {
        field: "AppendResponse.outcome",
    };

    let error = unreadable_answer(&malformed_message);

    assert_eq!(
        error,
        ReplicaError::Unavailable {
            reason: "the replica's answer cannot be read: field `AppendResponse.outcome` is missing".to_owned()
        }
    );
}

/// The replica did answer, so it may have applied the request. Asking
/// again is the only safe thing a caller can do, and every call is safe to
/// repeat.
#[test]
fn unreadable_answer_is_worth_asking_again() {
    let malformed_message = MalformedMessage::MissingField {
        field: "AppendResponse.outcome",
    };

    let error = unreadable_answer(&malformed_message);

    assert!(error.is_retriable());
}

#[test]
fn readable_error_is_read_unchanged() {
    let message = wire::ReplicaError::from(refusal());

    assert_eq!(replica_error_from_wire(message), refusal());
}

/// A newer server may send a kind this version does not know; it arrives
/// with no kind set. The client must not guess what the replica did.
#[test]
fn error_of_an_unknown_kind_is_read_as_unavailable() {
    let message = wire::ReplicaError { kind: None };

    let error = replica_error_from_wire(message);

    assert_eq!(error, unreadable_without("ReplicaError.kind"));
}

#[test]
fn append_success_survives_the_round_trip() {
    let result = Ok(domain::AppendResponse {
        local_end: SequenceNumber::new(60),
    });

    let message = append_response_to_wire(result.clone());

    assert_eq!(append_response_from_wire(message), result);
}

#[test]
fn append_refusal_survives_the_round_trip() {
    let message = append_response_to_wire(Err(refusal()));

    assert_eq!(append_response_from_wire(message), Err(refusal()));
}

#[test]
fn append_response_without_an_outcome_is_read_as_unavailable() {
    let message = wire::AppendResponse { outcome: None };

    let result = append_response_from_wire(message);

    assert_eq!(result, Err(unreadable_without("AppendResponse.outcome")));
}

#[test]
fn read_success_survives_the_round_trip() {
    let result = Ok(domain::ReadResponse {
        batch_frames: Bytes::from_static(b"batch frames are opaque to the transport"),
        readable_end: SequenceNumber::new(60),
        sealed_end: Some(SequenceNumber::new(60)),
    });

    let message = read_response_to_wire(result.clone());

    assert_eq!(read_response_from_wire(message), result);
}

/// Having nothing to serve is a success, and an open segment has no sealed
/// end: neither may turn into something else on the way.
#[test]
fn read_success_with_nothing_readable_survives_the_round_trip() {
    let result = Ok(domain::ReadResponse {
        batch_frames: Bytes::new(),
        readable_end: SequenceNumber::ZERO,
        sealed_end: None,
    });

    let message = read_response_to_wire(result.clone());

    assert_eq!(read_response_from_wire(message), result);
}

#[test]
fn read_refusal_survives_the_round_trip() {
    let message = read_response_to_wire(Err(refusal()));

    assert_eq!(read_response_from_wire(message), Err(refusal()));
}

#[test]
fn read_response_without_an_outcome_is_read_as_unavailable() {
    let message = wire::ReadResponse { outcome: None };

    let result = read_response_from_wire(message);

    assert_eq!(result, Err(unreadable_without("ReadResponse.outcome")));
}

#[test]
fn fence_success_survives_the_round_trip() {
    let result = Ok(domain::FenceResponse {
        local_end: Some(SequenceNumber::new(60)),
        sealed_end: Some(SequenceNumber::new(58)),
    });

    let message = fence_response_to_wire(result.clone());

    assert_eq!(fence_response_from_wire(message), result);
}

/// A replica without a copy reports no end. A recovering writer treats
/// "no copy" and "an empty copy" differently, so absent must stay absent.
#[test]
fn fence_success_of_a_replica_without_a_copy_survives_the_round_trip() {
    let result = Ok(domain::FenceResponse {
        local_end: None,
        sealed_end: None,
    });

    let message = fence_response_to_wire(result.clone());

    assert_eq!(fence_response_from_wire(message), result);
}

#[test]
fn fence_success_of_an_empty_copy_keeps_its_end_at_zero() {
    let result = Ok(domain::FenceResponse {
        local_end: Some(SequenceNumber::ZERO),
        sealed_end: None,
    });

    let message = fence_response_to_wire(result.clone());

    assert_eq!(fence_response_from_wire(message), result);
}

#[test]
fn fence_refusal_survives_the_round_trip() {
    let message = fence_response_to_wire(Err(refusal()));

    assert_eq!(fence_response_from_wire(message), Err(refusal()));
}

#[test]
fn fence_response_without_an_outcome_is_read_as_unavailable() {
    let message = wire::FenceResponse { outcome: None };

    let result = fence_response_from_wire(message);

    assert_eq!(result, Err(unreadable_without("FenceResponse.outcome")));
}

#[test]
fn seal_success_survives_the_round_trip() {
    let result = Ok(domain::SealResponse {
        local_end: Some(SequenceNumber::new(60)),
    });

    let message = seal_response_to_wire(result.clone());

    assert_eq!(seal_response_from_wire(message), result);
}

#[test]
fn seal_success_of_a_replica_without_a_copy_survives_the_round_trip() {
    let result = Ok(domain::SealResponse { local_end: None });

    let message = seal_response_to_wire(result.clone());

    assert_eq!(seal_response_from_wire(message), result);
}

#[test]
fn seal_refusal_survives_the_round_trip() {
    let message = seal_response_to_wire(Err(refusal()));

    assert_eq!(seal_response_from_wire(message), Err(refusal()));
}

#[test]
fn seal_response_without_an_outcome_is_read_as_unavailable() {
    let message = wire::SealResponse { outcome: None };

    let result = seal_response_from_wire(message);

    assert_eq!(result, Err(unreadable_without("SealResponse.outcome")));
}

#[test]
fn repair_success_survives_the_round_trip() {
    let result = Ok(domain::RepairResponse {
        local_end: SequenceNumber::new(60),
    });

    let message = repair_segment_response_to_wire(result.clone());

    assert_eq!(repair_segment_response_from_wire(message), result);
}

#[test]
fn repair_refusal_survives_the_round_trip() {
    let message = repair_segment_response_to_wire(Err(refusal()));

    assert_eq!(repair_segment_response_from_wire(message), Err(refusal()));
}

#[test]
fn upload_success_survives_the_round_trip() {
    let result = Ok(domain::UploadResponse { size_bytes: 4_096 });

    let message = upload_segment_response_to_wire(result.clone());

    assert_eq!(upload_segment_response_from_wire(message), result);
}

#[test]
fn upload_refusal_survives_the_round_trip() {
    let message = upload_segment_response_to_wire(Err(refusal()));

    assert_eq!(upload_segment_response_from_wire(message), Err(refusal()));
}

#[test]
fn upload_response_without_an_outcome_is_read_as_unavailable() {
    let message = wire::UploadSegmentResponse { outcome: None };

    let result = upload_segment_response_from_wire(message);

    assert_eq!(result, Err(unreadable_without("UploadSegmentResponse.outcome")));
}

#[test]
fn repair_response_without_an_outcome_is_read_as_unavailable() {
    let message = wire::RepairSegmentResponse { outcome: None };

    let result = repair_segment_response_from_wire(message);

    assert_eq!(result, Err(unreadable_without("RepairSegmentResponse.outcome")));
}

#[test]
fn status_success_survives_the_round_trip() {
    let message = status_response_to_wire(Ok(replica_status()));

    assert_eq!(status_response_from_wire(message), Ok(replica_status()));
}

#[test]
fn status_refusal_survives_the_round_trip() {
    let message = status_response_to_wire(Err(refusal()));

    assert_eq!(status_response_from_wire(message), Err(refusal()));
}

#[test]
fn status_response_without_an_outcome_is_read_as_unavailable() {
    let message = wire::StatusResponse { outcome: None };

    let result = status_response_from_wire(message);

    assert_eq!(result, Err(unreadable_without("StatusResponse.outcome")));
}

#[test]
fn status_under_an_invalid_replica_identifier_is_read_as_unavailable() {
    let mut message = status_response_to_wire(Ok(replica_status()));
    message.outcome = Some(StatusOutcome::Success(StatusSuccess {
        replica_id: "not a replica".to_owned(),
        capacity_bytes: 1_000_000,
        free_bytes: 400_000,
        is_draining: false,
        segment_count: 12,
    }));

    let result = status_response_from_wire(message);

    let malformed_identity = MalformedMessage::InvalidReplicaId {
        field: "StatusResponse.Success.replica_id",
        reason: InvalidReplicaId::ForbiddenCharacter { character: ' ' },
    };
    assert_eq!(result, Err(unreadable_answer(&malformed_identity)));
}

#[test]
fn drain_success_survives_the_round_trip() {
    let message = drain_response_to_wire(Ok(()));

    assert_eq!(drain_response_from_wire(message), Ok(()));
}

#[test]
fn drain_refusal_survives_the_round_trip() {
    let message = drain_response_to_wire(Err(refusal()));

    assert_eq!(drain_response_from_wire(message), Err(refusal()));
}

#[test]
fn drain_response_without_an_outcome_is_read_as_unavailable() {
    let message = wire::DrainResponse { outcome: None };

    let result = drain_response_from_wire(message);

    assert_eq!(result, Err(unreadable_without("DrainResponse.outcome")));
}

#[test]
fn listed_segments_survive_the_round_trip_in_order() {
    let result = Ok(vec![segment_status(1), segment_status(2), segment_status(3)]);

    let message = list_segments_response_to_wire(result.clone());

    assert_eq!(list_segments_response_from_wire(message), result);
}

#[test]
fn empty_segment_list_survives_the_round_trip() {
    let message = list_segments_response_to_wire(Ok(Vec::new()));

    assert_eq!(list_segments_response_from_wire(message), Ok(Vec::new()));
}

#[test]
fn list_refusal_survives_the_round_trip() {
    let message = list_segments_response_to_wire(Err(refusal()));

    assert_eq!(list_segments_response_from_wire(message), Err(refusal()));
}

#[test]
fn list_response_without_an_outcome_is_read_as_unavailable() {
    let message = wire::ListSegmentsResponse { outcome: None };

    let result = list_segments_response_from_wire(message);

    assert_eq!(result, Err(unreadable_without("ListSegmentsResponse.outcome")));
}

/// A list with a hole would look like a replica that lost a copy, so one
/// unreadable entry makes the whole answer unreadable.
#[test]
fn segment_list_with_an_unreadable_entry_is_read_as_unavailable() {
    let unreadable_entry = wire::SegmentStatus {
        address: None,
        ..wire::SegmentStatus::from(segment_status(2))
    };
    let message = wire::ListSegmentsResponse {
        outcome: Some(ListSegmentsOutcome::Success(ListSegmentsSuccess {
            segments: vec![segment_status(1).into(), unreadable_entry],
        })),
    };

    let result = list_segments_response_from_wire(message);

    assert_eq!(result, Err(unreadable_without("SegmentStatus.address")));
}

#[test]
fn delete_success_survives_the_round_trip() {
    let message = delete_segment_response_to_wire(Ok(()));

    assert_eq!(delete_segment_response_from_wire(message), Ok(()));
}

#[test]
fn delete_refusal_survives_the_round_trip() {
    let message = delete_segment_response_to_wire(Err(refusal()));

    assert_eq!(delete_segment_response_from_wire(message), Err(refusal()));
}

#[test]
fn delete_response_without_an_outcome_is_read_as_unavailable() {
    let message = wire::DeleteSegmentResponse { outcome: None };

    let result = delete_segment_response_from_wire(message);

    assert_eq!(result, Err(unreadable_without("DeleteSegmentResponse.outcome")));
}
