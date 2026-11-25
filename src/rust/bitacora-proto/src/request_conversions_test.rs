//! Tests of the request conversions in `request_conversions.rs`.
//!
//! For each call: the domain request survives the trip to its message and
//! back, and each malformed message is rejected with an error that names
//! the field. The fixtures at the top build one valid request per call;
//! every test changes at most one thing about it.

use bitacora_types::{InvalidReplicaId, ReplicaId, SegmentId, StreamId};
use bytes::Bytes;

use super::*;

/// The segment every request is about: segment 3 of stream 7.
fn segment_address() -> SegmentAddress {
    SegmentAddress::new(StreamId::new(7), SegmentId::new(3))
}

/// A descriptor of [`segment_address`], written at epoch 2 from record 41.
fn segment_descriptor() -> domain::SegmentDescriptor {
    domain::SegmentDescriptor {
        address: segment_address(),
        epoch: Epoch::new(2),
        first_sequence: SequenceNumber::new(41),
    }
}

/// An append of some bytes by the writer of epoch 2.
fn append_request() -> domain::AppendRequest {
    domain::AppendRequest {
        segment: segment_descriptor(),
        writer_epoch: Epoch::new(2),
        batch_frames: Bytes::from_static(b"batch frames are opaque to the transport"),
        durable_sequence: SequenceNumber::new(40),
    }
}

/// A reader's read from record 41 that may wait a quarter of a second.
fn read_request() -> domain::ReadRequest {
    domain::ReadRequest {
        segment: segment_address(),
        from_sequence: SequenceNumber::new(41),
        max_bytes: 1024 * 1024,
        wait_for_records: Some(Duration::from_millis(250)),
        limit: domain::ReadLimit::DurableSequence,
    }
}

/// A repair of the segment, sealed at record 60, from replica `replica-b`.
fn repair_request() -> domain::RepairRequest {
    domain::RepairRequest {
        segment: segment_descriptor(),
        sealed_end: SequenceNumber::new(60),
        source_replica_id: ReplicaId::new("replica-b").expect("a valid identifier"),
        source_address: "replica-b.example:7000".to_owned(),
    }
}

/// An upload of the segment, sealed at record 60.
fn upload_request() -> domain::UploadRequest {
    domain::UploadRequest {
        segment: segment_descriptor(),
        sealed_end: SequenceNumber::new(60),
        object_key: "streams/7/segments/00000000000000000003".to_owned(),
    }
}

#[test]
fn append_request_survives_the_round_trip() {
    let message = wire::AppendRequest::from(append_request());

    assert_eq!(domain::AppendRequest::try_from(message), Ok(append_request()));
}

/// The heartbeat of an idle stream is an append without batches.
#[test]
fn append_request_without_batches_survives_the_round_trip() {
    let heartbeat = domain::AppendRequest {
        batch_frames: Bytes::new(),
        ..append_request()
    };

    let message = wire::AppendRequest::from(heartbeat.clone());

    assert_eq!(domain::AppendRequest::try_from(message), Ok(heartbeat));
}

#[test]
fn append_request_without_a_segment_is_malformed() {
    let message = wire::AppendRequest {
        segment: None,
        ..wire::AppendRequest::from(append_request())
    };

    assert_eq!(
        domain::AppendRequest::try_from(message),
        Err(MalformedMessage::MissingField {
            field: "AppendRequest.segment"
        })
    );
}

#[test]
fn append_request_to_a_segment_starting_at_sequence_zero_is_malformed() {
    let mut message = wire::AppendRequest::from(append_request());
    message.segment.as_mut().expect("the descriptor is set").first_sequence = 0;

    assert_eq!(
        domain::AppendRequest::try_from(message),
        Err(MalformedMessage::ZeroRecordSequence {
            field: "SegmentDescriptor.first_sequence"
        })
    );
}

#[test]
fn read_request_survives_the_round_trip() {
    let message = wire::ReadRequest::from(read_request());

    assert_eq!(domain::ReadRequest::try_from(message), Ok(read_request()));
}

#[test]
fn read_request_that_does_not_wait_survives_the_round_trip() {
    let immediate_read = domain::ReadRequest {
        wait_for_records: None,
        ..read_request()
    };

    let message = wire::ReadRequest::from(immediate_read);

    assert_eq!(domain::ReadRequest::try_from(message), Ok(immediate_read));
}

/// Waiting for zero time is not the same request as not waiting, and
/// neither turns into the other.
#[test]
fn read_request_with_a_zero_wait_keeps_its_wait() {
    let zero_wait_read = domain::ReadRequest {
        wait_for_records: Some(Duration::ZERO),
        ..read_request()
    };

    let message = wire::ReadRequest::from(zero_wait_read);

    assert_eq!(domain::ReadRequest::try_from(message), Ok(zero_wait_read));
}

#[test]
fn read_request_keeps_a_wait_shorter_than_a_millisecond() {
    let short_wait_read = domain::ReadRequest {
        wait_for_records: Some(Duration::from_nanos(1)),
        ..read_request()
    };

    let message = wire::ReadRequest::from(short_wait_read);

    assert_eq!(domain::ReadRequest::try_from(message), Ok(short_wait_read));
}

#[test]
fn read_request_wait_beyond_what_the_message_holds_saturates() {
    let endless_wait_read = domain::ReadRequest {
        wait_for_records: Some(Duration::MAX),
        ..read_request()
    };

    let message = wire::ReadRequest::from(endless_wait_read);

    assert_eq!(message.wait_for_records_nanoseconds, Some(u64::MAX));
}

#[test]
fn read_request_without_a_size_cap_survives_the_round_trip() {
    let uncapped_read = domain::ReadRequest {
        max_bytes: usize::MAX,
        ..read_request()
    };

    let message = wire::ReadRequest::from(uncapped_read);

    assert_eq!(domain::ReadRequest::try_from(message), Ok(uncapped_read));
}

#[test]
fn recovery_read_survives_the_round_trip() {
    let recovery_read = domain::ReadRequest {
        limit: domain::ReadLimit::LocalEnd {
            recovery_epoch: Epoch::new(5),
        },
        ..read_request()
    };

    let message = wire::ReadRequest::from(recovery_read);

    assert_eq!(domain::ReadRequest::try_from(message), Ok(recovery_read));
}

/// The safe default: a client that leaves the limit out gets the read that
/// every reader is allowed, never the one that exposes uncommitted records.
#[test]
fn read_request_without_a_recovery_epoch_stops_at_the_durable_sequence() {
    let message = wire::ReadRequest {
        recovery_epoch: None,
        ..wire::ReadRequest::from(read_request())
    };

    let request = domain::ReadRequest::try_from(message).expect("a valid request");

    assert_eq!(request.limit, domain::ReadLimit::DurableSequence);
}

#[test]
fn read_request_without_a_segment_is_malformed() {
    let message = wire::ReadRequest {
        segment: None,
        ..wire::ReadRequest::from(read_request())
    };

    assert_eq!(
        domain::ReadRequest::try_from(message),
        Err(MalformedMessage::MissingField {
            field: "ReadRequest.segment"
        })
    );
}

#[test]
fn read_request_from_sequence_zero_is_malformed() {
    let message = wire::ReadRequest {
        from_sequence: 0,
        ..wire::ReadRequest::from(read_request())
    };

    assert_eq!(
        domain::ReadRequest::try_from(message),
        Err(MalformedMessage::ZeroRecordSequence {
            field: "ReadRequest.from_sequence"
        })
    );
}

#[test]
fn fence_request_survives_the_round_trip() {
    let fence_request = domain::FenceRequest {
        segment: segment_address(),
        epoch: Epoch::new(5),
    };

    let message = wire::FenceRequest::from(fence_request);

    assert_eq!(domain::FenceRequest::try_from(message), Ok(fence_request));
}

#[test]
fn fence_request_without_a_segment_is_malformed() {
    let message = wire::FenceRequest {
        segment: None,
        epoch: 5,
    };

    assert_eq!(
        domain::FenceRequest::try_from(message),
        Err(MalformedMessage::MissingField {
            field: "FenceRequest.segment"
        })
    );
}

#[test]
fn seal_request_survives_the_round_trip() {
    let seal_request = domain::SealRequest {
        segment: segment_address(),
        sealed_end: SequenceNumber::new(60),
    };

    let message = wire::SealRequest::from(seal_request);

    assert_eq!(domain::SealRequest::try_from(message), Ok(seal_request));
}

/// A segment whose writer failed before committing anything is sealed at
/// the position before its first record, which for the first segment is 0.
#[test]
fn seal_request_of_an_empty_first_segment_survives_the_round_trip() {
    let seal_request = domain::SealRequest {
        segment: segment_address(),
        sealed_end: SequenceNumber::ZERO,
    };

    let message = wire::SealRequest::from(seal_request);

    assert_eq!(domain::SealRequest::try_from(message), Ok(seal_request));
}

#[test]
fn seal_request_without_a_segment_is_malformed() {
    let message = wire::SealRequest {
        segment: None,
        sealed_end: 60,
    };

    assert_eq!(
        domain::SealRequest::try_from(message),
        Err(MalformedMessage::MissingField {
            field: "SealRequest.segment"
        })
    );
}

#[test]
fn repair_request_survives_the_round_trip() {
    let message = wire::RepairSegmentRequest::from(repair_request());

    assert_eq!(domain::RepairRequest::try_from(message), Ok(repair_request()));
}

#[test]
fn repair_request_without_a_segment_is_malformed() {
    let message = wire::RepairSegmentRequest {
        segment: None,
        ..wire::RepairSegmentRequest::from(repair_request())
    };

    assert_eq!(
        domain::RepairRequest::try_from(message),
        Err(MalformedMessage::MissingField {
            field: "RepairSegmentRequest.segment"
        })
    );
}

#[test]
fn upload_request_survives_the_round_trip() {
    let message = wire::UploadSegmentRequest::from(upload_request());

    assert_eq!(domain::UploadRequest::try_from(message), Ok(upload_request()));
}

#[test]
fn upload_request_without_a_segment_is_malformed() {
    let message = wire::UploadSegmentRequest {
        segment: None,
        ..wire::UploadSegmentRequest::from(upload_request())
    };

    assert_eq!(
        domain::UploadRequest::try_from(message),
        Err(MalformedMessage::MissingField {
            field: "UploadSegmentRequest.segment"
        })
    );
}

#[test]
fn repair_request_from_an_invalid_replica_identifier_is_malformed() {
    let message = wire::RepairSegmentRequest {
        source_replica_id: "not a replica".to_owned(),
        ..wire::RepairSegmentRequest::from(repair_request())
    };

    assert_eq!(
        domain::RepairRequest::try_from(message),
        Err(MalformedMessage::InvalidReplicaId {
            field: "RepairSegmentRequest.source_replica_id",
            reason: InvalidReplicaId::ForbiddenCharacter { character: ' ' },
        })
    );
}

#[test]
fn delete_request_survives_the_round_trip() {
    let message = wire::DeleteSegmentRequest::from(segment_address());

    assert_eq!(SegmentAddress::try_from(message), Ok(segment_address()));
}

#[test]
fn delete_request_without_a_segment_is_malformed() {
    let message = wire::DeleteSegmentRequest { segment: None };

    assert_eq!(
        SegmentAddress::try_from(message),
        Err(MalformedMessage::MissingField {
            field: "DeleteSegmentRequest.segment"
        })
    );
}
