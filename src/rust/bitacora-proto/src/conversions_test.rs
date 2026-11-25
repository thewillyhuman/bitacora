//! Tests of the shared value conversions in `conversions.rs`.
//!
//! Each conversion is checked in two ways: a domain value survives the trip
//! to its message and back, and each way a message can be malformed is
//! rejected with an error that names the field at fault.

use super::*;

/// A segment address used throughout: segment 3 of stream 7.
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

/// The status of a sealed, healthy copy of [`segment_address`].
fn sealed_segment_status() -> domain::SegmentStatus {
    domain::SegmentStatus {
        address: segment_address(),
        local_end: Some(SequenceNumber::new(60)),
        fenced_epoch: Epoch::new(2),
        sealed_end: Some(SequenceNumber::new(60)),
        size_bytes: 4096,
        is_damaged: false,
        descriptor: Some(segment_descriptor()),
    }
}

#[test]
fn segment_address_survives_the_round_trip() {
    let message = wire::SegmentAddress::from(segment_address());

    assert_eq!(SegmentAddress::from(message), segment_address());
}

#[test]
fn segment_descriptor_survives_the_round_trip() {
    let message = wire::SegmentDescriptor::from(segment_descriptor());

    assert_eq!(
        domain::SegmentDescriptor::try_from(message),
        Ok(segment_descriptor())
    );
}

#[test]
fn segment_descriptor_without_an_address_is_malformed() {
    let message = wire::SegmentDescriptor {
        address: None,
        ..wire::SegmentDescriptor::from(segment_descriptor())
    };

    assert_eq!(
        domain::SegmentDescriptor::try_from(message),
        Err(MalformedMessage::MissingField {
            field: "SegmentDescriptor.address"
        })
    );
}

#[test]
fn segment_descriptor_starting_at_sequence_zero_is_malformed() {
    let message = wire::SegmentDescriptor {
        first_sequence: 0,
        ..wire::SegmentDescriptor::from(segment_descriptor())
    };

    assert_eq!(
        domain::SegmentDescriptor::try_from(message),
        Err(MalformedMessage::ZeroRecordSequence {
            field: "SegmentDescriptor.first_sequence"
        })
    );
}

#[test]
fn status_of_a_sealed_copy_survives_the_round_trip() {
    let message = wire::SegmentStatus::from(sealed_segment_status());

    assert_eq!(
        domain::SegmentStatus::try_from(message),
        Ok(sealed_segment_status())
    );
}

/// A replica that only holds fencing state for a segment reports no end at
/// all, which must not turn into "ends at zero" on the way.
#[test]
fn status_of_a_copy_without_data_keeps_its_ends_absent() {
    let status = domain::SegmentStatus {
        local_end: None,
        sealed_end: None,
        is_damaged: true,
        descriptor: None,
        ..sealed_segment_status()
    };

    let message = wire::SegmentStatus::from(status);

    assert_eq!(domain::SegmentStatus::try_from(message), Ok(status));
}

#[test]
fn status_of_a_copy_with_a_file_carries_what_its_header_says() {
    let message = wire::SegmentStatus::from(sealed_segment_status());

    let status = domain::SegmentStatus::try_from(message).expect("a valid status");

    assert_eq!(status.descriptor, Some(segment_descriptor()));
}

/// A replica older than the descriptor field sends none. Its status must still be read.
#[test]
fn status_from_a_replica_that_sends_no_descriptor_is_read_without_one() {
    let message = wire::SegmentStatus {
        descriptor: None,
        ..wire::SegmentStatus::from(sealed_segment_status())
    };

    let status = domain::SegmentStatus::try_from(message).expect("a valid status");

    assert_eq!(status.descriptor, None);
}

#[test]
fn status_whose_descriptor_starts_at_sequence_zero_is_malformed() {
    let mut message = wire::SegmentStatus::from(sealed_segment_status());
    if let Some(descriptor) = &mut message.descriptor {
        descriptor.first_sequence = 0;
    }

    assert_eq!(
        domain::SegmentStatus::try_from(message),
        Err(MalformedMessage::ZeroRecordSequence {
            field: "SegmentDescriptor.first_sequence"
        })
    );
}

#[test]
fn segment_status_without_an_address_is_malformed() {
    let message = wire::SegmentStatus {
        address: None,
        ..wire::SegmentStatus::from(sealed_segment_status())
    };

    assert_eq!(
        domain::SegmentStatus::try_from(message),
        Err(MalformedMessage::MissingField {
            field: "SegmentStatus.address"
        })
    );
}

#[test]
fn record_sequence_accepts_the_first_record() {
    assert_eq!(
        record_sequence(1, "ReadRequest.from_sequence"),
        Ok(SequenceNumber::FIRST)
    );
}

#[test]
fn record_sequence_rejects_zero() {
    assert_eq!(
        record_sequence(0, "ReadRequest.from_sequence"),
        Err(MalformedMessage::ZeroRecordSequence {
            field: "ReadRequest.from_sequence"
        })
    );
}

#[test]
fn replica_identifier_with_a_path_separator_is_malformed() {
    let read = replica_id_from_wire(
        "zone/a".to_owned(),
        "RepairSegmentRequest.source_replica_id",
    );

    assert_eq!(
        read,
        Err(MalformedMessage::InvalidReplicaId {
            field: "RepairSegmentRequest.source_replica_id",
            reason: InvalidReplicaId::ForbiddenCharacter { character: '/' },
        })
    );
}

#[test]
fn empty_replica_identifier_is_malformed() {
    let read = replica_id_from_wire(String::new(), "StatusResponse.Success.replica_id");

    assert_eq!(
        read,
        Err(MalformedMessage::InvalidReplicaId {
            field: "StatusResponse.Success.replica_id",
            reason: InvalidReplicaId::Empty,
        })
    );
}

#[test]
fn malformed_message_names_the_field_at_fault() {
    let malformed = MalformedMessage::MissingField {
        field: "AppendRequest.segment",
    };

    assert_eq!(
        malformed.to_string(),
        "field `AppendRequest.segment` is missing"
    );
}
