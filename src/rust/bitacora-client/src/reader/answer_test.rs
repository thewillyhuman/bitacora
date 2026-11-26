//! Tests of deciding what a replica's answer to a read means.
//!
//! # What is covered
//!
//! - Records at the position are handed out, in order, with their payloads; records before it are dropped.
//! - An answer that does not start at the position, fails a checksum, belongs to another segment or has a gap is
//!   refused.
//! - An answer without records means "nothing here" unless the position is past the sealed end, in which case the
//!   segment is finished; the sealed end the reader's metadata records takes precedence over the replica's.
//!
//! # How the fixtures work
//!
//! `batch_frame` encodes a real batch of the segment under test with the codec the replicas use, so the checksums are
//! genuine. `answer_to` wraps frames into the response a replica would send and classifies it. Damage is made by
//! flipping one bit of an encoded frame.

use bitacora_types::{SegmentId, StreamId};
use bytes::{Bytes, BytesMut};

use super::*;

/// The segment the answers are about.
const SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
/// Another segment, whose batches must not be taken for this one's.
const OTHER_SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(4));

/// What is known about the seal of the segment when an answer arrives.
#[derive(Clone, Copy)]
struct SealKnowledge {
    /// The sealed end the reader's metadata records, if any.
    recorded_sealed_end: Option<u64>,
    /// The sealed end the replica reports, if any.
    reported_sealed_end: Option<u64>,
}

/// Nobody knows of a seal: the segment is open as far as the reader can tell.
const NOT_SEALED: SealKnowledge = SealKnowledge {
    recorded_sealed_end: None,
    reported_sealed_end: None,
};

/// The frame of one batch of `segment` that holds the records `first_sequence..=last_sequence`. Each payload names its
/// sequence number.
fn batch_frame_of(segment: SegmentAddress, first_sequence: u64, last_sequence: u64) -> Bytes {
    let mut payloads = Vec::new();
    for sequence in first_sequence..=last_sequence {
        payloads.push(Bytes::from(format!("record-{sequence}")));
    }
    let batch = Batch::encode(segment, SequenceNumber::new(first_sequence), &payloads).expect("a valid batch");
    batch.frame().clone()
}

/// The frame of one batch of the segment under test that holds the records `first_sequence..=last_sequence`.
fn batch_frame(first_sequence: u64, last_sequence: u64) -> Bytes {
    batch_frame_of(SEGMENT, first_sequence, last_sequence)
}

/// `frame` with one bit of its last payload flipped, so that its checksum no longer matches.
fn damaged(frame: &Bytes) -> Bytes {
    let mut bytes = BytesMut::from(frame.as_ref());
    let last_byte = bytes.len() - 1;
    bytes[last_byte] ^= 0x01;
    bytes.freeze()
}

/// Classifies the answer a replica gives to a read at `next_sequence` when it sends `frames` back to back.
fn answer_to(next_sequence: u64, frames: &[Bytes], seal_knowledge: SealKnowledge) -> ReplicaAnswer {
    let mut batch_frames = BytesMut::new();
    for frame in frames {
        batch_frames.extend_from_slice(frame);
    }
    let response = ReadResponse {
        batch_frames: batch_frames.freeze(),
        readable_end: SequenceNumber::new(1_000),
        sealed_end: seal_knowledge.reported_sealed_end.map(SequenceNumber::new),
    };
    classify_response(
        SEGMENT,
        &response,
        SequenceNumber::new(next_sequence),
        seal_knowledge.recorded_sealed_end.map(SequenceNumber::new),
    )
}

/// The sequence numbers of the records in `answer`, which must be one that carries records.
fn sequences_in(answer: &ReplicaAnswer) -> Vec<u64> {
    let ReplicaAnswer::Records(records) = answer else {
        panic!("the answer carries no records: {answer:?}");
    };
    let mut sequences = Vec::new();
    for record in records {
        sequences.push(record.sequence.get());
    }
    sequences
}

#[test]
fn records_at_the_position_are_handed_out() {
    let answer = answer_to(100, &[batch_frame(100, 104)], NOT_SEALED);

    assert_eq!(sequences_in(&answer), [100, 101, 102, 103, 104]);
}

#[test]
fn records_carry_the_payloads_that_were_written() {
    let answer = answer_to(100, &[batch_frame(100, 100)], NOT_SEALED);

    let expected_record = LogRecord {
        sequence: SequenceNumber::new(100),
        payload: Bytes::from("record-100"),
    };
    assert_eq!(answer, ReplicaAnswer::Records(vec![expected_record]));
}

#[test]
fn records_before_the_position_are_dropped() {
    let answer = answer_to(102, &[batch_frame(100, 104)], NOT_SEALED);

    assert_eq!(sequences_in(&answer), [102, 103, 104]);
}

#[test]
fn records_of_several_batches_come_in_order() {
    let answer = answer_to(101, &[batch_frame(100, 101), batch_frame(102, 104)], NOT_SEALED);

    assert_eq!(sequences_in(&answer), [101, 102, 103, 104]);
}

#[test]
fn answer_that_starts_after_the_position_is_refused() {
    let answer = answer_to(100, &[batch_frame(105, 106)], NOT_SEALED);

    assert_eq!(
        answer,
        ReplicaAnswer::Failed("answer starts at 105 instead of 100".to_owned())
    );
}

#[test]
fn batch_that_fails_its_checksum_is_refused() {
    let answer = answer_to(100, &[damaged(&batch_frame(100, 104))], NOT_SEALED);

    assert!(matches!(answer, ReplicaAnswer::Failed(_)), "{answer:?}");
}

#[test]
fn damage_in_a_later_batch_refuses_the_whole_answer() {
    let answer = answer_to(
        100,
        &[batch_frame(100, 101), damaged(&batch_frame(102, 104))],
        NOT_SEALED,
    );

    assert!(matches!(answer, ReplicaAnswer::Failed(_)), "{answer:?}");
}

#[test]
fn batch_of_another_segment_is_refused() {
    let answer = answer_to(100, &[batch_frame_of(OTHER_SEGMENT, 100, 104)], NOT_SEALED);

    assert!(matches!(answer, ReplicaAnswer::Failed(_)), "{answer:?}");
}

#[test]
fn batches_with_a_gap_between_them_are_refused() {
    let answer = answer_to(100, &[batch_frame(100, 101), batch_frame(103, 104)], NOT_SEALED);

    assert!(matches!(answer, ReplicaAnswer::Failed(_)), "{answer:?}");
}

#[test]
fn empty_answer_from_a_segment_that_is_not_sealed_means_nothing_is_there() {
    let answer = answer_to(105, &[], NOT_SEALED);

    assert_eq!(answer, ReplicaAnswer::Nothing);
}

#[test]
fn empty_answer_past_the_recorded_sealed_end_finishes_the_segment() {
    let sealed_at_104 = SealKnowledge {
        recorded_sealed_end: Some(104),
        reported_sealed_end: None,
    };

    let answer = answer_to(105, &[], sealed_at_104);

    assert_eq!(answer, ReplicaAnswer::SegmentFinished);
}

#[test]
fn empty_answer_past_the_sealed_end_the_replica_reports_finishes_the_segment() {
    let replica_knows_the_seal = SealKnowledge {
        recorded_sealed_end: None,
        reported_sealed_end: Some(104),
    };

    let answer = answer_to(105, &[], replica_knows_the_seal);

    assert_eq!(answer, ReplicaAnswer::SegmentFinished);
}

#[test]
fn empty_answer_at_the_sealed_end_means_the_copy_is_short() {
    let sealed_at_104 = SealKnowledge {
        recorded_sealed_end: Some(104),
        reported_sealed_end: Some(104),
    };

    let answer = answer_to(104, &[], sealed_at_104);

    assert_eq!(answer, ReplicaAnswer::Nothing);
}

#[test]
fn recorded_sealed_end_takes_precedence_over_the_one_the_replica_reports() {
    let replica_reports_an_earlier_end = SealKnowledge {
        recorded_sealed_end: Some(110),
        reported_sealed_end: Some(104),
    };

    let answer = answer_to(105, &[], replica_reports_an_earlier_end);

    assert_eq!(answer, ReplicaAnswer::Nothing);
}

#[test]
fn answer_that_ends_before_the_position_counts_as_empty() {
    let answer = answer_to(105, &[batch_frame(100, 101)], NOT_SEALED);

    assert_eq!(answer, ReplicaAnswer::Nothing);
}

#[test]
fn answer_that_ends_before_a_position_past_the_sealed_end_finishes_the_segment() {
    let sealed_at_104 = SealKnowledge {
        recorded_sealed_end: Some(104),
        reported_sealed_end: None,
    };

    let answer = answer_to(105, &[batch_frame(100, 104)], sealed_at_104);

    assert_eq!(answer, ReplicaAnswer::SegmentFinished);
}
