//! Tests of the decisions a cursor makes without I/O.
//!
//! # What is covered
//!
//! - How far the stream is known to be committed: the furthest a replica served up to, or the end of the last sealed
//!   segment, whichever is later.
//! - Whether the connections a cursor keeps still belong to a segment's replica set.
//! - Whether a round that brought no records failed, and why.
//!
//! Fetching itself, with replicas that fail, seals, rolls and the object store, is covered end to end in
//! `tests/reader_test.rs` and `tests/tiered_read_test.rs`.
//!
//! # How the fixtures work
//!
//! The functions under test are pure, so the tests build segment records and connections by hand. A connection needs a
//! handle; the tests give it a stand-in that fails every request, because none is sent.

use bitacora_meta::SealRecord;
use bitacora_types::replica_api::UnreachableReplica;
use bitacora_types::{Epoch, ReplicaId, Timestamp};

use super::*;

/// The replica identifier `name`.
fn replica_id(name: &str) -> ReplicaId {
    ReplicaId::new(name).expect("a valid identifier")
}

/// A connection to the replica `name` that fails every request.
fn connection_to(name: &str) -> ConnectedReplica {
    ConnectedReplica {
        replica_id: replica_id(name),
        client: Arc::new(UnreachableReplica::new("the functions under test send no request")),
    }
}

/// A copy of some segment on the replica `name`.
fn copy_on(name: &str) -> CopyRecord {
    CopyRecord {
        replica_id: replica_id(name),
        state: CopyState::Unverified,
    }
}

/// The record of a segment that starts at `first_sequence` and is still open.
fn open_segment(segment_id: u64, first_sequence: u64) -> SegmentRecord {
    SegmentRecord {
        segment_id: SegmentId::new(segment_id),
        epoch: Epoch::new(1),
        first_sequence: SequenceNumber::new(first_sequence),
        sealed: None,
        copies: Vec::new(),
        tiered: None,
    }
}

/// The record of a segment that starts at `first_sequence` and is sealed at `sealed_end`.
fn sealed_segment(segment_id: u64, first_sequence: u64, sealed_end: u64) -> SegmentRecord {
    SegmentRecord {
        sealed: Some(SealRecord {
            end: SequenceNumber::new(sealed_end),
            at: Timestamp::from_unix_millis(0),
        }),
        ..open_segment(segment_id, first_sequence)
    }
}

#[test]
fn nothing_is_known_to_be_committed_on_a_stream_without_segments() {
    let known_durable = known_durable_sequence(SequenceNumber::ZERO, &[]);

    assert_eq!(known_durable, SequenceNumber::ZERO);
}

#[test]
fn end_of_the_last_sealed_segment_is_known_to_be_committed() {
    let segments = [sealed_segment(1, 1, 40), sealed_segment(2, 41, 90), open_segment(3, 91)];

    let known_durable = known_durable_sequence(SequenceNumber::ZERO, &segments);

    assert_eq!(known_durable, SequenceNumber::new(90));
}

#[test]
fn what_a_replica_served_up_to_counts_when_it_is_past_the_sealed_segments() {
    let segments = [sealed_segment(1, 1, 40), open_segment(2, 41)];

    let known_durable = known_durable_sequence(SequenceNumber::new(57), &segments);

    assert_eq!(known_durable, SequenceNumber::new(57));
}

#[test]
fn sealed_end_counts_when_it_is_past_what_a_replica_served_up_to() {
    let segments = [sealed_segment(1, 1, 40), open_segment(2, 41)];

    let known_durable = known_durable_sequence(SequenceNumber::new(12), &segments);

    assert_eq!(known_durable, SequenceNumber::new(40));
}

#[test]
fn open_segment_alone_adds_nothing_to_what_is_known() {
    let segments = [open_segment(1, 1)];

    let known_durable = known_durable_sequence(SequenceNumber::new(12), &segments);

    assert_eq!(known_durable, SequenceNumber::new(12));
}

#[test]
fn connections_to_the_copies_in_their_order_are_the_segments_replicas() {
    let replicas = [connection_to("replica-a"), connection_to("replica-b")];
    let copies = [copy_on("replica-a"), copy_on("replica-b")];

    assert!(are_the_replicas_of(&replicas, &copies));
}

#[test]
fn connections_in_another_order_are_not_the_segments_replicas() {
    let replicas = [connection_to("replica-b"), connection_to("replica-a")];
    let copies = [copy_on("replica-a"), copy_on("replica-b")];

    assert!(!are_the_replicas_of(&replicas, &copies));
}

#[test]
fn connections_are_not_the_segments_replicas_after_a_copy_moved() {
    let replicas = [connection_to("replica-a"), connection_to("replica-b")];
    let copies = [copy_on("replica-a"), copy_on("replica-c")];

    assert!(!are_the_replicas_of(&replicas, &copies));
}

#[test]
fn connections_are_not_the_segments_replicas_after_a_copy_was_added() {
    let replicas = [connection_to("replica-a"), connection_to("replica-b")];
    let copies = [copy_on("replica-a"), copy_on("replica-b"), copy_on("replica-c")];

    assert!(!are_the_replicas_of(&replicas, &copies));
}

#[test]
fn connections_are_not_the_segments_replicas_after_a_copy_was_dropped() {
    let replicas = [connection_to("replica-a"), connection_to("replica-b")];
    let copies = [copy_on("replica-a")];

    assert!(!are_the_replicas_of(&replicas, &copies));
}

#[test]
fn no_connections_are_the_replicas_of_a_segment_without_copies() {
    assert!(are_the_replicas_of(&[], &[]));
}

#[test]
fn round_in_which_a_source_failed_is_tried_again_for_that_reason() {
    let reason = reason_to_try_again(Some("request timed out".to_owned()), false);

    assert_eq!(reason, Some("request timed out".to_owned()));
}

#[test]
fn failure_counts_even_when_the_position_is_in_the_open_segment() {
    let reason = reason_to_try_again(Some("request timed out".to_owned()), true);

    assert_eq!(reason, Some("request timed out".to_owned()));
}

#[test]
fn round_without_failures_in_the_open_segment_did_not_fail() {
    let reason = reason_to_try_again(None, true);

    assert_eq!(reason, None);
}

#[test]
fn round_without_failures_in_a_sealed_segment_waits_for_repair() {
    let reason = reason_to_try_again(None, false);

    assert_eq!(
        reason,
        Some("every reachable copy of the sealed segment is incomplete".to_owned())
    );
}
