//! Tests of the questions the metadata records answer about themselves.
//!
//! # What is covered
//!
//! - [`SegmentRecord::contains`]: a sealed segment, an open one, and one that was sealed empty.
//! - [`StreamRecord`]: locating the segment of a sequence number, telling the open segment, and saying where the next
//!   segment starts.
//! - [`RetentionPolicy`], [`ReplicaRecord::is_alive`] and [`ReplicaRecord::used_percent`].
//! - That a stream record survives its JSON encoding and that copy states are stored as words.
//!
//! The exact JSON of every record is held to golden texts in `tests/records_golden_test.rs`.
//!
//! # How the fixtures work
//!
//! `segment` builds a segment from three numbers: its identifier, its first sequence number and, if it is sealed, its
//! sealed end. `stream` wraps a list of such segments in a stream record whose other fields are the ones a new stream
//! has. `replica` is a replica that last renewed its registration at ten seconds past the epoch and uses three quarters
//! of its disk.

use super::*;

/// When the replica of the fixtures last renewed its registration.
const HEARTBEAT_AT: Timestamp = Timestamp::from_unix_millis(10_000);

/// The seal of a segment that ends at `sealed_end`. The time of the seal plays no part in these tests.
fn seal_at(sealed_end: u64) -> SealRecord {
    SealRecord {
        end: SequenceNumber::new(sealed_end),
        at: Timestamp::UNIX_EPOCH,
    }
}

/// A segment written in epoch 1. `sealed_end` is its sealed end, or `None` for an open segment.
fn segment(segment_id: u64, first_sequence: u64, sealed_end: Option<u64>) -> SegmentRecord {
    SegmentRecord {
        segment_id: SegmentId::new(segment_id),
        epoch: Epoch::new(1),
        first_sequence: SequenceNumber::new(first_sequence),
        sealed: sealed_end.map(seal_at),
        copies: Vec::new(),
        tiered: None,
    }
}

/// A stream called "orders" with the given segments and everything else as in a new stream.
fn stream(segments: Vec<SegmentRecord>) -> StreamRecord {
    let next_segment_id = match segments.last() {
        Some(last_segment) => last_segment.segment_id.next(),
        None => SegmentId::FIRST,
    };
    StreamRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        stream_id: StreamId::new(7),
        name: "orders".to_owned(),
        replication: ReplicationSettings::DEFAULT,
        retention: RetentionPolicy::default(),
        epoch: Epoch::new(1),
        writer: None,
        next_segment_id,
        segments,
        standby_replicas: None,
        truncate_before: None,
        low_water_mark: SequenceNumber::FIRST,
        materializers: BTreeSet::new(),
    }
}

/// A replica with a disk of 1000 bytes, 250 of them free, that registered at [`HEARTBEAT_AT`].
fn replica() -> ReplicaRecord {
    ReplicaRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        replica_id: ReplicaId::new("replica-a").expect("a valid identifier"),
        address: "replica-a:7000".to_owned(),
        failure_domains: Vec::new(),
        capacity_bytes: 1_000,
        free_bytes: 250,
        is_draining: false,
        heartbeat_at: HEARTBEAT_AT,
    }
}

/// The identifier of the segment that `stream` says holds `sequence`, if any.
fn segment_holding(stream: &StreamRecord, sequence: u64) -> Option<SegmentId> {
    let found = stream.segment_containing(SequenceNumber::new(sequence))?;
    Some(found.segment_id)
}

#[test]
fn sealed_segment_contains_its_first_and_its_last_record() {
    let sealed = segment(1, 10, Some(19));

    assert!(sealed.contains(SequenceNumber::new(10)));
    assert!(sealed.contains(SequenceNumber::new(19)));
}

#[test]
fn sealed_segment_contains_nothing_outside_its_range() {
    let sealed = segment(1, 10, Some(19));

    assert!(!sealed.contains(SequenceNumber::new(9)));
    assert!(!sealed.contains(SequenceNumber::new(20)));
}

#[test]
fn open_segment_contains_everything_from_its_start() {
    let open = segment(1, 10, None);

    assert!(open.contains(SequenceNumber::new(10)));
    assert!(open.contains(SequenceNumber::new(1_000_000)));
}

#[test]
fn open_segment_contains_nothing_before_its_start() {
    let open = segment(1, 10, None);

    assert!(!open.contains(SequenceNumber::new(9)));
}

#[test]
fn segment_sealed_empty_contains_nothing() {
    let sealed_empty = segment(1, 10, Some(9));

    assert!(!sealed_empty.contains(SequenceNumber::new(10)));
}

#[test]
fn sealed_segment_knows_its_end_and_is_not_open() {
    let sealed = segment(1, 10, Some(19));

    assert_eq!(sealed.sealed_end(), Some(SequenceNumber::new(19)));
    assert!(!sealed.is_open());
}

#[test]
fn segment_describes_itself_for_a_replica() {
    let open = segment(3, 10, None);

    let descriptor = open.descriptor(StreamId::new(7));

    assert_eq!(
        descriptor,
        SegmentDescriptor {
            address: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
            epoch: Epoch::new(1),
            first_sequence: SequenceNumber::new(10),
        }
    );
}

#[test]
fn stream_finds_the_segment_holding_a_sequence() {
    let orders = stream(vec![segment(1, 1, Some(9)), segment(2, 10, None)]);

    assert_eq!(segment_holding(&orders, 9), Some(SegmentId::new(1)));
    assert_eq!(segment_holding(&orders, 10), Some(SegmentId::new(2)));
}

#[test]
fn stream_skips_an_empty_sealed_segment_when_locating_a_sequence() {
    let orders = stream(vec![segment(1, 1, Some(0)), segment(2, 1, None)]);

    assert_eq!(segment_holding(&orders, 1), Some(SegmentId::new(2)));
}

#[test]
fn stream_finds_no_segment_for_a_sequence_past_a_sealed_end() {
    let orders = stream(vec![segment(1, 1, Some(9))]);

    assert_eq!(segment_holding(&orders, 10), None);
}

#[test]
fn last_segment_is_the_open_one_while_it_is_unsealed() {
    let writing = stream(vec![segment(1, 1, Some(9)), segment(2, 10, None)]);

    let open_segment = writing.open_segment();

    assert_eq!(open_segment, writing.last_segment());
}

#[test]
fn stream_whose_last_segment_is_sealed_has_no_open_segment() {
    let closed = stream(vec![segment(1, 1, Some(9))]);

    assert_eq!(closed.open_segment(), None);
}

#[test]
fn next_segment_of_a_fresh_stream_starts_at_the_first_record() {
    let fresh = stream(Vec::new());

    assert_eq!(fresh.next_segment_first_sequence(), SequenceNumber::FIRST);
}

#[test]
fn next_segment_of_a_stream_without_segments_starts_at_the_low_water_mark() {
    let mut truncated = stream(Vec::new());
    truncated.low_water_mark = SequenceNumber::new(51);

    assert_eq!(truncated.next_segment_first_sequence(), SequenceNumber::new(51));
}

#[test]
fn next_segment_starts_right_after_the_last_sealed_end() {
    let orders = stream(vec![segment(1, 1, Some(9))]);

    assert_eq!(orders.next_segment_first_sequence(), SequenceNumber::new(10));
}

#[test]
fn next_segment_after_an_empty_sealed_one_reuses_its_start() {
    let orders = stream(vec![segment(1, 1, Some(9)), segment(2, 10, Some(9))]);

    assert_eq!(orders.next_segment_first_sequence(), SequenceNumber::new(10));
}

#[test]
fn retention_duration_survives_the_millisecond_encoding() {
    let a_week = Duration::from_hours(7 * 24);

    let policy = RetentionPolicy::keep_for(a_week);

    assert_eq!(policy.keep_sealed_for(), Some(a_week));
}

#[test]
fn retention_too_long_to_count_is_stored_as_the_longest_that_fits() {
    let policy = RetentionPolicy::keep_for(Duration::MAX);

    assert_eq!(policy.keep_sealed_for_millis, Some(u64::MAX));
}

#[test]
fn replica_is_alive_up_to_the_end_of_its_liveness_window() {
    let liveness_window = Duration::from_secs(5);
    let end_of_the_window = HEARTBEAT_AT.plus(liveness_window);

    assert!(replica().is_alive(end_of_the_window, liveness_window));
}

#[test]
fn replica_is_dead_once_its_liveness_window_has_passed() {
    let liveness_window = Duration::from_secs(5);
    let just_after_the_window = HEARTBEAT_AT.plus(liveness_window).plus(Duration::from_millis(1));

    assert!(!replica().is_alive(just_after_the_window, liveness_window));
}

#[test]
fn replica_reports_the_used_share_of_its_disk() {
    assert_eq!(replica().used_percent(), 75);
}

#[test]
fn replica_without_capacity_counts_as_full() {
    let mut without_capacity = replica();
    without_capacity.capacity_bytes = 0;

    assert_eq!(without_capacity.used_percent(), 100);
}

#[test]
fn stream_record_survives_a_json_round_trip() {
    let mut orders = stream(vec![segment(1, 1, Some(9)), segment(2, 10, None)]);
    orders.materializers.insert("pages".to_owned());
    orders.writer = Some(WriterLease {
        writer_identity: "compute-1".to_owned(),
        expires_at: Timestamp::from_unix_millis(5_000),
    });

    let serialized = serde_json::to_vec(&orders).expect("records serialize to JSON");
    let deserialized: StreamRecord = serde_json::from_slice(&serialized).expect("what was just written is valid");

    assert_eq!(deserialized, orders);
}

#[test]
fn copy_states_are_stored_as_readable_words() {
    let serialized = serde_json::to_string(&CopyState::Unverified).expect("copy states serialize to JSON");

    assert_eq!(serialized, "\"unverified\"");
}
