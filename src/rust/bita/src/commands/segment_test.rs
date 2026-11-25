//! Tests of the `bita segment` commands.
//!
//! # What is covered
//!
//! - The state shown for a segment at each point of its life, as a pure function of its record.
//! - The listing of a real stream: a sealed segment with its copies, before and after the controller
//!   verified them.
//!
//! # How the fixtures work
//!
//! The pure tests build segment records by hand. The others use `testing.rs`: appending through the tool
//! leaves one sealed segment per append.

use std::collections::BTreeSet;

use bitacora_meta::{CopyRecord, MetaError, RECORD_SCHEMA_VERSION, RetentionPolicy, SealRecord, TieredRecord};
use bitacora_types::{Epoch, ReplicaId, ReplicationSettings, SegmentId, SequenceNumber, StreamId, Timestamp};

use super::*;
use crate::commands::testing::{STREAM, TestCluster};

/// A segment holding records 10 to 19, sealed, with one good copy.
fn sealed_segment() -> SegmentRecord {
    SegmentRecord {
        segment_id: SegmentId::new(2),
        epoch: Epoch::new(4),
        first_sequence: SequenceNumber::new(10),
        sealed: Some(SealRecord {
            end: SequenceNumber::new(19),
            at: Timestamp::UNIX_EPOCH,
        }),
        copies: vec![CopyRecord {
            replica_id: ReplicaId::new("replica-a").expect("a valid identifier"),
            state: CopyState::Good,
        }],
        tiered: None,
    }
}

/// The record of an uploaded segment, with or without its local copies.
fn tiered(are_local_copies_dropped: bool) -> TieredRecord {
    TieredRecord {
        object_key: "streams/7/segments/00000000000000000002".to_owned(),
        size_bytes: 2_048,
        uploaded_at: Timestamp::UNIX_EPOCH,
        are_local_copies_dropped,
    }
}

/// A stream whose low-water mark is at `low_water_mark`. The rows look at nothing else in it.
fn stream_with_mark_at(low_water_mark: u64) -> StreamRecord {
    StreamRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        stream_id: StreamId::new(7),
        name: STREAM.to_owned(),
        replication: ReplicationSettings::DEFAULT,
        retention: RetentionPolicy::default(),
        epoch: Epoch::new(4),
        writer: None,
        segments: Vec::new(),
        next_segment_id: SegmentId::new(3),
        standby_replicas: None,
        truncate_before: None,
        low_water_mark: SequenceNumber::new(low_water_mark),
        materializers: BTreeSet::new(),
    }
}

#[test]
fn segment_without_a_seal_is_open() {
    let mut segment = sealed_segment();
    segment.sealed = None;

    let row = segment_row(&stream_with_mark_at(1), &segment);

    assert_eq!(row.state, "open");
    assert_eq!(row.sealed_end, None);
}

#[test]
fn sealed_segment_on_replicas_only_is_sealed() {
    let row = segment_row(&stream_with_mark_at(1), &sealed_segment());

    assert_eq!(row.state, "sealed");
    assert_eq!(row.sealed_end, Some(19));
}

#[test]
fn uploaded_segment_that_replicas_still_hold_is_tiered() {
    let mut segment = sealed_segment();
    segment.tiered = Some(tiered(false));

    let row = segment_row(&stream_with_mark_at(1), &segment);

    assert_eq!(row.state, "tiered");
    assert_eq!(row.object_size_bytes, Some(2_048));
}

#[test]
fn segment_left_to_the_object_store_alone_is_released() {
    let mut segment = sealed_segment();
    segment.tiered = Some(tiered(true));
    segment.copies.clear();

    let row = segment_row(&stream_with_mark_at(1), &segment);

    assert_eq!(row.state, "released");
    assert_eq!(row.copies, Vec::new());
}

#[test]
fn sealed_segment_below_the_low_water_mark_awaits_deletion() {
    let row = segment_row(&stream_with_mark_at(20), &sealed_segment());

    assert!(row.awaits_deletion);
}

#[test]
fn segment_the_low_water_mark_has_only_entered_does_not_await_deletion() {
    let row = segment_row(&stream_with_mark_at(15), &sealed_segment());

    assert!(!row.awaits_deletion);
}

#[test]
fn copy_is_shown_with_its_replica_and_state() {
    let row = segment_row(&stream_with_mark_at(1), &sealed_segment());

    assert_eq!(
        row.copies,
        [CopyCell {
            replica_id: "replica-a".to_owned(),
            state: "good".to_owned(),
        }]
    );
}

#[tokio::test(start_paused = true)]
async fn stream_without_records_has_no_segments() {
    let started = TestCluster::with_stream(3).await;

    let listed = list(&started.cluster, STREAM).await.expect("lists");

    assert_eq!(listed.segments, Vec::new());
}

#[tokio::test(start_paused = true)]
async fn append_through_the_tool_leaves_one_sealed_segment() {
    let started = TestCluster::with_stream(3).await;
    started.append_numbered(1, 3).await;

    let listed = list(&started.cluster, STREAM).await.expect("lists");

    assert_eq!(listed.segments.len(), 1);
    assert_eq!(listed.segments[0].first_sequence, 1);
    assert_eq!(listed.segments[0].sealed_end, Some(3));
    assert_eq!(listed.segments[0].state, "sealed");
}

#[tokio::test(start_paused = true)]
async fn copies_are_unverified_until_the_controller_has_looked() {
    let started = TestCluster::with_stream(3).await;
    started.append_numbered(1, 3).await;

    let listed = list(&started.cluster, STREAM).await.expect("lists");

    assert_eq!(listed.segments[0].copies[0].state, "unverified");
}

#[tokio::test(start_paused = true)]
async fn copies_are_good_once_the_controller_has_verified_them() {
    let started = TestCluster::with_stream(3).await;
    started.append_numbered(1, 3).await;
    started.run_controller_pass().await;

    let listed = list(&started.cluster, STREAM).await.expect("lists");

    assert_eq!(listed.segments[0].copies[0].state, "good");
}

#[tokio::test(start_paused = true)]
async fn listing_the_segments_of_a_stream_that_does_not_exist_fails() {
    let started = TestCluster::start(3).await;

    let failed = list(&started.cluster, "no-such-stream").await;

    assert_eq!(
        failed,
        Err(CommandError::Metadata(MetaError::StreamNotFound {
            name: "no-such-stream".to_owned()
        }))
    );
}
