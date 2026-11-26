//! Tests of `bita metadata rebuild`.
//!
//! # What is covered
//!
//! - The plan, as a pure function of what was found: recorded and inferred seals, the open last segment,
//!   segments left to the object store, missing copies, more copies than the replication asks for, gaps,
//!   disagreeing copies, unnamed and deleted streams, what an existing record contributes, epochs and
//!   segment identifiers.
//! - The command, end to end: a cluster loses its metadata store, is rebuilt from its replicas and its
//!   object store, and afterwards a writer takes over and every committed record is read back.
//! - What stops a rebuild: no replicas, problems without force, and that a dry run writes nothing.
//!
//! # How the fixtures work
//!
//! The pure tests build inventories by hand. `file_copy` is what a replica reports for a copy with a
//! file, `bare_state` for one that only fenced or sealed. `tiered_object` builds a real tiered object and
//! reads its footer back, because a footer cannot be made any other way.
//!
//! The end-to-end test uses `testing.rs` and lets the real controller do its work between the steps.

use bitacora::TakeoverPolicy;
use bitacora_controller::ControllerSettings;
use bitacora_segment::format::{Batch, SegmentHeader};
use bitacora_segment::tiered::encode_tiered_segment;
use bitacora_types::replica_api::SegmentDescriptor;
use bytes::{BufMut, Bytes, BytesMut};

use super::*;
use crate::commands::stream;
use crate::commands::testing::{STREAM, TestCluster};

/// The stream the pure tests plan.
const STREAM_ID: StreamId = StreamId::new(7);
/// The moment the pure tests plan at.
const NOW: Timestamp = Timestamp::from_unix_millis(1_000_000);

/// The identifier of a replica called `replica-<letter>`.
fn replica(letter: char) -> ReplicaId {
    ReplicaId::new(format!("replica-{letter}")).expect("a valid identifier")
}

/// What a replica reports for a copy with a file: the segment written under `epoch`, starting at
/// `first_sequence`, holding records up to `local_end`, sealed or not.
fn file_copy(
    segment_id: u64,
    epoch: u64,
    first_sequence: u64,
    local_end: u64,
    sealed_end: Option<u64>,
) -> SegmentStatus {
    let address = SegmentAddress::new(STREAM_ID, SegmentId::new(segment_id));
    SegmentStatus {
        address,
        local_end: Some(SequenceNumber::new(local_end)),
        fenced_epoch: Epoch::new(epoch),
        sealed_end: sealed_end.map(SequenceNumber::new),
        size_bytes: 1_024,
        is_damaged: false,
        descriptor: Some(SegmentDescriptor {
            address,
            epoch: Epoch::new(epoch),
            first_sequence: SequenceNumber::new(first_sequence),
        }),
    }
}

/// What a replica reports for a segment it only fenced at `fenced_epoch`: no file, no header.
fn bare_state(segment_id: u64, fenced_epoch: u64) -> SegmentStatus {
    SegmentStatus {
        address: SegmentAddress::new(STREAM_ID, SegmentId::new(segment_id)),
        local_end: None,
        fenced_epoch: Epoch::new(fenced_epoch),
        sealed_end: None,
        size_bytes: 0,
        is_damaged: false,
        descriptor: None,
    }
}

/// The tiered object of a segment written under `epoch` that holds the records `first_sequence` to
/// `sealed_end`.
fn tiered_object(segment_id: u64, epoch: u64, first_sequence: u64, sealed_end: u64) -> TieredObject {
    let address = SegmentAddress::new(STREAM_ID, SegmentId::new(segment_id));
    let mut frames = BytesMut::new();
    for sequence in first_sequence..=sealed_end {
        let batch = Batch::encode(
            address,
            SequenceNumber::new(sequence),
            &[Bytes::from(format!("record-{sequence}"))],
        )
        .expect("a valid batch");
        frames.put_slice(batch.frame());
    }
    let header = SegmentHeader {
        address,
        epoch: Epoch::new(epoch),
        first_sequence: SequenceNumber::new(first_sequence),
    };
    let object =
        encode_tiered_segment(header, SequenceNumber::new(sealed_end), &frames.freeze()).expect("a valid segment");
    TieredObject {
        object_key: format!("streams/7/segments/{segment_id:020}"),
        size_bytes: object.len() as u64,
        footer: TieredFooter::decode(&object).expect("a valid footer"),
    }
}

/// Inputs in which every listed replica reports the same copies, the stream is called `orders`, and
/// nothing else is known.
fn inputs_with(holders: &[char], copies: &[SegmentStatus]) -> RebuildInputs {
    let mut inventories = Vec::new();
    for letter in holders {
        inventories.push(ReplicaInventory {
            replica_id: replica(*letter),
            copies: copies.to_vec(),
        });
    }
    let mut names_by_stream_id = BTreeMap::new();
    names_by_stream_id.insert(STREAM_ID.get(), "orders".to_owned());
    RebuildInputs {
        existing_streams: Vec::new(),
        deleted_stream_ids: BTreeSet::new(),
        names_by_stream_id,
        inventories,
        tiered_objects: Vec::new(),
        now: NOW,
    }
}

/// Plans a rebuild that is expected to concern exactly one stream, and returns that stream's plan.
fn plan_of_the_stream(inputs: &RebuildInputs) -> PlannedStream {
    let mut planned_streams = plan_rebuild(inputs);
    assert_eq!(planned_streams.len(), 1, "{planned_streams:?}");
    planned_streams.remove(0)
}

/// The record planned for the one stream of `inputs`.
fn record_of_the_stream(inputs: &RebuildInputs) -> StreamRecord {
    let planned = plan_of_the_stream(inputs);
    planned.record.expect("a record is planned")
}

/// The identifiers of the replicas a segment record lists, in order.
fn copy_holders(segment: &SegmentRecord) -> Vec<String> {
    let mut holders = Vec::new();
    for copy in &segment.copies {
        holders.push(copy.replica_id.to_string());
    }
    holders
}

#[test]
fn segment_whose_seal_the_replicas_recorded_is_sealed_there() {
    let inputs = inputs_with(&['a', 'b', 'c'], &[file_copy(1, 4, 1, 9, Some(9))]);

    let record = record_of_the_stream(&inputs);

    assert_eq!(record.segments[0].sealed_end(), Some(SequenceNumber::new(9)));
}

#[test]
fn rebuilt_segment_keeps_the_epoch_and_start_of_its_header() {
    let inputs = inputs_with(&['a', 'b', 'c'], &[file_copy(3, 4, 20, 29, Some(29))]);

    let record = record_of_the_stream(&inputs);

    assert_eq!(record.segments[0].segment_id, SegmentId::new(3));
    assert_eq!(record.segments[0].epoch, Epoch::new(4));
    assert_eq!(record.segments[0].first_sequence, SequenceNumber::new(20));
}

#[test]
fn found_copies_are_recorded_as_unverified() {
    let inputs = inputs_with(&['a', 'b', 'c'], &[file_copy(1, 4, 1, 9, Some(9))]);

    let record = record_of_the_stream(&inputs);

    for copy in &record.segments[0].copies {
        assert_eq!(copy.state, CopyState::Unverified);
    }
    assert_eq!(
        copy_holders(&record.segments[0]),
        ["replica-a", "replica-b", "replica-c"]
    );
}

#[test]
fn seal_nobody_recorded_is_inferred_from_the_start_of_the_next_segment() {
    let copies = [file_copy(1, 4, 1, 12, None), file_copy(2, 5, 10, 15, None)];
    let inputs = inputs_with(&['a', 'b', 'c'], &copies);

    let planned = plan_of_the_stream(&inputs);

    let record = planned.record.expect("a record is planned");
    assert_eq!(record.segments[0].sealed_end(), Some(SequenceNumber::new(9)));
    assert_eq!(planned.problems, Vec::<String>::new());
}

#[test]
fn last_segment_without_a_recorded_seal_is_left_open() {
    let copies = [file_copy(1, 4, 1, 9, Some(9)), file_copy(2, 5, 10, 15, None)];
    let inputs = inputs_with(&['a', 'b', 'c'], &copies);

    let record = record_of_the_stream(&inputs);

    assert!(record.segments[1].is_open());
}

#[test]
fn segment_with_an_object_and_no_file_is_left_to_the_object_store() {
    let mut inputs = inputs_with(&['a', 'b', 'c'], &[file_copy(2, 4, 10, 19, Some(19))]);
    inputs.tiered_objects = vec![tiered_object(1, 4, 1, 9)];

    let record = record_of_the_stream(&inputs);

    let released_segment = &record.segments[0];
    assert_eq!(released_segment.copies, Vec::new());
    assert!(
        released_segment
            .tiered
            .as_ref()
            .is_some_and(|tiered| tiered.are_local_copies_dropped)
    );
    assert_eq!(released_segment.sealed_end(), Some(SequenceNumber::new(9)));
}

#[test]
fn segment_with_an_object_and_files_keeps_its_copies() {
    let mut inputs = inputs_with(&['a', 'b', 'c'], &[file_copy(1, 4, 1, 9, Some(9))]);
    inputs.tiered_objects = vec![tiered_object(1, 4, 1, 9)];

    let record = record_of_the_stream(&inputs);

    let tiered_segment = &record.segments[0];
    assert_eq!(tiered_segment.copies.len(), 3);
    assert!(
        tiered_segment
            .tiered
            .as_ref()
            .is_some_and(|tiered| !tiered.are_local_copies_dropped)
    );
}

#[test]
fn copy_that_was_not_found_is_recorded_as_missing_for_the_controller_to_rebuild() {
    let inputs = inputs_with(&['a', 'b'], &[file_copy(1, 4, 1, 9, Some(9))]);

    let record = record_of_the_stream(&inputs);

    let copies = &record.segments[0].copies;
    assert_eq!(
        copy_holders(&record.segments[0]),
        ["replica-a", "replica-b", "rebuild-missing-3"]
    );
    assert_eq!(copies[2].state, CopyState::Bad);
}

#[test]
fn missing_copy_is_an_assumption_and_not_a_problem() {
    let inputs = inputs_with(&['a', 'b'], &[file_copy(1, 4, 1, 9, Some(9))]);

    let planned = plan_of_the_stream(&inputs);

    assert_eq!(planned.problems, Vec::<String>::new());
}

#[test]
fn longest_copies_are_kept_when_more_were_found_than_the_replication_asks_for() {
    let mut inputs = inputs_with(&['b', 'c', 'd'], &[file_copy(1, 4, 1, 9, Some(9))]);
    inputs.inventories.push(ReplicaInventory {
        replica_id: replica('a'),
        copies: vec![file_copy(1, 4, 1, 5, Some(9))],
    });

    let record = record_of_the_stream(&inputs);

    assert_eq!(
        copy_holders(&record.segments[0]),
        ["replica-b", "replica-c", "replica-d"]
    );
}

#[test]
fn segments_that_do_not_connect_are_a_problem() {
    let copies = [file_copy(1, 4, 1, 9, Some(9)), file_copy(3, 5, 20, 29, Some(29))];
    let inputs = inputs_with(&['a', 'b', 'c'], &copies);

    let planned = plan_of_the_stream(&inputs);

    assert_eq!(planned.problems.len(), 1);
}

#[test]
fn stream_with_a_gap_is_rebuilt_from_the_segment_after_it() {
    let copies = [file_copy(1, 4, 1, 9, Some(9)), file_copy(3, 5, 20, 29, Some(29))];
    let inputs = inputs_with(&['a', 'b', 'c'], &copies);

    let record = record_of_the_stream(&inputs);

    assert_eq!(record.segments.len(), 1);
    assert_eq!(record.low_water_mark, SequenceNumber::new(20));
}

#[test]
fn copies_that_disagree_about_the_seal_are_a_problem() {
    let mut inputs = inputs_with(&['a', 'b'], &[file_copy(1, 4, 1, 9, Some(9))]);
    inputs.inventories.push(ReplicaInventory {
        replica_id: replica('c'),
        copies: vec![file_copy(1, 4, 1, 9, Some(8))],
    });

    let planned = plan_of_the_stream(&inputs);

    assert_eq!(planned.problems.len(), 1);
}

#[test]
fn copies_that_disagree_about_the_header_are_a_problem() {
    let mut inputs = inputs_with(&['a', 'b'], &[file_copy(1, 4, 1, 9, Some(9))]);
    inputs.inventories.push(ReplicaInventory {
        replica_id: replica('c'),
        copies: vec![file_copy(1, 4, 2, 9, Some(9))],
    });

    let planned = plan_of_the_stream(&inputs);

    assert_eq!(planned.problems.len(), 1);
}

#[test]
fn sealed_segment_without_a_copy_that_reaches_its_end_is_a_problem() {
    let inputs = inputs_with(&['a', 'b', 'c'], &[file_copy(1, 4, 1, 7, Some(9))]);

    let planned = plan_of_the_stream(&inputs);

    assert_eq!(planned.problems.len(), 1);
}

#[test]
fn damaged_copy_does_not_count_as_complete() {
    let mut damaged_copy = file_copy(1, 4, 1, 9, Some(9));
    damaged_copy.is_damaged = true;
    let inputs = inputs_with(&['a', 'b', 'c'], &[damaged_copy]);

    let planned = plan_of_the_stream(&inputs);

    assert_eq!(planned.problems.len(), 1);
}

#[test]
fn open_segment_found_on_fewer_replicas_than_recovery_needs_is_a_problem() {
    let inputs = inputs_with(&['a'], &[file_copy(1, 4, 1, 9, None)]);

    let planned = plan_of_the_stream(&inputs);

    assert_eq!(planned.problems.len(), 1);
}

#[test]
fn open_segment_found_on_a_recovery_quorum_is_no_problem() {
    let inputs = inputs_with(&['a', 'b'], &[file_copy(1, 4, 1, 9, None)]);

    let planned = plan_of_the_stream(&inputs);

    assert_eq!(planned.problems, Vec::<String>::new());
}

#[test]
fn stream_without_a_name_is_not_rebuilt() {
    let mut inputs = inputs_with(&['a', 'b', 'c'], &[file_copy(1, 4, 1, 9, Some(9))]);
    inputs.names_by_stream_id.clear();

    let planned = plan_of_the_stream(&inputs);

    assert_eq!(planned.record, None);
    assert_eq!(planned.problems.len(), 1);
}

#[test]
fn stream_recorded_as_deleted_is_not_rebuilt() {
    let mut inputs = inputs_with(&['a', 'b', 'c'], &[file_copy(1, 4, 1, 9, Some(9))]);
    inputs.deleted_stream_ids.insert(STREAM_ID);

    let planned = plan_of_the_stream(&inputs);

    assert_eq!(planned.record, None);
    assert_eq!(planned.problems.len(), 1);
}

#[test]
fn stream_of_which_only_bare_state_was_found_is_not_rebuilt() {
    let inputs = inputs_with(&['a', 'b', 'c'], &[bare_state(1, 4)]);

    let planned = plan_of_the_stream(&inputs);

    assert_eq!(planned.record, None);
    assert_eq!(planned.problems.len(), 1);
}

#[test]
fn epoch_is_one_above_every_epoch_found() {
    let copies = [file_copy(1, 4, 1, 9, Some(9)), bare_state(2, 11)];
    let inputs = inputs_with(&['a', 'b', 'c'], &copies);

    let record = record_of_the_stream(&inputs);

    assert_eq!(record.epoch, Epoch::new(12));
}

#[test]
fn next_segment_identifier_is_above_every_segment_found() {
    let copies = [file_copy(1, 4, 1, 9, Some(9)), bare_state(6, 5)];
    let inputs = inputs_with(&['a', 'b', 'c'], &copies);

    let record = record_of_the_stream(&inputs);

    assert_eq!(record.next_segment_id, SegmentId::new(7));
}

#[test]
fn rebuilt_stream_has_no_writer_and_no_standby() {
    let inputs = inputs_with(&['a', 'b', 'c'], &[file_copy(1, 4, 1, 9, Some(9))]);

    let record = record_of_the_stream(&inputs);

    assert_eq!(record.writer, None);
    assert_eq!(record.standby_replicas, None);
}

#[test]
fn low_water_mark_is_the_first_sequence_still_found() {
    let inputs = inputs_with(&['a', 'b', 'c'], &[file_copy(4, 4, 50, 59, Some(59))]);

    let record = record_of_the_stream(&inputs);

    assert_eq!(record.low_water_mark, SequenceNumber::new(50));
}

/// The record a restored snapshot would hold for the stream: older than the replicas, but it knows what
/// replicas do not store.
fn restored_record() -> StreamRecord {
    let mut materializers = BTreeSet::new();
    materializers.insert("pages".to_owned());
    StreamRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        stream_id: STREAM_ID,
        name: "orders-from-the-snapshot".to_owned(),
        replication: ReplicationSettings::new(5, 3).expect("valid settings"),
        retention: RetentionPolicy::keep_for(std::time::Duration::from_hours(1)),
        epoch: Epoch::new(2),
        writer: None,
        segments: Vec::new(),
        next_segment_id: SegmentId::new(40),
        standby_replicas: None,
        truncate_before: Some(SequenceNumber::new(3)),
        low_water_mark: SequenceNumber::new(5),
        materializers,
    }
}

#[test]
fn record_the_metadata_store_still_has_supplies_what_replicas_do_not_store() {
    let mut inputs = inputs_with(&['a', 'b', 'c'], &[file_copy(1, 4, 1, 9, Some(9))]);
    inputs.existing_streams = vec![restored_record()];

    let record = record_of_the_stream(&inputs);

    assert_eq!(record.name, "orders-from-the-snapshot");
    assert_eq!(record.replication, restored_record().replication);
    assert_eq!(record.retention, restored_record().retention);
    assert_eq!(record.materializers, restored_record().materializers);
    assert_eq!(record.truncate_before, Some(SequenceNumber::new(3)));
}

#[test]
fn low_water_mark_of_an_existing_record_is_not_moved_back() {
    let mut inputs = inputs_with(&['a', 'b', 'c'], &[file_copy(1, 4, 1, 9, Some(9))]);
    inputs.existing_streams = vec![restored_record()];

    let record = record_of_the_stream(&inputs);

    assert_eq!(record.low_water_mark, SequenceNumber::new(5));
}

#[test]
fn next_segment_identifier_of_an_existing_record_is_not_moved_back() {
    let mut inputs = inputs_with(&['a', 'b', 'c'], &[file_copy(1, 4, 1, 9, Some(9))]);
    inputs.existing_streams = vec![restored_record()];

    let record = record_of_the_stream(&inputs);

    assert_eq!(record.next_segment_id, SegmentId::new(40));
}

#[test]
fn replication_asked_for_by_an_existing_record_decides_how_many_copies_are_listed() {
    let mut inputs = inputs_with(&['a', 'b', 'c'], &[file_copy(1, 4, 1, 9, Some(9))]);
    inputs.existing_streams = vec![restored_record()];

    let record = record_of_the_stream(&inputs);

    assert_eq!(record.segments[0].copies.len(), 5);
}

#[test]
fn every_stream_found_gets_a_plan_sorted_by_identifier() {
    let mut inputs = inputs_with(&['a', 'b', 'c'], &[file_copy(1, 4, 1, 9, Some(9))]);
    let other_stream = SegmentAddress::new(StreamId::new(3), SegmentId::new(1));
    let mut other_copy = file_copy(1, 2, 1, 4, Some(4));
    other_copy.address = other_stream;
    inputs.inventories[0].copies.push(other_copy);

    let planned_streams = plan_rebuild(&inputs);

    assert_eq!(planned_streams.len(), 2);
    assert_eq!(planned_streams[0].stream_id, StreamId::new(3));
}

#[test]
fn nothing_found_plans_nothing() {
    let inputs = inputs_with(&['a', 'b', 'c'], &[]);

    assert_eq!(plan_rebuild(&inputs), Vec::new());
}

#[test]
fn object_key_of_a_tiered_segment_names_its_segment() {
    assert_eq!(
        segment_address_of("streams/7/segments/00000000000000000003"),
        Some(SegmentAddress::new(STREAM_ID, SegmentId::new(3)))
    );
}

#[test]
fn object_key_of_something_else_names_no_segment() {
    assert_eq!(segment_address_of("streams/7/manifests/3"), None);
}

/// The names the end-to-end tests give the rebuild: the one stream of the cluster.
fn names() -> Vec<(u64, String)> {
    vec![(1, STREAM.to_owned())]
}

/// A cluster that went through the life of a stream and then lost its metadata store.
///
/// Before the loss: records 1 to 3 are in a segment that was uploaded and left to the object store;
/// records 4 to 6 are in a sealed segment of which one copy is lost with its node's disk; records 7 and 8
/// are in a segment that a writer left open when it stopped without a word. After the loss the replicas
/// have registered again, and nothing else is known.
async fn cluster_that_lost_its_metadata_store() -> TestCluster {
    let started = TestCluster::with_stream(4).await;
    let tier_at_once = ControllerSettings {
        tiering_delay: std::time::Duration::ZERO,
        hot_read_window: std::time::Duration::ZERO,
        ..ControllerSettings::default()
    };
    started.append_numbered(1, 3).await;
    for _pass in 0..3 {
        started
            .controller(tier_at_once.clone())
            .run_pass()
            .await
            .expect("the pass runs");
    }

    started.append_numbered(4, 3).await;
    started.run_controller_pass().await;
    let second_segment = started
        .cluster
        .metadata
        .get_stream(STREAM)
        .await
        .expect("the stream exists")
        .segments[1]
        .clone();
    let holder_that_loses_its_disk = second_segment.copies[0].replica_id.clone();
    for node in started.nodes.nodes() {
        if node.replica_id() == holder_that_loses_its_disk {
            node.crash();
            node.lose_disk();
            node.restart().await;
        }
    }

    started.run_controller_pass().await;
    let abandoned_writer = started
        .cluster
        .client("compute-1")
        .open_writer(STREAM, TakeoverPolicy::Force)
        .await
        .expect("the writer opens");
    for number in 7..=8 {
        abandoned_writer
            .append(Bytes::from(format!("record-{number}")))
            .await
            .expect("the record commits");
    }
    // The writer stops without closing: its segment stays open on the replicas.
    std::mem::forget(abandoned_writer);

    started.lose_the_metadata_store().await;
    started
        .nodes
        .renew_registrations()
        .await
        .expect("the replicas register again");
    started
}

/// The payload texts of every record of the stream that can be read, in order.
async fn readable_payloads(started: &TestCluster) -> Vec<String> {
    let found = stream::read(&started.cluster, STREAM, Some(1), 100)
        .await
        .expect("reads");
    let mut payloads = Vec::new();
    for record in found.records {
        payloads.push(record.payload_text);
    }
    payloads
}

#[tokio::test(start_paused = true)]
async fn dry_run_shows_the_plan_and_writes_nothing() {
    let started = cluster_that_lost_its_metadata_store().await;
    let keys_before = started.metadata_key_count().await;

    let report = rebuild(&started.cluster, &names(), &[], RebuildMode::default())
        .await
        .expect("the plan is made");

    assert!(!report.is_applied);
    assert_eq!(report.streams.len(), 1);
    assert_eq!(started.metadata_key_count().await, keys_before);
}

#[tokio::test(start_paused = true)]
async fn plan_lists_every_segment_that_still_holds_records() {
    let started = cluster_that_lost_its_metadata_store().await;

    let report = rebuild(&started.cluster, &names(), &[], RebuildMode::default())
        .await
        .expect("the plan is made");

    let mut states = Vec::new();
    for segment in &report.streams[0].segments {
        states.push((segment.first_sequence, segment.state.clone()));
    }
    assert_eq!(
        states,
        [
            (1, "released".to_owned()),
            (4, "sealed".to_owned()),
            (7, "open".to_owned())
        ]
    );
}

/// The reason for the whole command: after the metadata store is lost and rebuilt, a writer can take the
/// stream over and nothing that was committed is missing.
#[tokio::test(start_paused = true)]
async fn rebuilt_stream_serves_every_committed_record_and_takes_new_ones() {
    let started = cluster_that_lost_its_metadata_store().await;

    let report = rebuild(
        &started.cluster,
        &names(),
        &[],
        RebuildMode {
            is_applied: true,
            ..RebuildMode::default()
        },
    )
    .await
    .expect("the plan is applied");
    started.run_controller_pass().await;
    stream::append(
        &started.cluster,
        STREAM,
        vec![Bytes::from("record-9")],
        TakeoverPolicy::Force,
    )
    .await
    .expect("a writer takes the rebuilt stream over");

    assert!(report.is_applied);
    let mut expected_payloads = Vec::new();
    for number in 1..=9 {
        expected_payloads.push(format!("record-{number}"));
    }
    assert_eq!(readable_payloads(&started).await, expected_payloads);
}

#[tokio::test(start_paused = true)]
async fn controller_rebuilds_the_copy_that_was_lost_once_the_stream_is_rebuilt() {
    let started = cluster_that_lost_its_metadata_store().await;
    rebuild(
        &started.cluster,
        &names(),
        &[],
        RebuildMode {
            is_applied: true,
            ..RebuildMode::default()
        },
    )
    .await
    .expect("the plan is applied");

    // The controller verifies the copies that were found and rebuilds the one that was not.
    let first_pass = started.run_controller_pass().await;
    let second_pass = started.run_controller_pass().await;

    assert_eq!(first_pass.copies_repaired + second_pass.copies_repaired, 1);
    assert_eq!(second_pass.segments_missing_copies, 0);
}

#[tokio::test(start_paused = true)]
async fn stream_created_after_a_rebuild_does_not_reuse_an_identifier() {
    let started = cluster_that_lost_its_metadata_store().await;
    rebuild(
        &started.cluster,
        &names(),
        &[],
        RebuildMode {
            is_applied: true,
            ..RebuildMode::default()
        },
    )
    .await
    .expect("the plan is applied");

    let created = stream::create(&started.cluster, "audit", 3, 2, None)
        .await
        .expect("the stream is created");

    assert_eq!(created.stream_id, 2);
}

#[tokio::test(start_paused = true)]
async fn rebuild_without_a_name_for_the_stream_is_not_applied() {
    let started = cluster_that_lost_its_metadata_store().await;

    let refused = rebuild(
        &started.cluster,
        &[],
        &[],
        RebuildMode {
            is_applied: true,
            ..RebuildMode::default()
        },
    )
    .await;

    assert!(matches!(refused, Err(CommandError::Refused { .. })));
    assert!(stream::describe(&started.cluster, STREAM).await.is_err());
}

/// Cuts the link to one replica and returns its identifier.
fn cut_off_one_replica(started: &TestCluster) -> String {
    let node = &started.nodes.nodes()[1];
    node.set_reachable(false);
    node.replica_id().to_string()
}

#[tokio::test(start_paused = true)]
async fn replica_that_does_not_answer_is_a_problem_of_the_plan() {
    let started = cluster_that_lost_its_metadata_store().await;
    let cut_off_replica = cut_off_one_replica(&started);

    let report = rebuild(&started.cluster, &names(), &[], RebuildMode::default())
        .await
        .expect("the plan is made");

    let mut unanswered = Vec::new();
    for asked in &report.replicas {
        if !asked.has_answered {
            unanswered.push(asked.replica_id.clone());
        }
    }
    assert_eq!(unanswered, [cut_off_replica]);
    assert_eq!(report.problems.len(), 1, "{:?}", report.problems);
}

#[tokio::test(start_paused = true)]
async fn plan_with_problems_is_applied_only_when_forced() {
    let started = cluster_that_lost_its_metadata_store().await;
    cut_off_one_replica(&started);
    let refused = rebuild(
        &started.cluster,
        &names(),
        &[],
        RebuildMode {
            is_applied: true,
            ..RebuildMode::default()
        },
    )
    .await;

    let forced = rebuild(
        &started.cluster,
        &names(),
        &[],
        RebuildMode {
            is_applied: true,
            is_forced: true,
            ..RebuildMode::default()
        },
    )
    .await;

    assert!(matches!(refused, Err(CommandError::Refused { .. })));
    assert!(forced.is_ok_and(|report| report.is_applied));
}

#[tokio::test(start_paused = true)]
async fn replica_named_on_the_command_line_is_asked_although_it_is_not_registered() {
    let started = cluster_that_lost_its_metadata_store().await;
    let unregistered = started.nodes.nodes()[2].replica_id();
    let address = started.nodes.nodes()[2].address().to_owned();
    started
        .cluster
        .metadata
        .remove_replica(&unregistered)
        .await
        .expect("the replica is removed from the registry");

    let report = rebuild(
        &started.cluster,
        &names(),
        &[(unregistered.clone(), address)],
        RebuildMode::default(),
    )
    .await
    .expect("the plan is made");

    let mut asked_replicas = Vec::new();
    for asked in &report.replicas {
        asked_replicas.push(asked.replica_id.clone());
    }
    assert!(asked_replicas.contains(&unregistered.to_string()), "{asked_replicas:?}");
}

#[tokio::test(start_paused = true)]
async fn rebuild_with_no_replica_to_ask_does_nothing() {
    let started = TestCluster::start(3).await;
    started.lose_the_metadata_store().await;

    let refused = rebuild(&started.cluster, &names(), &[], RebuildMode::default()).await;

    assert!(matches!(refused, Err(CommandError::NothingToDo { .. })));
}

#[tokio::test(start_paused = true)]
async fn rebuild_without_an_object_store_says_what_it_cannot_find() {
    let mut started = cluster_that_lost_its_metadata_store().await;
    started.cluster.artifact_store = None;

    let report = rebuild(&started.cluster, &names(), &[], RebuildMode::default())
        .await
        .expect("the plan is made");

    assert_eq!(report.tiered_objects, 0);
    assert_eq!(report.problems.len(), 1, "{:?}", report.problems);
}

/// A cluster may have no object store at all. The tool cannot know: nothing configured looks the same as
/// a configuration that was forgotten, and a forgotten one would leave the segments in the object store out
/// of the rebuilt records. So the operator says it.
#[tokio::test(start_paused = true)]
async fn rebuild_of_a_cluster_said_to_have_no_object_store_has_no_problem() {
    let mut started = cluster_that_lost_its_metadata_store().await;
    started.cluster.artifact_store = None;
    let no_object_store = RebuildMode {
        has_no_object_store: true,
        ..RebuildMode::default()
    };

    let report = rebuild(&started.cluster, &names(), &[], no_object_store)
        .await
        .expect("the plan is made");

    assert_eq!(report.problems, Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn rebuild_of_a_cluster_said_to_have_no_object_store_is_applied_without_force() {
    let mut started = cluster_that_lost_its_metadata_store().await;
    started.cluster.artifact_store = None;
    let apply_without_an_object_store = RebuildMode {
        is_applied: true,
        has_no_object_store: true,
        ..RebuildMode::default()
    };

    let applied = rebuild(&started.cluster, &names(), &[], apply_without_an_object_store).await;

    assert!(applied.is_ok_and(|report| report.is_applied));
}

#[tokio::test(start_paused = true)]
async fn problem_of_a_missing_object_store_says_how_to_state_that_there_is_none() {
    let mut started = cluster_that_lost_its_metadata_store().await;
    started.cluster.artifact_store = None;

    let report = rebuild(&started.cluster, &names(), &[], RebuildMode::default())
        .await
        .expect("the plan is made");

    assert!(
        report.problems[0].contains("--no-object-store"),
        "{:?}",
        report.problems
    );
}

/// Saying that there is none while the configuration names one is a contradiction, not a preference: the
/// objects would be left out of the plan although they can be read.
#[tokio::test(start_paused = true)]
async fn saying_there_is_no_object_store_while_one_is_configured_is_refused() {
    let started = cluster_that_lost_its_metadata_store().await;
    let no_object_store = RebuildMode {
        has_no_object_store: true,
        ..RebuildMode::default()
    };

    let refused = rebuild(&started.cluster, &names(), &[], no_object_store).await;

    assert!(matches!(refused, Err(CommandError::Refused { .. })), "{refused:?}");
}
