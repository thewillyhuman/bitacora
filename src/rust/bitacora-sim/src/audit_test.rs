//! Tests of healing and of the audit's judgements.
//!
//! # What is covered
//!
//! - Healing makes both stores reachable, every link healthy and every node run, stops the clients, and says so in the
//!   history.
//! - Which segments need nothing more from the controller: open ones, truncated ones, released ones and fully
//!   replicated ones; and which still do.
//! - Truncation is recorded as a violation only when the low-water mark passed the store's checkpoint.
//!
//! # How the fixtures work
//!
//! The tests about segments build records by hand: `segment_with` describes one segment, and `stream_with` puts
//! segments into the stream record of a new world, which supplies the replication settings a real stream has. The tests
//! about healing break a small world of the test support by hand first.
//!
//! The audit as a whole (a store commits, every key is read, the stream is read back) needs a cluster with a controller
//! and is tested by every run of `tests/fault_matrix_test.rs`.

use std::sync::Arc;

use bitacora_artifact::ArtifactStore as _;
use bitacora_meta::{CopyRecord, SealRecord, TieredRecord};
use bitacora_types::{Epoch, ReplicaId, SegmentId, Timestamp};

use super::*;
use crate::history::Event;
use crate::test_support::{fault_descriptions, start_world};

/// Where the sealed segments of these tests end.
const SEALED_END: SequenceNumber = SequenceNumber::new(50);

/// What a test says about one segment.
struct SegmentShape {
    /// Whether the segment is sealed (at [`SEALED_END`]) or still open.
    is_sealed: bool,
    /// The state of each of its copies on replicas.
    copy_states: Vec<CopyState>,
    /// Whether it was uploaded, and if so whether its local copies were dropped since.
    tiering: Tiering,
}

/// Where a segment is with respect to the object store.
#[derive(Clone, Copy)]
enum Tiering {
    /// Not uploaded.
    NotUploaded,
    /// Uploaded; the replicas still hold their copies for hot reads.
    UploadedAndKeptLocally,
    /// Uploaded, and the local copies were released.
    UploadedAndReleased,
}

/// Builds the record of a segment of the given shape. Its copies are on replicas `replica-0`, `replica-1`, ...
fn segment_with(shape: &SegmentShape) -> SegmentRecord {
    let mut copies = Vec::new();
    for (copy_number, state) in shape.copy_states.iter().enumerate() {
        copies.push(CopyRecord {
            replica_id: ReplicaId::new(format!("replica-{copy_number}")).expect("a valid identifier"),
            state: *state,
        });
    }
    let uploaded_at = Timestamp::from_unix_millis(2_000);
    let tiered = match shape.tiering {
        Tiering::NotUploaded => None,
        Tiering::UploadedAndKeptLocally => Some(TieredRecord {
            object_key: "streams/1/segments/1".to_owned(),
            size_bytes: 4_096,
            uploaded_at,
            are_local_copies_dropped: false,
        }),
        Tiering::UploadedAndReleased => Some(TieredRecord {
            object_key: "streams/1/segments/1".to_owned(),
            size_bytes: 4_096,
            uploaded_at,
            are_local_copies_dropped: true,
        }),
    };
    let sealed = if shape.is_sealed {
        Some(SealRecord {
            end: SEALED_END,
            at: Timestamp::from_unix_millis(1_000),
        })
    } else {
        None
    };
    SegmentRecord {
        segment_id: SegmentId::FIRST,
        epoch: Epoch::new(1),
        first_sequence: SequenceNumber::FIRST,
        sealed,
        copies,
        tiered,
    }
}

/// Reads the stream record of a new world and gives it the low-water mark `low_water_mark`.
async fn stream_with_low_water_mark(low_water_mark: SequenceNumber) -> StreamRecord {
    let world = start_world(1).await;
    let mut stream = world
        .cluster
        .metadata()
        .get_stream(STREAM)
        .await
        .expect("a new world has its stream");
    stream.low_water_mark = low_water_mark;
    stream
}

/// Says whether a segment of the given shape needs nothing more, in a stream nothing was truncated from.
async fn belongs_where_it_is(shape: SegmentShape) -> bool {
    let stream = stream_with_low_water_mark(SequenceNumber::FIRST).await;
    is_where_it_belongs(&stream, &segment_with(&shape))
}

#[tokio::test(start_paused = true)]
async fn open_segment_needs_nothing_from_the_controller() {
    let needs_nothing = belongs_where_it_is(SegmentShape {
        is_sealed: false,
        copy_states: vec![CopyState::Unverified; 3],
        tiering: Tiering::NotUploaded,
    })
    .await;

    assert!(needs_nothing);
}

#[tokio::test(start_paused = true)]
async fn sealed_segment_with_every_copy_good_needs_nothing_more() {
    let needs_nothing = belongs_where_it_is(SegmentShape {
        is_sealed: true,
        copy_states: vec![CopyState::Good; 3],
        tiering: Tiering::NotUploaded,
    })
    .await;

    assert!(needs_nothing);
}

#[tokio::test(start_paused = true)]
async fn sealed_segment_with_a_bad_copy_still_needs_repair() {
    let needs_nothing = belongs_where_it_is(SegmentShape {
        is_sealed: true,
        copy_states: vec![CopyState::Good, CopyState::Bad, CopyState::Good],
        tiering: Tiering::NotUploaded,
    })
    .await;

    assert!(!needs_nothing);
}

#[tokio::test(start_paused = true)]
async fn sealed_segment_with_an_unverified_copy_still_needs_checking() {
    let needs_nothing = belongs_where_it_is(SegmentShape {
        is_sealed: true,
        copy_states: vec![CopyState::Good, CopyState::Good, CopyState::Unverified],
        tiering: Tiering::NotUploaded,
    })
    .await;

    assert!(!needs_nothing);
}

#[tokio::test(start_paused = true)]
async fn sealed_segment_short_of_a_copy_still_needs_one() {
    let needs_nothing = belongs_where_it_is(SegmentShape {
        is_sealed: true,
        copy_states: vec![CopyState::Good; 2],
        tiering: Tiering::NotUploaded,
    })
    .await;

    assert!(!needs_nothing);
}

#[tokio::test(start_paused = true)]
async fn segment_that_lives_in_the_object_store_alone_needs_nothing_more() {
    let needs_nothing = belongs_where_it_is(SegmentShape {
        is_sealed: true,
        copy_states: Vec::new(),
        tiering: Tiering::UploadedAndReleased,
    })
    .await;

    assert!(needs_nothing);
}

/// During its hot-read window an uploaded segment keeps its copies, and they must all be good like any other's.
#[tokio::test(start_paused = true)]
async fn uploaded_segment_that_keeps_a_bad_local_copy_still_needs_repair() {
    let needs_nothing = belongs_where_it_is(SegmentShape {
        is_sealed: true,
        copy_states: vec![CopyState::Good, CopyState::Good, CopyState::Bad],
        tiering: Tiering::UploadedAndKeptLocally,
    })
    .await;

    assert!(!needs_nothing);
}

/// A segment whose copies were released while the record still lists one has not finished being released.
#[tokio::test(start_paused = true)]
async fn released_segment_that_still_lists_a_copy_is_not_done() {
    let needs_nothing = belongs_where_it_is(SegmentShape {
        is_sealed: true,
        copy_states: vec![CopyState::Good],
        tiering: Tiering::UploadedAndReleased,
    })
    .await;

    assert!(!needs_nothing);
}

#[tokio::test(start_paused = true)]
async fn sealed_segment_without_copies_that_was_never_uploaded_is_not_done() {
    let needs_nothing = belongs_where_it_is(SegmentShape {
        is_sealed: true,
        copy_states: Vec::new(),
        tiering: Tiering::NotUploaded,
    })
    .await;

    assert!(!needs_nothing);
}

#[tokio::test(start_paused = true)]
async fn truncated_segment_needs_nothing_whatever_its_copies() {
    let stream = stream_with_low_water_mark(SEALED_END.next()).await;
    let truncated = segment_with(&SegmentShape {
        is_sealed: true,
        copy_states: vec![CopyState::Bad],
        tiering: Tiering::NotUploaded,
    });

    assert!(is_where_it_belongs(&stream, &truncated));
}

/// The record at the low-water mark is still readable, so the segment that ends there is not truncated.
#[tokio::test(start_paused = true)]
async fn segment_that_ends_at_the_low_water_mark_is_not_truncated() {
    let stream = stream_with_low_water_mark(SEALED_END).await;
    let still_readable = segment_with(&SegmentShape {
        is_sealed: true,
        copy_states: vec![CopyState::Bad],
        tiering: Tiering::NotUploaded,
    });

    assert!(!is_where_it_belongs(&stream, &still_readable));
}

#[tokio::test(start_paused = true)]
async fn stream_is_settled_only_when_every_segment_is() {
    let mut stream = stream_with_low_water_mark(SequenceNumber::FIRST).await;
    let whole = segment_with(&SegmentShape {
        is_sealed: true,
        copy_states: vec![CopyState::Good; 3],
        tiering: Tiering::NotUploaded,
    });
    let short_of_a_copy = segment_with(&SegmentShape {
        is_sealed: true,
        copy_states: vec![CopyState::Good; 2],
        tiering: Tiering::NotUploaded,
    });

    stream.segments = vec![whole.clone(), whole.clone()];
    let is_settled_when_all_are_whole = is_every_segment_where_it_belongs(&stream);
    stream.segments = vec![whole, short_of_a_copy];
    let is_settled_with_one_short = is_every_segment_where_it_belongs(&stream);

    assert!(is_settled_when_all_are_whole);
    assert!(!is_settled_with_one_short);
}

#[tokio::test(start_paused = true)]
async fn stream_without_segments_is_settled() {
    let stream = stream_with_low_water_mark(SequenceNumber::FIRST).await;

    assert!(is_every_segment_where_it_belongs(&stream));
}

#[tokio::test(start_paused = true)]
async fn healing_makes_both_stores_reachable() {
    let world = start_world(1).await;
    world.cluster.key_value_store().set_unavailable(true);
    world.cluster.artifact_store().set_unavailable(true);

    heal(&world, &mut JoinSet::new()).await;

    let registry = world.cluster.metadata().list_replicas().await;
    let objects = world.cluster.artifact_store().list_prefix("").await;
    assert!(registry.is_ok(), "{registry:?}");
    assert!(objects.is_ok(), "{objects:?}");
}

#[tokio::test(start_paused = true)]
async fn healing_makes_every_link_healthy() {
    let world = start_world(1).await;
    for node in world.cluster.nodes() {
        node.set_reachable(false);
    }

    heal(&world, &mut JoinSet::new()).await;

    let mut faults = Vec::new();
    for node in world.cluster.nodes() {
        faults.push(node.link().fault());
    }
    assert_eq!(faults, [LinkFault::default(); 3]);
}

#[tokio::test(start_paused = true)]
async fn healing_restarts_every_node_that_was_down() {
    let world = start_world(1).await;
    world.cluster.nodes()[0].crash();
    world.cluster.nodes()[2].lose_disk();

    heal(&world, &mut JoinSet::new()).await;

    let mut is_running_by_node = Vec::new();
    for node in world.cluster.nodes() {
        is_running_by_node.push(node.is_running());
    }
    assert_eq!(is_running_by_node, [true; 3]);
}

/// A restart is what makes a disk accept flushes again, so healing restarts the nodes that run, too.
#[tokio::test(start_paused = true)]
async fn healing_gives_a_running_node_a_new_process() {
    let world = start_world(1).await;
    let process_before = world.cluster.nodes()[1].replica().expect("the node is running");

    heal(&world, &mut JoinSet::new()).await;

    let process_after = world.cluster.nodes()[1].replica().expect("the node is running");
    assert!(!Arc::ptr_eq(&process_before, &process_after));
}

#[tokio::test(start_paused = true)]
async fn healing_stops_the_clients() {
    let world = start_world(1).await;
    let mut clients = JoinSet::new();
    clients.spawn(std::future::pending::<()>());

    heal(&world, &mut clients).await;

    assert_eq!(clients.len(), 0);
}

#[tokio::test(start_paused = true)]
async fn healing_is_recorded_in_the_history() {
    let world = start_world(1).await;

    heal(&world, &mut JoinSet::new()).await;

    assert_eq!(
        fault_descriptions(&world.history),
        ["healed: every fault removed, every node restarted"]
    );
}

#[tokio::test(start_paused = true)]
async fn truncation_behind_the_checkpoint_is_not_recorded() {
    let world = start_world(1).await;
    let events_before = world.history.events().len();

    let checked = check_truncation(&world, SequenceNumber::FIRST).await;

    assert_eq!(checked, Ok(()));
    assert_eq!(world.history.events().len(), events_before);
}

/// The store of a new world has applied nothing, so its checkpoint is zero: a log that starts at 5 has lost records 1
/// to 4, which the store never saw.
#[tokio::test(start_paused = true)]
async fn truncation_past_the_checkpoint_is_recorded() {
    let world = start_world(1).await;

    let checked = check_truncation(&world, SequenceNumber::new(5)).await;

    let last_event = world.history.events().pop().map(|timed| timed.event);
    assert_eq!(checked, Ok(()));
    assert_eq!(
        last_event,
        Some(Event::TruncatedPastCheckpoint {
            checkpoint: SequenceNumber::ZERO,
            low_water_mark: SequenceNumber::new(5),
        })
    );
}

#[tokio::test(start_paused = true)]
async fn truncation_cannot_be_checked_while_the_metadata_store_is_down() {
    let world = start_world(1).await;
    world.cluster.key_value_store().set_unavailable(true);

    let checked = check_truncation(&world, SequenceNumber::FIRST).await;

    assert!(checked.is_err(), "{checked:?}");
}
