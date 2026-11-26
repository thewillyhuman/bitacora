//! Tests of snapshots, restoring one, and adopting a rebuilt stream.
//!
//! # What is covered
//!
//! - Snapshots: every key in key order, with the stored bytes and the time given; an empty store; an unavailable store.
//! - Restoring into an empty store: every key comes back, the store serves what it held, and the outcome counts the
//!   keys. A store that holds keys is refused and left alone, and so is a store that somebody else writes to meanwhile.
//! - Restoring over a used store: exactly the snapshot is left, and the outcome counts the deleted keys.
//! - Batches: a snapshot larger than one transaction comes back whole, and only a restore into an empty store puts
//!   conditions on its batches.
//! - Adoption: the record is stored as given, the identifier counter only rises, the same stream is replaced, and every
//!   contradiction with the store is refused.
//!
//! # How the fixtures work
//!
//! `snapshot_of_a_used_store` is a snapshot of three keys: the stream `orders` with a writer and an open segment, the
//! checkpoint of its materializer `pages`, and the stream identifier counter. `rebuilt_stream` is a stream record as
//! `bita metadata rebuild` would put it together: made by a scratch store, then given the identifier and the epoch the
//! test wants. The stores come from `test_support.rs`.

use bitacora_types::{Epoch, StreamId};
use bytes::Bytes;

use super::*;
use crate::key_value::KeyValueStore;
use crate::store::test_support::{
    LEADERSHIP_EXPIRY, NOW, STREAM, create_stream, empty_store, empty_store_with_backend, replica,
    store_that_loses_every_conditional_write, store_with_open_segment, store_with_stream,
};

/// The epoch a rebuilt stream record carries: far from the zero of a new stream, so that a test can tell the two apart.
const REBUILT_EPOCH: Epoch = Epoch::new(9);

/// Returns a snapshot of a store with a stream, an open segment and a materializer: three keys.
async fn snapshot_of_a_used_store() -> MetadataSnapshot {
    let store = store_with_open_segment().await;
    store
        .register_materializer(STREAM, "pages")
        .await
        .expect("the materializer is registered");
    store.snapshot(NOW).await.expect("the store is available")
}

/// Returns the record of a stream called `name` with the identifier `stream_id`, as a rebuild would put it together.
async fn rebuilt_stream(name: &str, stream_id: u64) -> StreamRecord {
    let scratch_store = empty_store();
    let mut record = create_stream(&scratch_store, name).await;
    record.stream_id = StreamId::new(stream_id);
    record.epoch = REBUILT_EPOCH;
    record
}

/// The entries the store holds now.
async fn stored_entries(store: &MetadataStore) -> Vec<SnapshotEntry> {
    let snapshot = store.snapshot(NOW).await.expect("the store is available");
    snapshot.entries
}

#[tokio::test]
async fn snapshot_of_an_empty_store_has_no_entries() {
    let store = empty_store();

    let snapshot = store.snapshot(NOW).await.expect("the store is available");

    assert_eq!(snapshot.entries, Vec::new());
}

#[tokio::test]
async fn snapshot_records_when_it_was_taken() {
    let store = empty_store();

    let snapshot = store.snapshot(NOW).await.expect("the store is available");

    assert_eq!(snapshot.taken_at, NOW);
}

#[tokio::test]
async fn snapshot_holds_every_key_of_the_store_in_key_order() {
    let snapshot = snapshot_of_a_used_store().await;

    let mut keys = Vec::new();
    for entry in &snapshot.entries {
        keys.push(entry.key.as_str());
    }

    assert_eq!(keys, ["checkpoints/1/pages", "counters/stream-id", "streams/orders"]);
}

#[tokio::test]
async fn snapshot_holds_the_stored_bytes_of_each_key() {
    let fixture = empty_store_with_backend();
    fixture
        .store
        .try_lead("controller-1", NOW, LEADERSHIP_EXPIRY)
        .await
        .expect("the store is available");
    let stored_leader = fixture
        .key_value_store
        .get("controller/leader")
        .await
        .expect("the store is available")
        .expect("the leader record exists");

    let snapshot = fixture.store.snapshot(NOW).await.expect("the store is available");

    assert_eq!(snapshot.entries[0].value, stored_leader.value);
}

#[tokio::test]
async fn snapshot_fails_while_the_store_is_unavailable() {
    let fixture = empty_store_with_backend();
    fixture.key_value_store.set_unavailable(true);

    let snapshot = fixture.store.snapshot(NOW).await;

    assert!(matches!(snapshot, Err(MetaError::Unavailable(_))), "{snapshot:?}");
}

#[tokio::test]
async fn restore_into_an_empty_store_brings_back_every_key() {
    let snapshot = snapshot_of_a_used_store().await;
    let restored_store = empty_store();

    restored_store
        .restore(&snapshot, RestoreMode::IntoEmptyStore)
        .await
        .expect("the snapshot is restored");

    assert_eq!(stored_entries(&restored_store).await, snapshot.entries);
}

#[tokio::test]
async fn restored_store_serves_the_streams_it_held() {
    let snapshot = snapshot_of_a_used_store().await;
    let restored_store = empty_store();

    restored_store
        .restore(&snapshot, RestoreMode::IntoEmptyStore)
        .await
        .expect("the snapshot is restored");

    let stream = restored_store.get_stream(STREAM).await.expect("the stream exists");
    assert_eq!(stream.epoch, Epoch::new(1));
}

#[tokio::test]
async fn restore_reports_how_many_keys_it_wrote() {
    let snapshot = snapshot_of_a_used_store().await;
    let restored_store = empty_store();

    let outcome = restored_store.restore(&snapshot, RestoreMode::IntoEmptyStore).await;

    assert_eq!(
        outcome,
        Ok(RestoreOutcome {
            keys_written: 3,
            keys_deleted: 0
        })
    );
}

#[tokio::test]
async fn restore_into_a_store_that_holds_keys_is_refused() {
    let snapshot = snapshot_of_a_used_store().await;
    let used_store = store_with_stream().await;

    let refused = used_store.restore(&snapshot, RestoreMode::IntoEmptyStore).await;

    assert!(matches!(refused, Err(MetaError::InvalidState { .. })), "{refused:?}");
}

#[tokio::test]
async fn refused_restore_changes_nothing() {
    let snapshot = snapshot_of_a_used_store().await;
    let used_store = store_with_stream().await;
    let entries_before = stored_entries(&used_store).await;

    let _refused = used_store.restore(&snapshot, RestoreMode::IntoEmptyStore).await;

    assert_eq!(stored_entries(&used_store).await, entries_before);
}

/// A restore into an empty store writes a key only if it is still absent. A backend that fails that condition stands
/// for somebody else who wrote the key first.
#[tokio::test]
async fn restore_stops_when_somebody_else_writes_to_the_store() {
    let snapshot = snapshot_of_a_used_store().await;
    let fixture = empty_store_with_backend();
    let contended_store = store_that_loses_every_conditional_write(&fixture.key_value_store);

    let stopped = contended_store.restore(&snapshot, RestoreMode::IntoEmptyStore).await;

    assert_eq!(
        stopped,
        Err(MetaError::InvalidState {
            reason: "somebody else wrote to the metadata store while it was restored".to_owned(),
        })
    );
}

#[tokio::test]
async fn restore_that_replaces_everything_leaves_exactly_the_snapshot() {
    let snapshot = snapshot_of_a_used_store().await;
    let used_store = empty_store();
    for name in ["audit", "payments"] {
        create_stream(&used_store, name).await;
    }

    used_store
        .restore(&snapshot, RestoreMode::ReplaceEverything)
        .await
        .expect("the snapshot is restored");

    assert_eq!(stored_entries(&used_store).await, snapshot.entries);
}

/// The stream identifier counter is in the store and in the snapshot, so it is overwritten; only the other stream's
/// record is deleted.
#[tokio::test]
async fn restore_that_replaces_everything_reports_the_keys_it_deleted() {
    let snapshot = snapshot_of_a_used_store().await;
    let used_store = empty_store();
    create_stream(&used_store, "audit").await;

    let outcome = used_store.restore(&snapshot, RestoreMode::ReplaceEverything).await;

    assert_eq!(
        outcome,
        Ok(RestoreOutcome {
            keys_written: 3,
            keys_deleted: 1
        })
    );
}

/// A backend limits how much one transaction may change, so a restore is split. The split must not lose or reorder
/// anything.
#[tokio::test]
async fn restore_of_more_keys_than_one_transaction_takes_brings_back_every_key() {
    let source_store = empty_store();
    for number in 0..(RESTORE_BATCH_KEYS * 3 + 5) {
        source_store
            .put_replica(&replica(&format!("replica-{number}"), 1_000))
            .await
            .expect("the replica is registered");
    }
    let snapshot = source_store.snapshot(NOW).await.expect("the store is available");
    let restored_store = empty_store();

    restored_store
        .restore(&snapshot, RestoreMode::IntoEmptyStore)
        .await
        .expect("the snapshot is restored");

    assert_eq!(stored_entries(&restored_store).await, snapshot.entries);
}

#[test]
fn batch_of_a_restore_into_an_empty_store_requires_its_written_keys_to_be_absent() {
    let batch = [
        Mutation::Put {
            key: "streams/orders".to_owned(),
            value: Bytes::from_static(b"{}"),
        },
        Mutation::Delete {
            key: "streams/audit".to_owned(),
        },
    ];

    let conditions = conditions_of_a_restore_batch(RestoreMode::IntoEmptyStore, &batch);

    assert_eq!(
        conditions,
        [Condition::IsAbsent {
            key: "streams/orders".to_owned()
        }]
    );
}

#[test]
fn batch_of_a_restore_that_replaces_everything_has_no_conditions() {
    let batch = [Mutation::Put {
        key: "streams/orders".to_owned(),
        value: Bytes::from_static(b"{}"),
    }];

    let conditions = conditions_of_a_restore_batch(RestoreMode::ReplaceEverything, &batch);

    assert!(conditions.is_empty(), "{conditions:?}");
}

#[tokio::test]
async fn adopted_stream_is_read_back_as_it_was_given() {
    let store = empty_store();
    let record = rebuilt_stream(STREAM, 7).await;

    store.adopt_stream(&record).await.expect("the stream is adopted");

    assert_eq!(store.get_stream(STREAM).await, Ok(record));
}

#[tokio::test]
async fn stream_created_after_an_adoption_gets_a_higher_identifier() {
    let store = empty_store();
    store
        .adopt_stream(&rebuilt_stream(STREAM, 7).await)
        .await
        .expect("the stream is adopted");

    let created = create_stream(&store, "audit").await;

    assert_eq!(created.stream_id, StreamId::new(8));
}

#[tokio::test]
async fn adoption_does_not_lower_the_identifier_counter() {
    let store = empty_store();
    store
        .adopt_stream(&rebuilt_stream(STREAM, 7).await)
        .await
        .expect("the stream is adopted");
    store
        .adopt_stream(&rebuilt_stream("audit", 3).await)
        .await
        .expect("the stream is adopted");

    let created = create_stream(&store, "payments").await;

    assert_eq!(created.stream_id, StreamId::new(8));
}

#[tokio::test]
async fn adoption_replaces_the_record_of_the_same_stream() {
    let store = store_with_stream().await;
    let rebuilt_record = rebuilt_stream(STREAM, 1).await;

    store
        .adopt_stream(&rebuilt_record)
        .await
        .expect("the stream is adopted");

    let stream = store.get_stream(STREAM).await.expect("the stream exists");
    assert_eq!(stream.epoch, REBUILT_EPOCH);
}

#[tokio::test]
async fn adoption_under_a_name_that_belongs_to_another_stream_is_refused() {
    let store = store_with_stream().await;

    let refused = store.adopt_stream(&rebuilt_stream(STREAM, 7).await).await;

    assert!(matches!(refused, Err(MetaError::InvalidState { .. })), "{refused:?}");
}

#[tokio::test]
async fn adoption_of_a_stream_known_under_another_name_is_refused() {
    let store = store_with_stream().await;

    let refused = store.adopt_stream(&rebuilt_stream("audit", 1).await).await;

    assert!(matches!(refused, Err(MetaError::InvalidState { .. })), "{refused:?}");
}

#[tokio::test]
async fn adoption_of_a_stream_recorded_as_deleted_is_refused() {
    let store = store_with_stream().await;
    store.delete_stream(STREAM).await.expect("the stream is deleted");

    let refused = store.adopt_stream(&rebuilt_stream(STREAM, 1).await).await;

    assert!(matches!(refused, Err(MetaError::InvalidState { .. })), "{refused:?}");
}

#[tokio::test]
async fn adoption_under_a_name_that_cannot_be_a_key_is_refused() {
    let store = empty_store();
    let mut record = rebuilt_stream(STREAM, 7).await;
    record.name = "not/a/name".to_owned();

    let refused = store.adopt_stream(&record).await;

    assert!(matches!(refused, Err(MetaError::InvalidName { .. })), "{refused:?}");
}

#[tokio::test]
async fn refused_adoption_changes_nothing() {
    let store = store_with_stream().await;
    let entries_before = stored_entries(&store).await;

    let _refused = store.adopt_stream(&rebuilt_stream(STREAM, 7).await).await;

    assert_eq!(stored_entries(&store).await, entries_before);
}
