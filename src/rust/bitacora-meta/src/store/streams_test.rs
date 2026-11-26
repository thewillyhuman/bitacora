//! Tests of creating, reading, updating and deleting streams.
//!
//! # What is covered
//!
//! - Creation: what a new stream looks like, that identifiers are distinct, and which creations are refused.
//! - Reads: a missing stream, a listing, and an unavailable store.
//! - `update_stream`: what it returns, that a change that changes nothing writes nothing, that an aborted change leaves
//!   the record alone, and that it gives up when it never wins.
//! - Deletion: the record and its checkpoints go together, a note is left until it is forgotten, and the name is free
//!   at once.
//!
//! # How the fixtures work
//!
//! See `test_support.rs`. `store_with_stream` gives a store with the stream `orders`; `empty_store_with_backend` also
//! hands out the in-memory backend, to read revisions and to simulate an outage;
//! `store_that_loses_every_conditional_write` is a store on which no read-modify-write wins.

use super::*;
use crate::key_value::{KeyValueStore, Revision};
use crate::keys::MAX_NAME_LENGTH_BYTES;
use crate::memory_store::MemoryKeyValueStore;
use crate::store::test_support::{
    STREAM, create_stream, empty_store, empty_store_with_backend, store_that_loses_every_conditional_write,
    store_with_stream, stream_id_of,
};

/// The revision of the stored record of [`STREAM`].
async fn stream_record_revision(key_value_store: &MemoryKeyValueStore) -> Revision {
    let stored = key_value_store
        .get("streams/orders")
        .await
        .expect("the store is available")
        .expect("the stream record exists");
    stored.revision
}

#[tokio::test]
async fn new_stream_has_no_segments_and_no_writer() {
    let store = store_with_stream().await;

    let record = store.get_stream(STREAM).await.expect("the stream exists");

    assert_eq!(record.epoch, Epoch::ZERO);
    assert!(record.segments.is_empty(), "{record:?}");
    assert_eq!(record.writer, None);
    assert_eq!(record.low_water_mark, SequenceNumber::FIRST);
}

#[tokio::test]
async fn first_stream_of_a_store_gets_the_first_identifier() {
    let store = empty_store();

    let created = create_stream(&store, STREAM).await;

    assert_eq!(created.stream_id, StreamId::new(1));
}

#[tokio::test]
async fn streams_get_distinct_identifiers() {
    let store = store_with_stream().await;

    let second = create_stream(&store, "payments").await;

    assert_ne!(stream_id_of(&store).await, second.stream_id);
}

#[tokio::test]
async fn creating_a_stream_twice_fails() {
    let store = store_with_stream().await;

    let second_attempt = store
        .create_stream(STREAM, ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
        .await;

    assert_eq!(
        second_attempt,
        Err(MetaError::StreamAlreadyExists {
            name: STREAM.to_owned()
        })
    );
}

#[tokio::test]
async fn names_that_would_break_keys_are_refused() {
    let store = empty_store();
    let too_long_name = "n".repeat(MAX_NAME_LENGTH_BYTES + 1);

    for name in ["", "a/b", "with space", &too_long_name] {
        let created = store
            .create_stream(name, ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
            .await;

        assert!(
            matches!(created, Err(MetaError::InvalidName { .. })),
            "name {name:?} was accepted"
        );
    }
}

#[tokio::test]
async fn reading_a_missing_stream_fails() {
    let store = empty_store();

    let read = store.get_stream(STREAM).await;

    assert_eq!(
        read,
        Err(MetaError::StreamNotFound {
            name: STREAM.to_owned()
        })
    );
}

#[tokio::test]
async fn reading_a_stream_fails_while_the_store_is_unavailable() {
    let fixture = empty_store_with_backend();
    fixture.key_value_store.set_unavailable(true);

    let read = fixture.store.get_stream(STREAM).await;

    assert!(matches!(read, Err(MetaError::Unavailable(_))), "{read:?}");
}

#[tokio::test]
async fn streams_are_listed_sorted_by_name() {
    let store = store_with_stream().await;
    create_stream(&store, "billing").await;

    let listed = store.list_streams().await.expect("the store is available");

    let mut names = Vec::new();
    for record in listed {
        names.push(record.name);
    }
    assert_eq!(names, ["billing", "orders"]);
}

#[tokio::test]
async fn deleted_stream_can_no_longer_be_read() {
    let store = store_with_stream().await;

    store.delete_stream(STREAM).await.expect("the stream is deleted");

    assert_eq!(
        store.get_stream(STREAM).await,
        Err(MetaError::StreamNotFound {
            name: STREAM.to_owned()
        })
    );
}

#[tokio::test]
async fn deleted_stream_takes_its_checkpoints_with_it() {
    let store = store_with_stream().await;
    let stream_id = stream_id_of(&store).await;
    store
        .register_materializer(STREAM, "pages")
        .await
        .expect("the materializer is registered");

    store.delete_stream(STREAM).await.expect("the stream is deleted");

    assert_eq!(store.list_checkpoints(stream_id).await, Ok(Vec::new()));
}

#[tokio::test]
async fn deleted_stream_leaves_a_note_for_the_controller() {
    let store = store_with_stream().await;
    let stream_id = stream_id_of(&store).await;

    store.delete_stream(STREAM).await.expect("the stream is deleted");

    assert_eq!(
        store.list_deleted_streams().await,
        Ok(vec![DeletedStreamRecord {
            stream_id,
            name: STREAM.to_owned(),
        }])
    );
}

#[tokio::test]
async fn forgotten_deleted_stream_is_no_longer_noted() {
    let store = store_with_stream().await;
    let stream_id = stream_id_of(&store).await;
    store.delete_stream(STREAM).await.expect("the stream is deleted");

    store
        .forget_deleted_stream(stream_id)
        .await
        .expect("the note is dropped");

    assert_eq!(store.list_deleted_streams().await, Ok(Vec::new()));
}

#[tokio::test]
async fn forgetting_a_stream_that_was_never_deleted_succeeds() {
    let store = empty_store();

    let forgotten = store.forget_deleted_stream(StreamId::new(7)).await;

    assert_eq!(forgotten, Ok(()));
}

#[tokio::test]
async fn deleting_a_missing_stream_fails() {
    let store = empty_store();

    let deleted = store.delete_stream(STREAM).await;

    assert_eq!(
        deleted,
        Err(MetaError::StreamNotFound {
            name: STREAM.to_owned()
        })
    );
}

#[tokio::test]
async fn stream_name_can_be_reused_while_the_old_stream_is_cleaned_up() {
    let store = store_with_stream().await;
    let old_stream_id = stream_id_of(&store).await;
    store.delete_stream(STREAM).await.expect("the stream is deleted");

    let recreated = create_stream(&store, STREAM).await;

    assert_ne!(recreated.stream_id, old_stream_id);
}

#[tokio::test]
async fn update_returns_the_new_record_and_what_the_change_returned() {
    let store = store_with_stream().await;
    let requested_position = SequenceNumber::new(500);

    let updated = store
        .update_stream(STREAM, |record| {
            record.truncate_before = Some(requested_position);
            Ok("the output of the change")
        })
        .await;

    let (record, output) = updated.expect("the update is written");
    assert_eq!(record.truncate_before, Some(requested_position));
    assert_eq!(output, "the output of the change");
}

#[tokio::test]
async fn update_is_stored() {
    let store = store_with_stream().await;
    let requested_position = SequenceNumber::new(500);

    store
        .update_stream(STREAM, |record| {
            record.truncate_before = Some(requested_position);
            Ok(())
        })
        .await
        .expect("the update is written");

    let stored = store.get_stream(STREAM).await.expect("the stream exists");
    assert_eq!(stored.truncate_before, Some(requested_position));
}

#[tokio::test]
async fn update_that_changes_nothing_writes_nothing() {
    let fixture = empty_store_with_backend();
    create_stream(&fixture.store, STREAM).await;
    let revision_before = stream_record_revision(&fixture.key_value_store).await;

    fixture
        .store
        .update_stream(STREAM, |_record| Ok(()))
        .await
        .expect("the update succeeds");

    assert_eq!(stream_record_revision(&fixture.key_value_store).await, revision_before);
}

#[tokio::test]
async fn update_aborted_by_its_change_writes_nothing() {
    let fixture = empty_store_with_backend();
    create_stream(&fixture.store, STREAM).await;
    let revision_before = stream_record_revision(&fixture.key_value_store).await;

    let aborted = fixture
        .store
        .update_stream(STREAM, |record| {
            record.truncate_before = Some(SequenceNumber::new(500));
            Err::<(), MetaError>(MetaError::InvalidState {
                reason: "the change gives up".to_owned(),
            })
        })
        .await;

    assert!(matches!(aborted, Err(MetaError::InvalidState { .. })), "{aborted:?}");
    assert_eq!(stream_record_revision(&fixture.key_value_store).await, revision_before);
}

#[tokio::test]
async fn update_of_a_missing_stream_fails() {
    let store = empty_store();

    let updated = store.update_stream(STREAM, |_record| Ok(())).await;

    assert_eq!(
        updated.err(),
        Some(MetaError::StreamNotFound {
            name: STREAM.to_owned()
        })
    );
}

#[tokio::test]
async fn update_gives_up_after_too_many_conflicts() {
    let fixture = empty_store_with_backend();
    create_stream(&fixture.store, STREAM).await;
    let contended_store = store_that_loses_every_conditional_write(&fixture.key_value_store);

    let updated = contended_store
        .update_stream(STREAM, |record| {
            record.truncate_before = Some(SequenceNumber::new(500));
            Ok(())
        })
        .await;

    assert_eq!(
        updated.err(),
        Some(MetaError::TooMuchContention {
            key: "streams/orders".to_owned(),
            attempts: 32,
        })
    );
}

/// The change must follow from the record it is given, because it is asked again after every conflict.
#[tokio::test]
async fn update_runs_its_change_once_per_attempt() {
    let fixture = empty_store_with_backend();
    create_stream(&fixture.store, STREAM).await;
    let contended_store = store_that_loses_every_conditional_write(&fixture.key_value_store);
    let mut times_the_change_ran = 0;

    let _gave_up = contended_store
        .update_stream(STREAM, |record| {
            times_the_change_ran += 1;
            record.truncate_before = Some(SequenceNumber::new(500));
            Ok(())
        })
        .await;

    assert_eq!(times_the_change_ran, 32);
}

#[tokio::test]
async fn creation_gives_up_after_too_many_conflicts() {
    let fixture = empty_store_with_backend();
    let contended_store = store_that_loses_every_conditional_write(&fixture.key_value_store);

    let created = contended_store
        .create_stream(STREAM, ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
        .await;

    assert_eq!(
        created,
        Err(MetaError::TooMuchContention {
            key: "streams/orders".to_owned(),
            attempts: 32,
        })
    );
}

#[tokio::test]
async fn update_fails_while_the_store_is_unavailable() {
    let fixture = empty_store_with_backend();
    create_stream(&fixture.store, STREAM).await;
    fixture.key_value_store.set_unavailable(true);

    let updated = fixture.store.update_stream(STREAM, |_record| Ok(())).await;

    assert!(matches!(updated, Err(MetaError::Unavailable(_))));
}
