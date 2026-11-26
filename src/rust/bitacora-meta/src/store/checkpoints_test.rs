//! Tests of materializers and their checkpoints.
//!
//! # What is covered
//!
//! - Registering: where a new checkpoint starts, that it is recorded on the stream, that registering twice keeps what
//!   is there, and which registrations are refused.
//! - Unregistering: the checkpoint and the name go, and an unknown materializer is not an error.
//! - Fencing: each takeover raises the token, only the holder of the current one commits, and a takeover that never
//!   wins gives up.
//! - Committing: what is read back, that a commit can be repeated, and that a checkpoint never moves backwards.
//! - Listing: the checkpoints of one stream, sorted, and none for a stream without materializers.
//!
//! # How the fixtures work
//!
//! `registered_materializer` gives a store with the stream `orders` (see `test_support.rs`) on which the materializer
//! `pages` is registered, together with the stream's identifier. `acquire_token` takes the materializer over the way a
//! runner does, and `commit_at` commits without a manifest.

use super::*;
use crate::store::test_support::{
    STREAM, create_stream, empty_store, empty_store_with_backend, store_that_loses_every_conditional_write,
    store_with_stream, stream_id_of,
};

/// The materializer most tests register.
const MATERIALIZER: &str = "pages";

/// A store on which [`MATERIALIZER`] is registered on the stream [`STREAM`].
struct RegisteredMaterializer {
    /// The store under test.
    store: MetadataStore,
    /// The identifier of [`STREAM`], which the checkpoint operations take.
    stream_id: StreamId,
}

/// Returns a store on which [`MATERIALIZER`] is registered on [`STREAM`], and no runner has taken it yet.
async fn registered_materializer() -> RegisteredMaterializer {
    let store = store_with_stream().await;
    let stream_id = stream_id_of(&store).await;
    store
        .register_materializer(STREAM, MATERIALIZER)
        .await
        .expect("the materializer is registered");
    RegisteredMaterializer { store, stream_id }
}

/// Takes the materializer over, as a runner does when it starts, and returns the token it now holds.
async fn acquire_token(fixture: &RegisteredMaterializer) -> u64 {
    let acquired = fixture
        .store
        .acquire_checkpoint(fixture.stream_id, MATERIALIZER)
        .await
        .expect("the checkpoint is acquired");
    acquired.fencing_token
}

/// Commits the checkpoint `checkpoint` with `fencing_token` and no manifest.
async fn commit_at(fixture: &RegisteredMaterializer, fencing_token: u64, checkpoint: u64) -> Result<(), MetaError> {
    fixture
        .store
        .commit_checkpoint(
            fixture.stream_id,
            MATERIALIZER,
            fencing_token,
            SequenceNumber::new(checkpoint),
            None,
        )
        .await
}

/// The checkpoint of [`MATERIALIZER`] as it is stored now.
async fn stored_checkpoint(fixture: &RegisteredMaterializer) -> CheckpointRecord {
    fixture
        .store
        .get_checkpoint(fixture.stream_id, MATERIALIZER)
        .await
        .expect("the materializer is registered")
}

#[tokio::test]
async fn registered_materializer_starts_below_the_low_water_mark() {
    let store = store_with_stream().await;
    store
        .update_stream(STREAM, |record| {
            record.low_water_mark = SequenceNumber::new(500);
            Ok(())
        })
        .await
        .expect("the mark is set");

    let checkpoint = store
        .register_materializer(STREAM, MATERIALIZER)
        .await
        .expect("the materializer is registered");

    assert_eq!(checkpoint.checkpoint, SequenceNumber::new(499));
}

#[tokio::test]
async fn registered_materializer_has_no_runner_yet() {
    let store = store_with_stream().await;

    let checkpoint = store
        .register_materializer(STREAM, MATERIALIZER)
        .await
        .expect("the materializer is registered");

    assert_eq!(checkpoint.fencing_token, 0);
    assert_eq!(checkpoint.manifest, None);
}

#[tokio::test]
async fn registering_twice_keeps_the_existing_checkpoint() {
    let fixture = registered_materializer().await;
    let token = acquire_token(&fixture).await;
    commit_at(&fixture, token, 40)
        .await
        .expect("the checkpoint is committed");

    let registered_again = fixture
        .store
        .register_materializer(STREAM, MATERIALIZER)
        .await
        .expect("registering again is not an error");

    assert_eq!(registered_again.checkpoint, SequenceNumber::new(40));
}

#[tokio::test]
async fn registration_is_recorded_on_the_stream() {
    let fixture = registered_materializer().await;

    let record = fixture.store.get_stream(STREAM).await.expect("the stream exists");

    assert!(record.materializers.contains(MATERIALIZER), "{record:?}");
}

#[tokio::test]
async fn materializer_names_that_would_break_keys_are_refused() {
    let store = store_with_stream().await;

    let registered = store.register_materializer(STREAM, "pages/by-date").await;

    assert_eq!(
        registered,
        Err(MetaError::InvalidName {
            name: "pages/by-date".to_owned()
        })
    );
}

#[tokio::test]
async fn registering_on_a_missing_stream_fails() {
    let store = empty_store();

    let registered = store.register_materializer(STREAM, MATERIALIZER).await;

    assert_eq!(
        registered,
        Err(MetaError::StreamNotFound {
            name: STREAM.to_owned()
        })
    );
}

#[tokio::test]
async fn unregistered_materializer_no_longer_has_a_checkpoint() {
    let fixture = registered_materializer().await;

    fixture
        .store
        .unregister_materializer(STREAM, MATERIALIZER)
        .await
        .expect("the materializer is unregistered");

    assert_eq!(
        fixture.store.get_checkpoint(fixture.stream_id, MATERIALIZER).await,
        Err(MetaError::MaterializerNotRegistered {
            stream_id: fixture.stream_id,
            materializer: MATERIALIZER.to_owned(),
        })
    );
}

#[tokio::test]
async fn unregistered_materializer_is_no_longer_recorded_on_the_stream() {
    let fixture = registered_materializer().await;

    fixture
        .store
        .unregister_materializer(STREAM, MATERIALIZER)
        .await
        .expect("the materializer is unregistered");

    let record = fixture.store.get_stream(STREAM).await.expect("the stream exists");
    assert!(record.materializers.is_empty(), "{record:?}");
}

#[tokio::test]
async fn unregistering_an_unknown_materializer_succeeds() {
    let store = store_with_stream().await;

    let unregistered = store.unregister_materializer(STREAM, MATERIALIZER).await;

    assert_eq!(unregistered, Ok(()));
}

#[tokio::test]
async fn each_acquisition_raises_the_fencing_token() {
    let fixture = registered_materializer().await;

    let first_token = acquire_token(&fixture).await;
    let second_token = acquire_token(&fixture).await;

    assert_eq!(first_token, 1);
    assert_eq!(second_token, 2);
}

#[tokio::test]
async fn acquiring_an_unregistered_materializer_fails() {
    let store = store_with_stream().await;
    let stream_id = stream_id_of(&store).await;

    let acquired = store.acquire_checkpoint(stream_id, MATERIALIZER).await;

    assert_eq!(
        acquired,
        Err(MetaError::MaterializerNotRegistered {
            stream_id,
            materializer: MATERIALIZER.to_owned(),
        })
    );
}

/// A checkpoint has a key of its own, so a takeover that never wins is reported under that key and not under the
/// stream's.
#[tokio::test]
async fn acquisition_gives_up_after_too_many_conflicts() {
    let fixture = empty_store_with_backend();
    create_stream(&fixture.store, STREAM).await;
    fixture
        .store
        .register_materializer(STREAM, MATERIALIZER)
        .await
        .expect("the materializer is registered");
    let stream_id = stream_id_of(&fixture.store).await;
    let contended_store = store_that_loses_every_conditional_write(&fixture.key_value_store);

    let acquired = contended_store.acquire_checkpoint(stream_id, MATERIALIZER).await;

    assert_eq!(
        acquired,
        Err(MetaError::TooMuchContention {
            key: "checkpoints/1/pages".to_owned(),
            attempts: 32,
        })
    );
}

#[tokio::test]
async fn superseded_runner_cannot_commit() {
    let fixture = registered_materializer().await;
    let old_token = acquire_token(&fixture).await;
    acquire_token(&fixture).await;

    let stale_commit = commit_at(&fixture, old_token, 40).await;

    assert_eq!(
        stale_commit,
        Err(MetaError::MaterializerFenced {
            held_token: 1,
            current_token: 2,
        })
    );
}

#[tokio::test]
async fn committed_checkpoint_is_read_back_with_its_manifest() {
    let fixture = registered_materializer().await;
    let token = acquire_token(&fixture).await;

    fixture
        .store
        .commit_checkpoint(
            fixture.stream_id,
            MATERIALIZER,
            token,
            SequenceNumber::new(40),
            Some("pages/manifest-40".to_owned()),
        )
        .await
        .expect("the checkpoint is committed");

    let checkpoint = stored_checkpoint(&fixture).await;
    assert_eq!(checkpoint.checkpoint, SequenceNumber::new(40));
    assert_eq!(checkpoint.manifest, Some("pages/manifest-40".to_owned()));
}

#[tokio::test]
async fn committing_the_same_checkpoint_again_succeeds() {
    let fixture = registered_materializer().await;
    let token = acquire_token(&fixture).await;
    commit_at(&fixture, token, 40)
        .await
        .expect("the checkpoint is committed");

    let repeated_commit = commit_at(&fixture, token, 40).await;

    assert_eq!(repeated_commit, Ok(()));
}

#[tokio::test]
async fn checkpoint_cannot_move_backwards() {
    let fixture = registered_materializer().await;
    let token = acquire_token(&fixture).await;
    commit_at(&fixture, token, 40)
        .await
        .expect("the checkpoint is committed");

    let backwards = commit_at(&fixture, token, 39).await;

    assert!(
        matches!(backwards, Err(MetaError::InvalidState { .. })),
        "{backwards:?}"
    );
}

#[tokio::test]
async fn refused_commit_leaves_the_checkpoint_where_it_was() {
    let fixture = registered_materializer().await;
    let token = acquire_token(&fixture).await;
    commit_at(&fixture, token, 40)
        .await
        .expect("the checkpoint is committed");

    let _refused_commit = commit_at(&fixture, token, 39).await;

    assert_eq!(stored_checkpoint(&fixture).await.checkpoint, SequenceNumber::new(40));
}

#[tokio::test]
async fn checkpoints_of_one_stream_are_listed_together() {
    let store = store_with_stream().await;
    let stream_id = stream_id_of(&store).await;
    for materializer in ["pages", "blocks"] {
        store
            .register_materializer(STREAM, materializer)
            .await
            .expect("the materializer is registered");
    }

    let listed = store.list_checkpoints(stream_id).await.expect("the store is available");

    let mut materializers = Vec::new();
    for checkpoint in listed {
        materializers.push(checkpoint.materializer);
    }
    assert_eq!(materializers, ["blocks", "pages"]);
}

#[tokio::test]
async fn stream_without_materializers_has_no_checkpoints() {
    let store = store_with_stream().await;
    let stream_id = stream_id_of(&store).await;

    let listed = store.list_checkpoints(stream_id).await;

    assert_eq!(listed, Ok(Vec::new()));
}
