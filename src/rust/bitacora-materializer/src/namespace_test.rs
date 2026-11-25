//! Tests of the artifact namespace.
//!
//! # What is covered
//!
//! - Naming: every artifact gets a key nobody has used, across the clones of a view and across runners.
//! - Isolation: two materializers, or two streams, never see each other's artifacts.
//! - What a view refuses: writing through a read-only view, and labels that cannot be part of a key.
//!
//! # How the fixtures work
//!
//! Every test builds its views on one fresh in-memory artifact store, which the test also reads directly
//! to see where objects really went.

use bitacora_artifact::{ArtifactError, MemoryArtifactStore};

use super::*;

/// The stream every test materializes.
const STREAM: StreamId = StreamId::new(7);

/// A writable view for the materializer `word-count` of [`STREAM`], holding `fencing_token`.
fn runner_view(store: &MemoryArtifactStore, fencing_token: u64) -> ArtifactNamespace {
    ArtifactNamespace::for_runner(Arc::new(store.clone()), STREAM, "word-count", fencing_token)
}

/// Some artifact contents.
fn contents(text: &'static str) -> Bytes {
    Bytes::from_static(text.as_bytes())
}

#[tokio::test]
async fn stored_artifact_is_read_back() {
    let artifacts = runner_view(&MemoryArtifactStore::new(), 1);

    let artifact_key = artifacts
        .put_artifact("snapshot", contents("state"))
        .await
        .expect("the artifact is stored");

    assert_eq!(artifacts.get_artifact(&artifact_key).await, Ok(contents("state")));
}

#[tokio::test]
async fn part_of_an_artifact_is_read_without_the_rest() {
    let artifacts = runner_view(&MemoryArtifactStore::new(), 1);
    let artifact_key = artifacts
        .put_artifact("snapshot", contents("0123456789"))
        .await
        .expect("the artifact is stored");

    let part = artifacts.get_artifact_range(&artifact_key, 3..6).await;

    assert_eq!(part, Ok(contents("345")));
}

#[tokio::test]
async fn artifact_lives_under_the_prefix_of_its_stream_and_materializer() {
    let store = MemoryArtifactStore::new();
    let artifacts = runner_view(&store, 1);

    let artifact_key = artifacts
        .put_artifact("snapshot", contents("state"))
        .await
        .expect("the artifact is stored");

    let object_key = format!("materializers/7/word-count/{artifact_key}");
    assert_eq!(store.get_object(&object_key).await, Ok(contents("state")));
}

#[tokio::test]
async fn two_artifacts_with_the_same_label_get_different_keys() {
    let artifacts = runner_view(&MemoryArtifactStore::new(), 1);

    let first_key = artifacts.put_artifact("delta", contents("a")).await;
    let second_key = artifacts.put_artifact("delta", contents("b")).await;

    assert_ne!(first_key, second_key);
}

#[tokio::test]
async fn clones_of_a_view_never_hand_out_the_same_key() {
    let artifacts = runner_view(&MemoryArtifactStore::new(), 1);
    let clone = artifacts.clone();

    let first_key = artifacts.put_artifact("delta", contents("a")).await;
    let second_key = clone.put_artifact("delta", contents("b")).await;

    assert_ne!(first_key, second_key);
}

#[tokio::test]
async fn runner_with_another_token_cannot_overwrite_an_artifact() {
    let store = MemoryArtifactStore::new();
    let replaced_runner = runner_view(&store, 1);
    let current_runner = runner_view(&store, 2);
    let kept_key = current_runner
        .put_artifact("snapshot", contents("current"))
        .await
        .expect("the artifact is stored");

    replaced_runner
        .put_artifact("snapshot", contents("stale"))
        .await
        .expect("the artifact is stored");

    assert_eq!(current_runner.get_artifact(&kept_key).await, Ok(contents("current")));
}

#[tokio::test]
async fn artifact_key_names_the_token_of_its_writer() {
    let artifacts = runner_view(&MemoryArtifactStore::new(), 42);

    let artifact_key = artifacts
        .put_artifact("snapshot", contents("state"))
        .await
        .expect("the artifact is stored");

    assert_eq!(token_of_artifact(&artifact_key), Some(42));
}

#[tokio::test]
async fn later_runner_reads_what_an_earlier_one_wrote() {
    let store = MemoryArtifactStore::new();
    let artifact_key = runner_view(&store, 1)
        .put_artifact("snapshot", contents("state"))
        .await
        .expect("the artifact is stored");

    let read_by_successor = runner_view(&store, 2).get_artifact(&artifact_key).await;

    assert_eq!(read_by_successor, Ok(contents("state")));
}

#[tokio::test]
async fn another_materializer_does_not_see_the_artifact() {
    let store = MemoryArtifactStore::new();
    let artifact_key = runner_view(&store, 1)
        .put_artifact("snapshot", contents("state"))
        .await
        .expect("the artifact is stored");
    let other_materializer = ArtifactNamespace::read_only(Arc::new(store.clone()), STREAM, "other-materializer");

    let read = other_materializer.get_artifact(&artifact_key).await;

    assert!(matches!(
        read,
        Err(MaterializerError::ArtifactStore(ArtifactError::NotFound { .. }))
    ));
}

#[tokio::test]
async fn another_stream_does_not_see_the_artifact() {
    let store = MemoryArtifactStore::new();
    let artifact_key = runner_view(&store, 1)
        .put_artifact("snapshot", contents("state"))
        .await
        .expect("the artifact is stored");
    let other_stream = ArtifactNamespace::read_only(Arc::new(store.clone()), StreamId::new(8), "word-count");

    let read = other_stream.get_artifact(&artifact_key).await;

    assert!(matches!(
        read,
        Err(MaterializerError::ArtifactStore(ArtifactError::NotFound { .. }))
    ));
}

#[tokio::test]
async fn read_only_view_reads_artifacts() {
    let store = MemoryArtifactStore::new();
    let artifact_key = runner_view(&store, 1)
        .put_artifact("snapshot", contents("state"))
        .await
        .expect("the artifact is stored");
    let follower = ArtifactNamespace::read_only(Arc::new(store.clone()), STREAM, "word-count");

    assert_eq!(follower.get_artifact(&artifact_key).await, Ok(contents("state")));
}

#[tokio::test]
async fn read_only_view_refuses_to_add_an_artifact() {
    let store = MemoryArtifactStore::new();
    let follower = ArtifactNamespace::read_only(Arc::new(store.clone()), STREAM, "word-count");

    let stored = follower.put_artifact("snapshot", contents("state")).await;

    assert_eq!(stored, Err(MaterializerError::ReadOnly));
}

#[tokio::test]
async fn label_that_cannot_be_part_of_a_key_is_refused() {
    let artifacts = runner_view(&MemoryArtifactStore::new(), 1);
    let too_long = "x".repeat(MAX_LABEL_BYTES + 1);

    for label in ["", "a/b", "with space", "tab\t", too_long.as_str()] {
        let stored = artifacts.put_artifact(label, contents("state")).await;

        assert_eq!(
            stored,
            Err(MaterializerError::InvalidLabel {
                label: label.to_owned()
            })
        );
    }
}

#[tokio::test]
async fn store_outage_is_reported_as_transient() {
    let store = MemoryArtifactStore::new();
    let artifacts = runner_view(&store, 1);
    store.set_unavailable(true);

    let stored = artifacts.put_artifact("snapshot", contents("state")).await;

    assert!(stored.is_err_and(|error| error.is_transient()));
}

#[test]
fn key_not_written_by_a_view_names_no_token() {
    for foreign_key in ["notes.txt", "not-a-number/0000000000-x", ""] {
        assert_eq!(token_of_artifact(foreign_key), None, "{foreign_key:?}");
    }
}
