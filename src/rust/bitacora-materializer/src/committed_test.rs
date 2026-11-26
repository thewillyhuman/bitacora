//! Tests of reading what a materializer last committed.
//!
//! # What is covered
//!
//! - A materializer that never checkpointed, one that did, and one that checkpointed twice.
//! - What cannot be read: a materializer that is not registered, a metadata store or an object store that
//!   is down, a damaged manifest, and a manifest that stays gone.
//!
//! # How the fixtures work
//!
//! The cluster of `test_harness.rs` has a stream with one materializer registered. [`commit`] plays the
//! runner by hand: it takes the fencing token, stores a manifest under it and commits a checkpoint that
//! names it. Nothing here runs a runner; `runner_test.rs` covers that what a runner commits is read back.

use std::sync::Arc;

use bitacora_artifact::ArtifactStore;
use bytes::Bytes;

use super::*;
use crate::manifest::encode_manifest;
use crate::test_harness::{Harness, MATERIALIZER};

/// The read-only namespace of the materializer under test.
async fn reading_namespace(harness: &Harness) -> ArtifactNamespace {
    let stream_id = harness.stream().await.stream_id;
    ArtifactNamespace::read_only(Arc::new(harness.artifact_store.clone()), stream_id, MATERIALIZER)
}

/// Reads the committed checkpoint of the materializer under test.
async fn read(harness: &Harness) -> Result<CommittedCheckpoint, CommittedCheckpointError> {
    let stream_id = harness.stream().await.stream_id;
    let artifacts = reading_namespace(harness).await;
    read_committed_checkpoint(harness.cluster.metadata(), &artifacts, stream_id, MATERIALIZER).await
}

/// A manifest whose details say which checkpoint it was made for.
fn manifest_for(checkpoint: u64) -> CheckpointManifest {
    CheckpointManifest {
        artifact_keys: Vec::new(),
        details: Bytes::from(format!("details of checkpoint {checkpoint}")),
    }
}

/// Commits a checkpoint at `checkpoint` the way a runner does, and returns the key of its manifest.
async fn commit(harness: &Harness, checkpoint: u64) -> String {
    let metadata = harness.cluster.metadata();
    let stream_id = harness.stream().await.stream_id;
    let fencing_token = metadata
        .acquire_checkpoint(stream_id, MATERIALIZER)
        .await
        .expect("the token is given")
        .fencing_token;
    let writing = ArtifactNamespace::for_runner(
        Arc::new(harness.artifact_store.clone()),
        stream_id,
        MATERIALIZER,
        fencing_token,
    );
    let stored_manifest = encode_manifest(SequenceNumber::new(checkpoint), &manifest_for(checkpoint));
    let manifest_key = writing
        .put_artifact("manifest", stored_manifest)
        .await
        .expect("the manifest is stored");
    metadata
        .commit_checkpoint(
            stream_id,
            MATERIALIZER,
            fencing_token,
            SequenceNumber::new(checkpoint),
            Some(manifest_key.clone()),
        )
        .await
        .expect("the checkpoint is committed");
    manifest_key
}

/// The key of an artifact of the materializer under test, as the object store knows it.
async fn object_key(harness: &Harness, artifact_key: &str) -> String {
    let stream_id = harness.stream().await.stream_id;
    format!("{}{artifact_key}", crate::namespace_prefix(stream_id, MATERIALIZER))
}

#[tokio::test(start_paused = true)]
async fn materializer_that_never_checkpointed_has_no_manifest() {
    let harness = Harness::start().await;

    let committed = read(&harness).await.expect("the record is read");

    assert_eq!(committed.manifest, None);
    assert_eq!(committed.checkpoint, SequenceNumber::ZERO);
}

#[tokio::test(start_paused = true)]
async fn committed_manifest_is_read_with_its_checkpoint() {
    let harness = Harness::start().await;
    commit(&harness, 7).await;

    let committed = read(&harness).await.expect("the checkpoint is read");

    let expected = CommittedCheckpoint {
        checkpoint: SequenceNumber::new(7),
        manifest: Some(manifest_for(7)),
    };
    assert_eq!(committed, expected);
}

#[tokio::test(start_paused = true)]
async fn latest_of_two_checkpoints_is_read() {
    let harness = Harness::start().await;
    commit(&harness, 7).await;
    commit(&harness, 12).await;

    let committed = read(&harness).await.expect("the checkpoint is read");

    assert_eq!(committed.checkpoint, SequenceNumber::new(12));
    assert_eq!(committed.manifest, Some(manifest_for(12)));
}

#[tokio::test(start_paused = true)]
async fn materializer_that_is_not_registered_is_a_metadata_error() {
    let harness = Harness::start().await;
    let stream_id = harness.stream().await.stream_id;
    let artifacts = reading_namespace(&harness).await;

    let read = read_committed_checkpoint(harness.cluster.metadata(), &artifacts, stream_id, "nobody").await;

    assert!(matches!(read, Err(CommittedCheckpointError::Metadata(_))), "{read:?}");
}

#[tokio::test(start_paused = true)]
async fn metadata_store_that_is_down_is_a_metadata_error() {
    let harness = Harness::start().await;
    commit(&harness, 7).await;
    let stream_id = harness.stream().await.stream_id;
    let artifacts = reading_namespace(&harness).await;
    harness.cluster.key_value_store().set_unavailable(true);

    let read = read_committed_checkpoint(harness.cluster.metadata(), &artifacts, stream_id, MATERIALIZER).await;

    assert!(matches!(read, Err(CommittedCheckpointError::Metadata(_))), "{read:?}");
}

#[tokio::test(start_paused = true)]
async fn object_store_that_is_down_is_a_manifest_error() {
    let harness = Harness::start().await;
    commit(&harness, 7).await;
    harness.artifact_store.set_unavailable(true);

    let read = read(&harness).await;

    assert!(matches!(read, Err(CommittedCheckpointError::Manifest(_))), "{read:?}");
}

#[tokio::test(start_paused = true)]
async fn damaged_manifest_is_a_manifest_error() {
    let harness = Harness::start().await;
    let manifest_key = commit(&harness, 7).await;
    harness
        .artifact_store
        .corrupt_byte(&object_key(&harness, &manifest_key).await, 14, 0x01);

    let read = read(&harness).await;

    assert!(matches!(read, Err(CommittedCheckpointError::Manifest(_))), "{read:?}");
}

/// A manifest that is gone and stays gone is not a runner that committed in between. The reader must
/// give up instead of reading the record for ever.
#[tokio::test(start_paused = true)]
async fn manifest_that_stays_gone_ends_the_reading() {
    let harness = Harness::start().await;
    let manifest_key = commit(&harness, 7).await;
    harness
        .artifact_store
        .delete_object(&object_key(&harness, &manifest_key).await)
        .await
        .expect("the manifest is deleted behind the reader's back");

    let read = read(&harness).await;

    assert_eq!(
        read,
        Err(CommittedCheckpointError::ManifestKeptDisappearing { attempts: MAX_ATTEMPTS })
    );
}

#[tokio::test(start_paused = true)]
async fn reading_writes_nothing() {
    let harness = Harness::start().await;
    commit(&harness, 7).await;
    let objects_before = harness.stored_object_count().await;
    let record_before = harness.committed_checkpoint().await;

    read(&harness).await.expect("the checkpoint is read");

    assert_eq!(harness.stored_object_count().await, objects_before);
    assert_eq!(harness.committed_checkpoint().await, record_before);
}
