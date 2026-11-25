//! Tests of the orphan sweeper.
//!
//! # What is covered
//!
//! - What is deleted: artifacts of replaced runners that the committed manifest does not name.
//! - What is kept: everything the committed manifest names, the manifest itself, whatever the live runner
//!   has written and not committed yet, and objects the namespace did not write.
//! - The race the sweeper must not lose: a checkpoint that is written, and committed while or after the
//!   sweep runs, keeps all its artifacts.
//! - Failure: if the committed manifest cannot be read, nothing is deleted.
//!
//! # How the fixtures work
//!
//! No cluster is needed: a sweep only talks to the metadata store and the artifact store, and both are
//! in memory here. [`Fixture`] plays the runners by hand, one step at a time, which is what lets a test
//! put a sweep exactly between a runner's write and its commit.

use bitacora_artifact::MemoryArtifactStore;
use bitacora_meta::{MemoryKeyValueStore, RetentionPolicy};
use bitacora_types::{ReplicationSettings, SequenceNumber};
use bytes::Bytes;

use super::*;
use crate::contract::CheckpointManifest;
use crate::manifest::encode_manifest;

/// The stream the materializer under test follows.
const STREAM: &str = "orders";

/// The materializer under test.
const MATERIALIZER: &str = "word-count";

/// A metadata store with one stream and one registered materializer, and an empty artifact store.
struct Fixture {
    /// The metadata store.
    metadata: MetadataStore,
    /// The artifact store, also read directly by the tests.
    artifact_store: MemoryArtifactStore,
    /// The stream the materializer follows.
    stream_id: StreamId,
}

impl Fixture {
    /// Creates the stream and registers the materializer.
    async fn new() -> Self {
        let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
        let stream = metadata
            .create_stream(STREAM, ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
            .await
            .expect("a fresh metadata store accepts a stream");
        metadata
            .register_materializer(STREAM, MATERIALIZER)
            .await
            .expect("a fresh stream accepts a materializer");
        Self {
            metadata,
            artifact_store: MemoryArtifactStore::new(),
            stream_id: stream.stream_id,
        }
    }

    /// Does what a starting runner does: raises the fencing token and opens the namespace with it.
    async fn take_over(&self) -> ArtifactNamespace {
        let acquired = self
            .metadata
            .acquire_checkpoint(self.stream_id, MATERIALIZER)
            .await
            .expect("the materializer is registered");
        ArtifactNamespace::for_runner(
            Arc::new(self.artifact_store.clone()),
            self.stream_id,
            MATERIALIZER,
            acquired.fencing_token,
        )
    }

    /// Does the first half of a checkpoint: stores the manifest that names `artifact_keys`. Returns the
    /// manifest's key.
    async fn write_manifest(&self, runner: &ArtifactNamespace, checkpoint: u64, artifact_keys: &[String]) -> String {
        let manifest = CheckpointManifest {
            artifact_keys: artifact_keys.to_vec(),
            details: Bytes::new(),
        };
        let stored_manifest = encode_manifest(SequenceNumber::new(checkpoint), &manifest);
        runner
            .put_artifact("manifest", stored_manifest)
            .await
            .expect("the manifest is stored")
    }

    /// Does the second half of a checkpoint: commits the pointer to a stored manifest.
    async fn commit(&self, runner: &ArtifactNamespace, checkpoint: u64, manifest_key: &str) {
        self.metadata
            .commit_checkpoint(
                self.stream_id,
                MATERIALIZER,
                runner.fencing_token().expect("a runner's view can write"),
                SequenceNumber::new(checkpoint),
                Some(manifest_key.to_owned()),
            )
            .await
            .expect("the checkpoint is committed");
    }

    /// Writes and commits a checkpoint that names `artifact_keys`. Returns the manifest's key.
    async fn checkpoint(&self, runner: &ArtifactNamespace, checkpoint: u64, artifact_keys: &[String]) -> String {
        let manifest_key = self.write_manifest(runner, checkpoint, artifact_keys).await;
        self.commit(runner, checkpoint, &manifest_key).await;
        manifest_key
    }

    /// Runs the sweep anyone may run.
    async fn sweep(&self) -> Result<SweepReport, SweepError> {
        sweep_orphans(
            &self.metadata,
            Arc::new(self.artifact_store.clone()),
            self.stream_id,
            MATERIALIZER,
        )
        .await
    }

    /// Whether the artifact `artifact_key` of the materializer under test still exists.
    async fn holds(&self, runner: &ArtifactNamespace, artifact_key: &str) -> bool {
        runner.get_artifact(artifact_key).await.is_ok()
    }
}

/// Stores an artifact through `runner` and returns its key.
async fn put(runner: &ArtifactNamespace, label: &str) -> String {
    runner
        .put_artifact(label, Bytes::from_static(b"contents"))
        .await
        .expect("the artifact is stored")
}

#[tokio::test]
async fn orphan_of_a_replaced_runner_is_deleted() {
    let fixture = Fixture::new().await;
    let replaced_runner = fixture.take_over().await;
    let orphan_key = put(&replaced_runner, "never-committed").await;
    let current_runner = fixture.take_over().await;

    let report = fixture.sweep().await.expect("the sweep runs");

    assert_eq!(report.artifacts_deleted, 1);
    assert!(!fixture.holds(&current_runner, &orphan_key).await);
}

#[tokio::test]
async fn artifact_named_by_the_committed_manifest_is_kept_whoever_wrote_it() {
    let fixture = Fixture::new().await;
    let first_runner = fixture.take_over().await;
    let named_key = put(&first_runner, "snapshot").await;
    fixture
        .checkpoint(&first_runner, 10, std::slice::from_ref(&named_key))
        .await;
    let current_runner = fixture.take_over().await;

    let report = fixture.sweep().await.expect("the sweep runs");

    assert_eq!(report.artifacts_deleted, 0);
    assert!(fixture.holds(&current_runner, &named_key).await);
}

#[tokio::test]
async fn committed_manifest_is_kept() {
    let fixture = Fixture::new().await;
    let first_runner = fixture.take_over().await;
    let manifest_key = fixture.checkpoint(&first_runner, 10, &[]).await;
    let current_runner = fixture.take_over().await;

    fixture.sweep().await.expect("the sweep runs");

    assert!(fixture.holds(&current_runner, &manifest_key).await);
}

#[tokio::test]
async fn manifest_that_was_replaced_is_deleted_once_its_runner_is_gone() {
    let fixture = Fixture::new().await;
    let first_runner = fixture.take_over().await;
    let replaced_manifest_key = fixture.checkpoint(&first_runner, 10, &[]).await;
    fixture.checkpoint(&first_runner, 20, &[]).await;
    let current_runner = fixture.take_over().await;

    fixture.sweep().await.expect("the sweep runs");

    assert!(!fixture.holds(&current_runner, &replaced_manifest_key).await);
}

#[tokio::test]
async fn artifact_the_live_runner_has_not_committed_yet_is_kept() {
    let fixture = Fixture::new().await;
    let live_runner = fixture.take_over().await;
    let in_flight_key = put(&live_runner, "snapshot").await;

    let report = fixture.sweep().await.expect("the sweep runs");

    assert_eq!(report.artifacts_deleted, 0);
    assert!(fixture.holds(&live_runner, &in_flight_key).await);
}

#[tokio::test]
async fn checkpoint_swept_between_its_write_and_its_commit_keeps_its_artifacts() {
    let fixture = Fixture::new().await;
    let live_runner = fixture.take_over().await;
    let snapshot_key = put(&live_runner, "snapshot").await;
    let manifest_key = fixture
        .write_manifest(&live_runner, 10, std::slice::from_ref(&snapshot_key))
        .await;

    fixture.sweep().await.expect("the sweep runs");
    fixture.commit(&live_runner, 10, &manifest_key).await;

    assert!(fixture.holds(&live_runner, &snapshot_key).await);
    assert!(fixture.holds(&live_runner, &manifest_key).await);
}

#[tokio::test]
async fn checkpoint_committed_while_a_sweep_is_under_way_keeps_its_artifacts() {
    let fixture = Fixture::new().await;
    let replaced_runner = fixture.take_over().await;
    let old_snapshot_key = put(&replaced_runner, "snapshot").await;
    let old_manifest_key = fixture
        .checkpoint(&replaced_runner, 10, std::slice::from_ref(&old_snapshot_key))
        .await;
    let live_runner = fixture.take_over().await;
    let current_token = live_runner.fencing_token().expect("a runner's view can write");
    // The sweep reads the checkpoint record and the manifest ...
    let referenced_as_the_sweep_saw_it = referenced_artifacts(&live_runner, Some(&old_manifest_key))
        .await
        .expect("the manifest is readable");
    // ... the live runner commits a checkpoint with a new snapshot meanwhile ...
    let new_snapshot_key = put(&live_runner, "snapshot").await;
    let new_manifest_key = fixture
        .checkpoint(&live_runner, 20, std::slice::from_ref(&new_snapshot_key))
        .await;

    // ... and only then does the sweep delete.
    delete_unreferenced(&live_runner, &referenced_as_the_sweep_saw_it, current_token)
        .await
        .expect("the deletion runs");

    assert!(fixture.holds(&live_runner, &new_snapshot_key).await);
    assert!(fixture.holds(&live_runner, &new_manifest_key).await);
}

#[tokio::test]
async fn takeover_while_a_sweep_is_under_way_loses_nothing_the_successor_restores_from() {
    let fixture = Fixture::new().await;
    let first_runner = fixture.take_over().await;
    let snapshot_key = put(&first_runner, "snapshot").await;
    let manifest_key = fixture
        .checkpoint(&first_runner, 10, std::slice::from_ref(&snapshot_key))
        .await;
    let token_as_the_sweep_saw_it = first_runner.fencing_token().expect("a runner's view can write");
    let referenced_as_the_sweep_saw_it = referenced_artifacts(&first_runner, Some(&manifest_key))
        .await
        .expect("the manifest is readable");

    let successor = fixture.take_over().await;
    delete_unreferenced(&successor, &referenced_as_the_sweep_saw_it, token_as_the_sweep_saw_it)
        .await
        .expect("the deletion runs");

    assert!(fixture.holds(&successor, &snapshot_key).await);
    assert!(fixture.holds(&successor, &manifest_key).await);
}

#[tokio::test]
async fn runner_that_has_just_committed_deletes_its_own_leftovers() {
    let fixture = Fixture::new().await;
    let live_runner = fixture.take_over().await;
    let own_token = live_runner.fencing_token().expect("a runner's view can write");
    let leftover_key = put(&live_runner, "from-a-failed-attempt").await;
    let manifest_key = fixture.checkpoint(&live_runner, 10, &[]).await;
    let referenced = referenced_artifacts(&live_runner, Some(&manifest_key))
        .await
        .expect("the manifest is readable");

    delete_unreferenced(&live_runner, &referenced, own_token + 1)
        .await
        .expect("the deletion runs");

    assert!(!fixture.holds(&live_runner, &leftover_key).await);
    assert!(fixture.holds(&live_runner, &manifest_key).await);
}

#[tokio::test]
async fn object_the_namespace_did_not_write_is_kept() {
    let fixture = Fixture::new().await;
    fixture.take_over().await;
    fixture.take_over().await;
    let foreign_object_key = format!(
        "{}README.txt",
        crate::namespace::namespace_prefix(fixture.stream_id, MATERIALIZER)
    );
    fixture
        .artifact_store
        .put_object(&foreign_object_key, Bytes::from_static(b"left by an operator"))
        .await
        .expect("the object is stored");

    let report = fixture.sweep().await.expect("the sweep runs");

    assert_eq!(report.artifacts_deleted, 0);
    assert!(fixture.artifact_store.get_object(&foreign_object_key).await.is_ok());
}

#[tokio::test]
async fn artifacts_of_another_materializer_are_not_touched() {
    let fixture = Fixture::new().await;
    let other_materializer = ArtifactNamespace::for_runner(
        Arc::new(fixture.artifact_store.clone()),
        fixture.stream_id,
        "other-materializer",
        0,
    );
    let other_key = put(&other_materializer, "snapshot").await;
    fixture.take_over().await;
    fixture.take_over().await;

    fixture.sweep().await.expect("the sweep runs");

    assert!(fixture.holds(&other_materializer, &other_key).await);
}

#[tokio::test]
async fn nothing_is_deleted_when_the_committed_manifest_cannot_be_read() {
    let fixture = Fixture::new().await;
    let replaced_runner = fixture.take_over().await;
    let orphan_key = put(&replaced_runner, "never-committed").await;
    fixture
        .commit(&replaced_runner, 10, "00000000000000000001/0000000099-manifest")
        .await;
    let current_runner = fixture.take_over().await;

    let swept = fixture.sweep().await;

    assert!(matches!(swept, Err(SweepError::Artifacts(_))));
    assert!(fixture.holds(&current_runner, &orphan_key).await);
}

#[tokio::test]
async fn sweep_of_a_materializer_that_is_not_registered_fails() {
    let fixture = Fixture::new().await;

    let swept = sweep_orphans(
        &fixture.metadata,
        Arc::new(fixture.artifact_store.clone()),
        fixture.stream_id,
        "never-registered",
    )
    .await;

    assert!(matches!(
        swept,
        Err(SweepError::Metadata(MetaError::MaterializerNotRegistered { .. }))
    ));
}

#[tokio::test]
async fn second_sweep_finds_nothing_left_to_delete() {
    let fixture = Fixture::new().await;
    let replaced_runner = fixture.take_over().await;
    put(&replaced_runner, "never-committed").await;
    fixture.take_over().await;
    fixture.sweep().await.expect("the sweep runs");

    let second_report = fixture.sweep().await.expect("the sweep runs");

    assert_eq!(second_report, SweepReport::default());
}
