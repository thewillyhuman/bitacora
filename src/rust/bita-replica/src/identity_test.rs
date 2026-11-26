//! Tests of the check that keeps an empty data directory from starting under a known identity.
//!
//! # What is covered
//!
//! - An empty directory starts under an identity nobody knows, and is refused under one the registry or
//!   a stream names.
//! - A directory that has its identity is never checked against the cluster, and never waits for the
//!   metadata store.
//! - An empty directory does not start while the metadata store cannot be asked.
//!
//! # How the fixtures work
//!
//! The data directory is a `MemoryDisk`; the metadata store is the in-memory one. `PROPOSED` is the
//! identity the configuration proposes. A directory "that has its identity" is one a replica was opened
//! on before, which is what writes the identity file.

use std::sync::Arc;

use bitacora_meta::{CopyRecord, CopyState, MemoryKeyValueStore, ReplicaRecord, RetentionPolicy, SegmentRecord};
use bitacora_segment::MemoryDisk;
use bitacora_types::{Epoch, ReplicationSettings, SegmentId, SequenceNumber, Timestamp};

use super::*;
use crate::test_support::open_replica;

/// The identity the configuration proposes.
const PROPOSED: &str = "replica-a";

/// The stream the tests create.
const STREAM: &str = "orders";

/// A metadata store on `key_value_store`.
fn metadata_on(key_value_store: &MemoryKeyValueStore) -> MetadataStore {
    MetadataStore::new(Arc::new(key_value_store.clone()))
}

/// The identifier `name`.
fn replica_id(name: &str) -> ReplicaId {
    ReplicaId::new(name).expect("a valid identifier")
}

/// Registers a replica called `name` in the registry.
async fn register(metadata: &MetadataStore, name: &str) {
    let record = ReplicaRecord {
        schema_version: bitacora_meta::RECORD_SCHEMA_VERSION,
        replica_id: replica_id(name),
        address: format!("{name}.example.org:7000"),
        failure_domains: vec![format!("host-{name}")],
        capacity_bytes: 1_000_000,
        free_bytes: 900_000,
        is_draining: false,
        heartbeat_at: Timestamp::from_unix_millis(1_000),
    };
    metadata.put_replica(&record).await.expect("the replica is registered");
}

/// Creates [`STREAM`] with the three named replicas as its standby replica set.
async fn create_stream_with_standby(metadata: &MetadataStore, replica_names: [&str; 3]) {
    let mut standby_replicas = Vec::new();
    for name in replica_names {
        standby_replicas.push(replica_id(name));
    }
    metadata
        .create_stream(
            STREAM,
            ReplicationSettings::DEFAULT,
            RetentionPolicy::default(),
            Some(standby_replicas),
        )
        .await
        .expect("the stream is created");
}

/// Gives [`STREAM`] an open segment whose copies are on the three named replicas, as a writer does when
/// it opens one.
async fn open_segment_on(metadata: &MetadataStore, replica_names: [&str; 3]) {
    let mut copies = Vec::new();
    for name in replica_names {
        copies.push(CopyRecord {
            replica_id: replica_id(name),
            state: CopyState::Unverified,
        });
    }
    metadata
        .update_stream(STREAM, |stream| {
            stream.segments.push(SegmentRecord {
                segment_id: SegmentId::FIRST,
                epoch: Epoch::new(1),
                first_sequence: SequenceNumber::FIRST,
                sealed: None,
                copies: copies.clone(),
                tiered: None,
            });
            Ok(())
        })
        .await
        .expect("the segment is recorded");
}

/// Runs the check for [`PROPOSED`] on `disk`.
async fn check(disk: &MemoryDisk, metadata: &MetadataStore) -> Result<(), DaemonError> {
    refuse_a_known_identity_on_an_empty_directory(disk, &replica_id(PROPOSED), metadata).await
}

#[tokio::test]
async fn empty_directory_may_start_under_an_identity_nobody_knows() {
    let metadata = metadata_on(&MemoryKeyValueStore::new());
    register(&metadata, "replica-b").await;

    let checked = check(&MemoryDisk::new(), &metadata).await;

    assert_eq!(checked, Ok(()));
}

#[tokio::test]
async fn empty_directory_is_refused_under_a_registered_identity() {
    let metadata = metadata_on(&MemoryKeyValueStore::new());
    register(&metadata, PROPOSED).await;

    let checked = check(&MemoryDisk::new(), &metadata).await;

    assert!(
        matches!(checked, Err(DaemonError::IdentityAlreadyKnown { .. })),
        "{checked:?}"
    );
}

/// The registry entry of a replica can be removed while streams still list its copies. Those copies are
/// the acknowledgements that an empty directory must not answer for.
#[tokio::test]
async fn empty_directory_is_refused_under_an_identity_that_holds_a_copy() {
    let metadata = metadata_on(&MemoryKeyValueStore::new());
    create_stream_with_standby(&metadata, ["replica-b", "replica-c", "replica-d"]).await;
    open_segment_on(&metadata, [PROPOSED, "replica-b", "replica-c"]).await;

    let checked = check(&MemoryDisk::new(), &metadata).await;

    assert!(
        matches!(checked, Err(DaemonError::IdentityAlreadyKnown { .. })),
        "{checked:?}"
    );
}

#[tokio::test]
async fn empty_directory_is_refused_under_an_identity_in_a_standby_set() {
    let metadata = metadata_on(&MemoryKeyValueStore::new());
    create_stream_with_standby(&metadata, ["replica-b", "replica-c", PROPOSED]).await;

    let checked = check(&MemoryDisk::new(), &metadata).await;

    assert!(
        matches!(checked, Err(DaemonError::IdentityAlreadyKnown { .. })),
        "{checked:?}"
    );
}

#[tokio::test]
async fn refusal_says_how_the_cluster_knows_the_identity() {
    let metadata = metadata_on(&MemoryKeyValueStore::new());
    register(&metadata, PROPOSED).await;

    let checked = check(&MemoryDisk::new(), &metadata).await;

    assert_eq!(
        checked,
        Err(DaemonError::IdentityAlreadyKnown {
            replica_id: replica_id(PROPOSED),
            known_from: "it is registered at replica-a.example.org:7000".to_owned(),
        })
    );
}

/// The ordinary restart: the directory is the one that registered the identity.
#[tokio::test]
async fn directory_that_has_its_identity_starts_although_it_is_registered() {
    let metadata = metadata_on(&MemoryKeyValueStore::new());
    register(&metadata, PROPOSED).await;
    let disk = MemoryDisk::new();
    open_replica(&disk, PROPOSED).await.stop();

    let checked = check(&disk, &metadata).await;

    assert_eq!(checked, Ok(()));
}

#[tokio::test]
async fn directory_that_has_its_identity_starts_while_the_metadata_store_is_down() {
    let key_value_store = MemoryKeyValueStore::new();
    let disk = MemoryDisk::new();
    open_replica(&disk, PROPOSED).await.stop();
    key_value_store.set_unavailable(true);

    let checked = check(&disk, &metadata_on(&key_value_store)).await;

    assert_eq!(checked, Ok(()));
}

#[tokio::test]
async fn empty_directory_does_not_start_while_the_metadata_store_is_down() {
    let key_value_store = MemoryKeyValueStore::new();
    key_value_store.set_unavailable(true);

    let checked = check(&MemoryDisk::new(), &metadata_on(&key_value_store)).await;

    assert!(matches!(checked, Err(DaemonError::CannotStart { .. })), "{checked:?}");
}
