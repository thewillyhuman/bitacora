//! Tests of connecting to the replicas of a segment.
//!
//! # What is covered
//!
//! - A registered replica is reached through the connector, at its registered address.
//! - A replica that is not registered gets a handle that fails every request as unavailable.
//! - The handles come back in the order of the copies.
//! - A registry that cannot be read fails the whole call.
//!
//! # How the fixtures work
//!
//! The metadata store is in memory. The connector is the static one of `bitacora-types`, in which a handle is
//! registered per address. The handles themselves are `UnreachableReplica`s with a distinct reason each: asking one for
//! its status returns that reason, which tells the test which handle it was given without a real replica.

use bitacora_meta::{CopyState, MemoryKeyValueStore, RECORD_SCHEMA_VERSION};
use bitacora_types::replica_api::{ReplicaError, StaticReplicaConnector};
use bitacora_types::{ManualClock, Timestamp};

use super::*;

/// A context whose registry and connector know the replicas `registered_names`, with the backend of its metadata store.
struct ContextWithRegistry {
    /// The context under test.
    context: ClientContext,
    /// The store behind the context's metadata store, to switch it off.
    key_value_store: MemoryKeyValueStore,
}

/// The replica identifier `name`.
fn replica_id(name: &str) -> ReplicaId {
    ReplicaId::new(name).expect("a valid identifier")
}

/// The address the replica `name` is registered at.
fn address_of(name: &str) -> String {
    format!("{name}.example:7000")
}

/// The reason the stand-in handle of the replica `name` fails with.
fn handle_reason(name: &str) -> String {
    format!("the handle registered for {name}")
}

/// A copy of some segment on the replica `name`.
fn copy_on(name: &str) -> CopyRecord {
    CopyRecord {
        replica_id: replica_id(name),
        state: CopyState::Unverified,
    }
}

/// Builds a context in which every replica of `registered_names` is in the registry and known to the connector.
async fn context_with_registered(registered_names: &[&str]) -> ContextWithRegistry {
    let key_value_store = MemoryKeyValueStore::new();
    let metadata = MetadataStore::new(Arc::new(key_value_store.clone()));
    let mut connector = StaticReplicaConnector::new();
    for name in registered_names {
        let registration = ReplicaRecord {
            schema_version: RECORD_SCHEMA_VERSION,
            replica_id: replica_id(name),
            address: address_of(name),
            failure_domains: Vec::new(),
            capacity_bytes: 1_000,
            free_bytes: 1_000,
            is_draining: false,
            heartbeat_at: Timestamp::from_unix_millis(0),
        };
        metadata
            .put_replica(&registration)
            .await
            .expect("the replica is registered");
        connector.register(
            replica_id(name),
            address_of(name),
            Arc::new(UnreachableReplica::new(handle_reason(name))),
        );
    }
    let context = ClientContext {
        metadata,
        connector: Arc::new(connector),
        clock: Arc::new(ManualClock::starting_at(Timestamp::from_unix_millis(0))),
        artifact_store: None,
    };
    ContextWithRegistry {
        context,
        key_value_store,
    }
}

/// Asks the replica behind `connected_replica` for its status and returns why that failed.
async fn failure_reason_of(connected_replica: &ConnectedReplica) -> String {
    match connected_replica.client.status().await {
        Err(ReplicaError::Unavailable { reason }) => reason,
        other_answer => panic!("a stand-in handle fails as unavailable, got {other_answer:?}"),
    }
}

#[tokio::test]
async fn registered_replica_is_reached_through_the_connector() {
    let fixture = context_with_registered(&["replica-a"]).await;

    let connected_replicas = fixture
        .context
        .connect_copies(&[copy_on("replica-a")])
        .await
        .expect("the registry is readable");

    assert_eq!(
        failure_reason_of(&connected_replicas[0]).await,
        handle_reason("replica-a")
    );
}

#[tokio::test]
async fn replica_that_is_not_registered_fails_every_request_as_unavailable() {
    let fixture = context_with_registered(&["replica-a"]).await;

    let connected_replicas = fixture
        .context
        .connect_copies(&[copy_on("replica-gone")])
        .await
        .expect("the registry is readable");

    assert_eq!(
        failure_reason_of(&connected_replicas[0]).await,
        "replica replica-gone is not registered"
    );
}

#[tokio::test]
async fn replica_that_is_not_registered_keeps_its_identity() {
    let fixture = context_with_registered(&[]).await;

    let connected_replicas = fixture
        .context
        .connect_copies(&[copy_on("replica-gone")])
        .await
        .expect("the registry is readable");

    assert_eq!(connected_replicas[0].replica_id, replica_id("replica-gone"));
}

#[tokio::test]
async fn handles_come_back_in_the_order_of_the_copies() {
    let fixture = context_with_registered(&["replica-a", "replica-b", "replica-c"]).await;
    let copies = [copy_on("replica-c"), copy_on("replica-a"), copy_on("replica-b")];

    let connected_replicas = fixture
        .context
        .connect_copies(&copies)
        .await
        .expect("the registry is readable");

    let mut connected_ids = Vec::new();
    for connected_replica in &connected_replicas {
        connected_ids.push(connected_replica.replica_id.clone());
    }
    assert_eq!(
        connected_ids,
        [
            replica_id("replica-c"),
            replica_id("replica-a"),
            replica_id("replica-b")
        ]
    );
}

#[tokio::test]
async fn each_handle_belongs_to_the_copy_at_its_position() {
    let fixture = context_with_registered(&["replica-a", "replica-b"]).await;
    let copies = [copy_on("replica-b"), copy_on("replica-a")];

    let connected_replicas = fixture
        .context
        .connect_copies(&copies)
        .await
        .expect("the registry is readable");

    assert_eq!(
        failure_reason_of(&connected_replicas[0]).await,
        handle_reason("replica-b")
    );
    assert_eq!(
        failure_reason_of(&connected_replicas[1]).await,
        handle_reason("replica-a")
    );
}

#[tokio::test]
async fn segment_without_copies_connects_to_nothing() {
    let fixture = context_with_registered(&["replica-a"]).await;

    let connected_replicas = fixture
        .context
        .connect_copies(&[])
        .await
        .expect("the registry is readable");

    assert!(connected_replicas.is_empty());
}

#[tokio::test]
async fn unreadable_registry_fails_the_call() {
    let fixture = context_with_registered(&["replica-a"]).await;
    fixture.key_value_store.set_unavailable(true);

    let connected = fixture.context.connect_copies(&[copy_on("replica-a")]).await;

    assert!(matches!(connected, Err(MetaError::Unavailable(_))));
}
