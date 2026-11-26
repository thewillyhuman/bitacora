//! Tests of the replica registry.
//!
//! # What is covered
//!
//! - A registration is read back as it was written, and the registry is listed sorted by identifier.
//! - A renewal replaces the previous registration instead of adding a second one.
//! - A removed replica is no longer listed, and removing one that was never registered succeeds.
//! - The registry cannot be written or read while the store is unavailable.
//!
//! # How the fixtures work
//!
//! `replica` in `test_support.rs` builds the registration of a replica with a given name and heartbeat; everything else
//! about it is the same for every replica.

use super::*;
use crate::store::test_support::{empty_store, empty_store_with_backend, replica, replica_id};

#[tokio::test]
async fn registered_replica_is_read_back_as_it_was_written() {
    let store = empty_store();
    let registration = replica("replica-a", 1_000);

    store
        .put_replica(&registration)
        .await
        .expect("the replica is registered");

    assert_eq!(store.list_replicas().await, Ok(vec![registration]));
}

#[tokio::test]
async fn registered_replicas_are_listed_sorted() {
    let store = empty_store();
    for name in ["replica-b", "replica-a"] {
        store
            .put_replica(&replica(name, 0))
            .await
            .expect("the replica is registered");
    }

    let listed = store.list_replicas().await.expect("the store is available");

    let mut names = Vec::new();
    for record in listed {
        names.push(record.replica_id.to_string());
    }
    assert_eq!(names, ["replica-a", "replica-b"]);
}

#[tokio::test]
async fn heartbeat_replaces_the_previous_registration() {
    let store = empty_store();
    let renewal = replica("replica-a", 3_000);
    store
        .put_replica(&replica("replica-a", 0))
        .await
        .expect("the replica is registered");

    store.put_replica(&renewal).await.expect("the registration is renewed");

    assert_eq!(store.list_replicas().await, Ok(vec![renewal]));
}

#[tokio::test]
async fn removed_replica_is_no_longer_listed() {
    let store = empty_store();
    store
        .put_replica(&replica("replica-a", 0))
        .await
        .expect("the replica is registered");

    store
        .remove_replica(&replica_id("replica-a"))
        .await
        .expect("the replica is removed");

    assert_eq!(store.list_replicas().await, Ok(Vec::new()));
}

#[tokio::test]
async fn removing_a_replica_leaves_the_others_registered() {
    let store = empty_store();
    let kept = replica("replica-b", 0);
    store
        .put_replica(&replica("replica-a", 0))
        .await
        .expect("the replica is registered");
    store.put_replica(&kept).await.expect("the replica is registered");

    store
        .remove_replica(&replica_id("replica-a"))
        .await
        .expect("the replica is removed");

    assert_eq!(store.list_replicas().await, Ok(vec![kept]));
}

#[tokio::test]
async fn removing_a_replica_that_was_never_registered_succeeds() {
    let store = empty_store();

    let removed = store.remove_replica(&replica_id("replica-z")).await;

    assert_eq!(removed, Ok(()));
}

#[tokio::test]
async fn registration_fails_while_the_store_is_unavailable() {
    let fixture = empty_store_with_backend();
    fixture.key_value_store.set_unavailable(true);

    let registered = fixture.store.put_replica(&replica("replica-a", 0)).await;

    assert!(matches!(registered, Err(MetaError::Unavailable(_))), "{registered:?}");
}

#[tokio::test]
async fn listing_fails_while_the_store_is_unavailable() {
    let fixture = empty_store_with_backend();
    fixture.key_value_store.set_unavailable(true);

    let listed = fixture.store.list_replicas().await;

    assert!(matches!(listed, Err(MetaError::Unavailable(_))), "{listed:?}");
}
