//! What every [`ArtifactStore`] must do, as one reusable check.
//!
//! Each backend runs [`check`] against itself in its own tests, so that the
//! in-memory store that the simulator uses and the stores that hold real
//! data cannot drift apart.

use bytes::Bytes;

use crate::store::{ArtifactError, ArtifactStore, ObjectInfo};

/// Runs the artifact store contract against `store`, which must be empty.
///
/// # Panics
///
/// Panics, naming the broken rule, if the store breaks the contract.
pub async fn check(store: &dyn ArtifactStore) {
    stored_object_is_read_back(store).await;
    missing_object_is_reported_as_not_found(store).await;
    put_replaces_an_existing_object(store).await;
    range_reads_return_exactly_the_bytes_asked_for(store).await;
    range_outside_the_object_is_refused(store).await;
    listing_returns_the_prefix_sorted_with_sizes(store).await;
    deleted_object_is_gone(store).await;
    deleting_a_missing_object_succeeds(store).await;
    invalid_keys_are_refused(store).await;
    empty_object_is_a_real_object(store).await;
}

async fn stored_object_is_read_back(store: &dyn ArtifactStore) {
    store
        .put_object("contract/read-back", Bytes::from_static(b"payload"))
        .await
        .expect("put");
    assert_eq!(
        store.get_object("contract/read-back").await,
        Ok(Bytes::from_static(b"payload")),
        "a stored object must be read back"
    );
}

async fn missing_object_is_reported_as_not_found(store: &dyn ArtifactStore) {
    let missing = "contract/never-written";
    let not_found = Err(ArtifactError::NotFound {
        object_key: missing.to_owned(),
    });
    assert_eq!(
        store.get_object(missing).await,
        not_found,
        "a missing object must be NotFound"
    );
    assert_eq!(
        store.get_range(missing, 0..1).await,
        not_found,
        "a range of a missing object must be NotFound"
    );
}

async fn put_replaces_an_existing_object(store: &dyn ArtifactStore) {
    for contents in [&b"first version, longer"[..], &b"second"[..]] {
        store
            .put_object("contract/replaced", Bytes::from_static(contents))
            .await
            .expect("put");
    }
    assert_eq!(
        store.get_object("contract/replaced").await,
        Ok(Bytes::from_static(b"second")),
        "a put must replace the whole object"
    );
}

async fn range_reads_return_exactly_the_bytes_asked_for(store: &dyn ArtifactStore) {
    store
        .put_object("contract/ranged", Bytes::from_static(b"0123456789"))
        .await
        .expect("put");
    assert_eq!(
        store.get_range("contract/ranged", 3..7).await,
        Ok(Bytes::from_static(b"3456")),
        "a range read must return exactly the range"
    );
    assert_eq!(
        store.get_range("contract/ranged", 10..10).await,
        Ok(Bytes::new()),
        "an empty range at the end of an object is valid"
    );
}

async fn range_outside_the_object_is_refused(store: &dyn ArtifactStore) {
    assert!(
        matches!(
            store.get_range("contract/ranged", 5..11).await,
            Err(ArtifactError::RangeOutOfBounds { size_bytes: 10, .. })
        ),
        "a range past the end of an object must be refused"
    );
}

async fn listing_returns_the_prefix_sorted_with_sizes(store: &dyn ArtifactStore) {
    for (object_key, contents) in [
        ("contract/list/b", &b"22"[..]),
        ("contract/list/a", &b"1"[..]),
        ("contract/list/nested/c", &b"333"[..]),
        ("contract/listed-elsewhere", &b"x"[..]),
    ] {
        store
            .put_object(object_key, Bytes::from_static(contents))
            .await
            .expect("put");
    }
    let listed = store.list_prefix("contract/list/").await.expect("list");
    let expected: Vec<ObjectInfo> = [
        ("contract/list/a", 1),
        ("contract/list/b", 2),
        ("contract/list/nested/c", 3),
    ]
    .into_iter()
    .map(|(object_key, size_bytes)| ObjectInfo {
        object_key: object_key.to_owned(),
        size_bytes,
    })
    .collect();
    assert_eq!(
        listed, expected,
        "a listing must return exactly the prefix, sorted, with sizes"
    );
}

async fn deleted_object_is_gone(store: &dyn ArtifactStore) {
    store
        .put_object("contract/deleted", Bytes::from_static(b"gone soon"))
        .await
        .expect("put");
    store
        .delete_object("contract/deleted")
        .await
        .expect("delete");
    assert!(
        matches!(
            store.get_object("contract/deleted").await,
            Err(ArtifactError::NotFound { .. })
        ),
        "a deleted object must be gone"
    );
    assert_eq!(
        store.list_prefix("contract/deleted").await,
        Ok(Vec::new()),
        "a deleted object must not be listed"
    );
}

async fn deleting_a_missing_object_succeeds(store: &dyn ArtifactStore) {
    assert_eq!(
        store.delete_object("contract/never-written").await,
        Ok(()),
        "deleting a missing object must succeed"
    );
}

async fn invalid_keys_are_refused(store: &dyn ArtifactStore) {
    for object_key in ["", "/absolute", "a/../escape", "trailing/"] {
        assert!(
            matches!(
                store.put_object(object_key, Bytes::from_static(b"x")).await,
                Err(ArtifactError::InvalidKey { .. })
            ),
            "key {object_key:?} must be refused"
        );
    }
}

async fn empty_object_is_a_real_object(store: &dyn ArtifactStore) {
    store
        .put_object("contract/empty", Bytes::new())
        .await
        .expect("put");
    assert_eq!(
        store.get_object("contract/empty").await,
        Ok(Bytes::new()),
        "an empty object must exist and be empty"
    );
}
