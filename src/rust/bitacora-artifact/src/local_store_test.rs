//! Tests of the artifact store that keeps objects in a local directory.
//!
//! # What is covered
//!
//! - The store honours the contract every backend shares.
//! - What only a store on disk can get wrong: objects survive reopening; a write that was interrupted
//!   leaves nothing readable and is cleaned up; a key may look like one of the store's own scratch names; a
//!   listing matches its prefix by characters although it walks directories; and a key that a file system
//!   cannot represent is refused cleanly.
//!
//! # How the fixtures work
//!
//! Every test opens a store in a fresh temporary directory. `OpenedStore` keeps that directory alive next
//! to the store, because the directory is deleted when its handle is dropped. An interrupted write is
//! planted by creating a file in the scratch directory by hand, which is exactly what a crash between
//! "write the scratch file" and "rename it" leaves behind.

use tempfile::TempDir;

use super::*;
use crate::contract;

/// A store together with the temporary directory it lives in.
struct OpenedStore {
    /// The directory the store was opened in. Dropping it deletes the directory, so it is kept as long as
    /// the store is used.
    root_directory: TempDir,
    /// The store under test.
    artifact_store: LocalArtifactStore,
}

/// Opens a store in a fresh temporary directory.
async fn open_store() -> OpenedStore {
    let root_directory = TempDir::new().expect("the test can create a temporary directory");
    let artifact_store = open_store_in(&root_directory).await;
    OpenedStore {
        root_directory,
        artifact_store,
    }
}

/// Opens the store that lives in `root_directory`, as a restarted process would.
async fn open_store_in(root_directory: &TempDir) -> LocalArtifactStore {
    LocalArtifactStore::open(root_directory.path())
        .await
        .expect("a writable temporary directory opens as a store")
}

/// Stores `contents` under `object_key`.
async fn store_object(
    artifact_store: &LocalArtifactStore,
    object_key: &str,
    contents: &'static [u8],
) {
    artifact_store
        .put_object(object_key, Bytes::from_static(contents))
        .await
        .expect("a store on a writable directory accepts a valid object");
}

#[tokio::test]
async fn local_store_honours_the_artifact_store_contract() {
    let opened = open_store().await;

    contract::check(&opened.artifact_store).await;
}

#[tokio::test]
async fn objects_survive_reopening_the_store() {
    let opened = open_store().await;
    store_object(&opened.artifact_store, "streams/1/a", b"kept").await;
    drop(opened.artifact_store);

    let reopened = open_store_in(&opened.root_directory).await;

    assert_eq!(
        reopened.get_object("streams/1/a").await,
        Ok(Bytes::from_static(b"kept"))
    );
}

#[tokio::test]
async fn interrupted_write_is_neither_listed_nor_kept() {
    let opened = open_store().await;
    let leftover_scratch_file = opened
        .root_directory
        .path()
        .join(SCRATCH_DIRECTORY_NAME)
        .join("put-7");
    fs::write(&leftover_scratch_file, b"half an object").expect("the leftover is planted");
    drop(opened.artifact_store);

    let reopened = open_store_in(&opened.root_directory).await;

    assert_eq!(reopened.list_prefix("").await, Ok(Vec::new()));
    assert!(!leftover_scratch_file.exists());
}

/// The scratch directory is outside the objects directory, so a key spelled like a scratch path is an
/// ordinary key and a later write, which uses that scratch name, does not touch it.
#[tokio::test]
async fn object_key_may_look_like_a_scratch_name() {
    let opened = open_store().await;
    store_object(&opened.artifact_store, "scratch/put-0", b"an object").await;

    store_object(&opened.artifact_store, "other", b"another").await;

    assert_eq!(
        opened.artifact_store.get_object("scratch/put-0").await,
        Ok(Bytes::from_static(b"an object"))
    );
}

/// The listing walks the directory `logs/`, but the prefix ends in the middle of a file name.
#[tokio::test]
async fn prefix_that_ends_inside_a_name_matches_by_characters() {
    let opened = open_store().await;
    for object_key in ["logs/segment-1", "logs/segment-20", "logs/other"] {
        store_object(&opened.artifact_store, object_key, b"x").await;
    }

    let listed = opened
        .artifact_store
        .list_prefix("logs/segment-")
        .await
        .expect("the directory can be listed");

    let mut listed_keys = Vec::new();
    for object in listed {
        listed_keys.push(object.object_key);
    }
    assert_eq!(listed_keys, ["logs/segment-1", "logs/segment-20"]);
}

/// A prefix that climbs out of the store must not make the listing walk the parent directory.
#[tokio::test]
async fn prefix_that_no_key_can_have_lists_nothing() {
    let opened = open_store().await;
    store_object(&opened.artifact_store, "a", b"x").await;

    let listed = opened.artifact_store.list_prefix("../").await;

    assert_eq!(listed, Ok(Vec::new()));
}

/// `a` cannot be both an object and the directory that holds `a/b`. An object store would allow it; a
/// file system cannot, and the store has to say so and not corrupt anything.
#[tokio::test]
async fn object_cannot_be_stored_where_a_directory_is() {
    let opened = open_store().await;
    store_object(&opened.artifact_store, "a/b", b"x").await;

    let refused = opened
        .artifact_store
        .put_object("a", Bytes::from_static(b"y"))
        .await;

    assert!(matches!(refused, Err(ArtifactError::Unavailable { .. })));
}
