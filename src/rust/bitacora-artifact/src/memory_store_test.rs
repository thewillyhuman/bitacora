//! Tests of the in-memory artifact store.
//!
//! # What is covered
//!
//! - The store honours the contract every backend shares.
//! - Clones are handles on the same objects.
//! - Each fault switch does what it says and nothing more: an outage fails every operation and changes no
//!   object; a hang never answers, not even with a failure; corruption changes one byte of one object.
//! - The range read counter counts range reads that returned bytes, and nothing else.
//!
//! # How the fixtures work
//!
//! `store_with_object` gives a store that holds the three bytes `abc` under [`OBJECT_KEY`]. Tests of the
//! hang run on a paused clock, so the hour they wait for an answer takes no real time.

use std::time::Duration;

use super::*;
use crate::contract;

/// The key of the one object most tests store.
const OBJECT_KEY: &str = "layers/0001";

/// The contents stored under [`OBJECT_KEY`].
const OBJECT_CONTENTS: &[u8] = b"abc";

/// How long a test waits for a store that is not expected to answer. On a paused clock it costs nothing.
const LONGER_THAN_ANY_ANSWER: Duration = Duration::from_hours(1);

/// A store that holds [`OBJECT_CONTENTS`] under [`OBJECT_KEY`] and has every fault switched off.
async fn store_with_object() -> MemoryArtifactStore {
    let artifact_store = MemoryArtifactStore::new();
    artifact_store
        .put_object(OBJECT_KEY, Bytes::from_static(OBJECT_CONTENTS))
        .await
        .expect("a store without faults accepts a valid object");
    artifact_store
}

#[tokio::test]
async fn memory_store_honours_the_artifact_store_contract() {
    let artifact_store = MemoryArtifactStore::new();

    contract::check(&artifact_store).await;
}

#[tokio::test]
async fn clones_share_the_same_objects() {
    let artifact_store = MemoryArtifactStore::new();
    let second_handle = artifact_store.clone();

    second_handle
        .put_object(OBJECT_KEY, Bytes::from_static(b"shared"))
        .await
        .expect("a store without faults accepts a valid object");

    assert_eq!(
        artifact_store.get_object(OBJECT_KEY).await,
        Ok(Bytes::from_static(b"shared"))
    );
}

#[tokio::test]
async fn unavailable_store_fails_reads() {
    let artifact_store = store_with_object().await;
    artifact_store.set_unavailable(true);

    let read = artifact_store.get_object(OBJECT_KEY).await;

    assert!(matches!(read, Err(ArtifactError::Unavailable { .. })));
}

#[tokio::test]
async fn unavailable_store_fails_writes() {
    let artifact_store = store_with_object().await;
    artifact_store.set_unavailable(true);

    let written = artifact_store
        .put_object(OBJECT_KEY, Bytes::from_static(b"lost"))
        .await;

    assert!(matches!(written, Err(ArtifactError::Unavailable { .. })));
}

#[tokio::test]
async fn unavailable_store_fails_listings() {
    let artifact_store = store_with_object().await;
    artifact_store.set_unavailable(true);

    let listed = artifact_store.list_prefix("").await;

    assert!(matches!(listed, Err(ArtifactError::Unavailable { .. })));
}

/// A write refused during an outage must not have happened after all.
#[tokio::test]
async fn outage_leaves_the_stored_objects_as_they_were() {
    let artifact_store = store_with_object().await;
    artifact_store.set_unavailable(true);
    let _refused_write = artifact_store
        .put_object(OBJECT_KEY, Bytes::from_static(b"lost"))
        .await;

    artifact_store.set_unavailable(false);

    assert_eq!(
        artifact_store.get_object(OBJECT_KEY).await,
        Ok(Bytes::from_static(OBJECT_CONTENTS))
    );
}

#[tokio::test(start_paused = true)]
async fn unresponsive_store_never_answers() {
    let artifact_store = MemoryArtifactStore::new();
    artifact_store.set_unresponsive(true);

    let answer = tokio::time::timeout(
        LONGER_THAN_ANY_ANSWER,
        artifact_store.get_object(OBJECT_KEY),
    )
    .await;

    assert!(answer.is_err(), "{answer:?}");
}

/// A store that hangs does not get as far as reporting an outage: the hang is checked first.
#[tokio::test(start_paused = true)]
async fn unresponsive_store_does_not_answer_even_with_a_failure() {
    let artifact_store = MemoryArtifactStore::new();
    artifact_store.set_unavailable(true);
    artifact_store.set_unresponsive(true);

    let answer = tokio::time::timeout(
        LONGER_THAN_ANY_ANSWER,
        artifact_store.get_object(OBJECT_KEY),
    )
    .await;

    assert!(answer.is_err(), "{answer:?}");
}

#[tokio::test]
async fn corrupted_byte_reads_back_with_its_bits_flipped() {
    let artifact_store = store_with_object().await;

    artifact_store.corrupt_byte(OBJECT_KEY, 1, 0x01);

    assert_eq!(
        artifact_store.get_object(OBJECT_KEY).await,
        Ok(Bytes::from_static(b"acc"))
    );
}

#[tokio::test]
async fn corrupting_an_object_that_does_not_exist_changes_nothing() {
    let artifact_store = store_with_object().await;

    artifact_store.corrupt_byte("layers/never-written", 0, 0x01);

    assert_eq!(
        artifact_store.get_object(OBJECT_KEY).await,
        Ok(Bytes::from_static(OBJECT_CONTENTS))
    );
}

#[tokio::test]
async fn corrupting_a_byte_past_the_end_of_an_object_changes_nothing() {
    let artifact_store = store_with_object().await;
    let offset_one_past_the_last_byte = OBJECT_CONTENTS.len();

    artifact_store.corrupt_byte(OBJECT_KEY, offset_one_past_the_last_byte, 0x01);

    assert_eq!(
        artifact_store.get_object(OBJECT_KEY).await,
        Ok(Bytes::from_static(OBJECT_CONTENTS))
    );
}

#[tokio::test]
async fn served_range_reads_are_counted() {
    let artifact_store = store_with_object().await;

    let _first_range = artifact_store.get_range(OBJECT_KEY, 0..1).await;
    let _second_range = artifact_store.get_range(OBJECT_KEY, 1..3).await;

    assert_eq!(artifact_store.served_range_reads(""), 2);
}

#[tokio::test]
async fn served_range_reads_are_counted_by_key_prefix() {
    let artifact_store = store_with_object().await;
    artifact_store
        .put_object("elsewhere/object", Bytes::from_static(b"abc"))
        .await
        .expect("put");

    let _range_elsewhere = artifact_store.get_range("elsewhere/object", 0..1).await;

    assert_eq!(artifact_store.served_range_reads("elsewhere/"), 1);
    assert_eq!(artifact_store.served_range_reads(OBJECT_KEY), 0);
}

#[tokio::test]
async fn reads_of_whole_objects_are_not_counted_as_range_reads() {
    let artifact_store = store_with_object().await;

    let _whole_object = artifact_store.get_object(OBJECT_KEY).await;

    assert_eq!(artifact_store.served_range_reads(""), 0);
}

#[tokio::test]
async fn refused_range_reads_are_not_counted() {
    let artifact_store = store_with_object().await;
    let range_past_the_end = 2..9;

    let _refused_range = artifact_store
        .get_range(OBJECT_KEY, range_past_the_end)
        .await;

    assert_eq!(artifact_store.served_range_reads(""), 0);
}
