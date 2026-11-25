//! Objects on a real S3 server: what the in-memory tests cannot show.
//!
//! # What is covered
//!
//! - Range reads at every edge of an object of several mebibytes, the way a reader of a tiered segment
//!   makes them: the footer at the end, a block in the middle, and the ranges that lie outside.
//! - Keys with characters that need care in a URL keep their exact name.
//! - Stores with different key prefixes share one bucket without seeing each other.
//! - The bucket is addressed in the path of the URL, which is what S3-compatible services expect.
//!
//! # How the fixtures work
//!
//! `large_object` is five mebibytes in which every byte depends on its position, so a range that comes back
//! shifted or from the wrong place is noticed. Each test works under a key prefix of its own on the shared
//! server.

use std::ops::Range;

use bitacora_artifact::{ArtifactError, ArtifactStore};
use bitacora_artifact_s3::S3ArtifactStore;
use bytes::Bytes;

use crate::s3_server::{fresh_namespace, open_store, shared_s3_server};

/// The size of the object the range tests read from.
const LARGE_OBJECT_BYTES: usize = 5 * 1024 * 1024;

/// The same size as a number a range can use.
const LARGE_OBJECT_END: u64 = LARGE_OBJECT_BYTES as u64;

/// The key the range tests store their object under.
const LARGE_OBJECT_KEY: &str = "streams/7/segments/00000000000000000003";

/// The byte at `position` of the large object. The period is a prime, so the pattern does not line up with
/// any block size.
fn byte_at(position: u64) -> u8 {
    u8::try_from(position % 251).expect("a remainder below 251 fits in a byte")
}

/// The contents of the large object.
fn large_object() -> Bytes {
    let mut contents = Vec::with_capacity(LARGE_OBJECT_BYTES);
    for position in 0..LARGE_OBJECT_END {
        contents.push(byte_at(position));
    }
    Bytes::from(contents)
}

/// What `byte_range` of the large object must contain.
fn expected_bytes(byte_range: Range<u64>) -> Bytes {
    let mut contents = Vec::new();
    for position in byte_range {
        contents.push(byte_at(position));
    }
    Bytes::from(contents)
}

/// Stores the large object in `store`.
async fn store_large_object(store: &S3ArtifactStore) {
    store
        .put_object(LARGE_OBJECT_KEY, large_object())
        .await
        .expect("the large object is stored");
}

/// The error for `byte_range` lying outside the large object.
fn out_of_bounds(byte_range: &Range<u64>) -> ArtifactError {
    ArtifactError::RangeOutOfBounds {
        object_key: LARGE_OBJECT_KEY.to_owned(),
        start: byte_range.start,
        end: byte_range.end,
        size_bytes: LARGE_OBJECT_END,
    }
}

#[tokio::test]
async fn large_object_is_read_back_whole() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = server.store_in_fresh_namespace();
    store_large_object(&store).await;

    let contents = store.get_object(LARGE_OBJECT_KEY).await;

    assert_eq!(contents, Ok(large_object()));
}

#[tokio::test]
async fn first_byte_of_a_large_object_is_read() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = server.store_in_fresh_namespace();
    store_large_object(&store).await;

    let contents = store.get_range(LARGE_OBJECT_KEY, 0..1).await;

    assert_eq!(contents, Ok(expected_bytes(0..1)));
}

/// What a reader does first with a tiered segment: fetch the footer from the end of the object.
#[tokio::test]
async fn last_bytes_of_a_large_object_are_read() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = server.store_in_fresh_namespace();
    store_large_object(&store).await;
    let footer = LARGE_OBJECT_END - 72..LARGE_OBJECT_END;

    let contents = store.get_range(LARGE_OBJECT_KEY, footer.clone()).await;

    assert_eq!(contents, Ok(expected_bytes(footer)));
}

#[tokio::test]
async fn block_in_the_middle_of_a_large_object_is_read() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = server.store_in_fresh_namespace();
    store_large_object(&store).await;
    let block = 3_000_000..3_065_536;

    let contents = store.get_range(LARGE_OBJECT_KEY, block.clone()).await;

    assert_eq!(contents, Ok(expected_bytes(block)));
}

#[tokio::test]
async fn range_of_the_whole_large_object_is_read() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = server.store_in_fresh_namespace();
    store_large_object(&store).await;

    let contents = store.get_range(LARGE_OBJECT_KEY, 0..LARGE_OBJECT_END).await;

    assert_eq!(contents, Ok(large_object()));
}

#[tokio::test]
async fn empty_range_at_the_end_of_a_large_object_is_empty() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = server.store_in_fresh_namespace();
    store_large_object(&store).await;

    let contents = store
        .get_range(LARGE_OBJECT_KEY, LARGE_OBJECT_END..LARGE_OBJECT_END)
        .await;

    assert_eq!(contents, Ok(Bytes::new()));
}

/// The server answers this one with the bytes there are; the store must notice that they are too few.
#[tokio::test]
async fn range_that_ends_one_byte_past_a_large_object_reports_its_size() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = server.store_in_fresh_namespace();
    store_large_object(&store).await;
    let one_byte_too_far = LARGE_OBJECT_END - 10..LARGE_OBJECT_END + 1;

    let outcome = store
        .get_range(LARGE_OBJECT_KEY, one_byte_too_far.clone())
        .await;

    assert_eq!(outcome, Err(out_of_bounds(&one_byte_too_far)));
}

/// The server refuses this one without saying how large the object is; the store must find out.
#[tokio::test]
async fn range_that_starts_at_the_end_of_a_large_object_reports_its_size() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = server.store_in_fresh_namespace();
    store_large_object(&store).await;
    let starts_at_the_end = LARGE_OBJECT_END..LARGE_OBJECT_END + 1;

    let outcome = store
        .get_range(LARGE_OBJECT_KEY, starts_at_the_end.clone())
        .await;

    assert_eq!(outcome, Err(out_of_bounds(&starts_at_the_end)));
}

#[tokio::test]
async fn range_far_past_a_large_object_reports_its_size() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = server.store_in_fresh_namespace();
    store_large_object(&store).await;
    let far_past = 4 * LARGE_OBJECT_END..4 * LARGE_OBJECT_END + 100;

    let outcome = store.get_range(LARGE_OBJECT_KEY, far_past.clone()).await;

    assert_eq!(outcome, Err(out_of_bounds(&far_past)));
}

#[tokio::test]
async fn range_of_a_missing_object_is_not_found() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = server.store_in_fresh_namespace();

    let outcome = store.get_range("never-written", 0..10).await;

    assert_eq!(
        outcome,
        Err(ArtifactError::NotFound {
            object_key: "never-written".to_owned()
        })
    );
}

#[tokio::test]
async fn key_with_characters_that_need_care_in_a_url_is_read_back() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = server.store_in_fresh_namespace();
    let unusual_key = "reports/100% done + checked #1 (año 2026).txt";
    store
        .put_object(unusual_key, Bytes::from_static(b"kept"))
        .await
        .expect("the object is stored");

    let contents = store.get_object(unusual_key).await;

    assert_eq!(contents, Ok(Bytes::from_static(b"kept")));
}

#[tokio::test]
async fn key_with_characters_that_need_care_in_a_url_is_listed_unchanged() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = server.store_in_fresh_namespace();
    let unusual_key = "reports/100% done + checked #1 (año 2026).txt";
    store
        .put_object(unusual_key, Bytes::from_static(b"kept"))
        .await
        .expect("the object is stored");

    let listed = store
        .list_prefix("reports/")
        .await
        .expect("the listing succeeds");

    assert_eq!(listed[0].object_key, unusual_key);
}

#[tokio::test]
async fn stores_with_different_key_prefixes_do_not_read_each_other() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let one_cluster = server.store_in(&fresh_namespace());
    let another_cluster = server.store_in(&fresh_namespace());
    one_cluster
        .put_object("shared-name", Bytes::from_static(b"from one"))
        .await
        .expect("the object is stored");

    let read_by_the_other = another_cluster.get_object("shared-name").await;

    assert_eq!(
        read_by_the_other,
        Err(ArtifactError::NotFound {
            object_key: "shared-name".to_owned()
        })
    );
}

#[tokio::test]
async fn stores_with_different_key_prefixes_do_not_list_each_other() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let one_cluster = server.store_in(&fresh_namespace());
    let another_cluster = server.store_in(&fresh_namespace());
    one_cluster
        .put_object("shared-name", Bytes::from_static(b"from one"))
        .await
        .expect("the object is stored");

    let listed_by_the_other = another_cluster.list_prefix("").await;

    assert_eq!(listed_by_the_other, Ok(Vec::new()));
}

#[tokio::test]
async fn store_reopened_under_the_same_key_prefix_finds_its_objects() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let key_prefix = fresh_namespace();
    server
        .store_in(&key_prefix)
        .put_object("kept", Bytes::from_static(b"across restarts"))
        .await
        .expect("the object is stored");

    let contents = server.store_in(&key_prefix).get_object("kept").await;

    assert_eq!(contents, Ok(Bytes::from_static(b"across restarts")));
}

/// Path style is the default because it needs no DNS name per bucket; this pins that a store opened with
/// it explicitly reaches the bucket.
#[tokio::test]
async fn bucket_is_reached_with_path_style_addressing() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let mut path_style = server.settings();
    path_style.force_path_style = true;
    path_style.key_prefix = fresh_namespace();
    let store = open_store(&path_style);

    let stored = store.put_object("a", Bytes::from_static(b"x")).await;

    assert_eq!(stored, Ok(()));
}
