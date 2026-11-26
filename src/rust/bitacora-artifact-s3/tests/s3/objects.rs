//! Objects on a real S3 server: what the in-memory tests cannot show.
//!
//! # What is covered
//!
//! - Range reads at every edge of an object of several mebibytes, the way a reader of a tiered segment
//!   makes them: the footer at the end, a block in the middle, and the ranges that lie outside.
//! - Keys with characters that need care in a URL keep their exact name.
//! - Stores with different key prefixes share one bucket without seeing each other.
//! - The bucket is addressed in the path of the URL, which is what S3-compatible services expect.
//! - Objects that the store sends as a multipart upload: the server puts the parts together in order, an
//!   object is not listed while its parts arrive, and an upload aborted after it sent a part leaves
//!   nothing and does not block its key.
//!
//! # How the fixtures work
//!
//! `large_object` is five mebibytes in which every byte depends on its position, so a range that comes back
//! shifted or from the wrong place is noticed. Each test works under a key prefix of its own on the shared
//! server. `upload_pattern` hands the same pattern to an upload in pieces of a mebibyte and a little: 18 MiB
//! of it make three parts, 9 MiB make one part sent and a rest that is held back.

use std::ops::Range;

use bitacora_artifact::{ArtifactError, ArtifactStore, ObjectInfo, ObjectUpload};
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

    let outcome = store.get_range(LARGE_OBJECT_KEY, one_byte_too_far.clone()).await;

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

    let outcome = store.get_range(LARGE_OBJECT_KEY, starts_at_the_end.clone()).await;

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

    let listed = store.list_prefix("reports/").await.expect("the listing succeeds");

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

/// The size of the object the upload tests hand over: 18 MiB, which the store sends as two parts of
/// 8 MiB and a last one of 2 MiB.
const UPLOADED_OBJECT_BYTES: u64 = 18 * 1024 * 1024;

/// How much the tests that stop half way hand over: 9 MiB, which sends one whole part.
const ONE_PART_AND_A_LITTLE_BYTES: u64 = 9 * 1024 * 1024;

/// Where the second part of an uploaded object begins: one part of 8 MiB into it.
const SECOND_PART_START: u64 = 8 * 1024 * 1024;

/// The size of the pieces the upload tests hand over: a mebibyte and a little, so that no part ends where
/// a piece does.
const UPLOADED_PIECE_BYTES: u64 = 1024 * 1024 + 11;

/// The key the upload tests store their object under.
const UPLOADED_OBJECT_KEY: &str = "streams/7/segments/00000000000000000004";

/// Starts an upload to [`UPLOADED_OBJECT_KEY`] and hands it the first `object_bytes` bytes of the pattern
/// of the large object, piece by piece.
async fn upload_pattern(store: &S3ArtifactStore, object_bytes: u64) -> Box<dyn ObjectUpload> {
    let mut upload = store
        .start_upload(UPLOADED_OBJECT_KEY)
        .await
        .expect("an upload to a valid key starts");
    let mut handed_over = 0;
    while handed_over < object_bytes {
        let piece_end = object_bytes.min(handed_over + UPLOADED_PIECE_BYTES);
        upload
            .write(expected_bytes(handed_over..piece_end))
            .await
            .expect("the server takes the piece");
        handed_over = piece_end;
    }
    upload
}

/// A real server puts the parts of a multipart upload together itself. A range across the place where two
/// parts meet shows that it put them in order and lost nothing between them.
#[tokio::test]
async fn object_uploaded_in_parts_is_read_across_the_place_where_two_parts_meet() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = server.store_in_fresh_namespace();
    let upload = upload_pattern(&store, UPLOADED_OBJECT_BYTES).await;
    upload.complete().await.expect("the server completes the upload");
    let across_two_parts = SECOND_PART_START - 100..SECOND_PART_START + 100;

    let contents = store.get_range(UPLOADED_OBJECT_KEY, across_two_parts.clone()).await;

    assert_eq!(contents, Ok(expected_bytes(across_two_parts)));
}

#[tokio::test]
async fn object_uploaded_in_parts_is_listed_with_its_whole_size() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = server.store_in_fresh_namespace();
    let upload = upload_pattern(&store, UPLOADED_OBJECT_BYTES).await;
    upload.complete().await.expect("the server completes the upload");

    let listed = store.list_prefix(UPLOADED_OBJECT_KEY).await;

    assert_eq!(
        listed,
        Ok(vec![ObjectInfo {
            object_key: UPLOADED_OBJECT_KEY.to_owned(),
            size_bytes: UPLOADED_OBJECT_BYTES,
        }])
    );
}

/// The contract aborts an upload too, but a small one, which never reaches the server. This one has sent
/// a whole part when it is aborted.
#[tokio::test]
async fn upload_aborted_after_it_sent_a_part_leaves_no_object() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = server.store_in_fresh_namespace();
    let upload = upload_pattern(&store, ONE_PART_AND_A_LITTLE_BYTES).await;
    upload.abort().await.expect("the server drops the parts");

    let listed = store.list_prefix("").await;

    assert_eq!(listed, Ok(Vec::new()));
}

#[tokio::test]
async fn object_is_not_listed_while_its_parts_are_being_sent() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = server.store_in_fresh_namespace();
    let _upload_in_progress = upload_pattern(&store, ONE_PART_AND_A_LITTLE_BYTES).await;

    let listed = store.list_prefix("").await;

    assert_eq!(listed, Ok(Vec::new()));
}

/// What a replica does after a failed upload: it uploads the segment again, to the same key.
#[tokio::test]
async fn key_of_an_aborted_upload_can_be_uploaded_to_again() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = server.store_in_fresh_namespace();
    let first_attempt = upload_pattern(&store, ONE_PART_AND_A_LITTLE_BYTES).await;
    first_attempt.abort().await.expect("the server drops the parts");
    let second_attempt = upload_pattern(&store, UPLOADED_OBJECT_BYTES).await;

    let completed = second_attempt.complete().await;

    assert_eq!(completed, Ok(()));
}
