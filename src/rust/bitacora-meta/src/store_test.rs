//! Tests of the pieces every operation of the metadata store is made of.
//!
//! # What is covered
//!
//! - The handle: clones share one backend.
//! - Decoding: a record comes back as it was stored, and a stored value that is not a record is reported with its key,
//!   by a read and by a listing.
//! - The conditional write: applied while the key is unchanged, refused once it changed.
//! - The stream identifier counter: zero and absent in a new store, then the last identifier handed out.
//! - The conversions: a record becomes JSON, a read becomes the condition that nothing changed since, and a lost
//!   read-modify-write becomes an error that names the key.
//!
//! # How the fixtures work
//!
//! The stores come from `store/test_support.rs`. `store_with_a_corrupt_stream_record` writes bytes that are not JSON
//! under the key of the stream `orders`, straight into the backend, next to the healthy stream `audit`.

use super::*;
use crate::store::test_support::{STREAM, create_stream, empty_store, empty_store_with_backend};

/// The key of the record of [`STREAM`].
const STREAM_KEY: &str = "streams/orders";

/// Returns a store that holds the healthy stream `audit` and, under the key of [`STREAM`], bytes that are not a stream
/// record.
async fn store_with_a_corrupt_stream_record() -> MetadataStore {
    let fixture = empty_store_with_backend();
    create_stream(&fixture.store, "audit").await;
    let corrupt_record = Mutation::Put {
        key: STREAM_KEY.to_owned(),
        value: Bytes::from_static(b"not a stream record"),
    };
    fixture
        .key_value_store
        .transact(Vec::new(), vec![corrupt_record])
        .await
        .expect("the store is available");
    fixture.store
}

/// Returns the key a corrupt record was reported under, or `None` if `error` is another failure.
fn corrupt_key_of(error: &MetaError) -> Option<&str> {
    match error {
        MetaError::CorruptRecord { key, .. } => Some(key.as_str()),
        _another_failure => None,
    }
}

#[tokio::test]
async fn clones_share_the_backend() {
    let store = empty_store();
    let another_handle = store.clone();

    create_stream(&another_handle, STREAM).await;

    assert!(store.get_stream(STREAM).await.is_ok());
}

#[tokio::test]
async fn corrupt_record_is_reported_with_its_key() {
    let store = store_with_a_corrupt_stream_record().await;

    let read = store.get_stream(STREAM).await;

    let error = read.expect_err("the record cannot be decoded");
    assert_eq!(corrupt_key_of(&error), Some(STREAM_KEY));
}

/// A listing with a silent hole would be taken for the truth, so one corrupt record fails the listing.
#[tokio::test]
async fn listing_reports_a_corrupt_record_instead_of_skipping_it() {
    let store = store_with_a_corrupt_stream_record().await;

    let listed = store.list_streams().await;

    let error = listed.expect_err("one record cannot be decoded");
    assert_eq!(corrupt_key_of(&error), Some(STREAM_KEY));
}

#[tokio::test]
async fn record_is_replaced_while_its_key_is_unchanged() {
    let store = empty_store();
    create_stream(&store, STREAM).await;
    let read = store.read_stream(STREAM).await.expect("the stream exists");

    let was_written = store.replace_record(STREAM_KEY, read.revision, &read.record).await;

    assert_eq!(was_written, Ok(true));
}

#[tokio::test]
async fn record_is_not_replaced_once_its_key_has_changed() {
    let store = empty_store();
    create_stream(&store, STREAM).await;
    let stale_read = store.read_stream(STREAM).await.expect("the stream exists");
    store
        .replace_record(STREAM_KEY, stale_read.revision, &stale_read.record)
        .await
        .expect("the store is available");

    let was_written = store
        .replace_record(STREAM_KEY, stale_read.revision, &stale_read.record)
        .await;

    assert_eq!(was_written, Ok(false));
}

#[tokio::test]
async fn no_stream_identifier_is_handed_out_in_a_new_store() {
    let store = empty_store();

    let counter = store.read_stream_id_counter().await.expect("the store is available");

    assert_eq!(counter.last_stream_id, 0);
    assert_eq!(
        counter.unchanged,
        Condition::IsAbsent {
            key: "counters/stream-id".to_owned()
        }
    );
}

#[tokio::test]
async fn counter_holds_the_last_stream_identifier_handed_out() {
    let store = empty_store();
    for name in ["orders", "audit"] {
        create_stream(&store, name).await;
    }

    let counter = store.read_stream_id_counter().await.expect("the store is available");

    assert_eq!(counter.last_stream_id, 2);
}

#[test]
fn record_is_stored_as_json() {
    let last_stream_id: u64 = 7;

    let mutation = put_record("counters/stream-id", &last_stream_id);

    assert_eq!(
        mutation,
        Mutation::Put {
            key: "counters/stream-id".to_owned(),
            value: Bytes::from_static(b"7"),
        }
    );
}

#[test]
fn record_is_decoded_from_the_json_it_was_stored_as() {
    let decoded = decode_record::<u64>("counters/stream-id", b"7");

    assert_eq!(decoded, Ok(7));
}

#[test]
fn key_that_was_absent_when_read_must_stay_absent() {
    let condition = unchanged_since_read(STREAM_KEY, None);

    assert_eq!(
        condition,
        Condition::IsAbsent {
            key: STREAM_KEY.to_owned()
        }
    );
}

#[test]
fn key_that_was_read_must_keep_its_revision() {
    let stored = VersionedValue {
        value: Bytes::from_static(b"{}"),
        revision: Revision::new(4),
    };

    let condition = unchanged_since_read(STREAM_KEY, Some(&stored));

    assert_eq!(
        condition,
        Condition::HasRevision {
            key: STREAM_KEY.to_owned(),
            revision: Revision::new(4),
        }
    );
}

#[test]
fn contention_error_names_the_key_and_the_attempts_made() {
    let error = too_much_contention(STREAM_KEY);

    assert_eq!(
        error,
        MetaError::TooMuchContention {
            key: STREAM_KEY.to_owned(),
            attempts: MAX_UPDATE_ATTEMPTS,
        }
    );
}
