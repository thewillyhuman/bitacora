//! Tests of the instrumented object store.
//!
//! # What is covered
//!
//! - Each of the five operations on whole objects is recorded under its own name and reaches the wrapped
//!   store.
//! - Failures are counted by kind and passed on unchanged.
//! - Transferred bytes are counted by direction, for successful requests only.
//! - An upload: its start and each of its steps are recorded under their own names, the object reaches
//!   the wrapped store, and its bytes are counted as uploaded when it is completed and at no other time.
//!
//! # How the fixtures work
//!
//! The wrapped store is the in-memory store of `bitacora-artifact`, whose `set_unavailable` switch
//! simulates an outage. Metrics are read from the text exposition, as Prometheus reads them.

use bitacora_artifact::MemoryArtifactStore;

use super::*;

/// The key the tests store an object under.
const OBJECT_KEY: &str = "streams/7/segments/3";

/// An in-memory store, the same store instrumented, and the registry it records in.
fn instrumented_memory_store() -> (MemoryArtifactStore, InstrumentedArtifactStore, MetricsRegistry) {
    let registry = MetricsRegistry::new();
    let memory_store = MemoryArtifactStore::new();
    let instrumented_store = InstrumentedArtifactStore::new(Arc::new(memory_store.clone()), &registry);
    (memory_store, instrumented_store, registry)
}

/// The value at the end of the exposition line that starts with `series`, if there is such a line.
fn exposition_value(registry: &MetricsRegistry, series: &str) -> Option<String> {
    for line in registry.encode().lines() {
        if let Some(value) = line.strip_prefix(series) {
            return Some(value.trim().to_owned());
        }
    }
    None
}

/// The number of requests recorded for `operation`, as the exposition shows it.
fn requests_of(registry: &MetricsRegistry, operation: &str) -> Option<String> {
    exposition_value(
        registry,
        &format!("bitacora_object_store_requests_total{{operation=\"{operation}\"}} "),
    )
}

/// Stores the ten bytes `0123456789` under [`OBJECT_KEY`] through `store`.
async fn put_ten_bytes(store: &dyn ArtifactStore) {
    store
        .put_object(OBJECT_KEY, Bytes::from_static(b"0123456789"))
        .await
        .expect("the object is stored");
}

#[tokio::test(start_paused = true)]
async fn object_stored_through_the_decorator_is_in_the_wrapped_store() {
    let (memory_store, instrumented_store, _registry) = instrumented_memory_store();

    put_ten_bytes(&instrumented_store).await;

    assert_eq!(
        memory_store.get_object(OBJECT_KEY).await,
        Ok(Bytes::from_static(b"0123456789"))
    );
}

#[tokio::test(start_paused = true)]
async fn object_is_read_through_the_decorator() {
    let (memory_store, instrumented_store, _registry) = instrumented_memory_store();
    put_ten_bytes(&memory_store).await;

    let read = instrumented_store.get_object(OBJECT_KEY).await;

    assert_eq!(read, Ok(Bytes::from_static(b"0123456789")));
}

#[tokio::test(start_paused = true)]
async fn range_is_read_through_the_decorator() {
    let (memory_store, instrumented_store, _registry) = instrumented_memory_store();
    put_ten_bytes(&memory_store).await;

    let read = instrumented_store.get_range(OBJECT_KEY, 2..5).await;

    assert_eq!(read, Ok(Bytes::from_static(b"234")));
}

#[tokio::test(start_paused = true)]
async fn object_deleted_through_the_decorator_is_gone_from_the_wrapped_store() {
    let (memory_store, instrumented_store, _registry) = instrumented_memory_store();
    put_ten_bytes(&memory_store).await;

    instrumented_store
        .delete_object(OBJECT_KEY)
        .await
        .expect("the object is deleted");

    assert_eq!(memory_store.list_prefix("").await, Ok(Vec::new()));
}

#[tokio::test(start_paused = true)]
async fn listing_through_the_decorator_returns_what_the_wrapped_store_holds() {
    let (memory_store, instrumented_store, _registry) = instrumented_memory_store();
    put_ten_bytes(&memory_store).await;

    let listed_through_the_decorator = instrumented_store.list_prefix("streams/").await;

    assert_eq!(listed_through_the_decorator, memory_store.list_prefix("streams/").await);
}

#[tokio::test(start_paused = true)]
async fn put_is_counted_under_its_name() {
    let (_memory_store, instrumented_store, registry) = instrumented_memory_store();

    put_ten_bytes(&instrumented_store).await;

    assert_eq!(requests_of(&registry, "put_object"), Some("1".to_owned()));
}

#[tokio::test(start_paused = true)]
async fn get_is_counted_under_its_name() {
    let (_memory_store, instrumented_store, registry) = instrumented_memory_store();

    let _read = instrumented_store.get_object(OBJECT_KEY).await;

    assert_eq!(requests_of(&registry, "get_object"), Some("1".to_owned()));
}

#[tokio::test(start_paused = true)]
async fn range_read_is_counted_under_its_name() {
    let (_memory_store, instrumented_store, registry) = instrumented_memory_store();

    let _read = instrumented_store.get_range(OBJECT_KEY, 0..1).await;

    assert_eq!(requests_of(&registry, "get_range"), Some("1".to_owned()));
}

#[tokio::test(start_paused = true)]
async fn delete_is_counted_under_its_name() {
    let (_memory_store, instrumented_store, registry) = instrumented_memory_store();

    let _deleted = instrumented_store.delete_object(OBJECT_KEY).await;

    assert_eq!(requests_of(&registry, "delete_object"), Some("1".to_owned()));
}

#[tokio::test(start_paused = true)]
async fn listing_is_counted_under_its_name() {
    let (_memory_store, instrumented_store, registry) = instrumented_memory_store();

    let _listed = instrumented_store.list_prefix("streams/").await;

    assert_eq!(requests_of(&registry, "list_prefix"), Some("1".to_owned()));
}

#[tokio::test(start_paused = true)]
async fn stored_bytes_are_counted_as_uploaded() {
    let (_memory_store, instrumented_store, registry) = instrumented_memory_store();

    put_ten_bytes(&instrumented_store).await;

    assert_eq!(
        exposition_value(
            &registry,
            "bitacora_object_store_transferred_bytes_total{direction=\"uploaded\"} "
        ),
        Some("10".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn bytes_of_objects_and_ranges_read_are_counted_as_downloaded() {
    let (memory_store, instrumented_store, registry) = instrumented_memory_store();
    put_ten_bytes(&memory_store).await;

    let _whole_object = instrumented_store.get_object(OBJECT_KEY).await;
    let _three_bytes = instrumented_store.get_range(OBJECT_KEY, 2..5).await;

    assert_eq!(
        exposition_value(
            &registry,
            "bitacora_object_store_transferred_bytes_total{direction=\"downloaded\"} "
        ),
        Some("13".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn missing_object_is_passed_on_and_counted_by_its_kind() {
    let (_memory_store, instrumented_store, registry) = instrumented_memory_store();

    let read = instrumented_store.get_object(OBJECT_KEY).await;

    assert_eq!(
        read,
        Err(ArtifactError::NotFound {
            object_key: OBJECT_KEY.to_owned()
        })
    );
    assert_eq!(
        exposition_value(
            &registry,
            "bitacora_object_store_request_failures_total{operation=\"get_object\",error=\"not_found\"} "
        ),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn outage_is_counted_as_unavailable() {
    let (memory_store, instrumented_store, registry) = instrumented_memory_store();
    memory_store.set_unavailable(true);

    let stored = instrumented_store
        .put_object(OBJECT_KEY, Bytes::from_static(b"0123456789"))
        .await;

    assert!(matches!(stored, Err(ArtifactError::Unavailable { .. })));
    assert_eq!(
        exposition_value(
            &registry,
            "bitacora_object_store_request_failures_total{operation=\"put_object\",error=\"unavailable\"} "
        ),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn bytes_of_a_failed_upload_are_not_counted() {
    let (memory_store, instrumented_store, registry) = instrumented_memory_store();
    memory_store.set_unavailable(true);

    let _stored = instrumented_store
        .put_object(OBJECT_KEY, Bytes::from_static(b"0123456789"))
        .await;

    assert!(
        !registry
            .encode()
            .contains("bitacora_object_store_transferred_bytes_total{"),
        "{}",
        registry.encode()
    );
}

/// Starts an upload to [`OBJECT_KEY`] through `store` and hands it the ten bytes `0123456789` in two
/// pieces.
async fn upload_ten_bytes(store: &dyn ArtifactStore) -> Box<dyn ObjectUpload> {
    let mut upload = store.start_upload(OBJECT_KEY).await.expect("the upload starts");
    for piece in [&b"01234"[..], b"56789"] {
        upload
            .write(Bytes::from_static(piece))
            .await
            .expect("the piece is taken");
    }
    upload
}

/// The bytes counted as uploaded, as the exposition shows them.
fn uploaded_bytes(registry: &MetricsRegistry) -> Option<String> {
    exposition_value(
        registry,
        "bitacora_object_store_transferred_bytes_total{direction=\"uploaded\"} ",
    )
}

#[tokio::test(start_paused = true)]
async fn object_uploaded_through_the_decorator_is_in_the_wrapped_store() {
    let (memory_store, instrumented_store, _registry) = instrumented_memory_store();
    let upload = upload_ten_bytes(&instrumented_store).await;

    upload.complete().await.expect("the upload is completed");

    assert_eq!(
        memory_store.get_object(OBJECT_KEY).await,
        Ok(Bytes::from_static(b"0123456789"))
    );
}

#[tokio::test(start_paused = true)]
async fn start_of_an_upload_is_counted_under_its_name() {
    let (_memory_store, instrumented_store, registry) = instrumented_memory_store();

    let _upload = upload_ten_bytes(&instrumented_store).await;

    assert_eq!(requests_of(&registry, "start_upload"), Some("1".to_owned()));
}

#[tokio::test(start_paused = true)]
async fn every_piece_of_an_upload_is_counted_as_a_write() {
    let (_memory_store, instrumented_store, registry) = instrumented_memory_store();

    let _upload = upload_ten_bytes(&instrumented_store).await;

    assert_eq!(requests_of(&registry, "upload_write"), Some("2".to_owned()));
}

#[tokio::test(start_paused = true)]
async fn completion_of_an_upload_is_counted_under_its_name() {
    let (_memory_store, instrumented_store, registry) = instrumented_memory_store();
    let upload = upload_ten_bytes(&instrumented_store).await;

    upload.complete().await.expect("the upload is completed");

    assert_eq!(requests_of(&registry, "upload_complete"), Some("1".to_owned()));
}

#[tokio::test(start_paused = true)]
async fn abort_of_an_upload_is_counted_under_its_name() {
    let (_memory_store, instrumented_store, registry) = instrumented_memory_store();
    let upload = upload_ten_bytes(&instrumented_store).await;

    upload.abort().await.expect("the upload is aborted");

    assert_eq!(requests_of(&registry, "upload_abort"), Some("1".to_owned()));
}

#[tokio::test(start_paused = true)]
async fn bytes_of_a_completed_upload_are_counted_as_uploaded() {
    let (_memory_store, instrumented_store, registry) = instrumented_memory_store();
    let upload = upload_ten_bytes(&instrumented_store).await;

    upload.complete().await.expect("the upload is completed");

    assert_eq!(uploaded_bytes(&registry), Some("10".to_owned()));
}

/// The pieces left the process, but no object came of them: `uploaded` counts stored objects.
#[tokio::test(start_paused = true)]
async fn bytes_of_an_aborted_upload_are_not_counted() {
    let (_memory_store, instrumented_store, registry) = instrumented_memory_store();
    let upload = upload_ten_bytes(&instrumented_store).await;

    upload.abort().await.expect("the upload is aborted");

    assert_eq!(uploaded_bytes(&registry), None);
}

#[tokio::test(start_paused = true)]
async fn bytes_of_an_upload_whose_completion_failed_are_not_counted() {
    let (memory_store, instrumented_store, registry) = instrumented_memory_store();
    let upload = upload_ten_bytes(&instrumented_store).await;
    memory_store.set_unavailable(true);

    let _failed = upload.complete().await;

    assert_eq!(uploaded_bytes(&registry), None);
}

#[tokio::test(start_paused = true)]
async fn piece_refused_during_an_outage_is_counted_as_unavailable() {
    let (memory_store, instrumented_store, registry) = instrumented_memory_store();
    let mut upload = upload_ten_bytes(&instrumented_store).await;
    memory_store.set_unavailable(true);

    let _refused = upload.write(Bytes::from_static(b"more")).await;

    assert_eq!(
        exposition_value(
            &registry,
            "bitacora_object_store_request_failures_total{operation=\"upload_write\",error=\"unavailable\"} "
        ),
        Some("1".to_owned())
    );
}

#[test]
fn every_kind_of_error_has_its_own_label_value() {
    let object_key = OBJECT_KEY.to_owned();
    let errors_and_kinds = [
        (
            ArtifactError::NotFound {
                object_key: object_key.clone(),
            },
            "not_found",
        ),
        (
            ArtifactError::RangeOutOfBounds {
                object_key: object_key.clone(),
                start: 0,
                end: 9,
                size_bytes: 3,
            },
            "range_out_of_bounds",
        ),
        (ArtifactError::InvalidKey { object_key }, "invalid_key"),
        (ArtifactError::Unavailable { reason: String::new() }, "unavailable"),
    ];

    for (error, expected_kind) in &errors_and_kinds {
        assert_eq!(kind_of_artifact_error(error), *expected_kind, "{error:?}");
    }
}
