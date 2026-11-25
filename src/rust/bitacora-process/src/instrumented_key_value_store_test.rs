//! Tests of the instrumented metadata store.
//!
//! # What is covered
//!
//! - Each of the three operations is recorded under its own name and reaches the wrapped store.
//! - An outage is counted as a failure and passed on, and the availability gauge follows the last
//!   request.
//! - A transaction whose condition did not hold is an answer, not a failure.
//!
//! # How the fixtures work
//!
//! The wrapped store is the in-memory store of `bitacora-meta`, whose `set_unavailable` switch simulates
//! an outage. Metrics are read from the text exposition, as Prometheus reads them.

use bitacora_meta::MemoryKeyValueStore;
use bytes::Bytes;

use super::*;

/// An in-memory store, the same store instrumented, and the registry it records in.
fn instrumented_memory_store() -> (
    MemoryKeyValueStore,
    InstrumentedKeyValueStore,
    MetricsRegistry,
) {
    let registry = MetricsRegistry::new();
    let memory_store = MemoryKeyValueStore::new();
    let instrumented_store =
        InstrumentedKeyValueStore::new(Arc::new(memory_store.clone()), &registry);
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

/// A write of `value` under `key` that applies whatever the store holds.
fn put(key: &str, value: &'static str) -> Vec<Mutation> {
    vec![Mutation::Put {
        key: key.to_owned(),
        value: Bytes::from_static(value.as_bytes()),
    }]
}

#[tokio::test(start_paused = true)]
async fn write_through_the_decorator_reaches_the_wrapped_store() {
    let (memory_store, instrumented_store, _registry) = instrumented_memory_store();

    instrumented_store
        .transact(Vec::new(), put("streams/orders", "record"))
        .await
        .expect("the store is up");

    let stored = memory_store
        .get("streams/orders")
        .await
        .expect("the store is up")
        .expect("the key was written");
    assert_eq!(stored.value, Bytes::from_static(b"record"));
}

#[tokio::test(start_paused = true)]
async fn read_through_the_decorator_returns_what_the_wrapped_store_holds() {
    let (memory_store, instrumented_store, _registry) = instrumented_memory_store();
    memory_store
        .transact(Vec::new(), put("streams/orders", "record"))
        .await
        .expect("the store is up");

    let read_through_the_decorator = instrumented_store.get("streams/orders").await;

    assert_eq!(
        read_through_the_decorator,
        memory_store.get("streams/orders").await
    );
}

#[tokio::test(start_paused = true)]
async fn listing_through_the_decorator_returns_what_the_wrapped_store_holds() {
    let (memory_store, instrumented_store, _registry) = instrumented_memory_store();
    memory_store
        .transact(Vec::new(), put("streams/orders", "record"))
        .await
        .expect("the store is up");

    let listed_through_the_decorator = instrumented_store.list("streams/").await;

    assert_eq!(
        listed_through_the_decorator,
        memory_store.list("streams/").await
    );
}

#[tokio::test(start_paused = true)]
async fn get_is_counted_under_its_name() {
    let (_memory_store, instrumented_store, registry) = instrumented_memory_store();

    let _read = instrumented_store.get("streams/orders").await;

    assert_eq!(
        exposition_value(
            &registry,
            "bitacora_metadata_store_requests_total{operation=\"get\"} "
        ),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn list_is_counted_under_its_name() {
    let (_memory_store, instrumented_store, registry) = instrumented_memory_store();

    let _listed = instrumented_store.list("streams/").await;

    assert_eq!(
        exposition_value(
            &registry,
            "bitacora_metadata_store_requests_total{operation=\"list\"} "
        ),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn transaction_is_counted_under_its_name() {
    let (_memory_store, instrumented_store, registry) = instrumented_memory_store();

    let _applied = instrumented_store
        .transact(Vec::new(), put("streams/orders", "record"))
        .await;

    assert_eq!(
        exposition_value(
            &registry,
            "bitacora_metadata_store_requests_total{operation=\"transact\"} "
        ),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn outage_is_passed_on_to_the_caller() {
    let (memory_store, instrumented_store, _registry) = instrumented_memory_store();
    memory_store.set_unavailable(true);

    let read = instrumented_store.get("streams/orders").await;

    assert!(read.is_err(), "{read:?}");
}

#[tokio::test(start_paused = true)]
async fn outage_is_counted_as_a_failure_of_the_operation() {
    let (memory_store, instrumented_store, registry) = instrumented_memory_store();
    memory_store.set_unavailable(true);

    let _read = instrumented_store.get("streams/orders").await;

    assert_eq!(
        exposition_value(
            &registry,
            "bitacora_metadata_store_request_failures_total{operation=\"get\",error=\"unavailable\"} "
        ),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn store_counts_as_available_before_anybody_asked_it() {
    let (_memory_store, _instrumented_store, registry) = instrumented_memory_store();

    assert_eq!(
        exposition_value(&registry, "bitacora_metadata_store_available "),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn store_counts_as_unavailable_after_a_failed_request() {
    let (memory_store, instrumented_store, registry) = instrumented_memory_store();
    memory_store.set_unavailable(true);

    let _read = instrumented_store.get("streams/orders").await;

    assert_eq!(
        exposition_value(&registry, "bitacora_metadata_store_available "),
        Some("0".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn store_counts_as_available_again_after_an_answered_request() {
    let (memory_store, instrumented_store, registry) = instrumented_memory_store();
    memory_store.set_unavailable(true);
    let _failed_read = instrumented_store.get("streams/orders").await;
    memory_store.set_unavailable(false);

    let _answered_read = instrumented_store.get("streams/orders").await;

    assert_eq!(
        exposition_value(&registry, "bitacora_metadata_store_available "),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn transaction_whose_condition_did_not_hold_is_not_a_failure() {
    let (_memory_store, instrumented_store, registry) = instrumented_memory_store();
    instrumented_store
        .transact(Vec::new(), put("streams/orders", "first"))
        .await
        .expect("the store is up");
    let only_if_absent = vec![Condition::IsAbsent {
        key: "streams/orders".to_owned(),
    }];

    let outcome = instrumented_store
        .transact(only_if_absent, put("streams/orders", "second"))
        .await;

    assert_eq!(outcome, Ok(TransactionOutcome::ConditionFailed));
    assert!(
        !registry
            .encode()
            .contains("bitacora_metadata_store_request_failures_total{"),
        "{}",
        registry.encode()
    );
}
