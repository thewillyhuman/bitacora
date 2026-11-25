//! The behaviours every key-value backend must have, checked on etcd.
//!
//! # Why this file exists
//!
//! `bitacora_meta::MetadataStore` is written once against the key-value
//! interface and trusts every backend to behave the same. The in-memory
//! backend is tested for a list of behaviours in
//! `bitacora-meta/src/memory_store.rs`; this file checks the same list, by
//! the same test names, on etcd. If the two lists pass, code that works in
//! the simulator works on a real cluster.
//!
//! # What it contains
//!
//! - Reads and writes: values, revisions, deletion.
//! - Conditional transactions: absence and revision conditions, atomicity,
//!   and two writers racing for one key.
//! - Listings: prefix, order, and a listing larger than the default
//!   response limit of gRPC.
//! - Key prefixes: stores with different prefixes are isolated, stores with
//!   the same prefix share their data.
//! - What etcd refuses that the in-memory store accepts.
//!
//! # How the fixtures work
//!
//! Every test asks for the shared etcd and a store under a fresh key
//! prefix, so it starts empty and can use the same key names as the others.
//! A test returns at once when no etcd program is available (see
//! `etcd_process.rs`).

use bitacora_meta::{Condition, KeyValueStore, Mutation, Revision, TransactionOutcome};
use bytes::Bytes;

use crate::etcd_process::shared_etcd;
use crate::writes::{delete_mutation, put, put_mutation, revision_of};

/// The size of one value in the test of a large listing. Below the 1.5 MiB
/// that etcd accepts in one request.
const LARGE_VALUE_BYTES: usize = 1024 * 1024;

/// How many large values that test writes. Together they are larger than
/// the 4 MiB a gRPC client accepts in one response by default.
const LARGE_VALUE_COUNT: usize = 5;

// ---- reads and writes ----

#[tokio::test]
async fn missing_key_reads_as_absent() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;

    let stored = store.get("streams/orders").await;

    assert_eq!(stored, Ok(None));
}

#[tokio::test]
async fn written_value_is_read_back() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;
    put(&store, "streams/orders", b"v1").await.expect("put");

    let stored = store.get("streams/orders").await.expect("get");

    assert_eq!(
        stored.map(|stored| stored.value),
        Some(Bytes::from_static(b"v1"))
    );
}

#[tokio::test]
async fn empty_value_is_read_back_as_present() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;
    put(&store, "streams/orders", b"").await.expect("put");

    let stored = store.get("streams/orders").await.expect("get");

    assert_eq!(stored.map(|stored| stored.value), Some(Bytes::new()));
}

#[tokio::test]
async fn every_write_changes_the_revision() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;
    put(&store, "streams/orders", b"v1").await.expect("put");
    let first_revision = revision_of(&store, "streams/orders").await;

    put(&store, "streams/orders", b"v1").await.expect("put");

    assert!(revision_of(&store, "streams/orders").await > first_revision);
}

#[tokio::test]
async fn deleted_key_reads_as_absent() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;
    put(&store, "streams/orders", b"v1").await.expect("put");

    store
        .transact(Vec::new(), vec![delete_mutation("streams/orders")])
        .await
        .expect("delete");

    assert_eq!(store.get("streams/orders").await, Ok(None));
}

#[tokio::test]
async fn recreated_key_never_repeats_a_revision() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;
    put(&store, "streams/orders", b"v1").await.expect("put");
    let first_revision = revision_of(&store, "streams/orders").await;
    store
        .transact(Vec::new(), vec![delete_mutation("streams/orders")])
        .await
        .expect("delete");

    put(&store, "streams/orders", b"v2").await.expect("put");

    assert!(revision_of(&store, "streams/orders").await > first_revision);
}

#[tokio::test]
async fn deleting_a_missing_key_is_not_an_error() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;

    let outcome = store
        .transact(Vec::new(), vec![delete_mutation("streams/orders")])
        .await;

    assert_eq!(outcome, Ok(TransactionOutcome::Applied));
}

// ---- conditional transactions ----

#[tokio::test]
async fn transaction_applies_when_the_key_is_unchanged() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;
    put(&store, "streams/orders", b"v1").await.expect("put");
    let read_revision = revision_of(&store, "streams/orders").await;

    let outcome = store
        .transact(
            vec![Condition::HasRevision {
                key: "streams/orders".to_owned(),
                revision: read_revision,
            }],
            vec![put_mutation("streams/orders", b"v2")],
        )
        .await;

    assert_eq!(outcome, Ok(TransactionOutcome::Applied));
}

#[tokio::test]
async fn transaction_fails_when_someone_wrote_in_between() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;
    put(&store, "streams/orders", b"v1").await.expect("put");
    let read_revision = revision_of(&store, "streams/orders").await;
    put(&store, "streams/orders", b"other").await.expect("put");

    let outcome = store
        .transact(
            vec![Condition::HasRevision {
                key: "streams/orders".to_owned(),
                revision: read_revision,
            }],
            vec![put_mutation("streams/orders", b"v2")],
        )
        .await;

    assert_eq!(outcome, Ok(TransactionOutcome::ConditionFailed));
}

#[tokio::test]
async fn failed_transaction_changes_nothing() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;
    put(&store, "streams/orders", b"v1").await.expect("put");

    store
        .transact(
            vec![Condition::IsAbsent {
                key: "streams/orders".to_owned(),
            }],
            vec![
                put_mutation("streams/orders", b"v2"),
                put_mutation("streams/other", b"v2"),
            ],
        )
        .await
        .expect("transact");

    assert_eq!(
        store.list("streams/").await.expect("list").len(),
        1,
        "the failed transaction must not create streams/other"
    );
}

#[tokio::test]
async fn absence_condition_holds_for_a_missing_key() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;

    let outcome = store
        .transact(
            vec![Condition::IsAbsent {
                key: "streams/orders".to_owned(),
            }],
            vec![put_mutation("streams/orders", b"v1")],
        )
        .await;

    assert_eq!(outcome, Ok(TransactionOutcome::Applied));
}

#[tokio::test]
async fn absence_condition_fails_for_an_existing_key() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;
    put(&store, "streams/orders", b"v1").await.expect("put");

    let outcome = store
        .transact(
            vec![Condition::IsAbsent {
                key: "streams/orders".to_owned(),
            }],
            vec![put_mutation("streams/orders", b"v2")],
        )
        .await;

    assert_eq!(outcome, Ok(TransactionOutcome::ConditionFailed));
}

/// A key that was deleted is absent again, although etcd still remembers
/// revisions for it.
#[tokio::test]
async fn absence_condition_holds_for_a_deleted_key() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;
    put(&store, "streams/orders", b"v1").await.expect("put");
    store
        .transact(Vec::new(), vec![delete_mutation("streams/orders")])
        .await
        .expect("delete");

    let outcome = store
        .transact(
            vec![Condition::IsAbsent {
                key: "streams/orders".to_owned(),
            }],
            vec![put_mutation("streams/orders", b"v2")],
        )
        .await;

    assert_eq!(outcome, Ok(TransactionOutcome::Applied));
}

#[tokio::test]
async fn revision_condition_fails_for_a_missing_key() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;

    let outcome = store
        .transact(
            vec![Condition::HasRevision {
                key: "streams/orders".to_owned(),
                revision: Revision::new(1),
            }],
            vec![put_mutation("streams/orders", b"v1")],
        )
        .await;

    assert_eq!(outcome, Ok(TransactionOutcome::ConditionFailed));
}

/// etcd reports revision zero for a key that does not exist, so a naive
/// comparison against zero would hold here.
#[tokio::test]
async fn revision_zero_condition_fails_for_a_missing_key() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;

    let outcome = store
        .transact(
            vec![Condition::HasRevision {
                key: "streams/orders".to_owned(),
                revision: Revision::new(0),
            }],
            vec![put_mutation("streams/orders", b"v1")],
        )
        .await;

    assert_eq!(outcome, Ok(TransactionOutcome::ConditionFailed));
}

/// A revision too large for etcd to have assigned must fail the condition,
/// not the request.
#[tokio::test]
async fn revision_etcd_cannot_assign_fails_the_condition() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;
    put(&store, "streams/orders", b"v1").await.expect("put");

    let outcome = store
        .transact(
            vec![Condition::HasRevision {
                key: "streams/orders".to_owned(),
                revision: Revision::new(u64::MAX),
            }],
            vec![put_mutation("streams/orders", b"v2")],
        )
        .await;

    assert_eq!(outcome, Ok(TransactionOutcome::ConditionFailed));
}

#[tokio::test]
async fn transaction_needs_every_condition_to_hold() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;
    put(&store, "streams/orders", b"v1").await.expect("put");

    let outcome = store
        .transact(
            vec![
                Condition::IsAbsent {
                    key: "streams/payments".to_owned(),
                },
                Condition::IsAbsent {
                    key: "streams/orders".to_owned(),
                },
            ],
            vec![put_mutation("streams/payments", b"v1")],
        )
        .await;

    assert_eq!(outcome, Ok(TransactionOutcome::ConditionFailed));
}

#[tokio::test]
async fn transaction_writes_all_its_keys_at_one_revision() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;

    store
        .transact(
            Vec::new(),
            vec![put_mutation("a", b"1"), put_mutation("b", b"2")],
        )
        .await
        .expect("transact");

    assert_eq!(
        revision_of(&store, "a").await,
        revision_of(&store, "b").await
    );
}

/// Two writers that both saw the key absent: etcd must let exactly one of
/// them create it. This is the race every epoch claim depends on.
#[tokio::test]
async fn only_one_of_two_racing_creations_applies() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;
    let create = |value: &'static [u8]| {
        store.transact(
            vec![Condition::IsAbsent {
                key: "streams/orders".to_owned(),
            }],
            vec![put_mutation("streams/orders", value)],
        )
    };

    let (first, second) = tokio::join!(create(b"first"), create(b"second"));

    let mut applied_count = 0;
    for outcome in [first.expect("transact"), second.expect("transact")] {
        if outcome == TransactionOutcome::Applied {
            applied_count += 1;
        }
    }
    assert_eq!(applied_count, 1);
}

// ---- listings ----

#[tokio::test]
async fn list_returns_only_the_prefix_sorted_by_key() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;
    for key in ["streams/b", "replicas/x", "streams/a", "streamsX"] {
        put(&store, key, b"v").await.expect("put");
    }

    let listed = store.list("streams/").await.expect("list");

    let listed_keys: Vec<String> = listed.into_iter().map(|(key, _stored)| key).collect();
    assert_eq!(listed_keys, ["streams/a", "streams/b"]);
}

#[tokio::test]
async fn list_returns_each_value_with_its_revision() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;
    put(&store, "streams/orders", b"v1").await.expect("put");
    let read = store.get("streams/orders").await.expect("get");
    let read = read.expect("the key exists");

    let listed = store.list("streams/").await.expect("list");

    assert_eq!(listed, [("streams/orders".to_owned(), read)]);
}

#[tokio::test]
async fn list_of_a_prefix_without_keys_is_empty() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;

    let listed = store.list("streams/").await;

    assert_eq!(listed, Ok(Vec::new()));
}

/// A listing is one response. The client's default limit for a response is
/// 4 MiB, which a few hundred streams with long segment lists would pass.
#[tokio::test]
async fn listing_larger_than_the_default_response_limit_is_read() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;
    for value_number in 0..LARGE_VALUE_COUNT {
        let mutation = Mutation::Put {
            key: format!("streams/{value_number}"),
            value: Bytes::from(vec![b'x'; LARGE_VALUE_BYTES]),
        };
        store
            .transact(Vec::new(), vec![mutation])
            .await
            .expect("put");
    }

    let listed = store.list("streams/").await.expect("list");

    assert_eq!(listed.len(), LARGE_VALUE_COUNT);
}

// ---- key prefixes ----

#[tokio::test]
async fn stores_with_different_prefixes_do_not_read_each_other() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;
    let other_store = etcd.store_in_fresh_namespace().await;
    put(&store, "streams/orders", b"v1").await.expect("put");

    let stored = other_store.get("streams/orders").await;

    assert_eq!(stored, Ok(None));
}

#[tokio::test]
async fn stores_with_different_prefixes_do_not_list_each_other() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;
    let other_store = etcd.store_in_fresh_namespace().await;
    put(&store, "streams/orders", b"v1").await.expect("put");

    let listed = other_store.list("").await;

    assert_eq!(listed, Ok(Vec::new()));
}

/// The absence of a key is judged inside the store's own prefix: the same
/// key under another prefix does not count.
#[tokio::test]
async fn conditions_do_not_see_keys_of_another_prefix() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;
    let other_store = etcd.store_in_fresh_namespace().await;
    put(&store, "streams/orders", b"v1").await.expect("put");

    let outcome = other_store
        .transact(
            vec![Condition::IsAbsent {
                key: "streams/orders".to_owned(),
            }],
            vec![put_mutation("streams/orders", b"v2")],
        )
        .await;

    assert_eq!(outcome, Ok(TransactionOutcome::Applied));
}

/// Two processes configured alike are two stores with one prefix, and they
/// must see one set of data.
#[tokio::test]
async fn stores_with_the_same_prefix_share_their_data() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in("/shared-by-two-stores/").await;
    let other_store = etcd.store_in("/shared-by-two-stores/").await;
    put(&store, "streams/orders", b"v1").await.expect("put");

    let stored = other_store.get("streams/orders").await.expect("get");

    assert_eq!(
        stored.map(|stored| stored.value),
        Some(Bytes::from_static(b"v1"))
    );
}

// ---- what etcd refuses ----

/// The in-memory store applies such a transaction in order; etcd refuses
/// it. Nothing in bitacora writes a key twice in one transaction, and this
/// test pins what would happen if something did: a clear failure, not a
/// silent difference between the two backends.
#[tokio::test]
async fn transaction_that_writes_one_key_twice_is_refused() {
    let Some(etcd) = shared_etcd().await else {
        return;
    };
    let store = etcd.store_in_fresh_namespace().await;

    let outcome = store
        .transact(
            Vec::new(),
            vec![
                put_mutation("streams/orders", b"v1"),
                put_mutation("streams/orders", b"v2"),
            ],
        )
        .await;

    assert!(outcome.is_err());
}
