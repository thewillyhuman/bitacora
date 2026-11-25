//! What every artifact store must do, as one reusable check.
//!
//! # Why this file exists
//!
//! The simulator runs against the in-memory store, a developer's cluster against a directory, production
//! against an object store. If those disagree about anything a caller can observe, such as what a range
//! read past the end returns, a listing's order or whether deleting a missing object is an error, then
//! code that passed every simulated fault can still fail on real storage. Describing the behaviour in
//! prose in the trait's documentation does not prevent that; running the same assertions against every
//! backend does.
//!
//! # What it contains
//!
//! - [`check`]: runs every rule against one store.
//! - One private function per rule, named after the rule like a test.
//!
//! # How it works
//!
//! [`check`] is ordinary library code that panics with the broken rule's name when an assertion fails. It
//! is compiled into the crate, not only into its tests, so that a backend in another crate (the S3 backend)
//! calls it from its own tests. Each backend has one test that builds an empty store and hands it over.
//!
//! Every rule writes only below the key prefix `contract/`, and rules do not depend on each other's
//! objects except where a rule says so, so a rule can be read on its own.
//!
//! # Invariants
//!
//! - A rule added here applies to every backend at once. A backend cannot opt out of one.
//! - The check only needs the store to hold nothing below `contract/` when it starts.
//!
//! # See also
//!
//! - `store.rs`: the interface these rules pin down.
//! - `memory_store_test.rs` and `local_store_test.rs`: where the two backends of this crate run the check.

use bytes::Bytes;

use crate::store::{ArtifactError, ArtifactStore, ObjectInfo};

/// The object that the two range rules read. Its byte at offset `n` is the ASCII digit `n`.
const RANGED_OBJECT_KEY: &str = "contract/ranged";

/// The contents of [`RANGED_OBJECT_KEY`]: ten bytes, so offsets and expected results can be read off.
const RANGED_OBJECT_CONTENTS: &[u8] = b"0123456789";

/// The size of [`RANGED_OBJECT_CONTENTS`] in bytes, as a store must report it in a range error.
const RANGED_OBJECT_BYTES: u64 = 10;

/// Runs the artifact store contract against a store.
///
/// Stores, reads, lists and deletes a handful of small objects below the key prefix `contract/` and
/// asserts after each step that the store behaved as [`ArtifactStore`] documents. Does the store's I/O;
/// leaves its objects behind.
///
/// # Arguments
///
/// * `artifact_store` - The store to check. It must hold nothing below `contract/`; a fresh, empty store
///   is the usual choice.
///
/// # Panics
///
/// Panics, with a message naming the broken rule, if the store breaks the contract or fails an operation
/// the contract needs.
///
/// # Examples
///
/// The one test every backend has:
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// use bitacora_artifact::{MemoryArtifactStore, contract};
///
/// contract::check(&MemoryArtifactStore::new()).await;
/// # }
/// ```
pub async fn check(artifact_store: &dyn ArtifactStore) {
    stored_object_is_read_back(artifact_store).await;
    missing_object_is_reported_as_not_found(artifact_store).await;
    put_replaces_an_existing_object(artifact_store).await;
    range_reads_return_exactly_the_bytes_asked_for(artifact_store).await;
    range_outside_the_object_is_refused(artifact_store).await;
    listing_returns_the_prefix_sorted_with_sizes(artifact_store).await;
    deleted_object_is_gone(artifact_store).await;
    deleting_a_missing_object_succeeds(artifact_store).await;
    invalid_keys_are_refused(artifact_store).await;
    empty_object_is_a_real_object(artifact_store).await;
}

/// Stores `contents` under `object_key`, panicking if the store refuses: every rule needs its objects in
/// place before it can check anything.
async fn store_object(
    artifact_store: &dyn ArtifactStore,
    object_key: &str,
    contents: &'static [u8],
) {
    artifact_store
        .put_object(object_key, Bytes::from_static(contents))
        .await
        .expect("the store under test accepts a valid object");
}

/// Rule: what was stored under a key is what a read of that key returns, byte for byte.
async fn stored_object_is_read_back(artifact_store: &dyn ArtifactStore) {
    store_object(artifact_store, "contract/read-back", b"payload").await;

    let read_back = artifact_store.get_object("contract/read-back").await;

    assert_eq!(
        read_back,
        Ok(Bytes::from_static(b"payload")),
        "a stored object must be read back"
    );
}

/// Rule: reading a key nobody wrote is `NotFound`, for a whole read and for a range read alike. A caller
/// must be able to tell "not there" from "could not ask".
async fn missing_object_is_reported_as_not_found(artifact_store: &dyn ArtifactStore) {
    let missing_key = "contract/never-written";
    let not_found = Err(ArtifactError::NotFound {
        object_key: missing_key.to_owned(),
    });

    let whole_read = artifact_store.get_object(missing_key).await;
    let range_read = artifact_store.get_range(missing_key, 0..1).await;

    assert_eq!(whole_read, not_found, "a missing object must be NotFound");
    assert_eq!(
        range_read, not_found,
        "a range of a missing object must be NotFound"
    );
}

/// Rule: storing under an existing key replaces the whole object. The second object is shorter than the
/// first on purpose: a backend that overwrote in place would leave the first one's tail behind.
async fn put_replaces_an_existing_object(artifact_store: &dyn ArtifactStore) {
    store_object(
        artifact_store,
        "contract/replaced",
        b"first version, longer",
    )
    .await;
    store_object(artifact_store, "contract/replaced", b"second").await;

    let read_back = artifact_store.get_object("contract/replaced").await;

    assert_eq!(
        read_back,
        Ok(Bytes::from_static(b"second")),
        "a put must replace the whole object"
    );
}

/// Rule: a range read returns exactly the bytes of the range, and an empty range at the very end of the
/// object is valid and empty. Stores [`RANGED_OBJECT_KEY`], which the next rule reads too.
async fn range_reads_return_exactly_the_bytes_asked_for(artifact_store: &dyn ArtifactStore) {
    store_object(artifact_store, RANGED_OBJECT_KEY, RANGED_OBJECT_CONTENTS).await;

    let middle = artifact_store.get_range(RANGED_OBJECT_KEY, 3..7).await;
    let empty_at_the_end = artifact_store
        .get_range(RANGED_OBJECT_KEY, RANGED_OBJECT_BYTES..RANGED_OBJECT_BYTES)
        .await;

    assert_eq!(
        middle,
        Ok(Bytes::from_static(b"3456")),
        "a range read must return exactly the range"
    );
    assert_eq!(
        empty_at_the_end,
        Ok(Bytes::new()),
        "an empty range at the end of an object is valid"
    );
}

/// Rule: a range that runs past the end of the object is refused, with the object's real size in the
/// error, and is never answered with fewer bytes than were asked for. Reads the object the previous rule
/// stored.
async fn range_outside_the_object_is_refused(artifact_store: &dyn ArtifactStore) {
    let one_byte_past_the_end = 5..RANGED_OBJECT_BYTES + 1;

    let refused = artifact_store
        .get_range(RANGED_OBJECT_KEY, one_byte_past_the_end)
        .await;

    assert!(
        matches!(
            refused,
            Err(ArtifactError::RangeOutOfBounds {
                size_bytes: RANGED_OBJECT_BYTES,
                ..
            })
        ),
        "a range past the end of an object must be refused"
    );
}

/// Rule: a listing returns exactly the objects whose key starts with the prefix, objects below a deeper
/// path included, sorted by key, each with its size. The object `contract/listed-elsewhere` shares the
/// first characters of the prefix but not all of them, and must be left out.
async fn listing_returns_the_prefix_sorted_with_sizes(artifact_store: &dyn ArtifactStore) {
    // Stored out of order, so that a backend returning insertion order is caught.
    store_object(artifact_store, "contract/list/b", b"22").await;
    store_object(artifact_store, "contract/list/a", b"1").await;
    store_object(artifact_store, "contract/list/nested/c", b"333").await;
    store_object(artifact_store, "contract/listed-elsewhere", b"x").await;
    let expected = vec![
        object_info("contract/list/a", 1),
        object_info("contract/list/b", 2),
        object_info("contract/list/nested/c", 3),
    ];

    let listed = artifact_store.list_prefix("contract/list/").await;

    assert_eq!(
        listed,
        Ok(expected),
        "a listing must return exactly the prefix, sorted, with sizes"
    );
}

/// The listing entry of an object stored under `object_key` with `size_bytes` bytes.
fn object_info(object_key: &str, size_bytes: u64) -> ObjectInfo {
    ObjectInfo {
        object_key: object_key.to_owned(),
        size_bytes,
    }
}

/// Rule: after a delete, the object can be neither read nor listed.
async fn deleted_object_is_gone(artifact_store: &dyn ArtifactStore) {
    store_object(artifact_store, "contract/deleted", b"gone soon").await;

    let deleted = artifact_store.delete_object("contract/deleted").await;
    let read_after_delete = artifact_store.get_object("contract/deleted").await;
    let listed_after_delete = artifact_store.list_prefix("contract/deleted").await;

    assert_eq!(deleted, Ok(()), "deleting a stored object must succeed");
    assert!(
        matches!(read_after_delete, Err(ArtifactError::NotFound { .. })),
        "a deleted object must be gone"
    );
    assert_eq!(
        listed_after_delete,
        Ok(Vec::new()),
        "a deleted object must not be listed"
    );
}

/// Rule: deleting a key nobody wrote succeeds. A deletion that failed half way can then simply be
/// repeated.
async fn deleting_a_missing_object_succeeds(artifact_store: &dyn ArtifactStore) {
    let deleted = artifact_store.delete_object("contract/never-written").await;

    assert_eq!(deleted, Ok(()), "deleting a missing object must succeed");
}

/// Rule: a key that is empty, absolute, climbs out of the store or ends in a separator is refused as
/// `InvalidKey` before anything is stored.
async fn invalid_keys_are_refused(artifact_store: &dyn ArtifactStore) {
    for invalid_key in ["", "/absolute", "a/../escape", "trailing/"] {
        let refused = artifact_store
            .put_object(invalid_key, Bytes::from_static(b"x"))
            .await;

        assert!(
            matches!(refused, Err(ArtifactError::InvalidKey { .. })),
            "key {invalid_key:?} must be refused"
        );
    }
}

/// Rule: an object of no bytes exists like any other. A segment sealed empty, or a materializer with
/// nothing to say, must not look like a missing object.
async fn empty_object_is_a_real_object(artifact_store: &dyn ArtifactStore) {
    store_object(artifact_store, "contract/empty", b"").await;

    let read_back = artifact_store.get_object("contract/empty").await;

    assert_eq!(
        read_back,
        Ok(Bytes::new()),
        "an empty object must exist and be empty"
    );
}
