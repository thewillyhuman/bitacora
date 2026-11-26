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
//! - One private function per rule, named after the rule like a test. The first ten are about objects
//!   stored and read whole, the rest about objects handed over piece by piece.
//! - `store_object`, `start_upload`, `write_pieces`, `complete`: the steps the rules share, which panic
//!   when the store refuses what a rule needs before it can check anything.
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

use crate::store::{ArtifactError, ArtifactStore, ObjectInfo, ObjectUpload};

/// The size of the object that the rule about several parts uploads: 20 MiB. A store that sends an upload
/// to its service in parts must send several of them for the rule to mean anything, and S3 allows no part
/// but the last to be smaller than 5 MiB.
const LARGE_OBJECT_BYTES: usize = 20 * 1024 * 1024;

/// The sizes of the pieces the large object is handed over in, in turn: a little over 1 MiB, a little
/// under 3 MiB and a few bytes. None divides a part of a whole number of mebibytes.
const LARGE_PIECE_BYTES: [usize; 3] = [1024 * 1024 + 7, 3 * 1024 * 1024 - 5, 13];

/// The object that the two range rules read. Its byte at offset `n` is the ASCII digit `n`.
const RANGED_OBJECT_KEY: &str = "contract/ranged";

/// The contents of [`RANGED_OBJECT_KEY`]: ten bytes, so offsets and expected results can be read off.
const RANGED_OBJECT_CONTENTS: &[u8] = b"0123456789";

/// The size of [`RANGED_OBJECT_CONTENTS`] in bytes, as a store must report it in a range error.
const RANGED_OBJECT_BYTES: u64 = 10;

/// Runs the artifact store contract against a store.
///
/// Stores, reads, lists and deletes a handful of small objects below the key prefix `contract/`, and
/// uploads a few more piece by piece, one of them of 20 MiB. Asserts after each step that the store
/// behaved as [`ArtifactStore`] and [`ObjectUpload`] document. Does the store's I/O; leaves its objects
/// behind.
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
    object_written_in_pieces_is_read_back_whole(artifact_store).await;
    object_is_not_there_before_its_upload_is_completed(artifact_store).await;
    aborted_upload_leaves_no_object(artifact_store).await;
    abandoned_upload_leaves_no_object(artifact_store).await;
    upload_replaces_an_existing_object_only_when_completed(artifact_store).await;
    upload_without_pieces_stores_an_empty_object(artifact_store).await;
    upload_to_an_invalid_key_is_refused(artifact_store).await;
    uploads_at_the_same_time_do_not_mix(artifact_store).await;
    object_of_several_parts_is_stored_whole(artifact_store).await;
}

/// Stores `contents` under `object_key`, panicking if the store refuses: every rule needs its objects in
/// place before it can check anything.
async fn store_object(artifact_store: &dyn ArtifactStore, object_key: &str, contents: &'static [u8]) {
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
    assert_eq!(range_read, not_found, "a range of a missing object must be NotFound");
}

/// Rule: storing under an existing key replaces the whole object. The second object is shorter than the
/// first on purpose: a backend that overwrote in place would leave the first one's tail behind.
async fn put_replaces_an_existing_object(artifact_store: &dyn ArtifactStore) {
    store_object(artifact_store, "contract/replaced", b"first version, longer").await;
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

    let refused = artifact_store.get_range(RANGED_OBJECT_KEY, one_byte_past_the_end).await;

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
        let refused = artifact_store.put_object(invalid_key, Bytes::from_static(b"x")).await;

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

    assert_eq!(read_back, Ok(Bytes::new()), "an empty object must exist and be empty");
}

/// Starts an upload to `object_key`, panicking if the store refuses: the upload rules need one in hand
/// before they can check anything.
async fn start_upload(artifact_store: &dyn ArtifactStore, object_key: &str) -> Box<dyn ObjectUpload> {
    artifact_store
        .start_upload(object_key)
        .await
        .expect("the store under test starts an upload to a valid key")
}

/// Hands `pieces` to `upload` in order, panicking if the store refuses one.
async fn write_pieces(upload: &mut dyn ObjectUpload, pieces: &[&'static [u8]]) {
    for piece in pieces {
        upload
            .write(Bytes::from_static(piece))
            .await
            .expect("the store under test takes a piece of an upload");
    }
}

/// Completes `upload`, panicking if the store refuses.
async fn complete(upload: Box<dyn ObjectUpload>) {
    upload
        .complete()
        .await
        .expect("the store under test completes an upload");
}

/// Rule: an object handed over in pieces is the pieces laid end to end, whatever their sizes. A piece of
/// one byte, a piece of none and a longer one are all pieces.
async fn object_written_in_pieces_is_read_back_whole(artifact_store: &dyn ArtifactStore) {
    let mut upload = start_upload(artifact_store, "contract/upload/pieces").await;
    write_pieces(upload.as_mut(), &[b"a", b"", b"second piece, ", b"third"]).await;
    complete(upload).await;

    let read_back = artifact_store.get_object("contract/upload/pieces").await;
    let listed = artifact_store.list_prefix("contract/upload/pieces").await;

    assert_eq!(
        read_back,
        Ok(Bytes::from_static(b"asecond piece, third")),
        "an object written in pieces must be the pieces end to end"
    );
    assert_eq!(
        listed,
        Ok(vec![object_info("contract/upload/pieces", 20)]),
        "an object written in pieces must be listed with its whole size"
    );
}

/// Rule: an object does not exist until its upload is completed. A reader must never find the beginning
/// of an object, and a listing must never name one that is still being written.
async fn object_is_not_there_before_its_upload_is_completed(artifact_store: &dyn ArtifactStore) {
    let mut upload = start_upload(artifact_store, "contract/upload/unfinished").await;
    write_pieces(upload.as_mut(), &[b"the beginning"]).await;

    let read_while_uploading = artifact_store.get_object("contract/upload/unfinished").await;
    let listed_while_uploading = artifact_store.list_prefix("contract/upload/unfinished").await;
    complete(upload).await;
    let read_after_completion = artifact_store.get_object("contract/upload/unfinished").await;

    assert!(
        matches!(read_while_uploading, Err(ArtifactError::NotFound { .. })),
        "an object must not be readable before its upload is completed"
    );
    assert_eq!(
        listed_while_uploading,
        Ok(Vec::new()),
        "an object must not be listed before its upload is completed"
    );
    assert_eq!(
        read_after_completion,
        Ok(Bytes::from_static(b"the beginning")),
        "a completed upload must be readable"
    );
}

/// Rule: an upload that is aborted leaves no object under its key.
async fn aborted_upload_leaves_no_object(artifact_store: &dyn ArtifactStore) {
    let mut upload = start_upload(artifact_store, "contract/upload/aborted").await;
    write_pieces(upload.as_mut(), &[b"never to be seen"]).await;

    let aborted = upload.abort().await;
    let read_after_abort = artifact_store.get_object("contract/upload/aborted").await;
    let listed_after_abort = artifact_store.list_prefix("contract/upload/aborted").await;

    assert_eq!(aborted, Ok(()), "aborting an upload must succeed");
    assert!(
        matches!(read_after_abort, Err(ArtifactError::NotFound { .. })),
        "an aborted upload must leave no object"
    );
    assert_eq!(
        listed_after_abort,
        Ok(Vec::new()),
        "an aborted upload must not be listed"
    );
}

/// Rule: an upload that is dropped, neither completed nor aborted, leaves no object under its key either.
/// That is what a caller that fails half way, or is cancelled, does.
async fn abandoned_upload_leaves_no_object(artifact_store: &dyn ArtifactStore) {
    let mut upload = start_upload(artifact_store, "contract/upload/abandoned").await;
    write_pieces(upload.as_mut(), &[b"never to be seen"]).await;

    drop(upload);
    let read_after_drop = artifact_store.get_object("contract/upload/abandoned").await;

    assert!(
        matches!(read_after_drop, Err(ArtifactError::NotFound { .. })),
        "an abandoned upload must leave no object"
    );
}

/// Rule: an upload to an existing key leaves the object alone until it is completed, and replaces the
/// whole object then. The new object is shorter than the old one on purpose, as in the rule for `put`.
async fn upload_replaces_an_existing_object_only_when_completed(artifact_store: &dyn ArtifactStore) {
    store_object(artifact_store, "contract/upload/replaced", b"first version, longer").await;
    let mut upload = start_upload(artifact_store, "contract/upload/replaced").await;
    write_pieces(upload.as_mut(), &[b"sec", b"ond"]).await;

    let read_while_uploading = artifact_store.get_object("contract/upload/replaced").await;
    complete(upload).await;
    let read_after_completion = artifact_store.get_object("contract/upload/replaced").await;

    assert_eq!(
        read_while_uploading,
        Ok(Bytes::from_static(b"first version, longer")),
        "an upload must not disturb the object under its key before it is completed"
    );
    assert_eq!(
        read_after_completion,
        Ok(Bytes::from_static(b"second")),
        "a completed upload must replace the whole object"
    );
}

/// Rule: an upload that was handed nothing stores an object of no bytes, which is a real object.
async fn upload_without_pieces_stores_an_empty_object(artifact_store: &dyn ArtifactStore) {
    let upload = start_upload(artifact_store, "contract/upload/empty").await;
    complete(upload).await;

    let read_back = artifact_store.get_object("contract/upload/empty").await;

    assert_eq!(
        read_back,
        Ok(Bytes::new()),
        "an upload without pieces must store an empty object"
    );
}

/// Rule: an upload to a key that is not a plain relative path is refused when it is started, before
/// anything is handed over.
async fn upload_to_an_invalid_key_is_refused(artifact_store: &dyn ArtifactStore) {
    for invalid_key in ["", "/absolute", "a/../escape", "trailing/"] {
        let refused = artifact_store.start_upload(invalid_key).await;

        assert!(
            matches!(refused, Err(ArtifactError::InvalidKey { .. })),
            "an upload to key {invalid_key:?} must be refused"
        );
    }
}

/// Rule: two uploads that are open at once are two objects. The pieces are handed over in turns, so a
/// store that kept them in one place would mix them.
async fn uploads_at_the_same_time_do_not_mix(artifact_store: &dyn ArtifactStore) {
    let mut first_upload = start_upload(artifact_store, "contract/upload/first-of-two").await;
    let mut second_upload = start_upload(artifact_store, "contract/upload/second-of-two").await;
    write_pieces(first_upload.as_mut(), &[b"one "]).await;
    write_pieces(second_upload.as_mut(), &[b"two "]).await;
    write_pieces(first_upload.as_mut(), &[b"one"]).await;
    write_pieces(second_upload.as_mut(), &[b"two"]).await;
    complete(second_upload).await;
    complete(first_upload).await;

    let first_read = artifact_store.get_object("contract/upload/first-of-two").await;
    let second_read = artifact_store.get_object("contract/upload/second-of-two").await;

    assert_eq!(
        first_read,
        Ok(Bytes::from_static(b"one one")),
        "an upload must hold only its own pieces"
    );
    assert_eq!(
        second_read,
        Ok(Bytes::from_static(b"two two")),
        "an upload must hold only its own pieces"
    );
}

/// The contents of the large object of the contract: byte `n` is `n` modulo 251. The modulus is a prime
/// that divides no part or piece size, so a part stored at the wrong place does not look right.
fn large_object_contents() -> Bytes {
    let mut contents = Vec::with_capacity(LARGE_OBJECT_BYTES);
    for offset in 0..LARGE_OBJECT_BYTES {
        contents.push(u8::try_from(offset % 251).expect("a remainder below 251 fits in a byte"));
    }
    Bytes::from(contents)
}

/// Rule: an object larger than a store's parts, handed over in pieces that are neither equal nor a
/// divisor of a part, is stored whole and in order. A store that sends an upload to its service in parts
/// sends several here, the last one shorter than the others.
async fn object_of_several_parts_is_stored_whole(artifact_store: &dyn ArtifactStore) {
    let contents = large_object_contents();
    let mut upload = start_upload(artifact_store, "contract/upload/large").await;
    let mut handed_over = 0;
    let mut piece_number = 0;
    while handed_over < contents.len() {
        let piece_bytes = LARGE_PIECE_BYTES[piece_number % LARGE_PIECE_BYTES.len()];
        let piece_end = contents.len().min(handed_over + piece_bytes);
        upload
            .write(contents.slice(handed_over..piece_end))
            .await
            .expect("the store under test takes a piece of an upload");
        handed_over = piece_end;
        piece_number += 1;
    }
    complete(upload).await;

    let read_back = artifact_store.get_object("contract/upload/large").await;

    // Not `assert_eq!`: a failure would print twenty megabytes twice.
    assert!(
        read_back == Ok(contents),
        "an object of several parts must be read back whole and in order"
    );
}
