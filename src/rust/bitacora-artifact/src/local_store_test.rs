//! Tests of the artifact store that keeps objects in a local directory.
//!
//! # What is covered
//!
//! - The store honours the contract every backend shares.
//! - What only a store on disk can get wrong: objects survive reopening; a write that was interrupted
//!   leaves nothing readable; a key may look like one of the store's own scratch names; a listing matches
//!   its prefix by characters although it walks directories; and a key that a file system cannot represent
//!   is refused cleanly.
//! - Sharing: one root directory opened several times at once, as the replicas, the controller and the
//!   clients of a cluster on one machine do. Stores leave each other's writes in progress alone, see each
//!   other's objects, can open and put at the same moment, and remove only what a crashed process left
//!   behind long ago.
//! - Uploads: an object on its way is one scratch file and no object; completing or aborting leaves no
//!   scratch file, dropping leaves it as a leftover; a key the file system cannot take is refused at the
//!   start; an upload that lost its file refuses to go on.
//!
//! # How the fixtures work
//!
//! Every test opens a store in a fresh temporary directory. `OpenedStore` keeps that directory alive next
//! to the store, because the directory is deleted when its handle is dropped.
//!
//! An interrupted write is planted by `plant_leftover`: a file in the scratch directory of a store that is
//! not running, which is exactly what a crash between "write the scratch file" and "rename it" leaves
//! behind. Its age is set through modification times, because those are what a store looks at.
//!
//! The tests of things that happen "at the same moment" start many operations before they wait for any.
//! Each operation does its file system work on the runtime's blocking pool, so the operations run in
//! parallel on real threads even though the test's own runtime has one.

use tempfile::TempDir;
use tokio::task::JoinSet;

use super::*;
use crate::contract;

/// How many stores are opened on one root at once: enough for some of them to overlap on any machine.
const STORES_OPENED_AT_ONCE: usize = 32;

/// How many stores put objects on one root at once.
const STORES_PUTTING_AT_ONCE: usize = 8;

/// How many objects each of the stores that put at once stores.
const OBJECTS_PER_PUTTING_STORE: usize = 8;

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

/// Opens the store that lives in `root_directory`, as a restarted process, or another process, would.
async fn open_store_in(root_directory: &TempDir) -> LocalArtifactStore {
    LocalArtifactStore::open(root_directory.path())
        .await
        .expect("a writable temporary directory opens as a store")
}

/// Stores `contents` under `object_key`.
async fn store_object(artifact_store: &LocalArtifactStore, object_key: &str, contents: &'static [u8]) {
    artifact_store
        .put_object(object_key, Bytes::from_static(contents))
        .await
        .expect("a store on a writable directory accepts a valid object");
}

/// Plants a half-written object below `root_directory`, in the scratch directory of a store that is not
/// running: what a process that crashed in the middle of a put leaves behind.
///
/// The file and its directory are made to look last touched `untouched_for` ago. Returns the path of the
/// file.
fn plant_leftover(root_directory: &TempDir, untouched_for: Duration) -> PathBuf {
    let leftover_directory = root_directory.path().join(SCRATCH_DIRECTORY_NAME).join("crashed-store");
    let leftover_file = leftover_directory.join("put-7");
    fs::create_dir_all(&leftover_directory).expect("the leftover's directory is planted");
    fs::write(&leftover_file, b"half an object").expect("the leftover is planted");
    let last_touched = SystemTime::now()
        .checked_sub(untouched_for)
        .expect("the tests age files by days, which the clock can represent");
    // The file first: creating it touched the directory, so the directory is aged last.
    for aged_path in [&leftover_file, &leftover_directory] {
        File::open(aged_path)
            .expect("what was just planted can be opened")
            .set_modified(last_touched)
            .expect("the test owns what it planted and may change its modification time");
    }
    leftover_file
}

/// Opens a store of its own on `root_path` and puts [`OBJECTS_PER_PUTTING_STORE`] objects through it, under
/// keys that name `store_number`.
async fn put_objects_through_a_new_store(root_path: PathBuf, store_number: usize) {
    let artifact_store = LocalArtifactStore::open(root_path)
        .await
        .expect("a writable temporary directory opens as a store");
    for object_number in 0..OBJECTS_PER_PUTTING_STORE {
        let object_key = format!("stores/{store_number}/objects/{object_number}");
        artifact_store
            .put_object(&object_key, Bytes::from_static(b"an object"))
            .await
            .expect("a store on a writable directory accepts a valid object");
    }
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
async fn interrupted_write_is_never_listed() {
    let opened = open_store().await;
    plant_leftover(&opened.root_directory, Duration::ZERO);

    let listed = opened.artifact_store.list_prefix("").await;

    assert_eq!(listed, Ok(Vec::new()));
}

#[tokio::test]
async fn leftover_of_a_crashed_process_is_removed_once_it_is_stale() {
    let opened = open_store().await;
    let leftover_file = plant_leftover(&opened.root_directory, STALE_SCRATCH_AGE * 2);
    drop(opened.artifact_store);

    let _reopened = open_store_in(&opened.root_directory).await;

    assert!(!leftover_file.exists());
}

/// What looks like a leftover may be the work of a store that is open in another process right now.
#[tokio::test]
async fn recent_scratch_of_another_store_is_left_alone() {
    let opened = open_store().await;
    let write_in_progress = plant_leftover(&opened.root_directory, Duration::ZERO);

    let _second_store = open_store_in(&opened.root_directory).await;

    assert!(write_in_progress.exists());
}

/// Opening a store used to wipe the scratch directory, and with it the half-written objects of every other
/// store open on the same root.
#[tokio::test]
async fn opening_a_second_store_does_not_disturb_the_scratch_of_the_first() {
    let opened = open_store().await;
    let write_in_progress = opened
        .artifact_store
        .directories
        .scratch_directory
        .join("put-in-progress");
    fs::write(&write_in_progress, b"half an object").expect("the write in progress is planted");

    let _second_store = open_store_in(&opened.root_directory).await;

    assert!(write_in_progress.exists());
}

#[tokio::test]
async fn two_stores_on_one_root_build_their_objects_in_different_places() {
    let opened = open_store().await;

    let second_store = open_store_in(&opened.root_directory).await;

    assert_ne!(
        opened.artifact_store.directories.scratch_directory,
        second_store.directories.scratch_directory
    );
}

#[tokio::test]
async fn object_put_through_one_store_is_read_through_another_on_the_same_root() {
    let opened = open_store().await;
    let second_store = open_store_in(&opened.root_directory).await;

    store_object(&opened.artifact_store, "streams/1/a", b"shared").await;

    assert_eq!(
        second_store.get_object("streams/1/a").await,
        Ok(Bytes::from_static(b"shared"))
    );
}

/// The replicas, the controller and the clients of a cluster on one machine all open the same directory,
/// and at the same moment when the cluster starts.
#[tokio::test]
async fn stores_opened_on_one_root_at_the_same_moment_all_open() {
    let root_directory = TempDir::new().expect("the test can create a temporary directory");
    let mut opening = JoinSet::new();
    for _store_number in 0..STORES_OPENED_AT_ONCE {
        opening.spawn(LocalArtifactStore::open(root_directory.path().to_owned()));
    }

    let mut failed_opens = Vec::new();
    while let Some(joined) = opening.join_next().await {
        let opened = joined.expect("opening a store does not panic");
        if let Err(artifact_error) = opened {
            failed_opens.push(artifact_error);
        }
    }

    assert_eq!(failed_opens, Vec::new());
}

/// Two stores that numbered their scratch files alike used to build two objects in one file.
#[tokio::test]
async fn stores_on_one_root_putting_at_the_same_moment_lose_nothing() {
    let root_directory = TempDir::new().expect("the test can create a temporary directory");
    let mut putting = JoinSet::new();
    for store_number in 0..STORES_PUTTING_AT_ONCE {
        let root_path = root_directory.path().to_owned();
        putting.spawn(put_objects_through_a_new_store(root_path, store_number));
    }
    while let Some(joined) = putting.join_next().await {
        joined.expect("every store opens and every put is accepted");
    }

    let reader = open_store_in(&root_directory).await;
    let stored_objects = reader
        .list_prefix("stores/")
        .await
        .expect("the directory can be listed");

    assert_eq!(stored_objects.len(), STORES_PUTTING_AT_ONCE * OBJECTS_PER_PUTTING_STORE);
}

/// A store that wrote nothing for a day looks like a leftover to another store that opens the root, which
/// removes its scratch directory. The idle store must still be able to write.
#[tokio::test]
async fn store_whose_scratch_directory_was_removed_can_still_put() {
    let opened = open_store().await;
    fs::remove_dir_all(&opened.artifact_store.directories.scratch_directory)
        .expect("the scratch directory is removed, as another store would remove a stale one");

    let stored = opened
        .artifact_store
        .put_object("streams/1/a", Bytes::from_static(b"kept"))
        .await;

    assert_eq!(stored, Ok(()));
}

/// The scratch directory is outside the objects directory, so a key spelled like a scratch path is an
/// ordinary key, and a later write, which builds its object in the scratch directory, does not touch it.
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

    let refused = opened.artifact_store.put_object("a", Bytes::from_static(b"y")).await;

    assert!(matches!(refused, Err(ArtifactError::Unavailable { .. })));
}

/// An upload to `streams/7/segments/3` that was started on `artifact_store` and handed one piece.
async fn upload_with_one_piece(artifact_store: &LocalArtifactStore) -> Box<dyn ObjectUpload> {
    let mut upload = artifact_store
        .start_upload("streams/7/segments/3")
        .await
        .expect("a store on a writable directory starts an upload");
    upload
        .write(Bytes::from_static(b"the first piece"))
        .await
        .expect("a store on a writable directory takes a piece");
    upload
}

/// How many files the store has in its own scratch directory right now.
fn scratch_file_count(artifact_store: &LocalArtifactStore) -> usize {
    let scratch_directory = &artifact_store.directories.scratch_directory;
    fs::read_dir(scratch_directory)
        .expect("the store's scratch directory can be read")
        .count()
}

/// An upload keeps what it was handed on disk, not in memory: that is the point of it.
#[tokio::test]
async fn upload_in_progress_keeps_its_pieces_in_one_scratch_file() {
    let opened = open_store().await;

    let _upload = upload_with_one_piece(&opened.artifact_store).await;

    assert_eq!(scratch_file_count(&opened.artifact_store), 1);
}

#[tokio::test]
async fn completed_upload_leaves_no_scratch_file() {
    let opened = open_store().await;
    let upload = upload_with_one_piece(&opened.artifact_store).await;

    upload
        .complete()
        .await
        .expect("a store on a writable directory completes an upload");

    assert_eq!(scratch_file_count(&opened.artifact_store), 0);
}

#[tokio::test]
async fn aborted_upload_removes_its_scratch_file() {
    let opened = open_store().await;
    let upload = upload_with_one_piece(&opened.artifact_store).await;

    upload
        .abort()
        .await
        .expect("a store on a writable directory aborts an upload");

    assert_eq!(scratch_file_count(&opened.artifact_store), 0);
}

/// Whether the store's scratch directory is empty now, or becomes empty within a few seconds.
///
/// For what a drop removes: a drop cannot wait for the file system, so the removal runs on the blocking
/// pool and is over a moment after the drop returns.
async fn scratch_files_are_soon_gone(artifact_store: &LocalArtifactStore) -> bool {
    for _attempt in 0..5_000 {
        if scratch_file_count(artifact_store) == 0 {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    false
}

/// What happens to an upload whose caller was itself given up on, for example a request that was
/// cancelled: the gigabyte it wrote must not stay on the disk until the store is opened again.
#[tokio::test]
async fn dropped_upload_removes_its_scratch_file() {
    let opened = open_store().await;
    let upload = upload_with_one_piece(&opened.artifact_store).await;

    drop(upload);

    assert!(scratch_files_are_soon_gone(&opened.artifact_store).await);
}

/// An upload can be dropped where no runtime is left to do the removal, for example while a process shuts
/// down. Its scratch file is then a leftover like that of a crashed process, and the drop must be quiet.
#[test]
fn upload_dropped_outside_a_runtime_leaves_its_scratch_file_to_the_removal_of_leftovers() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime without drivers can be built");
    let opened = runtime.block_on(open_store());
    let upload = runtime.block_on(upload_with_one_piece(&opened.artifact_store));
    drop(runtime);

    drop(upload);

    assert_eq!(scratch_file_count(&opened.artifact_store), 1);
}

/// A completion takes the upload with it, so nobody could abort it afterwards: the completion that fails
/// removes the scratch file itself. It fails here because the key has become a directory meanwhile.
#[tokio::test]
async fn completion_that_fails_removes_its_scratch_file() {
    let opened = open_store().await;
    let upload = upload_with_one_piece(&opened.artifact_store).await;
    store_object(&opened.artifact_store, "streams/7/segments/3/below", b"x").await;

    let completed = upload.complete().await;

    assert!(matches!(completed, Err(ArtifactError::Unavailable { .. })));
    assert_eq!(scratch_file_count(&opened.artifact_store), 0);
}

/// The key is tried against the file system when the upload starts, so that a gigabyte is not written
/// before the store finds out that it has no place for it.
#[tokio::test]
async fn upload_to_a_key_below_an_object_is_refused_when_it_starts() {
    let opened = open_store().await;
    store_object(&opened.artifact_store, "a", b"x").await;

    let refused = opened.artifact_store.start_upload("a/b").await;

    assert!(matches!(refused, Err(ArtifactError::Unavailable { .. })));
}

#[tokio::test]
async fn store_whose_scratch_directory_was_removed_can_still_upload() {
    let opened = open_store().await;
    fs::remove_dir_all(&opened.artifact_store.directories.scratch_directory)
        .expect("the scratch directory is removed, as another store would remove a stale one");

    let started = opened.artifact_store.start_upload("streams/1/a").await;

    assert!(started.is_ok(), "{:?}", started.err());
}

#[tokio::test]
async fn object_uploaded_through_one_store_is_read_through_another_on_the_same_root() {
    let opened = open_store().await;
    let second_store = open_store_in(&opened.root_directory).await;
    let upload = upload_with_one_piece(&opened.artifact_store).await;

    upload
        .complete()
        .await
        .expect("a store on a writable directory completes an upload");

    assert_eq!(
        second_store.get_object("streams/7/segments/3").await,
        Ok(Bytes::from_static(b"the first piece"))
    );
}

/// An upload in the state a failed write leaves it in: its scratch file is gone from its hands, and what
/// is in the file nobody knows.
fn upload_that_lost_its_scratch_file(artifact_store: &LocalArtifactStore) -> LocalUpload {
    LocalUpload {
        directories: Arc::clone(&artifact_store.directories),
        object_key: "streams/7/segments/3".to_owned(),
        scratch_path: artifact_store.directories.next_scratch_path(),
        scratch_file: None,
        has_ended: false,
    }
}

#[tokio::test]
async fn upload_that_lost_its_scratch_file_takes_no_more_pieces() {
    let opened = open_store().await;
    let mut upload = upload_that_lost_its_scratch_file(&opened.artifact_store);

    let written = upload.write(Bytes::from_static(b"more")).await;

    assert!(matches!(written, Err(ArtifactError::Unavailable { .. })));
}

/// Above all, an upload whose file is in an unknown state must not make an object of it.
#[tokio::test]
async fn upload_that_lost_its_scratch_file_cannot_be_completed() {
    let opened = open_store().await;
    let upload = Box::new(upload_that_lost_its_scratch_file(&opened.artifact_store));

    let completed = upload.complete().await;

    assert!(matches!(completed, Err(ArtifactError::Unavailable { .. })));
}
