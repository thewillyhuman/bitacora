//! Tests of the disk on a local directory.
//!
//! # What is covered
//!
//! - Every operation of [`Disk`] and [`DiskFile`] on real files: creation with initial content and a
//!   preallocated size, opening, reading and writing, atomic replacement, removal, renaming, listing and
//!   free space.
//! - What is specific to real files: no scratch file is left behind or listed, content survives reopening
//!   the directory, and a path that would leave the data directory is refused.
//!
//! What a crash does to unflushed data cannot be tested on a real file system; that is what the in-memory
//! disk is for.
//!
//! # How the fixtures work
//!
//! Every test gets a temporary directory of its own, removed when the test ends, and opens the disk in
//! its `data` subdirectory with `disk_in`. Opening `data` twice in one test stands for a restart.

use tempfile::TempDir;

use super::*;

/// The path of the one file most tests use.
const PATH: &str = "7/3.seg";

/// A temporary directory that is removed when the test ends.
fn temporary_directory() -> TempDir {
    tempfile::tempdir().expect("the system has a place for temporary files")
}

/// Opens the disk in the `data` subdirectory of `directory`.
async fn disk_in(directory: &TempDir) -> LocalDisk {
    LocalDisk::open(directory.path().join("data"))
        .await
        .expect("a directory can be created below a fresh temporary directory")
}

#[tokio::test]
async fn written_bytes_are_read_back_after_reopening() {
    let directory = temporary_directory();
    let disk = disk_in(&directory).await;
    let mut file = disk.create_file(PATH, Bytes::new(), 0).await.expect("new file");
    file.write_at(3, Bytes::from_static(b"abc")).await.expect("write");
    file.sync_data().await.expect("flush");

    let reopened = disk_in(&directory).await.open_file(PATH).await.expect("open");

    assert_eq!(
        reopened.read_at(0, 64).await.expect("read"),
        Bytes::from_static(b"\0\0\0abc")
    );
}

#[tokio::test]
async fn new_file_starts_with_its_initial_content() {
    let directory = temporary_directory();
    let disk = disk_in(&directory).await;
    disk.create_file(PATH, Bytes::from_static(b"header"), 8)
        .await
        .expect("new file");

    let reopened = disk.open_file(PATH).await.expect("open");

    assert_eq!(
        reopened.read_at(0, 64).await.expect("read"),
        Bytes::from_static(b"header\0\0")
    );
}

#[tokio::test]
async fn renamed_file_is_found_under_its_new_name() {
    let directory = temporary_directory();
    let disk = disk_in(&directory).await;
    disk.write_file_atomically("7/3.seg", Bytes::from_static(b"old copy"))
        .await
        .expect("write");
    disk.write_file_atomically("7/3.seg.repair", Bytes::from_static(b"new copy"))
        .await
        .expect("write");

    disk.rename_file("7/3.seg.repair", "7/3.seg").await.expect("rename");

    assert_eq!(
        disk.read_file("7/3.seg").await.expect("read"),
        Some(Bytes::from_static(b"new copy"))
    );
}

#[tokio::test]
async fn renamed_file_is_gone_from_its_old_name() {
    let directory = temporary_directory();
    let disk = disk_in(&directory).await;
    disk.write_file_atomically("7/3.seg.repair", Bytes::from_static(b"new copy"))
        .await
        .expect("write");

    disk.rename_file("7/3.seg.repair", "7/3.seg").await.expect("rename");

    assert_eq!(disk.read_file("7/3.seg.repair").await.expect("read"), None);
}

#[tokio::test]
async fn rename_of_a_missing_file_fails() {
    let directory = temporary_directory();
    let disk = disk_in(&directory).await;
    disk.write_file_atomically("7/3.seg", Bytes::from_static(b"old copy"))
        .await
        .expect("write");

    let renamed = disk.rename_file("7/3.seg.repair", "7/3.seg").await;

    assert_eq!(renamed.map_err(|error| error.kind()), Err(io::ErrorKind::NotFound));
}

#[tokio::test]
async fn rename_of_a_missing_file_leaves_the_target_alone() {
    let directory = temporary_directory();
    let disk = disk_in(&directory).await;
    disk.write_file_atomically("7/3.seg", Bytes::from_static(b"old copy"))
        .await
        .expect("write");

    let _failed_rename = disk.rename_file("7/3.seg.repair", "7/3.seg").await;

    assert_eq!(
        disk.read_file("7/3.seg").await.expect("read"),
        Some(Bytes::from_static(b"old copy"))
    );
}

#[tokio::test]
async fn creation_leaves_no_scratch_file_behind() {
    let directory = temporary_directory();
    let disk = disk_in(&directory).await;

    disk.create_file(PATH, Bytes::from_static(b"header"), 0)
        .await
        .expect("new file");

    let scratch_path = directory.path().join("data").join("7").join("3.seg.tmp");
    assert!(!scratch_path.exists());
}

#[tokio::test]
async fn new_file_has_its_preallocated_length() {
    let directory = temporary_directory();
    let disk = disk_in(&directory).await;

    let file = disk.create_file(PATH, Bytes::new(), 4096).await.expect("new file");

    assert_eq!(file.length_bytes().await.expect("length"), 4096);
}

#[tokio::test]
async fn creating_an_existing_file_fails() {
    let directory = temporary_directory();
    let disk = disk_in(&directory).await;
    disk.create_file(PATH, Bytes::new(), 0).await.expect("new file");

    let second_attempt = disk.create_file(PATH, Bytes::new(), 0).await;

    assert_eq!(
        second_attempt.err().map(|error| error.kind()),
        Some(io::ErrorKind::AlreadyExists)
    );
}

#[tokio::test]
async fn opening_a_missing_file_fails() {
    let directory = temporary_directory();

    let opened = disk_in(&directory).await.open_file(PATH).await;

    assert_eq!(opened.err().map(|error| error.kind()), Some(io::ErrorKind::NotFound));
}

#[tokio::test]
async fn reading_past_the_end_returns_what_exists() {
    let directory = temporary_directory();
    let disk = disk_in(&directory).await;
    let mut file = disk.create_file(PATH, Bytes::new(), 0).await.expect("new file");
    file.write_at(0, Bytes::from_static(b"abc")).await.expect("write");

    let read = file.read_at(1, 64).await.expect("read");

    assert_eq!(read, Bytes::from_static(b"bc"));
}

#[tokio::test]
async fn atomic_write_replaces_the_previous_content() {
    let directory = temporary_directory();
    let disk = disk_in(&directory).await;
    for contents in [&b"first"[..], &b"second"[..]] {
        disk.write_file_atomically("7/3.state", Bytes::from_static(contents))
            .await
            .expect("write");
    }

    let read = disk.read_file("7/3.state").await.expect("read");

    assert_eq!(read, Some(Bytes::from_static(b"second")));
}

#[tokio::test]
async fn reading_a_missing_small_file_returns_none() {
    let directory = temporary_directory();
    let disk = disk_in(&directory).await;

    let read = disk.read_file("7/3.state").await.expect("read");

    assert_eq!(read, None);
}

#[tokio::test]
async fn files_are_listed_sorted_without_scratch_files() {
    let directory = temporary_directory();
    let disk = disk_in(&directory).await;
    for path in ["7/3.seg", "2/9.seg", "7/3.state.tmp"] {
        disk.create_file(path, Bytes::new(), 0).await.expect("new file");
    }

    let listed = disk.list_files().await.expect("list");

    assert_eq!(listed, ["2/9.seg", "7/3.seg"]);
}

#[tokio::test]
async fn removed_file_is_no_longer_listed() {
    let directory = temporary_directory();
    let disk = disk_in(&directory).await;
    disk.create_file(PATH, Bytes::new(), 0).await.expect("new file");

    disk.remove_file(PATH).await.expect("remove");

    assert_eq!(disk.list_files().await.expect("list"), Vec::<String>::new());
}

#[tokio::test]
async fn removing_a_missing_file_succeeds() {
    let directory = temporary_directory();
    let disk = disk_in(&directory).await;

    let removed = disk.remove_file(PATH).await;

    assert!(removed.is_ok());
}

#[tokio::test]
async fn shrinking_a_file_cuts_its_content() {
    let directory = temporary_directory();
    let disk = disk_in(&directory).await;
    let mut file = disk.create_file(PATH, Bytes::new(), 0).await.expect("new file");
    file.write_at(0, Bytes::from_static(b"keep-drop")).await.expect("write");

    file.set_length(4).await.expect("shrink");

    assert_eq!(file.read_at(0, 64).await.expect("read"), Bytes::from_static(b"keep"));
}

#[tokio::test]
async fn paths_that_escape_the_data_directory_are_refused() {
    let directory = temporary_directory();
    let disk = disk_in(&directory).await;

    for escaping_path in ["../outside", "/etc/passwd", "7/../../outside", ""] {
        let created = disk.create_file(escaping_path, Bytes::new(), 0).await;

        assert_eq!(
            created.err().map(|error| error.kind()),
            Some(io::ErrorKind::InvalidInput),
            "path {escaping_path:?} was accepted"
        );
    }
}

#[tokio::test]
async fn disk_reports_some_capacity() {
    let directory = temporary_directory();

    let space = disk_in(&directory).await.space().await.expect("space");

    assert!(space.capacity_bytes > 0);
}

#[tokio::test]
async fn disk_reports_free_space_within_its_capacity() {
    let directory = temporary_directory();

    let space = disk_in(&directory).await.space().await.expect("space");

    assert!(space.free_bytes <= space.capacity_bytes);
}
