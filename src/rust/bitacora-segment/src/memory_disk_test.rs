//! Tests of the in-memory disk.
//!
//! # What is covered
//!
//! - The [`Disk`] and [`DiskFile`] operations: creation, opening, reading and writing, atomic replacement,
//!   removal, renaming, listing, free space and changing a file's length.
//! - The crash model: what was flushed survives, what was not is lost, and each [`CrashMode`] leaves
//!   exactly what it says.
//! - Tearing at random: it never touches flushed bytes, always leaves visible damage, handles writes past
//!   the flushed length and writes in several places, and draws nothing for files it does not tear.
//! - The other fault switches: failing flushes and silent corruption.
//!
//! # How the fixtures work
//!
//! `file_with` creates the file at [`PATH`] with given content already flushed. The tearing tests call
//! `torn`, which tears one pair of durable and visible contents with the generator of a given seed, and
//! try [`SEEDS_PER_TEARING_TEST`] seeds so that every place a tear can fall is exercised.

use super::*;

/// The path of the one file most tests use.
const PATH: &str = "7/3.seg";

/// How many seeds a test of random tearing tries.
const SEEDS_PER_TEARING_TEST: u64 = 50;

/// Creates the file at [`PATH`] holding `flushed`, already flushed, and returns a handle to it.
async fn file_with(disk: &MemoryDisk, flushed: &'static [u8]) -> Box<dyn DiskFile> {
    let mut file = disk
        .create_file(PATH, Bytes::new(), 0)
        .await
        .expect("the path is free on a new disk");
    file.write_at(0, Bytes::from_static(flushed))
        .await
        .expect("a memory disk takes every write");
    file.sync_data().await.expect("flushes are not failing");
    file
}

/// Everything the file behind `file` holds, as a reader sees it.
async fn content_of(file: &dyn DiskFile) -> Bytes {
    file.read_at(0, 64).await.expect("the file exists")
}

/// What a torn write leaves of one file with the given contents, for the generator of `seed`.
fn torn(durable: &[u8], visible: &[u8], seed: u64) -> Vec<u8> {
    tear_unflushed_writes(durable, visible, &mut DeterministicRandom::from_seed(seed))
}

/// A crash mode that tears files ending in `.seg` with the generator of `seed`.
fn tear_segment_files(seed: u64) -> CrashMode {
    CrashMode::TearUnflushedWrites {
        random: DeterministicRandom::from_seed(seed),
        path_suffix: ".seg".to_owned(),
    }
}

#[tokio::test]
async fn new_file_is_zero_filled_to_its_preallocated_size() {
    let disk = MemoryDisk::new();

    let file = disk.create_file(PATH, Bytes::new(), 4).await.expect("new file");

    assert_eq!(content_of(file.as_ref()).await, Bytes::from_static(&[0; 4]));
}

#[tokio::test]
async fn new_file_starts_with_its_initial_content_even_after_a_crash() {
    let disk = MemoryDisk::new();
    let file = disk
        .create_file(PATH, Bytes::from_static(b"header"), 8)
        .await
        .expect("new file");

    disk.crash();

    assert_eq!(content_of(file.as_ref()).await, Bytes::from_static(b"header\0\0"));
}

#[tokio::test]
async fn creation_does_not_depend_on_a_flush() {
    let disk = MemoryDisk::new();
    disk.fail_flushes(true);

    let created = disk.create_file(PATH, Bytes::from_static(b"header"), 0).await;

    assert!(created.is_ok());
}

#[tokio::test]
async fn creating_an_existing_file_fails() {
    let disk = MemoryDisk::new();
    disk.create_file(PATH, Bytes::new(), 0).await.expect("new file");

    let second_attempt = disk.create_file(PATH, Bytes::new(), 0).await;

    assert_eq!(
        second_attempt.err().map(|error| error.kind()),
        Some(io::ErrorKind::AlreadyExists)
    );
}

#[tokio::test]
async fn opening_a_missing_file_fails() {
    let disk = MemoryDisk::new();

    let opened = disk.open_file(PATH).await;

    assert_eq!(opened.err().map(|error| error.kind()), Some(io::ErrorKind::NotFound));
}

#[tokio::test]
async fn opened_file_holds_what_was_written_through_another_handle() {
    let disk = MemoryDisk::new();
    let _first_handle = file_with(&disk, b"content").await;

    let second_handle = disk.open_file(PATH).await.expect("the file exists");

    assert_eq!(content_of(second_handle.as_ref()).await, Bytes::from_static(b"content"));
}

#[tokio::test]
async fn written_bytes_are_visible_before_a_flush() {
    let disk = MemoryDisk::new();
    let mut file = disk.create_file(PATH, Bytes::new(), 0).await.expect("new file");

    file.write_at(2, Bytes::from_static(b"ab")).await.expect("write");

    assert_eq!(content_of(file.as_ref()).await, Bytes::from_static(b"\0\0ab"));
}

#[tokio::test]
async fn read_past_the_end_of_a_file_is_empty() {
    let disk = MemoryDisk::new();
    let file = file_with(&disk, b"content").await;

    let past_the_end = file.read_at(100, 8).await.expect("read");

    assert_eq!(past_the_end, Bytes::new());
}

#[tokio::test]
async fn file_reports_the_length_of_what_was_written() {
    let disk = MemoryDisk::new();
    let file = file_with(&disk, b"content").await;

    let length_bytes = file.length_bytes().await.expect("the file exists");

    assert_eq!(length_bytes, 7);
}

#[tokio::test]
async fn crash_drops_bytes_that_were_not_flushed() {
    let disk = MemoryDisk::new();
    let mut file = file_with(&disk, b"flushed").await;
    file.write_at(7, Bytes::from_static(b"-lost")).await.expect("write");

    disk.crash();

    assert_eq!(content_of(file.as_ref()).await, Bytes::from_static(b"flushed"));
}

#[tokio::test]
async fn crash_keeps_bytes_that_were_flushed() {
    let disk = MemoryDisk::new();
    let mut file = file_with(&disk, b"first").await;
    file.write_at(5, Bytes::from_static(b"-second")).await.expect("write");
    file.sync_data().await.expect("flush");

    disk.crash();

    assert_eq!(content_of(file.as_ref()).await, Bytes::from_static(b"first-second"));
}

#[tokio::test]
async fn crash_can_leave_a_torn_tail_behind() {
    let disk = MemoryDisk::new();
    let mut file = file_with(&disk, b"flushed").await;
    file.write_at(7, Bytes::from_static(b"-unflushed"))
        .await
        .expect("write");

    disk.crash_with(CrashMode::KeepUnflushedBytes {
        offset_bytes: 7,
        length_bytes: 3,
    });

    assert_eq!(content_of(file.as_ref()).await, Bytes::from_static(b"flushed-un"));
}

#[tokio::test]
async fn surviving_range_inside_the_flushed_length_overwrites_only_itself() {
    let disk = MemoryDisk::new();
    let mut file = file_with(&disk, b"old-old-old").await;
    file.write_at(0, Bytes::from_static(b"new-new-new"))
        .await
        .expect("write");

    disk.crash_with(CrashMode::KeepUnflushedBytes {
        offset_bytes: 4,
        length_bytes: 3,
    });

    assert_eq!(content_of(file.as_ref()).await, Bytes::from_static(b"old-new-old"));
}

#[tokio::test]
async fn surviving_range_is_cut_at_the_end_of_what_was_written() {
    let disk = MemoryDisk::new();
    let mut file = file_with(&disk, b"flushed").await;
    file.write_at(7, Bytes::from_static(b"-un")).await.expect("write");

    disk.crash_with(CrashMode::KeepUnflushedBytes {
        offset_bytes: 7,
        length_bytes: 1_000,
    });

    assert_eq!(content_of(file.as_ref()).await, Bytes::from_static(b"flushed-un"));
}

#[tokio::test]
async fn surviving_range_past_everything_written_changes_nothing() {
    let disk = MemoryDisk::new();
    let mut file = file_with(&disk, b"flushed").await;
    file.write_at(7, Bytes::from_static(b"-lost")).await.expect("write");

    disk.crash_with(CrashMode::KeepUnflushedBytes {
        offset_bytes: 500,
        length_bytes: 3,
    });

    assert_eq!(content_of(file.as_ref()).await, Bytes::from_static(b"flushed"));
}

#[tokio::test]
async fn crash_can_keep_only_the_first_flushed_bytes() {
    let disk = MemoryDisk::new();
    let file = file_with(&disk, b"flushed-and-acknowledged").await;

    disk.crash_with(CrashMode::KeepFirstBytes(7));

    assert_eq!(content_of(file.as_ref()).await, Bytes::from_static(b"flushed"));
}

#[tokio::test]
async fn file_shorter_than_the_kept_bytes_keeps_everything_it_flushed() {
    let disk = MemoryDisk::new();
    let file = file_with(&disk, b"short").await;

    disk.crash_with(CrashMode::KeepFirstBytes(1_000));

    assert_eq!(content_of(file.as_ref()).await, Bytes::from_static(b"short"));
}

#[tokio::test]
async fn keeping_the_first_bytes_drops_what_was_not_flushed() {
    let disk = MemoryDisk::new();
    let mut file = file_with(&disk, b"flushed").await;
    file.write_at(7, Bytes::from_static(b"-lost")).await.expect("write");

    disk.crash_with(CrashMode::KeepFirstBytes(1_000));

    assert_eq!(content_of(file.as_ref()).await, Bytes::from_static(b"flushed"));
}

#[test]
fn tearing_never_changes_what_was_flushed() {
    let durable = b"flushed-\0\0\0\0".to_vec();
    let visible = b"flushed-tail".to_vec();

    for seed in 0..SEEDS_PER_TEARING_TEST {
        let survived = torn(&durable, &visible, seed);

        assert_eq!(survived[..8], durable[..8], "seed {seed}");
    }
}

#[test]
fn tearing_never_leaves_the_file_as_it_was_flushed() {
    let durable = b"flushed-\0\0\0\0".to_vec();
    let visible = b"flushed-tail".to_vec();

    for seed in 0..SEEDS_PER_TEARING_TEST {
        let survived = torn(&durable, &visible, seed);

        assert_ne!(survived, durable, "seed {seed} lost every unflushed byte cleanly");
    }
}

#[test]
fn tearing_never_leaves_the_file_as_it_was_written() {
    let durable = b"flushed-\0\0\0\0".to_vec();
    let visible = b"flushed-tail".to_vec();

    for seed in 0..SEEDS_PER_TEARING_TEST {
        let survived = torn(&durable, &visible, seed);

        assert_ne!(survived, visible, "seed {seed} left an intact file");
    }
}

#[test]
fn tearing_keeps_part_of_a_write_past_the_flushed_length() {
    let durable = b"flushed-".to_vec();
    let visible = b"flushed-tail".to_vec();

    for seed in 0..SEEDS_PER_TEARING_TEST {
        let survived = torn(&durable, &visible, seed);

        assert!(survived.len() > durable.len(), "seed {seed}");
    }
}

#[test]
fn tearing_a_write_past_the_flushed_length_keeps_the_flushed_bytes() {
    let durable = b"flushed-".to_vec();
    let visible = b"flushed-tail".to_vec();

    for seed in 0..SEEDS_PER_TEARING_TEST {
        let survived = torn(&durable, &visible, seed);

        assert_eq!(survived[..8], durable[..], "seed {seed}");
    }
}

#[test]
fn tearing_leaves_flushed_bytes_between_two_unflushed_writes_alone() {
    let durable = b"mark-A|flushed-data|\0\0\0\0".to_vec();
    let visible = b"mark-B|flushed-data|tail".to_vec();

    for seed in 0..SEEDS_PER_TEARING_TEST {
        let survived = torn(&durable, &visible, seed);

        assert_eq!(survived[6..20], durable[6..20], "seed {seed} damaged flushed bytes");
    }
}

#[test]
fn file_without_unflushed_writes_is_left_alone() {
    let flushed = b"everything was flushed".to_vec();

    let survived = torn(&flushed, &flushed, 1);

    assert_eq!(survived, flushed);
}

#[test]
fn file_without_unflushed_writes_draws_nothing() {
    let flushed = b"everything was flushed".to_vec();
    let mut random = DeterministicRandom::from_seed(1);

    let _survived = tear_unflushed_writes(&flushed, &flushed, &mut random);

    assert_eq!(random, DeterministicRandom::from_seed(1));
}

#[test]
fn unflushed_offsets_are_where_the_two_contents_differ() {
    let durable = b"mark-A|data|".to_vec();
    let visible = b"mark-B|data|ab".to_vec();

    let offsets = unflushed_offsets(&durable, &visible);

    assert_eq!(offsets, [5, 12, 13]);
}

#[tokio::test]
async fn torn_crash_damages_a_file_with_the_suffix() {
    let disk = MemoryDisk::new();
    let mut file = file_with(&disk, b"flushed").await;
    file.write_at(7, Bytes::from_static(b"-unflushed"))
        .await
        .expect("write");

    disk.crash_with(tear_segment_files(1));

    assert_ne!(content_of(file.as_ref()).await, Bytes::from_static(b"flushed"));
}

/// A file that is only ever replaced atomically has nothing unflushed, but a scratch file next to a
/// segment file does. Files outside the suffix lose their unflushed writes whole.
#[tokio::test]
async fn torn_crash_leaves_files_without_the_suffix_as_they_were_flushed() {
    let disk = MemoryDisk::new();
    let mut scratch_file = disk
        .create_file("7/3.seg.repair", Bytes::from_static(b"flushed"), 0)
        .await
        .expect("new file");
    scratch_file
        .write_at(7, Bytes::from_static(b"-unflushed"))
        .await
        .expect("write");

    disk.crash_with(tear_segment_files(1));

    assert_eq!(content_of(scratch_file.as_ref()).await, Bytes::from_static(b"flushed"));
}

/// The simulator tears segment files only. A state file is replaced atomically by the replica, and even
/// one that was written in place would lose its unflushed bytes whole.
#[tokio::test]
async fn torn_crash_never_tears_a_state_file() {
    let disk = MemoryDisk::new();
    let mut state_file = disk
        .create_file("7/3.state", Bytes::from_static(b"old-state"), 0)
        .await
        .expect("new file");
    state_file
        .write_at(0, Bytes::from_static(b"new-state"))
        .await
        .expect("write");

    disk.crash_with(tear_segment_files(1));

    assert_eq!(content_of(state_file.as_ref()).await, Bytes::from_static(b"old-state"));
}

/// A file outside the suffix must not advance the generator, or the files after it would be torn
/// differently depending on what else is on the disk.
#[tokio::test]
async fn torn_crash_draws_nothing_for_files_without_the_suffix() {
    let mut contents_left = Vec::new();

    for has_a_state_file in [false, true] {
        let disk = MemoryDisk::new();
        if has_a_state_file {
            let mut state_file = disk
                .create_file("7/2.state", Bytes::from_static(b"old-state"), 0)
                .await
                .expect("new file");
            state_file
                .write_at(0, Bytes::from_static(b"new-state"))
                .await
                .expect("write");
        }
        let mut file = file_with(&disk, b"flushed").await;
        file.write_at(7, Bytes::from_static(b"-unflushed"))
            .await
            .expect("write");
        disk.crash_with(tear_segment_files(9));
        contents_left.push(content_of(file.as_ref()).await);
    }

    assert_eq!(contents_left[0], contents_left[1]);
}

#[tokio::test]
async fn torn_crash_with_the_same_seed_leaves_the_same_content() {
    let mut contents_left = Vec::new();

    for _run in 0..2 {
        let disk = MemoryDisk::new();
        let mut file = file_with(&disk, b"flushed").await;
        file.write_at(7, Bytes::from_static(b"-unflushed"))
            .await
            .expect("write");
        disk.crash_with(tear_segment_files(9));
        contents_left.push(content_of(file.as_ref()).await);
    }

    assert_eq!(contents_left[0], contents_left[1]);
}

#[tokio::test]
async fn failing_flush_reports_an_error() {
    let disk = MemoryDisk::new();
    let mut file = file_with(&disk, b"flushed").await;
    disk.fail_flushes(true);

    let flushed = file.sync_data().await;

    assert!(flushed.is_err());
}

#[tokio::test]
async fn failing_flush_persists_nothing() {
    let disk = MemoryDisk::new();
    let mut file = file_with(&disk, b"flushed").await;
    file.write_at(7, Bytes::from_static(b"-lost")).await.expect("write");
    disk.fail_flushes(true);
    let _failed_flush = file.sync_data().await;

    disk.crash();

    assert_eq!(content_of(file.as_ref()).await, Bytes::from_static(b"flushed"));
}

#[tokio::test]
async fn flushes_work_again_once_the_failure_is_lifted() {
    let disk = MemoryDisk::new();
    let mut file = file_with(&disk, b"flushed").await;
    disk.fail_flushes(true);
    disk.fail_flushes(false);

    let flushed = file.sync_data().await;

    assert!(flushed.is_ok());
}

#[tokio::test]
async fn atomic_write_survives_a_crash() {
    let disk = MemoryDisk::new();
    disk.write_file_atomically("7/3.state", Bytes::from_static(b"state"))
        .await
        .expect("write");

    disk.crash();

    assert_eq!(
        disk.read_file("7/3.state").await.expect("read"),
        Some(Bytes::from_static(b"state"))
    );
}

#[tokio::test]
async fn renamed_file_is_found_under_its_new_name() {
    let disk = MemoryDisk::new();
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
    let disk = MemoryDisk::new();
    disk.write_file_atomically("7/3.seg.repair", Bytes::from_static(b"new copy"))
        .await
        .expect("write");

    disk.rename_file("7/3.seg.repair", "7/3.seg").await.expect("rename");

    assert_eq!(disk.read_file("7/3.seg.repair").await.expect("read"), None);
}

#[tokio::test]
async fn rename_replaces_the_file_that_was_there() {
    let disk = MemoryDisk::new();
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
async fn rename_survives_a_crash() {
    let disk = MemoryDisk::new();
    disk.write_file_atomically("7/3.seg.repair", Bytes::from_static(b"new copy"))
        .await
        .expect("write");
    disk.rename_file("7/3.seg.repair", "7/3.seg").await.expect("rename");

    disk.crash();

    assert_eq!(
        disk.read_file("7/3.seg").await.expect("read"),
        Some(Bytes::from_static(b"new copy"))
    );
}

#[tokio::test]
async fn rename_of_a_missing_file_fails() {
    let disk = MemoryDisk::new();

    let renamed = disk.rename_file("7/3.seg.repair", "7/3.seg").await;

    assert_eq!(renamed.map_err(|error| error.kind()), Err(io::ErrorKind::NotFound));
}

#[tokio::test]
async fn rename_of_a_missing_file_leaves_the_target_alone() {
    let disk = MemoryDisk::new();
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
async fn rename_fails_while_flushes_fail() {
    let disk = MemoryDisk::new();
    disk.write_file_atomically("7/3.seg.repair", Bytes::from_static(b"new copy"))
        .await
        .expect("write");
    disk.fail_flushes(true);

    let renamed = disk.rename_file("7/3.seg.repair", "7/3.seg").await;

    assert!(renamed.is_err());
}

#[tokio::test]
async fn rename_that_fails_while_flushes_fail_leaves_the_target_alone() {
    let disk = MemoryDisk::new();
    disk.write_file_atomically("7/3.seg", Bytes::from_static(b"old copy"))
        .await
        .expect("write");
    disk.write_file_atomically("7/3.seg.repair", Bytes::from_static(b"new copy"))
        .await
        .expect("write");
    disk.fail_flushes(true);

    let _failed_rename = disk.rename_file("7/3.seg.repair", "7/3.seg").await;

    assert_eq!(
        disk.read_file("7/3.seg").await.expect("read"),
        Some(Bytes::from_static(b"old copy"))
    );
}

#[tokio::test]
async fn reading_a_missing_small_file_returns_none() {
    let disk = MemoryDisk::new();

    let read = disk.read_file("7/3.state").await.expect("read");

    assert_eq!(read, None);
}

#[tokio::test]
async fn handle_to_a_removed_file_fails() {
    let disk = MemoryDisk::new();
    let file = file_with(&disk, b"content").await;

    disk.remove_file(PATH).await.expect("remove");

    assert_eq!(
        file.read_at(0, 1).await.err().map(|error| error.kind()),
        Some(io::ErrorKind::NotFound)
    );
}

#[tokio::test]
async fn removed_file_is_no_longer_listed() {
    let disk = MemoryDisk::new();
    let _file = file_with(&disk, b"content").await;

    disk.remove_file(PATH).await.expect("remove");

    assert_eq!(disk.list_files().await.expect("list"), Vec::<String>::new());
}

#[tokio::test]
async fn removing_a_missing_file_succeeds() {
    let disk = MemoryDisk::new();

    let removed = disk.remove_file(PATH).await;

    assert!(removed.is_ok());
}

#[tokio::test]
async fn files_are_listed_sorted_by_path() {
    let disk = MemoryDisk::new();
    for path in ["7/3.seg", "2/9.seg", "7/1.seg"] {
        disk.create_file(path, Bytes::new(), 0).await.expect("new file");
    }

    let listed = disk.list_files().await.expect("list");

    assert_eq!(listed, ["2/9.seg", "7/1.seg", "7/3.seg"]);
}

#[tokio::test]
async fn free_space_shrinks_by_what_files_occupy() {
    let disk = MemoryDisk::with_capacity(1_000);
    disk.create_file(PATH, Bytes::new(), 400).await.expect("new file");

    let space = disk.space().await.expect("space");

    assert_eq!(
        space,
        DiskSpace {
            capacity_bytes: 1_000,
            free_bytes: 600
        }
    );
}

#[tokio::test]
async fn new_disk_reports_the_default_capacity() {
    let disk = MemoryDisk::new();

    let space = disk.space().await.expect("space");

    assert_eq!(space.capacity_bytes, DEFAULT_MEMORY_DISK_CAPACITY_BYTES);
}

#[tokio::test]
async fn shrinking_a_file_cuts_its_content_durably() {
    let disk = MemoryDisk::new();
    let mut file = file_with(&disk, b"keep-drop").await;
    file.set_length(4).await.expect("shrink");

    disk.crash();

    assert_eq!(content_of(file.as_ref()).await, Bytes::from_static(b"keep"));
}

#[tokio::test]
async fn growing_a_file_fills_it_with_zeros() {
    let disk = MemoryDisk::new();
    let mut file = file_with(&disk, b"ab").await;

    file.set_length(4).await.expect("grow");

    assert_eq!(content_of(file.as_ref()).await, Bytes::from_static(b"ab\0\0"));
}

#[tokio::test]
async fn corruption_reaches_the_durable_content() {
    let disk = MemoryDisk::new();
    let _file = file_with(&disk, b"abc").await;

    disk.corrupt_byte(PATH, 1, 0xFF);

    assert_eq!(disk.durable_content(PATH), Some(vec![b'a', b'b' ^ 0xFF, b'c']));
}

#[tokio::test]
async fn corruption_reaches_the_visible_content() {
    let disk = MemoryDisk::new();
    let file = file_with(&disk, b"abc").await;

    disk.corrupt_byte(PATH, 1, 0x01);

    assert_eq!(content_of(file.as_ref()).await, Bytes::from_static(b"acc"));
}

#[tokio::test]
async fn corrupting_a_missing_file_or_byte_does_nothing() {
    let disk = MemoryDisk::new();
    let file = file_with(&disk, b"abc").await;

    disk.corrupt_byte("no/such.seg", 0, 0xFF);
    disk.corrupt_byte(PATH, 3, 0xFF);

    assert_eq!(content_of(file.as_ref()).await, Bytes::from_static(b"abc"));
}

#[tokio::test]
async fn durable_content_of_a_missing_file_is_none() {
    let disk = MemoryDisk::new();

    assert_eq!(disk.durable_content(PATH), None);
}

#[tokio::test]
async fn clones_of_a_disk_share_its_files() {
    let disk = MemoryDisk::new();
    let _file = file_with(&disk.clone(), b"shared").await;

    let read = disk.read_file(PATH).await.expect("read");

    assert_eq!(read, Some(Bytes::from_static(b"shared")));
}
