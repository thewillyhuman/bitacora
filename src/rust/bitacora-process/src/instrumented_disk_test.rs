//! Tests of the instrumented disk.
//!
//! # What is covered
//!
//! - Every call reaches the wrapped disk and its result comes back unchanged.
//! - Each metric records what the file header of `instrumented_disk.rs` says, and nothing else.
//!
//! # How the fixtures work
//!
//! The wrapped disk is the in-memory disk of `bitacora-segment`, whose `fail_flushes` switch provides a
//! failing flush. Metrics are read from the text exposition, as Prometheus reads them. Time is paused;
//! the in-memory disk answers at once, so a flush takes zero seconds.

use bitacora_segment::MemoryDisk;

use super::*;

/// A file that tests create and write to.
const FILE_PATH: &str = "7/3.seg";

/// How much room the tests reserve for [`FILE_PATH`].
const PREALLOCATED_BYTES: u64 = 64;

/// An in-memory disk, the same disk instrumented, and the registry it records in.
fn instrumented_memory_disk() -> (MemoryDisk, InstrumentedDisk, MetricsRegistry) {
    let registry = MetricsRegistry::new();
    let memory_disk = MemoryDisk::new();
    let instrumented_disk = InstrumentedDisk::new(Arc::new(memory_disk.clone()), &registry);
    (memory_disk, instrumented_disk, registry)
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

/// Creates [`FILE_PATH`] with the content `header` through the instrumented disk.
async fn create_file(disk: &InstrumentedDisk) -> Box<dyn DiskFile> {
    disk.create_file(FILE_PATH, Bytes::from_static(b"header"), PREALLOCATED_BYTES)
        .await
        .expect("the file is created")
}

#[tokio::test(start_paused = true)]
async fn created_file_exists_on_the_wrapped_disk() {
    let (memory_disk, instrumented_disk, _registry) = instrumented_memory_disk();

    create_file(&instrumented_disk).await;

    assert_eq!(memory_disk.list_files().await.expect("lists"), [FILE_PATH.to_owned()]);
}

#[tokio::test(start_paused = true)]
async fn initial_content_of_a_created_file_counts_as_written() {
    let (_memory_disk, instrumented_disk, registry) = instrumented_memory_disk();

    create_file(&instrumented_disk).await;

    assert_eq!(
        exposition_value(&registry, "bitacora_disk_written_bytes_total "),
        Some("6".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn bytes_written_to_a_file_are_counted() {
    let (_memory_disk, instrumented_disk, registry) = instrumented_memory_disk();
    let mut file = create_file(&instrumented_disk).await;

    file.write_at(6, Bytes::from_static(b"0123456789"))
        .await
        .expect("the write is accepted");

    assert_eq!(
        exposition_value(&registry, "bitacora_disk_written_bytes_total "),
        Some("16".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn written_bytes_can_be_read_back_through_the_instrumented_file() {
    let (_memory_disk, instrumented_disk, _registry) = instrumented_memory_disk();
    let mut file = create_file(&instrumented_disk).await;
    file.write_at(6, Bytes::from_static(b"0123456789"))
        .await
        .expect("the write is accepted");

    let read = file.read_at(6, 10).await;

    assert_eq!(read.ok(), Some(Bytes::from_static(b"0123456789")));
}

#[tokio::test(start_paused = true)]
async fn file_opened_through_the_instrumented_disk_is_instrumented_too() {
    let (_memory_disk, instrumented_disk, registry) = instrumented_memory_disk();
    drop(create_file(&instrumented_disk).await);
    let mut reopened_file = instrumented_disk.open_file(FILE_PATH).await.expect("the file opens");

    reopened_file.sync_data().await.expect("the flush succeeds");

    assert_eq!(
        exposition_value(&registry, "bitacora_disk_flush_duration_seconds_count "),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn every_flush_is_observed() {
    let (_memory_disk, instrumented_disk, registry) = instrumented_memory_disk();
    let mut file = create_file(&instrumented_disk).await;

    file.sync_data().await.expect("the flush succeeds");
    file.sync_data().await.expect("the flush succeeds");

    assert_eq!(
        exposition_value(&registry, "bitacora_disk_flush_duration_seconds_count "),
        Some("2".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn failed_flush_is_passed_on_to_the_caller() {
    let (memory_disk, instrumented_disk, _registry) = instrumented_memory_disk();
    let mut file = create_file(&instrumented_disk).await;
    memory_disk.fail_flushes(true);

    let flushed = file.sync_data().await;

    assert!(flushed.is_err(), "{flushed:?}");
}

#[tokio::test(start_paused = true)]
async fn failed_flush_is_counted_as_a_failure_of_its_operation() {
    let (memory_disk, instrumented_disk, registry) = instrumented_memory_disk();
    let mut file = create_file(&instrumented_disk).await;
    memory_disk.fail_flushes(true);

    let _flushed = file.sync_data().await;

    assert_eq!(
        exposition_value(
            &registry,
            "bitacora_disk_operation_failures_total{operation=\"sync_data\"} "
        ),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn failed_flush_is_observed_like_any_other() {
    let (memory_disk, instrumented_disk, registry) = instrumented_memory_disk();
    let mut file = create_file(&instrumented_disk).await;
    memory_disk.fail_flushes(true);

    let _flushed = file.sync_data().await;

    assert_eq!(
        exposition_value(&registry, "bitacora_disk_flush_duration_seconds_count "),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn successful_calls_count_no_failure() {
    let (_memory_disk, instrumented_disk, registry) = instrumented_memory_disk();
    let mut file = create_file(&instrumented_disk).await;

    file.sync_data().await.expect("the flush succeeds");

    assert!(
        !registry.encode().contains("bitacora_disk_operation_failures_total{"),
        "{}",
        registry.encode()
    );
}

#[tokio::test(start_paused = true)]
async fn creating_a_file_that_exists_is_counted_as_a_failure() {
    let (_memory_disk, instrumented_disk, registry) = instrumented_memory_disk();
    drop(create_file(&instrumented_disk).await);

    let created_again = instrumented_disk
        .create_file(FILE_PATH, Bytes::from_static(b"header"), PREALLOCATED_BYTES)
        .await;

    assert!(created_again.is_err());
    assert_eq!(
        exposition_value(
            &registry,
            "bitacora_disk_operation_failures_total{operation=\"create_file\"} "
        ),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn opening_an_absent_file_is_not_a_failure_of_the_disk() {
    let (_memory_disk, instrumented_disk, registry) = instrumented_memory_disk();

    let opened = instrumented_disk.open_file("7/9.seg").await;

    assert!(opened.is_err());
    assert!(
        !registry.encode().contains("bitacora_disk_operation_failures_total{"),
        "{}",
        registry.encode()
    );
}

#[tokio::test(start_paused = true)]
async fn small_file_written_atomically_is_stored_and_counted() {
    let (memory_disk, instrumented_disk, registry) = instrumented_memory_disk();

    instrumented_disk
        .write_file_atomically("replica.id", Bytes::from_static(b"replica-a"))
        .await
        .expect("the file is written");

    assert_eq!(
        memory_disk.read_file("replica.id").await.expect("reads"),
        Some(Bytes::from_static(b"replica-a"))
    );
    assert_eq!(
        exposition_value(&registry, "bitacora_disk_written_bytes_total "),
        Some("9".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn small_file_is_read_through_the_instrumented_disk() {
    let (memory_disk, instrumented_disk, _registry) = instrumented_memory_disk();
    memory_disk
        .write_file_atomically("replica.id", Bytes::from_static(b"replica-a"))
        .await
        .expect("the file is written");

    let read = instrumented_disk.read_file("replica.id").await;

    assert_eq!(read.ok(), Some(Some(Bytes::from_static(b"replica-a"))));
}

#[tokio::test(start_paused = true)]
async fn removed_file_is_gone_from_the_wrapped_disk() {
    let (memory_disk, instrumented_disk, _registry) = instrumented_memory_disk();
    drop(create_file(&instrumented_disk).await);

    instrumented_disk
        .remove_file(FILE_PATH)
        .await
        .expect("the file is removed");

    assert_eq!(memory_disk.list_files().await.expect("lists"), Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn renamed_file_is_moved_on_the_wrapped_disk() {
    let (memory_disk, instrumented_disk, _registry) = instrumented_memory_disk();
    drop(create_file(&instrumented_disk).await);
    let new_path = format!("{FILE_PATH}.moved");

    instrumented_disk
        .rename_file(FILE_PATH, &new_path)
        .await
        .expect("the file is renamed");

    assert_eq!(memory_disk.list_files().await.expect("lists"), [new_path]);
}

#[tokio::test(start_paused = true)]
async fn files_are_listed_through_the_instrumented_disk() {
    let (_memory_disk, instrumented_disk, _registry) = instrumented_memory_disk();
    drop(create_file(&instrumented_disk).await);

    let listed = instrumented_disk.list_files().await;

    assert_eq!(listed.ok(), Some(vec![FILE_PATH.to_owned()]));
}

#[tokio::test(start_paused = true)]
async fn reported_space_is_what_the_wrapped_disk_reports() {
    let (memory_disk, instrumented_disk, _registry) = instrumented_memory_disk();

    let space = instrumented_disk.space().await.expect("measures");

    assert_eq!(space, memory_disk.space().await.expect("measures"));
}

#[tokio::test(start_paused = true)]
async fn reported_capacity_is_kept_in_its_gauge() {
    let (_memory_disk, instrumented_disk, registry) = instrumented_memory_disk();

    let space = instrumented_disk.space().await.expect("measures");

    assert_eq!(
        exposition_value(&registry, "bitacora_disk_capacity_bytes "),
        Some(space.capacity_bytes.to_string())
    );
}

#[tokio::test(start_paused = true)]
async fn reported_free_space_is_kept_in_its_gauge() {
    let (_memory_disk, instrumented_disk, registry) = instrumented_memory_disk();
    drop(create_file(&instrumented_disk).await);

    let space = instrumented_disk.space().await.expect("measures");

    assert_eq!(
        exposition_value(&registry, "bitacora_disk_free_bytes "),
        Some(space.free_bytes.to_string())
    );
}

#[tokio::test(start_paused = true)]
async fn file_length_and_resizing_go_through_to_the_wrapped_file() {
    let (_memory_disk, instrumented_disk, _registry) = instrumented_memory_disk();
    let mut file = create_file(&instrumented_disk).await;

    file.set_length(32).await.expect("the file is resized");

    assert_eq!(file.length_bytes().await.ok(), Some(32));
}
