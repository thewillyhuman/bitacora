//! Tests of the task that owns a segment copy.
//!
//! # What is covered
//!
//! - When a waiting reader has a reason to stop waiting.
//! - A request sent through a handle reaches the copy, and a request that was queued is applied even if
//!   its caller stopped waiting for the answer.
//! - A copy with nothing stored reports no status.
//! - Deleting the copy ends the task, and a handle to a task that has ended says so instead of hanging.
//! - The task records a write the disk refused, with the segment it concerns.
//!
//! What each command does to the copy is tested with the copy, in `copy_test.rs` and `copy/*_test.rs`.
//! How requests find their task, and how reads wait, is tested through a replica in
//! `replica/api_test.rs`.
//!
//! # How the fixtures work
//!
//! [`SpawnedTask`] is the task of one copy of the segment under test on a `MemoryDisk`, with the set it
//! runs in and the report it records refusals in. The tests run on a single-threaded runtime, so a task
//! has finished everything it does after a command by the time the test sees the answer.

use bitacora_segment::MemoryDisk;

use super::*;
use crate::test_support::{ROOMY, SEGMENT, append_request, batch, new_copy};

/// The task of a copy of the segment under test, and what surrounds it.
struct SpawnedTask {
    /// The way to reach the task.
    handle: SegmentHandle,
    /// Where the task records a write the disk refused.
    write_failures: WriteFailureReport,
    /// The set the task runs in. Dropping it stops the task.
    tasks: JoinSet<()>,
}

/// Starts the task of a copy of the segment under test that has nothing stored on `disk` yet.
fn spawn_task_on(disk: &MemoryDisk) -> SpawnedTask {
    let mut tasks = JoinSet::new();
    let write_failures = WriteFailureReport::new();
    let handle = SegmentHandle::spawn(SEGMENT, new_copy(disk), &mut tasks, write_failures.clone());
    SpawnedTask {
        handle,
        write_failures,
        tasks,
    }
}

/// The progress of an open copy that readers may be served up to `readable_end`.
fn progress_of_an_open_copy_readable_to(readable_end: u64) -> Progress {
    Progress {
        readable_end: Some(SequenceNumber::new(readable_end)),
        sealed_end: None,
    }
}

// When a waiting reader stops waiting.

#[test]
fn reader_has_news_once_its_sequence_is_readable() {
    let progress = progress_of_an_open_copy_readable_to(104);

    assert!(progress.has_news_for(SequenceNumber::new(104)));
}

#[test]
fn reader_has_no_news_while_its_sequence_is_not_readable() {
    let progress = progress_of_an_open_copy_readable_to(104);

    assert!(!progress.has_news_for(SequenceNumber::new(105)));
}

#[test]
fn reader_has_no_news_from_a_copy_without_records() {
    let progress = Progress::default();

    assert!(!progress.has_news_for(SequenceNumber::new(100)));
}

#[test]
fn reader_past_the_end_of_a_sealed_segment_has_news() {
    let progress = Progress {
        readable_end: Some(SequenceNumber::new(104)),
        sealed_end: Some(SequenceNumber::new(104)),
    };

    assert!(progress.has_news_for(SequenceNumber::new(105)));
}

// Requests through a handle.

#[tokio::test]
async fn append_sent_through_a_handle_reaches_the_copy() {
    let disk = MemoryDisk::new();
    let task = spawn_task_on(&disk);

    let response = task.handle.append(append_request(&[batch(100, 5)], 0), ROOMY).await;

    assert_eq!(
        response,
        Ok(AppendResponse {
            local_end: SequenceNumber::new(104)
        })
    );
}

#[tokio::test]
async fn queued_append_is_applied_after_its_caller_stopped_waiting() {
    let disk = MemoryDisk::new();
    let task = spawn_task_on(&disk);

    // The caller is polled once, which queues the command, and is then dropped without its answer.
    tokio::select! {
        biased;
        _answer = task.handle.append(append_request(&[batch(100, 5)], 0), ROOMY) => {}
        () = std::future::ready(()) => {}
    }

    let status = task
        .handle
        .status()
        .await
        .expect("the task is running")
        .expect("the copy is stored once the append was applied");
    assert_eq!(status.local_end, Some(SequenceNumber::new(104)));
}

#[tokio::test]
async fn copy_with_nothing_stored_reports_no_status() {
    let disk = MemoryDisk::new();
    let task = spawn_task_on(&disk);

    let status = task.handle.status().await;

    assert_eq!(status, Ok(None));
}

// The end of a task.

#[tokio::test]
async fn task_ends_when_its_copy_is_deleted() {
    let disk = MemoryDisk::new();
    let mut task = spawn_task_on(&disk);

    task.handle.delete().await.expect("a working disk removes the files");

    let ended = task.tasks.join_next().await;
    assert!(matches!(ended, Some(Ok(()))), "{ended:?}");
}

#[tokio::test]
async fn handle_of_a_deleted_copy_says_that_its_task_has_stopped() {
    let disk = MemoryDisk::new();
    let task = spawn_task_on(&disk);
    task.handle.delete().await.expect("a working disk removes the files");

    let answer = task.handle.status().await;

    assert!(matches!(answer, Err(ReplicaError::Unavailable { .. })), "{answer:?}");
}

#[tokio::test]
async fn handle_of_an_aborted_task_says_that_its_task_has_stopped() {
    let disk = MemoryDisk::new();
    let mut task = spawn_task_on(&disk);
    task.tasks.shutdown().await;

    let answer = task.handle.status().await;

    assert!(matches!(answer, Err(ReplicaError::Unavailable { .. })), "{answer:?}");
}

// A write the disk refused.

#[tokio::test]
async fn task_records_a_write_the_disk_refused() {
    let disk = MemoryDisk::new();
    let task = spawn_task_on(&disk);
    disk.fail_flushes(true);

    let _refused = task.handle.append(append_request(&[batch(100, 5)], 0), ROOMY).await;

    let recorded = task
        .write_failures
        .first()
        .expect("the task records the refusal before it takes its next command");
    assert_eq!(recorded.segment, SEGMENT);
}

#[tokio::test]
async fn task_of_a_copy_on_a_working_disk_records_no_refused_write() {
    let disk = MemoryDisk::new();
    let task = spawn_task_on(&disk);

    task.handle
        .append(append_request(&[batch(100, 5)], 0), ROOMY)
        .await
        .expect("an empty copy on a working disk accepts its first batch");

    assert_eq!(task.write_failures.first(), None);
}
