//! Tests of the segment file.
//!
//! # What is covered
//!
//! - Creation and appends: where the copy ends, what is refused, and that a refusal changes nothing.
//! - Reads: order, payloads, whole batches at both ends of the range, the size limit.
//! - Crash recovery: what a crash keeps, torn tails, and files that are not the copy asked for.
//! - Damage to flushed records and the flushed mark that tells it from a torn write, including the window
//!   in which the mark lags.
//! - Truncation, giving back unused space and growth by preallocation steps.
//! - Scrubbing: silent corruption is found by verification and by reads.
//! - A file larger than several scan chunks, read and recovered across chunk boundaries.
//!
//! # How the fixtures work
//!
//! Every test uses one copy of [`SEGMENT`] at [`PATH`] on an in-memory disk, which starts at record
//! [`FIRST_SEQUENCE`]. `segment_with_three_batches` holds records 100 to 109 in batches of 3, 2 and 5,
//! flushed; payloads name their own sequence number. A crash is `MemoryDisk::crash`, or
//! `crash_with_torn_tail` to leave the start of an unflushed write behind, and `reopen` opens the copy
//! again the way a restarted replica does.

use bitacora_types::{DeterministicRandom, Epoch, LogRecord, SegmentId, StreamId};

use super::*;
use crate::format::decode_frames;
use crate::memory_disk::{CrashMode, MemoryDisk};

/// The segment the fixture's file is a copy of.
const SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));

/// The path of the fixture's file.
const PATH: &str = "7/3.seg";

/// The sequence number of the segment's first record.
const FIRST_SEQUENCE: u64 = 100;

/// How much space the fixture's file reserves, and grows by: small, so that a test can fill it.
const PREALLOCATED_BYTES: u64 = 4 * 1024;

/// An offset inside the first batch of the file, past its frame header: a byte of a record.
const BYTE_IN_THE_FIRST_BATCH: usize = SEGMENT_HEADER_BYTES + 30;

/// How many bytes of an unflushed batch a torn crash leaves behind: fewer than a frame header.
const TORN_BYTES: usize = 9;

/// How many reads from random positions the test of a large file makes.
const RANDOM_READS: usize = 20;

/// The header of [`SEGMENT`], written at epoch 5 and starting at [`FIRST_SEQUENCE`].
fn header() -> SegmentHeader {
    SegmentHeader {
        address: SEGMENT,
        epoch: Epoch::new(5),
        first_sequence: SequenceNumber::new(FIRST_SEQUENCE),
    }
}

/// One batch of `record_count` records starting at `first_sequence`, whose payloads name their own
/// sequence number.
fn batch(first_sequence: u64, record_count: u64) -> Batch {
    let payloads: Vec<Bytes> = (first_sequence..first_sequence + record_count)
        .map(|sequence| Bytes::from(format!("record-{sequence}")))
        .collect();
    Batch::encode(SEGMENT, SequenceNumber::new(first_sequence), &payloads).expect("small payloads are valid")
}

/// Creates the empty copy at [`PATH`].
async fn new_segment(disk: &MemoryDisk) -> SegmentFile {
    SegmentFile::create(disk, PATH, header(), PREALLOCATED_BYTES)
        .await
        .expect("the path is free on a new disk")
}

/// A segment holding batches 100..=102, 103..=104 and 105..=109, flushed.
async fn segment_with_three_batches(disk: &MemoryDisk) -> SegmentFile {
    let mut segment = new_segment(disk).await;
    segment
        .append(&[batch(100, 3), batch(103, 2), batch(105, 5)])
        .await
        .expect("the batches continue an empty copy");
    segment.flush().await.expect("flushes are not failing");
    segment
}

/// A segment whose three batches were flushed by one request and whose fourth by a later one, so that the
/// flushed mark covers the first three.
async fn segment_with_a_mark_over_three_batches(disk: &MemoryDisk) -> SegmentFile {
    let mut segment = segment_with_three_batches(disk).await;
    segment.append(&[batch(110, 1)]).await.expect("append");
    segment.flush().await.expect("flush");
    segment
}

/// What opening the copy at [`PATH`] again found.
struct Reopened {
    /// The copy as recovered from the disk.
    segment: SegmentFile,
    /// Why the recovery scan found no further batch.
    stop: ScanStop,
}

/// Opens the copy at [`PATH`] the way a restarted replica does.
async fn reopen(disk: &MemoryDisk) -> Reopened {
    let (segment, stop) = SegmentFile::open(disk, PATH, SEGMENT, PREALLOCATED_BYTES)
        .await
        .expect("the copy is intact enough to open");
    Reopened { segment, stop }
}

/// The records in a run of frames, in order.
fn records_in(frames: &Bytes) -> Vec<LogRecord> {
    let mut records = Vec::new();
    for batch in decode_frames(SEGMENT, frames).expect("a read returns valid frames") {
        records.extend(batch.records());
    }
    records
}

/// The sequence numbers of the records in a run of frames, in order.
fn sequences_in(frames: &Bytes) -> Vec<u64> {
    let mut sequences = Vec::new();
    for record in records_in(frames) {
        sequences.push(record.sequence.get());
    }
    sequences
}

/// The sequence numbers of every record the copy holds, in order.
async fn read_all(segment: &SegmentFile) -> Vec<u64> {
    let frames = segment
        .read_frames(
            SequenceNumber::new(FIRST_SEQUENCE),
            SequenceNumber::new(u64::MAX),
            usize::MAX,
        )
        .await
        .expect("the copy is intact");
    sequences_in(&frames)
}

/// The sequence numbers `first..=last`, as a read is expected to return them.
fn sequences(first: u64, last: u64) -> Vec<u64> {
    (first..=last).collect()
}

/// Crashes `disk` so that only the first `torn_bytes` of everything written after `flushed_bytes` reach
/// the platter: a torn write.
fn crash_with_torn_tail(disk: &MemoryDisk, flushed_bytes: u64, torn_bytes: usize) {
    disk.crash_with(CrashMode::KeepUnflushedBytes {
        offset_bytes: usize::try_from(flushed_bytes).expect("the fixture's file is small"),
        length_bytes: torn_bytes,
    });
}

/// Flips a bit in a record of the first batch, in what the disk holds.
fn damage_the_first_batch(disk: &MemoryDisk) {
    disk.corrupt_byte(PATH, BYTE_IN_THE_FIRST_BATCH, 0x04);
}

/// Reopens a copy that was created while flushes were failing, after a crash.
async fn reopen_a_copy_created_while_flushes_failed() -> Reopened {
    let disk = MemoryDisk::new();
    disk.fail_flushes(true);
    new_segment(&disk).await;
    disk.crash();
    disk.fail_flushes(false);
    reopen(&disk).await
}

/// Reopens a copy that crashed with a torn batch after its three flushed ones.
async fn reopen_a_copy_with_a_torn_tail() -> Reopened {
    let disk = MemoryDisk::new();
    let mut segment = segment_with_three_batches(&disk).await;
    let flushed_bytes = segment.content_bytes();
    segment.append(&[batch(110, 4)]).await.expect("append");
    crash_with_torn_tail(&disk, flushed_bytes, TORN_BYTES);
    reopen(&disk).await
}

/// Reopens a copy whose first batch was damaged after a flush that the mark does not cover yet.
async fn reopen_a_copy_damaged_past_its_mark() -> Reopened {
    let disk = MemoryDisk::new();
    segment_with_three_batches(&disk).await;
    damage_the_first_batch(&disk);
    disk.crash();
    reopen(&disk).await
}

/// A flushed copy that spans several scan chunks and many index intervals, one record per batch.
struct LargeSegment {
    /// The disk the copy is on.
    disk: MemoryDisk,
    /// The copy.
    segment: SegmentFile,
    /// The sequence number of the copy's last record.
    last_sequence: u64,
    /// The generator the copy was built with, for the test to go on drawing from.
    random: DeterministicRandom,
}

/// Appends batches of one random payload of up to 96 KiB until the file spans three scan chunks, then
/// flushes.
async fn large_segment() -> LargeSegment {
    let mut random = DeterministicRandom::from_seed(11);
    let disk = MemoryDisk::new();
    let mut segment = new_segment(&disk).await;
    let mut next_sequence = FIRST_SEQUENCE;
    while segment.content_bytes() < 3 * to_u64(SCAN_CHUNK_BYTES) {
        let payload_bytes = usize::try_from(random.between(1, 96 * 1024)).expect("at most 96 KiB");
        let payload = Bytes::from(random.bytes(payload_bytes));
        let large_batch = Batch::encode(SEGMENT, SequenceNumber::new(next_sequence), &[payload])
            .expect("a payload of 96 KiB is within the limits");
        segment.append(&[large_batch]).await.expect("append");
        next_sequence += 1;
    }
    segment.flush().await.expect("flush");
    LargeSegment {
        disk,
        segment,
        last_sequence: next_sequence - 1,
        random,
    }
}

// Creation and appends.

#[tokio::test]
async fn new_segment_ends_before_its_first_sequence() {
    let segment = new_segment(&MemoryDisk::new()).await;

    assert_eq!(segment.local_end(), SequenceNumber::new(FIRST_SEQUENCE - 1));
}

#[tokio::test]
async fn new_segment_reports_the_header_it_was_created_with() {
    let segment = new_segment(&MemoryDisk::new()).await;

    assert_eq!(segment.header(), header());
}

#[tokio::test]
async fn content_of_a_new_segment_is_its_header() {
    let segment = new_segment(&MemoryDisk::new()).await;

    assert_eq!(segment.content_bytes(), HEADER_END_OFFSET);
}

#[tokio::test]
async fn creating_a_segment_twice_fails() {
    let disk = MemoryDisk::new();
    new_segment(&disk).await;

    let second_attempt = SegmentFile::create(&disk, PATH, header(), PREALLOCATED_BYTES).await;

    assert!(matches!(second_attempt, Err(SegmentError::Io { .. })));
}

#[tokio::test]
async fn append_moves_the_end_to_the_last_record() {
    let segment = segment_with_three_batches(&MemoryDisk::new()).await;

    assert_eq!(segment.local_end(), SequenceNumber::new(109));
}

#[tokio::test]
async fn append_grows_the_content_by_the_frames_it_wrote() {
    let mut segment = new_segment(&MemoryDisk::new()).await;
    let appended = batch(100, 3);

    segment.append(std::slice::from_ref(&appended)).await.expect("append");

    assert_eq!(
        segment.content_bytes(),
        HEADER_END_OFFSET + to_u64(appended.frame().len())
    );
}

#[tokio::test]
async fn appending_nothing_changes_nothing() {
    let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;

    segment.append(&[]).await.expect("append");

    assert_eq!(segment.local_end(), SequenceNumber::new(109));
}

#[tokio::test]
async fn append_that_skips_a_sequence_is_refused() {
    let mut segment = new_segment(&MemoryDisk::new()).await;

    let refused = segment.append(&[batch(101, 1)]).await;

    assert!(matches!(
        refused,
        Err(SegmentError::OutOfOrder { found, .. }) if found == SequenceNumber::new(101)
    ));
}

#[tokio::test]
async fn refused_append_says_where_the_copy_ends() {
    let mut segment = new_segment(&MemoryDisk::new()).await;

    let refused = segment.append(&[batch(101, 1)]).await;

    assert!(matches!(
        refused,
        Err(SegmentError::OutOfOrder { local_end, .. }) if local_end == SequenceNumber::new(99)
    ));
}

#[tokio::test]
async fn append_that_repeats_a_sequence_is_refused() {
    let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;

    let refused = segment.append(&[batch(105, 5)]).await;

    assert!(matches!(refused, Err(SegmentError::OutOfOrder { .. })));
}

#[tokio::test]
async fn refused_append_leaves_the_copy_unchanged() {
    let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;

    let _refused = segment.append(&[batch(110, 1), batch(112, 1)]).await;

    assert_eq!(read_all(&segment).await, sequences(100, 109));
}

#[tokio::test]
async fn segment_created_while_flushes_fail_is_an_empty_copy() {
    let reopened = reopen_a_copy_created_while_flushes_failed().await;

    assert_eq!(reopened.segment.local_end(), SequenceNumber::new(FIRST_SEQUENCE - 1));
}

#[tokio::test]
async fn segment_created_while_flushes_fail_has_no_torn_tail() {
    let reopened = reopen_a_copy_created_while_flushes_failed().await;

    assert_eq!(reopened.stop, ScanStop::EndOfData);
}

#[tokio::test]
async fn failed_flush_is_reported() {
    let disk = MemoryDisk::new();
    let mut segment = segment_with_three_batches(&disk).await;
    disk.fail_flushes(true);

    let flushed = segment.flush().await;

    assert!(matches!(flushed, Err(SegmentError::Io { operation: "flush", .. })));
}

// Reads.

#[tokio::test]
async fn read_returns_the_appended_records_in_order() {
    let segment = segment_with_three_batches(&MemoryDisk::new()).await;

    assert_eq!(read_all(&segment).await, sequences(100, 109));
}

#[tokio::test]
async fn read_returns_the_payloads_that_were_appended() {
    let segment = segment_with_three_batches(&MemoryDisk::new()).await;

    let frames = segment
        .read_frames(SequenceNumber::new(103), SequenceNumber::new(104), usize::MAX)
        .await
        .expect("read");

    assert_eq!(records_in(&frames)[1].payload, Bytes::from("record-104"));
}

#[tokio::test]
async fn read_from_inside_a_batch_starts_with_that_whole_batch() {
    let segment = segment_with_three_batches(&MemoryDisk::new()).await;

    let frames = segment
        .read_frames(SequenceNumber::new(104), SequenceNumber::new(109), usize::MAX)
        .await
        .expect("read");

    assert_eq!(sequences_in(&frames), sequences(103, 109));
}

#[tokio::test]
async fn read_never_returns_a_batch_that_ends_past_the_limit() {
    let segment = segment_with_three_batches(&MemoryDisk::new()).await;

    let frames = segment
        .read_frames(SequenceNumber::new(100), SequenceNumber::new(107), usize::MAX)
        .await
        .expect("read");

    assert_eq!(sequences_in(&frames), sequences(100, 104));
}

#[tokio::test]
async fn read_past_the_end_is_empty() {
    let segment = segment_with_three_batches(&MemoryDisk::new()).await;

    let frames = segment
        .read_frames(SequenceNumber::new(110), SequenceNumber::new(200), usize::MAX)
        .await
        .expect("read");

    assert_eq!(frames, Bytes::new());
}

#[tokio::test]
async fn read_above_the_limit_is_empty() {
    let segment = segment_with_three_batches(&MemoryDisk::new()).await;

    let frames = segment
        .read_frames(SequenceNumber::new(105), SequenceNumber::new(104), usize::MAX)
        .await
        .expect("read");

    assert_eq!(frames, Bytes::new());
}

#[tokio::test]
async fn read_of_an_empty_segment_is_empty() {
    let segment = new_segment(&MemoryDisk::new()).await;

    assert_eq!(read_all(&segment).await, Vec::<u64>::new());
}

#[tokio::test]
async fn read_stops_after_the_batch_that_reaches_the_size_limit() {
    let segment = segment_with_three_batches(&MemoryDisk::new()).await;

    let frames = segment
        .read_frames(SequenceNumber::new(100), SequenceNumber::new(109), 1)
        .await
        .expect("read");

    assert_eq!(sequences_in(&frames), [100, 101, 102]);
}

#[tokio::test]
async fn read_returns_batches_that_were_not_flushed_yet() {
    let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;

    segment.append(&[batch(110, 2)]).await.expect("append");

    assert_eq!(read_all(&segment).await, sequences(100, 111));
}

// Crash recovery.

#[tokio::test]
async fn reopened_segment_holds_everything_that_was_flushed() {
    let disk = MemoryDisk::new();
    segment_with_three_batches(&disk).await;
    disk.crash();

    let reopened = reopen(&disk).await;

    assert_eq!(read_all(&reopened.segment).await, sequences(100, 109));
}

#[tokio::test]
async fn reopened_segment_that_was_flushed_has_no_torn_tail() {
    let disk = MemoryDisk::new();
    segment_with_three_batches(&disk).await;
    disk.crash();

    let reopened = reopen(&disk).await;

    assert_eq!(reopened.stop, ScanStop::EndOfData);
}

#[tokio::test]
async fn crash_before_a_flush_loses_only_the_unflushed_batches() {
    let disk = MemoryDisk::new();
    let mut segment = segment_with_three_batches(&disk).await;
    segment.append(&[batch(110, 4)]).await.expect("append");
    disk.crash();

    let reopened = reopen(&disk).await;

    assert_eq!(reopened.segment.local_end(), SequenceNumber::new(109));
}

#[tokio::test]
async fn torn_tail_is_ignored() {
    let reopened = reopen_a_copy_with_a_torn_tail().await;

    assert_eq!(reopened.segment.local_end(), SequenceNumber::new(109));
}

#[tokio::test]
async fn torn_tail_is_reported() {
    let reopened = reopen_a_copy_with_a_torn_tail().await;

    assert!(
        matches!(reopened.stop, ScanStop::Invalid(_)),
        "stop {:?}",
        reopened.stop
    );
}

#[tokio::test]
async fn segment_accepts_appends_over_a_torn_tail() {
    let disk = MemoryDisk::new();
    let mut segment = segment_with_three_batches(&disk).await;
    let flushed_bytes = segment.content_bytes();
    segment.append(&[batch(110, 4)]).await.expect("append");
    crash_with_torn_tail(&disk, flushed_bytes, TORN_BYTES);
    let mut reopened = reopen(&disk).await;

    reopened.segment.append(&[batch(110, 2)]).await.expect("append");
    reopened.segment.flush().await.expect("flush");
    disk.crash();

    let after_second_crash = reopen(&disk).await;
    assert_eq!(read_all(&after_second_crash.segment).await, sequences(100, 111));
}

#[tokio::test]
async fn opening_a_missing_segment_fails() {
    let opened = SegmentFile::open(&MemoryDisk::new(), PATH, SEGMENT, PREALLOCATED_BYTES).await;

    assert!(matches!(opened, Err(SegmentError::Io { .. })));
}

#[tokio::test]
async fn opening_another_segments_file_is_refused() {
    let disk = MemoryDisk::new();
    new_segment(&disk).await;
    let other_segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(4));

    let opened = SegmentFile::open(&disk, PATH, other_segment, PREALLOCATED_BYTES).await;

    assert!(matches!(opened, Err(SegmentError::WrongSegment { .. })));
}

#[tokio::test]
async fn opening_a_file_with_a_damaged_header_is_refused() {
    let disk = MemoryDisk::new();
    new_segment(&disk).await;
    disk.corrupt_byte(PATH, 20, 0x01);

    let opened = SegmentFile::open(&disk, PATH, SEGMENT, PREALLOCATED_BYTES).await;

    assert!(matches!(opened, Err(SegmentError::InvalidHeader { .. })));
}

// Damage to flushed records.

#[tokio::test]
async fn damage_to_flushed_records_is_found_when_the_copy_is_reopened() {
    let disk = MemoryDisk::new();
    segment_with_a_mark_over_three_batches(&disk).await;
    damage_the_first_batch(&disk);
    disk.crash();

    let reopened = SegmentFile::open(&disk, PATH, SEGMENT, PREALLOCATED_BYTES).await;

    assert!(
        matches!(reopened, Err(SegmentError::Corrupt { .. })),
        "the damaged copy was accepted"
    );
}

/// The mark lags by one flush, so damage to the most recently flushed records cannot be told from a torn
/// write until the mark catches up: the copy opens, and holds nothing from the damage on.
#[tokio::test]
async fn damage_past_the_mark_is_taken_for_a_torn_tail() {
    let reopened = reopen_a_copy_damaged_past_its_mark().await;

    assert_eq!(reopened.segment.local_end(), SequenceNumber::new(FIRST_SEQUENCE - 1));
}

#[tokio::test]
async fn damage_past_the_mark_is_reported_as_an_invalid_frame() {
    let reopened = reopen_a_copy_damaged_past_its_mark().await;

    assert!(matches!(reopened.stop, ScanStop::Invalid(_)));
}

#[tokio::test]
async fn flushing_the_mark_closes_the_window() {
    let disk = MemoryDisk::new();
    let mut segment = segment_with_three_batches(&disk).await;
    segment.flush_mark().await.expect("flush mark");
    damage_the_first_batch(&disk);
    disk.crash();

    let reopened = SegmentFile::open(&disk, PATH, SEGMENT, PREALLOCATED_BYTES).await;

    assert!(matches!(reopened, Err(SegmentError::Corrupt { .. })));
}

/// A copy with nothing flushed past its header has nothing for a mark to cover, so bringing the mark up
/// to date must not touch the disk: it succeeds even while every flush fails.
#[tokio::test]
async fn mark_that_is_up_to_date_needs_no_flush() {
    let disk = MemoryDisk::new();
    let mut segment = new_segment(&disk).await;
    disk.fail_flushes(true);

    let flushed = segment.flush_mark().await;

    assert!(flushed.is_ok());
}

#[tokio::test]
async fn mark_that_was_never_flushed_does_not_survive_a_crash() {
    let disk = MemoryDisk::new();
    let mut segment = segment_with_three_batches(&disk).await;
    // The mark for the three batches is written with this append, which is never flushed.
    segment.append(&[batch(110, 1)]).await.expect("append");
    damage_the_first_batch(&disk);
    disk.crash();

    let reopened = reopen(&disk).await;

    assert_eq!(reopened.segment.local_end(), SequenceNumber::new(FIRST_SEQUENCE - 1));
}

#[tokio::test]
async fn torn_tail_after_the_mark_is_not_damage() {
    let disk = MemoryDisk::new();
    let mut segment = segment_with_a_mark_over_three_batches(&disk).await;
    let flushed_bytes = segment.content_bytes();
    segment.append(&[batch(111, 4)]).await.expect("append");
    crash_with_torn_tail(&disk, flushed_bytes, TORN_BYTES);

    let reopened = reopen(&disk).await;

    assert_eq!(reopened.segment.local_end(), SequenceNumber::new(110));
}

#[tokio::test]
async fn truncating_below_the_mark_lowers_it() {
    let disk = MemoryDisk::new();
    let mut segment = segment_with_a_mark_over_three_batches(&disk).await;
    segment.truncate_to(SequenceNumber::new(102)).await.expect("truncate");
    disk.crash();

    let reopened = reopen(&disk).await;

    assert_eq!(reopened.segment.local_end(), SequenceNumber::new(102));
}

#[tokio::test]
async fn reopened_copy_keeps_detecting_damage_to_what_it_found() {
    let disk = MemoryDisk::new();
    segment_with_a_mark_over_three_batches(&disk).await;
    disk.crash();
    let mut reopened = reopen(&disk).await;
    reopened.segment.flush_mark().await.expect("flush mark");
    let byte_in_the_last_batch = usize::try_from(reopened.segment.content_bytes()).expect("a small file") - 2;
    disk.corrupt_byte(PATH, byte_in_the_last_batch, 0x04);
    disk.crash();

    let reopened_again = SegmentFile::open(&disk, PATH, SEGMENT, PREALLOCATED_BYTES).await;

    assert!(matches!(reopened_again, Err(SegmentError::Corrupt { .. })));
}

// Truncation and the file's size.

#[tokio::test]
async fn truncating_at_a_batch_boundary_discards_what_follows() {
    let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;

    segment.truncate_to(SequenceNumber::new(104)).await.expect("truncate");

    assert_eq!(read_all(&segment).await, sequences(100, 104));
}

#[tokio::test]
async fn truncation_moves_the_end_of_the_copy() {
    let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;

    segment.truncate_to(SequenceNumber::new(104)).await.expect("truncate");

    assert_eq!(segment.local_end(), SequenceNumber::new(104));
}

#[tokio::test]
async fn truncation_survives_a_crash() {
    let disk = MemoryDisk::new();
    let mut segment = segment_with_three_batches(&disk).await;
    segment.truncate_to(SequenceNumber::new(102)).await.expect("truncate");
    disk.crash();

    let reopened = reopen(&disk).await;

    assert_eq!(reopened.segment.local_end(), SequenceNumber::new(102));
}

#[tokio::test]
async fn truncating_to_before_the_first_record_empties_the_copy() {
    let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;

    segment
        .truncate_to(SequenceNumber::new(FIRST_SEQUENCE - 1))
        .await
        .expect("truncate");

    assert_eq!(read_all(&segment).await, Vec::<u64>::new());
}

#[tokio::test]
async fn truncating_at_the_current_end_changes_nothing() {
    let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;

    segment.truncate_to(SequenceNumber::new(109)).await.expect("truncate");

    assert_eq!(read_all(&segment).await, sequences(100, 109));
}

#[tokio::test]
async fn truncating_inside_a_batch_is_refused() {
    let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;

    let refused = segment.truncate_to(SequenceNumber::new(106)).await;

    assert!(matches!(refused, Err(SegmentError::NotABatchBoundary { .. })));
}

#[tokio::test]
async fn refused_truncation_leaves_the_copy_unchanged() {
    let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;

    let _refused = segment.truncate_to(SequenceNumber::new(106)).await;

    assert_eq!(read_all(&segment).await, sequences(100, 109));
}

#[tokio::test]
async fn truncating_past_the_end_is_refused() {
    let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;

    let refused = segment.truncate_to(SequenceNumber::new(110)).await;

    assert!(matches!(refused, Err(SegmentError::NotABatchBoundary { .. })));
}

#[tokio::test]
async fn truncating_below_the_segment_is_refused() {
    let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;

    let refused = segment.truncate_to(SequenceNumber::new(50)).await;

    assert!(matches!(refused, Err(SegmentError::NotABatchBoundary { .. })));
}

#[tokio::test]
async fn segment_accepts_appends_after_a_truncation() {
    let mut segment = segment_with_three_batches(&MemoryDisk::new()).await;
    segment.truncate_to(SequenceNumber::new(104)).await.expect("truncate");

    segment.append(&[batch(105, 2)]).await.expect("append");

    assert_eq!(read_all(&segment).await, sequences(100, 106));
}

#[tokio::test]
async fn releasing_unused_space_shrinks_the_file_to_its_content() {
    let disk = MemoryDisk::new();
    let mut segment = segment_with_three_batches(&disk).await;

    segment.release_unused_space().await.expect("release");

    let file_bytes = disk.durable_content(PATH).expect("the file exists").len();
    assert_eq!(to_u64(file_bytes), segment.content_bytes());
}

#[tokio::test]
async fn full_file_grows_by_whole_preallocation_steps() {
    let disk = MemoryDisk::new();
    let mut segment = new_segment(&disk).await;
    let payload = Bytes::from(vec![0xAB; 3 * 1024]);

    for sequence in FIRST_SEQUENCE..FIRST_SEQUENCE + 2 {
        let large_batch = Batch::encode(SEGMENT, SequenceNumber::new(sequence), std::slice::from_ref(&payload))
            .expect("a payload of 3 KiB is within the limits");
        segment.append(&[large_batch]).await.expect("append");
    }

    let file_bytes = disk.durable_content(PATH).expect("the file exists").len();
    assert_eq!(to_u64(file_bytes), 2 * PREALLOCATED_BYTES);
}

#[tokio::test]
async fn file_without_preallocation_grows_only_with_its_content() {
    let disk = MemoryDisk::new();
    let mut segment = SegmentFile::create(&disk, PATH, header(), 0)
        .await
        .expect("the path is free on a new disk");

    segment.append(&[batch(100, 3)]).await.expect("append");
    segment.flush().await.expect("flush");

    let file_bytes = disk.durable_content(PATH).expect("the file exists").len();
    assert_eq!(to_u64(file_bytes), segment.content_bytes());
}

// Scrubbing.

#[tokio::test]
async fn intact_segment_verifies() {
    let segment = segment_with_three_batches(&MemoryDisk::new()).await;

    let verified = segment.verify().await;

    assert!(verified.is_ok());
}

#[tokio::test]
async fn empty_segment_verifies() {
    let segment = new_segment(&MemoryDisk::new()).await;

    let verified = segment.verify().await;

    assert!(verified.is_ok());
}

#[tokio::test]
async fn silent_corruption_is_found_by_verification() {
    let disk = MemoryDisk::new();
    let segment = segment_with_three_batches(&disk).await;
    damage_the_first_batch(&disk);

    let verified = segment.verify().await;

    assert!(matches!(verified, Err(SegmentError::Corrupt { .. })));
}

#[tokio::test]
async fn verification_says_where_the_damaged_frame_starts() {
    let disk = MemoryDisk::new();
    let segment = segment_with_three_batches(&disk).await;
    damage_the_first_batch(&disk);

    let verified = segment.verify().await;

    assert!(matches!(
        verified,
        Err(SegmentError::Corrupt { offset_bytes, .. }) if offset_bytes == HEADER_END_OFFSET
    ));
}

#[tokio::test]
async fn silent_corruption_is_found_by_a_read() {
    let disk = MemoryDisk::new();
    let segment = segment_with_three_batches(&disk).await;
    damage_the_first_batch(&disk);

    let read = segment
        .read_frames(SequenceNumber::new(100), SequenceNumber::new(109), usize::MAX)
        .await;

    assert!(matches!(read, Err(SegmentError::Corrupt { .. })));
}

// Large files: chunked scans and the index.

/// Every batch of the large file holds one record, so the batch that holds a record starts with it.
#[tokio::test]
async fn large_segment_reads_start_at_the_wanted_record_across_chunk_boundaries() {
    let mut large = large_segment().await;

    for _read_number in 0..RANDOM_READS {
        let from_sequence = large.random.between(FIRST_SEQUENCE, large.last_sequence);

        let frames = large
            .segment
            .read_frames(
                SequenceNumber::new(from_sequence),
                SequenceNumber::new(large.last_sequence),
                256 * 1024,
            )
            .await
            .expect("read");

        assert_eq!(sequences_in(&frames).first(), Some(&from_sequence));
    }
}

#[tokio::test]
async fn large_segment_recovers_every_record_across_chunk_boundaries() {
    let large = large_segment().await;
    large.disk.crash();

    let reopened = reopen(&large.disk).await;

    assert_eq!(reopened.segment.local_end().get(), large.last_sequence);
}

#[tokio::test]
async fn large_segment_recovery_ends_at_the_end_of_data() {
    let large = large_segment().await;
    large.disk.crash();

    let reopened = reopen(&large.disk).await;

    assert_eq!(reopened.stop, ScanStop::EndOfData);
}
