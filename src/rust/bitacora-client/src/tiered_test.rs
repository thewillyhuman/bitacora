//! Tests of reading a sealed segment from the object store.
//!
//! # What is covered
//!
//! - Reads return the records from the requested sequence number on, block by block, to the sealed end.
//! - An object that is not the expected segment, or whose footer, index or batches are damaged, is refused.
//! - Object store trouble is reported as such and bounded by the request timeout.
//!
//! # How the fixtures work
//!
//! `store_with_segment` builds the tiered object of a segment of 40 batches of about 10 KiB (several
//! blocks), sequences 100 to 299, and stores it in an in-memory artifact store. Faults are injected with
//! the store's own switches.

use bitacora_artifact::MemoryArtifactStore;
use bitacora_segment::format::{Batch, SegmentHeader};
use bitacora_segment::tiered::encode_tiered_segment;
use bitacora_types::{Epoch, SegmentId, StreamId, Timestamp};
use bytes::{BufMut, BytesMut};

use super::*;

/// The segment the stored object holds.
const SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));
/// Another segment, which the object must not be taken for.
const OTHER_SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(4));
/// The sequence number of the segment's first record.
const FIRST_SEQUENCE: u64 = 100;
/// How many batches the segment holds.
const BATCH_COUNT: u64 = 40;
/// How many records each batch holds.
const RECORDS_PER_BATCH: u64 = 5;
/// The segment's last sequence number.
const SEALED_END: SequenceNumber = SequenceNumber::new(FIRST_SEQUENCE + BATCH_COUNT * RECORDS_PER_BATCH - 1);
/// The key the object is stored under.
const OBJECT_KEY: &str = "streams/7/segments/3";
/// The request timeout the tests open segments with.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// A byte limit that fetches one block per read.
const ONE_BLOCK: usize = 1;

/// The batch frames of the segment: records of 2 KiB whose first bytes name their sequence number.
fn segment_frames() -> Bytes {
    let mut frames = BytesMut::new();
    for batch_number in 0..BATCH_COUNT {
        let first_sequence = FIRST_SEQUENCE + batch_number * RECORDS_PER_BATCH;
        let mut payloads = Vec::new();
        for sequence in first_sequence..first_sequence + RECORDS_PER_BATCH {
            let mut payload = format!("record-{sequence}").into_bytes();
            payload.resize(2_000, b'.');
            payloads.push(Bytes::from(payload));
        }
        let batch = Batch::encode(SEGMENT, SequenceNumber::new(first_sequence), &payloads).expect("valid batch");
        frames.put_slice(batch.frame());
    }
    frames.freeze()
}

/// Stores the segment's tiered object and returns the store and the record the metadata store would hold.
async fn store_with_segment() -> (MemoryArtifactStore, TieredRecord) {
    let segment_header = SegmentHeader {
        address: SEGMENT,
        epoch: Epoch::new(5),
        first_sequence: SequenceNumber::new(FIRST_SEQUENCE),
    };
    let object = encode_tiered_segment(segment_header, SEALED_END, &segment_frames()).expect("valid segment");
    let tiered = TieredRecord {
        object_key: OBJECT_KEY.to_owned(),
        size_bytes: object.len() as u64,
        uploaded_at: Timestamp::from_unix_millis(0),
        are_local_copies_dropped: true,
    };
    let artifact_store = MemoryArtifactStore::new();
    artifact_store.put_object(OBJECT_KEY, object).await.expect("put");
    (artifact_store, tiered)
}

/// Opens the stored object as the segment it really is.
async fn open_segment(
    artifact_store: &MemoryArtifactStore,
    tiered: &TieredRecord,
) -> Result<TieredSegment, TieredReadError> {
    TieredSegment::open(artifact_store, SEGMENT, SEALED_END, tiered, REQUEST_TIMEOUT).await
}

/// The sequence numbers of `records`.
fn sequences_of(records: &[LogRecord]) -> Vec<u64> {
    let mut sequences = Vec::new();
    for record in records {
        sequences.push(record.sequence.get());
    }
    sequences
}

#[tokio::test]
async fn read_starts_exactly_at_the_requested_sequence() {
    let (artifact_store, tiered) = store_with_segment().await;
    let segment = open_segment(&artifact_store, &tiered).await.expect("opens");
    let middle_of_a_batch = SequenceNumber::new(FIRST_SEQUENCE + 7);

    let records = segment
        .read_from(&artifact_store, middle_of_a_batch, ONE_BLOCK)
        .await
        .expect("read");

    assert_eq!(records[0].sequence, middle_of_a_batch);
}

#[tokio::test]
async fn read_returns_the_payloads_that_were_written() {
    let (artifact_store, tiered) = store_with_segment().await;
    let segment = open_segment(&artifact_store, &tiered).await.expect("opens");

    let records = segment
        .read_from(&artifact_store, SequenceNumber::new(FIRST_SEQUENCE), ONE_BLOCK)
        .await
        .expect("read");

    assert!(records[0].payload.starts_with(b"record-100."));
}

#[tokio::test]
async fn reads_block_by_block_reach_every_record_once() {
    let (artifact_store, tiered) = store_with_segment().await;
    let segment = open_segment(&artifact_store, &tiered).await.expect("opens");
    let mut next_sequence = SequenceNumber::new(FIRST_SEQUENCE);
    let mut read_sequences = Vec::new();
    let mut read_count = 0;

    while next_sequence <= SEALED_END {
        let records = segment
            .read_from(&artifact_store, next_sequence, ONE_BLOCK)
            .await
            .expect("read");
        read_sequences.extend(sequences_of(&records));
        next_sequence = records.last().expect("records").sequence.next();
        read_count += 1;
    }

    let every_sequence: Vec<u64> = (FIRST_SEQUENCE..=SEALED_END.get()).collect();
    assert_eq!(read_sequences, every_sequence);
    assert!(read_count >= 5, "only {read_count} reads: the segment is one block");
}

#[tokio::test]
async fn byte_limit_larger_than_the_segment_reads_it_in_one_request() {
    let (artifact_store, tiered) = store_with_segment().await;
    let segment = open_segment(&artifact_store, &tiered).await.expect("opens");

    let records = segment
        .read_from(&artifact_store, SequenceNumber::new(FIRST_SEQUENCE), usize::MAX)
        .await
        .expect("read");

    assert_eq!(records.last().expect("records").sequence, SEALED_END);
}

#[tokio::test]
async fn read_past_the_sealed_end_returns_nothing() {
    let (artifact_store, tiered) = store_with_segment().await;
    let segment = open_segment(&artifact_store, &tiered).await.expect("opens");

    let records = segment
        .read_from(&artifact_store, SEALED_END.next(), ONE_BLOCK)
        .await
        .expect("read");

    assert_eq!(records, Vec::new());
}

#[tokio::test]
async fn object_of_another_segment_is_refused() {
    let (artifact_store, tiered) = store_with_segment().await;

    let refused = TieredSegment::open(&artifact_store, OTHER_SEGMENT, SEALED_END, &tiered, REQUEST_TIMEOUT).await;

    assert!(matches!(refused, Err(TieredReadError::WrongSegment { .. })));
}

#[tokio::test]
async fn object_sealed_elsewhere_than_the_metadata_says_is_refused() {
    let (artifact_store, tiered) = store_with_segment().await;

    let refused = TieredSegment::open(&artifact_store, SEGMENT, SEALED_END.next(), &tiered, REQUEST_TIMEOUT).await;

    assert!(matches!(refused, Err(TieredReadError::WrongSegment { .. })));
}

#[tokio::test]
async fn recorded_size_smaller_than_a_footer_is_refused() {
    let (artifact_store, mut tiered) = store_with_segment().await;
    tiered.size_bytes = TIERED_FOOTER_BYTES as u64 - 1;

    let refused = open_segment(&artifact_store, &tiered).await;

    assert!(matches!(refused, Err(TieredReadError::WrongSegment { .. })));
}

#[tokio::test]
async fn recorded_size_that_misses_the_footer_is_refused() {
    let (artifact_store, mut tiered) = store_with_segment().await;
    tiered.size_bytes -= 1;

    let refused = open_segment(&artifact_store, &tiered).await;

    assert!(matches!(refused, Err(TieredReadError::Damaged { .. })));
}

#[tokio::test]
async fn damaged_footer_is_refused() {
    let (artifact_store, tiered) = store_with_segment().await;
    let offset_inside_the_footer = usize::try_from(tiered.size_bytes).expect("small object") - 20;
    artifact_store.corrupt_byte(OBJECT_KEY, offset_inside_the_footer, 0x01);

    let refused = open_segment(&artifact_store, &tiered).await;

    assert!(matches!(refused, Err(TieredReadError::Damaged { .. })));
}

#[tokio::test]
async fn damaged_index_is_refused() {
    let (artifact_store, tiered) = store_with_segment().await;
    let offset_inside_the_index = segment_frames().len() + 3;
    artifact_store.corrupt_byte(OBJECT_KEY, offset_inside_the_index, 0x01);

    let refused = open_segment(&artifact_store, &tiered).await;

    assert!(matches!(refused, Err(TieredReadError::Damaged { .. })));
}

#[tokio::test]
async fn damaged_batch_is_refused_when_it_is_read() {
    let (artifact_store, tiered) = store_with_segment().await;
    let segment = open_segment(&artifact_store, &tiered).await.expect("opens");
    let offset_inside_the_first_batch = 30;
    artifact_store.corrupt_byte(OBJECT_KEY, offset_inside_the_first_batch, 0x01);

    let refused = segment
        .read_from(&artifact_store, SequenceNumber::new(FIRST_SEQUENCE), ONE_BLOCK)
        .await;

    assert!(matches!(refused, Err(TieredReadError::InvalidBatches { .. })));
}

#[tokio::test]
async fn damage_in_one_block_leaves_the_other_blocks_readable() {
    let (artifact_store, tiered) = store_with_segment().await;
    let segment = open_segment(&artifact_store, &tiered).await.expect("opens");
    let offset_inside_the_first_batch = 30;
    artifact_store.corrupt_byte(OBJECT_KEY, offset_inside_the_first_batch, 0x01);

    let records = segment
        .read_from(&artifact_store, SEALED_END, ONE_BLOCK)
        .await
        .expect("the last block is intact");

    assert_eq!(sequences_of(&records), [SEALED_END.get()]);
}

#[tokio::test]
async fn missing_object_is_reported_as_a_store_failure() {
    let (artifact_store, tiered) = store_with_segment().await;
    artifact_store.delete_object(OBJECT_KEY).await.expect("delete");

    let failed = open_segment(&artifact_store, &tiered).await;

    assert!(matches!(
        failed,
        Err(TieredReadError::StoreFailed {
            reason: ArtifactError::NotFound { .. },
            ..
        })
    ));
}

#[tokio::test]
async fn object_store_outage_is_reported_as_a_store_failure() {
    let (artifact_store, tiered) = store_with_segment().await;
    let segment = open_segment(&artifact_store, &tiered).await.expect("opens");
    artifact_store.set_unavailable(true);

    let failed = segment
        .read_from(&artifact_store, SequenceNumber::new(FIRST_SEQUENCE), ONE_BLOCK)
        .await;

    assert!(matches!(failed, Err(TieredReadError::StoreFailed { .. })));
}

#[tokio::test(start_paused = true)]
async fn object_store_that_never_answers_times_out() {
    let (artifact_store, tiered) = store_with_segment().await;
    artifact_store.set_unresponsive(true);
    let asked_at = tokio::time::Instant::now();

    let failed = open_segment(&artifact_store, &tiered).await;

    assert!(matches!(failed, Err(TieredReadError::TimedOut { .. })));
    assert_eq!(asked_at.elapsed(), REQUEST_TIMEOUT);
}
