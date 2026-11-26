//! Tests of uploading a sealed segment copy to the object store.
//!
//! # What is covered
//!
//! - The stored object is the whole sealed segment, whatever the chunk size it was read and verified with.
//! - A copy that is absent, open, sealed elsewhere, short or damaged is never uploaded.
//! - Object store trouble (an outage, a store that keeps other bytes than it was given, a store that never
//!   answers) fails the upload as `Unavailable`, which tells the controller to try again.
//!
//! # How the fixtures work
//!
//! Every test drives a real [`Replica`] on a [`MemoryDisk`] through its public interface, with the segment
//! and the requests of `crate::test_support`. The segment under test holds four batches of five records,
//! sequences 100 to 119. [`FaultyStore`] wraps the in-memory artifact store and misbehaves in one named
//! way.

use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bitacora_artifact::{MemoryArtifactStore, ObjectInfo};
use bitacora_segment::MemoryDisk;
use bitacora_segment::tiered::{TieredFooter, TieredIndex};
use bitacora_types::SequenceNumber;
use bitacora_types::replica_api::ReplicaApi;

use super::*;
use crate::test_support::{
    BYTE_IN_THE_FIRST_BATCH, FIRST_SEQUENCE, SEGMENT, SEGMENT_FILE, append_request, batch, descriptor, no_peers,
    open_replica_with, seal_request, sequences_in, small_segments,
};
use crate::{Replica, ReplicaSettings};

/// How many records each appended batch holds.
const RECORDS_PER_BATCH: u64 = 5;

/// How many batches the segment under test holds.
const BATCH_COUNT: u64 = 4;

/// The last sequence number of the segment under test.
const SEALED_END: u64 = FIRST_SEQUENCE + BATCH_COUNT * RECORDS_PER_BATCH - 1;

/// The key the tests upload to.
const OBJECT_KEY: &str = "streams/7/segments/3";

/// An upload chunk size that takes the whole segment under test in one read.
const ONE_READ_BYTES: usize = 1024;

/// One way an object store can misbehave.
#[derive(Clone, Copy, Debug)]
enum StoreFault {
    /// Every stored object has one bit flipped, as a store with a silent write error would.
    FlipsABitOfStoredObjects,
    /// No request is ever answered.
    NeverAnswers,
}

/// An in-memory artifact store with one fault.
struct FaultyStore {
    /// Where objects really go.
    objects: MemoryArtifactStore,
    /// How this store misbehaves.
    fault: StoreFault,
}

#[async_trait]
impl ArtifactStore for FaultyStore {
    async fn put_object(&self, object_key: &str, contents: Bytes) -> Result<(), ArtifactError> {
        match self.fault {
            StoreFault::FlipsABitOfStoredObjects => {
                let mut damaged_contents = contents.to_vec();
                let middle = damaged_contents.len() / 2;
                damaged_contents[middle] ^= 0x01;
                self.objects.put_object(object_key, Bytes::from(damaged_contents)).await
            }
            StoreFault::NeverAnswers => std::future::pending().await,
        }
    }

    async fn get_object(&self, object_key: &str) -> Result<Bytes, ArtifactError> {
        self.objects.get_object(object_key).await
    }

    async fn get_range(&self, object_key: &str, byte_range: Range<u64>) -> Result<Bytes, ArtifactError> {
        self.objects.get_range(object_key, byte_range).await
    }

    async fn delete_object(&self, object_key: &str) -> Result<(), ArtifactError> {
        self.objects.delete_object(object_key).await
    }

    async fn list_prefix(&self, key_prefix: &str) -> Result<Vec<ObjectInfo>, ArtifactError> {
        self.objects.list_prefix(key_prefix).await
    }
}

/// Settings with segment files small enough for an in-memory disk and the given upload chunk size.
fn settings_with_chunks_of(upload_chunk_bytes: usize) -> ReplicaSettings {
    ReplicaSettings {
        upload_chunk_bytes,
        ..small_segments()
    }
}

/// Opens a replica named `uploader` on `disk` that reads its copies in chunks of `upload_chunk_bytes` when
/// it uploads them. It has no peers and no object store yet.
async fn open_uploader(disk: &MemoryDisk, upload_chunk_bytes: usize) -> Replica {
    open_replica_with(
        disk,
        "uploader",
        settings_with_chunks_of(upload_chunk_bytes),
        no_peers(),
    )
    .await
}

/// Appends the batches of the segment under test to `replica`, one request per batch.
async fn append_batches(replica: &Replica) {
    for batch_number in 0..BATCH_COUNT {
        let first_sequence = FIRST_SEQUENCE + batch_number * RECORDS_PER_BATCH;
        replica
            .append(append_request(&[batch(first_sequence, RECORDS_PER_BATCH)], 0))
            .await
            .expect("the replica accepts consecutive batches");
    }
}

/// Seals the replica's copy of the segment under test at `sealed_end`.
async fn seal_at(replica: &Replica, sealed_end: u64) {
    replica
        .seal(seal_request(sealed_end))
        .await
        .expect("a seal at or beyond the copy's end is accepted");
}

/// A replica that holds the segment under test, sealed at its last record, and uploads to `artifact_store`.
async fn replica_with_sealed_copy(
    disk: &MemoryDisk,
    artifact_store: Arc<dyn ArtifactStore>,
    upload_chunk_bytes: usize,
) -> Replica {
    let replica = open_uploader(disk, upload_chunk_bytes)
        .await
        .with_artifact_store(artifact_store);
    append_batches(&replica).await;
    seal_at(&replica, SEALED_END).await;
    replica
}

/// A request to upload the segment under test, sealed at `sealed_end`.
fn upload_request(sealed_end: u64) -> UploadRequest {
    UploadRequest {
        segment: descriptor(),
        sealed_end: SequenceNumber::new(sealed_end),
        object_key: OBJECT_KEY.to_owned(),
    }
}

/// The sequence numbers of the records in the stored object, read the way a reader does: footer, index,
/// then the blocks.
async fn stored_sequences(artifact_store: &MemoryArtifactStore) -> Vec<u64> {
    let object = artifact_store
        .get_object(OBJECT_KEY)
        .await
        .expect("the upload stored the object");
    let footer = TieredFooter::decode(&object).expect("the stored object ends in a valid footer");
    let index_range = footer.index_range();
    let index_bytes = artifact_store
        .get_range(OBJECT_KEY, index_range)
        .await
        .expect("the footer points at the stored index");
    let index = TieredIndex::decode(&footer, &index_bytes).expect("the stored index is valid");
    let blocks_range = index.blocks_from(footer.segment.first_sequence, u64::MAX);
    let blocks = artifact_store
        .get_range(OBJECT_KEY, blocks_range)
        .await
        .expect("the index points at the stored blocks");
    sequences_in(&blocks)
}

#[tokio::test]
async fn uploaded_object_holds_every_record_of_the_sealed_segment() {
    let artifact_store = MemoryArtifactStore::new();
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), Arc::new(artifact_store.clone()), ONE_READ_BYTES).await;

    replica
        .upload_segment(upload_request(SEALED_END))
        .await
        .expect("a working object store takes the upload");

    let expected_sequences: Vec<u64> = (FIRST_SEQUENCE..=SEALED_END).collect();
    assert_eq!(stored_sequences(&artifact_store).await, expected_sequences);
}

#[tokio::test]
async fn upload_reports_the_size_of_the_stored_object() {
    let artifact_store = MemoryArtifactStore::new();
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), Arc::new(artifact_store.clone()), ONE_READ_BYTES).await;

    let uploaded = replica
        .upload_segment(upload_request(SEALED_END))
        .await
        .expect("a working object store takes the upload");

    let stored_object = artifact_store
        .get_object(OBJECT_KEY)
        .await
        .expect("the upload stored the object");
    assert_eq!(uploaded.size_bytes, stored_object.len() as u64);
}

#[tokio::test]
async fn copy_read_one_batch_at_a_time_uploads_the_same_object_as_in_one_read() {
    let store_of_small_reads = MemoryArtifactStore::new();
    let store_of_one_read = MemoryArtifactStore::new();
    // One byte per read still returns a whole batch: a read never splits one.
    let small_reads = replica_with_sealed_copy(&MemoryDisk::new(), Arc::new(store_of_small_reads.clone()), 1).await;
    let one_read = replica_with_sealed_copy(&MemoryDisk::new(), Arc::new(store_of_one_read.clone()), 1024 * 1024).await;

    small_reads
        .upload_segment(upload_request(SEALED_END))
        .await
        .expect("a working object store takes the upload");
    one_read
        .upload_segment(upload_request(SEALED_END))
        .await
        .expect("a working object store takes the upload");

    assert_eq!(
        store_of_small_reads.get_object(OBJECT_KEY).await,
        store_of_one_read.get_object(OBJECT_KEY).await
    );
}

#[tokio::test]
async fn upload_can_be_repeated() {
    let artifact_store = MemoryArtifactStore::new();
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), Arc::new(artifact_store.clone()), ONE_READ_BYTES).await;
    let first_upload = replica.upload_segment(upload_request(SEALED_END)).await;

    let second_upload = replica.upload_segment(upload_request(SEALED_END)).await;

    assert_eq!(second_upload, first_upload);
}

#[tokio::test]
async fn upload_leaves_the_local_copy_readable() {
    let artifact_store = MemoryArtifactStore::new();
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), Arc::new(artifact_store.clone()), ONE_READ_BYTES).await;

    replica
        .upload_segment(upload_request(SEALED_END))
        .await
        .expect("a working object store takes the upload");

    let statuses = replica
        .list_segments()
        .await
        .expect("a running replica lists its segments");
    assert_eq!(statuses[0].local_end, Some(SequenceNumber::new(SEALED_END)));
}

#[tokio::test]
async fn replica_without_an_object_store_refuses_to_upload() {
    let replica = open_uploader(&MemoryDisk::new(), ONE_READ_BYTES).await;
    append_batches(&replica).await;
    seal_at(&replica, SEALED_END).await;

    let refused = replica.upload_segment(upload_request(SEALED_END)).await;

    assert!(matches!(refused, Err(ReplicaError::InvalidRequest { .. })));
}

#[tokio::test]
async fn segment_the_replica_does_not_hold_is_not_found() {
    let artifact_store = MemoryArtifactStore::new();
    let replica = open_uploader(&MemoryDisk::new(), ONE_READ_BYTES)
        .await
        .with_artifact_store(Arc::new(artifact_store));

    let refused = replica.upload_segment(upload_request(SEALED_END)).await;

    assert_eq!(refused, Err(ReplicaError::SegmentNotFound { segment: SEGMENT }));
}

#[tokio::test]
async fn open_segment_is_not_uploaded() {
    let artifact_store = MemoryArtifactStore::new();
    let replica = open_uploader(&MemoryDisk::new(), ONE_READ_BYTES)
        .await
        .with_artifact_store(Arc::new(artifact_store.clone()));
    append_batches(&replica).await;

    let refused = replica.upload_segment(upload_request(SEALED_END)).await;

    assert!(matches!(refused, Err(ReplicaError::InvalidRequest { .. })));
    assert_eq!(artifact_store.list_prefix("").await, Ok(Vec::new()));
}

#[tokio::test]
async fn copy_sealed_at_another_end_than_requested_is_not_uploaded() {
    let artifact_store = MemoryArtifactStore::new();
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), Arc::new(artifact_store.clone()), ONE_READ_BYTES).await;

    let refused = replica.upload_segment(upload_request(SEALED_END - 1)).await;

    assert!(matches!(refused, Err(ReplicaError::InvalidRequest { .. })));
    assert_eq!(artifact_store.list_prefix("").await, Ok(Vec::new()));
}

#[tokio::test]
async fn copy_that_stops_short_of_its_sealed_end_is_not_uploaded() {
    let artifact_store = MemoryArtifactStore::new();
    let replica = open_uploader(&MemoryDisk::new(), ONE_READ_BYTES)
        .await
        .with_artifact_store(Arc::new(artifact_store.clone()));
    append_batches(&replica).await;
    // The segment was sealed past what this replica received: its copy awaits repair.
    let end_beyond_the_copy = SEALED_END + RECORDS_PER_BATCH;
    seal_at(&replica, end_beyond_the_copy).await;

    let refused = replica.upload_segment(upload_request(end_beyond_the_copy)).await;

    assert!(matches!(refused, Err(ReplicaError::Storage { .. })));
    assert_eq!(artifact_store.list_prefix("").await, Ok(Vec::new()));
}

#[tokio::test]
async fn damaged_copy_is_not_uploaded() {
    let disk = MemoryDisk::new();
    let artifact_store = MemoryArtifactStore::new();
    let replica = replica_with_sealed_copy(&disk, Arc::new(artifact_store.clone()), ONE_READ_BYTES).await;
    disk.corrupt_byte(SEGMENT_FILE, BYTE_IN_THE_FIRST_BATCH, 0x01);

    let refused = replica.upload_segment(upload_request(SEALED_END)).await;

    assert!(matches!(refused, Err(ReplicaError::Storage { .. })));
    assert_eq!(artifact_store.list_prefix("").await, Ok(Vec::new()));
}

#[tokio::test]
async fn object_store_outage_fails_the_upload_as_unavailable() {
    let artifact_store = MemoryArtifactStore::new();
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), Arc::new(artifact_store.clone()), ONE_READ_BYTES).await;
    artifact_store.set_unavailable(true);

    let failed = replica.upload_segment(upload_request(SEALED_END)).await;

    assert!(matches!(failed, Err(ReplicaError::Unavailable { .. })));
}

#[tokio::test]
async fn upload_succeeds_once_the_object_store_is_back() {
    let artifact_store = MemoryArtifactStore::new();
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), Arc::new(artifact_store.clone()), ONE_READ_BYTES).await;
    artifact_store.set_unavailable(true);
    let _failed = replica.upload_segment(upload_request(SEALED_END)).await;
    artifact_store.set_unavailable(false);

    let retried = replica.upload_segment(upload_request(SEALED_END)).await;

    assert!(retried.is_ok(), "{retried:?}");
}

#[tokio::test]
async fn object_stored_with_other_bytes_fails_verification() {
    let faulty_store = FaultyStore {
        objects: MemoryArtifactStore::new(),
        fault: StoreFault::FlipsABitOfStoredObjects,
    };
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), Arc::new(faulty_store), ONE_READ_BYTES).await;

    let failed = replica.upload_segment(upload_request(SEALED_END)).await;

    assert!(matches!(failed, Err(ReplicaError::Unavailable { .. })));
}

#[tokio::test(start_paused = true)]
async fn object_store_that_never_answers_fails_the_upload_after_its_timeout() {
    let faulty_store = FaultyStore {
        objects: MemoryArtifactStore::new(),
        fault: StoreFault::NeverAnswers,
    };
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), Arc::new(faulty_store), ONE_READ_BYTES).await;
    let asked_at = tokio::time::Instant::now();

    let failed = replica.upload_segment(upload_request(SEALED_END)).await;

    assert!(matches!(failed, Err(ReplicaError::Unavailable { .. })));
    assert_eq!(asked_at.elapsed(), ReplicaSettings::default().object_store_timeout);
}
