//! Tests of uploading a sealed segment copy to the object store.
//!
//! # What is covered
//!
//! - The stored object is the whole sealed segment, byte for byte what the encoder builds from all its
//!   frames at once, whatever the chunk size the copy was read and verified with.
//! - The bound on memory: however long the segment, the object store is handed one chunk at a time and is
//!   read back one chunk at a time, and the object is never stored or read in one piece.
//! - A copy that is absent, open, sealed elsewhere, short or damaged is never uploaded, and an upload that
//!   had already sent pieces when the copy turned out wrong is aborted.
//! - Object store trouble (an outage, a piece that is refused, a store that keeps other bytes than it was
//!   given, a store that stops answering at any step) fails the upload as `Unavailable`, which tells the
//!   controller to try again, and leaves no object behind where the upload was not completed.
//! - A copy that changes between its two readings is not reported as uploaded.
//!
//! # How the fixtures work
//!
//! Every test drives a real [`Replica`] on a [`MemoryDisk`] through its public interface, with the segment
//! and the requests of `crate::test_support`. The segment under test holds four batches of five records,
//! sequences 100 to 119; the long segment of the tests of the bound holds four hundred. [`WatchedStore`]
//! wraps the in-memory artifact store, writes down what an upload asks of it, and misbehaves in one named
//! way.
//!
//! With an upload chunk of one byte, a read returns one batch, so the pieces of an upload are known: one
//! for each batch, in order, and then the end of the object.

use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bitacora_artifact::{MemoryArtifactStore, ObjectInfo};
use bitacora_segment::MemoryDisk;
use bitacora_segment::format::SEGMENT_HEADER_BYTES;
use bitacora_segment::tiered::{TieredFooter, TieredIndex, encode_tiered_segment};
use bitacora_types::SequenceNumber;
use bitacora_types::replica_api::ReplicaApi;

use super::*;
use crate::test_support::{
    BYTE_IN_A_FRAME, BYTE_IN_THE_FIRST_BATCH, FIRST_SEQUENCE, SEGMENT, SEGMENT_EPOCH, SEGMENT_FILE, append_request,
    batch, descriptor, frames_of, no_peers, open_replica_with, read_request, seal_request, sequences_in,
    small_segments,
};
use crate::{Replica, ReplicaSettings};

/// How many records each appended batch holds.
const RECORDS_PER_BATCH: u64 = 5;

/// How many batches the segment under test holds.
const BATCH_COUNT: u64 = 4;

/// The last sequence number of the segment under test.
const SEALED_END: u64 = FIRST_SEQUENCE + BATCH_COUNT * RECORDS_PER_BATCH - 1;

/// A sealed end one batch past the last record of the segment under test.
const END_BEYOND_THE_COPY: u64 = SEALED_END + RECORDS_PER_BATCH;

/// How many batches the long segment holds: enough to be many chunks of [`SMALL_CHUNK_BYTES`].
const LONG_BATCH_COUNT: u64 = 400;

/// The last sequence number of the long segment.
const LONG_SEALED_END: u64 = FIRST_SEQUENCE + LONG_BATCH_COUNT * RECORDS_PER_BATCH - 1;

/// The key the tests upload to.
const OBJECT_KEY: &str = "streams/7/segments/3";

/// An upload chunk size that takes the whole segment under test in one read.
const ONE_READ_BYTES: usize = 1024;

/// An upload chunk size that takes one batch in each read: a read never splits a batch.
const ONE_BATCH_PER_READ: usize = 1;

/// An upload chunk size of a few batches, for the long segment.
const SMALL_CHUNK_BYTES: usize = 512;

/// The number of the piece that holds the second batch, when each read takes one batch.
const PIECE_OF_THE_SECOND_BATCH: usize = 1;

/// The number of the piece that ends the object of the segment under test, when each read takes one
/// batch: the block index and the footer follow one piece for each batch.
const PIECE_OF_THE_END_OF_THE_OBJECT: usize = 4;

/// One way an object store can misbehave while a segment is uploaded to it.
#[derive(Clone, Copy, Debug)]
enum StoreFault {
    /// The store does what it is asked.
    None,
    /// The piece with this number, counted from zero, is stored with one bit flipped, as by a store with
    /// a silent write error.
    FlipsABitOfPiece(usize),
    /// The piece with this number, counted from zero, is refused, as by a store that fails in the middle
    /// of an upload.
    RefusesPiece(usize),
    /// The request to start an upload is never answered.
    NeverStartsAnUpload,
    /// Uploads start, and no piece is ever taken.
    NeverTakesAPiece,
    /// Pieces are taken, and the completion is never answered.
    NeverCompletesAnUpload,
    /// Objects are stored, and no read of them is ever answered.
    NeverAnswersARead,
}

/// What a [`WatchedStore`] was asked while segments were uploaded through it.
#[derive(Clone, Copy, Debug, Default)]
struct StoreUse {
    /// How many pieces were handed to uploads.
    piece_count: usize,
    /// The size of the largest piece handed to an upload.
    largest_piece_bytes: usize,
    /// The size of the largest range of an object that was read.
    largest_range_bytes: u64,
    /// How many times an object was stored or read whole, in one request.
    whole_object_requests: usize,
    /// How many uploads were completed.
    completed_uploads: usize,
    /// How many uploads were aborted.
    aborted_uploads: usize,
}

/// An in-memory artifact store that writes down how it is used and has one fault.
struct WatchedStore {
    /// Where objects really go.
    objects: MemoryArtifactStore,
    /// How this store misbehaves.
    fault: StoreFault,
    /// What was asked of this store so far.
    store_use: Arc<Mutex<StoreUse>>,
    /// A disk whose copy of the segment under test gets a flipped bit in its first batch at the moment
    /// an upload is completed: between the two readings of the copy.
    disk_damaged_at_completion: Option<MemoryDisk>,
}

#[async_trait]
impl ArtifactStore for WatchedStore {
    async fn put_object(&self, object_key: &str, contents: Bytes) -> Result<(), ArtifactError> {
        note(&self.store_use, |store_use| store_use.whole_object_requests += 1);
        self.objects.put_object(object_key, contents).await
    }

    async fn get_object(&self, object_key: &str) -> Result<Bytes, ArtifactError> {
        note(&self.store_use, |store_use| store_use.whole_object_requests += 1);
        self.objects.get_object(object_key).await
    }

    async fn get_range(&self, object_key: &str, byte_range: Range<u64>) -> Result<Bytes, ArtifactError> {
        let range_bytes = byte_range.end.saturating_sub(byte_range.start);
        note(&self.store_use, |store_use| {
            store_use.largest_range_bytes = store_use.largest_range_bytes.max(range_bytes);
        });
        if matches!(self.fault, StoreFault::NeverAnswersARead) {
            return std::future::pending().await;
        }
        self.objects.get_range(object_key, byte_range).await
    }

    async fn delete_object(&self, object_key: &str) -> Result<(), ArtifactError> {
        self.objects.delete_object(object_key).await
    }

    async fn list_prefix(&self, key_prefix: &str) -> Result<Vec<ObjectInfo>, ArtifactError> {
        self.objects.list_prefix(key_prefix).await
    }

    async fn start_upload(&self, object_key: &str) -> Result<Box<dyn ObjectUpload>, ArtifactError> {
        if matches!(self.fault, StoreFault::NeverStartsAnUpload) {
            return std::future::pending().await;
        }
        let upload = self.objects.start_upload(object_key).await?;
        Ok(Box::new(WatchedUpload {
            upload,
            fault: self.fault,
            store_use: Arc::clone(&self.store_use),
            disk_damaged_at_completion: self.disk_damaged_at_completion.clone(),
            pieces_taken: 0,
        }))
    }
}

/// An upload of a [`WatchedStore`].
struct WatchedUpload {
    /// The upload of the in-memory store, which does the work.
    upload: Box<dyn ObjectUpload>,
    /// How the store misbehaves.
    fault: StoreFault,
    /// What was asked of the store so far.
    store_use: Arc<Mutex<StoreUse>>,
    /// The disk to damage when this upload is completed, if any.
    disk_damaged_at_completion: Option<MemoryDisk>,
    /// How many pieces this upload was handed so far.
    pieces_taken: usize,
}

#[async_trait]
impl ObjectUpload for WatchedUpload {
    async fn write(&mut self, piece: Bytes) -> Result<(), ArtifactError> {
        let piece_number = self.pieces_taken;
        self.pieces_taken += 1;
        note(&self.store_use, |store_use| {
            store_use.piece_count += 1;
            store_use.largest_piece_bytes = store_use.largest_piece_bytes.max(piece.len());
        });
        match self.fault {
            StoreFault::NeverTakesAPiece => std::future::pending().await,
            StoreFault::RefusesPiece(refused_piece) if refused_piece == piece_number => {
                Err(ArtifactError::Unavailable {
                    reason: format!("piece {piece_number} is refused"),
                })
            }
            StoreFault::FlipsABitOfPiece(damaged_piece) if damaged_piece == piece_number => {
                self.upload.write(with_a_flipped_bit(&piece)).await
            }
            _no_fault_for_this_piece => self.upload.write(piece).await,
        }
    }

    async fn complete(self: Box<Self>) -> Result<(), ArtifactError> {
        if matches!(self.fault, StoreFault::NeverCompletesAnUpload) {
            return std::future::pending().await;
        }
        note(&self.store_use, |store_use| store_use.completed_uploads += 1);
        if let Some(disk) = &self.disk_damaged_at_completion {
            disk.corrupt_byte(SEGMENT_FILE, BYTE_IN_THE_FIRST_BATCH, 0x01);
        }
        self.upload.complete().await
    }

    async fn abort(self: Box<Self>) -> Result<(), ArtifactError> {
        note(&self.store_use, |store_use| store_use.aborted_uploads += 1);
        self.upload.abort().await
    }
}

/// Writes down one thing a store was asked.
fn note(store_use: &Mutex<StoreUse>, change: impl FnOnce(&mut StoreUse)) {
    let mut store_use = store_use.lock().expect("no test panics while it takes notes");
    change(&mut store_use);
}

/// The bytes of `piece` with one bit flipped in the middle.
fn with_a_flipped_bit(piece: &Bytes) -> Bytes {
    let mut damaged_piece = piece.to_vec();
    let middle = damaged_piece.len() / 2;
    damaged_piece[middle] ^= 0x01;
    Bytes::from(damaged_piece)
}

/// A watched store, as a test holds it: the store to give a replica, and what to look at afterwards.
struct Watched {
    /// The store double.
    artifact_store: Arc<WatchedStore>,
    /// The in-memory store behind it, which holds what was really stored and is not watched.
    objects: MemoryArtifactStore,
}

impl Watched {
    /// The store, as a replica is given it.
    fn store(&self) -> Arc<dyn ArtifactStore> {
        Arc::<WatchedStore>::clone(&self.artifact_store)
    }

    /// What was asked of the store so far.
    fn store_use(&self) -> StoreUse {
        *self
            .artifact_store
            .store_use
            .lock()
            .expect("no test panics while it takes notes")
    }

    /// The keys and sizes of the objects that are really in the store.
    async fn stored_objects(&self) -> Vec<ObjectInfo> {
        self.objects
            .list_prefix("")
            .await
            .expect("the in-memory store lists its objects")
    }
}

/// A store with the given fault that writes down how it is used.
fn watched_store(fault: StoreFault) -> Watched {
    watched_store_damaging(fault, None)
}

/// A store with the given fault that, when an upload is completed, flips a bit in the first batch of the
/// copy of the segment under test on `disk_damaged_at_completion`, if there is one.
fn watched_store_damaging(fault: StoreFault, disk_damaged_at_completion: Option<MemoryDisk>) -> Watched {
    let objects = MemoryArtifactStore::new();
    let artifact_store = Arc::new(WatchedStore {
        objects: objects.clone(),
        fault,
        store_use: Arc::new(Mutex::new(StoreUse::default())),
        disk_damaged_at_completion,
    });
    Watched {
        artifact_store,
        objects,
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

/// Appends `batch_count` batches of [`RECORDS_PER_BATCH`] records to `replica`, from the first sequence
/// number of the segment on, one request per batch.
async fn append_batches(replica: &Replica, batch_count: u64) {
    for batch_number in 0..batch_count {
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
    append_batches(&replica, BATCH_COUNT).await;
    seal_at(&replica, SEALED_END).await;
    replica
}

/// A replica that holds the long segment, sealed at its last record, uploads to `artifact_store`, and
/// reads its copy in chunks of [`SMALL_CHUNK_BYTES`].
async fn replica_with_long_sealed_copy(artifact_store: Arc<dyn ArtifactStore>) -> Replica {
    let replica = open_uploader(&MemoryDisk::new(), SMALL_CHUNK_BYTES)
        .await
        .with_artifact_store(artifact_store);
    append_batches(&replica, LONG_BATCH_COUNT).await;
    seal_at(&replica, LONG_SEALED_END).await;
    replica
}

/// A replica whose copy of the segment under test was sealed one batch past what it received: the copy
/// awaits repair, and stops short of its sealed end.
async fn replica_with_short_copy(artifact_store: Arc<dyn ArtifactStore>) -> Replica {
    let replica = open_uploader(&MemoryDisk::new(), ONE_BATCH_PER_READ)
        .await
        .with_artifact_store(artifact_store);
    append_batches(&replica, BATCH_COUNT).await;
    seal_at(&replica, END_BEYOND_THE_COPY).await;
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

/// The longest chunk a read of the long segment in chunks of [`SMALL_CHUNK_BYTES`] can return: a read
/// stops after the first batch that takes it to its target, so it stays below the target and one batch.
/// The last batch of the segment is its largest, because its sequence numbers have the most digits.
fn longest_small_chunk_bytes() -> usize {
    let largest_batch = batch(LONG_SEALED_END + 1 - RECORDS_PER_BATCH, RECORDS_PER_BATCH);
    SMALL_CHUNK_BYTES + largest_batch.frame().len()
}

/// The offset in the records file of a byte of a record of the last batch of the segment under test.
fn byte_in_the_last_batch() -> usize {
    let mut batches_before_the_last = Vec::new();
    for batch_number in 0..BATCH_COUNT - 1 {
        batches_before_the_last.push(batch(
            FIRST_SEQUENCE + batch_number * RECORDS_PER_BATCH,
            RECORDS_PER_BATCH,
        ));
    }
    SEGMENT_HEADER_BYTES + frames_of(&batches_before_the_last).len() + BYTE_IN_A_FRAME
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

/// The object the encoder builds from all the frames of the replica's copy of the segment under test at
/// once: what an upload must store, however it goes about it.
async fn object_built_at_once(replica: &Replica) -> Bytes {
    let whole_copy = replica
        .read(read_request(FIRST_SEQUENCE))
        .await
        .expect("a sealed copy is readable to its end");
    let segment_header = SegmentHeader {
        address: SEGMENT,
        epoch: SEGMENT_EPOCH,
        first_sequence: SequenceNumber::new(FIRST_SEQUENCE),
    };
    encode_tiered_segment(
        segment_header,
        SequenceNumber::new(SEALED_END),
        &whole_copy.batch_frames,
    )
    .expect("the frames of a sealed copy are the sealed segment")
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

/// The object is the format's, not the upload's: sent in one chunk, it is byte for byte what the encoder
/// makes of all the frames at once.
#[tokio::test]
async fn copy_uploaded_in_one_chunk_is_stored_as_the_object_the_encoder_builds_at_once() {
    let artifact_store = MemoryArtifactStore::new();
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), Arc::new(artifact_store.clone()), ONE_READ_BYTES).await;

    replica
        .upload_segment(upload_request(SEALED_END))
        .await
        .expect("a working object store takes the upload");

    assert_eq!(
        artifact_store.get_object(OBJECT_KEY).await,
        Ok(object_built_at_once(&replica).await)
    );
}

#[tokio::test]
async fn copy_uploaded_one_batch_at_a_time_is_stored_as_the_object_the_encoder_builds_at_once() {
    let artifact_store = MemoryArtifactStore::new();
    let replica =
        replica_with_sealed_copy(&MemoryDisk::new(), Arc::new(artifact_store.clone()), ONE_BATCH_PER_READ).await;

    replica
        .upload_segment(upload_request(SEALED_END))
        .await
        .expect("a working object store takes the upload");

    assert_eq!(
        artifact_store.get_object(OBJECT_KEY).await,
        Ok(object_built_at_once(&replica).await)
    );
}

#[tokio::test]
async fn copy_read_one_batch_at_a_time_is_sent_as_one_piece_for_each_batch_and_the_end_of_the_object() {
    let watched = watched_store(StoreFault::None);
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), watched.store(), ONE_BATCH_PER_READ).await;

    replica
        .upload_segment(upload_request(SEALED_END))
        .await
        .expect("a working object store takes the upload");

    assert_eq!(watched.store_use().piece_count, PIECE_OF_THE_END_OF_THE_OBJECT + 1);
}

/// The bound on what an upload holds. The long segment is many times a chunk, and still no piece is longer
/// than a chunk can be: the replica reads a chunk, hands it over, and reads the next.
#[tokio::test]
async fn upload_hands_the_store_one_chunk_at_a_time_however_long_the_segment() {
    let watched = watched_store(StoreFault::None);
    let replica = replica_with_long_sealed_copy(watched.store()).await;

    let uploaded = replica
        .upload_segment(upload_request(LONG_SEALED_END))
        .await
        .expect("a working object store takes the upload");

    let longest_chunk_bytes = longest_small_chunk_bytes();
    assert!(
        uploaded.size_bytes > 40 * longest_chunk_bytes as u64,
        "the segment must be many chunks long for this test to say anything: {uploaded:?}"
    );
    assert!(
        watched.store_use().largest_piece_bytes < longest_chunk_bytes,
        "{:?}",
        watched.store_use()
    );
}

/// The other half of the bound: the object is verified in ranges of a chunk, each compared and dropped
/// before the next is read.
#[tokio::test]
async fn upload_reads_the_stored_object_back_one_chunk_at_a_time_however_long_the_segment() {
    let watched = watched_store(StoreFault::None);
    let replica = replica_with_long_sealed_copy(watched.store()).await;

    replica
        .upload_segment(upload_request(LONG_SEALED_END))
        .await
        .expect("a working object store takes the upload");

    assert!(
        watched.store_use().largest_range_bytes < longest_small_chunk_bytes() as u64,
        "{:?}",
        watched.store_use()
    );
}

#[tokio::test]
async fn upload_never_stores_or_reads_the_object_in_one_piece() {
    let watched = watched_store(StoreFault::None);
    let replica = replica_with_long_sealed_copy(watched.store()).await;

    replica
        .upload_segment(upload_request(LONG_SEALED_END))
        .await
        .expect("a working object store takes the upload");

    assert_eq!(watched.store_use().whole_object_requests, 0);
}

#[tokio::test]
async fn long_segment_is_uploaded_whole() {
    let watched = watched_store(StoreFault::None);
    let replica = replica_with_long_sealed_copy(watched.store()).await;

    replica
        .upload_segment(upload_request(LONG_SEALED_END))
        .await
        .expect("a working object store takes the upload");

    let expected_sequences: Vec<u64> = (FIRST_SEQUENCE..=LONG_SEALED_END).collect();
    assert_eq!(stored_sequences(&watched.objects).await, expected_sequences);
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
    append_batches(&replica, BATCH_COUNT).await;
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
    append_batches(&replica, BATCH_COUNT).await;

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

/// A copy that is refused for what its status says costs the object store nothing: the store of this test
/// would never answer.
#[tokio::test(start_paused = true)]
async fn copy_that_is_refused_for_its_seal_starts_no_upload() {
    let watched = watched_store(StoreFault::NeverStartsAnUpload);
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), watched.store(), ONE_READ_BYTES).await;

    let refused = replica.upload_segment(upload_request(SEALED_END - 1)).await;

    assert!(matches!(refused, Err(ReplicaError::InvalidRequest { .. })));
}

#[tokio::test]
async fn copy_that_stops_short_of_its_sealed_end_is_not_uploaded() {
    let watched = watched_store(StoreFault::None);
    let replica = replica_with_short_copy(watched.store()).await;

    let refused = replica.upload_segment(upload_request(END_BEYOND_THE_COPY)).await;

    assert!(matches!(refused, Err(ReplicaError::Storage { .. })));
    assert_eq!(watched.stored_objects().await, Vec::new());
}

/// The copy is found short only when its last batch has been read and sent: the object store holds
/// every piece by then, and must be told to drop them.
#[tokio::test]
async fn upload_of_a_copy_that_stops_short_is_aborted_after_its_pieces_were_sent() {
    let watched = watched_store(StoreFault::None);
    let replica = replica_with_short_copy(watched.store()).await;

    let _refused = replica.upload_segment(upload_request(END_BEYOND_THE_COPY)).await;

    let store_use = watched.store_use();
    assert_eq!(
        (
            store_use.piece_count,
            store_use.completed_uploads,
            store_use.aborted_uploads
        ),
        (4, 0, 1)
    );
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

/// Damage in the last batch is met after the batches before it were sent.
#[tokio::test]
async fn copy_damaged_in_its_last_batch_is_not_uploaded() {
    let disk = MemoryDisk::new();
    let watched = watched_store(StoreFault::None);
    let replica = replica_with_sealed_copy(&disk, watched.store(), ONE_BATCH_PER_READ).await;
    disk.corrupt_byte(SEGMENT_FILE, byte_in_the_last_batch(), 0x01);

    let refused = replica.upload_segment(upload_request(SEALED_END)).await;

    assert!(matches!(refused, Err(ReplicaError::Storage { .. })));
    assert_eq!(watched.stored_objects().await, Vec::new());
}

#[tokio::test]
async fn upload_of_a_copy_damaged_in_its_last_batch_is_aborted() {
    let disk = MemoryDisk::new();
    let watched = watched_store(StoreFault::None);
    let replica = replica_with_sealed_copy(&disk, watched.store(), ONE_BATCH_PER_READ).await;
    disk.corrupt_byte(SEGMENT_FILE, byte_in_the_last_batch(), 0x01);

    let _refused = replica.upload_segment(upload_request(SEALED_END)).await;

    let store_use = watched.store_use();
    assert_eq!((store_use.completed_uploads, store_use.aborted_uploads), (0, 1));
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
async fn piece_the_object_store_refuses_fails_the_upload_as_unavailable() {
    let watched = watched_store(StoreFault::RefusesPiece(PIECE_OF_THE_SECOND_BATCH));
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), watched.store(), ONE_BATCH_PER_READ).await;

    let failed = replica.upload_segment(upload_request(SEALED_END)).await;

    assert!(matches!(failed, Err(ReplicaError::Unavailable { .. })));
}

/// An object store that fails half way must not be left with half an object, under the key or beside it.
#[tokio::test]
async fn upload_that_was_refused_a_piece_is_aborted_and_leaves_no_object() {
    let watched = watched_store(StoreFault::RefusesPiece(PIECE_OF_THE_SECOND_BATCH));
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), watched.store(), ONE_BATCH_PER_READ).await;

    let _failed = replica.upload_segment(upload_request(SEALED_END)).await;

    assert_eq!(watched.store_use().aborted_uploads, 1);
    assert_eq!(watched.stored_objects().await, Vec::new());
}

#[tokio::test]
async fn upload_that_was_refused_the_end_of_the_object_leaves_no_object() {
    let watched = watched_store(StoreFault::RefusesPiece(PIECE_OF_THE_END_OF_THE_OBJECT));
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), watched.store(), ONE_BATCH_PER_READ).await;

    let failed = replica.upload_segment(upload_request(SEALED_END)).await;

    assert!(matches!(failed, Err(ReplicaError::Unavailable { .. })));
    assert_eq!(watched.stored_objects().await, Vec::new());
}

#[tokio::test]
async fn batch_stored_with_other_bytes_fails_verification() {
    let watched = watched_store(StoreFault::FlipsABitOfPiece(PIECE_OF_THE_SECOND_BATCH));
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), watched.store(), ONE_BATCH_PER_READ).await;

    let failed = replica.upload_segment(upload_request(SEALED_END)).await;

    assert!(matches!(failed, Err(ReplicaError::Unavailable { .. })));
}

/// The frames of the object can be compared with the local copy; its index and footer are not in the
/// copy. They are verified all the same: against what a second reading of the copy says they must be.
#[tokio::test]
async fn end_of_the_object_stored_with_other_bytes_fails_verification() {
    let watched = watched_store(StoreFault::FlipsABitOfPiece(PIECE_OF_THE_END_OF_THE_OBJECT));
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), watched.store(), ONE_BATCH_PER_READ).await;

    let failed = replica.upload_segment(upload_request(SEALED_END)).await;

    assert!(matches!(failed, Err(ReplicaError::Unavailable { .. })));
}

/// The object is verified against a second reading of the local copy. If that reading does not give a
/// whole, undamaged segment, nothing can be said about the object, and the upload is not reported.
#[tokio::test]
async fn copy_damaged_between_its_two_readings_is_not_reported_as_uploaded() {
    let disk = MemoryDisk::new();
    let watched = watched_store_damaging(StoreFault::None, Some(disk.clone()));
    let replica = replica_with_sealed_copy(&disk, watched.store(), ONE_READ_BYTES).await;

    let failed = replica.upload_segment(upload_request(SEALED_END)).await;

    assert!(matches!(failed, Err(ReplicaError::Storage { .. })), "{failed:?}");
}

#[tokio::test(start_paused = true)]
async fn object_store_that_never_starts_an_upload_fails_it_after_its_timeout() {
    let watched = watched_store(StoreFault::NeverStartsAnUpload);
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), watched.store(), ONE_READ_BYTES).await;
    let asked_at = tokio::time::Instant::now();

    let failed = replica.upload_segment(upload_request(SEALED_END)).await;

    assert!(matches!(failed, Err(ReplicaError::Unavailable { .. })));
    assert_eq!(asked_at.elapsed(), ReplicaSettings::default().object_store_timeout);
}

#[tokio::test(start_paused = true)]
async fn object_store_that_never_takes_a_piece_fails_the_upload_after_its_timeout() {
    let watched = watched_store(StoreFault::NeverTakesAPiece);
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), watched.store(), ONE_READ_BYTES).await;
    let asked_at = tokio::time::Instant::now();

    let failed = replica.upload_segment(upload_request(SEALED_END)).await;

    assert!(matches!(failed, Err(ReplicaError::Unavailable { .. })));
    assert_eq!(asked_at.elapsed(), ReplicaSettings::default().object_store_timeout);
}

#[tokio::test(start_paused = true)]
async fn upload_whose_piece_was_never_taken_is_aborted() {
    let watched = watched_store(StoreFault::NeverTakesAPiece);
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), watched.store(), ONE_READ_BYTES).await;

    let _failed = replica.upload_segment(upload_request(SEALED_END)).await;

    assert_eq!(watched.store_use().aborted_uploads, 1);
}

#[tokio::test(start_paused = true)]
async fn object_store_that_never_completes_an_upload_fails_it_after_its_timeout() {
    let watched = watched_store(StoreFault::NeverCompletesAnUpload);
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), watched.store(), ONE_READ_BYTES).await;
    let asked_at = tokio::time::Instant::now();

    let failed = replica.upload_segment(upload_request(SEALED_END)).await;

    assert!(matches!(failed, Err(ReplicaError::Unavailable { .. })));
    assert_eq!(asked_at.elapsed(), ReplicaSettings::default().object_store_timeout);
}

#[tokio::test(start_paused = true)]
async fn upload_that_was_never_completed_leaves_no_object() {
    let watched = watched_store(StoreFault::NeverCompletesAnUpload);
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), watched.store(), ONE_READ_BYTES).await;

    let _failed = replica.upload_segment(upload_request(SEALED_END)).await;

    assert_eq!(watched.stored_objects().await, Vec::new());
}

#[tokio::test(start_paused = true)]
async fn object_store_that_never_answers_a_read_fails_verification_after_its_timeout() {
    let watched = watched_store(StoreFault::NeverAnswersARead);
    let replica = replica_with_sealed_copy(&MemoryDisk::new(), watched.store(), ONE_READ_BYTES).await;
    let asked_at = tokio::time::Instant::now();

    let failed = replica.upload_segment(upload_request(SEALED_END)).await;

    assert!(matches!(failed, Err(ReplicaError::Unavailable { .. })));
    assert_eq!(asked_at.elapsed(), ReplicaSettings::default().object_store_timeout);
}
