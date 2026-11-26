//! Fixtures shared by the tests of this crate.
//!
//! # Why this file exists
//!
//! Every test file of the crate needs the same few things: a segment to talk about, batches of records
//! for it, requests built from them, a copy or a replica on an in-memory disk. Written once per file they
//! drift apart, and a reader of one test file has to learn its dialect. Here they are written once.
//!
//! # What it contains
//!
//! - The segment under test: [`SEGMENT`], its epoch, its first sequence number, the path of its file.
//! - Batches and requests: [`batch`], [`append_request`], [`heartbeat`], [`read_request`] and friends.
//! - Copies: [`new_copy`], [`copy_with_records`], [`reload`], [`readable_sequences`], with the two
//!   admissions [`ROOMY`] and [`NEARLY_FULL`].
//! - Replicas: [`open_replica`], [`open_replica_with`], [`small_segments`], [`no_peers`], and
//!   [`copy_of`] with [`CopySummary`] to look at what a replica holds.
//! - Repairs: [`source_sealed_at`], [`open_target`] and [`repair_request`].
//!
//! # How it works
//!
//! Everything concerns segment 3 of stream 7, written in epoch 5 and starting at record 100. A record's
//! payload names its own sequence number (`record-104`), so a test that reads records back can tell
//! which ones it got. Disks are `MemoryDisk`s, which a test can crash, corrupt and make refuse flushes.
//!
//! # Invariants
//!
//! - Fixtures build values; they assert nothing about the behaviour under test. An `expect` in a fixture
//!   says why the step cannot fail in a healthy fixture.
//! - This module exists in test builds only.
//!
//! # See also
//!
//! - The `_test.rs` file next to each source file: the tests that use these fixtures.

use std::sync::Arc;
use std::time::Duration;

use bitacora_segment::MemoryDisk;
use bitacora_segment::format::{Batch, SEGMENT_HEADER_BYTES, decode_frames};
use bitacora_types::replica_api::{
    AppendRequest, FenceRequest, ReadLimit, ReadRequest, RepairRequest, ReplicaApi, ReplicaConnector, SealRequest,
    SegmentDescriptor, StaticReplicaConnector,
};
use bitacora_types::{Epoch, ReplicaId, SegmentAddress, SegmentId, SequenceNumber, StreamId};
use bytes::{Bytes, BytesMut};

use crate::copy::{Admission, SegmentCopy};
use crate::{Replica, ReplicaSettings};

/// The segment the tests append to, read and repair.
pub(crate) const SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));

/// A second segment, of another stream, for tests that need the replica to hold two.
pub(crate) const OTHER_SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(8), SegmentId::new(1));

/// The epoch of the writer that authored [`SEGMENT`].
pub(crate) const SEGMENT_EPOCH: Epoch = Epoch::new(5);

/// The epoch of a writer that takes the stream over from the one of [`SEGMENT_EPOCH`].
pub(crate) const NEXT_EPOCH: Epoch = Epoch::new(6);

/// The sequence number of the first record of [`SEGMENT`].
pub(crate) const FIRST_SEQUENCE: u64 = 100;

/// The path of the records file of [`SEGMENT`] in a data directory.
pub(crate) const SEGMENT_FILE: &str = "7/3.seg";

/// The path of the state file of [`SEGMENT`] in a data directory.
pub(crate) const SEGMENT_STATE_FILE: &str = "7/3.state";

/// The path where a repair builds a new copy of [`SEGMENT`].
pub(crate) const REPAIR_SCRATCH_FILE: &str = "7/3.seg.repair";

/// An offset in a batch frame that lies past the frame's header: a byte of a record. Flipping a bit there
/// makes the batch fail its checksum.
pub(crate) const BYTE_IN_A_FRAME: usize = 30;

/// The offset in the records file of [`BYTE_IN_A_FRAME`] of the first batch.
pub(crate) const BYTE_IN_THE_FIRST_BATCH: usize = SEGMENT_HEADER_BYTES + BYTE_IN_A_FRAME;

/// The address under which the source of a repair is reached.
pub(crate) const SOURCE_ADDRESS: &str = "source:7000";

/// How much disk the records file of a copy under test reserves: small, so that an in-memory disk holds
/// many.
pub(crate) const SMALL_PREALLOCATION_BYTES: u64 = 4 * 1024;

/// The admission of a disk with plenty of room.
pub(crate) const ROOMY: Admission = Admission {
    may_create_segments: true,
    used_percent: 10,
};

/// The admission of a disk past the first threshold: no new segment files, appends still accepted.
pub(crate) const NEARLY_FULL: Admission = Admission {
    may_create_segments: false,
    used_percent: 90,
};

/// What a replica needs to know to create its copy of `segment`: written at [`SEGMENT_EPOCH`], starting
/// at [`FIRST_SEQUENCE`].
pub(crate) fn descriptor_of(segment: SegmentAddress) -> SegmentDescriptor {
    SegmentDescriptor {
        address: segment,
        epoch: SEGMENT_EPOCH,
        first_sequence: SequenceNumber::new(FIRST_SEQUENCE),
    }
}

/// What a replica needs to know to create its copy of [`SEGMENT`].
pub(crate) fn descriptor() -> SegmentDescriptor {
    descriptor_of(SEGMENT)
}

/// One batch of `segment` with `record_count` records starting at `first_sequence`. Each payload names
/// its own sequence number, as in `record-104`.
pub(crate) fn batch_of(segment: SegmentAddress, first_sequence: u64, record_count: u64) -> Batch {
    let mut payloads = Vec::new();
    for sequence in first_sequence..first_sequence + record_count {
        payloads.push(Bytes::from(format!("record-{sequence}")));
    }
    Batch::encode(segment, SequenceNumber::new(first_sequence), &payloads).expect("small payloads make a valid batch")
}

/// One batch of [`SEGMENT`] with `record_count` records starting at `first_sequence`.
pub(crate) fn batch(first_sequence: u64, record_count: u64) -> Batch {
    batch_of(SEGMENT, first_sequence, record_count)
}

/// The frames of `batches`, back to back, as they travel in a request.
pub(crate) fn frames_of(batches: &[Batch]) -> Bytes {
    let mut batch_frames = BytesMut::new();
    for batch in batches {
        batch_frames.extend_from_slice(batch.frame());
    }
    batch_frames.freeze()
}

/// An append to `segment` by the writer that authored it, carrying `batches` and declaring
/// `durable_sequence` durable.
pub(crate) fn append_request_to(segment: SegmentAddress, batches: &[Batch], durable_sequence: u64) -> AppendRequest {
    AppendRequest {
        segment: descriptor_of(segment),
        writer_epoch: SEGMENT_EPOCH,
        batch_frames: frames_of(batches),
        durable_sequence: SequenceNumber::new(durable_sequence),
    }
}

/// An append to [`SEGMENT`] by the writer that authored it, carrying `batches` and declaring
/// `durable_sequence` durable.
pub(crate) fn append_request(batches: &[Batch], durable_sequence: u64) -> AppendRequest {
    append_request_to(SEGMENT, batches, durable_sequence)
}

/// A heartbeat for [`SEGMENT`]: an append without batches that declares `durable_sequence` durable.
pub(crate) fn heartbeat(durable_sequence: u64) -> AppendRequest {
    append_request(&[], durable_sequence)
}

/// A read of [`SEGMENT`] from `from_sequence` on, as a reader makes it: up to the durable sequence,
/// without a size limit and without waiting.
pub(crate) fn read_request(from_sequence: u64) -> ReadRequest {
    ReadRequest {
        segment: SEGMENT,
        from_sequence: SequenceNumber::new(from_sequence),
        max_bytes: usize::MAX,
        wait_for_records: None,
        limit: ReadLimit::DurableSequence,
    }
}

/// A read of [`SEGMENT`] from `from_sequence` on that may wait up to `longest_wait` for records.
pub(crate) fn waiting_read_request(from_sequence: u64, longest_wait: Duration) -> ReadRequest {
    ReadRequest {
        wait_for_records: Some(longest_wait),
        ..read_request(from_sequence)
    }
}

/// A read of the whole of [`SEGMENT`], tail included, as a writer recovering at `recovery_epoch` makes it.
pub(crate) fn recovery_read_request(recovery_epoch: Epoch) -> ReadRequest {
    ReadRequest {
        limit: ReadLimit::LocalEnd { recovery_epoch },
        ..read_request(FIRST_SEQUENCE)
    }
}

/// A request to fence `segment` at `epoch`.
pub(crate) fn fence_request(segment: SegmentAddress, epoch: Epoch) -> FenceRequest {
    FenceRequest { segment, epoch }
}

/// A request to seal [`SEGMENT`] at `sealed_end`.
pub(crate) fn seal_request(sealed_end: u64) -> SealRequest {
    SealRequest {
        segment: SEGMENT,
        sealed_end: SequenceNumber::new(sealed_end),
    }
}

/// The sequence numbers of the records in `batch_frames`, which are frames of [`SEGMENT`], in order.
pub(crate) fn sequences_in(batch_frames: &Bytes) -> Vec<u64> {
    let batches = decode_frames(SEGMENT, batch_frames).expect("a replica answers with valid frames");
    let mut sequences = Vec::new();
    for batch in &batches {
        for record in batch.records() {
            sequences.push(record.sequence.get());
        }
    }
    sequences
}

/// A copy of [`SEGMENT`] on `disk` that the replica has nothing stored for yet.
pub(crate) fn new_copy(disk: &MemoryDisk) -> SegmentCopy {
    SegmentCopy::absent(SEGMENT, Arc::new(disk.clone()), SMALL_PREALLOCATION_BYTES)
}

/// A copy of [`SEGMENT`] on `disk` holding records 100 to 109 in two batches of five, of which the writer
/// has declared 100 to 104 durable.
pub(crate) async fn copy_with_records(disk: &MemoryDisk) -> SegmentCopy {
    let mut copy = new_copy(disk);
    copy.append(&append_request(&[batch(100, 5), batch(105, 5)], 104), ROOMY)
        .await
        .expect("an empty copy on a roomy disk accepts its first batches");
    copy
}

/// Loads the copy of [`SEGMENT`] from `disk`, as a restarted replica does.
pub(crate) async fn reload(disk: &MemoryDisk) -> SegmentCopy {
    SegmentCopy::load(SEGMENT, Arc::new(disk.clone()), SMALL_PREALLOCATION_BYTES).await
}

/// The sequence numbers `copy` serves to a read of the whole segment made with `request`.
pub(crate) async fn readable_sequences(copy: &mut SegmentCopy, request: &ReadRequest) -> Vec<u64> {
    let response = copy.read(request).await.expect("the copy serves the read");
    sequences_in(&response.batch_frames)
}

/// Replica settings with segment files small enough for an in-memory disk.
pub(crate) fn small_segments() -> ReplicaSettings {
    ReplicaSettings {
        preallocated_segment_bytes: 1024,
        ..ReplicaSettings::default()
    }
}

/// A connector that reaches no other replica.
pub(crate) fn no_peers() -> Arc<dyn ReplicaConnector> {
    Arc::new(StaticReplicaConnector::new())
}

/// Opens a replica on `disk` that proposes the identity `name`, with `settings` and `connector`.
pub(crate) async fn open_replica_with(
    disk: &MemoryDisk,
    name: &str,
    settings: ReplicaSettings,
    connector: Arc<dyn ReplicaConnector>,
) -> Replica {
    let proposed_replica_id = ReplicaId::new(name).expect("test names are valid identifiers");
    Replica::open(Arc::new(disk.clone()), proposed_replica_id, settings, connector)
        .await
        .expect("a replica opens on a working in-memory disk")
}

/// Opens a replica on `disk` that proposes the identity `name`, with small segment files and no peers.
pub(crate) async fn open_replica(disk: &MemoryDisk, name: &str) -> Replica {
    open_replica_with(disk, name, small_segments(), no_peers()).await
}

/// What a replica reports about the one segment copy it holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CopySummary {
    /// The last sequence number in the copy, or `None` if it has no records file.
    pub local_end: Option<u64>,
    /// The sealed end the copy knows, if any.
    pub sealed_end: Option<u64>,
    /// Whether the copy is damaged.
    pub is_damaged: bool,
}

/// What `replica` reports about the first segment copy it lists, which in these tests is its only one.
pub(crate) async fn copy_of(replica: &Replica) -> CopySummary {
    let statuses = replica
        .list_segments()
        .await
        .expect("a running replica lists its segments");
    let status = statuses.first().expect("the replica holds the segment");
    CopySummary {
        local_end: status.local_end.map(SequenceNumber::get),
        sealed_end: status.sealed_end.map(SequenceNumber::get),
        is_damaged: status.is_damaged,
    }
}

/// A connector that reaches, at [`SOURCE_ADDRESS`], a replica named `source` that holds [`SEGMENT`] sealed
/// at `sealed_end`. The source was sent records 100 to 109 in two batches of five before it was sealed,
/// so it holds them up to `sealed_end` and never more than 109.
pub(crate) async fn source_sealed_at(sealed_end: u64) -> Arc<dyn ReplicaConnector> {
    let source = Arc::new(open_replica(&MemoryDisk::new(), "source").await);
    for first_sequence in [100, 105] {
        source
            .append(append_request(&[batch(first_sequence, 5)], 0))
            .await
            .expect("the source accepts consecutive batches");
    }
    source
        .seal(seal_request(sealed_end))
        .await
        .expect("the source seals at a batch boundary");
    let mut connector = StaticReplicaConnector::new();
    let source_replica_id = source.replica_id().clone();
    connector.register(source_replica_id, SOURCE_ADDRESS, source as Arc<dyn ReplicaApi>);
    Arc::new(connector)
}

/// Opens the replica that repairs are sent to, on `disk`: it is named `target`, reaches its source
/// through `connector`, reads from it in chunks so small that a repair takes several, and copies without
/// a rate limit.
pub(crate) async fn open_target(disk: &MemoryDisk, connector: Arc<dyn ReplicaConnector>) -> Replica {
    let settings = ReplicaSettings {
        repair_chunk_bytes: 64,
        repair_bytes_per_second: 0,
        ..small_segments()
    };
    open_replica_with(disk, "target", settings, connector).await
}

/// A request to make the copy of [`SEGMENT`] complete up to `sealed_end` from the replica behind
/// [`source_sealed_at`].
pub(crate) fn repair_request(sealed_end: u64) -> RepairRequest {
    RepairRequest {
        segment: descriptor(),
        sealed_end: SequenceNumber::new(sealed_end),
        source_replica_id: ReplicaId::new("source").expect("test names are valid identifiers"),
        source_address: SOURCE_ADDRESS.to_owned(),
    }
}
