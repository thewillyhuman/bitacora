//! What the tests of the replica daemon share.
//!
//! # Why this file exists
//!
//! The tests of the registration, of the scrub schedule and of the daemon as a whole all need a real
//! replica on an in-memory disk with a segment in it. Building one takes a dozen lines that say nothing
//! about what each test checks, so they live here once.
//!
//! # What it contains
//!
//! - [`SEGMENT`], [`SEGMENT_FILE_PATH`]: the segment the tests write, and where its file lies on disk.
//! - [`open_replica`]: a replica without peers on a given disk.
//! - [`descriptor`], [`append_of`], [`seal_at`]: requests against [`SEGMENT`].
//! - [`write_sealed_segment`]: records 1 to 5 appended and sealed.
//!
//! # How it works
//!
//! Everything goes through the replica's public interface, exactly as a writer would use it.
//!
//! # Invariants
//!
//! - Nothing here is compiled into the daemon: the module only exists for tests.
//!
//! # See also
//!
//! - `registration_test.rs`, `scrub_test.rs`, `daemon_test.rs`: the tests that use it.

use std::sync::Arc;

use bitacora_replica::{Replica, ReplicaSettings};
use bitacora_segment::MemoryDisk;
use bitacora_segment::format::Batch;
use bitacora_types::replica_api::{AppendRequest, ReplicaApi, SealRequest, SegmentDescriptor, StaticReplicaConnector};
use bitacora_types::{Epoch, ReplicaId, SegmentAddress, SegmentId, SequenceNumber, StreamId};
use bytes::Bytes;

/// The segment the tests write.
pub(crate) const SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));

/// Where a replica keeps the file of [`SEGMENT`] on its disk.
pub(crate) const SEGMENT_FILE_PATH: &str = "7/3.seg";

/// The epoch of the writer of [`SEGMENT`].
const WRITER_EPOCH: Epoch = Epoch::new(1);

/// The last record [`write_sealed_segment`] writes.
pub(crate) const SEALED_END: u64 = 5;

/// Opens a replica without peers on `disk`, proposing `name` as its identity.
///
/// # Panics
///
/// Panics if the replica does not open, which an in-memory disk never causes.
pub(crate) async fn open_replica(disk: &MemoryDisk, name: &str) -> Arc<Replica> {
    let replica = Replica::open(
        Arc::new(disk.clone()),
        ReplicaId::new(name).expect("a valid identifier"),
        small_segments(),
        Arc::new(StaticReplicaConnector::new()),
    )
    .await
    .expect("a replica opens on a working disk");
    Arc::new(replica)
}

/// Replica settings with segment files small enough for an in-memory disk.
pub(crate) fn small_segments() -> ReplicaSettings {
    ReplicaSettings {
        preallocated_segment_bytes: 4096,
        ..ReplicaSettings::default()
    }
}

/// What a replica needs to know to create its copy of [`SEGMENT`].
pub(crate) fn descriptor() -> SegmentDescriptor {
    SegmentDescriptor {
        address: SEGMENT,
        epoch: WRITER_EPOCH,
        first_sequence: SequenceNumber::FIRST,
    }
}

/// An append to [`SEGMENT`] of `record_count` records starting at `first_sequence`, with everything
/// before it declared durable.
pub(crate) fn append_of(first_sequence: u64, record_count: u64) -> AppendRequest {
    let mut payloads = Vec::new();
    for sequence in first_sequence..first_sequence + record_count {
        payloads.push(Bytes::from(format!("record-{sequence}")));
    }
    let batch = Batch::encode(SEGMENT, SequenceNumber::new(first_sequence), &payloads).expect("a valid batch");
    AppendRequest {
        segment: descriptor(),
        writer_epoch: WRITER_EPOCH,
        batch_frames: batch.frame().clone(),
        durable_sequence: SequenceNumber::new(first_sequence - 1),
    }
}

/// A seal of [`SEGMENT`] at `sealed_end`.
pub(crate) fn seal_at(sealed_end: u64) -> SealRequest {
    SealRequest {
        segment: SEGMENT,
        sealed_end: SequenceNumber::new(sealed_end),
    }
}

/// Appends records 1 to [`SEALED_END`] to [`SEGMENT`] on `replica` and seals it there.
///
/// # Panics
///
/// Panics if the replica refuses the append or the seal.
pub(crate) async fn write_sealed_segment(replica: &dyn ReplicaApi) {
    replica
        .append(append_of(1, SEALED_END))
        .await
        .expect("the append is acknowledged");
    replica.seal(seal_at(SEALED_END)).await.expect("the seal is recorded");
}
