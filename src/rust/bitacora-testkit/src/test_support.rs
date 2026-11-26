//! Fixtures shared by the tests of this crate.
//!
//! # Why this file exists
//!
//! The tests of the link, the connector, the node and the cluster all start from the same place: a cluster of one node
//! whose replica holds three records of one segment, and requests that append more. Written once per test file they
//! would drift apart. Here they are written once.
//!
//! # What it contains
//!
//! - [`SEGMENT`]: the segment every test appends to.
//! - [`append_request`]: an append of some records to it.
//! - [`cluster_with_three_records`]: a one-node cluster whose replica holds records 1 to 3.
//! - [`registered_replicas`]: the registry as the metadata store lists it.
//! - [`is_unavailable`], [`operations_failed_as_unavailable`] and [`EVERY_OPERATION`]: to check that a fault applies to
//!   every operation of the replica interface, not only to the one a test happens to send.
//!
//! # How it works
//!
//! The segment is segment 1 of stream 1, written in epoch 1 and starting at the first sequence number. An append
//! declares everything before its own first record durable, which is what a writer does once the previous batch was
//! acknowledged, so after appending records 4 and 5 a reader is served records 1 to 3.
//!
//! # Invariants
//!
//! - Fixtures build values; they assert nothing about the behaviour under test. An `expect` in a fixture says why the
//!   step cannot fail in a healthy fixture.
//! - This module exists in test builds only.
//!
//! # See also
//!
//! - The `_test.rs` file next to each source file: the tests that use these fixtures.

use bitacora_meta::ReplicaRecord;
use bitacora_segment::format::Batch;
use bitacora_types::replica_api::{
    AppendRequest, FenceRequest, ReadLimit, ReadRequest, RepairRequest, ReplicaApi, ReplicaError, SealRequest,
    SegmentDescriptor, UploadRequest,
};
use bitacora_types::{Epoch, ReplicaId, SegmentAddress, SegmentId, SequenceNumber, StreamId};
use bytes::Bytes;

use crate::Cluster;

/// The segment the tests append to and read.
pub(crate) const SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(1), SegmentId::new(1));

/// The epoch of the writer that authored [`SEGMENT`].
const WRITER_EPOCH: Epoch = Epoch::new(1);

/// How many records the replica of [`cluster_with_three_records`] holds.
pub(crate) const STORED_RECORD_COUNT: u64 = 3;

/// Builds an append of `record_count` records to [`SEGMENT`], starting at `first_sequence`, with everything before it
/// declared durable.
///
/// # Arguments
///
/// * `first_sequence` - Sequence number of the first record. Must be at least 1.
/// * `record_count` - How many records the batch holds.
///
/// # Returns
///
/// The request, ready to send to a replica that holds the segment up to `first_sequence - 1`.
///
/// # Panics
///
/// Panics if the batch cannot be encoded, which a handful of short records cannot cause.
pub(crate) fn append_request(first_sequence: u64, record_count: u64) -> AppendRequest {
    let mut payloads = Vec::new();
    for record_number in 0..record_count {
        payloads.push(Bytes::from(format!("record-{record_number}")));
    }
    let batch = Batch::encode(SEGMENT, SequenceNumber::new(first_sequence), &payloads)
        .expect("a few short records fit in a batch");
    AppendRequest {
        segment: SegmentDescriptor {
            address: SEGMENT,
            epoch: WRITER_EPOCH,
            first_sequence: SequenceNumber::FIRST,
        },
        writer_epoch: WRITER_EPOCH,
        batch_frames: batch.frame().clone(),
        durable_sequence: SequenceNumber::new(first_sequence - 1),
    }
}

/// Starts a cluster of one node and appends records 1 to 3 of [`SEGMENT`] to it.
///
/// # Returns
///
/// The cluster. Its only node holds the three records, flushed; none of them is declared durable yet.
///
/// # Panics
///
/// Panics if the append fails, which a healthy node does not cause.
pub(crate) async fn cluster_with_three_records() -> Cluster {
    let cluster = Cluster::start(1).await;
    cluster.nodes()[0]
        .link()
        .append(append_request(1, STORED_RECORD_COUNT))
        .await
        .expect("a healthy node accepts the first batch of a segment");
    cluster
}

/// Lists the registry of `cluster` as the metadata store holds it, in the store's order.
///
/// # Arguments
///
/// * `cluster` - The cluster whose metadata store is read.
///
/// # Returns
///
/// One record per identity that ever registered and was not removed.
///
/// # Panics
///
/// Panics if the metadata store is unavailable; tests that make it so do not call this.
pub(crate) async fn registered_replicas(cluster: &Cluster) -> Vec<ReplicaRecord> {
    cluster
        .metadata()
        .list_replicas()
        .await
        .expect("the metadata store is available")
}

/// Says whether `answer` is the error a caller gets from a network that did not complete its request.
///
/// # Arguments
///
/// * `answer` - What a call through a link returned.
///
/// # Returns
///
/// `true` for [`ReplicaError::Unavailable`], `false` for a success and for every other error.
pub(crate) fn is_unavailable<Response>(answer: &Result<Response, ReplicaError>) -> bool {
    matches!(answer, Err(ReplicaError::Unavailable { .. }))
}

/// Sends one request of every kind through `link` and reports which of them failed as a network would fail them.
///
/// The requests are about [`SEGMENT`]; what the replica would answer does not matter, only whether the caller got
/// [`ReplicaError::Unavailable`].
///
/// # Arguments
///
/// * `link` - The link, or the connection made through the connector, to send the requests through.
///
/// # Returns
///
/// The names of the operations that failed as unavailable, in the order of [`EVERY_OPERATION`].
///
/// # Panics
///
/// Panics if the name of the repair source is not a valid identifier, which it is.
pub(crate) async fn operations_failed_as_unavailable(link: &dyn ReplicaApi) -> Vec<&'static str> {
    let peer = ReplicaId::new("replica-9").expect("a valid identifier");
    let descriptor = append_request(1, 1).segment;
    let sealed_end = SequenceNumber::new(3);
    let read = ReadRequest {
        segment: SEGMENT,
        from_sequence: SequenceNumber::FIRST,
        max_bytes: usize::MAX,
        wait_for_records: None,
        limit: ReadLimit::DurableSequence,
    };
    let fence = FenceRequest {
        segment: SEGMENT,
        epoch: Epoch::new(2),
    };
    let repair = RepairRequest {
        segment: descriptor,
        sealed_end,
        source_replica_id: peer,
        source_address: "replica-9:7000".to_owned(),
    };
    let upload = UploadRequest {
        segment: descriptor,
        sealed_end,
        object_key: "streams/1/segments/1".to_owned(),
    };
    let seal = SealRequest {
        segment: SEGMENT,
        sealed_end,
    };

    let outcomes = [
        ("append", is_unavailable(&link.append(append_request(4, 1)).await)),
        ("read", is_unavailable(&link.read(read).await)),
        ("fence", is_unavailable(&link.fence(fence).await)),
        ("seal", is_unavailable(&link.seal(seal).await)),
        ("repair_segment", is_unavailable(&link.repair_segment(repair).await)),
        ("upload_segment", is_unavailable(&link.upload_segment(upload).await)),
        ("delete_segment", is_unavailable(&link.delete_segment(SEGMENT).await)),
        ("list_segments", is_unavailable(&link.list_segments().await)),
        ("status", is_unavailable(&link.status().await)),
        ("drain", is_unavailable(&link.drain().await)),
    ];
    let mut failed_operations = Vec::new();
    for (operation, was_failed_as_unavailable) in outcomes {
        if was_failed_as_unavailable {
            failed_operations.push(operation);
        }
    }
    failed_operations
}

/// Every operation of the replica interface, in the order `operations_failed_as_unavailable` sends them.
pub(crate) const EVERY_OPERATION: [&str; 10] = [
    "append",
    "read",
    "fence",
    "seal",
    "repair_segment",
    "upload_segment",
    "delete_segment",
    "list_segments",
    "status",
    "drain",
];
