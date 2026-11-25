//! A real replica, served over gRPC on the loopback interface and driven
//! through `GrpcReplicaConnector`.
//!
//! # What is covered
//!
//! - Every operation of `ReplicaApi` gives over gRPC the answer the replica
//!   gives in process.
//! - Every error a replica can return reaches the caller unchanged, first
//!   from a real replica brought into each state, then from a stand-in
//!   that returns every variant through every call.
//! - A request for another replica never reaches this one and fails as
//!   unavailable; so does anything else that prevents an answer.
//! - The largest record fits in a message, in both directions.
//!
//! # How the fixtures work
//!
//! [`ServedReplica`] opens a `bitacora_replica::Replica` on an in-memory
//! disk, serves it on a free loopback port and connects to it. A test then
//! holds both ends: `remote`, the replica as a caller on another machine
//! sees it, and `replica`, the replica itself, to prepare a state without
//! going through the transport and to compare answers.
//!
//! Time is real here, because sockets are. Nothing sleeps: a test that
//! waits for something waits for an answer, under a generous time limit
//! that only matters when the test is about to fail.

mod support;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bitacora_artifact::{ArtifactStore, MemoryArtifactStore};
use bitacora_proto::v1;
use bitacora_proto::v1::fence_response::Outcome as FenceOutcome;
use bitacora_proto::v1::replica_admin_client::ReplicaAdminClient;
use bitacora_proto::v1::replica_data_client::ReplicaDataClient;
use bitacora_proto::{
    GrpcConnectorSettings, GrpcReplicaConnector, REPLICA_ID_HEADER, serve_replica,
};
use bitacora_replica::{Replica, ReplicaSettings};
use bitacora_segment::format::{Batch, decode_frames};
use bitacora_segment::{Disk, MemoryDisk};
use bitacora_types::replica_api::{
    AppendRequest, AppendResponse, FenceRequest, FenceResponse, ReadLimit, ReadRequest,
    ReadResponse, RepairRequest, RepairResponse, ReplicaApi, ReplicaConnector, ReplicaError,
    ReplicaStatus, SealRequest, SealResponse, SegmentDescriptor, SegmentStatus, UploadRequest,
    UploadResponse,
};
use bitacora_types::{
    Epoch, MAX_RECORD_PAYLOAD_BYTES, ReplicaId, SegmentAddress, SegmentId, SequenceNumber, StreamId,
};
use bytes::Bytes;
use support::serve_on_loopback;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request};

/// The segment most tests write to.
const SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));

/// A second segment, for tests that need two.
const OTHER_SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(8), SegmentId::new(1));

/// The epoch of the writer in these tests.
const WRITER_EPOCH: Epoch = Epoch::new(5);

/// Longer than any answer takes on a working transport. A test that waits
/// this long is failing, and the limit only keeps it from hanging.
const GENEROUS_WAIT: Duration = Duration::from_secs(10);

/// A replica served over gRPC, held from both sides.
struct ServedReplica {
    /// The replica itself, behind the server.
    replica: Arc<Replica>,
    /// The disk the replica stores on, to inject faults.
    disk: MemoryDisk,
    /// The object store the replica uploads sealed segments to.
    objects: MemoryArtifactStore,
    /// Where the server listens, as `127.0.0.1:port`.
    address: String,
    /// The same replica, reached over gRPC.
    remote: Arc<dyn ReplicaApi>,
}

impl ServedReplica {
    /// Serves a replica called `name` on an empty disk.
    async fn start(name: &str) -> Self {
        Self::start_on(MemoryDisk::new(), name, small_segments()).await
    }

    /// Serves a replica called `name` that stores on `disk` and is set up
    /// with `settings`. Like a real replica, it reaches its peers over
    /// gRPC.
    async fn start_on(disk: MemoryDisk, name: &str, settings: ReplicaSettings) -> Self {
        let connector = GrpcReplicaConnector::new(GrpcConnectorSettings::default());
        let replica = Replica::open(
            Arc::new(disk.clone()),
            replica_id(name),
            settings,
            Arc::new(connector.clone()),
            Arc::new(|_segment, _reason| {}),
        )
        .await
        .expect("the replica opens on a working disk");
        let objects = MemoryArtifactStore::new();
        let replica = Arc::new(replica.with_artifact_store(Arc::new(objects.clone())));
        let address = serve_on_loopback(
            Arc::clone(&replica) as Arc<dyn ReplicaApi>,
            replica_id(name),
        )
        .await;
        let remote = connector.connect(&replica_id(name), &address);
        Self {
            replica,
            disk,
            objects,
            address,
            remote,
        }
    }
}

/// Settings that keep segment files small, since most tests write a few
/// bytes.
fn small_segments() -> ReplicaSettings {
    ReplicaSettings {
        preallocated_segment_bytes: 1024,
        ..ReplicaSettings::default()
    }
}

/// The replica identifier `name`.
fn replica_id(name: &str) -> ReplicaId {
    ReplicaId::new(name).expect("a valid identifier")
}

/// What a replica needs to create its copy of `segment`.
fn descriptor_of(segment: SegmentAddress) -> SegmentDescriptor {
    SegmentDescriptor {
        address: segment,
        epoch: WRITER_EPOCH,
        first_sequence: SequenceNumber::FIRST,
    }
}

/// An append to `segment` of one batch holding `payloads`, the first of
/// which gets `first_sequence`, declaring `durable_sequence` durable.
fn append_of(
    segment: SegmentAddress,
    first_sequence: u64,
    payloads: &[Bytes],
    durable_sequence: u64,
) -> AppendRequest {
    let batch = Batch::encode(segment, SequenceNumber::new(first_sequence), payloads)
        .expect("the payloads fit in a batch");
    AppendRequest {
        segment: descriptor_of(segment),
        writer_epoch: WRITER_EPOCH,
        batch_frames: batch.frame().clone(),
        durable_sequence: SequenceNumber::new(durable_sequence),
    }
}

/// An append to [`SEGMENT`] of `record_count` records named after their
/// sequence numbers, starting at `first_sequence`.
fn append_records(first_sequence: u64, record_count: u64, durable_sequence: u64) -> AppendRequest {
    let mut payloads = Vec::new();
    for sequence in first_sequence..first_sequence + record_count {
        payloads.push(Bytes::from(format!("record-{sequence}")));
    }
    append_of(SEGMENT, first_sequence, &payloads, durable_sequence)
}

/// A reader's read of [`SEGMENT`] from `from_sequence` that does not wait.
fn read_from(from_sequence: u64) -> ReadRequest {
    ReadRequest {
        segment: SEGMENT,
        from_sequence: SequenceNumber::new(from_sequence),
        max_bytes: 1024 * 1024,
        wait_for_records: None,
        limit: ReadLimit::DurableSequence,
    }
}

/// The payloads of the records in a read's answer, in order.
fn payloads_in(response: &ReadResponse) -> Vec<Bytes> {
    let batches = decode_frames(SEGMENT, &response.batch_frames).expect("the frames are intact");
    let mut payloads = Vec::new();
    for batch in &batches {
        for record in batch.records() {
            payloads.push(record.payload);
        }
    }
    payloads
}

/// A repair of [`SEGMENT`], sealed at `sealed_end`, from `source`.
fn repair_from(source: &ServedReplica, sealed_end: u64) -> RepairRequest {
    RepairRequest {
        segment: descriptor_of(SEGMENT),
        sealed_end: SequenceNumber::new(sealed_end),
        source_replica_id: source.replica.replica_id().clone(),
        source_address: source.address.clone(),
    }
}

// ---- every operation ----

#[tokio::test]
async fn append_is_acknowledged_with_the_end_of_the_copy() {
    let served = ServedReplica::start("replica-a").await;

    let acknowledgement = served.remote.append(append_records(1, 3, 0)).await;

    assert_eq!(
        acknowledgement,
        Ok(AppendResponse {
            local_end: SequenceNumber::new(3)
        })
    );
}

#[tokio::test]
async fn appended_records_are_read_back_intact() {
    let served = ServedReplica::start("replica-a").await;
    served
        .remote
        .append(append_records(1, 3, 3))
        .await
        .expect("the append is acknowledged");

    let response = served
        .remote
        .read(read_from(1))
        .await
        .expect("the read is served");

    assert_eq!(payloads_in(&response), ["record-1", "record-2", "record-3"]);
}

#[tokio::test]
async fn read_gives_the_answer_the_replica_gives_in_process() {
    let served = ServedReplica::start("replica-a").await;
    served
        .replica
        .append(append_records(1, 3, 2))
        .await
        .expect("the append is acknowledged");

    let answer_over_grpc = served.remote.read(read_from(1)).await;

    assert_eq!(answer_over_grpc, served.replica.read(read_from(1)).await);
}

#[tokio::test]
async fn read_waiting_for_records_is_answered_when_they_become_durable() {
    let served = ServedReplica::start("replica-a").await;
    served
        .remote
        .append(append_records(1, 1, 1))
        .await
        .expect("the append is acknowledged");
    let waiting_read = ReadRequest {
        wait_for_records: Some(GENEROUS_WAIT),
        ..read_from(2)
    };
    let reader = Arc::clone(&served.remote);
    let read_in_flight = tokio::spawn(async move { reader.read(waiting_read).await });

    served
        .remote
        .append(append_records(2, 1, 2))
        .await
        .expect("the append is acknowledged");
    let response = timeout(GENEROUS_WAIT, read_in_flight)
        .await
        .expect("the read is answered before its wait runs out")
        .expect("the reading task does not panic")
        .expect("the read is served");

    assert_eq!(payloads_in(&response), ["record-2"]);
}

#[tokio::test]
async fn fence_reports_where_the_copy_ends() {
    let served = ServedReplica::start("replica-a").await;
    served
        .replica
        .append(append_records(1, 3, 0))
        .await
        .expect("the append is acknowledged");

    let fenced = served
        .remote
        .fence(FenceRequest {
            segment: SEGMENT,
            epoch: WRITER_EPOCH.next(),
        })
        .await;

    assert_eq!(
        fenced,
        Ok(FenceResponse {
            local_end: Some(SequenceNumber::new(3)),
            sealed_end: None,
        })
    );
}

/// A recovering writer tells "no copy" from "an empty copy" by the absence
/// of an end, so the absence has to survive the transport.
#[tokio::test]
async fn fence_of_a_segment_the_replica_does_not_hold_reports_no_end() {
    let served = ServedReplica::start("replica-a").await;

    let fenced = served
        .remote
        .fence(FenceRequest {
            segment: SEGMENT,
            epoch: WRITER_EPOCH,
        })
        .await;

    assert_eq!(
        fenced,
        Ok(FenceResponse {
            local_end: None,
            sealed_end: None,
        })
    );
}

#[tokio::test]
async fn seal_reports_the_end_of_the_sealed_copy() {
    let served = ServedReplica::start("replica-a").await;
    served
        .replica
        .append(append_records(1, 3, 0))
        .await
        .expect("the append is acknowledged");

    let sealed = served
        .remote
        .seal(SealRequest {
            segment: SEGMENT,
            sealed_end: SequenceNumber::new(3),
        })
        .await;

    assert_eq!(
        sealed,
        Ok(SealResponse {
            local_end: Some(SequenceNumber::new(3))
        })
    );
}

/// The whole path of a repair is gRPC: the order reaches the target over
/// it, and the target pulls the records from the source over it.
#[tokio::test]
async fn repair_copies_a_sealed_segment_from_another_replica() {
    let source = ServedReplica::start("source").await;
    source
        .replica
        .append(append_records(1, 5, 0))
        .await
        .expect("the append is acknowledged");
    source
        .replica
        .seal(SealRequest {
            segment: SEGMENT,
            sealed_end: SequenceNumber::new(5),
        })
        .await
        .expect("the seal is recorded");
    let target = ServedReplica::start("target").await;

    let repaired = target.remote.repair_segment(repair_from(&source, 5)).await;

    assert_eq!(
        repaired,
        Ok(RepairResponse {
            local_end: SequenceNumber::new(5)
        })
    );
}

#[tokio::test]
async fn upload_stores_a_sealed_segment_in_the_object_store() {
    let served = ServedReplica::start("replica-a").await;
    served
        .replica
        .append(append_records(1, 5, 0))
        .await
        .expect("the append is acknowledged");
    served
        .replica
        .seal(SealRequest {
            segment: SEGMENT,
            sealed_end: SequenceNumber::new(5),
        })
        .await
        .expect("the seal is recorded");
    let object_key = "streams/7/segments/00000000000000000003";

    let uploaded = served
        .remote
        .upload_segment(UploadRequest {
            segment: descriptor_of(SEGMENT),
            sealed_end: SequenceNumber::new(5),
            object_key: object_key.to_owned(),
        })
        .await;

    let stored_object = served
        .objects
        .get_object(object_key)
        .await
        .expect("the object is stored");
    assert_eq!(
        uploaded,
        Ok(UploadResponse {
            size_bytes: stored_object.len() as u64
        })
    );
}

#[tokio::test]
async fn status_reports_the_identity_of_the_replica() {
    let served = ServedReplica::start("replica-a").await;

    let status = served.remote.status().await.expect("the replica reports");

    assert_eq!(status.replica_id, replica_id("replica-a"));
}

#[tokio::test]
async fn status_gives_the_answer_the_replica_gives_in_process() {
    let served = ServedReplica::start("replica-a").await;

    let answer_over_grpc = served.remote.status().await;

    assert_eq!(answer_over_grpc, served.replica.status().await);
}

#[tokio::test]
async fn listed_segments_are_the_copies_the_replica_holds() {
    let served = ServedReplica::start("replica-a").await;
    served
        .replica
        .append(append_records(1, 3, 0))
        .await
        .expect("the append is acknowledged");
    served
        .replica
        .append(append_of(OTHER_SEGMENT, 1, &[Bytes::from("a record")], 0))
        .await
        .expect("the append is acknowledged");

    let statuses = served.remote.list_segments().await.expect("the list");

    let mut listed_addresses = Vec::new();
    for status in &statuses {
        listed_addresses.push(status.address);
    }
    assert_eq!(listed_addresses, [SEGMENT, OTHER_SEGMENT]);
}

#[tokio::test]
async fn listed_segments_give_the_answer_the_replica_gives_in_process() {
    let served = ServedReplica::start("replica-a").await;
    served
        .replica
        .append(append_records(1, 3, 2))
        .await
        .expect("the append is acknowledged");

    let answer_over_grpc = served.remote.list_segments().await;

    assert_eq!(answer_over_grpc, served.replica.list_segments().await);
}

#[tokio::test]
async fn drain_makes_the_replica_drain() {
    let served = ServedReplica::start("replica-a").await;

    served.remote.drain().await.expect("the replica drains");

    let status = served.replica.status().await.expect("the replica reports");
    assert!(status.is_draining);
}

#[tokio::test]
async fn delete_removes_the_copy_from_the_replica() {
    let served = ServedReplica::start("replica-a").await;
    served
        .replica
        .append(append_records(1, 3, 0))
        .await
        .expect("the append is acknowledged");

    served
        .remote
        .delete_segment(SEGMENT)
        .await
        .expect("the copy is deleted");

    assert_eq!(served.replica.list_segments().await, Ok(Vec::new()));
}

// ---- sizes ----

/// The largest record the system accepts is as large as the largest
/// message gRPC accepts by default. It has to travel in a request, on
/// append, and in a response, on read.
#[tokio::test]
async fn largest_record_is_appended_and_read_back() {
    let served = ServedReplica::start("replica-a").await;
    let largest_payload = Bytes::from(vec![0xB1; MAX_RECORD_PAYLOAD_BYTES]);
    let payloads = [largest_payload.clone()];

    served
        .remote
        .append(append_of(SEGMENT, 1, &payloads, 1))
        .await
        .expect("the append is acknowledged");
    let response = served
        .remote
        .read(read_from(1))
        .await
        .expect("the read is served");

    assert_eq!(payloads_in(&response), [largest_payload]);
}

// ---- every error a replica returns ----

#[tokio::test]
async fn append_from_a_fenced_writer_is_refused_as_fenced() {
    let served = ServedReplica::start("replica-a").await;
    served
        .replica
        .fence(FenceRequest {
            segment: SEGMENT,
            epoch: WRITER_EPOCH.next(),
        })
        .await
        .expect("the segment is fenced");

    let refused = served.remote.append(append_records(1, 1, 0)).await;

    assert_eq!(
        refused,
        Err(ReplicaError::Fenced {
            request_epoch: WRITER_EPOCH,
            fenced_epoch: WRITER_EPOCH.next(),
        })
    );
}

#[tokio::test]
async fn append_to_a_sealed_segment_is_refused_as_sealed() {
    let served = ServedReplica::start("replica-a").await;
    served
        .replica
        .append(append_records(1, 3, 0))
        .await
        .expect("the append is acknowledged");
    served
        .replica
        .seal(SealRequest {
            segment: SEGMENT,
            sealed_end: SequenceNumber::new(3),
        })
        .await
        .expect("the seal is recorded");

    let refused = served.remote.append(append_records(4, 1, 0)).await;

    assert_eq!(
        refused,
        Err(ReplicaError::Sealed {
            sealed_end: SequenceNumber::new(3)
        })
    );
}

#[tokio::test]
async fn append_that_leaves_a_gap_is_refused_as_out_of_order() {
    let served = ServedReplica::start("replica-a").await;
    served
        .replica
        .append(append_records(1, 3, 0))
        .await
        .expect("the append is acknowledged");

    let refused = served.remote.append(append_records(7, 1, 0)).await;

    assert_eq!(
        refused,
        Err(ReplicaError::OutOfOrder {
            first_sequence: SequenceNumber::new(7),
            local_end: SequenceNumber::new(3),
        })
    );
}

#[tokio::test]
async fn second_seal_at_another_end_is_refused_as_conflicting() {
    let served = ServedReplica::start("replica-a").await;
    served
        .replica
        .append(append_records(1, 3, 0))
        .await
        .expect("the append is acknowledged");
    served
        .replica
        .seal(SealRequest {
            segment: SEGMENT,
            sealed_end: SequenceNumber::new(3),
        })
        .await
        .expect("the seal is recorded");

    let refused = served
        .remote
        .seal(SealRequest {
            segment: SEGMENT,
            sealed_end: SequenceNumber::new(2),
        })
        .await;

    assert_eq!(
        refused,
        Err(ReplicaError::ConflictingSeal {
            existing_sealed_end: SequenceNumber::new(3),
            requested_sealed_end: SequenceNumber::new(2),
        })
    );
}

#[tokio::test]
async fn read_of_a_segment_the_replica_does_not_hold_finds_nothing() {
    let served = ServedReplica::start("replica-a").await;

    let refused = served.remote.read(read_from(1)).await;

    assert_eq!(
        refused,
        Err(ReplicaError::SegmentNotFound { segment: SEGMENT })
    );
}

#[tokio::test]
async fn append_of_damaged_frames_is_refused_as_invalid() {
    let served = ServedReplica::start("replica-a").await;
    let damaged_append = AppendRequest {
        batch_frames: Bytes::from_static(b"these bytes are not a batch frame"),
        ..append_records(1, 1, 0)
    };
    let answer_in_process = served.replica.append(damaged_append.clone()).await;

    let answer_over_grpc = served.remote.append(damaged_append).await;

    assert!(
        matches!(answer_in_process, Err(ReplicaError::InvalidRequest { .. })),
        "the replica answered {answer_in_process:?}"
    );
    assert_eq!(answer_over_grpc, answer_in_process);
}

#[tokio::test]
async fn append_to_a_full_disk_is_refused_for_lack_of_capacity() {
    let disk = MemoryDisk::with_capacity(100_000);
    let always_measure_the_disk = ReplicaSettings {
        space_refresh_interval: Duration::ZERO,
        ..small_segments()
    };
    let served = ServedReplica::start_on(disk, "replica-a", always_measure_the_disk).await;
    served
        .disk
        .create_file("ballast", Bytes::new(), 96_000)
        .await
        .expect("the ballast fits on the disk");
    let answer_in_process = served.replica.append(append_records(1, 1, 0)).await;

    let answer_over_grpc = served.remote.append(append_records(1, 1, 0)).await;

    assert!(
        matches!(answer_in_process, Err(ReplicaError::NoCapacity { .. })),
        "the replica answered {answer_in_process:?}"
    );
    assert_eq!(answer_over_grpc, answer_in_process);
}

#[tokio::test]
async fn append_to_a_draining_replica_is_refused_as_draining() {
    let served = ServedReplica::start("replica-a").await;
    served.replica.drain().await.expect("the replica drains");

    let refused = served.remote.append(append_records(1, 1, 0)).await;

    assert_eq!(refused, Err(ReplicaError::Draining));
}

#[tokio::test]
async fn append_to_a_copy_whose_flush_failed_is_refused_as_a_storage_failure() {
    let served = ServedReplica::start("replica-a").await;
    served
        .replica
        .append(append_records(1, 3, 0))
        .await
        .expect("the append is acknowledged");
    served.disk.fail_flushes(true);
    let _failed_flush = served.replica.append(append_records(4, 1, 0)).await;
    served.disk.fail_flushes(false);
    let answer_in_process = served.replica.append(append_records(4, 1, 0)).await;

    let answer_over_grpc = served.remote.append(append_records(4, 1, 0)).await;

    assert!(
        matches!(answer_in_process, Err(ReplicaError::Storage { .. })),
        "the replica answered {answer_in_process:?}"
    );
    assert_eq!(answer_over_grpc, answer_in_process);
}

/// A replica that is shutting down answers that it is unavailable. That
/// answer is the replica's own and arrives with the replica's own reason,
/// unlike the unavailability the transport reports when nothing answers.
#[tokio::test]
async fn request_to_a_replica_that_is_shutting_down_is_refused_as_unavailable() {
    let served = ServedReplica::start("replica-a").await;
    served.replica.stop();
    let answer_in_process = served.replica.list_segments().await;

    let answer_over_grpc = served.remote.list_segments().await;

    assert!(
        matches!(answer_in_process, Err(ReplicaError::Unavailable { .. })),
        "the replica answered {answer_in_process:?}"
    );
    assert_eq!(answer_over_grpc, answer_in_process);
}

/// One error per variant of `ReplicaError`, with a different value in
/// every field.
fn every_replica_error() -> Vec<ReplicaError> {
    vec![
        ReplicaError::Fenced {
            request_epoch: Epoch::new(3),
            fenced_epoch: Epoch::new(5),
        },
        ReplicaError::Sealed {
            sealed_end: SequenceNumber::new(60),
        },
        ReplicaError::OutOfOrder {
            first_sequence: SequenceNumber::new(12),
            local_end: SequenceNumber::new(9),
        },
        ReplicaError::ConflictingSeal {
            existing_sealed_end: SequenceNumber::new(60),
            requested_sealed_end: SequenceNumber::new(58),
        },
        ReplicaError::SegmentNotFound {
            segment: OTHER_SEGMENT,
        },
        ReplicaError::InvalidRequest {
            reason: "a batch failed its checksum".to_owned(),
        },
        ReplicaError::NoCapacity { used_percent: 96 },
        ReplicaError::Draining,
        ReplicaError::Storage {
            reason: "flush failed: input/output error".to_owned(),
        },
        ReplicaError::Unavailable {
            reason: "the replica is shutting down".to_owned(),
        },
    ]
}

/// Stands in for a replica and answers every call with the same error.
struct RefusingReplica {
    /// The error every call is answered with.
    refusal: ReplicaError,
}

#[async_trait]
impl ReplicaApi for RefusingReplica {
    async fn append(&self, _request: AppendRequest) -> Result<AppendResponse, ReplicaError> {
        Err(self.refusal.clone())
    }

    async fn read(&self, _request: ReadRequest) -> Result<ReadResponse, ReplicaError> {
        Err(self.refusal.clone())
    }

    async fn fence(&self, _request: FenceRequest) -> Result<FenceResponse, ReplicaError> {
        Err(self.refusal.clone())
    }

    async fn seal(&self, _request: SealRequest) -> Result<SealResponse, ReplicaError> {
        Err(self.refusal.clone())
    }

    async fn repair_segment(
        &self,
        _request: RepairRequest,
    ) -> Result<RepairResponse, ReplicaError> {
        Err(self.refusal.clone())
    }

    async fn upload_segment(
        &self,
        _request: UploadRequest,
    ) -> Result<UploadResponse, ReplicaError> {
        Err(self.refusal.clone())
    }

    async fn delete_segment(&self, _segment: SegmentAddress) -> Result<(), ReplicaError> {
        Err(self.refusal.clone())
    }

    async fn list_segments(&self) -> Result<Vec<SegmentStatus>, ReplicaError> {
        Err(self.refusal.clone())
    }

    async fn status(&self) -> Result<ReplicaStatus, ReplicaError> {
        Err(self.refusal.clone())
    }

    async fn drain(&self) -> Result<(), ReplicaError> {
        Err(self.refusal.clone())
    }
}

/// Serves a [`RefusingReplica`] and connects to it.
async fn connect_to_a_replica_that_answers(refusal: ReplicaError) -> Arc<dyn ReplicaApi> {
    let address = serve_on_loopback(
        Arc::new(RefusingReplica { refusal }),
        replica_id("replica-a"),
    )
    .await;
    GrpcReplicaConnector::new(GrpcConnectorSettings::default())
        .connect(&replica_id("replica-a"), &address)
}

/// A repair request whose source is never contacted, for calls that are
/// refused before anything happens.
fn repair_from_nowhere() -> RepairRequest {
    RepairRequest {
        segment: descriptor_of(SEGMENT),
        sealed_end: SequenceNumber::new(5),
        source_replica_id: replica_id("source"),
        source_address: "source.example:7000".to_owned(),
    }
}

/// An upload request, for calls that are refused before anything happens.
fn upload_to_nowhere() -> UploadRequest {
    UploadRequest {
        segment: descriptor_of(SEGMENT),
        sealed_end: SequenceNumber::new(5),
        object_key: "streams/7/segments/00000000000000000003".to_owned(),
    }
}

/// Each call has a response message of its own, so each of them is a way
/// for an error to be carried wrongly. This walks all of them, for every
/// variant.
#[tokio::test]
async fn every_error_reaches_the_caller_unchanged_through_every_call() {
    for refusal in every_replica_error() {
        let remote = connect_to_a_replica_that_answers(refusal.clone()).await;
        let fence_request = FenceRequest {
            segment: SEGMENT,
            epoch: WRITER_EPOCH,
        };
        let seal_request = SealRequest {
            segment: SEGMENT,
            sealed_end: SequenceNumber::new(3),
        };

        let refused_append = remote.append(append_records(1, 1, 0)).await;
        let refused_read = remote.read(read_from(1)).await;
        let refused_fence = remote.fence(fence_request).await;
        let refused_seal = remote.seal(seal_request).await;
        let refused_repair = remote.repair_segment(repair_from_nowhere()).await;
        let refused_upload = remote.upload_segment(upload_to_nowhere()).await;
        let refused_delete = remote.delete_segment(SEGMENT).await;
        let refused_list = remote.list_segments().await;
        let refused_status = remote.status().await;
        let refused_drain = remote.drain().await;

        assert_eq!(refused_append, Err(refusal.clone()));
        assert_eq!(refused_read, Err(refusal.clone()));
        assert_eq!(refused_fence, Err(refusal.clone()));
        assert_eq!(refused_seal, Err(refusal.clone()));
        assert_eq!(refused_repair, Err(refusal.clone()));
        assert_eq!(refused_upload, Err(refusal.clone()));
        assert_eq!(refused_delete, Err(refusal.clone()));
        assert_eq!(refused_list, Err(refusal.clone()));
        assert_eq!(refused_status, Err(refusal.clone()));
        assert_eq!(refused_drain, Err(refusal));
    }
}

// ---- identity ----

/// The safety property of connecting by identity: a node whose disk was
/// replaced answers at the old address under a new identity, and a caller
/// that still asks for the old one must not get an answer.
#[tokio::test]
async fn replica_asked_for_under_another_identity_is_unavailable() {
    let served = ServedReplica::start("replica-a").await;
    let connector = GrpcReplicaConnector::new(GrpcConnectorSettings::default());

    let mistaken_handle = connector.connect(&replica_id("replica-b"), &served.address);
    let answer = mistaken_handle.status().await;

    assert!(
        matches!(answer, Err(ReplicaError::Unavailable { .. })),
        "the handle answered {answer:?}"
    );
}

#[tokio::test]
async fn request_under_another_identity_does_not_reach_the_replica() {
    let served = ServedReplica::start("replica-a").await;
    let connector = GrpcReplicaConnector::new(GrpcConnectorSettings::default());
    let mistaken_handle = connector.connect(&replica_id("replica-b"), &served.address);

    let _refused = mistaken_handle.drain().await;

    let status = served.replica.status().await.expect("the replica reports");
    assert!(!status.is_draining);
}

#[tokio::test]
async fn refusal_of_another_identity_tells_which_replica_answered() {
    let served = ServedReplica::start("replica-a").await;
    let connector = GrpcReplicaConnector::new(GrpcConnectorSettings::default());
    let mistaken_handle = connector.connect(&replica_id("replica-b"), &served.address);

    let error = mistaken_handle.status().await.expect_err("refused");

    assert!(
        error.to_string().contains("this is replica replica-a"),
        "the error was: {error}"
    );
}

/// A channel to `address` for a generated client, without the connector
/// and so without the identity header it attaches.
async fn plain_channel_to(address: &str) -> Channel {
    Endpoint::from_shared(format!("http://{address}"))
        .expect("a loopback address is a valid endpoint")
        .connect()
        .await
        .expect("the server accepts connections")
}

#[tokio::test]
async fn request_without_an_identity_is_refused_as_a_failed_precondition() {
    let served = ServedReplica::start("replica-a").await;
    let mut admin_client = ReplicaAdminClient::new(plain_channel_to(&served.address).await);

    let refusal = admin_client
        .drain(v1::DrainRequest {})
        .await
        .expect_err("refused");

    assert_eq!(refusal.code(), Code::FailedPrecondition);
}

/// A request that cannot be converted is answered by the transport, as an
/// invalid request that names the field at fault, with the gRPC status OK
/// like every other refusal.
#[tokio::test]
async fn request_without_a_required_field_is_refused_as_invalid() {
    let served = ServedReplica::start("replica-a").await;
    let mut data_client = ReplicaDataClient::new(plain_channel_to(&served.address).await);
    let mut request_without_a_segment = Request::new(v1::FenceRequest {
        segment: None,
        epoch: 6,
    });
    request_without_a_segment
        .metadata_mut()
        .insert(REPLICA_ID_HEADER, MetadataValue::from_static("replica-a"));

    let response = data_client
        .fence(request_without_a_segment)
        .await
        .expect("a refusal is an answer")
        .into_inner();

    let invalid_request = ReplicaError::InvalidRequest {
        reason: "field `FenceRequest.segment` is missing".to_owned(),
    };
    assert_eq!(
        response.outcome,
        Some(FenceOutcome::Error(invalid_request.into()))
    );
}

// ---- when nothing answers ----

/// An address that was free a moment ago: the operating system handed it
/// out and the socket was closed again.
async fn address_nobody_listens_on() -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the loopback interface has a free port");
    listener
        .local_addr()
        .expect("a bound socket has an address")
        .to_string()
}

#[tokio::test]
async fn replica_at_an_address_nobody_listens_on_is_unavailable() {
    let connector = GrpcReplicaConnector::new(GrpcConnectorSettings {
        connect_timeout: Duration::from_millis(500),
    });
    let replica = connector.connect(&replica_id("replica-a"), &address_nobody_listens_on().await);

    let answer = timeout(GENEROUS_WAIT, replica.status())
        .await
        .expect("a refused connection fails at once");

    assert!(
        matches!(answer, Err(ReplicaError::Unavailable { .. })),
        "the handle answered {answer:?}"
    );
}

/// A replica served until the test stops it.
struct StoppableServer {
    /// The replica, reached over gRPC.
    remote: Arc<dyn ReplicaApi>,
    /// Completes the server's shutdown future when sent to.
    stop_sender: oneshot::Sender<()>,
    /// The task `serve_replica` runs in, which yields what it returned.
    server_task: tokio::task::JoinHandle<Result<(), bitacora_proto::ServeError>>,
}

impl StoppableServer {
    /// Serves a replica on an empty disk, under the identity `replica-a`.
    async fn start() -> Self {
        let replica = Replica::open(
            Arc::new(MemoryDisk::new()),
            replica_id("replica-a"),
            small_segments(),
            Arc::new(GrpcReplicaConnector::new(GrpcConnectorSettings::default())),
            Arc::new(|_segment, _reason| {}),
        )
        .await
        .expect("the replica opens on a working disk");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the loopback interface has a free port");
        let address = listener
            .local_addr()
            .expect("a bound socket has an address")
            .to_string();
        let (stop_sender, stop_receiver) = oneshot::channel::<()>();
        let shutdown = async move {
            // Stopping is asked for by sending or by dropping the sender;
            // both complete this future.
            let _stop_was_sent = stop_receiver.await;
        };
        let server_task = tokio::spawn(serve_replica(
            Arc::new(replica),
            replica_id("replica-a"),
            listener,
            shutdown,
        ));
        let remote = GrpcReplicaConnector::new(GrpcConnectorSettings::default())
            .connect(&replica_id("replica-a"), &address);
        Self {
            remote,
            stop_sender,
            server_task,
        }
    }
}

#[tokio::test]
async fn server_returns_cleanly_when_asked_to_stop() {
    let server = StoppableServer::start().await;
    server
        .remote
        .status()
        .await
        .expect("the replica reports while the server runs");

    drop(server.stop_sender);
    let what_serving_returned = timeout(GENEROUS_WAIT, server.server_task)
        .await
        .expect("the server stops when asked")
        .expect("the server task does not panic");

    assert!(
        what_serving_returned.is_ok(),
        "the server returned {what_serving_returned:?}"
    );
}

/// The connection a handle used is gone and nothing listens any more. The
/// handle must report that like any other unavailability, not hang on the
/// dead connection.
#[tokio::test]
async fn replica_whose_server_stopped_is_unavailable() {
    let server = StoppableServer::start().await;
    server
        .remote
        .status()
        .await
        .expect("the replica reports while the server runs");
    drop(server.stop_sender);
    let _what_serving_returned = timeout(GENEROUS_WAIT, server.server_task)
        .await
        .expect("the server stops when asked");

    let answer = timeout(GENEROUS_WAIT, server.remote.status())
        .await
        .expect("a lost connection fails at once");

    assert!(
        matches!(answer, Err(ReplicaError::Unavailable { .. })),
        "the handle answered {answer:?}"
    );
}
