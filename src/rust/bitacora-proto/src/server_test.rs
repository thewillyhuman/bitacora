//! Tests of the parts of `server.rs` that need no socket.
//!
//! The identity check is a function of a replica identifier and a set of
//! headers, so it is tested directly. The size cap on reads is tested by
//! calling the service method with [`RecordingReplica`] behind it, a
//! stand-in that remembers the read request it was handed. Everything that
//! needs a connection is in `tests/replica_transport_test.rs`.

use std::sync::Mutex;

use bitacora_segment::format::{BATCH_HEADER_BYTES, MAX_BATCH_BODY_BYTES};
use bitacora_types::{SegmentId, SequenceNumber, StreamId};
use tonic::Code;
use tonic::metadata::MetadataValue;

use super::*;

/// The identity of the replica behind the server under test.
fn own_replica_id() -> ReplicaId {
    ReplicaId::new("replica-a").expect("a valid identifier")
}

/// Request headers that name each of `replica_ids` as the replica the
/// request is meant for.
fn headers_naming(replica_ids: &[&'static str]) -> MetadataMap {
    let mut request_metadata = MetadataMap::new();
    for replica_id in replica_ids {
        request_metadata.append(REPLICA_ID_HEADER, MetadataValue::from_static(replica_id));
    }
    request_metadata
}

/// The gRPC status code `check_identity` refuses `request_metadata` with,
/// or `None` if it lets the request through.
fn refusal_code(request_metadata: &MetadataMap) -> Option<Code> {
    check_identity(&own_replica_id(), request_metadata)
        .err()
        .map(|status| status.code())
}

#[test]
fn request_for_this_replica_passes_the_identity_check() {
    let request_metadata = headers_naming(&["replica-a"]);

    assert_eq!(refusal_code(&request_metadata), None);
}

#[test]
fn request_without_an_identity_is_refused() {
    let request_metadata = MetadataMap::new();

    assert_eq!(
        refusal_code(&request_metadata),
        Some(Code::FailedPrecondition)
    );
}

#[test]
fn request_for_another_replica_is_refused() {
    let request_metadata = headers_naming(&["replica-b"]);

    assert_eq!(
        refusal_code(&request_metadata),
        Some(Code::FailedPrecondition)
    );
}

/// An identifier is compared whole: one that merely starts like this
/// replica's is another replica.
#[test]
fn request_for_a_replica_with_a_longer_name_is_refused() {
    let request_metadata = headers_naming(&["replica-a2"]);

    assert_eq!(
        refusal_code(&request_metadata),
        Some(Code::FailedPrecondition)
    );
}

/// A request that names two replicas is not unambiguously for this one.
#[test]
fn request_for_this_replica_and_another_is_refused() {
    let request_metadata = headers_naming(&["replica-a", "replica-b"]);

    assert_eq!(
        refusal_code(&request_metadata),
        Some(Code::FailedPrecondition)
    );
}

#[test]
fn refusal_tells_which_replica_answered_and_which_was_asked_for() {
    let request_metadata = headers_naming(&["replica-b"]);

    let refusal = check_identity(&own_replica_id(), &request_metadata).expect_err("refused");

    assert_eq!(
        refusal.message(),
        "this is replica replica-a, not \"replica-b\""
    );
}

/// Ties [`LARGEST_READ_CAP_BYTES`] to the size of a batch frame: the
/// largest answer a replica can give under the cap must fit in a message,
/// with room for the other fields of the response.
#[test]
fn largest_answer_to_a_capped_read_fits_in_a_message() {
    let room_for_other_fields = 1024;

    let largest_answer = LARGEST_READ_CAP_BYTES + BATCH_HEADER_BYTES + MAX_BATCH_BODY_BYTES;

    assert!(largest_answer + room_for_other_fields <= MAX_MESSAGE_BYTES);
}

/// A replica that remembers the size cap of the last read it was handed
/// and refuses everything, so that a test can see what the server passes
/// on.
#[derive(Default)]
struct RecordingReplica {
    /// The `max_bytes` of the last read request, if any arrived.
    last_read_cap: Mutex<Option<usize>>,
}

#[tonic::async_trait]
impl ReplicaApi for RecordingReplica {
    async fn append(
        &self,
        _request: domain::AppendRequest,
    ) -> Result<domain::AppendResponse, ReplicaError> {
        Err(ReplicaError::Draining)
    }

    async fn read(
        &self,
        request: domain::ReadRequest,
    ) -> Result<domain::ReadResponse, ReplicaError> {
        *self.last_read_cap.lock().expect("no test panicked") = Some(request.max_bytes);
        Err(ReplicaError::Draining)
    }

    async fn fence(
        &self,
        _request: domain::FenceRequest,
    ) -> Result<domain::FenceResponse, ReplicaError> {
        Err(ReplicaError::Draining)
    }

    async fn seal(
        &self,
        _request: domain::SealRequest,
    ) -> Result<domain::SealResponse, ReplicaError> {
        Err(ReplicaError::Draining)
    }

    async fn repair_segment(
        &self,
        _request: domain::RepairRequest,
    ) -> Result<domain::RepairResponse, ReplicaError> {
        Err(ReplicaError::Draining)
    }

    async fn upload_segment(
        &self,
        _request: domain::UploadRequest,
    ) -> Result<domain::UploadResponse, ReplicaError> {
        Err(ReplicaError::Draining)
    }

    async fn delete_segment(&self, _segment: SegmentAddress) -> Result<(), ReplicaError> {
        Err(ReplicaError::Draining)
    }

    async fn list_segments(&self) -> Result<Vec<domain::SegmentStatus>, ReplicaError> {
        Err(ReplicaError::Draining)
    }

    async fn status(&self) -> Result<domain::ReplicaStatus, ReplicaError> {
        Err(ReplicaError::Draining)
    }

    async fn drain(&self) -> Result<(), ReplicaError> {
        Err(ReplicaError::Draining)
    }
}

/// Sends a read with the size cap `requested_cap_bytes` through the
/// service and returns the cap the replica behind it was handed.
async fn cap_handed_to_the_replica(requested_cap_bytes: usize) -> Option<usize> {
    let recording_replica = Arc::new(RecordingReplica::default());
    let service = ReplicaService {
        replica: Arc::clone(&recording_replica) as Arc<dyn ReplicaApi>,
    };
    let read_request = domain::ReadRequest {
        segment: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
        from_sequence: SequenceNumber::FIRST,
        max_bytes: requested_cap_bytes,
        wait_for_records: None,
        limit: domain::ReadLimit::DurableSequence,
    };

    let _refusal = service
        .read(Request::new(wire::ReadRequest::from(read_request)))
        .await;

    *recording_replica
        .last_read_cap
        .lock()
        .expect("no test panicked")
}

#[tokio::test]
async fn read_without_a_size_cap_is_capped_by_the_server() {
    let handed_cap = cap_handed_to_the_replica(usize::MAX).await;

    assert_eq!(handed_cap, Some(LARGEST_READ_CAP_BYTES));
}

#[tokio::test]
async fn read_with_a_small_size_cap_keeps_it() {
    let handed_cap = cap_handed_to_the_replica(4096).await;

    assert_eq!(handed_cap, Some(4096));
}

#[tokio::test]
async fn malformed_request_is_answered_without_calling_the_replica() {
    let recording_replica = Arc::new(RecordingReplica::default());
    let service = ReplicaService {
        replica: Arc::clone(&recording_replica) as Arc<dyn ReplicaApi>,
    };
    let request_without_a_segment = wire::ReadRequest {
        segment: None,
        from_sequence: 1,
        max_bytes: 4096,
        wait_for_records_nanoseconds: None,
        recovery_epoch: None,
    };

    let _answer = service.read(Request::new(request_without_a_segment)).await;

    let handed_cap = *recording_replica
        .last_read_cap
        .lock()
        .expect("no test panicked");
    assert_eq!(handed_cap, None);
}
