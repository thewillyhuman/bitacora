//! Tests of the connector over a fixed set of replicas, of the unreachable replica, and of replica errors.
//!
//! # What is covered
//!
//! - [`StaticReplicaConnector`]: a registered replica is reached by identity and address together; an
//!   unknown address and an address served by another identity both behave like a replica that is down.
//! - [`UnreachableReplica`]: every operation fails as unavailable with the handle's reason.
//! - [`ReplicaError`]: which errors are worth retrying, and the wording of the one message other code
//!   quotes.
//!
//! The operations of [`ReplicaApi`] themselves are tested where they are implemented, in
//! `bitacora-replica`.
//!
//! # How the fixtures work
//!
//! `connector_with_replica_a` registers, at `replica-a:7000`, a handle that fails with a reason of its
//! own. A test that sees that reason knows the request reached the registered handle and not a stand-in.

use super::*;
use crate::{SegmentId, StreamId};

/// The address `connector_with_replica_a` registers its replica at.
const REPLICA_A_ADDRESS: &str = "replica-a:7000";

/// The reason the registered handle fails with, which proves a request reached it.
const REGISTERED_HANDLE_ANSWERED: &str = "the registered handle answered";

/// The replica identifier `name`. Panics if `name` is not a valid identifier.
fn replica_id(name: &str) -> ReplicaId {
    ReplicaId::new(name).expect("the test names a valid identifier")
}

/// A connector that knows one replica, `replica-a`, at [`REPLICA_A_ADDRESS`].
fn connector_with_replica_a() -> StaticReplicaConnector {
    let mut connector = StaticReplicaConnector::new();
    connector.register(
        replica_id("replica-a"),
        REPLICA_A_ADDRESS,
        Arc::new(UnreachableReplica::new(REGISTERED_HANDLE_ANSWERED)),
    );
    connector
}

/// The error every operation of a handle with `reason` fails with.
fn unavailable(reason: &str) -> ReplicaError {
    ReplicaError::Unavailable {
        reason: reason.to_owned(),
    }
}

/// The segment the requests of these tests name.
fn segment() -> SegmentAddress {
    SegmentAddress::new(StreamId::new(7), SegmentId::new(3))
}

/// What a replica needs to create its copy of [`segment`].
fn descriptor() -> SegmentDescriptor {
    SegmentDescriptor {
        address: segment(),
        epoch: Epoch::new(5),
        first_sequence: SequenceNumber::new(100),
    }
}

#[tokio::test]
async fn unknown_address_behaves_like_a_replica_that_is_down() {
    let connector = StaticReplicaConnector::new();

    let status = connector
        .connect(&replica_id("replica-a"), REPLICA_A_ADDRESS)
        .status()
        .await;

    assert_eq!(status, Err(unavailable("no replica is reachable at replica-a:7000")));
}

#[tokio::test]
async fn registered_replica_is_reached_at_its_address() {
    let status = connector_with_replica_a()
        .connect(&replica_id("replica-a"), REPLICA_A_ADDRESS)
        .status()
        .await;

    assert_eq!(status, Err(unavailable(REGISTERED_HANDLE_ANSWERED)));
}

#[tokio::test]
async fn address_served_by_another_replica_behaves_like_one_that_is_down() {
    let status = connector_with_replica_a()
        .connect(&replica_id("replica-a-disk-1"), REPLICA_A_ADDRESS)
        .status()
        .await;

    assert_eq!(
        status,
        Err(unavailable("replica-a:7000 is replica replica-a, not replica-a-disk-1"))
    );
}

#[tokio::test]
async fn registering_again_at_an_address_replaces_the_replica_there() {
    let mut connector = connector_with_replica_a();
    connector.register(
        replica_id("replica-a-disk-1"),
        REPLICA_A_ADDRESS,
        Arc::new(UnreachableReplica::new("the rebuilt node answered")),
    );

    let status = connector
        .connect(&replica_id("replica-a-disk-1"), REPLICA_A_ADDRESS)
        .status()
        .await;

    assert_eq!(status, Err(unavailable("the rebuilt node answered")));
}

#[tokio::test]
async fn unreachable_replica_fails_every_data_operation_with_its_reason() {
    let replica = UnreachableReplica::new("gone");
    let append_request = AppendRequest {
        segment: descriptor(),
        writer_epoch: Epoch::new(5),
        batch_frames: Bytes::new(),
        durable_sequence: SequenceNumber::ZERO,
    };
    let read_request = ReadRequest {
        segment: segment(),
        from_sequence: SequenceNumber::new(100),
        max_bytes: 1024,
        wait_for_records: None,
        limit: ReadLimit::DurableSequence,
    };
    let fence_request = FenceRequest {
        segment: segment(),
        epoch: Epoch::new(6),
    };
    let seal_request = SealRequest {
        segment: segment(),
        sealed_end: SequenceNumber::new(104),
    };

    assert_eq!(replica.append(append_request).await, Err(unavailable("gone")));
    assert_eq!(replica.read(read_request).await, Err(unavailable("gone")));
    assert_eq!(replica.fence(fence_request).await, Err(unavailable("gone")));
    assert_eq!(replica.seal(seal_request).await, Err(unavailable("gone")));
}

#[tokio::test]
async fn unreachable_replica_fails_every_transfer_operation_with_its_reason() {
    let replica = UnreachableReplica::new("gone");
    let repair_request = RepairRequest {
        segment: descriptor(),
        sealed_end: SequenceNumber::new(104),
        source_replica_id: replica_id("replica-b"),
        source_address: "replica-b:7000".to_owned(),
    };
    let upload_request = UploadRequest {
        segment: descriptor(),
        sealed_end: SequenceNumber::new(104),
        object_key: "streams/7/segments/00000000000000000003".to_owned(),
    };

    assert_eq!(replica.repair_segment(repair_request).await, Err(unavailable("gone")));
    assert_eq!(replica.upload_segment(upload_request).await, Err(unavailable("gone")));
}

#[tokio::test]
async fn unreachable_replica_fails_every_administrative_operation_with_its_reason() {
    let replica = UnreachableReplica::new("gone");

    assert_eq!(replica.delete_segment(segment()).await, Err(unavailable("gone")));
    assert_eq!(replica.list_segments().await, Err(unavailable("gone")));
    assert_eq!(replica.status().await, Err(unavailable("gone")));
    assert_eq!(replica.drain().await, Err(unavailable("gone")));
}

#[test]
fn unavailability_is_worth_retrying() {
    assert!(unavailable("connection reset").is_retriable());
}

#[test]
fn refusal_is_not_worth_retrying() {
    assert!(!ReplicaError::Draining.is_retriable());
}

#[test]
fn fenced_error_names_both_epochs() {
    let fenced = ReplicaError::Fenced {
        request_epoch: Epoch::new(4),
        fenced_epoch: Epoch::new(5),
    };

    assert_eq!(
        fenced.to_string(),
        "epoch 4 is fenced: the replica accepts epoch 5 or later"
    );
}
