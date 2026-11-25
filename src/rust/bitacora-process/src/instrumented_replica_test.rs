//! Tests of the instrumented replica.
//!
//! # What is covered
//!
//! - Every operation is recorded under its own name, and a failure under its own kind.
//! - Results, successful or not, come back unchanged.
//! - Every variant of `ReplicaError` has a label value of its own.
//!
//! # How the fixtures work
//!
//! Two replicas stand behind the decorator. `UnreachableReplica` fails every call the same way, which
//! lets one loop walk all ten operations. A real replica on an in-memory disk provides answers that are
//! worth comparing. Metrics are read from the text exposition, as Prometheus reads them.

use bitacora_replica::{Replica, ReplicaSettings};
use bitacora_segment::MemoryDisk;
use bitacora_types::replica_api::{ReadLimit, SegmentDescriptor, StaticReplicaConnector, UnreachableReplica};
use bitacora_types::{Epoch, ReplicaId, SegmentId, SequenceNumber, StreamId};
use bytes::Bytes;

use super::*;

/// The segment the requests of the tests name.
const SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));

/// Every operation of the interface, as its `operation` label.
const EVERY_OPERATION: [&str; 10] = [
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

/// What a replica needs to know to create its copy of [`SEGMENT`].
fn descriptor() -> SegmentDescriptor {
    SegmentDescriptor {
        address: SEGMENT,
        epoch: Epoch::new(1),
        first_sequence: SequenceNumber::FIRST,
    }
}

/// The value at the end of the exposition line that starts with `series`, if there is such a line.
fn exposition_value(registry: &MetricsRegistry, series: &str) -> Option<String> {
    for line in registry.encode().lines() {
        if let Some(value) = line.strip_prefix(series) {
            return Some(value.trim().to_owned());
        }
    }
    None
}

/// A decorator around a replica that fails every call as unavailable, and the registry it records in.
fn instrumented_unreachable_replica() -> (InstrumentedReplica, MetricsRegistry) {
    let registry = MetricsRegistry::new();
    let unreachable: Arc<dyn ReplicaApi> = Arc::new(UnreachableReplica::new("the test says so"));
    (InstrumentedReplica::new(unreachable, &registry), registry)
}

/// A real replica on an empty in-memory disk, the same replica instrumented, and the registry.
async fn instrumented_real_replica() -> (Arc<Replica>, InstrumentedReplica, MetricsRegistry) {
    let replica = Replica::open(
        Arc::new(MemoryDisk::new()),
        ReplicaId::new("replica-a").expect("a valid identifier"),
        ReplicaSettings::default(),
        Arc::new(StaticReplicaConnector::new()),
        Arc::new(|_segment, _reason| {}),
    )
    .await
    .expect("a replica opens on an empty disk");
    let replica = Arc::new(replica);
    let registry = MetricsRegistry::new();
    let instrumented = InstrumentedReplica::new(Arc::clone(&replica) as Arc<dyn ReplicaApi>, &registry);
    (replica, instrumented, registry)
}

/// Calls every operation of `replica` once, in the order of [`EVERY_OPERATION`], ignoring the answers.
async fn call_every_operation(replica: &dyn ReplicaApi) {
    let _append = replica
        .append(AppendRequest {
            segment: descriptor(),
            writer_epoch: Epoch::new(1),
            batch_frames: Bytes::new(),
            durable_sequence: SequenceNumber::ZERO,
        })
        .await;
    let _read = replica
        .read(ReadRequest {
            segment: SEGMENT,
            from_sequence: SequenceNumber::FIRST,
            max_bytes: 1024,
            wait_for_records: None,
            limit: ReadLimit::DurableSequence,
        })
        .await;
    let _fence = replica
        .fence(FenceRequest {
            segment: SEGMENT,
            epoch: Epoch::new(2),
        })
        .await;
    let _seal = replica
        .seal(SealRequest {
            segment: SEGMENT,
            sealed_end: SequenceNumber::ZERO,
        })
        .await;
    let _repair = replica
        .repair_segment(RepairRequest {
            segment: descriptor(),
            sealed_end: SequenceNumber::ZERO,
            source_replica_id: ReplicaId::new("replica-b").expect("a valid identifier"),
            source_address: "replica-b.example:7000".to_owned(),
        })
        .await;
    let _upload = replica
        .upload_segment(UploadRequest {
            segment: descriptor(),
            sealed_end: SequenceNumber::ZERO,
            object_key: "streams/7/segments/00000000000000000003".to_owned(),
        })
        .await;
    let _delete = replica.delete_segment(SEGMENT).await;
    let _list = replica.list_segments().await;
    let _status = replica.status().await;
    let _drain = replica.drain().await;
}

#[tokio::test(start_paused = true)]
async fn every_operation_is_counted_once_under_its_own_name() {
    let (instrumented, registry) = instrumented_unreachable_replica();

    call_every_operation(&instrumented).await;

    for operation in EVERY_OPERATION {
        assert_eq!(
            exposition_value(
                &registry,
                &format!("bitacora_replica_requests_total{{operation=\"{operation}\"}} ")
            ),
            Some("1".to_owned()),
            "operation {operation}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn every_failed_operation_is_counted_under_its_name_and_kind() {
    let (instrumented, registry) = instrumented_unreachable_replica();

    call_every_operation(&instrumented).await;

    for operation in EVERY_OPERATION {
        assert_eq!(
            exposition_value(
                &registry,
                &format!("bitacora_replica_request_failures_total{{operation=\"{operation}\",error=\"unavailable\"}} ")
            ),
            Some("1".to_owned()),
            "operation {operation}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn every_operation_has_its_duration_observed() {
    let (instrumented, registry) = instrumented_unreachable_replica();

    call_every_operation(&instrumented).await;

    for operation in EVERY_OPERATION {
        assert_eq!(
            exposition_value(
                &registry,
                &format!("bitacora_replica_request_duration_seconds_count{{operation=\"{operation}\"}} ")
            ),
            Some("1".to_owned()),
            "operation {operation}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn failure_of_the_wrapped_replica_is_returned_unchanged() {
    let (instrumented, _registry) = instrumented_unreachable_replica();

    let status = instrumented.status().await;

    assert_eq!(
        status,
        Err(ReplicaError::Unavailable {
            reason: "the test says so".to_owned()
        })
    );
}

#[tokio::test(start_paused = true)]
async fn answer_of_the_wrapped_replica_is_returned_unchanged() {
    let (replica, instrumented, _registry) = instrumented_real_replica().await;

    let status_through_the_decorator = instrumented.status().await;

    assert_eq!(status_through_the_decorator, replica.status().await);
}

#[tokio::test(start_paused = true)]
async fn request_through_the_decorator_reaches_the_wrapped_replica() {
    let (replica, instrumented, _registry) = instrumented_real_replica().await;

    instrumented.drain().await.expect("the replica drains");

    let status = replica.status().await.expect("the replica reports");
    assert!(status.is_draining);
}

#[tokio::test(start_paused = true)]
async fn successful_request_counts_no_failure() {
    let (_replica, instrumented, registry) = instrumented_real_replica().await;

    instrumented.status().await.expect("the replica reports");

    assert!(
        !registry.encode().contains("bitacora_replica_request_failures_total{"),
        "{}",
        registry.encode()
    );
}

#[tokio::test(start_paused = true)]
async fn refusal_is_counted_under_the_kind_the_replica_gave() {
    let (_replica, instrumented, registry) = instrumented_real_replica().await;

    let read_of_an_unknown_segment = instrumented
        .read(ReadRequest {
            segment: SEGMENT,
            from_sequence: SequenceNumber::FIRST,
            max_bytes: 1024,
            wait_for_records: None,
            limit: ReadLimit::DurableSequence,
        })
        .await;

    assert_eq!(
        read_of_an_unknown_segment,
        Err(ReplicaError::SegmentNotFound { segment: SEGMENT })
    );
    assert_eq!(
        exposition_value(
            &registry,
            "bitacora_replica_request_failures_total{operation=\"read\",error=\"segment_not_found\"} "
        ),
        Some("1".to_owned())
    );
}

#[test]
fn every_kind_of_error_has_its_own_label_value() {
    let sequence = SequenceNumber::FIRST;
    let reason = String::new();
    let errors_and_kinds = [
        (
            ReplicaError::Fenced {
                request_epoch: Epoch::new(1),
                fenced_epoch: Epoch::new(2),
            },
            "fenced",
        ),
        (ReplicaError::Sealed { sealed_end: sequence }, "sealed"),
        (
            ReplicaError::OutOfOrder {
                first_sequence: sequence,
                local_end: sequence,
            },
            "out_of_order",
        ),
        (
            ReplicaError::ConflictingSeal {
                existing_sealed_end: sequence,
                requested_sealed_end: sequence,
            },
            "conflicting_seal",
        ),
        (ReplicaError::SegmentNotFound { segment: SEGMENT }, "segment_not_found"),
        (
            ReplicaError::InvalidRequest { reason: reason.clone() },
            "invalid_request",
        ),
        (ReplicaError::NoCapacity { used_percent: 96 }, "no_capacity"),
        (ReplicaError::Draining, "draining"),
        (ReplicaError::Storage { reason: reason.clone() }, "storage"),
        (ReplicaError::Unavailable { reason }, "unavailable"),
    ];

    for (error, expected_kind) in &errors_and_kinds {
        assert_eq!(kind_of_replica_error(error), *expected_kind, "{error:?}");
    }
}
