//! Tests of the status codes and of the mapping from the client's errors to them.
//!
//! # What is covered
//!
//! - The number of every status, which C programs have compiled in and must never change.
//! - One test per case of every client error type: which status it becomes.
//! - What travels with a status: the client's own message, and the low-water mark of a truncation.
//!
//! # How the fixtures work
//!
//! Errors are built directly where their types can be named, with made-up but valid contents. The three
//! reasons a cluster configuration cannot be used are obtained from the facade itself, by connecting with
//! a configuration that has that defect, because their inner types belong to crates this one does not
//! depend on.

use std::path::PathBuf;

use bitacora::{ClusterConfig, DEFAULT_ETCD_KEY_PREFIX, ObjectStoreConfig};
use bitacora_meta::StoreUnavailable;
use bitacora_segment::format::EncodeError;
use bitacora_types::{Epoch, SegmentAddress, SegmentId, StreamId, Timestamp};

use super::*;

/// A segment for the errors that name one.
const SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));

/// The status an error of the client becomes.
fn status_of(client_error: impl Into<Failure>) -> BitacoraStatus {
    client_error.into().status
}

/// A cluster configuration with one etcd endpoint and the given object store.
fn config_with(object_store: ObjectStoreConfig) -> ClusterConfig {
    ClusterConfig {
        etcd_endpoints: vec!["http://127.0.0.1:2379".to_owned()],
        etcd_key_prefix: DEFAULT_ETCD_KEY_PREFIX.to_owned(),
        object_store,
    }
}

#[test]
fn status_numbers_are_the_ones_c_programs_were_compiled_with() {
    let numbers = [
        BitacoraStatus::Ok as u32,
        BitacoraStatus::InvalidArgument as u32,
        BitacoraStatus::NotFound as u32,
        BitacoraStatus::AlreadyExists as u32,
        BitacoraStatus::Unavailable as u32,
        BitacoraStatus::TimedOut as u32,
        BitacoraStatus::Fenced as u32,
        BitacoraStatus::Indeterminate as u32,
        BitacoraStatus::Truncated as u32,
        BitacoraStatus::NothingNew as u32,
        BitacoraStatus::Closed as u32,
        BitacoraStatus::LeaseHeld as u32,
        BitacoraStatus::Internal as u32,
    ];

    assert_eq!(numbers, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
}

#[test]
fn failure_keeps_the_message_of_the_error_it_was_made_from() {
    let failure = Failure::from(AppendError::Fenced);

    assert_eq!(failure.message, AppendError::Fenced.to_string());
}

#[test]
fn failure_for_an_invalid_argument_has_that_status() {
    let failure = Failure::invalid_argument("writer is null");

    assert_eq!(failure.status, BitacoraStatus::InvalidArgument);
}

#[test]
fn append_by_a_replaced_writer_is_fenced() {
    assert_eq!(status_of(AppendError::Fenced), BitacoraStatus::Fenced);
}

#[test]
fn append_with_an_unknown_outcome_is_indeterminate() {
    let append_error = AppendError::Indeterminate {
        reason: "the writer lost its replicas".to_owned(),
    };

    assert_eq!(status_of(append_error), BitacoraStatus::Indeterminate);
}

#[test]
fn append_to_a_closed_writer_is_closed() {
    assert_eq!(status_of(AppendError::Closed), BitacoraStatus::Closed);
}

#[test]
fn append_of_a_record_that_breaks_a_limit_is_an_invalid_argument() {
    let append_error = AppendError::Rejected(EncodeError::NoRecords);

    assert_eq!(status_of(append_error), BitacoraStatus::InvalidArgument);
}

#[test]
fn opening_a_stream_that_does_not_exist_is_not_found() {
    let open_error = OpenError::Metadata(MetaError::StreamNotFound {
        name: "orders".to_owned(),
    });

    assert_eq!(status_of(open_error), BitacoraStatus::NotFound);
}

#[test]
fn opening_a_stream_another_candidate_claimed_first_is_fenced() {
    let open_error = OpenError::TakenByAnotherWriter {
        current_epoch: Epoch::new(9),
    };

    assert_eq!(status_of(open_error), BitacoraStatus::Fenced);
}

#[test]
fn opening_a_stream_whose_writer_is_alive_is_lease_held() {
    let open_error = OpenError::LeaseHeld {
        writer_identity: "compute-1".to_owned(),
        expires_at: Timestamp::from_unix_millis(1_000),
    };

    assert_eq!(status_of(open_error), BitacoraStatus::LeaseHeld);
}

#[test]
fn opening_a_stream_without_a_recovery_quorum_is_unavailable() {
    let open_error = OpenError::RecoveryQuorumUnavailable {
        segment: SEGMENT,
        needed: 2,
        reached: 1,
    };

    assert_eq!(status_of(open_error), BitacoraStatus::Unavailable);
}

#[test]
fn opening_a_stream_without_a_standby_segment_is_unavailable() {
    let open_error = OpenError::NoStandbySegment {
        stream: "orders".to_owned(),
    };

    assert_eq!(status_of(open_error), BitacoraStatus::Unavailable);
}

#[test]
fn reading_a_stream_that_does_not_exist_is_not_found() {
    let read_error = ReadError::Metadata(MetaError::StreamNotFound {
        name: "orders".to_owned(),
    });

    assert_eq!(status_of(read_error), BitacoraStatus::NotFound);
}

#[test]
fn reading_deleted_records_is_truncated() {
    let read_error = ReadError::Truncated {
        low_water_mark: SequenceNumber::new(40),
    };

    assert_eq!(status_of(read_error), BitacoraStatus::Truncated);
}

#[test]
fn truncation_carries_the_low_water_mark() {
    let read_error = ReadError::Truncated {
        low_water_mark: SequenceNumber::new(40),
    };

    let failure = Failure::from(read_error);

    assert_eq!(failure.low_water_mark, Some(SequenceNumber::new(40)));
}

#[test]
fn failure_other_than_a_truncation_carries_no_low_water_mark() {
    let read_error = ReadError::Unavailable {
        segment: SEGMENT,
        reason: "no replica answered".to_owned(),
    };

    let failure = Failure::from(read_error);

    assert_eq!(failure.low_water_mark, None);
}

#[test]
fn reading_records_no_replica_serves_is_unavailable() {
    let read_error = ReadError::Unavailable {
        segment: SEGMENT,
        reason: "no replica answered".to_owned(),
    };

    assert_eq!(status_of(read_error), BitacoraStatus::Unavailable);
}

#[tokio::test]
async fn configuration_without_etcd_endpoints_is_an_invalid_argument() {
    let config = ClusterConfig {
        etcd_endpoints: Vec::new(),
        ..config_with(ObjectStoreConfig::None)
    };

    let connect_error = bitacora::connect_metadata(&config)
        .await
        .err()
        .expect("no endpoint can never work");

    assert_eq!(status_of(connect_error), BitacoraStatus::InvalidArgument);
}

#[tokio::test]
async fn object_store_settings_that_can_never_work_are_an_invalid_argument() {
    let plain_http_without_consent = ObjectStoreConfig::S3 {
        bucket: "bitacora".to_owned(),
        region: "us-east-1".to_owned(),
        endpoint: Some("http://127.0.0.1:9000".to_owned()),
        key_prefix: String::new(),
        force_path_style: true,
        allow_http: false,
    };

    let connect_error = bitacora::open_object_store(&plain_http_without_consent)
        .await
        .err()
        .expect("plain HTTP is refused unless allowed");

    assert_eq!(status_of(connect_error), BitacoraStatus::InvalidArgument);
}

#[tokio::test]
async fn object_store_that_cannot_be_opened_is_unavailable() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let occupied_by_a_file = directory.path().join("occupied");
    // A test plants a file where the store wants a directory; there is no injected disk to go through.
    #[allow(clippy::disallowed_methods)]
    std::fs::write(&occupied_by_a_file, b"not a directory").expect("the file is planted");
    let below_a_file = ObjectStoreConfig::LocalDirectory {
        path: PathBuf::from(&occupied_by_a_file).join("store"),
    };

    let connect_error = bitacora::open_object_store(&below_a_file)
        .await
        .err()
        .expect("a path below a file cannot become a directory");

    assert_eq!(status_of(connect_error), BitacoraStatus::Unavailable);
}

#[test]
fn metadata_store_that_cannot_be_reached_is_unavailable() {
    let meta_error = MetaError::Unavailable(StoreUnavailable {
        reason: "connection refused".to_owned(),
    });

    assert_eq!(status_of(meta_error), BitacoraStatus::Unavailable);
}

#[test]
fn stream_without_a_standby_segment_is_unavailable() {
    let meta_error = MetaError::NoStandbySegment {
        name: "orders".to_owned(),
    };

    assert_eq!(status_of(meta_error), BitacoraStatus::Unavailable);
}

#[test]
fn record_changed_by_others_on_every_attempt_is_unavailable() {
    let meta_error = MetaError::TooMuchContention {
        key: "streams/orders".to_owned(),
        attempts: 16,
    };

    assert_eq!(status_of(meta_error), BitacoraStatus::Unavailable);
}

#[test]
fn stream_that_does_not_exist_is_not_found() {
    let meta_error = MetaError::StreamNotFound {
        name: "orders".to_owned(),
    };

    assert_eq!(status_of(meta_error), BitacoraStatus::NotFound);
}

#[test]
fn materializer_that_is_not_registered_is_not_found() {
    let meta_error = MetaError::MaterializerNotRegistered {
        stream_id: StreamId::new(7),
        materializer: "pages".to_owned(),
    };

    assert_eq!(status_of(meta_error), BitacoraStatus::NotFound);
}

#[test]
fn stream_that_exists_already_is_already_exists() {
    let meta_error = MetaError::StreamAlreadyExists {
        name: "orders".to_owned(),
    };

    assert_eq!(status_of(meta_error), BitacoraStatus::AlreadyExists);
}

#[test]
fn name_that_is_not_allowed_is_an_invalid_argument() {
    let meta_error = MetaError::InvalidName {
        name: "or/ders".to_owned(),
    };

    assert_eq!(status_of(meta_error), BitacoraStatus::InvalidArgument);
}

#[test]
fn request_that_contradicts_the_store_is_an_invalid_argument() {
    let meta_error = MetaError::InvalidState {
        reason: "the segment is not the stream's last".to_owned(),
    };

    assert_eq!(status_of(meta_error), BitacoraStatus::InvalidArgument);
}

#[test]
fn superseded_epoch_is_fenced() {
    let meta_error = MetaError::Fenced {
        held_epoch: Epoch::new(4),
        current_epoch: Epoch::new(5),
    };

    assert_eq!(status_of(meta_error), BitacoraStatus::Fenced);
}

#[test]
fn superseded_materializer_token_is_fenced() {
    let meta_error = MetaError::MaterializerFenced {
        held_token: 4,
        current_token: 5,
    };

    assert_eq!(status_of(meta_error), BitacoraStatus::Fenced);
}

#[test]
fn stored_record_that_cannot_be_decoded_is_internal() {
    let meta_error = MetaError::CorruptRecord {
        key: "streams/orders".to_owned(),
        reason: "not JSON".to_owned(),
    };

    assert_eq!(status_of(meta_error), BitacoraStatus::Internal);
}
