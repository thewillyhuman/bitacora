//! Tests of stream administration that need no cluster.
//!
//! # What is covered
//!
//! - Turning the options C passes into the client's: the defaults, explicit values, and values that do
//!   not go together.
//! - Picking the fields of a description out of a stream record, one field per test.
//! - The sizes of the two structures, which programs have compiled in.
//! - Every argument check of every stream function.
//!
//! Creating, describing, truncating and deleting against a metadata store are covered by
//! `tests/c_programs_test.rs`, which runs a C program against a cluster of real processes.
//!
//! # How the fixtures work
//!
//! `described_stream` is the description of a stream with a sealed segment that ends at 40, an open
//! segment after it, a writer, two materializers and a low-water mark of 12. Each test of the conversion
//! reads one field of what it becomes.

use std::collections::BTreeSet;

use bitacora_meta::{RECORD_SCHEMA_VERSION, SealRecord, SegmentRecord, StreamRecord, WriterLease};
use bitacora_types::{Epoch, SegmentId, StreamId, Timestamp};

use super::*;
use crate::test_support::{TestClient, last_message};

/// Options with their size set and everything else left at zero.
fn zeroed_options() -> StreamOptions {
    StreamOptions {
        struct_size: u32::try_from(size_of::<StreamOptions>()).expect("a small structure"),
        ..StreamOptions::with_defaults()
    }
}

/// A segment of the described stream.
fn segment(segment_id: u64, first_sequence: u64, sealed_end: Option<u64>) -> SegmentRecord {
    SegmentRecord {
        segment_id: SegmentId::new(segment_id),
        epoch: Epoch::new(3),
        first_sequence: SequenceNumber::new(first_sequence),
        sealed: sealed_end.map(|end| SealRecord {
            end: SequenceNumber::new(end),
            at: Timestamp::from_unix_millis(1_000),
        }),
        copies: Vec::new(),
        tiered: None,
    }
}

/// The description of a stream with one sealed and one open segment, a writer and two materializers.
fn described_stream() -> ClientStreamDescription {
    let mut materializers = BTreeSet::new();
    materializers.insert("pages".to_owned());
    materializers.insert("search".to_owned());
    ClientStreamDescription {
        stream: StreamRecord {
            schema_version: RECORD_SCHEMA_VERSION,
            stream_id: StreamId::new(7),
            name: "orders".to_owned(),
            replication: ReplicationSettings::new(5, 3).expect("3 of 5 is valid"),
            retention: RetentionPolicy::default(),
            epoch: Epoch::new(3),
            writer: Some(WriterLease {
                writer_identity: "compute-1".to_owned(),
                expires_at: Timestamp::from_unix_millis(2_000),
            }),
            segments: vec![segment(0, 1, Some(40)), segment(1, 41, None)],
            next_segment_id: SegmentId::new(2),
            standby_replicas: None,
            truncate_before: None,
            low_water_mark: SequenceNumber::new(12),
            materializers,
        },
        checkpoints: Vec::new(),
    }
}

#[test]
fn stream_options_are_sixteen_bytes() {
    assert_eq!(size_of::<StreamOptions>(), 16);
}

#[test]
fn stream_description_is_forty_eight_bytes() {
    assert_eq!(size_of::<StreamDescription>(), 48);
}

#[test]
fn options_left_at_zero_take_the_default_replication() {
    let options = client_stream_options(zeroed_options()).expect("the defaults are valid");

    assert_eq!(options.replication, ReplicationSettings::DEFAULT);
}

#[test]
fn options_left_at_zero_set_no_minimum_retention() {
    let options = client_stream_options(zeroed_options()).expect("the defaults are valid");

    assert_eq!(options.retention, RetentionPolicy::default());
}

#[test]
fn explicit_replication_is_taken_over() {
    let passed = StreamOptions {
        replica_count: 5,
        write_quorum: 3,
        ..zeroed_options()
    };

    let options = client_stream_options(passed).expect("3 of 5 is valid");

    assert_eq!(
        options.replication,
        ReplicationSettings::new(5, 3).expect("3 of 5 is valid")
    );
}

#[test]
fn retention_in_seconds_is_taken_over() {
    let passed = StreamOptions {
        keep_sealed_for_seconds: 3_600,
        ..zeroed_options()
    };

    let options = client_stream_options(passed).expect("an hour is valid");

    assert_eq!(options.retention.keep_sealed_for(), Some(Duration::from_secs(3_600)));
}

#[test]
fn write_quorum_above_the_replica_count_is_an_invalid_argument() {
    let passed = StreamOptions {
        replica_count: 3,
        write_quorum: 4,
        ..zeroed_options()
    };

    let refused = client_stream_options(passed).expect_err("4 of 3 is not valid");

    assert_eq!(refused.status, BitacoraStatus::InvalidArgument);
}

#[test]
fn replica_count_without_a_write_quorum_is_an_invalid_argument() {
    let passed = StreamOptions {
        replica_count: 3,
        write_quorum: 0,
        ..zeroed_options()
    };

    let refused = client_stream_options(passed).expect_err("a quorum of 0 is not valid");

    assert_eq!(refused.status, BitacoraStatus::InvalidArgument);
}

#[test]
fn description_reports_the_stream_identifier() {
    assert_eq!(stream_description(&described_stream()).stream_id, 7);
}

#[test]
fn description_reports_the_epoch() {
    assert_eq!(stream_description(&described_stream()).epoch, 3);
}

#[test]
fn description_reports_the_low_water_mark() {
    assert_eq!(stream_description(&described_stream()).low_water_mark, 12);
}

#[test]
fn description_counts_the_segments() {
    assert_eq!(stream_description(&described_stream()).segment_count, 2);
}

#[test]
fn description_reports_the_end_of_the_last_sealed_segment() {
    assert_eq!(stream_description(&described_stream()).last_sealed_sequence, 40);
}

#[test]
fn description_of_a_stream_without_sealed_segments_reports_zero() {
    let mut description = described_stream();
    description.stream.segments = vec![segment(0, 1, None)];

    assert_eq!(stream_description(&description).last_sealed_sequence, 0);
}

#[test]
fn description_counts_the_materializers() {
    assert_eq!(stream_description(&described_stream()).materializer_count, 2);
}

#[test]
fn description_reports_the_replication() {
    let description = stream_description(&described_stream());

    assert_eq!((description.replica_count, description.write_quorum), (5, 3));
}

#[test]
fn description_says_that_a_writer_holds_the_stream() {
    assert_eq!(stream_description(&described_stream()).has_writer, 1);
}

#[test]
fn description_says_that_no_writer_holds_the_stream() {
    let mut description = described_stream();
    description.stream.writer = None;

    assert_eq!(stream_description(&description).has_writer, 0);
}

#[test]
fn creating_without_a_client_is_refused_by_name() {
    // SAFETY: a null client is allowed, the name is NUL-terminated, and null options mean the defaults.
    let status = unsafe { bitacora_stream_create(std::ptr::null(), c"orders".as_ptr(), std::ptr::null()) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "client is null");
}

#[test]
fn creating_without_a_stream_name_is_refused_by_name() {
    let client = TestClient::connect();

    // SAFETY: the client is live, and null pointers are allowed.
    let status = unsafe { bitacora_stream_create(client.handle(), std::ptr::null(), std::ptr::null()) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "stream_name is null");
}

#[test]
fn creating_with_options_whose_size_was_not_set_is_an_invalid_argument() {
    let client = TestClient::connect();
    let options = StreamOptions::with_defaults();

    // SAFETY: the client is live, the name is NUL-terminated, and the options are a live structure.
    let status = unsafe { bitacora_stream_create(client.handle(), c"orders".as_ptr(), &raw const options) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
}

#[test]
fn creating_with_options_that_do_not_go_together_is_an_invalid_argument() {
    let client = TestClient::connect();
    let options = StreamOptions {
        replica_count: 3,
        write_quorum: 4,
        ..zeroed_options()
    };

    // SAFETY: the client is live, the name is NUL-terminated, and the options are a live structure with
    // their size set.
    let status = unsafe { bitacora_stream_create(client.handle(), c"orders".as_ptr(), &raw const options) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
}

#[test]
fn describing_without_a_client_is_refused_by_name() {
    let mut description = StreamDescription::with_defaults();

    // SAFETY: a null client is allowed, the name is NUL-terminated, and the output is a live structure.
    let status = unsafe { bitacora_stream_describe(std::ptr::null(), c"orders".as_ptr(), &raw mut description) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "client is null");
}

#[test]
fn describing_without_a_stream_name_is_refused_by_name() {
    let client = TestClient::connect();
    let mut description = StreamDescription::with_defaults();

    // SAFETY: the client is live, a null name is allowed, and the output is a live structure.
    let status = unsafe { bitacora_stream_describe(client.handle(), std::ptr::null(), &raw mut description) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "stream_name is null");
}

#[test]
fn describing_without_a_place_for_the_description_is_refused_by_name() {
    let client = TestClient::connect();

    // SAFETY: the client is live, the name is NUL-terminated, and a null output is allowed.
    let status = unsafe { bitacora_stream_describe(client.handle(), c"orders".as_ptr(), std::ptr::null_mut()) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "description_out is null");
}

#[test]
fn describing_into_a_description_whose_size_was_not_set_is_refused_before_anything_is_asked() {
    let client = TestClient::connect();
    let mut description = StreamDescription::with_defaults();

    // SAFETY: the client is live, the name is NUL-terminated, and the output is a live structure.
    let status = unsafe { bitacora_stream_describe(client.handle(), c"orders".as_ptr(), &raw mut description) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(
        last_message(),
        "description_out.struct_size is 0: set it to the size of the structure"
    );
}

#[test]
fn truncating_without_a_client_is_refused_by_name() {
    // SAFETY: a null client is allowed, and the name is NUL-terminated.
    let status = unsafe { bitacora_stream_truncate_before(std::ptr::null(), c"orders".as_ptr(), 10) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "client is null");
}

#[test]
fn truncating_without_a_stream_name_is_refused_by_name() {
    let client = TestClient::connect();

    // SAFETY: the client is live, and a null name is allowed.
    let status = unsafe { bitacora_stream_truncate_before(client.handle(), std::ptr::null(), 10) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "stream_name is null");
}

#[test]
fn deleting_without_a_client_is_refused_by_name() {
    // SAFETY: a null client is allowed, and the name is NUL-terminated.
    let status = unsafe { bitacora_stream_delete(std::ptr::null(), c"orders".as_ptr()) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "client is null");
}

#[test]
fn deleting_without_a_stream_name_is_refused_by_name() {
    let client = TestClient::connect();

    // SAFETY: the client is live, and a null name is allowed.
    let status = unsafe { bitacora_stream_delete(client.handle(), std::ptr::null()) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "stream_name is null");
}

#[test]
fn registering_without_a_client_is_refused_by_name() {
    // SAFETY: a null client is allowed, and both names are NUL-terminated.
    let status =
        unsafe { bitacora_stream_register_materializer(std::ptr::null(), c"orders".as_ptr(), c"pages".as_ptr()) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "client is null");
}

#[test]
fn registering_without_a_stream_name_is_refused_by_name() {
    let client = TestClient::connect();

    // SAFETY: the client is live, a null name is allowed, and the materializer is NUL-terminated.
    let status = unsafe { bitacora_stream_register_materializer(client.handle(), std::ptr::null(), c"pages".as_ptr()) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "stream_name is null");
}

#[test]
fn registering_without_a_materializer_is_refused_by_name() {
    let client = TestClient::connect();

    // SAFETY: the client is live, the name is NUL-terminated, and a null materializer is allowed.
    let status =
        unsafe { bitacora_stream_register_materializer(client.handle(), c"orders".as_ptr(), std::ptr::null()) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "materializer is null");
}

#[test]
fn unregistering_without_a_client_is_refused_by_name() {
    // SAFETY: a null client is allowed, and both names are NUL-terminated.
    let status =
        unsafe { bitacora_stream_unregister_materializer(std::ptr::null(), c"orders".as_ptr(), c"pages".as_ptr()) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "client is null");
}

#[test]
fn unregistering_without_a_stream_name_is_refused_by_name() {
    let client = TestClient::connect();

    // SAFETY: the client is live, a null name is allowed, and the materializer is NUL-terminated.
    let status =
        unsafe { bitacora_stream_unregister_materializer(client.handle(), std::ptr::null(), c"pages".as_ptr()) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "stream_name is null");
}

#[test]
fn unregistering_without_a_materializer_is_refused_by_name() {
    let client = TestClient::connect();

    // SAFETY: the client is live, the name is NUL-terminated, and a null materializer is allowed.
    let status =
        unsafe { bitacora_stream_unregister_materializer(client.handle(), c"orders".as_ptr(), std::ptr::null()) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "materializer is null");
}
