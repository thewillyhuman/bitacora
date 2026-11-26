//! Tests of the one decision the reader's handles make themselves.
//!
//! # What is covered
//!
//! - Which errors end a followed stream: truncation of the follower's position and deletion of its stream. Every other
//!   error is trouble that can pass, which a follower retries.
//!
//! Range reads and followers as a whole are covered end to end in `tests/reader_test.rs` and
//! `tests/tiered_read_test.rs`; fetching in `reader/cursor_test.rs` and `reader/answer_test.rs`.
//!
//! # How the fixtures work
//!
//! The function under test is pure. The tests build one error each and ask whether it ends the stream.

use bitacora_meta::StoreUnavailable;
use bitacora_types::{SegmentAddress, SegmentId, StreamId};

use super::*;

#[test]
fn truncation_of_the_position_ends_a_followed_stream() {
    let truncated = ReadError::Truncated {
        low_water_mark: SequenceNumber::new(4_000),
    };

    assert!(ends_a_followed_stream(&truncated));
}

#[test]
fn deletion_of_the_stream_ends_a_followed_stream() {
    let deleted = ReadError::Metadata(MetaError::StreamNotFound {
        name: "orders".to_owned(),
    });

    assert!(ends_a_followed_stream(&deleted));
}

#[test]
fn replicas_that_cannot_serve_do_not_end_a_followed_stream() {
    let unavailable = ReadError::Unavailable {
        segment: SegmentAddress::new(StreamId::new(7), SegmentId::new(3)),
        reason: "request timed out".to_owned(),
    };

    assert!(!ends_a_followed_stream(&unavailable));
}

#[test]
fn metadata_store_outage_does_not_end_a_followed_stream() {
    let outage = ReadError::Metadata(MetaError::Unavailable(StoreUnavailable {
        reason: "connection refused".to_owned(),
    }));

    assert!(!ends_a_followed_stream(&outage));
}
