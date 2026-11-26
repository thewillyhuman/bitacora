//! Tests of the replica's default settings.
//!
//! # What is covered
//!
//! The defaults that the design states as numbers, so that a change to one of them is a decision and not
//! an accident: the two disk thresholds and their order.
//!
//! # How the fixtures work
//!
//! There are none: each test looks at [`ReplicaSettings::default`].

use super::*;

#[test]
fn new_segments_are_refused_from_85_percent_of_used_disk_by_default() {
    assert_eq!(ReplicaSettings::default().refuse_new_segments_at_used_percent, 85);
}

#[test]
fn appends_are_refused_from_95_percent_of_used_disk_by_default() {
    assert_eq!(ReplicaSettings::default().refuse_appends_at_used_percent, 95);
}

/// A replica must stop taking new segments before it stops taking appends: a writer that is refused an
/// append rolls to a new segment, and that segment must not land on a disk that is about to be full.
#[test]
fn new_segments_are_refused_before_appends_are() {
    let settings = ReplicaSettings::default();

    assert!(settings.refuse_new_segments_at_used_percent < settings.refuse_appends_at_used_percent);
}

#[test]
fn repair_is_rate_limited_by_default() {
    assert!(ReplicaSettings::default().repair_bytes_per_second > 0);
}
