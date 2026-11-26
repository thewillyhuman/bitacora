//! Tests of the layout of a replica's data directory.
//!
//! # What is covered
//!
//! - The three paths of a segment copy are the ones `docs/protocol/disk-format.md` names.
//! - A path this crate builds is recognized again, and nothing else is: not the identity file, not a
//!   scratch file, not a path with a name that is not a number.
//! - A listing of the directory gives each segment once, in order, whichever of its files are there.
//!
//! # How the fixtures work
//!
//! Everything concerns segment 3 of stream 7, whose records live in `7/3.seg`. The functions under test
//! are pure, so no disk is involved.

use super::*;

/// The segment whose paths the tests build and recognize.
const SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));

/// Turns path literals into the owned paths a disk listing consists of.
fn listing(paths: &[&str]) -> Vec<String> {
    let mut stored_paths = Vec::new();
    for path in paths {
        stored_paths.push((*path).to_owned());
    }
    stored_paths
}

#[test]
fn records_of_a_segment_live_under_its_stream_and_segment_numbers() {
    assert_eq!(segment_file_path(SEGMENT), "7/3.seg");
}

#[test]
fn state_of_a_segment_lives_next_to_its_records() {
    assert_eq!(segment_state_path(SEGMENT), "7/3.state");
}

#[test]
fn repair_builds_its_new_copy_next_to_the_records() {
    assert_eq!(segment_repair_path(SEGMENT), "7/3.seg.repair");
}

#[test]
fn records_path_names_its_segment() {
    assert_eq!(parse_segment_path(&segment_file_path(SEGMENT)), Some(SEGMENT));
}

#[test]
fn state_path_names_its_segment() {
    assert_eq!(parse_segment_path(&segment_state_path(SEGMENT)), Some(SEGMENT));
}

#[test]
fn unrelated_paths_are_not_segments() {
    for path in ["replica.id", "7/3.tmp", "x/3.seg", "7/y.seg", "7/3/4.seg"] {
        assert_eq!(parse_segment_path(path), None, "path {path:?}");
    }
}

/// A half-built copy must never be loaded as the segment's records.
#[test]
fn scratch_file_of_a_repair_is_not_a_segment() {
    assert_eq!(parse_segment_path(&segment_repair_path(SEGMENT)), None);
}

#[test]
fn scratch_file_of_a_repair_is_recognized_as_one() {
    assert!(is_repair_scratch_path(&segment_repair_path(SEGMENT)));
}

#[test]
fn records_file_is_not_a_scratch_file() {
    assert!(!is_repair_scratch_path(&segment_file_path(SEGMENT)));
}

#[test]
fn segment_with_records_and_state_is_listed_once() {
    let stored_paths = listing(&["7/3.seg", "7/3.state", "replica.id"]);

    assert_eq!(stored_segment_addresses(&stored_paths), [SEGMENT]);
}

#[test]
fn segment_with_only_its_state_is_listed() {
    let stored_paths = listing(&["7/3.state"]);

    assert_eq!(stored_segment_addresses(&stored_paths), [SEGMENT]);
}

/// A disk lists paths as text, where `10` sorts before `9`. Segments are loaded in their own order.
#[test]
fn segments_are_listed_in_the_order_of_their_numbers() {
    let stored_paths = listing(&["7/10.seg", "7/9.seg", "12/1.seg"]);
    let ninth_segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(9));
    let tenth_segment = SegmentAddress::new(StreamId::new(7), SegmentId::new(10));
    let segment_of_a_later_stream = SegmentAddress::new(StreamId::new(12), SegmentId::new(1));

    assert_eq!(
        stored_segment_addresses(&stored_paths),
        [ninth_segment, tenth_segment, segment_of_a_later_stream]
    );
}

#[test]
fn directory_without_segments_lists_none() {
    let stored_paths = listing(&["replica.id", "7/3.seg.repair"]);

    assert_eq!(stored_segment_addresses(&stored_paths), Vec::new());
}
