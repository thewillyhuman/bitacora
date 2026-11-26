//! Where a replica keeps what in its data directory.
//!
//! # Why this file exists
//!
//! A replica's data directory is read back after every restart, by this crate and, when things go wrong,
//! by an operator. Its layout is therefore a format like any other (`docs/protocol/disk-format.md`), and
//! the code that writes a file and the code that finds it again must agree on its name. Keeping every name
//! in one file is what makes them agree: a copy stores its records under the path this file gives, and a
//! starting replica recognizes that path with the function next to it.
//!
//! # What it contains
//!
//! - [`REPLICA_ID_PATH`]: the file that holds the replica's identity.
//! - [`segment_file_path`], [`segment_state_path`], [`segment_repair_path`]: the three files a segment
//!   copy can have.
//! - [`stored_segment_addresses`]: which segments a listing of the directory holds something for.
//! - [`is_repair_scratch_path`]: whether a path is the leftover of an interrupted repair.
//!
//! # How it works
//!
//! ```text
//! replica.id                          the replica's identity, as text
//! <stream id>/<segment id>.seg        the records of one segment copy
//! <stream id>/<segment id>.state      its fenced epoch and sealed end
//! <stream id>/<segment id>.seg.repair a new copy being built by a repair
//! ```
//!
//! Identifiers are decimal numbers without padding. A path is recognized as belonging to a segment only if
//! it has exactly this shape; anything else in the directory is left alone.
//!
//! # Invariants
//!
//! - Everything here is a pure function over text: no disk is touched.
//! - A path this file builds is always recognized by this file. `paths_test.rs` holds the two to each
//!   other.
//! - The scratch file of a repair is never recognized as a segment's records: it ends in `.repair`, not in
//!   `.seg`.
//!
//! # See also
//!
//! - `copy.rs`: the copy that reads and writes these files.
//! - `replica.rs`: the replica that lists the directory when it opens.
//! - `docs/protocol/disk-format.md`: the layout, for other programs and for people.

use std::collections::BTreeSet;

use bitacora_types::{SegmentAddress, SegmentId, StreamId};

/// The file that ties a data directory to its replica identity. It is written once, when a replica first
/// opens an empty directory, and never changes.
pub(crate) const REPLICA_ID_PATH: &str = "replica.id";

/// The suffix of the file that holds a segment copy's records.
const SEGMENT_FILE_SUFFIX: &str = ".seg";

/// The suffix of the file that holds a segment copy's fenced epoch and sealed end.
const SEGMENT_STATE_SUFFIX: &str = ".state";

/// The suffix a repair adds to a segment file's path for the new copy it builds beside the old one.
const REPAIR_SUFFIX: &str = ".repair";

/// Builds the path of the file that holds a segment copy's records.
///
/// # Arguments
///
/// * `address` - The segment the copy belongs to.
///
/// # Returns
///
/// The path relative to the data directory, for example `7/3.seg` for segment 3 of stream 7.
pub(crate) fn segment_file_path(address: SegmentAddress) -> String {
    format!("{}/{}{SEGMENT_FILE_SUFFIX}", address.stream_id, address.segment_id)
}

/// Builds the path of the file that holds a segment copy's fencing and sealing state.
///
/// # Arguments
///
/// * `address` - The segment the copy belongs to.
///
/// # Returns
///
/// The path relative to the data directory, for example `7/3.state` for segment 3 of stream 7.
pub(crate) fn segment_state_path(address: SegmentAddress) -> String {
    format!("{}/{}{SEGMENT_STATE_SUFFIX}", address.stream_id, address.segment_id)
}

/// Builds the path where a repair builds the replacement of a segment copy before it takes the copy's
/// place.
///
/// A file left there by an interrupted repair holds nothing that is not elsewhere, and is removed when
/// the replica starts.
///
/// # Arguments
///
/// * `address` - The segment the copy belongs to.
///
/// # Returns
///
/// The path relative to the data directory, for example `7/3.seg.repair` for segment 3 of stream 7.
pub(crate) fn segment_repair_path(address: SegmentAddress) -> String {
    format!("{}{REPAIR_SUFFIX}", segment_file_path(address))
}

/// Tells whether a path of the data directory is the scratch file of a repair.
///
/// # Arguments
///
/// * `path` - A path as the disk lists it, relative to the data directory.
///
/// # Returns
///
/// `true` if a repair built a new copy at this path and never moved it into place.
pub(crate) fn is_repair_scratch_path(path: &str) -> bool {
    path.ends_with(REPAIR_SUFFIX)
}

/// Finds the segments that a listing of the data directory holds something for.
///
/// A segment is found if the listing has its records file, its state file, or both.
///
/// # Arguments
///
/// * `stored_paths` - Every path of the data directory, as the disk lists them.
///
/// # Returns
///
/// The address of each such segment, once, in ascending order of stream and segment. Paths that are not
/// a segment's records or state are ignored.
pub(crate) fn stored_segment_addresses(stored_paths: &[String]) -> Vec<SegmentAddress> {
    let mut addresses = BTreeSet::new();
    for path in stored_paths {
        if let Some(address) = parse_segment_path(path) {
            addresses.insert(address);
        }
    }
    addresses.into_iter().collect()
}

/// Recognizes the path of a segment's records file or state file.
///
/// # Arguments
///
/// * `path` - A path as the disk lists it, relative to the data directory.
///
/// # Returns
///
/// The segment the file belongs to, or `None` if the path is not `<stream id>/<segment id>.seg` or
/// `<stream id>/<segment id>.state` with two decimal numbers.
fn parse_segment_path(path: &str) -> Option<SegmentAddress> {
    let (stream_directory, file_name) = path.split_once('/')?;
    let segment_number = match file_name.strip_suffix(SEGMENT_FILE_SUFFIX) {
        Some(segment_number) => segment_number,
        None => file_name.strip_suffix(SEGMENT_STATE_SUFFIX)?,
    };
    let stream_id = stream_directory.parse().ok()?;
    let segment_id = segment_number.parse().ok()?;
    Some(SegmentAddress::new(
        StreamId::new(stream_id),
        SegmentId::new(segment_id),
    ))
}

#[cfg(test)]
#[path = "paths_test.rs"]
mod tests;
