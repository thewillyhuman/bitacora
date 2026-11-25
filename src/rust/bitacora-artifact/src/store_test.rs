//! Tests of the key rule and of cutting a range out of an object.
//!
//! # What is covered
//!
//! - [`validate_key`]: plain relative paths are accepted, and every way of pointing somewhere else is
//!   refused.
//! - `slice_range`: a range inside the object is cut out exactly, an empty range at the end is allowed,
//!   and a range past the end or running backwards is refused with the object's real size.
//!
//! The operations of [`ArtifactStore`] themselves are tested per backend, through the shared check in
//! `contract.rs`.
//!
//! # How the fixtures work
//!
//! `ten_digits` is an object whose byte at offset `n` is the digit `n`, so a cut range can be read off
//! the expected text.

use super::*;

/// The key the range tests name in their errors.
const OBJECT_KEY: &str = "layers/0001";

/// An object of ten bytes whose byte at offset `n` is the ASCII digit `n`.
fn ten_digits() -> Bytes {
    Bytes::from_static(b"0123456789")
}

#[test]
fn plain_relative_paths_are_valid_keys() {
    for object_key in ["a", "streams/7/segments/3.seg", "a.b/c-d_e"] {
        let validated = validate_key(object_key);

        assert_eq!(validated, Ok(()), "{object_key:?}");
    }
}

#[test]
fn keys_that_could_point_elsewhere_are_refused() {
    for object_key in ["", "/a", "a/", "a//b", "a/./b", "a/../b", ".."] {
        let validated = validate_key(object_key);

        assert!(
            matches!(validated, Err(ArtifactError::InvalidKey { .. })),
            "{object_key:?} was accepted"
        );
    }
}

#[test]
fn refused_key_is_named_in_the_error() {
    let validated = validate_key("a/../b");

    assert_eq!(
        validated,
        Err(ArtifactError::InvalidKey {
            object_key: "a/../b".to_owned()
        })
    );
}

#[test]
fn range_inside_an_object_is_cut_out() {
    let cut = slice_range(OBJECT_KEY, &ten_digits(), &(2..5));

    assert_eq!(cut, Ok(Bytes::from_static(b"234")));
}

#[test]
fn empty_range_at_the_end_of_an_object_is_empty() {
    let cut = slice_range(OBJECT_KEY, &ten_digits(), &(10..10));

    assert_eq!(cut, Ok(Bytes::new()));
}

#[test]
fn range_past_the_end_of_an_object_is_refused() {
    let cut = slice_range(OBJECT_KEY, &ten_digits(), &(8..11));

    assert_eq!(
        cut,
        Err(ArtifactError::RangeOutOfBounds {
            object_key: OBJECT_KEY.to_owned(),
            start: 8,
            end: 11,
            size_bytes: 10,
        })
    );
}

#[test]
fn inverted_range_is_refused() {
    let range_that_ends_before_it_starts = Range { start: 5, end: 2 };

    let cut = slice_range(OBJECT_KEY, &ten_digits(), &range_that_ends_before_it_starts);

    assert!(matches!(cut, Err(ArtifactError::RangeOutOfBounds { .. })));
}
