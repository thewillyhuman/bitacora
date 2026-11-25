//! The artifact store interface (design, section 11, "Artifact store").

use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

/// Why an artifact store operation failed.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ArtifactError {
    /// No object has this key.
    #[error("object {object_key:?} does not exist")]
    NotFound {
        /// The key that was looked up.
        object_key: String,
    },
    /// The requested bytes lie outside the object.
    #[error("range {start}..{end} is outside object {object_key:?} of {size_bytes} bytes")]
    RangeOutOfBounds {
        /// The object that was read.
        object_key: String,
        /// The first byte asked for.
        start: u64,
        /// One past the last byte asked for.
        end: u64,
        /// How large the object is.
        size_bytes: u64,
    },
    /// The key cannot be used: it is empty, absolute, or has a component
    /// that would escape the store.
    #[error("object key {object_key:?} is not valid")]
    InvalidKey {
        /// The rejected key.
        object_key: String,
    },
    /// The store could not be reached or failed. The operation may or may
    /// not have taken effect; every operation is safe to repeat.
    #[error("artifact store is unavailable: {reason}")]
    Unavailable {
        /// What went wrong.
        reason: String,
    },
}

/// What a listing says about one object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectInfo {
    /// The object's key.
    pub object_key: String,
    /// The object's size.
    pub size_bytes: u64,
}

/// Immutable objects addressed by key: where tiered log segments and
/// materializer artifacts live.
///
/// Keys are `/`-separated paths. An object is written whole and never
/// modified; writing an existing key replaces it atomically, so a reader
/// sees either the old object or the new one.
#[async_trait]
pub trait ArtifactStore: Send + Sync {
    /// Stores `contents` under `object_key`, durably, replacing any object
    /// already there.
    async fn put_object(&self, object_key: &str, contents: Bytes) -> Result<(), ArtifactError>;

    /// Reads the whole object.
    async fn get_object(&self, object_key: &str) -> Result<Bytes, ArtifactError>;

    /// Reads `byte_range` of the object. The range must lie inside it.
    async fn get_range(
        &self,
        object_key: &str,
        byte_range: Range<u64>,
    ) -> Result<Bytes, ArtifactError>;

    /// Deletes the object. Deleting an absent object succeeds.
    async fn delete_object(&self, object_key: &str) -> Result<(), ArtifactError>;

    /// Lists every object whose key starts with `key_prefix`, sorted by key.
    async fn list_prefix(&self, key_prefix: &str) -> Result<Vec<ObjectInfo>, ArtifactError>;
}

/// Checks that a key is a plain relative path, so that no backend ever has
/// to wonder where it points.
///
/// # Errors
///
/// Returns [`ArtifactError::InvalidKey`] if the key is empty, starts or ends
/// with `/`, or has an empty, `.` or `..` component.
pub fn validate_key(object_key: &str) -> Result<(), ArtifactError> {
    let is_valid = !object_key.is_empty()
        && object_key
            .split('/')
            .all(|component| !component.is_empty() && component != "." && component != "..");
    if is_valid {
        Ok(())
    } else {
        Err(ArtifactError::InvalidKey {
            object_key: object_key.to_owned(),
        })
    }
}

/// Cuts `byte_range` out of an object's contents, as every backend that
/// holds whole objects does.
pub(crate) fn slice_range(
    object_key: &str,
    contents: &Bytes,
    byte_range: &Range<u64>,
) -> Result<Bytes, ArtifactError> {
    let size_bytes = u64::try_from(contents.len()).expect("lengths fit in 64 bits");
    if byte_range.start > byte_range.end || byte_range.end > size_bytes {
        return Err(ArtifactError::RangeOutOfBounds {
            object_key: object_key.to_owned(),
            start: byte_range.start,
            end: byte_range.end,
            size_bytes,
        });
    }
    let start = usize::try_from(byte_range.start).expect("checked against the length");
    let end = usize::try_from(byte_range.end).expect("checked against the length");
    Ok(contents.slice(start..end))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_relative_paths_are_valid_keys() {
        for object_key in ["a", "streams/7/segments/3.seg", "a.b/c-d_e"] {
            assert_eq!(validate_key(object_key), Ok(()), "{object_key:?}");
        }
    }

    #[test]
    fn keys_that_could_point_elsewhere_are_refused() {
        for object_key in ["", "/a", "a/", "a//b", "a/./b", "a/../b", ".."] {
            assert!(
                matches!(
                    validate_key(object_key),
                    Err(ArtifactError::InvalidKey { .. })
                ),
                "{object_key:?} was accepted"
            );
        }
    }

    #[test]
    fn range_inside_an_object_is_cut_out() {
        let contents = Bytes::from_static(b"0123456789");
        assert_eq!(
            slice_range("k", &contents, &(2..5)),
            Ok(Bytes::from_static(b"234"))
        );
    }

    #[test]
    fn empty_range_at_the_end_of_an_object_is_empty() {
        let contents = Bytes::from_static(b"0123456789");
        assert_eq!(slice_range("k", &contents, &(10..10)), Ok(Bytes::new()));
    }

    #[test]
    fn range_past_the_end_of_an_object_is_refused() {
        let contents = Bytes::from_static(b"0123456789");
        assert_eq!(
            slice_range("k", &contents, &(8..11)),
            Err(ArtifactError::RangeOutOfBounds {
                object_key: "k".to_owned(),
                start: 8,
                end: 11,
                size_bytes: 10,
            })
        );
    }

    #[test]
    fn inverted_range_is_refused() {
        let contents = Bytes::from_static(b"0123456789");
        let inverted = Range { start: 5, end: 2 };
        assert!(matches!(
            slice_range("k", &contents, &inverted),
            Err(ArtifactError::RangeOutOfBounds { .. })
        ));
    }
}
