//! The sidecar file that holds a segment copy's fencing and sealing state.
//!
//! # Why this file exists
//!
//! Besides its records, a replica must remember two things about a segment copy across restarts (design,
//! section 7, "Per-segment state"): the lowest writer epoch it still accepts, and the segment's final
//! sequence number once it is sealed. Forgetting the first would let a writer that was fenced off commit
//! again after the replica restarts, which breaks the single-writer guarantee. Forgetting the second would
//! let the copy accept appends past the seal. Neither belongs in the segment file: that file is
//! append-only, a fence can arrive before the copy has a single record, and a replica may have to
//! remember a fence for a segment it holds no records of at all. So the two values live in a small file
//! next to the segment file. This file defines its bytes.
//!
//! # What it contains
//!
//! - [`SegmentState`]: the two values, with [`SegmentState::encode`] and [`SegmentState::decode`].
//! - [`StateError`]: why bytes are not a valid state file.
//! - [`STATE_MAGIC`], [`STATE_FORMAT_VERSION`], [`STATE_BYTES`]: the constants of the layout.
//!
//! # How it works
//!
//! The file is 33 bytes, all integers little-endian:
//!
//! ```text
//! magic 8 | format version u32 | fenced epoch u64 | sealed flag u8 | sealed end u64 | crc32c u32
//! ```
//!
//! The checksum covers everything before it. The sealed flag says whether the sealed end means anything:
//! a segment can be sealed at the empty position, so the sealed end alone cannot say "not sealed".
//!
//! The file is small enough to be written whole. The replica replaces it atomically (write a scratch file,
//! flush, rename), so after a crash it is either the old state or the new one, never a mix, and the bytes
//! here need no recovery rules of their own.
//!
//! # Invariants
//!
//! - Encoding a state and decoding the result gives the same state.
//! - Any single flipped bit after the magic is detected by the checksum.
//! - A state file of a newer format is refused, never guessed at.
//! - The layout is format version 1 and is held to golden bytes by `tests/format_golden_test.rs`.
//!
//! # See also
//!
//! - `format.rs`: the segment file this one sits next to.
//! - `disk.rs`: `write_file_atomically`, which is how the file is replaced.
//! - `docs/protocol/disk-format.md`: the same layout, for other languages.

use bitacora_types::{Epoch, SequenceNumber};
use bytes::BufMut;
use thiserror::Error;

/// The first bytes of every segment state file. They let a state file be told from any other file, and
/// from a segment file, whose magic differs.
pub const STATE_MAGIC: [u8; 8] = *b"BITASTA\0";

/// The state format this build writes.
pub const STATE_FORMAT_VERSION: u32 = 1;

/// The size of an encoded [`SegmentState`], in bytes.
pub const STATE_BYTES: usize = 33;

/// Where the format version starts in the file, right after the magic.
const FORMAT_VERSION_OFFSET: usize = 8;

/// Where the fenced epoch starts in the file.
const FENCED_EPOCH_OFFSET: usize = 12;

/// Where the sealed flag is in the file.
const SEALED_FLAG_OFFSET: usize = 20;

/// Where the sealed end starts in the file.
const SEALED_END_OFFSET: usize = 21;

/// Where the checksum starts in the file. It is the last field and covers every byte before it.
const STATE_CHECKSUM_OFFSET: usize = 29;

/// Why bytes are not a valid segment state.
///
/// # Examples
///
/// ```
/// use bitacora_segment::{SegmentState, StateError};
///
/// let refused = SegmentState::decode(b"not a state file");
///
/// assert_eq!(refused, Err(StateError::WrongLength { found_bytes: 16 }));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum StateError {
    /// The file does not have the size of a state file.
    #[error("segment state needs {STATE_BYTES} bytes, found {found_bytes}")]
    WrongLength {
        /// How many bytes were found.
        found_bytes: usize,
    },
    /// The file does not start with [`STATE_MAGIC`].
    #[error("not a segment state file: wrong magic")]
    WrongMagic,
    /// The file was written by a newer build.
    #[error("segment state version {found_version} is newer than {STATE_FORMAT_VERSION}")]
    UnsupportedVersion {
        /// The version found in the file.
        found_version: u32,
    },
    /// The bytes do not match their checksum.
    #[error("segment state checksum mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    ChecksumMismatch {
        /// The checksum stored in the file.
        stored: u32,
        /// The checksum of the bytes actually read.
        computed: u32,
    },
}

/// What a replica remembers about one segment copy besides its records.
///
/// # Examples
///
/// ```
/// use bitacora_segment::SegmentState;
/// use bitacora_types::{Epoch, SequenceNumber};
///
/// let fenced_and_sealed = SegmentState {
///     fenced_epoch: Epoch::new(6),
///     sealed_end: Some(SequenceNumber::new(104)),
/// };
///
/// let file_content = fenced_and_sealed.encode();
///
/// assert_eq!(SegmentState::decode(&file_content), Ok(fenced_and_sealed));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentState {
    /// Requests from a writer below this epoch are refused.
    pub fenced_epoch: Epoch,
    /// The segment's final sequence number, once it is sealed. `None` while the segment is open.
    pub sealed_end: Option<SequenceNumber>,
}

impl Default for SegmentState {
    /// Returns the state of a copy no writer has fenced or sealed yet.
    fn default() -> Self {
        Self {
            fenced_epoch: Epoch::ZERO,
            sealed_end: None,
        }
    }
}

impl SegmentState {
    /// Serializes the state as the whole content of a state file, checksum included.
    ///
    /// # Returns
    ///
    /// The [`STATE_BYTES`] bytes to store.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_segment::{STATE_BYTES, SegmentState};
    ///
    /// let file_content = SegmentState::default().encode();
    ///
    /// assert_eq!(file_content.len(), STATE_BYTES);
    /// assert!(file_content.starts_with(b"BITASTA\0"));
    /// ```
    #[must_use]
    pub fn encode(&self) -> [u8; STATE_BYTES] {
        let mut encoded = [0_u8; STATE_BYTES];
        let mut unwritten = &mut encoded[..];
        unwritten.put_slice(&STATE_MAGIC);
        unwritten.put_u32_le(STATE_FORMAT_VERSION);
        unwritten.put_u64_le(self.fenced_epoch.get());
        unwritten.put_u8(u8::from(self.sealed_end.is_some()));
        unwritten.put_u64_le(self.sealed_end.unwrap_or(SequenceNumber::ZERO).get());
        let checksum = crc32c::crc32c(&encoded[..STATE_CHECKSUM_OFFSET]);
        encoded[STATE_CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_le_bytes());
        encoded
    }

    /// Parses and verifies the whole content of a state file.
    ///
    /// The checks run in the order that gives the most useful error: size, magic, checksum, then version.
    /// The version is looked at only once the checksum has shown that it is what was written.
    ///
    /// # Arguments
    ///
    /// * `file_content` - Everything read from the state file.
    ///
    /// # Returns
    ///
    /// The state the file holds.
    ///
    /// # Errors
    ///
    /// * [`StateError::WrongLength`] if the content is not exactly [`STATE_BYTES`] long.
    /// * [`StateError::WrongMagic`] if it does not start with [`STATE_MAGIC`].
    /// * [`StateError::ChecksumMismatch`] if it fails its checksum.
    /// * [`StateError::UnsupportedVersion`] if it was written by a newer format.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_segment::{SegmentState, StateError};
    ///
    /// let mut file_content = SegmentState::default().encode();
    /// file_content[15] ^= 0x01;
    ///
    /// assert!(matches!(
    ///     SegmentState::decode(&file_content),
    ///     Err(StateError::ChecksumMismatch { .. })
    /// ));
    /// ```
    pub fn decode(file_content: &[u8]) -> Result<Self, StateError> {
        let Ok(encoded) = <&[u8; STATE_BYTES]>::try_from(file_content) else {
            return Err(StateError::WrongLength {
                found_bytes: file_content.len(),
            });
        };
        if encoded[..STATE_MAGIC.len()] != STATE_MAGIC {
            return Err(StateError::WrongMagic);
        }
        let stored = u32::from_le_bytes(array_at(encoded, STATE_CHECKSUM_OFFSET));
        let computed = crc32c::crc32c(&encoded[..STATE_CHECKSUM_OFFSET]);
        if stored != computed {
            return Err(StateError::ChecksumMismatch { stored, computed });
        }
        let found_version = u32::from_le_bytes(array_at(encoded, FORMAT_VERSION_OFFSET));
        if found_version > STATE_FORMAT_VERSION {
            return Err(StateError::UnsupportedVersion { found_version });
        }
        let is_sealed = encoded[SEALED_FLAG_OFFSET] != 0;
        let sealed_end = SequenceNumber::new(u64::from_le_bytes(array_at(encoded, SEALED_END_OFFSET)));
        Ok(Self {
            fenced_epoch: Epoch::new(u64::from_le_bytes(array_at(encoded, FENCED_EPOCH_OFFSET))),
            sealed_end: is_sealed.then_some(sealed_end),
        })
    }
}

/// Copies `LENGTH` bytes out of `source`, starting at `offset`.
///
/// Panics if `source` is shorter than `offset + LENGTH`; the callers pass offsets inside a state file whose
/// size was already checked.
fn array_at<const LENGTH: usize>(source: &[u8], offset: usize) -> [u8; LENGTH] {
    let mut copied = [0_u8; LENGTH];
    copied.copy_from_slice(&source[offset..offset + LENGTH]);
    copied
}

#[cfg(test)]
#[path = "state_test.rs"]
mod tests;
