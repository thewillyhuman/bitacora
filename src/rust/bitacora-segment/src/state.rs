//! The sidecar that holds a segment copy's fencing and sealing state
//! (design, section 7, "Per-segment state").
//!
//! It lives next to the segment file, in `<segment_id>.state`, and is always
//! replaced atomically, so it is either the old state or the new one.
//!
//! ```text
//! magic 8 | format version u32 | fenced epoch u64 | sealed flag u8
//! | sealed end u64 | crc32c u32
//! ```

use bitacora_types::{Epoch, SequenceNumber};
use bytes::BufMut;
use thiserror::Error;

/// The first bytes of every segment state file.
pub const STATE_MAGIC: [u8; 8] = *b"BITASTA\0";

/// The state format this build writes.
pub const STATE_FORMAT_VERSION: u32 = 1;

/// The size of an encoded [`SegmentState`].
pub const STATE_BYTES: usize = 33;

const STATE_CHECKSUM_OFFSET: usize = 29;

/// Why bytes are not a valid segment state.
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentState {
    /// Requests from a writer below this epoch are refused.
    pub fenced_epoch: Epoch,
    /// The segment's final sequence number, once it is sealed.
    pub sealed_end: Option<SequenceNumber>,
}

impl Default for SegmentState {
    /// The state of a copy no writer has fenced or sealed yet.
    fn default() -> Self {
        Self {
            fenced_epoch: Epoch::ZERO,
            sealed_end: None,
        }
    }
}

impl SegmentState {
    /// Serializes the state, checksum included.
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

    /// Parses and verifies a state file's whole content.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] if the bytes have the wrong size, are not a
    /// state file, come from a newer format, or fail their checksum.
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
        let found_version = u32::from_le_bytes(array_at(encoded, 8));
        if found_version > STATE_FORMAT_VERSION {
            return Err(StateError::UnsupportedVersion { found_version });
        }
        let is_sealed = encoded[20] != 0;
        let sealed_end = SequenceNumber::new(u64::from_le_bytes(array_at(encoded, 21)));
        Ok(Self {
            fenced_epoch: Epoch::new(u64::from_le_bytes(array_at(encoded, 12))),
            sealed_end: is_sealed.then_some(sealed_end),
        })
    }
}

/// Copies `LENGTH` bytes out of `source`, starting at `offset`.
fn array_at<const LENGTH: usize>(source: &[u8], offset: usize) -> [u8; LENGTH] {
    let mut copied = [0_u8; LENGTH];
    copied.copy_from_slice(&source[offset..offset + LENGTH]);
    copied
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sealed_state() -> SegmentState {
        SegmentState {
            fenced_epoch: Epoch::new(6),
            sealed_end: Some(SequenceNumber::new(4_096)),
        }
    }

    #[test]
    fn sealed_state_survives_a_round_trip() {
        assert_eq!(SegmentState::decode(&sealed_state().encode()), Ok(sealed_state()));
    }

    #[test]
    fn open_state_survives_a_round_trip() {
        let open_state = SegmentState {
            fenced_epoch: Epoch::new(6),
            sealed_end: None,
        };
        assert_eq!(SegmentState::decode(&open_state.encode()), Ok(open_state));
    }

    #[test]
    fn segment_sealed_empty_is_not_confused_with_an_open_one() {
        let sealed_empty = SegmentState {
            fenced_epoch: Epoch::new(6),
            sealed_end: Some(SequenceNumber::ZERO),
        };
        assert_eq!(SegmentState::decode(&sealed_empty.encode()), Ok(sealed_empty));
    }

    #[test]
    fn fresh_copy_is_neither_fenced_nor_sealed() {
        assert_eq!(
            SegmentState::default(),
            SegmentState {
                fenced_epoch: Epoch::ZERO,
                sealed_end: None,
            }
        );
    }

    #[test]
    fn state_of_the_wrong_size_is_refused() {
        let encoded = sealed_state().encode();
        assert_eq!(
            SegmentState::decode(&encoded[..STATE_BYTES - 1]),
            Err(StateError::WrongLength {
                found_bytes: STATE_BYTES - 1
            })
        );
    }

    #[test]
    fn state_with_wrong_magic_is_refused() {
        let mut encoded = sealed_state().encode();
        encoded[0] ^= 0xFF;
        assert_eq!(SegmentState::decode(&encoded), Err(StateError::WrongMagic));
    }

    #[test]
    fn state_with_any_flipped_field_bit_fails_its_checksum() {
        for flipped_byte in STATE_MAGIC.len()..STATE_BYTES {
            let mut encoded = sealed_state().encode();
            encoded[flipped_byte] ^= 0x01;
            assert!(
                matches!(SegmentState::decode(&encoded), Err(StateError::ChecksumMismatch { .. })),
                "flip at byte {flipped_byte} went undetected"
            );
        }
    }

    #[test]
    fn state_from_a_newer_format_is_refused() {
        let mut encoded = sealed_state().encode();
        encoded[8..12].copy_from_slice(&(STATE_FORMAT_VERSION + 1).to_le_bytes());
        let checksum = crc32c::crc32c(&encoded[..STATE_CHECKSUM_OFFSET]);
        encoded[STATE_CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_le_bytes());
        assert_eq!(
            SegmentState::decode(&encoded),
            Err(StateError::UnsupportedVersion {
                found_version: STATE_FORMAT_VERSION + 1
            })
        );
    }
}
