//! Tests of the segment state file.
//!
//! # What is covered
//!
//! - A state survives encoding and decoding: sealed, open, and sealed at the empty position.
//! - Every way a file is refused: wrong size, wrong magic, any flipped bit, a newer format.
//!
//! The exact bytes of the layout are held by `tests/format_golden_test.rs`.
//!
//! # How the fixtures work
//!
//! `sealed_state` is a copy fenced at epoch 6 and sealed at record 4096. Tests that damage a file start
//! from its encoding.

use super::*;

/// A copy fenced at epoch 6 and sealed at record 4096.
fn sealed_state() -> SegmentState {
    SegmentState {
        fenced_epoch: Epoch::new(6),
        sealed_end: Some(SequenceNumber::new(4_096)),
    }
}

#[test]
fn sealed_state_survives_a_round_trip() {
    let decoded = SegmentState::decode(&sealed_state().encode());

    assert_eq!(decoded, Ok(sealed_state()));
}

#[test]
fn open_state_survives_a_round_trip() {
    let open_state = SegmentState {
        fenced_epoch: Epoch::new(6),
        sealed_end: None,
    };

    let decoded = SegmentState::decode(&open_state.encode());

    assert_eq!(decoded, Ok(open_state));
}

#[test]
fn segment_sealed_empty_is_not_confused_with_an_open_one() {
    let sealed_empty = SegmentState {
        fenced_epoch: Epoch::new(6),
        sealed_end: Some(SequenceNumber::ZERO),
    };

    let decoded = SegmentState::decode(&sealed_empty.encode());

    assert_eq!(decoded, Ok(sealed_empty));
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

    let decoded = SegmentState::decode(&encoded[..STATE_BYTES - 1]);

    assert_eq!(
        decoded,
        Err(StateError::WrongLength {
            found_bytes: STATE_BYTES - 1
        })
    );
}

#[test]
fn state_followed_by_extra_bytes_is_refused() {
    let mut too_long = sealed_state().encode().to_vec();
    too_long.push(0);

    let decoded = SegmentState::decode(&too_long);

    assert_eq!(
        decoded,
        Err(StateError::WrongLength {
            found_bytes: STATE_BYTES + 1
        })
    );
}

#[test]
fn state_with_wrong_magic_is_refused() {
    let mut encoded = sealed_state().encode();
    encoded[0] ^= 0xFF;

    let decoded = SegmentState::decode(&encoded);

    assert_eq!(decoded, Err(StateError::WrongMagic));
}

#[test]
fn state_with_any_flipped_field_bit_fails_its_checksum() {
    for flipped_byte in STATE_MAGIC.len()..STATE_BYTES {
        let mut encoded = sealed_state().encode();
        encoded[flipped_byte] ^= 0x01;

        let decoded = SegmentState::decode(&encoded);

        assert!(
            matches!(decoded, Err(StateError::ChecksumMismatch { .. })),
            "flip at byte {flipped_byte} went undetected"
        );
    }
}

#[test]
fn state_from_a_newer_format_is_refused() {
    let mut encoded = sealed_state().encode();
    encoded[FORMAT_VERSION_OFFSET..FENCED_EPOCH_OFFSET].copy_from_slice(&(STATE_FORMAT_VERSION + 1).to_le_bytes());
    let checksum = crc32c::crc32c(&encoded[..STATE_CHECKSUM_OFFSET]);
    encoded[STATE_CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_le_bytes());

    let decoded = SegmentState::decode(&encoded);

    assert_eq!(
        decoded,
        Err(StateError::UnsupportedVersion {
            found_version: STATE_FORMAT_VERSION + 1
        })
    );
}
