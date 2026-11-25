//! Tests of the stored form of a checkpoint manifest.
//!
//! # What is covered
//!
//! - Round trips: what is encoded decodes to the same manifest and checkpoint, including the empty one.
//! - Damage: every flipped bit and every cut is refused, never misread.
//! - Versions: a manifest of a newer format is refused.
//!
//! # How the fixtures work
//!
//! `sample_manifest` names two artifacts and carries a few bytes of details, so that every part of the
//! layout is present.

use super::*;

/// The checkpoint every sample belongs to.
const CHECKPOINT: SequenceNumber = SequenceNumber::new(1_234);

/// A manifest with every part of the layout filled in.
fn sample_manifest() -> CheckpointManifest {
    CheckpointManifest {
        artifact_keys: vec![
            "00000000000000000003/0000000000-image".to_owned(),
            "00000000000000000003/0000000001-delta".to_owned(),
        ],
        details: Bytes::from_static(b"layers: image then delta"),
    }
}

#[test]
fn stored_manifest_decodes_to_what_was_encoded() {
    let stored = encode_manifest(CHECKPOINT, &sample_manifest());

    let decoded = decode_manifest(&stored);

    assert_eq!(decoded, Ok((CHECKPOINT, sample_manifest())));
}

#[test]
fn manifest_without_artifacts_or_details_round_trips() {
    let empty_manifest = CheckpointManifest::default();
    let stored = encode_manifest(SequenceNumber::ZERO, &empty_manifest);

    let decoded = decode_manifest(&stored);

    assert_eq!(decoded, Ok((SequenceNumber::ZERO, empty_manifest)));
}

#[test]
fn every_flipped_bit_is_refused() {
    let stored = encode_manifest(CHECKPOINT, &sample_manifest());

    for damaged_offset in 0..stored.len() {
        let mut damaged = stored.to_vec();
        damaged[damaged_offset] ^= 0x10;

        let decoded = decode_manifest(&Bytes::from(damaged));

        assert!(
            matches!(decoded, Err(MaterializerError::InvalidManifest { .. })),
            "damage at byte {damaged_offset} gave {decoded:?}"
        );
    }
}

#[test]
fn every_cut_is_refused() {
    let stored = encode_manifest(CHECKPOINT, &sample_manifest());

    for kept_bytes in 0..stored.len() {
        let decoded = decode_manifest(&stored.slice(..kept_bytes));

        assert!(
            matches!(decoded, Err(MaterializerError::InvalidManifest { .. })),
            "a manifest cut to {kept_bytes} bytes gave {decoded:?}"
        );
    }
}

#[test]
fn bytes_after_the_manifest_are_refused() {
    let mut stored = encode_manifest(CHECKPOINT, &sample_manifest()).to_vec();
    stored.push(0);

    let decoded = decode_manifest(&Bytes::from(stored));

    assert!(matches!(
        decoded,
        Err(MaterializerError::InvalidManifest { .. })
    ));
}

#[test]
fn manifest_of_a_newer_format_is_refused() {
    let mut newer = encode_manifest(CHECKPOINT, &sample_manifest()).to_vec();
    newer[MANIFEST_MAGIC.len()] = 2;
    let checksummed_bytes = newer.len() - CHECKSUM_BYTES;
    let checksum = crc32c::crc32c(&newer[..checksummed_bytes]);
    newer[checksummed_bytes..].copy_from_slice(&checksum.to_le_bytes());

    let decoded = decode_manifest(&Bytes::from(newer));

    assert_eq!(
        decoded,
        Err(MaterializerError::InvalidManifest {
            reason: "it was written by a newer format".to_owned()
        })
    );
}
