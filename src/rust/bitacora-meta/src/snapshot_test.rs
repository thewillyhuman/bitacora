//! Tests of the metadata snapshot format.
//!
//! # What is covered
//!
//! - Round trips: an empty store, a store with entries, values that are not text, empty keys and values.
//! - Refusals: bytes that are too short, not a snapshot, from a newer format, cut off, or extended.
//! - Damage: every single flipped bit of a snapshot is detected.
//! - Naming: keys sort by time and convert back to the time they name.
//!
//! # How the fixtures work
//!
//! `orders_snapshot` is a snapshot of two keys, one holding JSON and one holding bytes that are not
//! UTF-8, taken at a fixed moment.

use bitacora_types::DeterministicRandom;

use super::*;

/// The moment the fixture snapshots were taken at.
const TAKEN_AT: Timestamp = Timestamp::from_unix_millis(1_798_761_599_000);

/// A snapshot of a store with two keys.
fn orders_snapshot() -> MetadataSnapshot {
    MetadataSnapshot {
        taken_at: TAKEN_AT,
        entries: vec![
            SnapshotEntry {
                key: "counters/stream-id".to_owned(),
                value: Bytes::from_static(&[0x00, 0xFF, 0x10]),
            },
            SnapshotEntry {
                key: "streams/orders".to_owned(),
                value: Bytes::from_static(br#"{"name":"orders"}"#),
            },
        ],
    }
}

/// The offset of the format version within an encoded snapshot.
const VERSION_OFFSET: usize = SNAPSHOT_MAGIC.len();

#[test]
fn snapshot_with_entries_survives_the_round_trip() {
    let encoded = orders_snapshot().encode();

    assert_eq!(MetadataSnapshot::decode(&encoded), Ok(orders_snapshot()));
}

#[test]
fn snapshot_of_an_empty_store_survives_the_round_trip() {
    let empty = MetadataSnapshot {
        taken_at: TAKEN_AT,
        entries: Vec::new(),
    };

    let encoded = empty.encode();

    assert_eq!(MetadataSnapshot::decode(&encoded), Ok(empty));
}

#[test]
fn empty_key_and_empty_value_survive_the_round_trip() {
    let snapshot = MetadataSnapshot {
        taken_at: TAKEN_AT,
        entries: vec![SnapshotEntry {
            key: String::new(),
            value: Bytes::new(),
        }],
    };

    let encoded = snapshot.encode();

    assert_eq!(MetadataSnapshot::decode(&encoded), Ok(snapshot));
}

#[test]
fn entries_keep_their_order() {
    let mut reversed = orders_snapshot();
    reversed.entries.reverse();

    let decoded = MetadataSnapshot::decode(&reversed.encode()).expect("valid snapshot");

    assert_eq!(decoded.entries[0].key, "streams/orders");
}

#[test]
fn bytes_shorter_than_an_empty_snapshot_are_refused() {
    let refused = MetadataSnapshot::decode(&[0; 31]);

    assert_eq!(
        refused,
        Err(SnapshotError::TooShort {
            available_bytes: 31,
            minimum_bytes: 32
        })
    );
}

#[test]
fn object_that_is_not_a_snapshot_is_refused() {
    let refused = MetadataSnapshot::decode(&[b'x'; 200]);

    assert_eq!(refused, Err(SnapshotError::BadMagic));
}

#[test]
fn snapshot_of_a_newer_format_is_refused() {
    let mut encoded = orders_snapshot().encode().to_vec();
    encoded[VERSION_OFFSET] = 2;

    let refused = MetadataSnapshot::decode(&encoded);

    assert_eq!(refused, Err(SnapshotError::UnsupportedVersion { found: 2 }));
}

#[test]
fn snapshot_that_lost_its_last_byte_is_refused() {
    let encoded = orders_snapshot().encode();

    let refused = MetadataSnapshot::decode(&encoded[..encoded.len() - 1]);

    assert!(matches!(
        refused,
        Err(SnapshotError::ChecksumMismatch { .. })
    ));
}

#[test]
fn snapshot_with_a_byte_added_is_refused() {
    let mut encoded = orders_snapshot().encode().to_vec();
    encoded.push(0);

    let refused = MetadataSnapshot::decode(&encoded);

    assert!(matches!(
        refused,
        Err(SnapshotError::ChecksumMismatch { .. })
    ));
}

#[test]
fn every_flipped_bit_is_detected() {
    let encoded = orders_snapshot().encode();
    let mut random = DeterministicRandom::from_seed(1);

    for damaged_offset in 0..encoded.len() {
        let mut damaged = encoded.to_vec();
        damaged[damaged_offset] ^= 1 << random.index(8);
        let decoded = MetadataSnapshot::decode(&damaged);
        assert!(
            decoded.is_err(),
            "damage at byte {damaged_offset} went unnoticed"
        );
    }
}

/// Builds bytes that carry a valid checksum over content a correct writer never produces.
fn with_valid_checksum(mut content: Vec<u8>) -> Vec<u8> {
    let checksum = crc32c::crc32c(&content);
    content.extend_from_slice(&checksum.to_le_bytes());
    content
}

/// The header of a snapshot that announces `entry_count` entries.
fn header_announcing(entry_count: u64) -> Vec<u8> {
    let mut header = Vec::new();
    header.extend_from_slice(&SNAPSHOT_MAGIC);
    header.extend_from_slice(&SNAPSHOT_FORMAT_VERSION.to_le_bytes());
    header.extend_from_slice(&TAKEN_AT.unix_millis().to_le_bytes());
    header.extend_from_slice(&entry_count.to_le_bytes());
    header
}

#[test]
fn snapshot_that_announces_more_entries_than_it_holds_is_refused() {
    let encoded = with_valid_checksum(header_announcing(1));

    let refused = MetadataSnapshot::decode(&encoded);

    assert!(matches!(refused, Err(SnapshotError::Malformed { .. })));
}

#[test]
fn entry_whose_length_runs_past_the_end_is_refused() {
    let mut content = header_announcing(1);
    content.extend_from_slice(&100_u32.to_le_bytes());
    content.extend_from_slice(b"short");
    let encoded = with_valid_checksum(content);

    let refused = MetadataSnapshot::decode(&encoded);

    assert!(matches!(refused, Err(SnapshotError::Malformed { .. })));
}

#[test]
fn bytes_after_the_last_entry_are_refused() {
    let mut content = header_announcing(0);
    content.extend_from_slice(b"extra");
    let encoded = with_valid_checksum(content);

    let refused = MetadataSnapshot::decode(&encoded);

    assert_eq!(
        refused,
        Err(SnapshotError::Malformed {
            reason: "bytes follow the last entry"
        })
    );
}

#[test]
fn key_that_is_not_text_is_refused() {
    let mut content = header_announcing(1);
    content.extend_from_slice(&1_u32.to_le_bytes());
    content.push(0xFF);
    content.extend_from_slice(&0_u32.to_le_bytes());
    let encoded = with_valid_checksum(content);

    let refused = MetadataSnapshot::decode(&encoded);

    assert_eq!(
        refused,
        Err(SnapshotError::Malformed {
            reason: "a key is not UTF-8 text"
        })
    );
}

#[test]
fn object_key_names_the_time_padded_to_twenty_digits() {
    assert_eq!(
        snapshot_object_key(TAKEN_AT),
        "metadata-snapshots/00000001798761599000"
    );
}

#[test]
fn later_snapshot_sorts_after_an_earlier_one() {
    let earlier = snapshot_object_key(Timestamp::from_unix_millis(999));
    let later = snapshot_object_key(Timestamp::from_unix_millis(1_000));

    assert!(earlier < later);
}

#[test]
fn time_is_read_back_from_an_object_key() {
    assert_eq!(
        snapshot_time_of(&snapshot_object_key(TAKEN_AT)),
        Some(TAKEN_AT)
    );
}

#[test]
fn key_outside_the_snapshot_prefix_names_no_time() {
    assert_eq!(
        snapshot_time_of("streams/7/segments/00000000000000000003"),
        None
    );
}

#[test]
fn snapshot_key_without_a_padded_number_names_no_time() {
    assert_eq!(snapshot_time_of("metadata-snapshots/latest"), None);
}

#[test]
fn snapshot_key_with_too_few_digits_names_no_time() {
    assert_eq!(snapshot_time_of("metadata-snapshots/1798761599000"), None);
}
