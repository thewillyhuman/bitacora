//! Tests of the stored form of an artifact catalog.
//!
//! # What is covered
//!
//! - What survives: entries, summaries of any length, and saved pending records with and without entries before
//!   them.
//! - What is refused: another format, a catalog cut at any byte, bytes after the end, an unclear pending flag, an
//!   artifact key that is not text, an artifact that covers no record, a gap, an overlap, and saved records that
//!   do not follow the last artifact.
//!
//! # How the fixtures work
//!
//! `entry` builds a catalog entry from the first and last record it covers, with a made-up artifact whose key and
//! summary say the same, so that a failed assertion names the entry. `refusal_of` gives the reason a stored catalog
//! is refused for.
//!
//! That a catalog written by the adapter is read back, by its next instance and by a process that only queries, is
//! tested in `artifact_catalog_test.rs`.

use super::*;

/// A catalog entry that covers `first` to `last` with a made-up artifact.
fn entry(first: u64, last: u64) -> CatalogEntry {
    CatalogEntry {
        covered_range: SequenceRange::new(SequenceNumber::new(first), SequenceNumber::new(last))
            .expect("a valid range"),
        artifact: ArtifactDescriptor {
            artifact_key: format!("00000000000000000001/0000000000-{first}-{last}"),
            size_bytes: 10,
            summary: Bytes::from(format!("records {first} to {last}")),
        },
    }
}

/// The reason `decode_catalog` gives for refusing `stored`.
fn refusal_of(stored: &Bytes) -> String {
    match decode_catalog(stored) {
        Err(MaterializerError::InvalidManifest { reason }) => reason,
        other => panic!("expected the catalog to be refused as an invalid manifest, got {other:?}"),
    }
}

#[test]
fn entries_survive_the_stored_catalog() {
    let entries = [entry(1, 5), entry(6, 9)];

    let stored = encode_catalog(&entries, None);

    let expected = StoredCatalog {
        entries: entries.to_vec(),
        pending: None,
    };
    assert_eq!(decode_catalog(&stored), Ok(expected));
}

#[test]
fn empty_catalog_survives_the_stored_catalog() {
    let stored = encode_catalog(&[], None);

    assert_eq!(decode_catalog(&stored), Ok(StoredCatalog::default()));
}

#[test]
fn summaries_of_any_length_survive_the_stored_catalog() {
    let mut without_summary = entry(1, 5);
    without_summary.artifact.summary = Bytes::new();
    let entries = [without_summary, entry(6, 9)];

    let stored = encode_catalog(&entries, None);

    let expected = StoredCatalog {
        entries: entries.to_vec(),
        pending: None,
    };
    assert_eq!(decode_catalog(&stored), Ok(expected));
}

#[test]
fn saved_pending_records_survive_the_stored_catalog() {
    let stored = encode_catalog(&[entry(1, 5)], Some(&entry(6, 9)));

    let expected = StoredCatalog {
        entries: vec![entry(1, 5)],
        pending: Some(entry(6, 9)),
    };
    assert_eq!(decode_catalog(&stored), Ok(expected));
}

#[test]
fn saved_pending_records_without_any_artifact_survive_the_stored_catalog() {
    let stored = encode_catalog(&[], Some(&entry(1, 3)));

    let expected = StoredCatalog {
        entries: Vec::new(),
        pending: Some(entry(1, 3)),
    };
    assert_eq!(decode_catalog(&stored), Ok(expected));
}

#[test]
fn stored_catalog_of_an_earlier_format_is_refused() {
    // Format version 2, which had no place for saved pending records, with no entry.
    let earlier_format = Bytes::from_static(&[2, 0, 0, 0, 0, 0, 0, 0]);

    let reason = refusal_of(&earlier_format);

    assert_eq!(
        reason,
        "the artifact catalog is not valid: it was written by another format"
    );
}

#[test]
fn stored_catalog_that_is_cut_short_is_refused() {
    let stored = encode_catalog(&[entry(1, 5), entry(6, 9)], Some(&entry(10, 12)));

    for kept_bytes in 0..stored.len() {
        let decoded = decode_catalog(&stored.slice(..kept_bytes));

        assert!(
            matches!(decoded, Err(MaterializerError::InvalidManifest { .. })),
            "a catalog cut to {kept_bytes} bytes gave {decoded:?}"
        );
    }
}

#[test]
fn stored_catalog_cut_inside_an_entry_says_that_it_is_cut_short() {
    let stored = encode_catalog(&[entry(1, 5)], None);
    let inside_the_entry = stored.slice(..stored.len() - 2);

    let reason = refusal_of(&inside_the_entry);

    assert_eq!(reason, "the artifact catalog is not valid: it is cut short");
}

#[test]
fn stored_catalog_with_bytes_after_it_is_refused() {
    let mut longer = encode_catalog(&[entry(1, 5)], None).to_vec();
    longer.push(0);

    let reason = refusal_of(&Bytes::from(longer));

    assert_eq!(
        reason,
        "the artifact catalog is not valid: it holds more than its lengths say"
    );
}

#[test]
fn stored_catalog_that_does_not_say_whether_records_were_saved_is_refused() {
    let mut unclear = encode_catalog(&[entry(1, 5)], None).to_vec();
    let last_byte = unclear.len() - 1;
    unclear[last_byte] = 7;

    let reason = refusal_of(&Bytes::from(unclear));

    assert_eq!(
        reason,
        "the artifact catalog is not valid: it does not say whether pending records were saved"
    );
}

#[test]
fn artifact_key_that_is_not_text_is_refused() {
    let mut not_text = entry(1, 5);
    not_text.artifact.summary = Bytes::new();
    let mut stored = encode_catalog(&[not_text], None).to_vec();
    // The last byte of the key: after it come the four bytes of the empty summary's length and the flag.
    let last_byte_of_the_key = stored.len() - 6;
    stored[last_byte_of_the_key] = 0xFF;

    let reason = refusal_of(&Bytes::from(stored));

    assert_eq!(reason, "the artifact catalog is not valid: an artifact key is not text");
}

#[test]
fn artifact_that_covers_no_record_is_refused() {
    let mut stored = encode_catalog(&[entry(5, 5)], None).to_vec();
    // The first sequence number of the first entry follows the format version and the entry count.
    let first_sequence_at = 8;
    stored[first_sequence_at] = 6;

    let reason = refusal_of(&Bytes::from(stored));

    assert_eq!(
        reason,
        "the artifact catalog is not valid: an artifact covers no record"
    );
}

#[test]
fn catalog_with_a_gap_is_refused() {
    let with_a_gap = encode_catalog(&[entry(1, 5), entry(7, 9)], None);

    let reason = refusal_of(&with_a_gap);

    assert_eq!(
        reason,
        "the artifact catalog is not valid: its artifacts leave a gap or overlap"
    );
}

#[test]
fn catalog_with_an_overlap_is_refused() {
    let with_an_overlap = encode_catalog(&[entry(1, 5), entry(5, 9)], None);

    let reason = refusal_of(&with_an_overlap);

    assert_eq!(
        reason,
        "the artifact catalog is not valid: its artifacts leave a gap or overlap"
    );
}

#[test]
fn saved_pending_records_that_do_not_follow_the_last_artifact_are_refused() {
    let with_a_gap = encode_catalog(&[entry(1, 5)], Some(&entry(7, 9)));

    let reason = refusal_of(&with_a_gap);

    assert_eq!(
        reason,
        "the artifact catalog is not valid: its saved records do not follow its last artifact"
    );
}
