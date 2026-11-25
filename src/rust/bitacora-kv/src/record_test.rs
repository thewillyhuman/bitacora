//! Tests of the bitacora-kv record format.
//!
//! # What is covered
//!
//! - Round trips: puts, deletes, batches, empty keys and values, and the barrier.
//! - What is refused: another format version, an unknown kind, a payload cut anywhere, trailing bytes.
//! - The bytes themselves, pinned for one record, so that the format cannot change by accident.

use super::*;

/// A key or value from text.
fn bytes(text: &'static str) -> Bytes {
    Bytes::from_static(text.as_bytes())
}

/// A record with one put and one delete.
fn sample_record() -> KvRecord {
    KvRecord {
        operations: vec![
            KvOperation::Put {
                key: bytes("colour"),
                value: bytes("red"),
            },
            KvOperation::Delete { key: bytes("shape") },
        ],
    }
}

#[test]
fn record_decodes_to_what_was_encoded() {
    let payload = sample_record().encode();

    assert_eq!(KvRecord::decode(&payload), Ok(sample_record()));
}

#[test]
fn barrier_is_a_record_without_operations() {
    let payload = KvRecord::barrier().encode();

    assert_eq!(KvRecord::decode(&payload), Ok(KvRecord::default()));
}

#[test]
fn empty_key_and_empty_value_round_trip() {
    let record = KvRecord::put(Bytes::new(), Bytes::new());

    assert_eq!(KvRecord::decode(&record.encode()), Ok(record));
}

#[test]
fn put_of_one_key_has_these_exact_bytes() {
    let payload = KvRecord::put(bytes("k"), bytes("vw")).encode();

    let expected: &[u8] = &[
        1, // format version
        1, 0, 0, 0, // one operation
        1, // put
        1, 0, 0, 0, b'k', // key
        2, 0, 0, 0, b'v', b'w', // value
    ];
    assert_eq!(&payload[..], expected);
}

#[test]
fn delete_of_one_key_has_these_exact_bytes() {
    let payload = KvRecord::delete(bytes("k")).encode();

    let expected: &[u8] = &[
        1, // format version
        1, 0, 0, 0, // one operation
        2, // delete
        1, 0, 0, 0, b'k', // key
    ];
    assert_eq!(&payload[..], expected);
}

#[test]
fn equal_records_encode_to_equal_bytes() {
    assert_eq!(sample_record().encode(), sample_record().encode());
}

#[test]
fn operation_names_the_key_it_changes() {
    let record = sample_record();

    assert_eq!(record.operations[0].key(), &bytes("colour"));
    assert_eq!(record.operations[1].key(), &bytes("shape"));
}

#[test]
fn payload_of_another_format_version_is_refused() {
    let mut payload = sample_record().encode().to_vec();
    payload[0] = 2;

    let decoded = KvRecord::decode(&Bytes::from(payload));

    assert_eq!(decoded, Err(KvRecordError::UnsupportedVersion { found: 2 }));
}

#[test]
fn empty_payload_is_refused() {
    assert_eq!(
        KvRecord::decode(&Bytes::new()),
        Err(KvRecordError::Malformed { reason: "it is empty" })
    );
}

#[test]
fn unknown_kind_of_operation_is_refused() {
    let mut payload = KvRecord::delete(bytes("k")).encode().to_vec();
    payload[5] = 9;

    let decoded = KvRecord::decode(&Bytes::from(payload));

    assert_eq!(
        decoded,
        Err(KvRecordError::Malformed {
            reason: "it holds an unknown kind of operation"
        })
    );
}

#[test]
fn payload_cut_anywhere_is_refused() {
    let payload = sample_record().encode();

    for kept_bytes in 1..payload.len() {
        let decoded = KvRecord::decode(&payload.slice(..kept_bytes));

        assert_eq!(
            decoded,
            Err(KvRecordError::Malformed {
                reason: "it is cut short"
            }),
            "a payload cut to {kept_bytes} bytes"
        );
    }
}

#[test]
fn bytes_after_the_last_operation_are_refused() {
    let mut payload = sample_record().encode().to_vec();
    payload.push(0);

    let decoded = KvRecord::decode(&Bytes::from(payload));

    assert_eq!(
        decoded,
        Err(KvRecordError::Malformed {
            reason: "it goes on after its last operation"
        })
    );
}
