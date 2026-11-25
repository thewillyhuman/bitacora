//! Tests of the bucket pages of bitacora-kv.
//!
//! # What is covered
//!
//! - The hash: pinned to the standard CRC-32C check value, and always within the bucket count.
//! - Which buckets a record touches, including none for a barrier.
//! - How a record changes one bucket: puts, overwrites, deletes, batches, and operations that belong to
//!   another bucket.
//! - What is refused: a payload that is not a record, and a page that is not a bucket.
//!
//! # How the fixtures work
//!
//! `keys_in_bucket` searches for keys by the bucket they hash to, so that the tests do
//! not depend on which bucket a hand-picked key happens to hash to.

use bitacora_types::SequenceNumber;

use super::*;

/// A log record with the payload of `record`.
fn log_record(record: &KvRecord) -> LogRecord {
    LogRecord {
        sequence: SequenceNumber::new(7),
        payload: record.encode(),
    }
}

/// A key or value from text.
fn bytes(text: &str) -> Bytes {
    Bytes::copy_from_slice(text.as_bytes())
}

/// The keys `key-0`, `key-1`, ... that hash to `bucket`, the first `count` of them.
fn keys_in_bucket(bucket: u32, count: usize) -> Vec<Bytes> {
    let mut keys = Vec::new();
    for key_number in 0..10_000 {
        let key = bytes(&format!("key-{key_number}"));
        if bucket_of(&key) == bucket {
            keys.push(key);
        }
        if keys.len() == count {
            break;
        }
    }
    keys
}

/// Applies `record` to the page of `bucket` and returns the pairs of the page afterwards.
fn pairs_after(bucket: u32, page_before: Option<&[u8]>, record: &KvRecord) -> BTreeMap<Bytes, Bytes> {
    let page = KvPages
        .apply_record(&bucket, page_before, &log_record(record))
        .expect("the record is valid");
    decode_bucket(&Bytes::from(page)).expect("the page is a bucket")
}

// ---- the hash ----

#[test]
fn hash_is_crc32c_of_the_key_modulo_the_bucket_count() {
    // 0xE3069283 is the check value of CRC-32C: the checksum of the ASCII digits 1 to 9.
    assert_eq!(bucket_of(b"123456789"), 0xE306_9283 % BUCKET_COUNT);
}

#[test]
fn every_key_lands_in_a_bucket_that_exists() {
    for key_number in 0..1_000 {
        let key = format!("key-{key_number}");

        assert!(bucket_of(key.as_bytes()) < BUCKET_COUNT, "{key}");
    }
}

// ---- pages touched ----

#[test]
fn put_touches_the_bucket_of_its_key() {
    let record = KvRecord::put(bytes("colour"), bytes("red"));

    let touched = KvPages.pages_touched(&log_record(&record));

    assert_eq!(touched, Ok(vec![bucket_of(b"colour")]));
}

#[test]
fn delete_touches_the_bucket_of_its_key() {
    let record = KvRecord::delete(bytes("colour"));

    let touched = KvPages.pages_touched(&log_record(&record));

    assert_eq!(touched, Ok(vec![bucket_of(b"colour")]));
}

#[test]
fn barrier_touches_no_bucket() {
    let touched = KvPages.pages_touched(&log_record(&KvRecord::barrier()));

    assert_eq!(touched, Ok(Vec::new()));
}

#[test]
fn batch_touches_each_bucket_of_its_keys_once() {
    let same_bucket = keys_in_bucket(3, 2);
    let other_bucket = keys_in_bucket(5, 1);
    let record = KvRecord {
        operations: vec![
            KvOperation::Put {
                key: same_bucket[0].clone(),
                value: bytes("a"),
            },
            KvOperation::Put {
                key: other_bucket[0].clone(),
                value: bytes("b"),
            },
            KvOperation::Delete {
                key: same_bucket[1].clone(),
            },
        ],
    };

    let touched = KvPages.pages_touched(&log_record(&record));

    assert_eq!(touched, Ok(vec![3, 5]));
}

#[test]
fn payload_that_is_not_a_record_is_refused_when_pages_are_asked_for() {
    let not_a_record = LogRecord {
        sequence: SequenceNumber::new(7),
        payload: Bytes::from_static(b"\x09something else"),
    };

    let touched = KvPages.pages_touched(&not_a_record);

    assert_eq!(
        touched,
        Err(MaterializerError::InvalidRecord {
            sequence: SequenceNumber::new(7),
            reason: "record format version 9 is not supported".to_owned(),
        })
    );
}

// ---- applying to one bucket ----

#[test]
fn put_into_a_bucket_nothing_has_touched_creates_the_page() {
    let key = keys_in_bucket(3, 1).remove(0);

    let pairs = pairs_after(3, None, &KvRecord::put(key.clone(), bytes("red")));

    assert_eq!(pairs.get(&key), Some(&bytes("red")));
    assert_eq!(pairs.len(), 1);
}

#[test]
fn put_of_an_existing_key_replaces_its_value() {
    let key = keys_in_bucket(3, 1).remove(0);
    let page_before = KvPages
        .apply_record(&3, None, &log_record(&KvRecord::put(key.clone(), bytes("red"))))
        .expect("the record is valid");

    let pairs = pairs_after(3, Some(&page_before), &KvRecord::put(key.clone(), bytes("blue")));

    assert_eq!(pairs.get(&key), Some(&bytes("blue")));
    assert_eq!(pairs.len(), 1);
}

#[test]
fn put_leaves_the_other_keys_of_the_bucket_alone() {
    let keys = keys_in_bucket(3, 2);
    let page_before = KvPages
        .apply_record(&3, None, &log_record(&KvRecord::put(keys[0].clone(), bytes("red"))))
        .expect("the record is valid");

    let pairs = pairs_after(3, Some(&page_before), &KvRecord::put(keys[1].clone(), bytes("blue")));

    assert_eq!(pairs.get(&keys[0]), Some(&bytes("red")));
    assert_eq!(pairs.get(&keys[1]), Some(&bytes("blue")));
}

#[test]
fn delete_removes_the_key() {
    let key = keys_in_bucket(3, 1).remove(0);
    let page_before = KvPages
        .apply_record(&3, None, &log_record(&KvRecord::put(key.clone(), bytes("red"))))
        .expect("the record is valid");

    let pairs = pairs_after(3, Some(&page_before), &KvRecord::delete(key));

    assert_eq!(pairs.len(), 0);
}

#[test]
fn delete_of_a_key_that_does_not_exist_changes_nothing() {
    let keys = keys_in_bucket(3, 2);
    let page_before = KvPages
        .apply_record(&3, None, &log_record(&KvRecord::put(keys[0].clone(), bytes("red"))))
        .expect("the record is valid");

    let pairs = pairs_after(3, Some(&page_before), &KvRecord::delete(keys[1].clone()));

    assert_eq!(pairs.get(&keys[0]), Some(&bytes("red")));
    assert_eq!(pairs.len(), 1);
}

#[test]
fn put_of_an_empty_value_keeps_the_key() {
    let key = keys_in_bucket(3, 1).remove(0);

    let pairs = pairs_after(3, None, &KvRecord::put(key.clone(), Bytes::new()));

    assert_eq!(pairs.get(&key), Some(&Bytes::new()));
}

#[test]
fn operations_of_a_batch_are_applied_in_order() {
    let key = keys_in_bucket(3, 1).remove(0);
    let put_then_delete_then_put = KvRecord {
        operations: vec![
            KvOperation::Put {
                key: key.clone(),
                value: bytes("first"),
            },
            KvOperation::Delete { key: key.clone() },
            KvOperation::Put {
                key: key.clone(),
                value: bytes("last"),
            },
        ],
    };

    let pairs = pairs_after(3, None, &put_then_delete_then_put);

    assert_eq!(pairs.get(&key), Some(&bytes("last")));
}

#[test]
fn operations_for_another_bucket_are_not_applied_to_this_one() {
    let key_here = keys_in_bucket(3, 1).remove(0);
    let key_elsewhere = keys_in_bucket(5, 1).remove(0);
    let record = KvRecord {
        operations: vec![
            KvOperation::Put {
                key: key_here.clone(),
                value: bytes("here"),
            },
            KvOperation::Put {
                key: key_elsewhere.clone(),
                value: bytes("elsewhere"),
            },
        ],
    };

    let pairs = pairs_after(3, None, &record);

    assert_eq!(pairs.get(&key_here), Some(&bytes("here")));
    assert_eq!(pairs.get(&key_elsewhere), None);
}

#[test]
fn pairs_of_a_page_are_sorted_by_key() {
    let keys = keys_in_bucket(3, 3);
    let mut page = None;
    for key in keys.iter().rev() {
        let next_page = KvPages
            .apply_record(
                &3,
                page.as_deref(),
                &log_record(&KvRecord::put(key.clone(), bytes("v"))),
            )
            .expect("the record is valid");
        page = Some(next_page);
    }
    let page = Bytes::from(page.expect("three records were applied"));

    let keys_in_page_order: Vec<Bytes> = decode_bucket(&page)
        .expect("the page is a bucket")
        .into_keys()
        .collect();
    let mut sorted_keys = keys.clone();
    sorted_keys.sort();

    assert_eq!(keys_in_page_order, sorted_keys);
}

#[test]
fn applying_the_same_record_to_the_same_page_gives_the_same_page() {
    let key = keys_in_bucket(3, 1).remove(0);
    let record = log_record(&KvRecord::put(key, bytes("red")));

    let first = KvPages.apply_record(&3, None, &record);
    let second = KvPages.apply_record(&3, None, &record);

    assert_eq!(first, second);
}

#[test]
fn payload_that_is_not_a_record_cannot_be_applied() {
    let not_a_record = LogRecord {
        sequence: SequenceNumber::new(7),
        payload: Bytes::from_static(b""),
    };

    let applied = KvPages.apply_record(&3, None, &not_a_record);

    assert!(matches!(applied, Err(MaterializerError::InvalidRecord { .. })));
}

#[test]
fn page_that_is_not_a_bucket_cannot_be_applied_to() {
    let key = keys_in_bucket(3, 1).remove(0);
    let record = log_record(&KvRecord::put(key, bytes("red")));

    let applied = KvPages.apply_record(&3, Some(b"\x05"), &record);

    assert_eq!(
        applied,
        Err(MaterializerError::InvalidRecord {
            sequence: SequenceNumber::new(7),
            reason: "the page is cut short".to_owned(),
        })
    );
}

// ---- reading a page ----

#[test]
fn key_is_looked_up_in_its_bucket_page() {
    let key = keys_in_bucket(3, 1).remove(0);
    let page = Bytes::from(
        KvPages
            .apply_record(&3, None, &log_record(&KvRecord::put(key.clone(), bytes("red"))))
            .expect("the record is valid"),
    );

    assert_eq!(value_in_bucket(Some(&page), &key), Ok(Some(bytes("red"))));
}

#[test]
fn key_is_absent_from_a_bucket_nothing_has_touched() {
    assert_eq!(value_in_bucket(None, b"colour"), Ok(None));
}

#[test]
fn page_with_bytes_after_its_last_pair_is_not_a_bucket() {
    let page = Bytes::from_static(&[0, 0, 0, 0, 9]);

    assert_eq!(
        decode_bucket(&page),
        Err(DamagedBucket {
            reason: "the page goes on after its last pair"
        })
    );
}

// ---- page identifiers ----

#[test]
fn page_identifier_round_trips() {
    for bucket in 0..BUCKET_COUNT {
        let encoded = KvPages::encode_page_id(&bucket);

        assert_eq!(KvPages::decode_page_id(&encoded), Some(bucket));
    }
}

#[test]
fn bytes_that_name_no_bucket_are_not_a_page_identifier() {
    let beyond_the_last_bucket = BUCKET_COUNT.to_le_bytes();

    assert_eq!(KvPages::decode_page_id(&beyond_the_last_bucket), None);
    assert_eq!(KvPages::decode_page_id(b"abc"), None);
}
