//! How bitacora-kv lays its keys out on pages.
//!
//! # Why this file exists
//!
//! bitacora-kv is built on the page adapter of the materializer framework, to keep that interface honest
//! (design, section 12). The adapter wants to know two things about a record: which pages it touches and
//! how it changes one of them. This file answers both for a key-value store, and defines what a page
//! looks like.
//!
//! # What it contains
//!
//! - [`KvPages`]: the [`PageMaterializer`] of bitacora-kv.
//! - [`bucket_of`]: which page a key lives on.
//! - [`decode_bucket`] and [`value_in_bucket`]: reading a page.
//! - [`BUCKET_COUNT`]: how many pages there are.
//!
//! # How it works
//!
//! Keys are hashed into [`BUCKET_COUNT`] buckets. A bucket is one page: the key-value pairs that hash to
//! it, sorted by key.
//!
//! ```text
//! page  pair count u32 | (key length u32 | key | value length u32 | value)*
//! ```
//!
//! A record touches the buckets of the keys of its operations. Applying it to one bucket decodes the
//! page, carries out the operations whose keys hash to that bucket, in order, and encodes the page again.
//!
//! The hash is CRC-32C of the key, modulo the bucket count. It has to be a fixed, documented function:
//! every process that ever reads the pages must put a key in the same bucket, so a hasher that may differ
//! between versions of a library or runs of a program will not do.
//!
//! # Invariants
//!
//! - [`BUCKET_COUNT`] and the hash never change for an existing store. They are constants, not settings,
//!   because changing them would silently lose every key that moves to another bucket.
//! - A page holds each key at most once, in ascending byte order.
//! - Applying is deterministic: the page after a record depends only on the page before and the record.
//! - Applying a record to a bucket decodes the whole page and encodes it again. That runs for every record of a
//!   bucket on every read that has to replay it, and it is the price of a page format this simple; a store with
//!   large buckets would keep the pairs decoded. The cost was not measured.
//!
//! # See also
//!
//! - `record.rs`: the records applied here.
//! - `store.rs`: the front end that reads keys through these pages.

use std::collections::{BTreeMap, BTreeSet};

use bitacora_materializer::{MaterializerError, PageMaterializer};
use bitacora_types::LogRecord;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::record::{KvOperation, KvRecord};

/// How many buckets, and therefore pages, a store has. A constant of the format: see the invariants of
/// this file.
pub const BUCKET_COUNT: u32 = 64;

/// The error for a page that ends before its pairs do.
const CUT_SHORT: DamagedBucket = DamagedBucket {
    reason: "the page is cut short",
};

/// The bucket a key lives in: CRC-32C of the key, modulo [`BUCKET_COUNT`].
///
/// # Arguments
///
/// * `key` - The key, as its bytes.
///
/// # Returns
///
/// The number of the bucket, from 0 to [`BUCKET_COUNT`] minus one. It is the identifier of the page the key
/// lives on, and it is the same in every process and every version.
///
/// # Examples
///
/// ```
/// use bitacora_kv::bucket_of;
///
/// // The CRC-32C of "123456789" is 0xE3069283, the standard check value.
/// assert_eq!(bucket_of(b"123456789"), 0xE306_9283 % 64);
/// ```
#[must_use]
pub fn bucket_of(key: &[u8]) -> u32 {
    crc32c::crc32c(key) % BUCKET_COUNT
}

/// Why a page is not a bucket.
///
/// # Examples
///
/// ```
/// use bitacora_kv::{DamagedBucket, decode_bucket};
/// use bytes::Bytes;
///
/// // A page that says it holds one pair and ends there.
/// let cut_short = Bytes::from_static(&[1, 0, 0, 0]);
/// assert_eq!(decode_bucket(&cut_short), Err(DamagedBucket { reason: "the page is cut short" }));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DamagedBucket {
    /// What is wrong with the page.
    pub reason: &'static str,
}

/// Reads the pairs out of a bucket page.
///
/// # Arguments
///
/// * `page` - A page as [`KvPages`] wrote it. Keys and values of the result share its memory.
///
/// # Returns
///
/// Every pair of the bucket, by key.
///
/// # Errors
///
/// Returns [`DamagedBucket`] if the page is cut short or goes on after its last pair.
///
/// # Examples
///
/// ```
/// use bitacora_kv::{KvPages, KvRecord, bucket_of, decode_bucket};
/// use bitacora_materializer::PageMaterializer;
/// use bitacora_types::{LogRecord, SequenceNumber};
/// use bytes::Bytes;
///
/// let put = LogRecord {
///     sequence: SequenceNumber::new(1),
///     payload: KvRecord::put(Bytes::from_static(b"colour"), Bytes::from_static(b"red")).encode(),
/// };
/// let page = KvPages.apply_record(&bucket_of(b"colour"), None, &put).expect("a valid record");
///
/// let pairs = decode_bucket(&Bytes::from(page)).expect("a page that was just written");
/// assert_eq!(pairs.get(&b"colour"[..]), Some(&Bytes::from_static(b"red")));
/// ```
pub fn decode_bucket(page: &Bytes) -> Result<BTreeMap<Bytes, Bytes>, DamagedBucket> {
    let mut unread = page.clone();
    let Ok(pair_count) = unread.try_get_u32_le() else {
        return Err(CUT_SHORT);
    };
    let mut pairs = BTreeMap::new();
    for _pair in 0..pair_count {
        let key = read_length_prefixed(&mut unread).ok_or(CUT_SHORT)?;
        let value = read_length_prefixed(&mut unread).ok_or(CUT_SHORT)?;
        pairs.insert(key, value);
    }
    if unread.has_remaining() {
        return Err(DamagedBucket {
            reason: "the page goes on after its last pair",
        });
    }
    Ok(pairs)
}

/// Looks one key up in a bucket page.
///
/// # Arguments
///
/// * `page` - The page of the key's bucket, or `None` if no record has touched that bucket yet.
/// * `key` - The key to look up.
///
/// # Returns
///
/// The key's value, or `None` if the bucket does not hold the key.
///
/// # Errors
///
/// Returns [`DamagedBucket`] if the page cannot be decoded.
///
/// # Examples
///
/// ```
/// use bitacora_kv::value_in_bucket;
///
/// // No record has touched the key's bucket yet, so there is no page and no value.
/// assert_eq!(value_in_bucket(None, b"colour"), Ok(None));
/// ```
pub fn value_in_bucket(page: Option<&Bytes>, key: &[u8]) -> Result<Option<Bytes>, DamagedBucket> {
    let Some(page) = page else {
        return Ok(None);
    };
    let pairs = decode_bucket(page)?;
    Ok(pairs.get(key).cloned())
}

/// Reads a length and then that many bytes, or `None` if the page ends first.
fn read_length_prefixed(unread: &mut Bytes) -> Option<Bytes> {
    let length = unread.try_get_u32_le().ok()? as usize;
    if unread.remaining() < length {
        return None;
    }
    Some(unread.split_to(length))
}

/// Writes pairs as a bucket page.
fn encode_bucket(pairs: &BTreeMap<Bytes, Bytes>) -> Vec<u8> {
    let mut page = BytesMut::new();
    page.put_u32_le(length_as_u32(pairs.len()));
    for (key, value) in pairs {
        page.put_u32_le(length_as_u32(key.len()));
        page.put_slice(key);
        page.put_u32_le(length_as_u32(value.len()));
        page.put_slice(value);
    }
    page.to_vec()
}

/// A length as the four bytes it is stored in.
fn length_as_u32(length: usize) -> u32 {
    u32::try_from(length).expect("a part of a bucket is smaller than 4 GiB")
}

/// The error for a log record that is not a bitacora-kv record.
fn invalid_record(log_record: &LogRecord, reason: &str) -> MaterializerError {
    MaterializerError::InvalidRecord {
        sequence: log_record.sequence,
        reason: reason.to_owned(),
    }
}

/// Reads the operations out of a log record, or says which record cannot be applied and why.
fn decode_record(log_record: &LogRecord) -> Result<KvRecord, MaterializerError> {
    match KvRecord::decode(&log_record.payload) {
        Ok(record) => Ok(record),
        Err(record_error) => Err(invalid_record(log_record, &record_error.to_string())),
    }
}

/// Carries out one operation on the pairs of the bucket its key hashes to.
fn apply_operation(pairs: &mut BTreeMap<Bytes, Bytes>, operation: KvOperation) {
    match operation {
        KvOperation::Put { key, value } => {
            pairs.insert(key, value);
        }
        KvOperation::Delete { key } => {
            pairs.remove(&key);
        }
    }
}

/// The page logic of bitacora-kv: buckets of sorted key-value pairs.
///
/// It has no state and no settings. The page adapter creates it with `Default`.
///
/// # Examples
///
/// ```
/// use bitacora_kv::{KvPages, KvRecord, bucket_of, value_in_bucket};
/// use bitacora_materializer::PageMaterializer;
/// use bitacora_types::{LogRecord, SequenceNumber};
/// use bytes::Bytes;
///
/// let put = LogRecord {
///     sequence: SequenceNumber::new(1),
///     payload: KvRecord::put(Bytes::from_static(b"colour"), Bytes::from_static(b"red")).encode(),
/// };
/// let bucket = bucket_of(b"colour");
/// assert_eq!(KvPages.pages_touched(&put), Ok(vec![bucket]));
///
/// let page = Bytes::from(KvPages.apply_record(&bucket, None, &put).expect("a valid record"));
/// assert_eq!(value_in_bucket(Some(&page), b"colour"), Ok(Some(Bytes::from_static(b"red"))));
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct KvPages;

impl PageMaterializer for KvPages {
    /// The number of a bucket.
    type PageId = u32;

    /// The buckets of the keys the record changes, each once, in ascending order.
    fn pages_touched(&self, log_record: &LogRecord) -> Result<Vec<u32>, MaterializerError> {
        let record = decode_record(log_record)?;
        let mut buckets = BTreeSet::new();
        for operation in &record.operations {
            buckets.insert(bucket_of(operation.key()));
        }
        Ok(buckets.into_iter().collect())
    }

    /// Carries out the record's operations on the keys of one bucket.
    fn apply_record(
        &self,
        page_id: &u32,
        page_before: Option<&[u8]>,
        log_record: &LogRecord,
    ) -> Result<Vec<u8>, MaterializerError> {
        let record = decode_record(log_record)?;
        let mut pairs = match page_before {
            Some(page) => decode_bucket(&Bytes::copy_from_slice(page))
                .map_err(|damaged| invalid_record(log_record, damaged.reason))?,
            None => BTreeMap::new(),
        };
        for operation in record.operations {
            if bucket_of(operation.key()) == *page_id {
                apply_operation(&mut pairs, operation);
            }
        }
        Ok(encode_bucket(&pairs))
    }

    /// The bucket number as four little-endian bytes.
    fn encode_page_id(page_id: &u32) -> Vec<u8> {
        page_id.to_le_bytes().to_vec()
    }

    /// The bucket number four little-endian bytes stand for, if it is a bucket of the store.
    fn decode_page_id(encoded_page_id: &[u8]) -> Option<u32> {
        let bucket = u32::from_le_bytes(encoded_page_id.try_into().ok()?);
        (bucket < BUCKET_COUNT).then_some(bucket)
    }
}

#[cfg(test)]
#[path = "pages_test.rs"]
mod tests;
