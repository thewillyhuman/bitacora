//! The log records of the metrics store.
//!
//! # Why this file exists
//!
//! Every sample enters the store as part of one record of its stream: a batch of samples for one or more
//! series, appended together (design, section 19, "Prometheus metrics": a record is one batch of
//! samples). The bytes of that record outlive any one process. The materializer of next year reads the
//! records of today, and a producer in another language must be able to write them. This file is the one
//! place that defines them.
//!
//! # What it contains
//!
//! - [`encode_record`]: a batch of samples as the payload of a log record.
//! - [`decode_record`]: the batch back out of a payload.
//! - [`InvalidBatch`]: why a batch cannot be appended.
//! - [`MetricsRecordError`]: why a payload is not a metrics record.
//!
//! # How it works
//!
//! All integers are little-endian.
//!
//! ```text
//! record  format version u8 | series count u32 | series*
//! series  label count u16 | label* | sample count u32 | sample*
//! label   name length u16 | name | value length u16 | value
//! sample  timestamp i64 | value u64
//! ```
//!
//! The format version is 1. Names and values are UTF-8. The labels of a series are in ascending order of
//! name, without a name twice. A timestamp is milliseconds since the Unix epoch. A value is the bits of
//! an IEEE 754 double, so that every value, NaN payloads included, comes back as it went in. Samples are
//! stored in the order the caller gave them; they need not be ordered by time.
//!
//! There is no checksum: the log already checksums every batch of records it stores. There is no
//! compression either. A record is read once by each materializer and then lives on only in blocks,
//! which are compressed (see `chunk.rs`).
//!
//! # Invariants
//!
//! - Encoding is deterministic: equal batches encode to equal bytes.
//! - Decoding accepts exactly what encoding produces. A payload of another format version is refused,
//!   never guessed at, and a count that the payload is too short for is refused before anything is
//!   allocated for it.
//!
//! # See also
//!
//! - `sample.rs`: the samples and labels a record holds.
//! - `builder.rs`: where decoded records enter the head.
//! - `store.rs`: where records are appended.

use bitacora_types::MAX_RECORD_PAYLOAD_BYTES;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

use crate::sample::{InvalidLabels, Label, Labels, Sample, SeriesSamples};

/// The record format this code reads and writes.
const RECORD_FORMAT_VERSION: u8 = 1;

/// The stored size of one sample: a timestamp and a value of eight bytes each.
const SAMPLE_BYTES: usize = 8 + 8;

/// The stored size of the fixed start of a record: the format version and the number of series.
const RECORD_START_BYTES: usize = 1 + 4;

/// The stored size of the number of samples of one series.
const SAMPLE_COUNT_BYTES: usize = 4;

/// The stored size of the number of labels of a label set.
const LABEL_COUNT_BYTES: usize = 2;

/// The stored size of the length in front of a label's name or value.
const TEXT_LENGTH_BYTES: usize = 2;

/// Why a batch of samples cannot become a log record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum InvalidBatch {
    /// The batch holds no sample at all. Appending it would change nothing.
    #[error("the batch holds no sample")]
    NoSamples,
    /// The batch does not fit in one log record. Split it.
    #[error("the batch encodes to {encoded_bytes} bytes; one record holds {MAX_RECORD_PAYLOAD_BYTES}")]
    TooLarge {
        /// The size the batch would have as a record.
        encoded_bytes: usize,
    },
}

/// Why a log record's payload is not a metrics record.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum MetricsRecordError {
    /// The payload was written by a format this code does not know.
    #[error("record format version {found} is not supported")]
    UnsupportedVersion {
        /// The version found in the payload.
        found: u8,
    },
    /// The payload ends before the record does, or goes on after the record ended.
    #[error("record is malformed: {reason}")]
    Malformed {
        /// What is wrong with it.
        reason: &'static str,
    },
    /// The labels of a series are not a valid label set.
    #[error("record holds a series with invalid labels: {0}")]
    InvalidLabels(#[from] InvalidLabels),
}

/// Encodes a batch of samples as the payload of one log record.
///
/// # Arguments
///
/// * `batch` - The samples to append, by series. A series may appear more than once and its samples may
///   be in any order; the head sorts them out when the record is applied.
///
/// # Returns
///
/// The payload to append to the stream.
///
/// # Errors
///
/// Returns [`InvalidBatch::NoSamples`] if no series of the batch holds a sample, and
/// [`InvalidBatch::TooLarge`] if the payload would exceed what one record may carry.
///
/// # Panics
///
/// Does not panic: a batch that fits in one record has far fewer series and samples than the count
/// fields of the format can hold.
///
/// # Examples
///
/// ```
/// use bitacora_metrics::{Labels, Sample, SeriesSamples, decode_record, encode_record};
///
/// let batch = vec![SeriesSamples {
///     labels: Labels::from_pairs(&[("__name__", "requests_total")])?,
///     samples: vec![Sample { timestamp_millis: 1_000, value: 12.0 }],
/// }];
///
/// let payload = encode_record(&batch)?;
///
/// assert_eq!(decode_record(&payload)?, batch);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn encode_record(batch: &[SeriesSamples]) -> Result<Bytes, InvalidBatch> {
    let mut sample_count = 0;
    for series in batch {
        sample_count += series.samples.len();
    }
    if sample_count == 0 {
        return Err(InvalidBatch::NoSamples);
    }
    let encoded_bytes = encoded_record_bytes(batch);
    if encoded_bytes > MAX_RECORD_PAYLOAD_BYTES {
        return Err(InvalidBatch::TooLarge { encoded_bytes });
    }

    let mut payload = BytesMut::with_capacity(encoded_bytes);
    payload.put_u8(RECORD_FORMAT_VERSION);
    payload.put_u32_le(u32::try_from(batch.len()).expect("a batch that fits in a record has few series"));
    for series in batch {
        put_labels(&mut payload, &series.labels);
        payload.put_u32_le(u32::try_from(series.samples.len()).expect("a batch that fits in a record has few samples"));
        for sample in &series.samples {
            payload.put_i64_le(sample.timestamp_millis);
            payload.put_u64_le(sample.value.to_bits());
        }
    }
    Ok(payload.freeze())
}

/// How many bytes `batch` takes as a record.
fn encoded_record_bytes(batch: &[SeriesSamples]) -> usize {
    let mut encoded_bytes = RECORD_START_BYTES;
    for series in batch {
        encoded_bytes += encoded_labels_bytes(&series.labels);
        encoded_bytes += SAMPLE_COUNT_BYTES + series.samples.len() * SAMPLE_BYTES;
    }
    encoded_bytes
}

/// How many bytes a label set takes in a record or in a block: its count, then each name and value with
/// their lengths.
///
/// # Arguments
///
/// * `labels` - The label set.
///
/// # Returns
///
/// The number of bytes [`put_labels`] writes for it.
pub(crate) fn encoded_labels_bytes(labels: &Labels) -> usize {
    let mut encoded_bytes = LABEL_COUNT_BYTES;
    for label in labels.labels() {
        encoded_bytes += TEXT_LENGTH_BYTES + label.name.len() + TEXT_LENGTH_BYTES + label.value.len();
    }
    encoded_bytes
}

/// Writes a label set the way records and blocks both store it.
///
/// # Arguments
///
/// * `output` - Where the bytes are appended.
/// * `labels` - The label set to write.
///
/// # Panics
///
/// Never for a valid label set: [`Labels`] holds at most 64 labels of at most 1024 bytes each, which fit the
/// two-byte counts.
pub(crate) fn put_labels(output: &mut BytesMut, labels: &Labels) {
    output.put_u16_le(u16::try_from(labels.labels().len()).expect("a label set has at most 64 labels"));
    for label in labels.labels() {
        put_text(output, &label.name);
        put_text(output, &label.value);
    }
}

/// Writes one name or value with its length in front.
fn put_text(output: &mut BytesMut, text: &str) {
    output.put_u16_le(u16::try_from(text.len()).expect("a label text is at most 1024 bytes"));
    output.put_slice(text.as_bytes());
}

/// Why stored labels could not be read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LabelsReadError {
    /// The bytes end before the labels do, or a text is not UTF-8.
    Malformed(&'static str),
    /// The labels were read but are not a valid label set.
    Invalid(InvalidLabels),
}

/// Reads a label set written by [`put_labels`] and moves `unread` past it.
///
/// # Arguments
///
/// * `unread` - Bytes that start with a stored label set. They are consumed up to its end.
///
/// # Returns
///
/// The label set.
///
/// # Errors
///
/// Returns [`LabelsReadError`] if the bytes are cut short, a text is not UTF-8, or the labels are not a
/// valid, sorted label set.
pub(crate) fn take_labels(unread: &mut Bytes) -> Result<Labels, LabelsReadError> {
    let Ok(label_count) = unread.try_get_u16_le() else {
        return Err(LabelsReadError::Malformed("it ends inside a label count"));
    };
    let mut labels = Vec::new();
    for _label in 0..label_count {
        let name = take_text(unread)?;
        let value = take_text(unread)?;
        labels.push(Label { name, value });
    }
    Labels::from_sorted_labels(labels).map_err(LabelsReadError::Invalid)
}

/// Reads one name or value written by [`put_text`].
fn take_text(unread: &mut Bytes) -> Result<String, LabelsReadError> {
    let Ok(text_bytes) = unread.try_get_u16_le() else {
        return Err(LabelsReadError::Malformed("it ends inside a label length"));
    };
    let text_bytes = usize::from(text_bytes);
    if unread.remaining() < text_bytes {
        return Err(LabelsReadError::Malformed("it ends inside a label"));
    }
    match String::from_utf8(unread.split_to(text_bytes).to_vec()) {
        Ok(text) => Ok(text),
        Err(_not_utf8) => Err(LabelsReadError::Malformed("a label is not UTF-8")),
    }
}

/// Decodes the payload of a log record into the batch of samples it carries.
///
/// One vector of samples is allocated per series, with the exact size the record states. Nothing is
/// allocated per sample.
///
/// # Arguments
///
/// * `payload` - The payload of one record of a metrics stream.
///
/// # Returns
///
/// The series of the record with their samples, in the order they were appended.
///
/// # Errors
///
/// Returns [`MetricsRecordError`] if the payload comes from another format version, is cut short, holds
/// more than the record, or names a series with invalid labels.
///
/// # Examples
///
/// ```
/// use bitacora_metrics::{MetricsRecordError, decode_record};
/// use bytes::Bytes;
///
/// let from_the_future = Bytes::from_static(&[9, 0, 0, 0, 0]);
///
/// assert_eq!(
///     decode_record(&from_the_future),
///     Err(MetricsRecordError::UnsupportedVersion { found: 9 })
/// );
/// ```
pub fn decode_record(payload: &Bytes) -> Result<Vec<SeriesSamples>, MetricsRecordError> {
    let malformed = |reason| MetricsRecordError::Malformed { reason };
    let mut unread = payload.clone();
    let Ok(format_version) = unread.try_get_u8() else {
        return Err(malformed("it is empty"));
    };
    if format_version != RECORD_FORMAT_VERSION {
        return Err(MetricsRecordError::UnsupportedVersion { found: format_version });
    }
    let Ok(series_count) = unread.try_get_u32_le() else {
        return Err(malformed("it ends inside the series count"));
    };
    let mut batch = Vec::new();
    for _series in 0..series_count {
        let labels = match take_labels(&mut unread) {
            Ok(labels) => labels,
            Err(LabelsReadError::Malformed(reason)) => return Err(malformed(reason)),
            Err(LabelsReadError::Invalid(invalid_labels)) => return Err(invalid_labels.into()),
        };
        let samples = take_samples(&mut unread).map_err(malformed)?;
        batch.push(SeriesSamples { labels, samples });
    }
    if unread.has_remaining() {
        return Err(malformed("it holds more than its counts say"));
    }
    Ok(batch)
}

/// Reads the sample count and the samples of one series of a record.
fn take_samples(unread: &mut Bytes) -> Result<Vec<Sample>, &'static str> {
    let Ok(sample_count) = unread.try_get_u32_le() else {
        return Err("it ends inside a sample count");
    };
    let sample_count = sample_count as usize;
    // Checked before allocating: a damaged count must not ask for gigabytes.
    if unread.remaining() / SAMPLE_BYTES < sample_count {
        return Err("it ends inside the samples");
    }
    let mut samples = Vec::with_capacity(sample_count);
    for _sample in 0..sample_count {
        samples.push(Sample {
            timestamp_millis: unread.get_i64_le(),
            value: f64::from_bits(unread.get_u64_le()),
        });
    }
    Ok(samples)
}

#[cfg(test)]
#[path = "record_test.rs"]
mod tests;
