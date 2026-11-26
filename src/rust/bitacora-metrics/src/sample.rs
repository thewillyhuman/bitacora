//! Samples, the series they belong to, and the labels that name a series.
//!
//! # Why this file exists
//!
//! Everything else in the crate moves the same three things around: a sample (a value at a moment), a
//! series (the samples of one thing being measured), and the label set that says which thing that is.
//! The log record, the in-memory head, the block file and the query result all hold them. This file
//! defines them once, with the two rules the rest relies on: a label set has exactly one form, and the
//! samples of a series are ordered by time with one sample per moment.
//!
//! # What it contains
//!
//! - [`Label`] and [`Labels`]: one name and value, and the sorted set of them that identifies a series.
//! - [`Sample`]: a millisecond timestamp and a floating-point value.
//! - [`SeriesSamples`]: the samples of one series, as appended and as returned by a query.
//! - [`TimeRange`]: the span of time a query asks for, both ends included.
//! - [`InvalidLabels`] and [`InvalidTimeRange`]: why a label set or a range was refused.
//! - `insert_sample`, `merge_later_wins` and `merge_series_later_wins`: the operations that keep samples
//!   ordered.
//!
//! # How it works
//!
//! A [`Labels`] value is always sorted by name, has no name twice, and has no empty name or value. Two
//! label sets are the same series exactly when they compare equal, so a label set can be the key of an
//! ordered map without any normalising at the point of use. An empty value is refused because it would
//! be a second way to say "this series does not have the label".
//!
//! A series holds at most one sample per timestamp. When a second sample arrives for a timestamp the
//! series already has, the later one replaces the earlier one: "later" by the order of the log, which is
//! the only order every reader agrees on. That one rule is what makes replaying records, merging blocks
//! and answering a query from several sources all give the same result.
//!
//! Samples compare by the bits of their value, not by floating-point equality, so that a NaN is equal to
//! itself and `-0.0` differs from `0.0`. A store must give back exactly what it was given.
//!
//! # Invariants
//!
//! - A [`Labels`] value is sorted by name, without duplicate names, without empty names or values.
//! - A sample vector kept by this crate is strictly increasing in timestamp.
//!
//! # See also
//!
//! - `record.rs`: samples as bytes of a log record.
//! - `chunk.rs`: samples of one series as bytes of a block.
//! - `query.rs`: selecting series by their labels.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use thiserror::Error;

/// How many labels one series may have. Far more than a real series has; the bound keeps a corrupted
/// count from asking for an absurd allocation.
pub const MAX_LABELS_PER_SERIES: usize = 64;

/// The longest name or value of a label, in bytes.
pub const MAX_LABEL_TEXT_BYTES: usize = 1024;

/// Why a set of labels cannot identify a series.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum InvalidLabels {
    /// A series needs at least one label to tell it from every other series.
    #[error("a series needs at least one label")]
    NoLabels,
    /// More labels than [`MAX_LABELS_PER_SERIES`].
    #[error("a series may have {MAX_LABELS_PER_SERIES} labels, not {label_count}")]
    TooManyLabels {
        /// How many labels were given.
        label_count: usize,
    },
    /// A label without a name.
    #[error("a label has no name")]
    EmptyName,
    /// A label without a value. A series that lacks a label simply does not list it.
    #[error("label {name:?} has no value; leave the label out instead")]
    EmptyValue {
        /// The label whose value is empty.
        name: String,
    },
    /// A name or value longer than [`MAX_LABEL_TEXT_BYTES`].
    #[error("label {name:?} has a name or value of {text_bytes} bytes; the limit is {MAX_LABEL_TEXT_BYTES}")]
    TextTooLong {
        /// The label that is too long, cut to a readable length.
        name: String,
        /// The length that was refused.
        text_bytes: usize,
    },
    /// The same name twice.
    #[error("label {name:?} is given twice")]
    DuplicateName {
        /// The name that appears twice.
        name: String,
    },
    /// Stored labels that are not in ascending order of name. Only bytes written by something else than
    /// this crate, or damaged since, can be in this state.
    #[error("stored labels are not sorted: {name:?} comes after a greater name")]
    NotSorted {
        /// The first label that is out of place.
        name: String,
    },
}

/// One name and value of a label set.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Label {
    /// The label's name, for example `job`.
    pub name: String,
    /// The label's value, for example `api`.
    pub value: String,
}

/// The label set that identifies one series: sorted by name, every name once, nothing empty.
///
/// Two series are the same series exactly when their labels are equal. Label sets are ordered (by their
/// labels, name before value), which is the order series have in a block and in a query result.
///
/// # Examples
///
/// ```
/// use bitacora_metrics::Labels;
///
/// let labels = Labels::from_pairs(&[("job", "api"), ("__name__", "requests_total")])?;
///
/// assert_eq!(labels.value_of("job"), "api");
/// assert_eq!(labels.value_of("instance"), "");
/// assert_eq!(labels.labels()[0].name, "__name__");
/// # Ok::<(), bitacora_metrics::InvalidLabels>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Labels {
    /// The labels, in ascending order of name.
    labels: Vec<Label>,
}

impl Labels {
    /// Builds the label set of a series from names and values in any order.
    ///
    /// # Arguments
    ///
    /// * `pairs` - Each label as `(name, value)`. The order does not matter.
    ///
    /// # Returns
    ///
    /// The label set, sorted by name.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidLabels`] if there is no label, there are too many, a name or value is empty or
    /// too long, or a name appears twice.
    pub fn from_pairs(pairs: &[(&str, &str)]) -> Result<Self, InvalidLabels> {
        let mut labels = Vec::with_capacity(pairs.len());
        for (name, value) in pairs {
            labels.push(Label {
                name: (*name).to_owned(),
                value: (*value).to_owned(),
            });
        }
        labels.sort();
        Self::from_sorted_labels(labels)
    }

    /// Builds a label set from labels that must already be sorted by name, as stored labels are.
    ///
    /// Stored labels are written sorted. Sorting them again on the way in would hide damage and would
    /// let two different byte strings stand for the same series.
    ///
    /// # Arguments
    ///
    /// * `labels` - The labels, in ascending order of name.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidLabels`]: the same checks as [`Labels::from_pairs`], and
    /// [`InvalidLabels::NotSorted`] if the labels are not in order.
    pub(crate) fn from_sorted_labels(labels: Vec<Label>) -> Result<Self, InvalidLabels> {
        if labels.is_empty() {
            return Err(InvalidLabels::NoLabels);
        }
        if labels.len() > MAX_LABELS_PER_SERIES {
            return Err(InvalidLabels::TooManyLabels {
                label_count: labels.len(),
            });
        }
        for (position, label) in labels.iter().enumerate() {
            check_label(label)?;
            if position == 0 {
                continue;
            }
            let previous_name = &labels[position - 1].name;
            if label.name == *previous_name {
                return Err(InvalidLabels::DuplicateName {
                    name: label.name.clone(),
                });
            }
            if label.name < *previous_name {
                return Err(InvalidLabels::NotSorted {
                    name: label.name.clone(),
                });
            }
        }
        Ok(Self { labels })
    }

    /// The labels, in ascending order of name.
    #[must_use]
    pub fn labels(&self) -> &[Label] {
        &self.labels
    }

    /// The value of the label called `name`.
    ///
    /// # Returns
    ///
    /// The value, or the empty text if the series does not have the label. Matchers treat a missing label
    /// and an empty value alike, and so does this.
    #[must_use]
    pub fn value_of(&self, name: &str) -> &str {
        for label in &self.labels {
            if label.name == name {
                return &label.value;
            }
        }
        ""
    }
}

/// Checks the name and value of one label on their own.
fn check_label(label: &Label) -> Result<(), InvalidLabels> {
    if label.name.is_empty() {
        return Err(InvalidLabels::EmptyName);
    }
    if label.value.is_empty() {
        return Err(InvalidLabels::EmptyValue {
            name: label.name.clone(),
        });
    }
    let longest_text_bytes = label.name.len().max(label.value.len());
    if longest_text_bytes > MAX_LABEL_TEXT_BYTES {
        // The name itself may be what is too long; a prefix is enough to find it.
        let readable_name: String = label.name.chars().take(64).collect();
        return Err(InvalidLabels::TextTooLong {
            name: readable_name,
            text_bytes: longest_text_bytes,
        });
    }
    Ok(())
}

/// One measurement: a value at a moment.
///
/// Two samples are equal when their timestamps are equal and their values have the same bits. A NaN is
/// therefore equal to the same NaN, which floating-point comparison would deny.
///
/// # Examples
///
/// ```
/// use bitacora_metrics::Sample;
///
/// let not_a_number = Sample { timestamp_millis: 1_000, value: f64::NAN };
/// assert_eq!(not_a_number, not_a_number);
/// ```
#[derive(Clone, Copy, Debug)]
pub struct Sample {
    /// When the value was measured, in milliseconds since the Unix epoch. May be negative.
    pub timestamp_millis: i64,
    /// The measured value. Any `f64`, NaN and the infinities included.
    pub value: f64,
}

impl PartialEq for Sample {
    /// Compares the timestamp and the bits of the value.
    fn eq(&self, other: &Self) -> bool {
        self.timestamp_millis == other.timestamp_millis && self.value.to_bits() == other.value.to_bits()
    }
}

impl Eq for Sample {}

/// The samples of one series.
///
/// This is what a caller appends and what a query returns. In a query result the samples are strictly
/// increasing in timestamp; in an append they may come in any order and may repeat a timestamp, in which
/// case the last one given wins.
///
/// # Examples
///
/// ```
/// use bitacora_metrics::{Labels, Sample, SeriesSamples};
///
/// let requests = SeriesSamples {
///     labels: Labels::from_pairs(&[("__name__", "requests_total"), ("job", "api")])?,
///     samples: vec![Sample { timestamp_millis: 1_000, value: 12.0 }],
/// };
/// assert_eq!(requests.samples.len(), 1);
/// # Ok::<(), bitacora_metrics::InvalidLabels>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeriesSamples {
    /// The series the samples belong to.
    pub labels: Labels,
    /// The samples.
    pub samples: Vec<Sample>,
}

/// The range ends before it starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
#[error("a time range cannot end at {end_millis}, before it starts at {start_millis}")]
pub struct InvalidTimeRange {
    /// The requested start.
    pub start_millis: i64,
    /// The requested end.
    pub end_millis: i64,
}

/// A span of time, both ends included, in milliseconds since the Unix epoch.
///
/// # Examples
///
/// ```
/// use bitacora_metrics::TimeRange;
///
/// let first_minute = TimeRange::new(0, 59_999)?;
///
/// assert!(first_minute.contains(59_999));
/// assert!(!first_minute.contains(60_000));
/// assert!(TimeRange::ALL.contains(i64::MIN));
/// # Ok::<(), bitacora_metrics::InvalidTimeRange>(())
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeRange {
    /// The first moment of the range.
    start_millis: i64,
    /// The last moment of the range.
    end_millis: i64,
}

impl TimeRange {
    /// Every moment a timestamp can name.
    pub const ALL: Self = Self {
        start_millis: i64::MIN,
        end_millis: i64::MAX,
    };

    /// Builds the range from `start_millis` to `end_millis`, both included.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidTimeRange`] if the range would end before it starts.
    pub const fn new(start_millis: i64, end_millis: i64) -> Result<Self, InvalidTimeRange> {
        if end_millis < start_millis {
            return Err(InvalidTimeRange {
                start_millis,
                end_millis,
            });
        }
        Ok(Self {
            start_millis,
            end_millis,
        })
    }

    /// The first moment of the range.
    #[must_use]
    pub const fn start_millis(self) -> i64 {
        self.start_millis
    }

    /// The last moment of the range.
    #[must_use]
    pub const fn end_millis(self) -> i64 {
        self.end_millis
    }

    /// Whether `timestamp_millis` lies in the range.
    #[must_use]
    pub const fn contains(self, timestamp_millis: i64) -> bool {
        self.start_millis <= timestamp_millis && timestamp_millis <= self.end_millis
    }

    /// Whether anything between `min_time_millis` and `max_time_millis`, both included, lies in the range.
    ///
    /// A block or a series knows the times of its first and last sample; this says whether it can hold
    /// anything a query for this range wants.
    #[must_use]
    pub const fn overlaps(self, min_time_millis: i64, max_time_millis: i64) -> bool {
        min_time_millis <= self.end_millis && self.start_millis <= max_time_millis
    }
}

/// Puts one sample into the ordered samples of a series. A sample for a timestamp the series already has
/// replaces the one that was there.
///
/// Appending in time order, which is what almost every sample does, is one comparison and a push. A
/// sample that arrives late costs a binary search and a shift.
///
/// # Arguments
///
/// * `samples` - The samples of one series, strictly increasing in timestamp. They stay so.
/// * `new_sample` - The sample to add. It is later in the log than everything in `samples`.
///
/// # Returns
///
/// `true` if the series gained a sample, `false` if the sample replaced one with the same timestamp.
pub(crate) fn insert_sample(samples: &mut Vec<Sample>, new_sample: Sample) -> bool {
    let is_newest = match samples.last() {
        Some(last_sample) => new_sample.timestamp_millis > last_sample.timestamp_millis,
        None => true,
    };
    if is_newest {
        samples.push(new_sample);
        return true;
    }
    let position = samples.partition_point(|sample| sample.timestamp_millis < new_sample.timestamp_millis);
    if samples[position].timestamp_millis == new_sample.timestamp_millis {
        samples[position] = new_sample;
        return false;
    }
    samples.insert(position, new_sample);
    true
}

/// Merges the samples of one series from two sources into one ordered vector. Where both have a sample
/// for the same timestamp, the one from `later` is kept.
///
/// # Arguments
///
/// * `earlier` - Samples from the source that is earlier in the log, strictly increasing in timestamp.
/// * `later` - Samples from the source that is later in the log, strictly increasing in timestamp.
///
/// # Returns
///
/// Every timestamp of either source once, strictly increasing.
pub(crate) fn merge_later_wins(earlier: &[Sample], later: &[Sample]) -> Vec<Sample> {
    let mut merged = Vec::with_capacity(earlier.len() + later.len());
    let mut earlier_position = 0;
    let mut later_position = 0;
    while earlier_position < earlier.len() && later_position < later.len() {
        let earlier_sample = earlier[earlier_position];
        let later_sample = later[later_position];
        match earlier_sample.timestamp_millis.cmp(&later_sample.timestamp_millis) {
            Ordering::Less => {
                merged.push(earlier_sample);
                earlier_position += 1;
            }
            Ordering::Greater => {
                merged.push(later_sample);
                later_position += 1;
            }
            Ordering::Equal => {
                merged.push(later_sample);
                earlier_position += 1;
                later_position += 1;
            }
        }
    }
    merged.extend_from_slice(&earlier[earlier_position..]);
    merged.extend_from_slice(&later[later_position..]);
    merged
}

/// Merges the series of a later source into what earlier sources gave, series by series.
///
/// # Arguments
///
/// * `merged` - The samples read so far, by series. Changed in place.
/// * `later_labels` - The series the samples belong to.
/// * `later_samples` - One more run of that series' samples, strictly increasing in time, from a source
///   later in the log than everything in `merged`. Where both have a sample for a timestamp, this one
///   is kept.
pub(crate) fn merge_series_later_wins(
    merged: &mut BTreeMap<Labels, Vec<Sample>>,
    later_labels: &Labels,
    later_samples: &[Sample],
) {
    if later_samples.is_empty() {
        return;
    }
    match merged.get_mut(later_labels) {
        Some(earlier_samples) => *earlier_samples = merge_later_wins(earlier_samples, later_samples),
        None => {
            merged.insert(later_labels.clone(), later_samples.to_vec());
        }
    }
}

#[cfg(test)]
#[path = "sample_test.rs"]
mod tests;
