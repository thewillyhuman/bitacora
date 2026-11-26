//! The head: the samples that are in memory and not yet in a block.
//!
//! # Why this file exists
//!
//! A sample is first a record in the log, then, once the materializer has applied the record, a sample
//! in memory, and only later part of a block in the object store. A query must see it from the moment it
//! is applied (design, section 19: "recent data is served from an in-memory head"), so the samples in
//! memory have to be readable from any task while the materializer keeps adding to them. This file holds
//! those samples, and the bookkeeping that lets a query tell which records they come from.
//!
//! # What it contains
//!
//! - `HeadSamples`: the samples of some consecutive records, by series: what one block is made from.
//! - `SharedHead`: `HeadSamples` behind a lock, written by the materializer and read by queries.
//! - `HeadSnapshot`: what one query took from the head, and which records that covers.
//!
//! # How it works
//!
//! `HeadSamples` is an ordered map from label set to the series' samples, strictly increasing in time.
//! Adding a record's samples keeps that order and lets a later sample replace an earlier one with the
//! same timestamp (see `sample.rs`). It also remembers the first and the last record it holds and the
//! span of time its samples cover, which is what decides when a block is cut.
//!
//! `SharedHead` makes that readable while it is written: the materializer takes the lock once per batch
//! of records, a query once per query. A query reads the head and the catalog of blocks at two different
//! moments. The head tells it which records its samples come from, and that is all the query needs to
//! tell whether the head adds anything to the blocks it then finds (see `query.rs`).
//!
//! Adding samples is a hot path: every sample passes through `HeadSamples::add_series` once. A sample
//! that arrives in time order costs one comparison and a push. Nothing is allocated per sample; a new
//! series costs one clone of its labels.
//!
//! # Invariants
//!
//! - The samples of every series are strictly increasing in time.
//! - `sample_count` is the number of samples held, after replacements.
//! - Records are added in log order, each exactly once, and removed only all together.
//!
//! # See also
//!
//! - `builder.rs`: the materializer side, which adds records and seals blocks.
//! - `query.rs`: the reading side.

use std::collections::BTreeMap;
use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use bitacora_types::SequenceNumber;

use crate::sample::{Labels, Sample, SeriesSamples, TimeRange, insert_sample, merge_series_later_wins};
use crate::selector::Selector;

/// The samples of some consecutive records, by series.
#[derive(Debug, Default)]
pub(crate) struct HeadSamples {
    /// Every series with at least one sample, and its samples, strictly increasing in time.
    samples_by_series: BTreeMap<Labels, Vec<Sample>>,
    /// How many samples are held, over all series.
    sample_count: u64,
    /// The time of the earliest sample held. Meaningless while `sample_count` is 0.
    min_time_millis: i64,
    /// The time of the latest sample held. Meaningless while `sample_count` is 0.
    max_time_millis: i64,
    /// The first and the last record whose samples are held, or `None` if no record was added.
    held_records: Option<(SequenceNumber, SequenceNumber)>,
}

impl HeadSamples {
    /// Adds the samples of the record at `sequence`.
    ///
    /// # Arguments
    ///
    /// * `sequence` - The record's position in the log. It must follow the last record added.
    /// * `batch` - The record's samples, as decoded.
    pub(crate) fn add_record(&mut self, sequence: SequenceNumber, batch: &[SeriesSamples]) {
        for series in batch {
            self.add_series(&series.labels, &series.samples);
        }
        self.held_records = match self.held_records {
            Some((first_held, _last_held)) => Some((first_held, sequence)),
            None => Some((sequence, sequence)),
        };
    }

    /// Adds samples to one series, creating the series if this is its first sample.
    fn add_series(&mut self, labels: &Labels, new_samples: &[Sample]) {
        if new_samples.is_empty() {
            return;
        }
        // Looked up first, so that only a series seen for the first time costs a clone of its labels. The
        // entry interface would clone them for every batch of every series.
        #[allow(clippy::map_entry)]
        if !self.samples_by_series.contains_key(labels) {
            self.samples_by_series.insert(labels.clone(), Vec::new());
        }
        let samples = self
            .samples_by_series
            .get_mut(labels)
            .expect("the series was just ensured");
        for new_sample in new_samples {
            if insert_sample(samples, *new_sample) {
                self.sample_count += 1;
            }
            if self.sample_count == 1 {
                self.min_time_millis = new_sample.timestamp_millis;
                self.max_time_millis = new_sample.timestamp_millis;
            } else {
                self.min_time_millis = self.min_time_millis.min(new_sample.timestamp_millis);
                self.max_time_millis = self.max_time_millis.max(new_sample.timestamp_millis);
            }
        }
    }

    /// Every series held, with its samples.
    pub(crate) const fn samples_by_series(&self) -> &BTreeMap<Labels, Vec<Sample>> {
        &self.samples_by_series
    }

    /// How many samples are held.
    pub(crate) const fn sample_count(&self) -> u64 {
        self.sample_count
    }

    /// How many milliseconds lie between the earliest and the latest sample held. 0 without samples.
    pub(crate) const fn time_span_millis(&self) -> u64 {
        if self.sample_count == 0 {
            return 0;
        }
        self.max_time_millis.abs_diff(self.min_time_millis)
    }

    /// The first and the last record whose samples are held.
    pub(crate) const fn held_records(&self) -> Option<(SequenceNumber, SequenceNumber)> {
        self.held_records
    }

    /// Merges the samples of the selected series that lie in `time_range` into `selected`. Where
    /// `selected` already has a sample for a series and timestamp, the one held here replaces it: the
    /// head is later in the log than anything read before it.
    pub(crate) fn select_into(
        &self,
        selector: &Selector,
        time_range: TimeRange,
        selected: &mut BTreeMap<Labels, Vec<Sample>>,
    ) {
        if self.sample_count == 0 || !time_range.overlaps(self.min_time_millis, self.max_time_millis) {
            return;
        }
        for (labels, samples) in &self.samples_by_series {
            if !selector.matches(labels) {
                continue;
            }
            let range_start = samples.partition_point(|sample| sample.timestamp_millis < time_range.start_millis());
            let range_end = samples.partition_point(|sample| sample.timestamp_millis <= time_range.end_millis());
            let samples_in_range = &samples[range_start..range_end];
            merge_series_later_wins(selected, labels, samples_in_range);
        }
    }
}

/// What one query took from the head.
#[derive(Debug, Default)]
pub(crate) struct HeadSnapshot {
    /// The selected samples in the query's time range, by series.
    pub samples_by_series: BTreeMap<Labels, Vec<Sample>>,
    /// The first and the last record whose samples the head held at that moment, if any.
    pub held_records: Option<(SequenceNumber, SequenceNumber)>,
}

/// A head that queries can read while the materializer adds to it.
#[derive(Debug, Default)]
pub(crate) struct SharedHead {
    /// The samples applied and not yet in a block. Never held locked across an await.
    unsealed: RwLock<HeadSamples>,
}

impl SharedHead {
    /// Adds the samples of consecutive records, in one step as far as queries can tell.
    ///
    /// # Arguments
    ///
    /// * `decoded_records` - Each record's position and its samples, in log order, following the last
    ///   record added.
    pub(crate) fn add_records(&self, decoded_records: &[(SequenceNumber, Vec<SeriesSamples>)]) {
        let mut unsealed = self.write();
        for (sequence, batch) in decoded_records {
            unsealed.add_record(*sequence, batch);
        }
    }

    /// Drops every sample from the head, because blocks now hold them.
    ///
    /// Call it only once the catalog shows those blocks: a query that does not find a record in the head
    /// looks for it in the catalog.
    pub(crate) fn clear(&self) {
        *self.write() = HeadSamples::default();
    }

    /// Copies the selected samples in `time_range` out of the head, with the records they come from.
    pub(crate) fn snapshot(&self, selector: &Selector, time_range: TimeRange) -> HeadSnapshot {
        let unsealed = self.read();
        let mut snapshot = HeadSnapshot {
            samples_by_series: BTreeMap::new(),
            held_records: unsealed.held_records(),
        };
        unsealed.select_into(selector, time_range, &mut snapshot.samples_by_series);
        snapshot
    }

    /// Locks the head for reading, whether or not another holder of the lock panicked: a head is only
    /// ever changed by whole records, so it is consistent at every moment the lock is free.
    fn read(&self) -> RwLockReadGuard<'_, HeadSamples> {
        self.unsealed.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Locks the head for writing; see [`SharedHead::read`] for why poisoning is ignored.
    fn write(&self) -> RwLockWriteGuard<'_, HeadSamples> {
        self.unsealed.write().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
#[path = "head_test.rs"]
mod tests;
