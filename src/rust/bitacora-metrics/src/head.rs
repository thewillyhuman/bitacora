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
//! - `BlockLimits`: when such a part is full.
//! - `SharedHead`: the parts of the head behind a lock, written by the builder and read by queries.
//! - `HeadSnapshot`: what one query took from the head, and which records that covers.
//!
//! # How it works
//!
//! `HeadSamples` is an ordered map from label set to the series' samples, strictly increasing in time.
//! Adding a record's samples keeps that order and lets a later sample replace an earlier one with the
//! same timestamp (see `sample.rs`). It also remembers the first and the last record it holds and the
//! span of time its samples cover, which is what decides when a block is cut.
//!
//! The head is a row of such parts in log order. Records go into the last one, the part that is
//! filling. The record that brings it to the limits of a block makes it full: the part is closed and the
//! next record starts a new one. A full part is exactly one future block, and where it ends was decided
//! by the records alone. Whenever a checkpoint happens, and whichever instance runs, the same records
//! therefore give the same blocks. The builder seals full parts, oldest first, and saves the filling
//! part at a checkpoint (`builder.rs`).
//!
//! `SharedHead` makes that readable while it is written: the builder takes the lock once per batch of
//! records, a query once per query. The builder owns the head and adds to it; the store takes a handle
//! on it for its queries. There is one copy of every unsealed sample.
//!
//! A query reads the head and the catalog of blocks at two different moments, and the builder drops
//! sealed samples from the head a moment before the framework lists their block in the catalog. The
//! head therefore tells a query which records its samples come from and up to which record it has given
//! samples up to a block. With those the query can tell whether the head and the catalog it then finds
//! fit together (see `query.rs`).
//!
//! Adding samples is a hot path: every sample passes through `HeadSamples::add_series` once. A sample
//! that arrives in time order costs one comparison and a push. Nothing is allocated per sample; a new
//! series costs one clone of its labels.
//!
//! # Invariants
//!
//! - The samples of every series are strictly increasing in time.
//! - `sample_count` is the number of samples held, after replacements.
//! - Records are added in log order, each exactly once, and removed only by whole parts.
//! - A part is closed by the record that fills it and never changes afterwards.
//! - A part rebuilt from its saved block behaves as the part that was saved: it holds the same samples,
//!   so it is filled by the same later record.
//!
//! # See also
//!
//! - `builder.rs`: the builder, which owns the head, adds records to it and seals blocks from it.
//! - `query.rs`: the reading side.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use bitacora_types::SequenceNumber;

use crate::block::{EncodedBlock, encode_block};
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
    /// Rebuilds the part that was saved as a block.
    ///
    /// # Arguments
    ///
    /// * `samples_by_series` - What the saved block holds: every series with its samples, strictly
    ///   increasing in time.
    /// * `held_records` - The first and the last record whose samples those are.
    pub(crate) fn from_saved(
        samples_by_series: BTreeMap<Labels, Vec<Sample>>,
        held_records: (SequenceNumber, SequenceNumber),
    ) -> Self {
        let mut rebuilt = Self {
            samples_by_series,
            held_records: Some(held_records),
            ..Self::default()
        };
        for samples in rebuilt.samples_by_series.values() {
            let (Some(earliest), Some(latest)) = (samples.first(), samples.last()) else {
                continue;
            };
            if rebuilt.sample_count == 0 {
                rebuilt.min_time_millis = earliest.timestamp_millis;
                rebuilt.max_time_millis = latest.timestamp_millis;
            } else {
                rebuilt.min_time_millis = rebuilt.min_time_millis.min(earliest.timestamp_millis);
                rebuilt.max_time_millis = rebuilt.max_time_millis.max(latest.timestamp_millis);
            }
            rebuilt.sample_count += samples.len() as u64;
        }
        rebuilt
    }

    /// Whether the part holds as many samples, or samples that span as much time, as one block may.
    pub(crate) const fn is_full(&self, limits: BlockLimits) -> bool {
        self.sample_count >= limits.max_samples || self.time_span_millis() >= limits.max_span_millis
    }

    /// The last record whose samples are held, if any record was added.
    fn last_record(&self) -> Option<SequenceNumber> {
        self.held_records.map(|(_first_held, last_held)| last_held)
    }

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

    /// How many samples are held. Only the tests ask; the head itself uses the count through
    /// [`HeadSamples::is_full`].
    #[cfg(test)]
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
#[derive(Debug)]
pub(crate) struct HeadSnapshot {
    /// The selected samples in the query's time range, by series.
    pub samples_by_series: BTreeMap<Labels, Vec<Sample>>,
    /// The first and the last record whose samples the head held at that moment, if any.
    pub held_records: Option<(SequenceNumber, SequenceNumber)>,
    /// The last record whose samples the head has given up to a block; [`SequenceNumber::ZERO`] if it
    /// has not given any up. The catalog must reach at least this far for nothing to be missing.
    pub sealed_through: SequenceNumber,
}

/// When a part of the head is full: the limits of one block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BlockLimits {
    /// A part is full once it holds this many samples.
    pub max_samples: u64,
    /// A part is full once its samples span this many milliseconds, by their own timestamps.
    pub max_span_millis: u64,
}

/// What the builder and the queries share of a head.
#[derive(Debug)]
struct HeadState {
    /// The parts that are full and wait to be sealed, oldest first. Each becomes one block.
    full_parts: VecDeque<HeadSamples>,
    /// The part that takes the next record: the samples of the records after the last full part.
    filling: HeadSamples,
    /// The last record whose samples were dropped from the head because a block holds them.
    sealed_through: SequenceNumber,
}

impl HeadState {
    /// How many of the head's parts, oldest first, end at or before `last_record`, if a part ends
    /// exactly there. The filling part counts as one more part after the full ones.
    fn parts_through(&self, last_record: SequenceNumber) -> Option<usize> {
        let mut part_count = 0;
        for part in self.full_parts.iter().chain([&self.filling]) {
            let part_end = part.last_record()?;
            part_count += 1;
            if part_end == last_record {
                return Some(part_count);
            }
            if part_end > last_record {
                return None;
            }
        }
        None
    }
}

/// A head that queries can read while the builder adds to it.
#[derive(Debug)]
pub(crate) struct SharedHead {
    /// The head. Never held locked across an await.
    state: RwLock<HeadState>,
}

impl Default for SharedHead {
    /// A head that holds nothing and has given nothing up to a block.
    fn default() -> Self {
        Self {
            state: RwLock::new(HeadState {
                full_parts: VecDeque::new(),
                filling: HeadSamples::default(),
                sealed_through: SequenceNumber::ZERO,
            }),
        }
    }
}

impl SharedHead {
    /// Adds the samples of consecutive records, in one step as far as queries can tell. A record that
    /// fills the part it goes into closes that part.
    ///
    /// # Arguments
    ///
    /// * `decoded_records` - Each record's position and its samples, in log order, following the last
    ///   record added.
    /// * `limits` - When a part is full.
    pub(crate) fn add_records(&self, decoded_records: &[(SequenceNumber, Vec<SeriesSamples>)], limits: BlockLimits) {
        let mut state = self.write();
        for (sequence, batch) in decoded_records {
            state.filling.add_record(*sequence, batch);
            if state.filling.is_full(limits) {
                let full_part = std::mem::take(&mut state.filling);
                state.full_parts.push_back(full_part);
            }
        }
    }

    /// The last record of the oldest full part: where the next block ends. `None` while no part is full.
    pub(crate) fn oldest_full_part_end(&self) -> Option<SequenceNumber> {
        self.read().full_parts.front().and_then(HeadSamples::last_record)
    }

    /// The samples of the records up to `last_record`, as the bytes of a block. The head is left as it
    /// is.
    ///
    /// # Returns
    ///
    /// `None` if no part of the head ends at `last_record`: a block is made of whole parts.
    pub(crate) fn encode_through(&self, last_record: SequenceNumber) -> Option<EncodedBlock> {
        let state = self.read();
        let part_count = state.parts_through(last_record)?;
        let mut parts = state.full_parts.iter().chain([&state.filling]).take(part_count);
        let first_part = parts.next()?;
        if part_count == 1 {
            return Some(encode_block(first_part.samples_by_series()));
        }
        // Several parts in one block: their samples are merged, the later part winning.
        let mut merged = first_part.samples_by_series().clone();
        for later_part in parts {
            for (labels, samples) in later_part.samples_by_series() {
                merge_series_later_wins(&mut merged, labels, samples);
            }
        }
        Some(encode_block(&merged))
    }

    /// Drops the parts that end at or before `sealed_through` from the head, because a block now holds
    /// their records.
    ///
    /// The block is not in the catalog yet when the builder calls this; the framework lists it a moment
    /// later. A query that looks in between is told by [`HeadSnapshot::sealed_through`] that the catalog
    /// it sees is too short.
    pub(crate) fn drop_through(&self, sealed_through: SequenceNumber) {
        let mut state = self.write();
        while let Some(oldest_end) = state.full_parts.front().and_then(HeadSamples::last_record) {
            if oldest_end > sealed_through {
                break;
            }
            state.full_parts.pop_front();
        }
        let is_filling_part_sealed = state.filling.last_record().is_some_and(|end| end <= sealed_through);
        if state.full_parts.is_empty() && is_filling_part_sealed {
            state.filling = HeadSamples::default();
        }
        state.sealed_through = sealed_through;
    }

    /// The filling part as the bytes of a block, to be saved at a checkpoint. The head is left as it is.
    ///
    /// # Panics
    ///
    /// Panics if a full part still waits to be sealed. The framework seals every block that is due
    /// before it saves; saving around a full part would make the next instance cut other blocks.
    pub(crate) fn encode_filling_part(&self) -> EncodedBlock {
        let state = self.read();
        assert!(
            state.full_parts.is_empty(),
            "the head was asked to save its filling part while {} full parts wait to be sealed",
            state.full_parts.len()
        );
        encode_block(state.filling.samples_by_series())
    }

    /// Makes a head that holds nothing hold the part that an earlier instance saved.
    ///
    /// # Arguments
    ///
    /// * `saved_part` - The part, rebuilt with [`HeadSamples::from_saved`].
    pub(crate) fn restore_filling_part(&self, saved_part: HeadSamples) {
        self.write().filling = saved_part;
    }

    /// Copies the selected samples in `time_range` out of the head, with the records they come from.
    pub(crate) fn snapshot(&self, selector: &Selector, time_range: TimeRange) -> HeadSnapshot {
        let state = self.read();
        let mut snapshot = HeadSnapshot {
            samples_by_series: BTreeMap::new(),
            held_records: None,
            sealed_through: state.sealed_through,
        };
        // Oldest part first, so that a later record wins a shared timestamp.
        for part in state.full_parts.iter().chain([&state.filling]) {
            let Some((first_held, last_held)) = part.held_records() else {
                continue;
            };
            snapshot.held_records = match snapshot.held_records {
                Some((first_of_all, _last_so_far)) => Some((first_of_all, last_held)),
                None => Some((first_held, last_held)),
            };
            part.select_into(selector, time_range, &mut snapshot.samples_by_series);
        }
        snapshot
    }

    /// Locks the head for reading, whether or not another holder of the lock panicked: a head is only
    /// ever changed by whole records, so it is consistent at every moment the lock is free.
    fn read(&self) -> RwLockReadGuard<'_, HeadState> {
        self.state.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Locks the head for writing; see [`SharedHead::read`] for why poisoning is ignored.
    fn write(&self) -> RwLockWriteGuard<'_, HeadState> {
        self.state.write().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
#[path = "head_test.rs"]
mod tests;
