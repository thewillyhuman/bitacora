//! Tests of the head.
//!
//! # What is covered
//!
//! - Adding records: series are created, samples are ordered, a later sample replaces an earlier one, and
//!   the counts, the time span and the held records follow.
//! - Selecting: by labels and by time, and merging into what a query already has.
//! - Parts: the record that fills a part closes it, by count or by time span, and a part rebuilt from
//!   its saved block is the part that was saved.
//! - The shared head: what a snapshot holds and which records it says it covers, across parts and
//!   before and after a seal; where the next block ends; what it encodes, drops, saves and restores.
//!
//! # How the fixtures work
//!
//! `batch`, from `test_support.rs`, builds the samples of one record for the series `job="<job>"`; a
//! value is its timestamp unless a test says otherwise. [`FOUR_SAMPLES`] are limits under which a part is
//! full with four samples or with samples a second apart; [`NO_LIMITS`] never fill a part.

use super::*;
use crate::test_support::{batch, job, sample};

/// The record position `sequence`.
fn at(sequence: u64) -> SequenceNumber {
    SequenceNumber::new(sequence)
}

/// Limits that no part reaches: the whole head is one filling part.
const NO_LIMITS: BlockLimits = BlockLimits {
    max_samples: u64::MAX,
    max_span_millis: u64::MAX,
};

/// Limits under which a part is full with four samples, or with samples that lie a second apart.
const FOUR_SAMPLES: BlockLimits = BlockLimits {
    max_samples: 4,
    max_span_millis: 1_000,
};

/// A head to which the records 7 (`api` at 10 and 20), 8 (`api` at 30 and 40), 9 (`web` at 50) were
/// added under [`FOUR_SAMPLES`]: records 7 and 8 fill a part, record 9 is in the filling part.
fn head_with_a_full_part() -> SharedHead {
    let head = SharedHead::default();
    head.add_records(
        &[
            (at(7), batch("api", &[10, 20])),
            (at(8), batch("api", &[30, 40])),
            (at(9), batch("web", &[50])),
        ],
        FOUR_SAMPLES,
    );
    head
}

/// The timestamps of the series `job="<job>"` in an encoded block.
fn encoded_timestamps(encoded: &EncodedBlock, job_name: &str) -> Vec<i64> {
    let samples_by_series = crate::block::decode_whole_block(&encoded.bytes).expect("a valid block");
    let mut timestamps = Vec::new();
    if let Some(samples) = samples_by_series.get(&job(job_name)) {
        for encoded_sample in samples {
            timestamps.push(encoded_sample.timestamp_millis);
        }
    }
    timestamps
}

/// The timestamps the head holds for the series `job="<job>"`.
fn held_timestamps(head: &HeadSamples, job_name: &str) -> Vec<i64> {
    let mut timestamps = Vec::new();
    if let Some(samples) = head.samples_by_series().get(&job(job_name)) {
        for held_sample in samples {
            timestamps.push(held_sample.timestamp_millis);
        }
    }
    timestamps
}

#[test]
fn empty_head_holds_nothing() {
    let head = HeadSamples::default();

    assert_eq!(head.sample_count(), 0);
    assert_eq!(head.time_span_millis(), 0);
    assert_eq!(head.held_records(), None);
}

#[test]
fn samples_of_a_record_are_held_by_series() {
    let mut head = HeadSamples::default();

    head.add_record(at(1), &batch("api", &[10, 20]));

    assert_eq!(held_timestamps(&head, "api"), [10, 20]);
    assert_eq!(head.sample_count(), 2);
}

#[test]
fn records_for_different_series_make_different_series() {
    let mut head = HeadSamples::default();

    head.add_record(at(1), &batch("api", &[10]));
    head.add_record(at(2), &batch("web", &[10]));

    assert_eq!(head.samples_by_series().len(), 2);
}

#[test]
fn late_sample_is_held_in_time_order() {
    let mut head = HeadSamples::default();
    head.add_record(at(1), &batch("api", &[10, 30]));

    head.add_record(at(2), &batch("api", &[20]));

    assert_eq!(held_timestamps(&head, "api"), [10, 20, 30]);
}

#[test]
fn later_record_replaces_the_sample_of_a_timestamp() {
    let mut head = HeadSamples::default();
    head.add_record(at(1), &batch("api", &[10]));
    let replacement = vec![SeriesSamples {
        labels: job("api"),
        samples: vec![sample(10, 7.0)],
    }];

    head.add_record(at(2), &replacement);

    assert_eq!(head.samples_by_series().get(&job("api")), Some(&vec![sample(10, 7.0)]));
}

#[test]
fn replaced_sample_is_not_counted_twice() {
    let mut head = HeadSamples::default();
    head.add_record(at(1), &batch("api", &[10]));

    head.add_record(at(2), &batch("api", &[10]));

    assert_eq!(head.sample_count(), 1);
}

#[test]
fn series_without_samples_in_a_record_is_not_created() {
    let mut head = HeadSamples::default();

    head.add_record(at(1), &batch("api", &[]));

    assert_eq!(head.samples_by_series().len(), 0);
}

#[test]
fn record_without_samples_still_counts_as_held() {
    let mut head = HeadSamples::default();

    head.add_record(at(4), &batch("api", &[]));

    assert_eq!(head.held_records(), Some((at(4), at(4))));
}

#[test]
fn held_records_run_from_the_first_to_the_last_added() {
    let mut head = HeadSamples::default();

    head.add_record(at(4), &batch("api", &[10]));
    head.add_record(at(5), &batch("api", &[20]));
    head.add_record(at(6), &batch("web", &[30]));

    assert_eq!(head.held_records(), Some((at(4), at(6))));
}

#[test]
fn time_span_runs_from_the_earliest_to_the_latest_sample_of_any_series() {
    let mut head = HeadSamples::default();

    head.add_record(at(1), &batch("api", &[100, 250]));
    head.add_record(at(2), &batch("web", &[40]));

    assert_eq!(head.time_span_millis(), 210);
}

#[test]
fn time_span_of_one_sample_is_zero() {
    let mut head = HeadSamples::default();

    head.add_record(at(1), &batch("api", &[100]));

    assert_eq!(head.time_span_millis(), 0);
}

#[test]
fn time_span_across_the_whole_range_of_timestamps_does_not_overflow() {
    let mut head = HeadSamples::default();

    head.add_record(at(1), &batch("api", &[i64::MIN / 10, i64::MAX / 10]));

    assert_eq!(head.time_span_millis(), (i64::MAX / 10).abs_diff(i64::MIN / 10));
}

#[test]
fn selection_takes_only_the_series_that_match() {
    let mut head = HeadSamples::default();
    head.add_record(at(1), &batch("api", &[10]));
    head.add_record(at(2), &batch("web", &[10]));
    let only_api = Selector::new(vec![crate::selector::Matcher::equal("job", "api")]);
    let mut selected = BTreeMap::new();

    head.select_into(&only_api, TimeRange::ALL, &mut selected);

    let selected_series: Vec<&Labels> = selected.keys().collect();
    assert_eq!(selected_series, [&job("api")]);
}

#[test]
fn selection_takes_only_the_samples_in_the_time_range_ends_included() {
    let mut head = HeadSamples::default();
    head.add_record(at(1), &batch("api", &[10, 20, 30, 40]));
    let range = TimeRange::new(20, 30).expect("valid range");
    let mut selected = BTreeMap::new();

    head.select_into(&Selector::all(), range, &mut selected);

    assert_eq!(
        selected.get(&job("api")),
        Some(&vec![sample(20, 20.0), sample(30, 30.0)])
    );
}

#[test]
fn series_without_a_sample_in_the_time_range_is_not_selected() {
    let mut head = HeadSamples::default();
    head.add_record(at(1), &batch("api", &[10]));
    head.add_record(at(2), &batch("web", &[500]));
    let range = TimeRange::new(400, 600).expect("valid range");
    let mut selected = BTreeMap::new();

    head.select_into(&Selector::all(), range, &mut selected);

    assert!(!selected.contains_key(&job("api")));
}

#[test]
fn selection_replaces_what_was_read_earlier_for_the_same_timestamp() {
    let mut head = HeadSamples::default();
    head.add_record(at(2), &batch("api", &[20, 30]));
    let mut selected = BTreeMap::new();
    selected.insert(job("api"), vec![sample(10, 1.0), sample(20, 1.0)]);

    head.select_into(&Selector::all(), TimeRange::ALL, &mut selected);

    assert_eq!(
        selected.get(&job("api")),
        Some(&vec![sample(10, 1.0), sample(20, 20.0), sample(30, 30.0)])
    );
}

#[test]
fn part_is_not_full_below_its_limits() {
    let mut part = HeadSamples::default();

    part.add_record(at(1), &batch("api", &[10, 20, 30]));

    assert!(!part.is_full(FOUR_SAMPLES));
}

#[test]
fn part_is_full_at_the_sample_limit() {
    let mut part = HeadSamples::default();

    part.add_record(at(1), &batch("api", &[10, 20, 30, 40]));

    assert!(part.is_full(FOUR_SAMPLES));
}

#[test]
fn part_is_full_once_its_samples_span_the_time_limit() {
    let mut part = HeadSamples::default();

    part.add_record(at(1), &batch("api", &[10, 1_010]));

    assert!(part.is_full(FOUR_SAMPLES));
}

#[test]
fn replaced_sample_does_not_fill_a_part() {
    let mut part = HeadSamples::default();
    part.add_record(at(1), &batch("api", &[10, 20, 30]));

    part.add_record(at(2), &batch("api", &[30]));

    assert!(!part.is_full(FOUR_SAMPLES));
}

#[test]
fn part_without_a_record_has_no_last_record_and_is_not_full() {
    let part = HeadSamples::default();

    assert!(!part.is_full(FOUR_SAMPLES));
    assert_eq!(part.held_records(), None);
}

#[test]
fn part_rebuilt_from_its_samples_holds_them() {
    let mut saved = HeadSamples::default();
    saved.add_record(at(7), &batch("api", &[10, 20]));
    saved.add_record(at(8), &batch("web", &[5]));

    let rebuilt = HeadSamples::from_saved(saved.samples_by_series().clone(), (at(7), at(8)));

    assert_eq!(rebuilt.samples_by_series(), saved.samples_by_series());
    assert_eq!(rebuilt.held_records(), Some((at(7), at(8))));
}

#[test]
fn part_rebuilt_from_its_samples_is_as_full_as_the_part_that_was_saved() {
    let mut saved = HeadSamples::default();
    saved.add_record(at(7), &batch("api", &[10, 20]));
    saved.add_record(at(8), &batch("web", &[5]));

    let rebuilt = HeadSamples::from_saved(saved.samples_by_series().clone(), (at(7), at(8)));

    assert_eq!(rebuilt.sample_count(), saved.sample_count());
    assert_eq!(rebuilt.time_span_millis(), saved.time_span_millis());
}

#[test]
fn part_rebuilt_without_samples_still_knows_its_records() {
    let rebuilt = HeadSamples::from_saved(BTreeMap::new(), (at(7), at(9)));

    assert_eq!(rebuilt.sample_count(), 0);
    assert_eq!(rebuilt.held_records(), Some((at(7), at(9))));
}

#[test]
fn snapshot_of_a_new_shared_head_covers_no_record() {
    let head = SharedHead::default();

    let snapshot = head.snapshot(&Selector::all(), TimeRange::ALL);

    assert_eq!(snapshot.held_records, None);
    assert_eq!(snapshot.samples_by_series.len(), 0);
}

#[test]
fn snapshot_says_which_records_its_samples_come_from() {
    let head = SharedHead::default();
    head.add_records(&[(at(7), batch("api", &[10])), (at(8), batch("api", &[20]))], NO_LIMITS);

    let snapshot = head.snapshot(&Selector::all(), TimeRange::ALL);

    assert_eq!(snapshot.held_records, Some((at(7), at(8))));
}

#[test]
fn snapshot_holds_the_selected_samples() {
    let head = SharedHead::default();
    head.add_records(&[(at(7), batch("api", &[10])), (at(8), batch("web", &[20]))], NO_LIMITS);
    let only_web = Selector::new(vec![crate::selector::Matcher::equal("job", "web")]);

    let snapshot = head.snapshot(&only_web, TimeRange::ALL);

    let mut expected = BTreeMap::new();
    expected.insert(job("web"), vec![sample(20, 20.0)]);
    assert_eq!(snapshot.samples_by_series, expected);
}

#[test]
fn head_that_never_sealed_says_so() {
    let head = SharedHead::default();
    head.add_records(&[(at(7), batch("api", &[10]))], NO_LIMITS);

    let snapshot = head.snapshot(&Selector::all(), TimeRange::ALL);

    assert_eq!(snapshot.sealed_through, SequenceNumber::ZERO);
}

#[test]
fn head_without_a_full_part_names_no_end_of_a_block() {
    let head = SharedHead::default();

    head.add_records(&[(at(7), batch("api", &[10, 20, 30]))], FOUR_SAMPLES);

    assert_eq!(head.oldest_full_part_end(), None);
}

#[test]
fn record_that_fills_a_part_ends_the_block() {
    let head = head_with_a_full_part();

    assert_eq!(head.oldest_full_part_end(), Some(at(8)));
}

#[test]
fn record_after_a_full_part_starts_the_next_part() {
    let head = head_with_a_full_part();

    let filling = head.encode_filling_part_after_dropping_through(at(8));

    assert_eq!(encoded_timestamps(&filling, "web"), [50]);
    assert_eq!(encoded_timestamps(&filling, "api"), Vec::<i64>::new());
}

#[test]
fn several_parts_can_be_full_at_once() {
    let head = SharedHead::default();

    head.add_records(
        &[
            (at(1), batch("api", &[10, 20, 30, 40])),
            (at(2), batch("api", &[50, 60, 70, 80])),
            (at(3), batch("api", &[90])),
        ],
        FOUR_SAMPLES,
    );

    assert_eq!(head.oldest_full_part_end(), Some(at(1)));
    head.drop_through(at(1));
    assert_eq!(head.oldest_full_part_end(), Some(at(2)));
    head.drop_through(at(2));
    assert_eq!(head.oldest_full_part_end(), None);
}

#[test]
fn parts_end_at_the_same_records_however_the_records_arrive() {
    let records = [
        (at(1), batch("api", &[10, 20])),
        (at(2), batch("api", &[30, 40])),
        (at(3), batch("api", &[50, 60, 70])),
        (at(4), batch("api", &[80])),
    ];
    let all_at_once = SharedHead::default();
    all_at_once.add_records(&records, FOUR_SAMPLES);
    let one_by_one = SharedHead::default();

    for record in &records {
        one_by_one.add_records(std::slice::from_ref(record), FOUR_SAMPLES);
    }

    assert_eq!(all_at_once.oldest_full_part_end(), Some(at(2)));
    assert_eq!(one_by_one.oldest_full_part_end(), Some(at(2)));
    all_at_once.drop_through(at(2));
    one_by_one.drop_through(at(2));
    assert_eq!(all_at_once.oldest_full_part_end(), Some(at(4)));
    assert_eq!(one_by_one.oldest_full_part_end(), Some(at(4)));
}

#[test]
fn snapshot_covers_the_records_of_every_part() {
    let head = head_with_a_full_part();

    let snapshot = head.snapshot(&Selector::all(), TimeRange::ALL);

    assert_eq!(snapshot.held_records, Some((at(7), at(9))));
}

#[test]
fn snapshot_holds_the_samples_of_every_part() {
    let head = head_with_a_full_part();

    let snapshot = head.snapshot(&Selector::all(), TimeRange::ALL);

    let mut expected = BTreeMap::new();
    expected.insert(
        job("api"),
        vec![sample(10, 10.0), sample(20, 20.0), sample(30, 30.0), sample(40, 40.0)],
    );
    expected.insert(job("web"), vec![sample(50, 50.0)]);
    assert_eq!(snapshot.samples_by_series, expected);
}

#[test]
fn sample_in_a_later_part_replaces_the_one_in_an_earlier_part() {
    let head = SharedHead::default();
    let mut replacement = batch("api", &[10]);
    replacement[0].samples[0].value = 99.0;
    head.add_records(
        &[(at(1), batch("api", &[10, 20, 30, 40])), (at(2), replacement)],
        FOUR_SAMPLES,
    );

    let snapshot = head.snapshot(&Selector::all(), TimeRange::ALL);

    assert_eq!(snapshot.samples_by_series[&job("api")][0], sample(10, 99.0));
}

#[test]
fn block_of_the_oldest_full_part_holds_exactly_its_records() {
    let head = head_with_a_full_part();

    let encoded = head.encode_through(at(8)).expect("a part ends at record 8");

    assert_eq!(encoded_timestamps(&encoded, "api"), [10, 20, 30, 40]);
    assert_eq!(encoded_timestamps(&encoded, "web"), Vec::<i64>::new());
}

#[test]
fn block_of_everything_merges_the_parts() {
    let head = head_with_a_full_part();

    let encoded = head.encode_through(at(9)).expect("the filling part ends at record 9");

    assert_eq!(encoded_timestamps(&encoded, "api"), [10, 20, 30, 40]);
    assert_eq!(encoded_timestamps(&encoded, "web"), [50]);
}

#[test]
fn no_block_ends_inside_a_part() {
    let head = head_with_a_full_part();

    assert!(head.encode_through(at(7)).is_none());
}

#[test]
fn no_block_ends_past_what_the_head_holds() {
    let head = head_with_a_full_part();

    assert!(head.encode_through(at(10)).is_none());
}

#[test]
fn empty_head_encodes_no_block() {
    let head = SharedHead::default();

    assert!(head.encode_through(at(1)).is_none());
}

#[test]
fn encoding_leaves_the_head_as_it_was() {
    let head = head_with_a_full_part();

    drop(head.encode_through(at(8)));

    assert_eq!(
        head.snapshot(&Selector::all(), TimeRange::ALL).held_records,
        Some((at(7), at(9)))
    );
}

#[test]
fn sealed_part_is_dropped_and_the_rest_is_kept() {
    let head = head_with_a_full_part();

    head.drop_through(at(8));

    let snapshot = head.snapshot(&Selector::all(), TimeRange::ALL);
    assert_eq!(snapshot.held_records, Some((at(9), at(9))));
    assert_eq!(snapshot.samples_by_series.len(), 1);
}

#[test]
fn head_holds_no_sample_after_all_its_samples_were_sealed() {
    let head = head_with_a_full_part();

    head.drop_through(at(9));

    assert_eq!(
        head.snapshot(&Selector::all(), TimeRange::ALL).samples_by_series.len(),
        0
    );
}

#[test]
fn head_covers_no_record_after_all_its_samples_were_sealed() {
    let head = head_with_a_full_part();

    head.drop_through(at(9));

    assert_eq!(head.snapshot(&Selector::all(), TimeRange::ALL).held_records, None);
}

#[test]
fn head_remembers_up_to_which_record_it_was_sealed() {
    let head = head_with_a_full_part();

    head.drop_through(at(8));

    assert_eq!(head.snapshot(&Selector::all(), TimeRange::ALL).sealed_through, at(8));
}

#[test]
fn head_takes_records_again_after_a_seal() {
    let head = SharedHead::default();
    head.add_records(&[(at(7), batch("api", &[10]))], NO_LIMITS);
    head.drop_through(at(7));

    head.add_records(&[(at(8), batch("api", &[20]))], NO_LIMITS);

    assert_eq!(
        head.snapshot(&Selector::all(), TimeRange::ALL).held_records,
        Some((at(8), at(8)))
    );
}

#[test]
fn filling_part_is_saved_as_a_block() {
    let head = SharedHead::default();
    head.add_records(&[(at(7), batch("api", &[10, 20]))], FOUR_SAMPLES);

    let saved = head.encode_filling_part();

    assert_eq!(encoded_timestamps(&saved, "api"), [10, 20]);
}

#[test]
fn saving_leaves_the_head_as_it_was() {
    let head = SharedHead::default();
    head.add_records(&[(at(7), batch("api", &[10, 20]))], FOUR_SAMPLES);

    drop(head.encode_filling_part());

    assert_eq!(
        head.snapshot(&Selector::all(), TimeRange::ALL).held_records,
        Some((at(7), at(7)))
    );
}

#[test]
#[should_panic(expected = "full parts wait to be sealed")]
fn saving_around_a_full_part_is_a_mistake_of_the_caller() {
    let head = head_with_a_full_part();

    drop(head.encode_filling_part());
}

#[test]
fn restored_head_is_filled_by_the_record_that_would_have_filled_the_saved_one() {
    let uninterrupted = SharedHead::default();
    uninterrupted.add_records(&[(at(7), batch("api", &[10, 20]))], FOUR_SAMPLES);
    let saved = uninterrupted.encode_filling_part();
    let saved_samples = crate::block::decode_whole_block(&saved.bytes).expect("a valid block");
    let restored = SharedHead::default();
    restored.restore_filling_part(HeadSamples::from_saved(saved_samples, (at(7), at(7))));
    let later_records = [
        (at(8), batch("api", &[30])),
        (at(9), batch("api", &[40])),
        (at(10), batch("api", &[50])),
    ];

    uninterrupted.add_records(&later_records, FOUR_SAMPLES);
    restored.add_records(&later_records, FOUR_SAMPLES);

    assert_eq!(uninterrupted.oldest_full_part_end(), Some(at(9)));
    assert_eq!(restored.oldest_full_part_end(), Some(at(9)));
}

#[test]
fn restored_head_encodes_the_block_the_saved_head_would_have() {
    let uninterrupted = SharedHead::default();
    uninterrupted.add_records(&[(at(7), batch("api", &[10, 20]))], FOUR_SAMPLES);
    let saved = uninterrupted.encode_filling_part();
    let saved_samples = crate::block::decode_whole_block(&saved.bytes).expect("a valid block");
    let restored = SharedHead::default();
    restored.restore_filling_part(HeadSamples::from_saved(saved_samples, (at(7), at(7))));
    let later_records = [(at(8), batch("web", &[30, 40]))];
    uninterrupted.add_records(&later_records, FOUR_SAMPLES);
    restored.add_records(&later_records, FOUR_SAMPLES);

    let block_of_the_restored_head = restored.encode_through(at(8)).expect("a part ends at record 8");

    let block_of_the_uninterrupted_head = uninterrupted.encode_through(at(8)).expect("a part ends at record 8");
    assert_eq!(block_of_the_restored_head.bytes, block_of_the_uninterrupted_head.bytes);
}

#[test]
fn restored_head_shows_the_saved_records_to_queries() {
    let saved_part = {
        let mut part = HeadSamples::default();
        part.add_record(at(7), &batch("api", &[10]));
        part
    };
    let restored = SharedHead::default();

    restored.restore_filling_part(HeadSamples::from_saved(
        saved_part.samples_by_series().clone(),
        (at(7), at(7)),
    ));

    let snapshot = restored.snapshot(&Selector::all(), TimeRange::ALL);
    assert_eq!(snapshot.held_records, Some((at(7), at(7))));
    assert_eq!(snapshot.samples_by_series.len(), 1);
}

impl SharedHead {
    /// Drops the parts up to `sealed_through` and returns the filling part as a block: what a seal
    /// followed by a save does.
    fn encode_filling_part_after_dropping_through(&self, sealed_through: SequenceNumber) -> EncodedBlock {
        self.drop_through(sealed_through);
        self.encode_filling_part()
    }
}
