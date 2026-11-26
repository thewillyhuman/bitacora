//! Tests of the head.
//!
//! # What is covered
//!
//! - Adding records: series are created, samples are ordered, a later sample replaces an earlier one, and
//!   the counts, the time span and the held records follow.
//! - Selecting: by labels and by time, and merging into what a query already has.
//! - The shared head: what a snapshot holds and which records it says it covers, before and after a
//!   seal; how full it is; what it encodes.
//!
//! # How the fixtures work
//!
//! `batch`, from `test_support.rs`, builds the samples of one record for the series `job="<job>"`; a
//! value is its timestamp unless a test says otherwise.

use super::*;
use crate::test_support::{batch, job, sample};

/// The record position `sequence`.
fn at(sequence: u64) -> SequenceNumber {
    SequenceNumber::new(sequence)
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
fn snapshot_of_a_new_shared_head_covers_no_record() {
    let head = SharedHead::default();

    let snapshot = head.snapshot(&Selector::all(), TimeRange::ALL);

    assert_eq!(snapshot.held_records, None);
}

#[test]
fn snapshot_says_which_records_its_samples_come_from() {
    let head = SharedHead::default();
    head.add_records(&[(at(7), batch("api", &[10])), (at(8), batch("api", &[20]))]);

    let snapshot = head.snapshot(&Selector::all(), TimeRange::ALL);

    assert_eq!(snapshot.held_records, Some((at(7), at(8))));
}

#[test]
fn snapshot_holds_the_selected_samples() {
    let head = SharedHead::default();
    head.add_records(&[(at(7), batch("api", &[10])), (at(8), batch("web", &[20]))]);
    let only_web = Selector::new(vec![crate::selector::Matcher::equal("job", "web")]);

    let snapshot = head.snapshot(&only_web, TimeRange::ALL);

    let mut expected = BTreeMap::new();
    expected.insert(job("web"), vec![sample(20, 20.0)]);
    assert_eq!(snapshot.samples_by_series, expected);
}

#[test]
fn head_that_never_sealed_says_so() {
    let head = SharedHead::default();
    head.add_records(&[(at(7), batch("api", &[10]))]);

    let snapshot = head.snapshot(&Selector::all(), TimeRange::ALL);

    assert_eq!(snapshot.sealed_through, SequenceNumber::ZERO);
}

#[test]
fn head_holds_no_sample_after_its_samples_were_sealed() {
    let head = SharedHead::default();
    head.add_records(&[(at(7), batch("api", &[10]))]);

    head.drop_sealed(at(7));

    assert_eq!(
        head.snapshot(&Selector::all(), TimeRange::ALL).samples_by_series.len(),
        0
    );
}

#[test]
fn head_covers_no_record_after_its_samples_were_sealed() {
    let head = SharedHead::default();
    head.add_records(&[(at(7), batch("api", &[10]))]);

    head.drop_sealed(at(7));

    assert_eq!(head.snapshot(&Selector::all(), TimeRange::ALL).held_records, None);
}

#[test]
fn head_remembers_up_to_which_record_it_was_sealed() {
    let head = SharedHead::default();
    head.add_records(&[(at(7), batch("api", &[10]))]);

    head.drop_sealed(at(7));

    assert_eq!(head.snapshot(&Selector::all(), TimeRange::ALL).sealed_through, at(7));
}

#[test]
fn fill_counts_the_samples_and_the_time_they_span() {
    let head = SharedHead::default();

    head.add_records(&[(at(7), batch("api", &[100, 250])), (at(8), batch("web", &[40]))]);

    let expected = HeadFill {
        sample_count: 3,
        time_span_millis: 210,
    };
    assert_eq!(head.fill(), expected);
}

#[test]
fn head_encodes_what_it_holds_as_a_block() {
    let head = SharedHead::default();
    head.add_records(&[(at(7), batch("api", &[10, 20]))]);

    let encoded = head.encode_block();

    let mut expected = BTreeMap::new();
    expected.insert(job("api"), vec![sample(10, 10.0), sample(20, 20.0)]);
    assert_eq!(crate::block::decode_whole_block(&encoded.bytes), Ok(expected));
}

#[test]
fn head_takes_records_again_after_a_seal() {
    let head = SharedHead::default();
    head.add_records(&[(at(7), batch("api", &[10]))]);
    head.drop_sealed(at(7));

    head.add_records(&[(at(8), batch("api", &[20]))]);

    assert_eq!(
        head.snapshot(&Selector::all(), TimeRange::ALL).held_records,
        Some((at(8), at(8)))
    );
}
