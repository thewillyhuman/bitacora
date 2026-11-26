//! Tests of labels, samples, time ranges and the two operations that keep samples ordered.
//!
//! # What is covered
//!
//! - A label set has one form: sorted, and refused when empty, duplicated, too many or too long.
//! - Samples compare by the bits of their value.
//! - A time range includes both of its ends.
//! - Inserting and merging keep samples strictly ordered, and the later sample wins a timestamp.
//!
//! # How the fixtures work
//!
//! `sample` and `samples` build samples from plain numbers; a value is usually ten times its timestamp so
//! that a wrong pairing shows in a failure.

use super::*;

/// A sample at `timestamp_millis` with the value `value`.
fn sample(timestamp_millis: i64, value: f64) -> Sample {
    Sample {
        timestamp_millis,
        value,
    }
}

/// Samples at the given timestamps, each with ten times its timestamp as value.
fn samples(timestamps: &[i64]) -> Vec<Sample> {
    let mut built = Vec::new();
    for timestamp_millis in timestamps {
        #[allow(clippy::cast_precision_loss)]
        built.push(sample(*timestamp_millis, (*timestamp_millis * 10) as f64));
    }
    built
}

/// The timestamps of `samples`, in the order they are held.
fn timestamps_of(samples: &[Sample]) -> Vec<i64> {
    let mut timestamps = Vec::new();
    for sample in samples {
        timestamps.push(sample.timestamp_millis);
    }
    timestamps
}

#[test]
fn labels_are_sorted_by_name_whatever_order_they_are_given_in() {
    let labels =
        Labels::from_pairs(&[("job", "api"), ("__name__", "requests"), ("instance", "a")]).expect("valid labels");

    let mut names = Vec::new();
    for label in labels.labels() {
        names.push(label.name.as_str());
    }
    assert_eq!(names, ["__name__", "instance", "job"]);
}

#[test]
fn label_sets_given_in_different_orders_are_the_same_series() {
    let one_order = Labels::from_pairs(&[("job", "api"), ("instance", "a")]);
    let other_order = Labels::from_pairs(&[("instance", "a"), ("job", "api")]);

    assert_eq!(one_order, other_order);
}

#[test]
fn value_of_a_label_the_series_has_is_returned() {
    let labels = Labels::from_pairs(&[("job", "api")]).expect("valid labels");

    assert_eq!(labels.value_of("job"), "api");
}

#[test]
fn value_of_a_label_the_series_lacks_is_empty() {
    let labels = Labels::from_pairs(&[("job", "api")]).expect("valid labels");

    assert_eq!(labels.value_of("instance"), "");
}

#[test]
fn series_without_labels_is_refused() {
    assert_eq!(Labels::from_pairs(&[]), Err(InvalidLabels::NoLabels));
}

#[test]
fn label_without_a_name_is_refused() {
    assert_eq!(Labels::from_pairs(&[("", "api")]), Err(InvalidLabels::EmptyName));
}

#[test]
fn label_without_a_value_is_refused() {
    assert_eq!(
        Labels::from_pairs(&[("job", "")]),
        Err(InvalidLabels::EmptyValue { name: "job".to_owned() })
    );
}

#[test]
fn label_given_twice_is_refused() {
    assert_eq!(
        Labels::from_pairs(&[("job", "api"), ("job", "web")]),
        Err(InvalidLabels::DuplicateName { name: "job".to_owned() })
    );
}

#[test]
fn more_labels_than_the_limit_are_refused() {
    let names: Vec<String> = (0..=MAX_LABELS_PER_SERIES)
        .map(|number| format!("label_{number:03}"))
        .collect();
    let mut pairs = Vec::new();
    for name in &names {
        pairs.push((name.as_str(), "value"));
    }

    assert_eq!(
        Labels::from_pairs(&pairs),
        Err(InvalidLabels::TooManyLabels {
            label_count: MAX_LABELS_PER_SERIES + 1
        })
    );
}

#[test]
fn as_many_labels_as_the_limit_are_accepted() {
    let names: Vec<String> = (0..MAX_LABELS_PER_SERIES)
        .map(|number| format!("label_{number:03}"))
        .collect();
    let mut pairs = Vec::new();
    for name in &names {
        pairs.push((name.as_str(), "value"));
    }

    assert!(Labels::from_pairs(&pairs).is_ok());
}

#[test]
fn label_value_longer_than_the_limit_is_refused() {
    let too_long = "v".repeat(MAX_LABEL_TEXT_BYTES + 1);

    assert_eq!(
        Labels::from_pairs(&[("job", too_long.as_str())]),
        Err(InvalidLabels::TextTooLong {
            name: "job".to_owned(),
            text_bytes: MAX_LABEL_TEXT_BYTES + 1
        })
    );
}

#[test]
fn label_value_of_exactly_the_limit_is_accepted() {
    let longest = "v".repeat(MAX_LABEL_TEXT_BYTES);

    assert!(Labels::from_pairs(&[("job", longest.as_str())]).is_ok());
}

#[test]
fn stored_labels_out_of_order_are_refused() {
    let out_of_order = vec![
        Label {
            name: "job".to_owned(),
            value: "api".to_owned(),
        },
        Label {
            name: "instance".to_owned(),
            value: "a".to_owned(),
        },
    ];

    assert_eq!(
        Labels::from_sorted_labels(out_of_order),
        Err(InvalidLabels::NotSorted {
            name: "instance".to_owned()
        })
    );
}

#[test]
fn label_sets_are_ordered_by_their_first_differing_label() {
    let api = Labels::from_pairs(&[("job", "api")]).expect("valid labels");
    let web = Labels::from_pairs(&[("job", "web")]).expect("valid labels");

    assert!(api < web);
}

#[test]
fn nan_sample_equals_itself() {
    let not_a_number = sample(1, f64::NAN);

    assert_eq!(not_a_number, not_a_number);
}

#[test]
fn negative_zero_differs_from_zero() {
    assert_ne!(sample(1, -0.0), sample(1, 0.0));
}

#[test]
fn samples_at_different_times_differ() {
    assert_ne!(sample(1, 5.0), sample(2, 5.0));
}

#[test]
fn time_range_includes_its_start() {
    let range = TimeRange::new(10, 20).expect("valid range");

    assert!(range.contains(10));
}

#[test]
fn time_range_includes_its_end() {
    let range = TimeRange::new(10, 20).expect("valid range");

    assert!(range.contains(20));
}

#[test]
fn time_range_excludes_the_moment_before_its_start() {
    let range = TimeRange::new(10, 20).expect("valid range");

    assert!(!range.contains(9));
}

#[test]
fn time_range_excludes_the_moment_after_its_end() {
    let range = TimeRange::new(10, 20).expect("valid range");

    assert!(!range.contains(21));
}

#[test]
fn time_range_of_one_moment_is_valid() {
    assert!(TimeRange::new(10, 10).is_ok());
}

#[test]
fn time_range_that_ends_before_it_starts_is_refused() {
    assert_eq!(
        TimeRange::new(20, 10),
        Err(InvalidTimeRange {
            start_millis: 20,
            end_millis: 10
        })
    );
}

#[test]
fn time_range_overlaps_a_span_that_ends_at_its_start() {
    let range = TimeRange::new(10, 20).expect("valid range");

    assert!(range.overlaps(0, 10));
}

#[test]
fn time_range_overlaps_a_span_that_starts_at_its_end() {
    let range = TimeRange::new(10, 20).expect("valid range");

    assert!(range.overlaps(20, 30));
}

#[test]
fn time_range_does_not_overlap_a_span_wholly_before_it() {
    let range = TimeRange::new(10, 20).expect("valid range");

    assert!(!range.overlaps(0, 9));
}

#[test]
fn time_range_does_not_overlap_a_span_wholly_after_it() {
    let range = TimeRange::new(10, 20).expect("valid range");

    assert!(!range.overlaps(21, 30));
}

#[test]
fn sample_newer_than_all_is_appended() {
    let mut series = samples(&[10, 20]);

    let was_added = insert_sample(&mut series, sample(30, 1.0));

    assert!(was_added);
    assert_eq!(timestamps_of(&series), [10, 20, 30]);
}

#[test]
fn first_sample_of_a_series_is_appended() {
    let mut series = Vec::new();

    let was_added = insert_sample(&mut series, sample(30, 1.0));

    assert!(was_added);
    assert_eq!(timestamps_of(&series), [30]);
}

#[test]
fn late_sample_is_put_in_its_place() {
    let mut series = samples(&[10, 30]);

    let was_added = insert_sample(&mut series, sample(20, 1.0));

    assert!(was_added);
    assert_eq!(timestamps_of(&series), [10, 20, 30]);
}

#[test]
fn sample_older_than_all_is_put_first() {
    let mut series = samples(&[10, 30]);

    insert_sample(&mut series, sample(5, 1.0));

    assert_eq!(timestamps_of(&series), [5, 10, 30]);
}

#[test]
fn sample_for_a_timestamp_the_series_has_replaces_the_old_one() {
    let mut series = samples(&[10, 20]);

    let was_added = insert_sample(&mut series, sample(20, 7.0));

    assert!(!was_added);
    assert_eq!(series, [sample(10, 100.0), sample(20, 7.0)]);
}

#[test]
fn merge_interleaves_two_sources_in_time_order() {
    let merged = merge_later_wins(&samples(&[10, 30, 50]), &samples(&[20, 40]));

    assert_eq!(timestamps_of(&merged), [10, 20, 30, 40, 50]);
}

#[test]
fn merge_keeps_the_later_source_for_a_shared_timestamp() {
    let earlier = [sample(10, 1.0), sample(20, 1.0)];
    let later = [sample(20, 2.0), sample(30, 2.0)];

    let merged = merge_later_wins(&earlier, &later);

    assert_eq!(merged, [sample(10, 1.0), sample(20, 2.0), sample(30, 2.0)]);
}

#[test]
fn merge_with_an_empty_earlier_source_is_the_later_source() {
    let later = samples(&[10, 20]);

    assert_eq!(merge_later_wins(&[], &later), later);
}

#[test]
fn merge_with_an_empty_later_source_is_the_earlier_source() {
    let earlier = samples(&[10, 20]);

    assert_eq!(merge_later_wins(&earlier, &[]), earlier);
}
