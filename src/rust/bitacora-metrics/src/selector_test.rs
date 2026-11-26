//! Tests of selectors and matchers.
//!
//! # What is covered
//!
//! - Each kind of matcher, on a series that has the label and on one that lacks it.
//! - A selector needs every matcher to hold, and an empty one selects everything.
//! - Which matchers a block's postings can answer.
//!
//! # How the fixtures work
//!
//! `api_series` is a series with the labels `__name__="up"` and `job="api"`.

use super::*;

/// A series named `up` of the job `api`.
fn api_series() -> Labels {
    Labels::from_pairs(&[("__name__", "up"), ("job", "api")]).expect("valid labels")
}

#[test]
fn equal_matcher_accepts_the_value_the_series_has() {
    assert!(Matcher::equal("job", "api").matches(&api_series()));
}

#[test]
fn equal_matcher_refuses_another_value() {
    assert!(!Matcher::equal("job", "web").matches(&api_series()));
}

#[test]
fn equal_matcher_refuses_a_series_without_the_label() {
    assert!(!Matcher::equal("zone", "eu").matches(&api_series()));
}

#[test]
fn equal_matcher_for_the_empty_value_accepts_a_series_without_the_label() {
    assert!(Matcher::equal("zone", "").matches(&api_series()));
}

#[test]
fn equal_matcher_for_the_empty_value_refuses_a_series_with_the_label() {
    assert!(!Matcher::equal("job", "").matches(&api_series()));
}

#[test]
fn not_equal_matcher_accepts_another_value() {
    assert!(Matcher::not_equal("job", "web").matches(&api_series()));
}

#[test]
fn not_equal_matcher_refuses_the_value_the_series_has() {
    assert!(!Matcher::not_equal("job", "api").matches(&api_series()));
}

#[test]
fn not_equal_matcher_accepts_a_series_without_the_label() {
    assert!(Matcher::not_equal("zone", "eu").matches(&api_series()));
}

#[test]
fn not_equal_matcher_for_the_empty_value_refuses_a_series_without_the_label() {
    assert!(!Matcher::not_equal("zone", "").matches(&api_series()));
}

#[test]
fn selector_without_matchers_selects_every_series() {
    assert!(Selector::all().matches(&api_series()));
}

#[test]
fn selector_selects_a_series_that_meets_every_matcher() {
    let selector = Selector::new(vec![Matcher::equal("__name__", "up"), Matcher::not_equal("job", "web")]);

    assert!(selector.matches(&api_series()));
}

#[test]
fn selector_refuses_a_series_that_fails_one_matcher() {
    let selector = Selector::new(vec![Matcher::equal("__name__", "up"), Matcher::equal("job", "web")]);

    assert!(!selector.matches(&api_series()));
}

#[test]
fn postings_can_answer_matchers_that_require_a_value() {
    let selector = Selector::new(vec![
        Matcher::equal("job", "api"),
        Matcher::not_equal("zone", "eu"),
        Matcher::equal("__name__", "up"),
    ]);

    let required_labels = selector.required_labels();

    assert_eq!(
        required_labels,
        [
            Label {
                name: "job".to_owned(),
                value: "api".to_owned()
            },
            Label {
                name: "__name__".to_owned(),
                value: "up".to_owned()
            },
        ]
    );
}

#[test]
fn postings_cannot_answer_a_matcher_for_the_empty_value() {
    let selector = Selector::new(vec![Matcher::equal("zone", "")]);

    assert_eq!(selector.required_labels(), Vec::new());
}
