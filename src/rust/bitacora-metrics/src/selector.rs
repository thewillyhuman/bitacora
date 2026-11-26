//! Selecting series by their labels.
//!
//! # Why this file exists
//!
//! A query does not name series one by one. It says "every series whose `job` is `api`", and the store
//! finds them: in the head, by looking at each series it holds, and in a block, through the block's
//! postings. Both must agree on what a selector means, to the last corner case, or a sample would be
//! returned or not depending on whether it had been sealed yet. This file is that one meaning.
//!
//! # What it contains
//!
//! - [`Matcher`]: one condition on one label.
//! - [`Selector`]: the conditions a series must all meet.
//!
//! # How it works
//!
//! A matcher compares the value a series has for one label with a text. A series that does not have the
//! label is treated as having the empty value for it, as in Prometheus: `Equal { name: "zone", value: "" }`
//! matches exactly the series without a `zone` label, and `NotEqual { name: "zone", value: "eu" }` matches
//! series without one too. A selector without matchers matches every series.
//!
//! Matchers that require one particular, non-empty value can be answered from a block's postings; the
//! selector says which those are with `required_labels`. Whatever the postings leave is checked against
//! every matcher, so the postings are only ever a way to look at fewer series.
//!
//! Regular expressions are not supported. They would need a regular expression engine, and the two
//! matchers here are enough to show what a block must offer a query.
//!
//! # Invariants
//!
//! - `matches` is a pure function of the selector and the labels.
//! - Every series that `matches` accepts has every label of `required_labels`.
//!
//! # See also
//!
//! - `block.rs`: where the postings narrow the series of a block.
//! - `head.rs`: where the series in memory are matched one by one.

use crate::sample::{Label, Labels};

/// One condition on one label of a series.
///
/// A series without the label counts as having the empty value for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Matcher {
    /// The series' value for the label is exactly this text.
    Equal {
        /// The label's name.
        name: String,
        /// The value the series must have. Empty: the series must not have the label.
        value: String,
    },
    /// The series' value for the label is anything but this text.
    NotEqual {
        /// The label's name.
        name: String,
        /// The value the series must not have. Empty: the series must have the label.
        value: String,
    },
}

impl Matcher {
    /// A matcher for series whose label `name` has the value `value`.
    #[must_use]
    pub fn equal(name: &str, value: &str) -> Self {
        Self::Equal {
            name: name.to_owned(),
            value: value.to_owned(),
        }
    }

    /// A matcher for series whose label `name` does not have the value `value`.
    #[must_use]
    pub fn not_equal(name: &str, value: &str) -> Self {
        Self::NotEqual {
            name: name.to_owned(),
            value: value.to_owned(),
        }
    }

    /// Whether a series with these labels meets the condition.
    #[must_use]
    pub fn matches(&self, labels: &Labels) -> bool {
        match self {
            Self::Equal { name, value } => labels.value_of(name) == value,
            Self::NotEqual { name, value } => labels.value_of(name) != value,
        }
    }
}

/// The conditions a series must all meet to be part of a query's answer.
///
/// # Examples
///
/// ```
/// use bitacora_metrics::{Labels, Matcher, Selector};
///
/// let api_requests = Selector::new(vec![
///     Matcher::equal("__name__", "requests_total"),
///     Matcher::not_equal("job", "batch"),
/// ]);
/// let from_the_api = Labels::from_pairs(&[("__name__", "requests_total"), ("job", "api")])?;
/// let from_a_batch = Labels::from_pairs(&[("__name__", "requests_total"), ("job", "batch")])?;
///
/// assert!(api_requests.matches(&from_the_api));
/// assert!(!api_requests.matches(&from_a_batch));
/// assert!(Selector::all().matches(&from_a_batch));
/// # Ok::<(), bitacora_metrics::InvalidLabels>(())
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Selector {
    /// The conditions. A series must meet every one.
    matchers: Vec<Matcher>,
}

impl Selector {
    /// A selector for the series that meet every one of `matchers`.
    #[must_use]
    pub const fn new(matchers: Vec<Matcher>) -> Self {
        Self { matchers }
    }

    /// A selector that matches every series.
    #[must_use]
    pub const fn all() -> Self {
        Self { matchers: Vec::new() }
    }

    /// Whether a series with these labels is selected.
    #[must_use]
    pub fn matches(&self, labels: &Labels) -> bool {
        for matcher in &self.matchers {
            if !matcher.matches(labels) {
                return false;
            }
        }
        true
    }

    /// The labels every selected series is certain to have: one for each matcher that requires a
    /// particular, non-empty value.
    ///
    /// A block looks these up in its postings to avoid examining every series it holds.
    pub(crate) fn required_labels(&self) -> Vec<Label> {
        let mut required_labels = Vec::new();
        for matcher in &self.matchers {
            if let Matcher::Equal { name, value } = matcher
                && !value.is_empty()
            {
                required_labels.push(Label {
                    name: name.clone(),
                    value: value.clone(),
                });
            }
        }
        required_labels
    }
}

#[cfg(test)]
#[path = "selector_test.rs"]
mod tests;
