//! Tests of pacing.
//!
//! # What is covered
//!
//! The schedule of single records and of batches, rates that do not divide a second evenly, send numbers
//! large enough to overflow naive arithmetic, and the zero values a command line could pass.

use super::*;

#[test]
fn first_send_is_due_at_the_start() {
    let pace = Pace::new(1_000, 1);

    assert_eq!(pace.due_after(0), Duration::ZERO);
}

#[test]
fn single_records_at_a_thousand_per_second_are_a_millisecond_apart() {
    let pace = Pace::new(1_000, 1);

    assert_eq!(pace.due_after(7), Duration::from_millis(7));
}

#[test]
fn batches_are_spaced_by_the_records_they_carry() {
    let batches_of_ten = Pace::new(1_000, 10);

    assert_eq!(batches_of_ten.due_after(3), Duration::from_millis(30));
}

#[test]
fn rate_that_does_not_divide_a_second_does_not_drift() {
    let three_per_second = Pace::new(3, 1);

    // Three sends later exactly one second has passed, although a third of a second is not a whole
    // number of nanoseconds.
    assert_eq!(three_per_second.due_after(3), Duration::from_secs(1));
    assert_eq!(three_per_second.due_after(3_000_000), Duration::from_secs(1_000_000));
}

#[test]
fn offset_within_an_uneven_interval_is_rounded_down() {
    let three_per_second = Pace::new(3, 1);

    assert_eq!(three_per_second.due_after(1), Duration::from_nanos(333_333_333));
}

#[test]
fn send_number_too_large_for_naive_arithmetic_is_scheduled() {
    let slow = Pace::new(1, 1);

    assert_eq!(slow.due_after(10_000_000_000), Duration::from_secs(10_000_000_000));
}

#[test]
fn offset_beyond_what_a_duration_holds_is_the_longest_duration() {
    let slow_and_large = Pace::new(1, u64::MAX);

    assert_eq!(slow_and_large.due_after(u64::MAX), Duration::MAX);
}

#[test]
fn rate_of_zero_is_read_as_one_record_per_second() {
    let pace = Pace::new(0, 1);

    assert_eq!(pace.due_after(2), Duration::from_secs(2));
}

#[test]
fn send_of_zero_records_is_read_as_one_record() {
    let pace = Pace::new(1, 0);

    assert_eq!(pace.due_after(2), Duration::from_secs(2));
}
