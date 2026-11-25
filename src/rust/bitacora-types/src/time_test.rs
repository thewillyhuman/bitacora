//! Tests of timestamps and clocks.
//!
//! # What is covered
//!
//! - Timestamp arithmetic: moving forward, saturating at the far future, and distances in both directions.
//! - The manual clock: it stands still, and moves by exactly what it is told.
//! - The system clock: it reads a plausible present.
//! - The displayed and serialized forms of a timestamp.
//!
//! # How the fixtures work
//!
//! There are none: every test builds the timestamps it needs from small numbers of milliseconds.

use super::*;

#[test]
fn plus_moves_a_timestamp_forward() {
    let start = Timestamp::from_unix_millis(1_000);

    let later = start.plus(Duration::from_secs(10));

    assert_eq!(later, Timestamp::from_unix_millis(11_000));
}

#[test]
fn plus_saturates_instead_of_overflowing() {
    let far_future = Timestamp::from_unix_millis(u64::MAX - 1);

    let later = far_future.plus(Duration::MAX);

    assert_eq!(later, Timestamp::from_unix_millis(u64::MAX));
}

#[test]
fn duration_since_an_earlier_timestamp_is_their_distance() {
    let earlier = Timestamp::from_unix_millis(1_000);
    let later = Timestamp::from_unix_millis(4_500);

    let distance = later.duration_since(earlier);

    assert_eq!(distance, Duration::from_millis(3_500));
}

#[test]
fn duration_since_a_later_timestamp_is_zero() {
    let earlier = Timestamp::from_unix_millis(1_000);
    let later = Timestamp::from_unix_millis(4_500);

    let distance = earlier.duration_since(later);

    assert_eq!(distance, Duration::ZERO);
}

#[test]
fn manual_clock_shows_the_time_it_was_started_at() {
    let clock = ManualClock::starting_at(Timestamp::from_unix_millis(1_000));

    assert_eq!(clock.now(), Timestamp::from_unix_millis(1_000));
}

#[test]
fn manual_clock_stands_still_until_advanced() {
    let clock = ManualClock::starting_at(Timestamp::from_unix_millis(1_000));

    assert_eq!(clock.now(), clock.now());
}

#[test]
fn manual_clock_advances_by_the_given_duration() {
    let clock = ManualClock::starting_at(Timestamp::from_unix_millis(1_000));

    clock.advance(Duration::from_secs(3));

    assert_eq!(clock.now(), Timestamp::from_unix_millis(4_000));
}

#[test]
fn manual_clock_without_a_start_is_at_the_unix_epoch() {
    let clock = ManualClock::default();

    assert_eq!(clock.now(), Timestamp::UNIX_EPOCH);
}

#[test]
fn system_clock_is_past_the_year_2020() {
    let start_of_2020 = Timestamp::from_unix_millis(1_577_836_800_000);

    assert!(SystemClock.now() > start_of_2020);
}

#[test]
fn timestamp_displays_as_milliseconds() {
    assert_eq!(Timestamp::from_unix_millis(1_500).to_string(), "1500ms");
}

#[test]
fn timestamps_serialize_as_plain_numbers() {
    let serialized = serde_json::to_string(&Timestamp::from_unix_millis(1_500)).expect("a number serializes");

    assert_eq!(serialized, "1500");
}
