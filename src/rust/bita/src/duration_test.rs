//! Tests of the duration notation.
//!
//! # What is covered
//!
//! Every unit, and every way a text can fail to be a duration: no number, no unit, an unknown unit, a
//! fraction, and a value too large to represent.

use super::*;

#[test]
fn milliseconds_are_read() {
    assert_eq!(parse_duration("250ms"), Ok(Duration::from_millis(250)));
}

#[test]
fn seconds_are_read() {
    assert_eq!(parse_duration("30s"), Ok(Duration::from_secs(30)));
}

#[test]
fn minutes_are_read() {
    assert_eq!(parse_duration("15m"), Ok(Duration::from_mins(15)));
}

#[test]
fn hours_are_read() {
    assert_eq!(parse_duration("24h"), Ok(Duration::from_hours(24)));
}

#[test]
fn days_are_read() {
    assert_eq!(parse_duration("7d"), Ok(Duration::from_hours(7 * 24)));
}

#[test]
fn zero_is_a_duration() {
    assert_eq!(parse_duration("0s"), Ok(Duration::ZERO));
}

#[test]
fn text_without_a_number_is_refused() {
    assert_eq!(
        parse_duration("soon"),
        Err(InvalidDuration::MissingNumber {
            text: "soon".to_owned()
        })
    );
}

#[test]
fn empty_text_is_refused() {
    assert_eq!(
        parse_duration(""),
        Err(InvalidDuration::MissingNumber { text: String::new() })
    );
}

#[test]
fn number_without_a_unit_is_refused() {
    assert_eq!(
        parse_duration("30"),
        Err(InvalidDuration::UnknownUnit { text: "30".to_owned() })
    );
}

#[test]
fn unknown_unit_is_refused() {
    assert_eq!(
        parse_duration("3weeks"),
        Err(InvalidDuration::UnknownUnit {
            text: "3weeks".to_owned()
        })
    );
}

#[test]
fn fraction_is_refused() {
    assert_eq!(
        parse_duration("1.5h"),
        Err(InvalidDuration::UnknownUnit {
            text: "1.5h".to_owned()
        })
    );
}

#[test]
fn value_that_overflows_milliseconds_is_refused() {
    assert_eq!(
        parse_duration("18446744073709551615d"),
        Err(InvalidDuration::TooLong {
            text: "18446744073709551615d".to_owned()
        })
    );
}

#[test]
fn number_with_more_digits_than_fit_is_refused() {
    assert_eq!(
        parse_duration("99999999999999999999999s"),
        Err(InvalidDuration::TooLong {
            text: "99999999999999999999999s".to_owned()
        })
    );
}
