//! Durations as an operator types them.
//!
//! # Why this file exists
//!
//! Several options take a length of time: how long sealed segments are kept, how long to wait for a
//! replica, how long a drain may take. Typing them in seconds invites mistakes (`604800` is a week), so
//! the tool accepts a number with a unit. This file is the one place that reads that notation.
//!
//! # What it contains
//!
//! - [`parse_duration`]: reads `250ms`, `30s`, `15m`, `24h` or `7d`.
//! - [`InvalidDuration`]: why a text is not a duration.
//!
//! # How it works
//!
//! The text is split where its digits end. The digits must form a whole number and the rest must be one
//! of the five units. There are no fractions and no combined forms such as `1h30m`: one number, one unit.
//!
//! # Invariants
//!
//! - Pure: no clock is read.
//! - A value that does not fit in 64 bits of milliseconds is refused instead of being truncated.
//!
//! # See also
//!
//! - `arguments.rs`: the options that use this parser.

use std::time::Duration;

use thiserror::Error;

/// How many milliseconds one second has.
const MILLISECONDS_PER_SECOND: u64 = 1_000;
/// How many milliseconds one minute has.
const MILLISECONDS_PER_MINUTE: u64 = 60 * MILLISECONDS_PER_SECOND;
/// How many milliseconds one hour has.
const MILLISECONDS_PER_HOUR: u64 = 60 * MILLISECONDS_PER_MINUTE;
/// How many milliseconds one day has.
const MILLISECONDS_PER_DAY: u64 = 24 * MILLISECONDS_PER_HOUR;

/// Why a text is not a duration.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub(crate) enum InvalidDuration {
    /// The text does not start with a whole number.
    #[error("duration {text:?} must start with a whole number, as in 30s")]
    MissingNumber {
        /// The text that was given.
        text: String,
    },
    /// The number is not followed by a known unit.
    #[error("duration {text:?} needs one of the units ms, s, m, h or d, as in 30s")]
    UnknownUnit {
        /// The text that was given.
        text: String,
    },
    /// The duration is longer than this tool can represent.
    #[error("duration {text:?} is too long")]
    TooLong {
        /// The text that was given.
        text: String,
    },
}

/// Reads a duration written as a whole number and a unit.
///
/// # Arguments
///
/// * `text` - The duration, for example `250ms`, `30s`, `15m`, `24h` or `7d`. No spaces, no fractions.
///
/// # Returns
///
/// The duration.
///
/// # Errors
///
/// * [`InvalidDuration::MissingNumber`] if the text does not start with digits.
/// * [`InvalidDuration::UnknownUnit`] if the digits are not followed by `ms`, `s`, `m`, `h` or `d`.
/// * [`InvalidDuration::TooLong`] if the value does not fit in 64 bits of milliseconds.
pub(crate) fn parse_duration(text: &str) -> Result<Duration, InvalidDuration> {
    let digits_end = text
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(text.len());
    let (digits, unit) = text.split_at(digits_end);
    let Ok(number) = digits.parse::<u64>() else {
        // Empty digits, or more digits than fit in 64 bits: the second case is reported as too long.
        if digits.is_empty() {
            return Err(InvalidDuration::MissingNumber { text: text.to_owned() });
        }
        return Err(InvalidDuration::TooLong { text: text.to_owned() });
    };
    let milliseconds_per_unit = match unit {
        "ms" => 1,
        "s" => MILLISECONDS_PER_SECOND,
        "m" => MILLISECONDS_PER_MINUTE,
        "h" => MILLISECONDS_PER_HOUR,
        "d" => MILLISECONDS_PER_DAY,
        _unknown_unit => {
            return Err(InvalidDuration::UnknownUnit { text: text.to_owned() });
        }
    };
    match number.checked_mul(milliseconds_per_unit) {
        Some(milliseconds) => Ok(Duration::from_millis(milliseconds)),
        None => Err(InvalidDuration::TooLong { text: text.to_owned() }),
    }
}

#[cfg(test)]
#[path = "duration_test.rs"]
mod tests;
