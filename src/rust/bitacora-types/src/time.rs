//! Wall-clock time as an injected interface (design, section 15).
//!
//! Timers use `tokio::time`, which is virtual under simulation. Wall-clock
//! time is needed only for values that outlive a process: lease expiries,
//! heartbeats and seal times in the metadata store. Those come from a
//! [`Clock`], and no safety property depends on them (design, section 8).

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// A point in wall-clock time, in milliseconds since the Unix epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(u64);

impl Timestamp {
    /// The Unix epoch. Also the natural "never" for an expiry.
    pub const UNIX_EPOCH: Self = Self(0);

    /// Builds a timestamp from milliseconds since the Unix epoch.
    #[must_use]
    pub const fn from_unix_millis(unix_millis: u64) -> Self {
        Self(unix_millis)
    }

    /// Returns the milliseconds since the Unix epoch.
    #[must_use]
    pub const fn unix_millis(self) -> u64 {
        self.0
    }

    /// Returns the timestamp `duration` later, saturating at the far future.
    #[must_use]
    pub fn plus(self, duration: Duration) -> Self {
        let added_millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        Self(self.0.saturating_add(added_millis))
    }

    /// Returns how long after `earlier` this timestamp lies, or zero if it
    /// does not lie after it.
    #[must_use]
    pub const fn duration_since(self, earlier: Self) -> Duration {
        Duration::from_millis(self.0.saturating_sub(earlier.0))
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}ms", self.0)
    }
}

/// A source of wall-clock time.
pub trait Clock: Send + Sync {
    /// Returns the current time.
    fn now(&self) -> Timestamp;
}

/// The operating system's clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    // This is the one adapter allowed to read the system clock.
    #[allow(clippy::disallowed_methods)]
    fn now(&self) -> Timestamp {
        let since_unix_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        Timestamp::UNIX_EPOCH.plus(since_unix_epoch)
    }
}

/// A clock that only moves when told to, for tests.
#[derive(Debug, Default)]
pub struct ManualClock {
    unix_millis: AtomicU64,
}

impl ManualClock {
    /// Creates a clock that shows `now` until it is moved.
    #[must_use]
    pub const fn starting_at(now: Timestamp) -> Self {
        Self {
            unix_millis: AtomicU64::new(now.unix_millis()),
        }
    }

    /// Moves the clock forward by `duration`.
    pub fn advance(&self, duration: Duration) {
        let moved = self.now().plus(duration);
        self.unix_millis.store(moved.unix_millis(), Ordering::Relaxed);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_unix_millis(self.unix_millis.load(Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plus_moves_a_timestamp_forward() {
        let start = Timestamp::from_unix_millis(1_000);
        assert_eq!(start.plus(Duration::from_secs(10)), Timestamp::from_unix_millis(11_000));
    }

    #[test]
    fn plus_saturates_instead_of_overflowing() {
        let far_future = Timestamp::from_unix_millis(u64::MAX - 1);
        assert_eq!(far_future.plus(Duration::MAX), Timestamp::from_unix_millis(u64::MAX));
    }

    #[test]
    fn duration_since_an_earlier_timestamp_is_their_distance() {
        let earlier = Timestamp::from_unix_millis(1_000);
        let later = Timestamp::from_unix_millis(4_500);
        assert_eq!(later.duration_since(earlier), Duration::from_millis(3_500));
    }

    #[test]
    fn duration_since_a_later_timestamp_is_zero() {
        let earlier = Timestamp::from_unix_millis(1_000);
        let later = Timestamp::from_unix_millis(4_500);
        assert_eq!(earlier.duration_since(later), Duration::ZERO);
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
    fn system_clock_is_past_the_year_2020() {
        let start_of_2020 = Timestamp::from_unix_millis(1_577_836_800_000);
        assert!(SystemClock.now() > start_of_2020);
    }

    #[test]
    fn timestamps_serialize_as_plain_numbers() {
        let serialized = serde_json::to_string(&Timestamp::from_unix_millis(1_500)).expect("serializable");
        assert_eq!(serialized, "1500");
    }
}
