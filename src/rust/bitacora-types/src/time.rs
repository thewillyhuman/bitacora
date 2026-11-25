//! Wall-clock time as an injected interface.
//!
//! # Why this file exists
//!
//! A few values in bitacora are points in real time that outlive a process: when a writer's lease lapses,
//! when a replica last renewed its registration, when a segment was sealed. They are written to the
//! metadata store by one process and compared with "now" by another. If code read the system clock for
//! "now" directly, the deterministic simulator could not control it and a test of lease expiry would have
//! to wait in real time (design, section 15). So "now" is an interface, like the disk and the network, and
//! the simulator and the tests pass a clock they move by hand.
//!
//! # What it contains
//!
//! - [`Timestamp`]: a point in wall-clock time.
//! - [`Clock`]: the source of the current time.
//! - [`SystemClock`]: the operating system's clock, for real processes.
//! - [`ManualClock`]: a clock that only moves when told to, for tests.
//!
//! # How it works
//!
//! A [`Timestamp`] is milliseconds since the Unix epoch in a `u64`, and it serializes as that plain
//! number. Milliseconds are enough: the shortest thing measured in wall-clock time is a lease of seconds.
//! Arithmetic saturates instead of overflowing, because "far future" and "no time has passed" are the safe
//! readings of an out-of-range value.
//!
//! Timers do not use this clock. Waiting and timeouts go through `tokio::time`, which a paused runtime
//! makes virtual. This clock is only for values that are stored and compared across processes.
//!
//! # Invariants
//!
//! - No safety property depends on wall-clock time (design, section 8). Clocks that disagree can delay a
//!   takeover or trigger an unnecessary repair; they cannot lose or duplicate a record, because fencing
//!   compares epochs, not times.
//! - [`SystemClock`] is the only place in the workspace that reads the system clock. The workspace's lints
//!   forbid it everywhere else.
//! - The serialized form of a [`Timestamp`] is part of the metadata schema.
//!
//! # See also
//!
//! - `random.rs`: the injected random source, the other thing a deterministic run must control.
//! - `bitacora-testkit/src/clock.rs`: the clock tied to a simulated run's virtual time.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// A point in wall-clock time, in milliseconds since the Unix epoch.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use bitacora_types::Timestamp;
///
/// let lease_granted_at = Timestamp::from_unix_millis(1_700_000_000_000);
/// let lease_expires_at = lease_granted_at.plus(Duration::from_secs(10));
///
/// assert_eq!(lease_expires_at.duration_since(lease_granted_at), Duration::from_secs(10));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(u64);

impl Timestamp {
    /// The Unix epoch. Also the natural "never happened" for a time that is compared with now.
    pub const UNIX_EPOCH: Self = Self(0);

    /// Builds a timestamp from milliseconds since the Unix epoch.
    ///
    /// # Arguments
    ///
    /// * `unix_millis` - Milliseconds since 1970-01-01 00:00:00 UTC. Any value is accepted.
    ///
    /// # Returns
    ///
    /// That point in time.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::Timestamp;
    ///
    /// assert_eq!(Timestamp::from_unix_millis(0), Timestamp::UNIX_EPOCH);
    /// ```
    #[must_use]
    pub const fn from_unix_millis(unix_millis: u64) -> Self {
        Self(unix_millis)
    }

    /// Returns the milliseconds since the Unix epoch, for writing the timestamp to a record.
    ///
    /// # Returns
    ///
    /// The number given to [`Timestamp::from_unix_millis`].
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::Timestamp;
    ///
    /// assert_eq!(Timestamp::from_unix_millis(1_500).unix_millis(), 1_500);
    /// ```
    #[must_use]
    pub const fn unix_millis(self) -> u64 {
        self.0
    }

    /// Returns the timestamp a duration later.
    ///
    /// # Arguments
    ///
    /// * `duration` - How far to move forward. Fractions of a millisecond are dropped.
    ///
    /// # Returns
    ///
    /// The later timestamp, or the farthest representable one if the sum does not fit.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use bitacora_types::Timestamp;
    ///
    /// let start = Timestamp::from_unix_millis(1_000);
    /// assert_eq!(start.plus(Duration::from_secs(10)), Timestamp::from_unix_millis(11_000));
    /// ```
    #[must_use]
    pub fn plus(self, duration: Duration) -> Self {
        let added_millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        Self(self.0.saturating_add(added_millis))
    }

    /// Returns how long after an earlier timestamp this one lies.
    ///
    /// # Arguments
    ///
    /// * `earlier` - The timestamp to measure from.
    ///
    /// # Returns
    ///
    /// The time between the two, or zero if `earlier` is not before this timestamp. Clocks of different
    /// machines disagree a little, and "no time has passed" is the safe reading of a negative distance.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use bitacora_types::Timestamp;
    ///
    /// let heartbeat_at = Timestamp::from_unix_millis(1_000);
    /// let now = Timestamp::from_unix_millis(4_500);
    ///
    /// assert_eq!(now.duration_since(heartbeat_at), Duration::from_millis(3_500));
    /// assert_eq!(heartbeat_at.duration_since(now), Duration::ZERO);
    /// ```
    #[must_use]
    pub const fn duration_since(self, earlier: Self) -> Duration {
        Duration::from_millis(self.0.saturating_sub(earlier.0))
    }
}

impl fmt::Display for Timestamp {
    /// Writes the timestamp as its milliseconds followed by `ms`, for log lines.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}ms", self.0)
    }
}

/// A source of wall-clock time.
///
/// Code that stores or compares a point in real time takes an `Arc<dyn Clock>` and never reads the system
/// clock itself, so that tests and the simulator decide what time it is.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use bitacora_types::{Clock, ManualClock, Timestamp};
///
/// fn lease_has_lapsed(clock: &dyn Clock, lease_expires_at: Timestamp) -> bool {
///     clock.now() >= lease_expires_at
/// }
///
/// let clock = ManualClock::starting_at(Timestamp::from_unix_millis(1_000));
/// let lease_expires_at = Timestamp::from_unix_millis(4_000);
/// assert!(!lease_has_lapsed(&clock, lease_expires_at));
///
/// clock.advance(Duration::from_secs(3));
/// assert!(lease_has_lapsed(&clock, lease_expires_at));
/// ```
pub trait Clock: Send + Sync {
    /// Returns the current time.
    ///
    /// # Returns
    ///
    /// What this clock says it is now. Two calls may return the same value, and a real clock may even go
    /// back; callers must not assume it only moves forward.
    fn now(&self) -> Timestamp;
}

/// The operating system's clock.
///
/// # Examples
///
/// ```
/// use bitacora_types::{Clock, SystemClock, Timestamp};
///
/// let start_of_2020 = Timestamp::from_unix_millis(1_577_836_800_000);
/// assert!(SystemClock.now() > start_of_2020);
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    /// Reads the system's real-time clock. A clock set before 1970 reads as the Unix epoch.
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
///
/// It can be shared between the code under test and the test that moves it: moving it takes `&self`.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use bitacora_types::{Clock, ManualClock, Timestamp};
///
/// let clock = ManualClock::starting_at(Timestamp::from_unix_millis(1_000));
/// clock.advance(Duration::from_secs(3));
///
/// assert_eq!(clock.now(), Timestamp::from_unix_millis(4_000));
/// ```
#[derive(Debug, Default)]
pub struct ManualClock {
    /// What the clock shows, in milliseconds since the Unix epoch. Atomic so that the clock can be moved
    /// through a shared reference.
    unix_millis: AtomicU64,
}

impl ManualClock {
    /// Creates a clock that shows a given time until it is moved.
    ///
    /// # Arguments
    ///
    /// * `now` - The time the clock shows at first.
    ///
    /// # Returns
    ///
    /// The clock, standing still at `now`.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::{Clock, ManualClock, Timestamp};
    ///
    /// let clock = ManualClock::starting_at(Timestamp::from_unix_millis(1_000));
    /// assert_eq!(clock.now(), Timestamp::from_unix_millis(1_000));
    /// ```
    #[must_use]
    pub const fn starting_at(now: Timestamp) -> Self {
        Self {
            unix_millis: AtomicU64::new(now.unix_millis()),
        }
    }

    /// Moves the clock forward.
    ///
    /// Meant to be called from one place, the test. Two calls at the same moment from two threads could
    /// lose one of the two moves.
    ///
    /// # Arguments
    ///
    /// * `duration` - How far to move. Fractions of a millisecond are dropped.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use bitacora_types::{Clock, ManualClock, Timestamp};
    ///
    /// let clock = ManualClock::starting_at(Timestamp::UNIX_EPOCH);
    /// clock.advance(Duration::from_millis(250));
    ///
    /// assert_eq!(clock.now(), Timestamp::from_unix_millis(250));
    /// ```
    pub fn advance(&self, duration: Duration) {
        let moved = self.now().plus(duration);
        self.unix_millis.store(moved.unix_millis(), Ordering::Relaxed);
    }
}

impl Clock for ManualClock {
    /// Returns the time the clock was last moved to.
    fn now(&self) -> Timestamp {
        Timestamp::from_unix_millis(self.unix_millis.load(Ordering::Relaxed))
    }
}

#[cfg(test)]
#[path = "time_test.rs"]
mod tests;
