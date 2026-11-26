//! Wall-clock time that follows the runtime's clock.
//!
//! # Why this file exists
//!
//! Bitacora uses two kinds of time. Timers (`tokio::time`) decide when something happens: a heartbeat is sent, a
//! request times out. Wall-clock time, read through the `Clock` interface, is what gets written down: the moment of a
//! heartbeat in the registry, the expiry of a writer's lease. The two must agree, or a test that advances its timers by
//! a minute would find leases that never expire, because the wall clock did not move.
//!
//! The system clock cannot be used: it moves on its own, differently on every run, and a paused test runtime would
//! leave it behind in an instant. This file gives the cluster a wall clock that is derived from the runtime's clock, so
//! both kinds of time are one.
//!
//! # What it contains
//!
//! - [`VirtualClock`]: a `Clock` that shows a fixed starting moment plus the runtime time elapsed since it was created.
//!
//! # How it works
//!
//! The clock remembers the runtime instant at which it was created. Reading it adds the time elapsed since then to
//! [`VirtualClock::START`]. Under a paused runtime the elapsed time only grows when the test advances time or when
//! every task is waiting on a timer, so the clock is as deterministic as the runtime.
//!
//! # Invariants
//!
//! - The clock never goes back, and it advances by exactly what the runtime's clock advances.
//! - A clock created at the start of a test shows [`VirtualClock::START`] then, on every run.
//!
//! # See also
//!
//! - `cluster.rs`: the cluster owns one clock and hands it out as [`Clock`].
//! - `bitacora-types`: the `Clock` interface and `Timestamp`.

use bitacora_types::{Clock, Timestamp};
use tokio::time::Instant;

/// A [`Clock`] whose wall-clock time advances exactly as the runtime's time does.
///
/// Under a paused runtime it only moves when the runtime's virtual time moves, so lease expiries and heartbeats written
/// to the metadata store stay in step with the timers that act on them. Create it inside the runtime it is to follow.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() {
/// use std::time::Duration;
///
/// use bitacora_testkit::VirtualClock;
/// use bitacora_types::Clock;
///
/// let clock = VirtualClock::new();
///
/// tokio::time::advance(Duration::from_secs(30)).await;
///
/// assert_eq!(clock.now(), VirtualClock::START.plus(Duration::from_secs(30)));
/// # }
/// ```
#[derive(Debug)]
pub struct VirtualClock {
    /// The runtime instant at which the clock showed [`VirtualClock::START`].
    started_at: Instant,
}

impl VirtualClock {
    /// What the clock shows when it is created: an arbitrary moment well after the Unix epoch, so that "never"
    /// (timestamp zero) and "now" cannot be confused.
    pub const START: Timestamp = Timestamp::from_unix_millis(1_000_000);

    /// Creates a clock that shows [`VirtualClock::START`] now.
    ///
    /// Reads the runtime's clock once and remembers the instant.
    ///
    /// # Returns
    ///
    /// The clock. It shows [`VirtualClock::START`] until the runtime's time moves.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_testkit::VirtualClock;
    /// use bitacora_types::Clock;
    ///
    /// let clock = VirtualClock::new();
    ///
    /// assert_eq!(clock.now(), VirtualClock::START);
    /// # }
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
        }
    }
}

impl Default for VirtualClock {
    /// Creates a clock that shows [`VirtualClock::START`] now, as [`VirtualClock::new`] does.
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for VirtualClock {
    /// Shows the starting moment plus the runtime time elapsed since the clock was created.
    fn now(&self) -> Timestamp {
        Self::START.plus(self.started_at.elapsed())
    }
}

#[cfg(test)]
#[path = "clock_test.rs"]
mod tests;
