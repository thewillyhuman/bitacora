//! Wall-clock time that follows Tokio's clock.

use bitacora_types::{Clock, Timestamp};
use tokio::time::Instant;

/// A [`Clock`] whose wall-clock time advances exactly as Tokio's time does.
///
/// Under a paused runtime it only moves when Tokio's virtual time moves, so
/// lease expiries and heartbeats written to the metadata store stay in step
/// with the timers that act on them.
#[derive(Debug)]
pub struct VirtualClock {
    started_at: Instant,
}

impl VirtualClock {
    /// What the clock shows when it is created: an arbitrary moment well
    /// after the Unix epoch, so that "never" and "now" cannot be confused.
    pub const START: Timestamp = Timestamp::from_unix_millis(1_000_000);

    /// Creates a clock that shows [`VirtualClock::START`] now.
    #[must_use]
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
        }
    }
}

impl Default for VirtualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for VirtualClock {
    fn now(&self) -> Timestamp {
        Self::START.plus(self.started_at.elapsed())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn clock_starts_at_its_documented_moment() {
        assert_eq!(VirtualClock::new().now(), VirtualClock::START);
    }

    #[tokio::test(start_paused = true)]
    async fn clock_advances_with_tokio_time() {
        let clock = VirtualClock::new();
        tokio::time::advance(Duration::from_secs(90)).await;
        assert_eq!(
            clock.now(),
            VirtualClock::START.plus(Duration::from_secs(90))
        );
    }
}
