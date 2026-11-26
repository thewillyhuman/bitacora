//! Tests of the wall clock that follows the runtime's clock.
//!
//! # What is covered
//!
//! - A new clock shows its documented starting moment.
//! - The clock advances by exactly what the runtime's clock advances.
//!
//! # How the fixtures work
//!
//! Every test runs under a paused runtime, where time only moves when the test advances it. There are no other
//! fixtures.

use std::time::Duration;

use super::*;

/// How far the test moves the runtime's clock.
const ADVANCE: Duration = Duration::from_secs(90);

#[tokio::test(start_paused = true)]
async fn clock_starts_at_its_documented_moment() {
    let clock = VirtualClock::new();

    assert_eq!(clock.now(), VirtualClock::START);
}

#[tokio::test(start_paused = true)]
async fn clock_advances_with_tokio_time() {
    let clock = VirtualClock::new();

    tokio::time::advance(ADVANCE).await;

    assert_eq!(clock.now(), VirtualClock::START.plus(ADVANCE));
}

#[tokio::test(start_paused = true)]
async fn default_clock_starts_at_its_documented_moment() {
    let clock = VirtualClock::default();

    assert_eq!(clock.now(), VirtualClock::START);
}
