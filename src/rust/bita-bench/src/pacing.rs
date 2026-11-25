//! Pacing a workload to a target rate.
//!
//! # Why this file exists
//!
//! A latency number means little without the load it was measured under. To measure latency at, say, half
//! of what a cluster can take, the benchmark has to send at a chosen rate instead of as fast as it can.
//! This file decides when each append of a paced workload is due.
//!
//! # What it contains
//!
//! - [`Pace`]: a target rate, and the moment each send is due.
//!
//! # How it works
//!
//! Sends are numbered from zero across all workers of a workload. Send `n` is due `n` times the interval
//! between two sends after the workload started, where the interval is the records of one send divided by
//! the target rate. A worker takes the next number, sleeps until that send is due, and sends. If the
//! cluster falls behind, sends become due in the past and go out at once, so the workload catches up
//! instead of silently lowering its rate.
//!
//! The schedule is a pure function of the send's number, which is what makes it testable and what keeps
//! workers from drifting apart.
//!
//! # Invariants
//!
//! - The moment a send is due never depends on when earlier sends actually went out.
//! - No rounding accumulates: the offset is computed from the send's number, not by adding intervals.
//!
//! # See also
//!
//! - `workload.rs`: the workers that follow the schedule, and why latency is measured from the moment a
//!   send was due.

use std::time::Duration;

/// How many nanoseconds one second has.
const NANOSECONDS_PER_SECOND: u128 = 1_000_000_000;

/// A target rate for a workload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Pace {
    /// How many records per second the workload is to send. Never zero.
    records_per_second: u64,
    /// How many records one send carries. Never zero.
    records_per_send: u64,
}

impl Pace {
    /// Creates a pace.
    ///
    /// # Arguments
    ///
    /// * `records_per_second` - The target rate of the whole workload. Zero is read as one.
    /// * `records_per_send` - How many records each send carries. Zero is read as one.
    pub(crate) fn new(records_per_second: u64, records_per_send: u64) -> Self {
        Self {
            records_per_second: records_per_second.max(1),
            records_per_send: records_per_send.max(1),
        }
    }

    /// How long after the start of the workload the send with this number is due.
    ///
    /// # Arguments
    ///
    /// * `send_number` - The send's number, counted from zero across all workers.
    ///
    /// # Returns
    ///
    /// The offset from the workload's start, to the nanosecond. An offset beyond what a duration can hold
    /// is returned as the longest duration.
    pub(crate) fn due_after(self, send_number: u64) -> Duration {
        // Two 64-bit numbers multiply without overflow in 128 bits; the nanoseconds may not, and then
        // the offset is beyond any duration anyway.
        let records_before = u128::from(send_number) * u128::from(self.records_per_send);
        let nanoseconds = records_before.saturating_mul(NANOSECONDS_PER_SECOND) / u128::from(self.records_per_second);
        match u64::try_from(nanoseconds) {
            Ok(nanoseconds) => Duration::from_nanos(nanoseconds),
            Err(_too_long) => Duration::MAX,
        }
    }
}

#[cfg(test)]
#[path = "pacing_test.rs"]
mod tests;
