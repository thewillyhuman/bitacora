//! The metrics the replica daemon records about itself.
//!
//! # Why this file exists
//!
//! The decorators of `bitacora-process` measure what passes through the replica's interfaces: requests,
//! flushes, calls to the metadata store. Some things an operator needs are not requests: how many
//! segment copies the replica holds, how many of them are damaged, whether it is draining, and whether
//! its registration and its scrubber are working. The daemon knows these from its own timers, and this
//! file is where it records them.
//!
//! # What it contains
//!
//! - [`ReplicaDaemonMetrics`]: the gauges and counters, registered in the process's registry.
//!
//! # How it works
//!
//! | Metric | Updated |
//! | --- | --- |
//! | `bitacora_replica_segment_copies` | On every registration renewal, from what the replica lists |
//! | `bitacora_replica_damaged_segment_copies` | On every renewal, and after every scrub |
//! | `bitacora_replica_draining` | On every renewal |
//! | `bitacora_replica_registrations_total` | On every renewal that reached the metadata store |
//! | `bitacora_replica_registration_failures_total` | On every renewal that failed |
//! | `bitacora_replica_scrubs_total` | After every completed scrub |
//!
//! The names live under the `replica` subsystem, next to the request metrics of the replica interface.
//!
//! # Invariants
//!
//! - Every metric here is listed in `docs/operations/metrics.md`; a test fails otherwise.
//!
//! # See also
//!
//! - `registration.rs` and `scrub.rs`: the timers that update these metrics.
//! - `bitacora-process/src/metrics.rs`: the naming scheme.

use bitacora_process::{Counter, Gauge, MetricsRegistry};

/// What the replica daemon records about itself. Clones share the same metrics.
#[derive(Clone, Debug)]
pub(crate) struct ReplicaDaemonMetrics {
    /// How many segment copies the replica holds.
    pub segment_copies: Gauge,
    /// How many of them failed a checksum or a flush and await replacement.
    pub damaged_segment_copies: Gauge,
    /// 1 while the replica is draining, 0 otherwise.
    pub draining: Gauge,
    /// Renewals of the registration that reached the metadata store.
    pub registrations: Counter,
    /// Renewals of the registration that failed.
    pub registration_failures: Counter,
    /// Completed scrubs of the sealed segments.
    pub scrubs: Counter,
}

impl ReplicaDaemonMetrics {
    /// Registers the daemon's metrics in `registry`.
    ///
    /// # Arguments
    ///
    /// * `registry` - The registry of the process. Register once per registry.
    pub(crate) fn register(registry: &MetricsRegistry) -> Self {
        Self {
            segment_copies: registry.gauge("replica_segment_copies", "Segment copies the replica holds"),
            damaged_segment_copies: registry.gauge(
                "replica_damaged_segment_copies",
                "Segment copies that failed a checksum or a flush and await replacement",
            ),
            draining: registry.gauge("replica_draining", "Whether the replica is draining (1) or not (0)"),
            registrations: registry.counter(
                "replica_registrations",
                "Renewals of the replica's registration that reached the metadata store",
            ),
            registration_failures: registry.counter(
                "replica_registration_failures",
                "Renewals of the replica's registration that failed",
            ),
            scrubs: registry.counter("replica_scrubs", "Completed scrubs of the replica's sealed segments"),
        }
    }
}

#[cfg(test)]
#[path = "metrics_test.rs"]
mod tests;
