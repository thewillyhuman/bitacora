//! Finding out whether a network cut is real.
//!
//! # Why this file exists
//!
//! A cut is made by asking the cluster's management for it, and the management saying yes proves nothing. A
//! Kubernetes cluster whose network plugin does not enforce network policies accepts a policy, reports the pod as
//! isolated, and lets every packet through. A soak that counted such a cut would report a week of rounds as having
//! survived partitions that never happened. So a cut counts only once the tool has seen its effect with its own
//! eyes, and this file is where it looks.
//!
//! # What it contains
//!
//! - [`CutCheckSettings`]: how long a replica must stay silent for the cut to count, and how long the tool waits
//!   for that.
//! - [`CutEffect`]: what the tool saw.
//! - [`check_cut`]: watches the registry of replicas and says whether the cut is in effect.
//! - `RenewalWatch`: when each replica behind the cut was last seen to renew its registration.
//!
//! # How it works
//!
//! Every replica renews its registration in the metadata store every few seconds, and the tool reads that store
//! anyway. A replica that is cut off cannot reach the store, so its registration stops changing. The tool reads
//! the registry once a second and notes, for every replica behind the cut, when it last saw the registration
//! change. Once none of them has changed for the quiet period, the cut is in effect. If the time limit passes
//! first and a replica is still renewing, the cut is not in effect: the evidence is the renewal the tool saw.
//!
//! Only changes are compared, never the registration's time against the tool's clock, so the clocks of the
//! replicas and of the tool need not agree.
//!
//! # Invariants
//!
//! - [`CutEffect::InEffect`] is returned only after the registry was read and no replica behind the cut had
//!   renewed its registration for the whole quiet period.
//! - [`CutEffect::NotInEffect`] is returned only with a renewal the tool saw. A registry that could not be read,
//!   or a tool that was told to stop, is [`CutEffect::Unknown`]: no evidence either way.
//! - The check ends within the time limit, or as soon as the tool is told to stop.
//!
//! # See also
//!
//! - `nemesis.rs`: makes the cut, calls [`check_cut`], and counts the cut only if it is in effect.
//! - `targets.rs`: says which replicas are behind a cut.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bitacora_meta::ReplicaRecord;
use bitacora_types::{ReplicaId, Timestamp};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::cluster::SoakedCluster;
use crate::health::registration_of;
use crate::nemesis::{is_told_to_stop, pause};

/// How long [`check_cut`] waits between two looks at the registry.
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// How a cut is checked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CutCheckSettings {
    /// How long every replica behind the cut must go without renewing its registration for the cut to count.
    /// Several times the interval at which replicas register.
    pub(crate) quiet_period: Duration,
    /// How long the tool waits for that before it calls the cut ineffective.
    pub(crate) time_limit: Duration,
}

/// What the tool saw of a cut.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CutEffect {
    /// No replica behind the cut renewed its registration for the quiet period.
    InEffect,
    /// A replica behind the cut kept renewing its registration. The text says which, and when.
    NotInEffect(String),
    /// The tool could not tell, for the reason in the text.
    Unknown(String),
}

/// Watches the registry and says whether the cut that hides `cut_off` is in effect.
///
/// # Arguments
///
/// * `cluster` - The cluster under test.
/// * `cut_off` - The replicas behind the cut.
/// * `settings` - How long they must stay silent, and how long to wait for it.
/// * `stop` - Becomes `true`, or is dropped, when the tool must end.
///
/// # Returns
///
/// What the tool saw. See [`CutEffect`].
pub(crate) async fn check_cut(
    cluster: &SoakedCluster,
    cut_off: &BTreeSet<ReplicaId>,
    settings: CutCheckSettings,
    stop: &mut watch::Receiver<bool>,
) -> CutEffect {
    if cut_off.is_empty() {
        return CutEffect::Unknown("no registered replica is behind the cut".to_owned());
    }
    let deadline = Instant::now() + settings.time_limit;
    let mut watch = RenewalWatch::starting_now(cut_off);
    let mut registry_was_read = watch.look(cluster).await;
    while !(registry_was_read && watch.latest_renewal_within(settings.quiet_period).is_none()) {
        if !pause(POLL_INTERVAL, deadline, stop).await {
            return watch.verdict_at_the_end(registry_was_read, settings.quiet_period, stop);
        }
        registry_was_read = watch.look(cluster).await;
    }
    CutEffect::InEffect
}

/// When a replica was last seen to renew its registration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Renewal {
    /// The time in the registration when it was last read. `None` if the replica was not registered, or if the
    /// registry has not been read yet.
    heartbeat_at: Option<Timestamp>,
    /// When the tool first read that time.
    seen_at: Instant,
}

/// When each replica behind a cut was last seen to renew its registration.
#[derive(Debug)]
struct RenewalWatch {
    /// The last renewal of every replica behind the cut.
    renewals: BTreeMap<ReplicaId, Renewal>,
}

impl RenewalWatch {
    /// A watch over `cut_off` that has seen nothing yet.
    fn starting_now(cut_off: &BTreeSet<ReplicaId>) -> Self {
        let now = Instant::now();
        let mut renewals = BTreeMap::new();
        for replica_id in cut_off {
            let nothing_seen_yet = Renewal {
                heartbeat_at: None,
                seen_at: now,
            };
            renewals.insert(replica_id.clone(), nothing_seen_yet);
        }
        Self { renewals }
    }

    /// Reads the registry and notes what changed. Returns whether the registry could be read.
    async fn look(&mut self, cluster: &SoakedCluster) -> bool {
        match cluster.metadata.list_replicas().await {
            Ok(registry) => {
                self.observe(&registry, Instant::now());
                true
            }
            Err(_metadata_store_does_not_answer) => false,
        }
    }

    /// Notes, for every replica behind the cut, whether its registration in `registry` differs from the last one
    /// seen. A first reading counts as a change: the quiet period starts with it.
    fn observe(&mut self, registry: &[ReplicaRecord], now: Instant) {
        for (replica_id, renewal) in &mut self.renewals {
            let heartbeat_at = registration_of(replica_id, registry).map(|registration| registration.heartbeat_at);
            if heartbeat_at != renewal.heartbeat_at {
                *renewal = Renewal {
                    heartbeat_at,
                    seen_at: now,
                };
            }
        }
    }

    /// The replica whose registration changed most recently, and how long ago, if that is less than
    /// `quiet_period` ago. `None` once every replica behind the cut has been silent for the whole period.
    fn latest_renewal_within(&self, quiet_period: Duration) -> Option<(&ReplicaId, Duration)> {
        let now = Instant::now();
        let mut latest: Option<(&ReplicaId, Duration)> = None;
        for (replica_id, renewal) in &self.renewals {
            let silent_for = now.duration_since(renewal.seen_at);
            let is_more_recent = match latest {
                Some((_replica_id, shortest_silence)) => silent_for < shortest_silence,
                None => true,
            };
            if silent_for < quiet_period && is_more_recent {
                latest = Some((replica_id, silent_for));
            }
        }
        latest
    }

    /// What to say when the wait is over and the replicas were not seen to go silent.
    fn verdict_at_the_end(
        &self,
        registry_was_read: bool,
        quiet_period: Duration,
        stop: &watch::Receiver<bool>,
    ) -> CutEffect {
        if is_told_to_stop(stop) {
            return CutEffect::Unknown("the tool was told to stop before the cut could be checked".to_owned());
        }
        match self.latest_renewal_within(quiet_period) {
            Some((replica_id, silent_for)) if registry_was_read => CutEffect::NotInEffect(format!(
                "replica {replica_id} renewed its registration {silent_for:?} ago, from behind the cut"
            )),
            _ => CutEffect::Unknown("the registry of replicas could not be read".to_owned()),
        }
    }
}

#[cfg(test)]
#[path = "cut_check_test.rs"]
mod tests;
