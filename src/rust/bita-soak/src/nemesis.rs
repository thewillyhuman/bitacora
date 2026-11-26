//! The nemesis: what goes wrong in a round, and when.
//!
//! # Why this file exists
//!
//! A cluster that is left alone proves little. The promises of the log are about what survives a killed replica, a
//! lost controller, a zone that drops off the network; a soak has to make those happen, again and again, for days,
//! and write down what it did so that a broken invariant can be traced to the fault before it. This file is the
//! loop that does it during a round.
//!
//! # What it contains
//!
//! - [`NemesisSettings`]: which kinds of fault are injected, how often, how long a cut lasts and how it is checked.
//! - [`NemesisMemory`]: what the nemesis carries from one round to the next: its random source, and the kinds of
//!   fault it has given up on.
//! - [`NemesisTally`]: what was injected in a round, by kind, and how often nothing was.
//! - [`Nemesis`]: the loop of one round.
//! - [`pause`]: a wait that ends early at a deadline or when the tool is told to stop.
//! - [`is_told_to_stop`]: whether the tool must end.
//! - [`remove_cuts_within`]: removes every network cut, trying again for a bounded time.
//!
//! # How it works
//!
//! Once per interval the nemesis looks at the cluster. If the cluster is not healthy, the fault before is still
//! being recovered from, and nothing is injected: there is never more than one fault at a time. Otherwise it picks
//! a kind from its settings and a target from what the cluster offers (`targets.rs`), has the injector carry the
//! fault out, and records it in the history of every stream. A kill needs nothing more.
//!
//! A cut is different, because the cluster's management agreeing to a cut does not make it one: a cluster whose
//! network does not enforce such cuts accepts them and lets everything through. So a cut is first checked
//! (`cut_check.rs`): the replicas behind it must stop renewing their registration. Only then is it counted and
//! recorded, left in place for its duration, and removed, which is recorded too. A cut that is not in effect is
//! removed, counted as not injected and recorded nowhere, and its kind is given up for the rest of the run, with
//! the reason kept for the reports. An option makes the nemesis try such a kind again instead.
//!
//! The choices come from a seeded random source, so that a test of a round makes the same choices every time.
//!
//! # Invariants
//!
//! - No fault is injected while the cluster is not healthy, and no cut outlives the call that made it by more than
//!   its check and its duration, unless removing it fails. Then the round's own healing tries again.
//! - A fault is recorded in the histories and counted only once it is known to have happened: a kill when the
//!   injector said it was done, a cut when its effect was seen.
//! - The loop ends at its deadline or as soon as the tool is told to stop, whichever comes first. A cut that is
//!   being checked at the deadline is checked to the end.
//!
//! # See also
//!
//! - `faults.rs`: the faults and the injector.
//! - `targets.rs`: what may be hit.
//! - `cut_check.rs`: whether a cut is real.
//! - `health.rs`: when the cluster is healthy.
//! - `round.rs`: runs the nemesis and heals after it.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bitacora_types::{DeterministicRandom, ReplicaId};
use tokio::sync::watch;
use tokio::time::{Instant, sleep, sleep_until};

use crate::cluster::SoakedCluster;
use crate::cut_check::{CutCheckSettings, CutEffect, check_cut};
use crate::faults::{Fault, FaultKind};
use crate::health::read_unhealthy_reason;
use crate::targets::{ClusterView, choose_fault, replicas_cut_by};
use crate::workload::SoakedStream;

/// How long [`remove_cuts_within`] waits before it asks again.
const REMOVAL_RETRY_PAUSE: Duration = Duration::from_secs(2);

/// How long the nemesis itself keeps trying to remove a cut whose time is up, before it leaves the cut to the
/// healing of the round.
const CUT_REMOVAL_TIME_LIMIT: Duration = Duration::from_secs(60);

/// What the nemesis of a round does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NemesisSettings {
    /// The kinds of fault to choose from. Empty means no faults.
    pub(crate) kinds: Vec<FaultKind>,
    /// How long the nemesis waits between two faults.
    pub(crate) fault_interval: Duration,
    /// How long a network cut stays in place once it was seen to be in effect.
    pub(crate) cut_duration: Duration,
    /// How a cut is checked before it counts.
    pub(crate) cut_check: CutCheckSettings,
    /// Whether a kind of cut that was not in effect is tried again. Without this, the kind is given up for the
    /// rest of the run.
    pub(crate) retries_cuts_not_in_effect: bool,
}

/// What the nemesis carries from one round to the next.
#[derive(Debug)]
pub(crate) struct NemesisMemory {
    /// Picks the kind and the target of every fault.
    pub(crate) random: DeterministicRandom,
    /// The kinds of fault that were asked for and are no longer tried, each with the reason.
    pub(crate) disabled: BTreeMap<FaultKind, String>,
}

impl NemesisMemory {
    /// The memory of a nemesis that has done nothing yet and makes its choices from `seed`.
    pub(crate) fn from_seed(seed: u64) -> Self {
        Self {
            random: DeterministicRandom::from_seed(seed),
            disabled: BTreeMap::new(),
        }
    }
}

/// What the nemesis of a round did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct NemesisTally {
    /// How many faults of each kind were injected.
    pub(crate) injected: BTreeMap<FaultKind, u64>,
    /// How many times a fault was due and none was injected: the cluster was not healthy yet, it offered no target,
    /// its management refused, every kind asked for was given up, or a cut was not in effect.
    pub(crate) not_injected: u64,
}

/// Waits for `duration`, but not past `deadline` and not once `stop` says to end.
///
/// # Arguments
///
/// * `duration` - How long to wait.
/// * `deadline` - The moment at which the wait ends at the latest.
/// * `stop` - Becomes `true`, or is dropped, when the tool must end.
///
/// # Returns
///
/// Whether the whole duration passed before the deadline.
pub(crate) async fn pause(duration: Duration, deadline: Instant, stop: &mut watch::Receiver<bool>) -> bool {
    let wake_at = Instant::now() + duration;
    // A wait that would end exactly at the deadline leaves no time to do anything after it.
    let ends_at_the_deadline = wake_at >= deadline;
    tokio::select! {
        () = sleep_until(wake_at.min(deadline)) => !ends_at_the_deadline,
        _told_to_stop = stop.wait_for(|is_stopped| *is_stopped) => false,
    }
}

/// Whether `stop` says the tool must end: its value is `true`, or whoever would say so is gone.
pub(crate) fn is_told_to_stop(stop: &watch::Receiver<bool>) -> bool {
    *stop.borrow() || stop.has_changed().is_err()
}

/// Removes every network cut the tool made, trying again until it works or `time_limit` has passed.
///
/// # Arguments
///
/// * `cluster` - The cluster under test.
/// * `time_limit` - How long to keep trying.
///
/// # Errors
///
/// Returns why the last attempt failed, in words. A cut may still be in place then.
pub(crate) async fn remove_cuts_within(cluster: &SoakedCluster, time_limit: Duration) -> Result<(), String> {
    let deadline = Instant::now() + time_limit;
    loop {
        let fault_error = match cluster.faults.remove_cuts().await {
            Ok(()) => return Ok(()),
            Err(fault_error) => fault_error,
        };
        if Instant::now() >= deadline {
            return Err(format!("a network cut could not be removed: {fault_error}"));
        }
        sleep(REMOVAL_RETRY_PAUSE).await;
    }
}

/// A fault that was chosen, and the replicas it cuts off the network if it is a cut.
struct ChosenFault {
    /// The fault.
    fault: Fault,
    /// The replicas behind the cut. Empty for a kill.
    cut_off: BTreeSet<ReplicaId>,
}

/// The nemesis of one round.
pub(crate) struct Nemesis<'round> {
    /// The cluster under test.
    cluster: &'round SoakedCluster,
    /// Which faults, how often, and for how long.
    settings: &'round NemesisSettings,
    /// The replicas the run started with, which must all be alive before a fault is injected.
    expected_replicas: &'round BTreeSet<ReplicaId>,
    /// The streams of the round. Every fault is recorded in each of their histories.
    streams: &'round [SoakedStream],
    /// What was done so far.
    tally: NemesisTally,
}

impl<'round> Nemesis<'round> {
    /// The nemesis of a round, before its first fault.
    pub(crate) fn new(
        cluster: &'round SoakedCluster,
        settings: &'round NemesisSettings,
        expected_replicas: &'round BTreeSet<ReplicaId>,
        streams: &'round [SoakedStream],
    ) -> Self {
        Self {
            cluster,
            settings,
            expected_replicas,
            streams,
            tally: NemesisTally::default(),
        }
    }

    /// Injects one fault per interval until `deadline` or until `stop` says to end.
    ///
    /// # Arguments
    ///
    /// * `memory` - The random source of the choices, and the kinds given up so far. A kind whose cut is found
    ///   not to be in effect is added to it.
    /// * `deadline` - When the round's workload is over.
    /// * `stop` - Becomes `true`, or is dropped, when the tool must end.
    ///
    /// # Returns
    ///
    /// What was injected. A cut that could not be removed is still in place; the caller heals.
    pub(crate) async fn run(
        mut self,
        memory: &mut NemesisMemory,
        deadline: Instant,
        stop: &mut watch::Receiver<bool>,
    ) -> NemesisTally {
        while pause(self.settings.fault_interval, deadline, stop).await {
            if !self.settings.kinds.is_empty() {
                self.inject_one_fault(memory, deadline, stop).await;
            }
        }
        self.tally
    }

    /// Chooses a fault, injects it, and counts and records it once it is known to have happened. A cut is checked
    /// first, and removed when its time is up.
    async fn inject_one_fault(
        &mut self,
        memory: &mut NemesisMemory,
        deadline: Instant,
        stop: &mut watch::Receiver<bool>,
    ) {
        let Some(chosen) = self.next_fault(memory).await else {
            self.tally.not_injected += 1;
            return;
        };
        let fault = &chosen.fault;
        let injected_at = Instant::now();
        if let Err(fault_error) = self.cluster.faults.inject(fault).await {
            eprintln!("bita-soak: not done: {}: {fault_error}", fault.description());
            self.tally.not_injected += 1;
            // A cut that failed half way may be in place.
            self.remove_cut_of(fault).await;
            return;
        }
        if !fault.is_cut() {
            self.record(&fault.description());
            *self.tally.injected.entry(fault.kind()).or_default() += 1;
            return;
        }
        if !self.is_in_effect(&chosen, memory, stop).await {
            self.tally.not_injected += 1;
            self.remove_cut_of(fault).await;
            return;
        }
        self.record(&format!("{} ({:?} ago)", fault.description(), injected_at.elapsed()));
        *self.tally.injected.entry(fault.kind()).or_default() += 1;
        let _whole_duration_passed = pause(self.settings.cut_duration, deadline, stop).await;
        if self.remove_cut_of(fault).await {
            self.record(&format!("{}: healed", fault.description()));
        }
    }

    /// Chooses the next fault among the kinds that are still tried, or nothing if the cluster is not healthy,
    /// cannot be read, or offers no target.
    async fn next_fault(&self, memory: &mut NemesisMemory) -> Option<ChosenFault> {
        if read_unhealthy_reason(self.cluster, self.expected_replicas)
            .await
            .is_some()
        {
            return None;
        }
        let view = self.read_view().await?;
        let mut kinds = Vec::new();
        for kind in &self.settings.kinds {
            if !memory.disabled.contains_key(kind) {
                kinds.push(*kind);
            }
        }
        let kind = *memory.random.choose(&kinds)?;
        let fault = choose_fault(kind, &view, &mut memory.random)?;
        let cut_off = replicas_cut_by(&fault, &view);
        Some(ChosenFault { fault, cut_off })
    }

    /// Reads what the cluster offers as targets. `None` if the metadata store or the cluster's management does not
    /// answer.
    async fn read_view(&self) -> Option<ClusterView> {
        let replicas = self.cluster.metadata.list_replicas().await.ok()?;
        let leader = self.cluster.metadata.get_leader().await.ok()?;
        let etcd_members = self.cluster.faults.etcd_members().await.ok()?;
        Some(ClusterView {
            replicas,
            leading_controller: leader.map(|leader| leader.holder),
            etcd_members,
        })
    }

    /// Finds out whether the cut that was just made keeps its replicas from the metadata store. A cut that is seen
    /// not to do so has its kind given up, unless the settings say to try again.
    async fn is_in_effect(
        &self,
        chosen: &ChosenFault,
        memory: &mut NemesisMemory,
        stop: &mut watch::Receiver<bool>,
    ) -> bool {
        let description = chosen.fault.description();
        match check_cut(self.cluster, &chosen.cut_off, self.settings.cut_check, stop).await {
            CutEffect::InEffect => true,
            CutEffect::Unknown(reason) => {
                eprintln!("bita-soak: not counted: {description}: {reason}");
                false
            }
            CutEffect::NotInEffect(evidence) => {
                let reason = format!("network cuts are not enforced on this cluster: {description}, but {evidence}");
                if self.settings.retries_cuts_not_in_effect {
                    eprintln!("bita-soak: {reason}; the cut is not counted, and its kind will be tried again");
                } else {
                    eprintln!("bita-soak: {reason}; no more faults of this kind in this run");
                    memory.disabled.insert(chosen.fault.kind(), reason);
                }
                false
            }
        }
    }

    /// Removes the cut that `fault` made, if it is one. Returns whether a cut was removed.
    async fn remove_cut_of(&self, fault: &Fault) -> bool {
        if !fault.is_cut() {
            return false;
        }
        match remove_cuts_within(self.cluster, CUT_REMOVAL_TIME_LIMIT).await {
            Ok(()) => true,
            Err(reason) => {
                eprintln!("bita-soak: {reason}; the round's healing tries again");
                false
            }
        }
    }

    /// Records `description` in the history of every stream of the round.
    fn record(&self, description: &str) {
        for stream in self.streams {
            stream.history.fault(description);
        }
    }
}

#[cfg(test)]
#[path = "nemesis_test.rs"]
mod tests;
