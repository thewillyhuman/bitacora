//! The faults a soak injects, and the interface through which it injects them.
//!
//! # Why this file exists
//!
//! A soak has to break the cluster it drives: kill its processes, cut them off the network. How that is done depends
//! on where the cluster runs. On Kubernetes it is a call to the API server; in the tests of this crate the cluster
//! lives inside the test process and a fault is a method call. The rounds, the nemesis and the judge must not care
//! which it is, or they could only be tested on a real cluster. This file is the line between the two: what a fault
//! is, as plain data, and the one interface that makes it happen.
//!
//! # What it contains
//!
//! - [`FaultKind`]: the five kinds of fault, as a person names them on the command line.
//! - [`Fault`]: one fault with its target.
//! - [`EtcdMember`]: one member of the metadata store, as far as a fault needs to know it.
//! - [`FaultInjector`]: injects a fault, removes the network cuts, lists the members of the metadata store.
//! - [`NoFaults`]: the injector of a run without faults.
//! - [`FaultError`]: why a fault could not be injected or removed.
//!
//! # How it works
//!
//! The nemesis decides which fault comes next and builds a [`Fault`]; the injector carries it out. Faults are of two
//! sorts. A kill needs no undoing: whatever supervises the process starts it again. A cut stays until it is removed,
//! and [`FaultInjector::remove_cuts`] removes every cut the tool ever made, also those of an earlier run that died
//! before it could.
//!
//! # Invariants
//!
//! - A fault names its target by what the cluster calls it: the name of a pod, the name of a zone. Nothing here knows
//!   how a target is reached.
//! - [`FaultInjector::remove_cuts`] is safe to call at any time and any number of times: with no cut in place it does
//!   nothing.
//!
//! # See also
//!
//! - `nemesis.rs`: chooses the faults and calls the injector.
//! - `targets.rs`: finds the pods and zones a fault may name.
//! - `test_support.rs`: the injector over the in-process test cluster.

use async_trait::async_trait;
use clap::ValueEnum;
use thiserror::Error;

/// A kind of fault, as it is named on the command line and counted in a report.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub(crate) enum FaultKind {
    /// Kill the process of one replica without warning.
    KillReplica,
    /// Kill the process of the controller that leads.
    KillController,
    /// Kill the process of one member of the metadata store.
    KillEtcdMember,
    /// Cut one replica off the network for a while.
    CutReplica,
    /// Cut a whole zone off the network for a while: its replicas and its member of the metadata store.
    CutZone,
}

impl FaultKind {
    /// Every kind, in the order reports list them.
    pub(crate) const ALL: [Self; 5] = [
        Self::KillReplica,
        Self::KillController,
        Self::KillEtcdMember,
        Self::CutReplica,
        Self::CutZone,
    ];

    /// The name of the kind in a report: the name on the command line with underscores.
    pub(crate) const fn report_name(self) -> &'static str {
        match self {
            Self::KillReplica => "kill_replica",
            Self::KillController => "kill_controller",
            Self::KillEtcdMember => "kill_etcd_member",
            Self::CutReplica => "cut_replica",
            Self::CutZone => "cut_zone",
        }
    }
}

/// One fault and what it hits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Fault {
    /// Kill the replica that runs in `pod`.
    KillReplica {
        /// The pod of the replica.
        pod: String,
    },
    /// Kill the controller that runs in `pod`.
    KillController {
        /// The pod of the controller.
        pod: String,
    },
    /// Kill the member of the metadata store that runs in `pod`.
    KillEtcdMember {
        /// The pod of the member.
        pod: String,
    },
    /// Cut the replica that runs in `pod` off the network.
    CutReplica {
        /// The pod of the replica.
        pod: String,
    },
    /// Cut the replicas and the member of the metadata store that run in `zone` off the network.
    CutZone {
        /// The zone, as the pods' zone label names it.
        zone: String,
    },
}

impl Fault {
    /// The kind of the fault.
    pub(crate) const fn kind(&self) -> FaultKind {
        match self {
            Self::KillReplica { .. } => FaultKind::KillReplica,
            Self::KillController { .. } => FaultKind::KillController,
            Self::KillEtcdMember { .. } => FaultKind::KillEtcdMember,
            Self::CutReplica { .. } => FaultKind::CutReplica,
            Self::CutZone { .. } => FaultKind::CutZone,
        }
    }

    /// Whether the fault stays in place until it is removed. A kill does not: the process is started again by
    /// whatever supervises it.
    pub(crate) const fn is_cut(&self) -> bool {
        matches!(self, Self::CutReplica { .. } | Self::CutZone { .. })
    }

    /// What the fault does, in words, for the history and the log.
    pub(crate) fn description(&self) -> String {
        match self {
            Self::KillReplica { pod } => format!("replica {pod} is killed"),
            Self::KillController { pod } => format!("controller {pod} is killed"),
            Self::KillEtcdMember { pod } => format!("etcd member {pod} is killed"),
            Self::CutReplica { pod } => format!("replica {pod} is cut off the network"),
            Self::CutZone { zone } => format!("zone {zone} is cut off the network"),
        }
    }
}

/// One member of the metadata store, as far as a fault needs to know it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EtcdMember {
    /// The pod the member runs in.
    pub(crate) pod: String,
    /// The zone the pod is in, if its zone label says.
    pub(crate) zone: Option<String>,
}

/// Why a fault could not be injected or removed.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("{reason}")]
pub(crate) struct FaultError {
    /// What went wrong, with the target and what the cluster's management answered.
    pub(crate) reason: String,
}

/// Makes faults happen in the cluster a soak drives.
#[async_trait]
pub(crate) trait FaultInjector: Send + Sync {
    /// Injects `fault`. A kill returns once the process was told to die; a cut returns once it is in place.
    ///
    /// # Errors
    ///
    /// Fails if the cluster's management refused or did not answer. Nothing is known then about whether the fault
    /// happened; a cut may be in place.
    async fn inject(&self, fault: &Fault) -> Result<(), FaultError>;

    /// Removes every network cut this tool made, in this run or an earlier one. Does nothing if there is none.
    ///
    /// # Errors
    ///
    /// Fails if the cluster's management refused or did not answer. A cut may still be in place then.
    async fn remove_cuts(&self) -> Result<(), FaultError>;

    /// Lists the members of the metadata store that a fault may hit.
    ///
    /// # Errors
    ///
    /// Fails if the cluster's management refused or did not answer.
    async fn etcd_members(&self) -> Result<Vec<EtcdMember>, FaultError>;
}

/// The injector of a run without faults: it has nothing to reach, so it needs no access to the cluster's management.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct NoFaults;

#[async_trait]
impl FaultInjector for NoFaults {
    /// Refuses: a run without faults never asks for one.
    async fn inject(&self, fault: &Fault) -> Result<(), FaultError> {
        Err(FaultError {
            reason: format!("this run injects no faults, but was asked for: {}", fault.description()),
        })
    }

    /// Does nothing: a run without faults made no cut.
    async fn remove_cuts(&self) -> Result<(), FaultError> {
        Ok(())
    }

    /// Lists nothing: a run without faults hits no member.
    async fn etcd_members(&self) -> Result<Vec<EtcdMember>, FaultError> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
#[path = "faults_test.rs"]
mod tests;
