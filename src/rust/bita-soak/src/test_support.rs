//! A cluster inside the test process, with faults that are method calls.
//!
//! # Why this file exists
//!
//! The rounds, the nemesis and the judge of a soak are logic, and logic has to be tested on every change, in a
//! second, without a Kubernetes cluster. The in-process test cluster of `bitacora-testkit` has real replicas behind
//! links that can be cut, on a clock the test controls. What it lacks to stand in for a deployment is what a
//! deployment has around its replicas: a controller, registrations that are renewed, something that starts a killed
//! process again. This file adds those and puts the tool's fault interface on top, so that a test plays whole
//! rounds, with every kind of fault, in virtual time.
//!
//! # What it contains
//!
//! - [`InProcessCluster`]: six replicas in three zones, a controller, renewed registrations, and the
//!   [`SoakedCluster`] to hand to the code under test.
//! - [`InProcessFaults`]: the fault interface over that cluster, and what tests ask it afterwards.
//! - [`quick_round`]: settings of a round that takes a virtual minute.
//!
//! # How it works
//!
//! A kill crashes the node's replica and starts it again three seconds later, as a stateful set would. Killing the
//! controller ends its task and starts one under the next name, as a Deployment would. Killing a member of the
//! metadata store makes the store unavailable for a second, which is what clients see while etcd elects a new
//! leader. A cut cuts the link of a node, or of every node of a zone; removing the cuts heals every link.
//!
//! Every two seconds the registration of every node that runs and is not cut off is renewed. A node behind a cut
//! cannot reach the metadata store, so its registration goes stale, as it does on a real cluster; that is what the
//! tool looks at to see whether a cut is real.
//!
//! A test can make the cluster behave like one whose network does not enforce cuts
//! ([`InProcessFaults::let_cuts_do_nothing`]): a cut is then accepted, remembered, and changes nothing.
//!
//! Three members of the metadata store are listed, one per zone, although the store is one value in memory: the
//! rules about which zone may be cut need them.
//!
//! # Invariants
//!
//! - Every task the cluster starts ends when the cluster is dropped.
//! - The faults remember what they were asked to do, in order, whether or not a test looks.
//! - A fault whose target does not exist is refused, never reported as done.
//!
//! # See also
//!
//! - `faults.rs`: the interface implemented here.
//! - `round_test.rs`, `soak_test.rs`, `nemesis_test.rs`, `cut_check_test.rs`: the tests that use it.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use bitacora::{BitacoraClient, ClientSettings};
use bitacora_controller::{Controller, ControllerSettings};
use bitacora_meta::{MetaError, ReplicaRecord};
use bitacora_replica::ReplicaSettings;
use bitacora_testkit::{Cluster, FailureDomainLayout, LinkFault};
use bitacora_types::ReplicaId;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::sleep;

use crate::cluster::SoakedCluster;
use crate::cut_check::CutCheckSettings;
use crate::faults::{EtcdMember, Fault, FaultError, FaultInjector, FaultKind};
use crate::nemesis::NemesisSettings;
use crate::round::RoundSettings;
use crate::targets::pod_of_address;
use crate::workload::WorkloadSettings;

/// How many replicas the cluster has.
const NODE_COUNT: usize = 6;

/// How many zones the replicas are dealt out to: node `N` is in `zone-<N modulo 3>`.
const ZONE_COUNT: usize = 3;

/// The size of the segment files, small so that a round rolls segments.
const SEGMENT_FILE_BYTES: u64 = 4 * 1024;

/// How often every node that can reach the metadata store renews its registration.
const REGISTRATION_INTERVAL: Duration = Duration::from_secs(2);

/// How long a killed replica stays down before it is started again.
const RESTART_DELAY: Duration = Duration::from_secs(3);

/// How long the metadata store is unavailable after one of its members was killed.
const ELECTION_TIME: Duration = Duration::from_secs(1);

/// How long the controller rests between two passes.
const CONTROLLER_PASS_INTERVAL: Duration = Duration::from_millis(500);

/// How long a controller's leadership lasts without renewal.
const CONTROLLER_LEADERSHIP_DURATION: Duration = Duration::from_secs(5);

/// How long the controller waits for a replica to answer.
const CONTROLLER_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// How a cut is checked in tests: three registration intervals of silence, within twenty seconds.
pub(crate) const QUICK_CUT_CHECK: CutCheckSettings = CutCheckSettings {
    quiet_period: Duration::from_secs(6),
    time_limit: Duration::from_secs(20),
};

/// A cluster inside the test process that behaves like a deployment.
pub(crate) struct InProcessCluster {
    /// What the code under test is handed.
    pub(crate) soaked: SoakedCluster,
    /// The faults of this cluster, for a test to ask what happened.
    pub(crate) faults: Arc<InProcessFaults>,
    /// The task that renews the registrations. It ends when the cluster is dropped.
    _registrations: JoinSet<()>,
}

impl InProcessCluster {
    /// Starts six replicas in three zones, a controller and the renewal of registrations.
    pub(crate) async fn start() -> Self {
        let settings = ReplicaSettings {
            preallocated_segment_bytes: SEGMENT_FILE_BYTES,
            ..ReplicaSettings::default()
        };
        let layout = FailureDomainLayout::HostsInZones { zone_count: ZONE_COUNT };
        let cluster = Arc::new(Cluster::start_with(NODE_COUNT, settings, layout).await);
        let faults = Arc::new(InProcessFaults::over(Arc::clone(&cluster)));
        let client = BitacoraClient::new(
            cluster.metadata().clone(),
            cluster.connector(),
            cluster.clock(),
            ClientSettings::default(),
        );
        let mut registrations = JoinSet::new();
        registrations.spawn(renew_registrations(Arc::clone(&cluster)));
        Self {
            soaked: SoakedCluster {
                client,
                metadata: cluster.metadata().clone(),
                clock: cluster.clock(),
                faults: Arc::clone(&faults) as Arc<dyn FaultInjector>,
            },
            faults,
            _registrations: registrations,
        }
    }
}

/// Renews, for ever, the registration of every node that runs and can reach the metadata store.
async fn renew_registrations(cluster: Arc<Cluster>) {
    loop {
        // A metadata outage makes this fail; the next turn tries again.
        drop(renew_reachable_registrations(&cluster).await);
        sleep(REGISTRATION_INTERVAL).await;
    }
}

/// Renews the registration of every node that runs and whose link is not cut.
async fn renew_reachable_registrations(cluster: &Cluster) -> Result<(), MetaError> {
    let now = cluster.clock().now();
    for registration in cluster.metadata().list_replicas().await? {
        if can_register(cluster, &registration.replica_id) {
            let renewed = ReplicaRecord {
                heartbeat_at: now,
                ..registration
            };
            cluster.metadata().put_replica(&renewed).await?;
        }
    }
    Ok(())
}

/// Whether the replica `replica_id` runs on a node that can reach the metadata store.
fn can_register(cluster: &Cluster, replica_id: &ReplicaId) -> bool {
    for node in cluster.nodes() {
        if &node.replica_id() == replica_id {
            return node.is_running() && !node.link().fault().is_cut;
        }
    }
    false
}

/// The controller that is running, and how many there have been.
struct RunningController {
    /// The number in the name of the running controller: `controller-<number>`.
    number: u64,
    /// The task of the running controller.
    task: JoinHandle<()>,
}

/// The fault interface over the in-process cluster.
pub(crate) struct InProcessFaults {
    /// The cluster the faults act on.
    cluster: Arc<Cluster>,
    /// The controller, replaced whenever it is killed.
    controller: Mutex<RunningController>,
    /// Every fault that was carried out, in order.
    injected: Mutex<Vec<Fault>>,
    /// The nodes behind a cut that was made and not removed, whether or not the cut does anything.
    nodes_behind_a_cut: Mutex<BTreeSet<usize>>,
    /// Whether removing the cuts fails, as it does while the cluster's management does not answer.
    refuses_to_remove_cuts: AtomicBool,
    /// Whether a cut is accepted and changes nothing, as on a cluster whose network does not enforce cuts.
    cuts_do_nothing: AtomicBool,
}

impl InProcessFaults {
    /// The faults of `cluster`, with its first controller started.
    fn over(cluster: Arc<Cluster>) -> Self {
        let first_controller = RunningController {
            number: 0,
            task: tokio::spawn(run_controller(Arc::clone(&cluster), 0)),
        };
        Self {
            cluster,
            controller: Mutex::new(first_controller),
            injected: Mutex::new(Vec::new()),
            nodes_behind_a_cut: Mutex::new(BTreeSet::new()),
            refuses_to_remove_cuts: AtomicBool::new(false),
            cuts_do_nothing: AtomicBool::new(false),
        }
    }

    /// Every fault that was carried out so far, in order. A cut that does nothing is among them: the cluster's
    /// management carried it out.
    pub(crate) fn injected(&self) -> Vec<Fault> {
        self.injected.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// How many nodes are behind a cut that was made and not removed.
    pub(crate) fn cuts_in_place(&self) -> usize {
        self.lock_nodes_behind_a_cut().len()
    }

    /// Makes every removal of the cuts fail from now on, or work again.
    pub(crate) fn refuse_to_remove_cuts(&self, refuses: bool) {
        self.refuses_to_remove_cuts.store(refuses, Ordering::Relaxed);
    }

    /// Makes every cut from now on be accepted and change nothing, or cut again.
    pub(crate) fn let_cuts_do_nothing(&self, do_nothing: bool) {
        self.cuts_do_nothing.store(do_nothing, Ordering::Relaxed);
    }

    /// Crashes the replica of `node_number` and starts it again after `downtime`.
    pub(crate) fn crash_for(&self, node_number: usize, downtime: Duration) {
        self.cluster.nodes()[node_number].crash();
        let cluster = Arc::clone(&self.cluster);
        tokio::spawn(async move {
            sleep(downtime).await;
            cluster.nodes()[node_number].restart().await;
        });
    }

    /// Makes the metadata store unavailable, or available again.
    pub(crate) fn set_metadata_store_unavailable(&self, is_unavailable: bool) {
        self.cluster.key_value_store().set_unavailable(is_unavailable);
    }

    /// Locks the set of nodes behind a cut. It is plain data, so a lock poisoned by a panicking test is still good.
    fn lock_nodes_behind_a_cut(&self) -> MutexGuard<'_, BTreeSet<usize>> {
        self.nodes_behind_a_cut.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The number of the node whose address starts with `pod`.
    fn node_of_pod(&self, pod: &str) -> Result<usize, FaultError> {
        for (node_number, node) in self.cluster.nodes().iter().enumerate() {
            if pod_of_address(node.address()) == pod {
                return Ok(node_number);
            }
        }
        Err(FaultError {
            reason: format!("no replica runs in a pod called {pod}"),
        })
    }

    /// Ends the controller called `pod` and starts the next one.
    fn kill_controller(&self, pod: &str) -> Result<(), FaultError> {
        let mut running = self.controller.lock().unwrap_or_else(PoisonError::into_inner);
        if controller_name(running.number) != pod {
            return Err(FaultError {
                reason: format!("no controller runs in a pod called {pod}"),
            });
        }
        running.task.abort();
        running.number += 1;
        running.task = tokio::spawn(run_controller(Arc::clone(&self.cluster), running.number));
        Ok(())
    }

    /// Makes the metadata store unavailable for [`ELECTION_TIME`].
    fn kill_etcd_member(&self) {
        let key_value_store = self.cluster.key_value_store().clone();
        key_value_store.set_unavailable(true);
        tokio::spawn(async move {
            sleep(ELECTION_TIME).await;
            key_value_store.set_unavailable(false);
        });
    }

    /// Puts the nodes `node_numbers` behind a cut, which cuts their links unless cuts do nothing.
    fn cut(&self, node_numbers: &[usize]) {
        let is_enforced = !self.cuts_do_nothing.load(Ordering::Relaxed);
        for node_number in node_numbers {
            self.lock_nodes_behind_a_cut().insert(*node_number);
            if is_enforced {
                self.set_link_cut(*node_number, true);
            }
        }
    }

    /// Cuts or heals the link of the node `node_number`.
    fn set_link_cut(&self, node_number: usize, is_cut: bool) {
        self.cluster.nodes()[node_number].link().set_fault(LinkFault {
            is_cut,
            ..LinkFault::default()
        });
    }
}

/// The name of the `number`-th controller of a cluster, which it writes into the metadata store when it leads.
fn controller_name(number: u64) -> String {
    format!("controller-{number}")
}

/// The zone the node `node_number` is in, as the cluster's layout names it.
fn zone_of_node(node_number: usize) -> String {
    format!("zone-{}", node_number % ZONE_COUNT)
}

/// The numbers of the nodes in `zone`.
fn nodes_of_zone(zone: &str) -> Vec<usize> {
    let mut node_numbers = Vec::new();
    for node_number in 0..NODE_COUNT {
        if zone_of_node(node_number) == zone {
            node_numbers.push(node_number);
        }
    }
    node_numbers
}

/// Runs the `number`-th controller of `cluster` until its task is ended.
async fn run_controller(cluster: Arc<Cluster>, number: u64) {
    let settings = ControllerSettings {
        identity: controller_name(number),
        pass_interval: CONTROLLER_PASS_INTERVAL,
        leadership_duration: CONTROLLER_LEADERSHIP_DURATION,
        request_timeout: CONTROLLER_REQUEST_TIMEOUT,
        ..ControllerSettings::default()
    };
    let controller = Controller::new(
        cluster.metadata().clone(),
        cluster.connector(),
        cluster.clock(),
        settings,
    );
    controller.run().await;
}

#[async_trait]
impl FaultInjector for InProcessFaults {
    /// Carries the fault out on the in-process cluster.
    async fn inject(&self, fault: &Fault) -> Result<(), FaultError> {
        match fault {
            Fault::KillReplica { pod } => self.crash_for(self.node_of_pod(pod)?, RESTART_DELAY),
            Fault::KillController { pod } => self.kill_controller(pod)?,
            Fault::KillEtcdMember { .. } => self.kill_etcd_member(),
            Fault::CutReplica { pod } => self.cut(&[self.node_of_pod(pod)?]),
            Fault::CutZone { zone } => self.cut(&nodes_of_zone(zone)),
        }
        self.injected
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(fault.clone());
        Ok(())
    }

    /// Heals every link, unless a test asked for removals to fail.
    async fn remove_cuts(&self) -> Result<(), FaultError> {
        if self.refuses_to_remove_cuts.load(Ordering::Relaxed) {
            return Err(FaultError {
                reason: "the test cluster was told to refuse".to_owned(),
            });
        }
        for node_number in 0..NODE_COUNT {
            self.set_link_cut(node_number, false);
        }
        self.lock_nodes_behind_a_cut().clear();
        Ok(())
    }

    /// Lists three members, one per zone.
    async fn etcd_members(&self) -> Result<Vec<EtcdMember>, FaultError> {
        let mut members = Vec::new();
        for member_number in 0..ZONE_COUNT {
            members.push(EtcdMember {
                pod: format!("metadata-{member_number}"),
                zone: Some(zone_of_node(member_number)),
            });
        }
        Ok(members)
    }
}

impl Drop for InProcessFaults {
    /// Ends the controller with the cluster it looked after.
    fn drop(&mut self) {
        self.controller
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .task
            .abort();
    }
}

/// Settings of a round that takes a virtual minute: two streams at five appends a second, a fault of the given
/// kinds every ten seconds, cuts of five seconds once they were seen to be in effect, and two minutes to heal.
pub(crate) fn quick_round(kinds: Vec<FaultKind>, dump_directory: &Path) -> RoundSettings {
    RoundSettings {
        duration: Duration::from_secs(60),
        stream_count: 2,
        workload: WorkloadSettings {
            appends_per_second: 5,
            record_bytes: 32,
        },
        nemesis: NemesisSettings {
            kinds,
            fault_interval: Duration::from_secs(10),
            cut_duration: Duration::from_secs(5),
            cut_check: QUICK_CUT_CHECK,
            retries_cuts_not_in_effect: false,
        },
        heal_time_limit: Duration::from_secs(120),
        dump_directory: dump_directory.to_path_buf(),
    }
}
