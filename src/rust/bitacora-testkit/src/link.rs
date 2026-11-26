//! The network path to one node, with switches for what can go wrong on it.
//!
//! # Why this file exists
//!
//! Most of what makes a replicated log hard happens between machines: a request that never arrives, one that arrives
//! late, one that is applied although its answer is lost. The code under test must meet all three, at moments a test
//! chooses. A real network cannot be told to do that, so the cluster's nodes are reached through this stand-in for one:
//! an object that passes a request to the replica behind it unless a switch says otherwise.
//!
//! The link is also where a node is up or down. A crashed machine and a cut cable look the same to a caller, so the
//! same object answers for both.
//!
//! # What it contains
//!
//! - [`LinkFault`]: what is wrong with a link right now: cut, losing answers, slow.
//! - [`Link`]: the path to one node. It implements [`ReplicaApi`], so anything that talks to a replica can be handed a
//!   link instead.
//! - `Process`: what is at the far end of a link: a running replica, or nothing.
//!
//! # How it works
//!
//! A request crosses the link in two steps, and each of the ten operations of [`ReplicaApi`] is those two steps around
//! a call to the replica:
//!
//! 1. [`Link::carry_to_replica`] takes the request to the far end. A cut link fails it at once. A slow link holds it
//!    for the link's delay; when the delay is over the link is looked at again, because the node may have gone down or
//!    the link been cut while the request was on its way. A caller that named the replica it expects is refused if
//!    another replica answers at the address by now.
//! 2. The replica runs the request.
//! 3. [`Link::carry_back`] brings the answer back, unless the link loses answers at that moment: then the caller gets
//!    an error although the request took effect.
//!
//! # Invariants
//!
//! - Every fault is reported as [`ReplicaError::Unavailable`], which is all a real network can say. The reason text is
//!   for the person reading a test failure.
//! - A request that was refused on the way never reached the replica.
//! - A request whose answer was lost was applied in full.
//! - The link's state is plain data behind a lock that is held for one read or one replacement, never across an
//!   `await`.
//! - Nothing here is random and nothing happens by itself: a fault lasts until the caller replaces it.
//!
//! # See also
//!
//! - `connector.rs`: how callers that know a replica by address and identity are given a link.
//! - `node.rs`: the node, which puts a replica behind its link when it starts and takes it away when it crashes.
//! - `link_test.rs`: the tests of each fault.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use bitacora_replica::Replica;
use bitacora_types::replica_api::{
    AppendRequest, AppendResponse, FenceRequest, FenceResponse, ReadRequest, ReadResponse, RepairRequest,
    RepairResponse, ReplicaApi, ReplicaError, ReplicaStatus, SealRequest, SealResponse, SegmentStatus, UploadRequest,
    UploadResponse,
};
use bitacora_types::{ReplicaId, SegmentAddress};

/// Why a request failed on a link that is cut.
const LINK_IS_CUT: &str = "the link is cut";

/// Why a request failed on a link with no process behind it.
const REPLICA_IS_DOWN: &str = "the replica is down";

/// Why a request failed that was addressed to a replica which no longer lives at the address.
const ANOTHER_REPLICA_ANSWERS: &str = "another replica answers at this address";

/// Why a request failed whose answer the link lost.
const ANSWER_WAS_LOST: &str = "the answer was lost";

/// What is wrong with a link right now.
///
/// The default is a healthy link: not cut, losing nothing, no delay. The three faults are independent and may be
/// combined. Set one with [`Link::set_fault`], which replaces the whole value: use struct update syntax on
/// [`Link::fault`] to change one fault and keep the others.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() {
/// use std::time::Duration;
///
/// use bitacora_testkit::{Cluster, LinkFault};
///
/// let cluster = Cluster::start(1).await;
/// let link = cluster.nodes()[0].link();
/// let slow = LinkFault {
///     delay: Duration::from_millis(200),
///     ..LinkFault::default()
/// };
///
/// link.set_fault(slow);
///
/// assert_eq!(link.fault(), slow);
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LinkFault {
    /// Requests fail without reaching the replica, as in a partition.
    pub is_cut: bool,
    /// Requests reach the replica and are applied, but the answers are lost: the caller cannot tell whether its request
    /// took effect.
    pub loses_responses: bool,
    /// How long every request is held before it reaches the replica. Zero for a link that is not slow.
    pub delay: Duration,
}

/// What is at the far end of a link.
///
/// This is the one place that says whether a node is up. A node that crashed and a node that lost its disk are both
/// `Down`; they differ in what their disk holds, which is the node's business (`node.rs`).
enum Process {
    /// A replica process is running and takes the requests the link delivers.
    Running(Arc<Replica>),
    /// No process is running. Requests fail as they would against a machine that is switched off.
    Down,
}

/// The parts of a link that change while a cluster runs.
struct LinkState {
    /// What is at the far end.
    process: Process,
    /// What is wrong with the link.
    fault: LinkFault,
}

/// The network path to one node.
///
/// It implements [`ReplicaApi`] by passing requests to the replica process behind it, unless a fault says otherwise.
/// Every fault is reported the way a real network would report it: as [`ReplicaError::Unavailable`]. A cluster creates
/// one link per node; get it from [`Node::link`](crate::Node::link).
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() {
/// use bitacora_testkit::{Cluster, LinkFault};
/// use bitacora_types::replica_api::{ReplicaApi, ReplicaError};
///
/// let cluster = Cluster::start(1).await;
/// let link = cluster.nodes()[0].link();
/// let reachable = link.status().await;
///
/// link.set_fault(LinkFault {
///     is_cut: true,
///     ..LinkFault::default()
/// });
/// let cut_off = link.status().await;
///
/// assert!(reachable.is_ok());
/// assert!(matches!(cut_off, Err(ReplicaError::Unavailable { .. })));
/// # }
/// ```
pub struct Link {
    /// What is behind the link and what is wrong with it. A lock, because the node, the test and every caller of the
    /// link share it; it is never held across an `await`.
    state: Mutex<LinkState>,
}

impl Link {
    /// Creates a healthy link with `replica` running behind it.
    ///
    /// # Arguments
    ///
    /// * `replica` - The replica process the link delivers requests to.
    ///
    /// # Returns
    ///
    /// The link, with no fault.
    pub(crate) fn new(replica: Arc<Replica>) -> Self {
        Self {
            state: Mutex::new(LinkState {
                process: Process::Running(replica),
                fault: LinkFault::default(),
            }),
        }
    }

    /// Replaces whatever is wrong with the link.
    ///
    /// The new fault applies to every request from now on, and to requests that are on their way: a request held by a
    /// delay is refused on arrival if the link was cut meanwhile, and an answer is lost if the link loses answers when
    /// the answer is ready.
    ///
    /// # Arguments
    ///
    /// * `fault` - The whole new fault. Pass `LinkFault::default()` to heal the link.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_testkit::{Cluster, LinkFault};
    /// use bitacora_types::replica_api::ReplicaApi;
    ///
    /// let cluster = Cluster::start(1).await;
    /// let link = cluster.nodes()[0].link();
    /// link.set_fault(LinkFault {
    ///     loses_responses: true,
    ///     ..LinkFault::default()
    /// });
    ///
    /// link.set_fault(LinkFault::default());
    ///
    /// assert!(link.status().await.is_ok());
    /// # }
    /// ```
    pub fn set_fault(&self, fault: LinkFault) {
        self.lock_state().fault = fault;
    }

    /// Reports the fault currently applied to the link.
    ///
    /// # Returns
    ///
    /// A copy of the fault last set, or the default (no fault) if none was.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use bitacora_testkit::{Cluster, LinkFault};
    ///
    /// let cluster = Cluster::start(1).await;
    ///
    /// let fault = cluster.nodes()[0].link().fault();
    ///
    /// assert_eq!(fault, LinkFault::default());
    /// # }
    /// ```
    #[must_use]
    pub fn fault(&self) -> LinkFault {
        self.lock_state().fault
    }

    /// Puts a running replica behind the link, as when a node starts.
    ///
    /// Replaces whatever was behind the link; a replica that was running there is neither stopped nor told.
    ///
    /// # Arguments
    ///
    /// * `replica` - The replica process that answers at the far end from now on.
    pub(crate) fn start_process(&self, replica: Arc<Replica>) {
        self.lock_state().process = Process::Running(replica);
    }

    /// Takes the process behind the link away and stops it, as when a node crashes.
    ///
    /// The replica's tasks are stopped; requests it is working on are abandoned where they are. Does nothing if no
    /// process is running.
    pub(crate) fn stop_process(&self) {
        let stopped = std::mem::replace(&mut self.lock_state().process, Process::Down);
        if let Process::Running(replica) = stopped {
            replica.stop();
        }
    }

    /// Reports the replica process behind the link, bypassing every fault.
    ///
    /// # Returns
    ///
    /// The running replica, or `None` while no process is running.
    pub(crate) fn running_replica(&self) -> Option<Arc<Replica>> {
        match &self.lock_state().process {
            Process::Running(replica) => Some(Arc::clone(replica)),
            Process::Down => None,
        }
    }

    /// Locks the link's state. The state is plain data replaced in one step, so a lock poisoned by a panicking test
    /// thread still guards a consistent value and is used as it is.
    fn lock_state(&self) -> MutexGuard<'_, LinkState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Carries one request to the far end of the link and reports the replica that is to run it.
    ///
    /// Applies the faults that act on the way there: a cut link refuses, a slow link sleeps for its delay and then
    /// looks at the link again. It does not run the request; the caller does, and passes the replica's answer to
    /// [`Link::carry_back`].
    ///
    /// # Arguments
    ///
    /// * `addressed_to` - The identity of the replica the caller expects behind the link, if it expects one. `None`
    ///   delivers to whatever replica is running.
    ///
    /// # Returns
    ///
    /// The replica that receives the request.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaError::Unavailable`] if the link is cut when the request leaves or when it arrives, if no
    /// process is running when it arrives, or if the replica running there is not the one the request is addressed to.
    /// In every case the replica has not seen the request.
    pub(crate) async fn carry_to_replica(
        &self,
        addressed_to: Option<&ReplicaId>,
    ) -> Result<Arc<Replica>, ReplicaError> {
        let fault_on_departure = self.fault();
        if fault_on_departure.is_cut {
            return Err(unavailable(LINK_IS_CUT));
        }
        if !fault_on_departure.delay.is_zero() {
            tokio::time::sleep(fault_on_departure.delay).await;
        }
        // The node may have gone down, or the link been cut, while the request was on its way.
        if self.fault().is_cut {
            return Err(unavailable(LINK_IS_CUT));
        }
        let Some(replica) = self.running_replica() else {
            return Err(unavailable(REPLICA_IS_DOWN));
        };
        // The node behind the link may have been rebuilt with an empty disk and a new identity since the caller looked
        // it up.
        if addressed_to.is_some_and(|expected_replica_id| expected_replica_id != replica.replica_id()) {
            return Err(unavailable(ANOTHER_REPLICA_ANSWERS));
        }
        Ok(replica)
    }

    /// Carries a replica's answer back to the caller, unless the link loses answers.
    ///
    /// The fault is read now, when the answer is ready, so a link that started to lose answers while the replica was
    /// working loses this one too.
    ///
    /// # Arguments
    ///
    /// * `answer` - What the replica answered, success or error.
    ///
    /// # Returns
    ///
    /// `answer`, unchanged, on a link that does not lose answers.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaError::Unavailable`] in place of the answer if the link loses answers, and the replica's own
    /// error if `answer` is one.
    pub(crate) fn carry_back<Response>(
        &self,
        answer: Result<Response, ReplicaError>,
    ) -> Result<Response, ReplicaError> {
        if self.fault().loses_responses {
            return Err(unavailable(ANSWER_WAS_LOST));
        }
        answer
    }
}

/// Builds the error a caller gets for a request the network did not complete.
fn unavailable(reason: &str) -> ReplicaError {
    ReplicaError::Unavailable {
        reason: reason.to_owned(),
    }
}

#[async_trait]
impl ReplicaApi for Link {
    /// Appends through the link, to whatever replica is running behind it.
    async fn append(&self, request: AppendRequest) -> Result<AppendResponse, ReplicaError> {
        let replica = self.carry_to_replica(None).await?;
        self.carry_back(replica.append(request).await)
    }

    /// Reads through the link, from whatever replica is running behind it.
    async fn read(&self, request: ReadRequest) -> Result<ReadResponse, ReplicaError> {
        let replica = self.carry_to_replica(None).await?;
        self.carry_back(replica.read(request).await)
    }

    /// Fences through the link.
    async fn fence(&self, request: FenceRequest) -> Result<FenceResponse, ReplicaError> {
        let replica = self.carry_to_replica(None).await?;
        self.carry_back(replica.fence(request).await)
    }

    /// Seals through the link.
    async fn seal(&self, request: SealRequest) -> Result<SealResponse, ReplicaError> {
        let replica = self.carry_to_replica(None).await?;
        self.carry_back(replica.seal(request).await)
    }

    /// Asks for a repair through the link.
    async fn repair_segment(&self, request: RepairRequest) -> Result<RepairResponse, ReplicaError> {
        let replica = self.carry_to_replica(None).await?;
        self.carry_back(replica.repair_segment(request).await)
    }

    /// Asks for an upload through the link.
    async fn upload_segment(&self, request: UploadRequest) -> Result<UploadResponse, ReplicaError> {
        let replica = self.carry_to_replica(None).await?;
        self.carry_back(replica.upload_segment(request).await)
    }

    /// Deletes a segment copy through the link.
    async fn delete_segment(&self, segment: SegmentAddress) -> Result<(), ReplicaError> {
        let replica = self.carry_to_replica(None).await?;
        self.carry_back(replica.delete_segment(segment).await)
    }

    /// Lists the segment copies through the link.
    async fn list_segments(&self) -> Result<Vec<SegmentStatus>, ReplicaError> {
        let replica = self.carry_to_replica(None).await?;
        self.carry_back(replica.list_segments().await)
    }

    /// Asks for the replica's status through the link.
    async fn status(&self) -> Result<ReplicaStatus, ReplicaError> {
        let replica = self.carry_to_replica(None).await?;
        self.carry_back(replica.status().await)
    }

    /// Asks the replica to drain, through the link.
    async fn drain(&self) -> Result<(), ReplicaError> {
        let replica = self.carry_to_replica(None).await?;
        self.carry_back(replica.drain().await)
    }
}

#[cfg(test)]
#[path = "link_test.rs"]
mod tests;
