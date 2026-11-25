//! The network path to one replica, with switches for what can go wrong.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use bitacora_replica::Replica;
use bitacora_types::SegmentAddress;
use bitacora_types::replica_api::{
    AppendRequest, AppendResponse, FenceRequest, FenceResponse, ReadRequest, ReadResponse,
    ReplicaApi, ReplicaError, ReplicaStatus, SealRequest, SealResponse, SegmentStatus,
};

/// What is wrong with a link right now.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LinkFault {
    /// Requests fail without reaching the replica, as in a partition.
    pub is_cut: bool,
    /// Requests reach the replica and are applied, but the answers are
    /// lost: the caller cannot tell whether its request took effect.
    pub loses_responses: bool,
    /// How long every request is held before it reaches the replica.
    pub delay: Duration,
}

struct LinkState {
    /// The process behind the link. `None` while the node is down.
    replica: Option<Arc<Replica>>,
    fault: LinkFault,
}

/// The network path to one replica.
///
/// It implements [`ReplicaApi`] by passing requests to the replica process
/// behind it, unless a fault says otherwise. Every fault is reported the
/// way a real network would: as [`ReplicaError::Unavailable`].
pub struct Link {
    state: Mutex<LinkState>,
}

impl Link {
    pub(crate) fn new(replica: Arc<Replica>) -> Self {
        Self {
            state: Mutex::new(LinkState {
                replica: Some(replica),
                fault: LinkFault::default(),
            }),
        }
    }

    /// Replaces whatever is wrong with the link.
    pub fn set_fault(&self, fault: LinkFault) {
        self.lock().fault = fault;
    }

    /// The fault currently applied to the link.
    #[must_use]
    pub fn fault(&self) -> LinkFault {
        self.lock().fault
    }

    /// Puts a process behind the link, or takes it away.
    pub(crate) fn set_replica(&self, replica: Option<Arc<Replica>>) {
        self.lock().replica = replica;
    }

    pub(crate) fn replica(&self) -> Option<Arc<Replica>> {
        self.lock().replica.clone()
    }

    fn lock(&self) -> MutexGuard<'_, LinkState> {
        // The state is plain data replaced in one step.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Carries one request to the replica and its answer back, applying the
    /// link's fault on the way.
    async fn carry<Response, Request>(
        &self,
        request: impl FnOnce(Arc<Replica>) -> Request,
    ) -> Result<Response, ReplicaError>
    where
        Request: Future<Output = Result<Response, ReplicaError>>,
    {
        let fault = self.fault();
        if fault.is_cut {
            return Err(unavailable("the link is cut"));
        }
        if !fault.delay.is_zero() {
            tokio::time::sleep(fault.delay).await;
        }
        // The node may have gone down, or the link been cut, while the
        // request was on its way.
        let (replica, fault_on_arrival) = {
            let state = self.lock();
            (state.replica.clone(), state.fault)
        };
        if fault_on_arrival.is_cut {
            return Err(unavailable("the link is cut"));
        }
        let Some(replica) = replica else {
            return Err(unavailable("the replica is down"));
        };
        let answer = request(replica).await;
        if self.fault().loses_responses {
            return Err(unavailable("the answer was lost"));
        }
        answer
    }
}

fn unavailable(reason: &str) -> ReplicaError {
    ReplicaError::Unavailable {
        reason: reason.to_owned(),
    }
}

#[async_trait]
impl ReplicaApi for Link {
    async fn append(&self, request: AppendRequest) -> Result<AppendResponse, ReplicaError> {
        self.carry(|replica| async move { replica.append(request).await })
            .await
    }

    async fn read(&self, request: ReadRequest) -> Result<ReadResponse, ReplicaError> {
        self.carry(|replica| async move { replica.read(request).await })
            .await
    }

    async fn fence(&self, request: FenceRequest) -> Result<FenceResponse, ReplicaError> {
        self.carry(|replica| async move { replica.fence(request).await })
            .await
    }

    async fn seal(&self, request: SealRequest) -> Result<SealResponse, ReplicaError> {
        self.carry(|replica| async move { replica.seal(request).await })
            .await
    }

    async fn delete_segment(&self, segment: SegmentAddress) -> Result<(), ReplicaError> {
        self.carry(|replica| async move { replica.delete_segment(segment).await })
            .await
    }

    async fn list_segments(&self) -> Result<Vec<SegmentStatus>, ReplicaError> {
        self.carry(|replica| async move { replica.list_segments().await })
            .await
    }

    async fn status(&self) -> Result<ReplicaStatus, ReplicaError> {
        self.carry(|replica| async move { replica.status().await })
            .await
    }

    async fn drain(&self) -> Result<(), ReplicaError> {
        self.carry(|replica| async move { replica.drain().await })
            .await
    }
}
