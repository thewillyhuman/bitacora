//! What every part of the client needs to reach the cluster.

use std::sync::Arc;

use bitacora_meta::{CopyRecord, MetaError, MetadataStore};
use bitacora_types::replica_api::{ReplicaApi, ReplicaConnector, UnreachableReplica};
use bitacora_types::{Clock, ReplicaId};

/// A replica of a segment, ready to be talked to.
#[derive(Clone)]
pub(crate) struct ConnectedReplica {
    pub replica_id: ReplicaId,
    pub client: Arc<dyn ReplicaApi>,
}

/// The injected interfaces of the client: metadata, network and clock.
pub(crate) struct ClientContext {
    pub metadata: MetadataStore,
    pub connector: Arc<dyn ReplicaConnector>,
    pub clock: Arc<dyn Clock>,
}

impl ClientContext {
    /// Connects to the replicas that hold `copies`, in the same order.
    ///
    /// A replica that is no longer registered gets a handle that fails every
    /// request, so callers treat it like any other replica that is down.
    pub(crate) async fn connect_copies(
        &self,
        copies: &[CopyRecord],
    ) -> Result<Vec<ConnectedReplica>, MetaError> {
        let registered_replicas = self.metadata.list_replicas().await?;
        Ok(copies
            .iter()
            .map(|copy| {
                let registration = registered_replicas
                    .iter()
                    .find(|replica| replica.replica_id == copy.replica_id);
                let client: Arc<dyn ReplicaApi> = match registration {
                    Some(replica) => self
                        .connector
                        .connect(&replica.replica_id, &replica.address),
                    None => Arc::new(UnreachableReplica::new(format!(
                        "replica {} is not registered",
                        copy.replica_id
                    ))),
                };
                ConnectedReplica {
                    replica_id: copy.replica_id.clone(),
                    client,
                }
            })
            .collect())
    }
}
