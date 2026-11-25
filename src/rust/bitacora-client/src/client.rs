//! The entry point of the client library.

use std::sync::Arc;

use bitacora_meta::MetadataStore;
use bitacora_types::Clock;
use bitacora_types::replica_api::ReplicaConnector;

use crate::context::ClientContext;
use crate::error::OpenError;
use crate::settings::{ReaderSettings, TakeoverPolicy, WriterSettings};
use crate::writer::StreamWriter;

/// The settings of everything a client opens.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClientSettings {
    /// How writers opened by this client behave.
    pub writer: WriterSettings,
    /// How readers opened by this client behave.
    pub reader: ReaderSettings,
}

/// A handle to a bitacora cluster.
///
/// It is built from the three injected interfaces of the design: the
/// metadata store, the way to reach replicas, and the clock. Production code
/// passes the etcd store, the gRPC connector and the system clock; tests and
/// the simulator pass in-memory ones.
#[derive(Clone)]
pub struct BitacoraClient {
    context: Arc<ClientContext>,
    settings: ClientSettings,
}

impl BitacoraClient {
    /// Creates a client over the given interfaces.
    #[must_use]
    pub fn new(
        metadata: MetadataStore,
        connector: Arc<dyn ReplicaConnector>,
        clock: Arc<dyn Clock>,
        settings: ClientSettings,
    ) -> Self {
        Self {
            context: Arc::new(ClientContext {
                metadata,
                connector,
                clock,
            }),
            settings,
        }
    }

    /// Becomes the writer of `stream_name`.
    ///
    /// This runs the takeover protocol (design, section 9): it claims the
    /// next epoch, fences and seals whatever segment the previous writer
    /// left open, and opens a new one. bitacora does not choose which
    /// compute node should be the writer; it guarantees that whoever calls
    /// this last wins cleanly.
    ///
    /// # Errors
    ///
    /// Returns [`OpenError`] if the stream does not exist, another candidate
    /// claimed it first, a live writer still holds the lease and `policy`
    /// says to wait, or recovery could not reach enough replicas in time.
    pub async fn open_writer(
        &self,
        stream_name: &str,
        policy: TakeoverPolicy,
    ) -> Result<StreamWriter, OpenError> {
        StreamWriter::open(
            Arc::clone(&self.context),
            self.settings.writer.clone(),
            stream_name,
            policy,
        )
        .await
    }
}
