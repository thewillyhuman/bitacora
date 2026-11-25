//! The entry point of the client library.

use std::sync::Arc;

use bitacora_artifact::ArtifactStore;
use bitacora_meta::MetadataStore;
use bitacora_types::Clock;
use bitacora_types::replica_api::ReplicaConnector;

use crate::admin::StreamAdmin;
use crate::context::ClientContext;
use crate::error::OpenError;
use crate::reader::StreamReader;
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
                artifact_store: None,
            }),
            settings,
        }
    }

    /// Gives the client the object store that sealed segments are tiered
    /// to (design, section 10, "Tiering"), so that its readers can read
    /// segments whose copies the replicas have dropped. Without one, such
    /// a read fails as [`ReadError::Unavailable`](crate::ReadError).
    ///
    /// Call it right after [`BitacoraClient::new`]: writers and readers
    /// opened before keep reading from replicas only.
    #[must_use]
    pub fn with_artifact_store(self, artifact_store: Arc<dyn ArtifactStore>) -> Self {
        Self {
            context: Arc::new(ClientContext {
                metadata: self.context.metadata.clone(),
                connector: Arc::clone(&self.context.connector),
                clock: Arc::clone(&self.context.clock),
                artifact_store: Some(artifact_store),
            }),
            settings: self.settings,
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

    /// Returns the administrative interface: creating, describing,
    /// truncating and deleting streams, and registering materializers.
    #[must_use]
    pub fn admin(&self) -> StreamAdmin {
        StreamAdmin::new(Arc::clone(&self.context))
    }

    /// Returns a reader of `stream_name`. Any number of readers can follow
    /// the same stream. Nothing is contacted until the reader is used.
    #[must_use]
    pub fn open_reader(&self, stream_name: &str) -> StreamReader {
        StreamReader::new(
            Arc::clone(&self.context),
            self.settings.reader.clone(),
            stream_name,
        )
    }
}
