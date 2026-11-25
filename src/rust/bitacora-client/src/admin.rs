//! Stream administration (design, section 13, "API surface").

use std::collections::BTreeSet;
use std::sync::Arc;

use bitacora_meta::{
    CheckpointRecord, MetaError, PlacementPolicy, RetentionPolicy, StreamRecord, choose_replica_set,
};
use bitacora_types::{ReplicationSettings, SequenceNumber};

use crate::context::ClientContext;

/// How a new stream is replicated and how long it keeps its records.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StreamOptions {
    /// How many replicas hold each segment and how many commit a record.
    pub replication: ReplicationSettings,
    /// How long records nothing needs are kept.
    pub retention: RetentionPolicy,
}

/// Everything the metadata store knows about one stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamDescription {
    /// The stream's record: epoch, writer, segments, low-water mark.
    pub stream: StreamRecord,
    /// The checkpoints of the materializers registered on the stream.
    pub checkpoints: Vec<CheckpointRecord>,
}

/// Creates, describes, truncates and deletes streams, and registers the
/// materializers whose checkpoints decide what may be truncated.
#[derive(Clone)]
pub struct StreamAdmin {
    context: Arc<ClientContext>,
}

impl StreamAdmin {
    pub(crate) const fn new(context: Arc<ClientContext>) -> Self {
        Self { context }
    }

    /// Creates a stream and places its first segment, so that a writer can
    /// open it at once. If too few replicas are registered right now, the
    /// stream is created anyway and the controller places the segment as
    /// soon as it can.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::StreamAlreadyExists`] or
    /// [`MetaError::InvalidName`].
    pub async fn create_stream(
        &self,
        name: &str,
        options: StreamOptions,
    ) -> Result<StreamRecord, MetaError> {
        let registered_replicas = self.context.metadata.list_replicas().await?;
        let first_replica_set = choose_replica_set(
            &registered_replicas,
            options.replication.replica_count(),
            self.context.clock.now(),
            PlacementPolicy::default(),
            &BTreeSet::new(),
        );
        if let Err(placement_error) = &first_replica_set {
            tracing::warn!(
                stream = name,
                %placement_error,
                "created without a first segment; the controller will place it"
            );
        }
        self.context
            .metadata
            .create_stream(
                name,
                options.replication,
                options.retention,
                first_replica_set.ok(),
            )
            .await
    }

    /// Describes a stream and its materializers.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::StreamNotFound`] if no stream has this name.
    pub async fn describe_stream(&self, name: &str) -> Result<StreamDescription, MetaError> {
        let stream = self.context.metadata.get_stream(name).await?;
        let checkpoints = self
            .context
            .metadata
            .list_checkpoints(stream.stream_id)
            .await?;
        Ok(StreamDescription {
            stream,
            checkpoints,
        })
    }

    /// Lists every stream, sorted by name.
    ///
    /// # Errors
    ///
    /// Fails if the metadata store cannot be reached.
    pub async fn list_streams(&self) -> Result<Vec<StreamRecord>, MetaError> {
        self.context.metadata.list_streams().await
    }

    /// Allows every record below `sequence` to be deleted, as far as the
    /// registered materializers' checkpoints and the retention policy
    /// permit. The position only ever moves forward: asking for an earlier
    /// one changes nothing.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::StreamNotFound`] if no stream has this name.
    pub async fn truncate_before(
        &self,
        name: &str,
        sequence: SequenceNumber,
    ) -> Result<(), MetaError> {
        self.context
            .metadata
            .update_stream(name, |stream| {
                stream.truncate_before = Some(
                    stream
                        .truncate_before
                        .map_or(sequence, |existing| existing.max(sequence)),
                );
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Deletes a stream. Its segments are removed by the controller.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::StreamNotFound`] if no stream has this name.
    pub async fn delete_stream(&self, name: &str) -> Result<(), MetaError> {
        self.context.metadata.delete_stream(name).await?;
        Ok(())
    }

    /// Registers a materializer. From then on the log keeps every record
    /// above its checkpoint: a registered materializer that stops
    /// checkpointing pins the log until it is unregistered.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::StreamNotFound`] or [`MetaError::InvalidName`].
    pub async fn register_materializer(
        &self,
        stream_name: &str,
        materializer: &str,
    ) -> Result<CheckpointRecord, MetaError> {
        self.context
            .metadata
            .register_materializer(stream_name, materializer)
            .await
    }

    /// Unregisters a materializer, so that it no longer pins the log.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::StreamNotFound`] if no stream has this name.
    pub async fn unregister_materializer(
        &self,
        stream_name: &str,
        materializer: &str,
    ) -> Result<(), MetaError> {
        self.context
            .metadata
            .unregister_materializer(stream_name, materializer)
            .await
    }
}
