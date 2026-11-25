//! What the client's tests add to the shared in-process cluster: a stream
//! to write to, clients, and the part of the controller's job they need.

use std::ops::Deref;
use std::time::Duration;

use bitacora_meta::{RetentionPolicy, StreamRecord};
use bitacora_types::{ReplicationSettings, SegmentAddress, SegmentId};

use crate::{BitacoraClient, ClientSettings, WriterSettings};

/// The stream every test writes to.
pub(crate) const STREAM: &str = "orders";

/// An in-process cluster with [`STREAM`] created on it.
pub(crate) struct Cluster {
    cluster: bitacora_testkit::Cluster,
}

impl Deref for Cluster {
    type Target = bitacora_testkit::Cluster;

    fn deref(&self) -> &Self::Target {
        &self.cluster
    }
}

impl Cluster {
    /// Starts `node_count` replicas and creates [`STREAM`] with its first
    /// segment placed on the first three.
    pub(crate) async fn start(node_count: usize) -> Self {
        let cluster = bitacora_testkit::Cluster::start(node_count).await;
        cluster
            .metadata()
            .create_stream(
                STREAM,
                ReplicationSettings::DEFAULT,
                RetentionPolicy::default(),
                Some(cluster.replica_ids(&[0, 1, 2])),
            )
            .await
            .expect("the stream is created");
        Self { cluster }
    }

    /// A client whose writers identify as `writer_identity` and use
    /// `settings` for everything else.
    pub(crate) fn client_with(
        &self,
        writer_identity: &str,
        settings: &WriterSettings,
    ) -> BitacoraClient {
        BitacoraClient::new(
            self.metadata().clone(),
            self.connector(),
            self.clock(),
            ClientSettings {
                writer: WriterSettings {
                    writer_identity: writer_identity.to_owned(),
                    ..settings.clone()
                },
                ..ClientSettings::default()
            },
        )
    }

    /// A client with the default settings.
    pub(crate) fn client(&self, writer_identity: &str) -> BitacoraClient {
        self.client_with(writer_identity, &WriterSettings::default())
    }

    /// Does the controller's job of keeping a standby segment ready.
    pub(crate) async fn place_standby(&self, node_numbers: &[usize]) {
        let standby_replicas = self.replica_ids(node_numbers);
        self.metadata()
            .update_stream(STREAM, |stream| {
                stream.standby_replicas = Some(standby_replicas.clone());
                Ok(())
            })
            .await
            .expect("the standby is placed");
    }

    pub(crate) async fn stream(&self) -> StreamRecord {
        self.metadata()
            .get_stream(STREAM)
            .await
            .expect("the stream exists")
    }

    /// The address of segment `segment_id` of [`STREAM`].
    pub(crate) async fn segment_address(&self, segment_id: u64) -> SegmentAddress {
        SegmentAddress::new(self.stream().await.stream_id, SegmentId::new(segment_id))
    }
}

/// Lets every task that is ready run, and timers up to `duration` fire.
pub(crate) async fn settle(duration: Duration) {
    tokio::time::sleep(duration).await;
}
