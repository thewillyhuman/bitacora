//! An in-process cluster for the client's tests: real replicas on in-memory
//! disks, an in-memory metadata store, and links that can be cut.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bitacora_meta::{
    MemoryKeyValueStore, MetadataStore, RECORD_SCHEMA_VERSION, ReplicaRecord, RetentionPolicy,
    StreamRecord,
};
use bitacora_replica::{Replica, ReplicaSettings};
use bitacora_segment::MemoryDisk;
use bitacora_segment::format::{Batch, decode_frames};
use bitacora_types::replica_api::{
    AppendRequest, AppendResponse, FenceRequest, FenceResponse, ReadLimit, ReadRequest,
    ReadResponse, ReplicaApi, ReplicaError, ReplicaStatus, SealRequest, SealResponse,
    SegmentStatus, StaticReplicaConnector,
};
use bitacora_types::{
    Clock, ReplicaId, ReplicationSettings, SegmentAddress, SegmentId, SequenceNumber, Timestamp,
};
use tokio::time::Instant;

use crate::{BitacoraClient, ClientSettings, WriterSettings};

/// The stream every test writes to.
pub(crate) const STREAM: &str = "orders";

/// Wall-clock time that follows Tokio's clock, so that a paused test
/// runtime moves leases and timers together.
pub(crate) struct VirtualClock {
    started_at: Instant,
}

impl VirtualClock {
    const START: Timestamp = Timestamp::from_unix_millis(1_000_000);

    fn new() -> Self {
        Self {
            started_at: Instant::now(),
        }
    }
}

impl Clock for VirtualClock {
    fn now(&self) -> Timestamp {
        Self::START.plus(self.started_at.elapsed())
    }
}

/// The network path to one replica, with a switch that cuts it.
pub(crate) struct Link {
    replica: Arc<Replica>,
    is_cut: AtomicBool,
}

impl Link {
    fn check(&self) -> Result<(), ReplicaError> {
        if self.is_cut.load(Ordering::Relaxed) {
            return Err(ReplicaError::Unavailable {
                reason: "the link is cut".to_owned(),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl ReplicaApi for Link {
    async fn append(&self, request: AppendRequest) -> Result<AppendResponse, ReplicaError> {
        self.check()?;
        self.replica.append(request).await
    }

    async fn read(&self, request: ReadRequest) -> Result<ReadResponse, ReplicaError> {
        self.check()?;
        self.replica.read(request).await
    }

    async fn fence(&self, request: FenceRequest) -> Result<FenceResponse, ReplicaError> {
        self.check()?;
        self.replica.fence(request).await
    }

    async fn seal(&self, request: SealRequest) -> Result<SealResponse, ReplicaError> {
        self.check()?;
        self.replica.seal(request).await
    }

    async fn delete_segment(&self, segment: SegmentAddress) -> Result<(), ReplicaError> {
        self.check()?;
        self.replica.delete_segment(segment).await
    }

    async fn list_segments(&self) -> Result<Vec<SegmentStatus>, ReplicaError> {
        self.check()?;
        self.replica.list_segments().await
    }

    async fn status(&self) -> Result<ReplicaStatus, ReplicaError> {
        self.check()?;
        self.replica.status().await
    }

    async fn drain(&self) -> Result<(), ReplicaError> {
        self.check()?;
        self.replica.drain().await
    }
}

/// One storage node of the test cluster.
pub(crate) struct Node {
    pub replica_id: ReplicaId,
    pub replica: Arc<Replica>,
    link: Arc<Link>,
}

impl Node {
    /// Cuts or restores the network path to this node.
    pub(crate) fn set_reachable(&self, is_reachable: bool) {
        self.link.is_cut.store(!is_reachable, Ordering::Relaxed);
    }

    /// The end of this node's copy of `segment`, if it has one.
    pub(crate) async fn local_end(&self, segment: SegmentAddress) -> Option<SequenceNumber> {
        self.replica
            .list_segments()
            .await
            .expect("the replica is running")
            .into_iter()
            .find(|status| status.address == segment)
            .and_then(|status| status.local_end)
    }

    /// The sequence numbers this node serves to readers from `segment`.
    pub(crate) async fn readable_sequences(&self, segment: SegmentAddress) -> Vec<u64> {
        let response = self
            .replica
            .read(ReadRequest {
                segment,
                from_sequence: SequenceNumber::FIRST,
                max_bytes: usize::MAX,
                wait_for_records: None,
                limit: ReadLimit::DurableSequence,
            })
            .await
            .expect("the replica holds the segment");
        decode_frames(segment, &response.batch_frames)
            .expect("valid frames")
            .iter()
            .flat_map(Batch::records)
            .map(|record| record.sequence.get())
            .collect()
    }
}

/// An in-process cluster.
pub(crate) struct Cluster {
    pub key_value_store: MemoryKeyValueStore,
    pub metadata: MetadataStore,
    pub nodes: Vec<Node>,
    clock: Arc<VirtualClock>,
    connector: Arc<StaticReplicaConnector>,
}

impl Cluster {
    /// Starts `node_count` replicas named `replica-0`, `replica-1`, ... and
    /// creates [`STREAM`] with its first segment placed on the first three.
    pub(crate) async fn start(node_count: usize) -> Self {
        let key_value_store = MemoryKeyValueStore::new();
        let metadata = MetadataStore::new(Arc::new(key_value_store.clone()));
        let clock = Arc::new(VirtualClock::new());
        let mut connector = StaticReplicaConnector::new();
        let mut nodes = Vec::new();
        for node_number in 0..node_count {
            let replica_id =
                ReplicaId::new(format!("replica-{node_number}")).expect("valid identifier");
            let replica = Arc::new(
                Replica::open(
                    Arc::new(MemoryDisk::new()),
                    replica_id.clone(),
                    ReplicaSettings {
                        preallocated_segment_bytes: 4 * 1024,
                        ..ReplicaSettings::default()
                    },
                    Arc::new(|_segment, _reason| {}),
                )
                .await
                .expect("the replica opens"),
            );
            let link = Arc::new(Link {
                replica: Arc::clone(&replica),
                is_cut: AtomicBool::new(false),
            });
            let address = format!("{replica_id}:7000");
            connector.register(address.clone(), Arc::clone(&link) as Arc<dyn ReplicaApi>);
            metadata
                .put_replica(&ReplicaRecord {
                    schema_version: RECORD_SCHEMA_VERSION,
                    replica_id: replica_id.clone(),
                    address,
                    failure_domains: Vec::new(),
                    capacity_bytes: 1 << 40,
                    free_bytes: 1 << 40,
                    is_draining: false,
                    heartbeat_at: clock.now(),
                })
                .await
                .expect("the store is available");
            nodes.push(Node {
                replica_id,
                replica,
                link,
            });
        }

        let cluster = Self {
            key_value_store,
            metadata,
            nodes,
            clock,
            connector: Arc::new(connector),
        };
        cluster
            .metadata
            .create_stream(
                STREAM,
                ReplicationSettings::DEFAULT,
                RetentionPolicy::default(),
                Some(cluster.replica_ids(&[0, 1, 2])),
            )
            .await
            .expect("the stream is created");
        cluster
    }

    /// A client whose writers identify as `writer_identity` and use
    /// `settings` for everything else.
    pub(crate) fn client_with(
        &self,
        writer_identity: &str,
        settings: &WriterSettings,
    ) -> BitacoraClient {
        BitacoraClient::new(
            self.metadata.clone(),
            Arc::clone(&self.connector) as _,
            Arc::clone(&self.clock) as _,
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

    /// The identifiers of the nodes at `node_numbers`.
    pub(crate) fn replica_ids(&self, node_numbers: &[usize]) -> Vec<ReplicaId> {
        node_numbers
            .iter()
            .map(|node_number| self.nodes[*node_number].replica_id.clone())
            .collect()
    }

    /// Does the controller's job of keeping a standby segment ready.
    pub(crate) async fn place_standby(&self, node_numbers: &[usize]) {
        let standby_replicas = self.replica_ids(node_numbers);
        self.metadata
            .update_stream(STREAM, |stream| {
                stream.standby_replicas = Some(standby_replicas.clone());
                Ok(())
            })
            .await
            .expect("the standby is placed");
    }

    pub(crate) async fn stream(&self) -> StreamRecord {
        self.metadata
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
