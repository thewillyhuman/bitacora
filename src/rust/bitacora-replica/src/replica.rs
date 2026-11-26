//! The replica: every segment copy on one disk, behind one interface.
//!
//! # Why this file exists
//!
//! A storage node holds copies of many segments of many streams, and everything that talks to it (writers,
//! readers, recovering writers, the controller, other replicas) talks to the node, not to a copy. Something
//! has to be the node: find the copies on the disk when the process starts, route each request to the
//! copy it is about, create a copy the first time a segment is mentioned, and carry the few facts that
//! belong to the node as a whole, such as its identity and whether it is draining. [`Replica`] is that
//! something. It is free of any transport: the `bita-replica` daemon puts a gRPC server in front of it,
//! and tests and the simulator call it directly.
//!
//! # What it contains
//!
//! - [`Replica`]: the node, with how it is opened, stopped and scrubbed, and how its host learns that the
//!   disk refused a write.
//! - `Registry`: the tasks of the node's segment copies.
//! - `replica/api.rs`: the operations other programs call, as the `ReplicaApi` implementation.
//! - `replica/admission.rs`: the decision whether the disk takes more data.
//!
//! # How it works
//!
//! **Identity.** The identity belongs to the data directory, not to the machine. An empty directory
//! adopts the identity the replica is opened with and stores it; a directory used before keeps the one it
//! has, whatever is proposed. A replica whose disk was wiped therefore comes back under a new identity and
//! can never claim copies it no longer holds.
//!
//! **Opening.** The directory is listed once. Scratch files of repairs that were interrupted are removed:
//! they hold nothing that is not elsewhere. Every segment that has a records file or a state file gets its
//! copy loaded, which scans and verifies the records, and a task of its own.
//!
//! **Routing.** The registry maps a segment's address to the handle of its task. A request for a segment
//! the replica has nothing for either finds nothing (reads, uploads, deletes) or starts a task with an
//! empty copy (appends, fences, seals, repairs), depending on the operation.
//!
//! **Stopping.** [`Replica::stop`] ends every task at once, as a killed process would, and refuses every
//! later request. Nothing is flushed or tidied: whatever was acknowledged is on the disk already.
//!
//! # Invariants
//!
//! - One task per segment copy, registered under the segment's address. No task is started after `stop`.
//! - The registry lock is held only for single map operations, never across an `await`.
//! - The replica's identity never changes after it is opened.
//! - Draining and stopping are two separate switches: a draining replica still serves reads and accepts
//!   fences and seals; a stopped one answers nothing.
//! - Looking up a segment's task is on the path of every append and read: one lock, one map lookup and
//!   one clone of a handle, with no allocation.
//!
//! # See also
//!
//! - `replica/api.rs` and `replica/admission.rs`: the rest of the replica.
//! - `actor.rs`: the tasks the registry holds, and `copy.rs`: what they own.
//! - `paths.rs`: the layout of the data directory.
//! - `write_failure.rs`: what the host is told when the disk refuses a write.

use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bitacora_artifact::ArtifactStore;
use bitacora_segment::Disk;
use bitacora_types::replica_api::{ReplicaConnector, ReplicaError};
use bitacora_types::{ReplicaId, SegmentAddress};
use bytes::Bytes;
use tokio::task::JoinSet;

use crate::actor::SegmentHandle;
use crate::copy::SegmentCopy;
use crate::paths::{REPLICA_ID_PATH, is_repair_scratch_path, stored_segment_addresses};
use crate::settings::ReplicaSettings;
use crate::write_failure::{WriteFailure, WriteFailureReport};

mod admission;
mod api;

use admission::SpaceMeasurement;

/// The segment tasks of a replica. Dropping it stops them all.
struct Registry {
    /// The handle of each segment's task, by the segment's address.
    handles_by_address: BTreeMap<SegmentAddress, SegmentHandle>,
    /// The tasks themselves. Aborting the set ends them.
    tasks: JoinSet<()>,
}

/// One storage node: it appends batches to segment files, flushes them and serves them back. It makes no
/// decisions about ordering, membership or leadership (design, section 7).
///
/// Open one with [`Replica::open`] on the disk that holds its data directory, and call it through
/// `bitacora_types::replica_api::ReplicaApi`, which it implements. A replica is shared behind an `Arc`
/// and may be called from any number of tasks at once.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::sync::Arc;
///
/// use bitacora_replica::{Replica, ReplicaSettings};
/// use bitacora_segment::MemoryDisk;
/// use bitacora_types::ReplicaId;
/// use bitacora_types::replica_api::{ReplicaApi, StaticReplicaConnector};
///
/// let replica = Replica::open(
///     Arc::new(MemoryDisk::new()),
///     ReplicaId::new("replica-a")?,
///     ReplicaSettings::default(),
///     Arc::new(StaticReplicaConnector::new()),
/// )
/// .await?;
///
/// let stored_segments = replica.list_segments().await?;
///
/// assert!(stored_segments.is_empty());
/// # Ok(())
/// # }
/// ```
pub struct Replica {
    /// The identity of the data directory this replica was opened on.
    replica_id: ReplicaId,
    /// The disk that holds the data directory.
    disk: Arc<dyn Disk>,
    /// How the replica uses its disk, its peers and the object store.
    settings: ReplicaSettings,
    /// How this replica reaches its peers when it repairs a copy.
    connector: Arc<dyn ReplicaConnector>,
    /// Where this replica uploads sealed segments to. A replica without one refuses uploads and does
    /// everything else.
    artifact_store: Option<Arc<dyn ArtifactStore>>,
    /// The tasks of the segment copies. A plain mutex: it is held for single map operations only.
    registry: Mutex<Registry>,
    /// Whether the replica is being drained for maintenance: it takes no new data, and writers roll away.
    is_draining: AtomicBool,
    /// Whether the replica was stopped. A stopped replica answers nothing.
    is_stopped: AtomicBool,
    /// The last measurement of the disk's free space, reused for a short while so that appends do not
    /// each ask the file system.
    last_space_measurement: Mutex<Option<SpaceMeasurement>>,
    /// Where the segment tasks record that the disk refused a write.
    write_failures: WriteFailureReport,
}

impl Replica {
    /// Opens the replica that lives on `disk` and loads every segment copy found there.
    ///
    /// Reads or stores the identity, lists the data directory, removes what interrupted repairs left
    /// behind, and scans and verifies every records file, so opening takes time in proportion to the data
    /// on the disk. See the file header for how the identity is chosen.
    ///
    /// # Arguments
    ///
    /// * `disk` - The disk that holds the data directory.
    /// * `proposed_replica_id` - The identity to store if the directory has none yet. A directory that was
    ///   used before keeps its own.
    /// * `settings` - How the replica uses its disk, its peers and the object store.
    /// * `connector` - How the replica reaches its peers when it is asked to repair a copy.
    ///
    /// # Returns
    ///
    /// The replica, ready to take requests. Copies that could not be read or verified are loaded as
    /// damaged and reported in their status; they do not keep the replica from opening.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaError::Storage`] if the data directory cannot be listed or cleaned, or if the
    /// identity cannot be read, is not a valid identifier, or cannot be stored.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use std::sync::Arc;
    ///
    /// use bitacora_replica::{Replica, ReplicaSettings};
    /// use bitacora_segment::MemoryDisk;
    /// use bitacora_types::ReplicaId;
    /// use bitacora_types::replica_api::StaticReplicaConnector;
    ///
    /// let disk = MemoryDisk::new();
    /// let first_start = Replica::open(
    ///     Arc::new(disk.clone()),
    ///     ReplicaId::new("replica-a")?,
    ///     ReplicaSettings::default(),
    ///     Arc::new(StaticReplicaConnector::new()),
    /// )
    /// .await?;
    /// first_start.stop();
    ///
    /// // The directory keeps its identity, whatever is proposed later.
    /// let second_start = Replica::open(
    ///     Arc::new(disk),
    ///     ReplicaId::new("replica-b")?,
    ///     ReplicaSettings::default(),
    ///     Arc::new(StaticReplicaConnector::new()),
    /// )
    /// .await?;
    ///
    /// assert_eq!(second_start.replica_id().as_str(), "replica-a");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn open(
        disk: Arc<dyn Disk>,
        proposed_replica_id: ReplicaId,
        settings: ReplicaSettings,
        connector: Arc<dyn ReplicaConnector>,
    ) -> Result<Self, ReplicaError> {
        let replica_id = adopt_identity(disk.as_ref(), proposed_replica_id).await?;
        let stored_paths = match disk.list_files().await {
            Ok(stored_paths) => stored_paths,
            Err(io_error) => return Err(directory_failure("list", &io_error)),
        };
        remove_repair_leftovers(disk.as_ref(), &stored_paths).await?;

        let write_failures = WriteFailureReport::new();
        let registry = load_stored_copies(&disk, &stored_paths, settings, &write_failures).await;
        tracing::info!(
            replica_id = %replica_id,
            segment_count = registry.handles_by_address.len(),
            "replica opened"
        );
        Ok(Self {
            replica_id,
            disk,
            settings,
            connector,
            artifact_store: None,
            registry: Mutex::new(registry),
            is_draining: AtomicBool::new(false),
            is_stopped: AtomicBool::new(false),
            last_space_measurement: Mutex::new(None),
            write_failures,
        })
    }

    /// Gives the replica the object store it uploads sealed segments to (design, section 10, "Tiering").
    ///
    /// Without one, the replica refuses `ReplicaApi::upload_segment` and does everything else. Call it
    /// right after [`Replica::open`], before the replica is shared.
    ///
    /// # Arguments
    ///
    /// * `artifact_store` - The object store. It must be the one the controller and the readers of the
    ///   cluster use.
    ///
    /// # Returns
    ///
    /// The same replica, now able to upload.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use std::sync::Arc;
    ///
    /// use bitacora_artifact::MemoryArtifactStore;
    /// use bitacora_replica::{Replica, ReplicaSettings};
    /// use bitacora_segment::MemoryDisk;
    /// use bitacora_types::ReplicaId;
    /// use bitacora_types::replica_api::StaticReplicaConnector;
    ///
    /// let replica = Replica::open(
    ///     Arc::new(MemoryDisk::new()),
    ///     ReplicaId::new("replica-a")?,
    ///     ReplicaSettings::default(),
    ///     Arc::new(StaticReplicaConnector::new()),
    /// )
    /// .await?
    /// .with_artifact_store(Arc::new(MemoryArtifactStore::new()));
    ///
    /// assert_eq!(replica.replica_id().as_str(), "replica-a");
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_artifact_store(mut self, artifact_store: Arc<dyn ArtifactStore>) -> Self {
        self.artifact_store = Some(artifact_store);
        self
    }

    /// Reports the identity of this replica's data directory.
    ///
    /// # Returns
    ///
    /// The identity the directory was first opened with. It is what the replica registers under and what
    /// requests must name.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use std::sync::Arc;
    /// # use bitacora_replica::{Replica, ReplicaSettings};
    /// # use bitacora_segment::MemoryDisk;
    /// # use bitacora_types::ReplicaId;
    /// # use bitacora_types::replica_api::StaticReplicaConnector;
    /// let replica = Replica::open(
    ///     Arc::new(MemoryDisk::new()),
    ///     ReplicaId::new("replica-a")?,
    ///     ReplicaSettings::default(),
    ///     Arc::new(StaticReplicaConnector::new()),
    /// )
    /// .await?;
    ///
    /// assert_eq!(replica.replica_id().as_str(), "replica-a");
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub const fn replica_id(&self) -> &ReplicaId {
        &self.replica_id
    }

    /// Stops every segment task at once, as when the process is killed.
    ///
    /// Requests in flight get no answer, nothing more reaches the disk, and every later request fails as
    /// [`ReplicaError::Unavailable`]. What was flushed stays on the disk for the next [`Replica::open`].
    /// Stopping twice is harmless.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use std::sync::Arc;
    /// # use bitacora_replica::{Replica, ReplicaSettings};
    /// # use bitacora_segment::MemoryDisk;
    /// # use bitacora_types::ReplicaId;
    /// use bitacora_types::replica_api::{ReplicaApi, ReplicaError, StaticReplicaConnector};
    ///
    /// let replica = Replica::open(
    ///     Arc::new(MemoryDisk::new()),
    ///     ReplicaId::new("replica-a")?,
    ///     ReplicaSettings::default(),
    ///     Arc::new(StaticReplicaConnector::new()),
    /// )
    /// .await?;
    ///
    /// replica.stop();
    ///
    /// let refused = replica.list_segments().await;
    /// assert!(matches!(refused, Err(ReplicaError::Unavailable { .. })));
    /// # Ok(())
    /// # }
    /// ```
    pub fn stop(&self) {
        self.is_stopped.store(true, Ordering::Relaxed);
        let mut registry = self.lock_registry();
        registry.tasks.abort_all();
        registry.handles_by_address.clear();
        tracing::info!(replica_id = %self.replica_id, "replica stopped");
    }

    /// Rereads every sealed copy and marks the ones that fail a checksum as damaged, so that the
    /// controller replaces them (design, section 7, "Integrity").
    ///
    /// Reads every sealed records file from the disk, one after the other. Open copies are not reread:
    /// they are verified by the reads and the recovery that touch them.
    ///
    /// # Returns
    ///
    /// How many copies the replica holds that are damaged afterwards, sealed or not.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaError::Unavailable`] if the replica is stopped while it scrubs.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use std::sync::Arc;
    /// # use bitacora_replica::{Replica, ReplicaSettings};
    /// # use bitacora_segment::MemoryDisk;
    /// # use bitacora_types::ReplicaId;
    /// # use bitacora_types::replica_api::StaticReplicaConnector;
    /// let replica = Replica::open(
    ///     Arc::new(MemoryDisk::new()),
    ///     ReplicaId::new("replica-a")?,
    ///     ReplicaSettings::default(),
    ///     Arc::new(StaticReplicaConnector::new()),
    /// )
    /// .await?;
    ///
    /// let damaged_copies = replica.scrub_sealed_segments().await?;
    ///
    /// assert_eq!(damaged_copies, 0);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn scrub_sealed_segments(&self) -> Result<usize, ReplicaError> {
        let mut damaged_count = 0;
        for handle in self.all_handles() {
            let mut is_sealed = false;
            if let Some(status) = handle.status().await? {
                is_sealed = status.sealed_end.is_some();
            }
            if is_sealed {
                handle.verify().await?;
            }
            if let Some(status) = handle.status().await?
                && status.is_damaged
            {
                damaged_count += 1;
            }
        }
        Ok(damaged_count)
    }

    /// Reports the first write or flush the disk refused since the replica was opened.
    ///
    /// A host that polls uses this; one that can wait uses [`Replica::write_failed`]. Either way, a
    /// replica that reports a refusal must be stopped: after a refused flush the kernel may have dropped
    /// the unwritten pages, so the process can no longer trust what it sees of its files (design, section
    /// 7, "Flush policy"). The copy concerned already refuses every request.
    ///
    /// # Returns
    ///
    /// The refusal, or `None` if the disk has refused nothing.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use std::sync::Arc;
    /// # use bitacora_replica::{Replica, ReplicaSettings};
    /// # use bitacora_segment::MemoryDisk;
    /// # use bitacora_types::ReplicaId;
    /// # use bitacora_types::replica_api::StaticReplicaConnector;
    /// let replica = Replica::open(
    ///     Arc::new(MemoryDisk::new()),
    ///     ReplicaId::new("replica-a")?,
    ///     ReplicaSettings::default(),
    ///     Arc::new(StaticReplicaConnector::new()),
    /// )
    /// .await?;
    ///
    /// assert_eq!(replica.write_failure(), None);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn write_failure(&self) -> Option<WriteFailure> {
        self.write_failures.first()
    }

    /// Waits until the disk refuses a write or a flush.
    ///
    /// Returns at once if it already has. Never returns on a healthy disk, so a host runs it next to
    /// whatever else it waits for and stops the process when it completes: see
    /// [`Replica::write_failure`] for why.
    ///
    /// # Returns
    ///
    /// The first refusal: the segment whose copy was being written, and what the disk reported.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use std::sync::Arc;
    /// # use bitacora_replica::{Replica, ReplicaSettings};
    /// # use bitacora_segment::MemoryDisk;
    /// # use bitacora_types::ReplicaId;
    /// # use bitacora_types::replica_api::StaticReplicaConnector;
    /// use std::time::Duration;
    ///
    /// let replica = Replica::open(
    ///     Arc::new(MemoryDisk::new()),
    ///     ReplicaId::new("replica-a")?,
    ///     ReplicaSettings::default(),
    ///     Arc::new(StaticReplicaConnector::new()),
    /// )
    /// .await?;
    ///
    /// // A host waits for this together with its shutdown signal. Here nothing is refused.
    /// let waited = tokio::time::timeout(Duration::from_millis(10), replica.write_failed()).await;
    ///
    /// assert!(waited.is_err());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write_failed(&self) -> WriteFailure {
        self.write_failures.wait_for_first().await
    }

    /// Locks the registry, whether or not another holder of the lock panicked: registry updates are
    /// single map operations, so a panic elsewhere cannot leave the map half-updated.
    fn lock_registry(&self) -> MutexGuard<'_, Registry> {
        self.registry.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Returns the handle of every segment task, in the order of the segments' addresses.
    fn all_handles(&self) -> Vec<SegmentHandle> {
        self.lock_registry().handles_by_address.values().cloned().collect()
    }

    /// Returns the handle of the task of the copy at `address`, or `None` if the replica has no task for
    /// that segment.
    fn existing_handle(&self, address: SegmentAddress) -> Option<SegmentHandle> {
        self.lock_registry().handles_by_address.get(&address).cloned()
    }

    /// Refuses the request at hand if the replica was stopped.
    fn fail_if_stopped(&self) -> Result<(), ReplicaError> {
        if self.is_stopped.load(Ordering::Relaxed) {
            return Err(ReplicaError::Unavailable {
                reason: "the replica is stopped".to_owned(),
            });
        }
        Ok(())
    }

    /// Returns the handle of the task of the copy at `address`, starting a task with an empty copy if
    /// the replica has nothing for that segment yet.
    ///
    /// Whether the replica is stopped is checked under the registry lock, so that no task can be started
    /// after [`Replica::stop`] has aborted them all.
    fn handle_or_spawn(&self, address: SegmentAddress) -> Result<SegmentHandle, ReplicaError> {
        let mut registry = self.lock_registry();
        self.fail_if_stopped()?;
        if let Some(handle) = registry.handles_by_address.get(&address) {
            return Ok(handle.clone());
        }
        let copy = SegmentCopy::absent(
            address,
            Arc::clone(&self.disk),
            self.settings.preallocated_segment_bytes,
        );
        let handle = SegmentHandle::spawn(address, copy, &mut registry.tasks, self.write_failures.clone());
        registry.handles_by_address.insert(address, handle.clone());
        Ok(handle)
    }
}

/// Reads the identity stored on `disk`, or stores `proposed_replica_id` if the disk has none.
///
/// The identity is stored durably before it is returned, so a replica never answers under an identity
/// that a crash could make it forget.
async fn adopt_identity(disk: &dyn Disk, proposed_replica_id: ReplicaId) -> Result<ReplicaId, ReplicaError> {
    let stored_identity = match disk.read_file(REPLICA_ID_PATH).await {
        Ok(stored_identity) => stored_identity,
        Err(io_error) => {
            return Err(ReplicaError::Storage {
                reason: format!("cannot read the identity of the data directory: {io_error}"),
            });
        }
    };
    match stored_identity {
        Some(stored_identity) => parse_stored_identity(&stored_identity),
        None => store_proposed_identity(disk, proposed_replica_id).await,
    }
}

/// Stores `proposed_replica_id` as the identity of the data directory on `disk`, which has none yet, and
/// returns it.
async fn store_proposed_identity(disk: &dyn Disk, proposed_replica_id: ReplicaId) -> Result<ReplicaId, ReplicaError> {
    let proposed_identity = Bytes::copy_from_slice(proposed_replica_id.as_str().as_bytes());
    if let Err(io_error) = disk.write_file_atomically(REPLICA_ID_PATH, proposed_identity).await {
        return Err(ReplicaError::Storage {
            reason: format!("cannot store the identity in the data directory: {io_error}"),
        });
    }
    Ok(proposed_replica_id)
}

/// Reads the identity of a data directory out of the contents of its identity file. A file that is not
/// text, or not a valid identifier, means the directory cannot be used as it is.
fn parse_stored_identity(stored_identity: &Bytes) -> Result<ReplicaId, ReplicaError> {
    let identity_text = match String::from_utf8(stored_identity.to_vec()) {
        Ok(identity_text) => identity_text,
        Err(utf8_error) => {
            return Err(ReplicaError::Storage {
                reason: format!("{REPLICA_ID_PATH} is not text: {utf8_error}"),
            });
        }
    };
    match ReplicaId::new(identity_text) {
        Ok(replica_id) => Ok(replica_id),
        Err(invalid) => Err(ReplicaError::Storage {
            reason: format!("{REPLICA_ID_PATH} is invalid: {invalid}"),
        }),
    }
}

/// Removes the scratch files that interrupted repairs left in the data directory. A repair builds a new
/// copy there and moves it into place only when it is complete, so a leftover holds nothing that is not
/// elsewhere.
async fn remove_repair_leftovers(disk: &dyn Disk, stored_paths: &[String]) -> Result<(), ReplicaError> {
    for path in stored_paths {
        if !is_repair_scratch_path(path) {
            continue;
        }
        if let Err(io_error) = disk.remove_file(path).await {
            return Err(directory_failure("clean", &io_error));
        }
    }
    Ok(())
}

/// Loads the copy of every segment that `stored_paths` holds a file for and starts its task.
async fn load_stored_copies(
    disk: &Arc<dyn Disk>,
    stored_paths: &[String],
    settings: ReplicaSettings,
    write_failures: &WriteFailureReport,
) -> Registry {
    let mut registry = Registry {
        handles_by_address: BTreeMap::new(),
        tasks: JoinSet::new(),
    };
    for address in stored_segment_addresses(stored_paths) {
        let copy = SegmentCopy::load(address, Arc::clone(disk), settings.preallocated_segment_bytes).await;
        let handle = SegmentHandle::spawn(address, copy, &mut registry.tasks, write_failures.clone());
        registry.handles_by_address.insert(address, handle);
    }
    registry
}

/// Builds the answer for an operation on the data directory as a whole that the disk refused.
/// `operation` is a verb that fits "cannot ... the data directory".
fn directory_failure(operation: &str, io_error: &io::Error) -> ReplicaError {
    ReplicaError::Storage {
        reason: format!("cannot {operation} the data directory: {io_error}"),
    }
}

#[cfg(test)]
#[path = "replica_test.rs"]
mod tests;
