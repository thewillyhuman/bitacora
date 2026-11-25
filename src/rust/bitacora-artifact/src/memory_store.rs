//! The in-memory artifact store, for tests and simulation.
//!
//! # Why this file exists
//!
//! Most of bitacora is tested in process: unit tests, the test cluster, and the deterministic simulator,
//! which replays a run from a seed. None of them can use a real object store, and all of them need to see
//! what the code under test does when the object store misbehaves: when it is down, when it never answers,
//! when it hands back other bytes than it was given. This store is a map in memory with a switch for each
//! of those faults.
//!
//! # What it contains
//!
//! - [`MemoryArtifactStore`]: the store, with its fault switches and one counter.
//! - `MemoryArtifacts`: the state behind the store's lock.
//!
//! # How it works
//!
//! Objects live in a sorted map from key to contents, behind one mutex. A store is a handle: cloning it
//! gives a second handle on the same objects, the way two processes share one bucket. That is how a test
//! gives the same store to three replicas, a controller and a reader.
//!
//! Every operation of the interface first passes the fault switches. While the store is *unresponsive*
//! the operation waits forever, so the caller's own timeout is what ends it. While the store is
//! *unavailable* the operation fails with [`ArtifactError::Unavailable`]. An unresponsive store never
//! gets as far as failing, so that switch wins when both are on. Neither fault changes any object.
//!
//! The sorted map is not an optimization: a listing must be sorted by key, and iterating a sorted map from
//! the prefix on gives exactly that. The workspace also forbids hash maps, whose iteration order would
//! make a simulated run differ from its replay.
//!
//! # Invariants
//!
//! - The mutex is never held across an `await`: every critical section is one map operation.
//! - A fault switch never changes what is stored. Only [`MemoryArtifactStore::corrupt_byte`] damages an
//!   object, and only the one byte it is told to.
//! - The store behaves exactly like any other backend when no switch is on: it passes
//!   [`crate::contract::check`].
//!
//! # See also
//!
//! - `store.rs`: the interface this implements.
//! - `local_store.rs`: the backend that survives a restart.

use std::collections::BTreeMap;
use std::future::pending;
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;

use crate::store::{ArtifactError, ArtifactStore, ObjectInfo, slice_range, validate_key};

/// What an operation is told while the outage switch is on. A test that prints the error sees at once
/// that the failure was injected and not real.
const INJECTED_OUTAGE_REASON: &str = "injected outage";

/// Everything a [`MemoryArtifactStore`] holds, behind its lock.
#[derive(Debug, Default)]
struct MemoryArtifacts {
    /// The stored objects. Sorted by key, which is the order a listing must have.
    objects_by_key: BTreeMap<String, Bytes>,
    /// While set, every operation of the interface fails with `Unavailable`.
    is_unavailable: bool,
    /// While set, no operation of the interface ever answers. Checked before `is_unavailable`.
    is_unresponsive: bool,
    /// How many range reads of each object were answered with bytes since the store was created.
    served_range_reads_by_key: BTreeMap<String, u64>,
}

/// An artifact store that keeps its objects in memory.
///
/// Use it wherever a test or the simulator needs an object store. It behaves like any other backend, and
/// adds what only a test wants: switches that make it fail or hang, a way to damage a stored object, and
/// a count of the range reads it served.
///
/// A store is a handle. Clones share the same objects and the same switches, like two clients of one
/// bucket, so a test keeps one clone to inject faults and gives the others to the code under test.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), bitacora_artifact::ArtifactError> {
/// use bitacora_artifact::{ArtifactError, ArtifactStore, MemoryArtifactStore};
/// use bytes::Bytes;
///
/// let artifact_store = MemoryArtifactStore::new();
/// let handle_of_the_code_under_test = artifact_store.clone();
/// handle_of_the_code_under_test
///     .put_object("streams/7/segments/3", Bytes::from_static(b"segment"))
///     .await?;
///
/// artifact_store.set_unavailable(true);
/// let during_the_outage = handle_of_the_code_under_test
///     .get_object("streams/7/segments/3")
///     .await;
///
/// assert!(matches!(
///     during_the_outage,
///     Err(ArtifactError::Unavailable { .. })
/// ));
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default)]
pub struct MemoryArtifactStore {
    /// The objects and switches, shared by every clone of this handle.
    artifacts: Arc<Mutex<MemoryArtifacts>>,
}

impl MemoryArtifactStore {
    /// Creates a store that holds no objects and has every fault switched off.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_artifact::MemoryArtifactStore;
    ///
    /// let artifact_store = MemoryArtifactStore::new();
    /// assert_eq!(artifact_store.served_range_reads(""), 0);
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Switches a simulated outage on or off.
    ///
    /// While the store is unavailable, every operation of the interface fails with
    /// [`ArtifactError::Unavailable`] and nothing stored changes. Operations already past the switch are
    /// not affected.
    ///
    /// # Arguments
    ///
    /// * `is_unavailable` - `true` starts the outage, `false` ends it.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// use bitacora_artifact::{ArtifactError, ArtifactStore, MemoryArtifactStore};
    ///
    /// let artifact_store = MemoryArtifactStore::new();
    /// artifact_store.set_unavailable(true);
    ///
    /// let listed = artifact_store.list_prefix("").await;
    ///
    /// assert!(matches!(listed, Err(ArtifactError::Unavailable { .. })));
    /// # }
    /// ```
    pub fn set_unavailable(&self, is_unavailable: bool) {
        self.lock_artifacts().is_unavailable = is_unavailable;
    }

    /// Switches a simulated hang on or off.
    ///
    /// While the store is unresponsive, every operation of the interface that starts waits forever and
    /// nothing stored changes: this is a store that accepts requests and never answers them. The caller's
    /// own timeout is what ends the wait. An operation that began to wait keeps waiting after the switch
    /// is turned off; only operations started afterwards are answered.
    ///
    /// # Arguments
    ///
    /// * `is_unresponsive` - `true` makes the store stop answering, `false` makes it answer again.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() {
    /// use std::time::Duration;
    ///
    /// use bitacora_artifact::{ArtifactStore, MemoryArtifactStore};
    ///
    /// let artifact_store = MemoryArtifactStore::new();
    /// artifact_store.set_unresponsive(true);
    ///
    /// let answer =
    ///     tokio::time::timeout(Duration::from_secs(30), artifact_store.list_prefix("")).await;
    ///
    /// assert!(answer.is_err(), "the caller's timeout is what ended the wait");
    /// # }
    /// ```
    pub fn set_unresponsive(&self, is_unresponsive: bool) {
        self.lock_artifacts().is_unresponsive = is_unresponsive;
    }

    /// Simulates silent corruption by flipping bits in one byte of a stored object.
    ///
    /// The object keeps its key and its size; only the chosen byte changes. Does nothing if there is no
    /// such object or the object has no byte at that offset. The fault switches do not apply: this is the
    /// test reaching into the store, not a request to it.
    ///
    /// # Arguments
    ///
    /// * `object_key` - The key of the object to damage.
    /// * `offset_bytes` - The offset of the byte to change, counted from the start of the object.
    /// * `flipped_bits` - A mask of the bits to flip in that byte. `0x01` flips the lowest bit.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_artifact::ArtifactError> {
    /// use bitacora_artifact::{ArtifactStore, MemoryArtifactStore};
    /// use bytes::Bytes;
    ///
    /// let artifact_store = MemoryArtifactStore::new();
    /// artifact_store
    ///     .put_object("layers/0001", Bytes::from_static(b"abc"))
    ///     .await?;
    ///
    /// artifact_store.corrupt_byte("layers/0001", 1, 0x01);
    ///
    /// let damaged = artifact_store.get_object("layers/0001").await?;
    /// assert_eq!(damaged, Bytes::from_static(b"acc"));
    /// # Ok(())
    /// # }
    /// ```
    pub fn corrupt_byte(&self, object_key: &str, offset_bytes: usize, flipped_bits: u8) {
        let mut artifacts = self.lock_artifacts();
        let Some(contents) = artifacts.objects_by_key.get_mut(object_key) else {
            return;
        };
        // Stored contents are shared and immutable, so the damaged object is a new copy.
        let mut damaged_contents = contents.to_vec();
        if let Some(damaged_byte) = damaged_contents.get_mut(offset_bytes) {
            *damaged_byte ^= flipped_bits;
        }
        *contents = Bytes::from(damaged_contents);
    }

    /// Reports how many range reads the store has answered with bytes since it was created.
    ///
    /// A reader served by a replica and a reader served by the object store return the same records, so
    /// the result of a read does not say where it came from. This count does. Reads that failed, and reads
    /// of whole objects, are not counted.
    ///
    /// # Arguments
    ///
    /// * `key_prefix` - Only reads of objects whose key starts with this are counted. Several kinds of
    ///   object share a store, each under its own prefix.
    ///
    /// # Returns
    ///
    /// The number of [`ArtifactStore::get_range`] calls that returned bytes of an object whose key starts
    /// with `key_prefix`, over all clones of the store. An empty prefix counts every range read.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_artifact::ArtifactError> {
    /// use bitacora_artifact::{ArtifactStore, MemoryArtifactStore};
    /// use bytes::Bytes;
    ///
    /// let artifact_store = MemoryArtifactStore::new();
    /// artifact_store
    ///     .put_object("layers/0001", Bytes::from_static(b"abc"))
    ///     .await?;
    ///
    /// artifact_store.get_range("layers/0001", 0..2).await?;
    ///
    /// assert_eq!(artifact_store.served_range_reads("layers/"), 1);
    /// assert_eq!(artifact_store.served_range_reads("segments/"), 0);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn served_range_reads(&self, key_prefix: &str) -> u64 {
        let artifacts = self.lock_artifacts();
        let mut served_range_reads = 0;
        for (object_key, read_count) in &artifacts.served_range_reads_by_key {
            if object_key.starts_with(key_prefix) {
                served_range_reads += read_count;
            }
        }
        served_range_reads
    }

    /// Locks the store's state, whether or not another holder of the lock panicked.
    ///
    /// A poisoned lock is taken anyway: every critical section is a single map operation or the write of
    /// one field, so a panic elsewhere cannot have left the state half changed.
    fn lock_artifacts(&self) -> MutexGuard<'_, MemoryArtifacts> {
        self.artifacts.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Locks the store's state for one operation of the interface, after applying the fault switches.
    ///
    /// Waits forever while the store is unresponsive. The check is made once, when the operation starts.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactError::Unavailable`] while the outage switch is on.
    async fn lock_when_available(&self) -> Result<MutexGuard<'_, MemoryArtifacts>, ArtifactError> {
        // The guard of this first look is dropped before the wait: the lock is never held across it.
        let is_unresponsive = self.lock_artifacts().is_unresponsive;
        if is_unresponsive {
            pending::<()>().await;
        }
        let artifacts = self.lock_artifacts();
        if artifacts.is_unavailable {
            return Err(ArtifactError::Unavailable {
                reason: INJECTED_OUTAGE_REASON.to_owned(),
            });
        }
        Ok(artifacts)
    }
}

#[async_trait]
impl ArtifactStore for MemoryArtifactStore {
    /// Stores the object in the map, replacing any object under the same key. The map shares the
    /// caller's bytes; nothing is copied.
    async fn put_object(&self, object_key: &str, contents: Bytes) -> Result<(), ArtifactError> {
        validate_key(object_key)?;
        let mut artifacts = self.lock_when_available().await?;
        artifacts.objects_by_key.insert(object_key.to_owned(), contents);
        Ok(())
    }

    /// Returns the stored object. The result shares the stored bytes; nothing is copied.
    async fn get_object(&self, object_key: &str) -> Result<Bytes, ArtifactError> {
        validate_key(object_key)?;
        let artifacts = self.lock_when_available().await?;
        match artifacts.objects_by_key.get(object_key) {
            Some(contents) => Ok(contents.clone()),
            None => Err(ArtifactError::NotFound {
                object_key: object_key.to_owned(),
            }),
        }
    }

    /// Reads the whole object and cuts the range out of it, counting the read if it returned bytes.
    async fn get_range(&self, object_key: &str, byte_range: Range<u64>) -> Result<Bytes, ArtifactError> {
        let contents = self.get_object(object_key).await?;
        let range_contents = slice_range(object_key, &contents, &byte_range)?;
        *self
            .lock_artifacts()
            .served_range_reads_by_key
            .entry(object_key.to_owned())
            .or_default() += 1;
        Ok(range_contents)
    }

    /// Removes the object from the map, if it is there.
    async fn delete_object(&self, object_key: &str) -> Result<(), ArtifactError> {
        validate_key(object_key)?;
        let mut artifacts = self.lock_when_available().await?;
        artifacts.objects_by_key.remove(object_key);
        Ok(())
    }

    /// Walks the sorted map from the prefix on and stops at the first key that no longer starts with it.
    async fn list_prefix(&self, key_prefix: &str) -> Result<Vec<ObjectInfo>, ArtifactError> {
        let artifacts = self.lock_when_available().await?;
        let mut listed = Vec::new();
        for (object_key, contents) in artifacts.objects_by_key.range(key_prefix.to_owned()..) {
            if !object_key.starts_with(key_prefix) {
                break;
            }
            listed.push(ObjectInfo {
                object_key: object_key.clone(),
                size_bytes: u64::try_from(contents.len()).expect("lengths fit in 64 bits"),
            });
        }
        Ok(listed)
    }
}

#[cfg(test)]
#[path = "memory_store_test.rs"]
mod tests;
